package upgrade

// TODO consider whether this needs to exist after https://github.com/kubernetes/kubernetes/pull/79495
// is merged, or we make the minimum supported version of k8s v1.14

import (
	"context"
	"strings"
	"time"

	"github.com/open-policy-agent/gatekeeper/pkg/util"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var log = logf.Log.WithName("controller").WithValues("metaKind", "upgrade")

const (
	crdName = "constrainttemplates.templates.gatekeeper.sh"
)

// Manager allows us to upgrade resources on startup.
type Manager struct {
	client client.Client
	mgr    manager.Manager
	ctx    context.Context
}

// New creates a new manager for audit.
func New(ctx context.Context, mgr manager.Manager) (*Manager, error) {
	am := &Manager{
		mgr: mgr,
		ctx: ctx,
	}
	return am, nil
}

// Start implements the Runnable interface.
func (um *Manager) Start(ctx context.Context) error {
	log.Info("Starting Upgrade Manager")
	defer log.Info("Stopping upgrade manager workers")
	errCh := make(chan error)
	go func() { errCh <- um.upgrade(ctx) }()
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if err != nil {
			return err
		}
	}
	// We must block indefinitely or manager will exit
	<-ctx.Done()
	return nil
}

func (um *Manager) ensureCRDExists(ctx context.Context) error {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	return um.client.Get(ctx, types.NamespacedName{Name: crdName}, crd)
}

func (um *Manager) getAllKinds(groupVersion string) (*metav1.APIResourceList, error) {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(um.mgr.GetConfig())
	if err != nil {
		return nil, err
	}
	return discoveryClient.ServerResourcesForGroupVersion(groupVersion)
}

func (um *Manager) upgrade(ctx context.Context) error {
	gvs := []string{
		"constraints.gatekeeper.sh/v1alpha1",
		"templates.gatekeeper.sh/v1alpha1",
	}
	for _, gv := range gvs {
		if err := um.upgradeGroupVersion(ctx, gv); err != nil {
			return err
		}
	}
	return nil
}

// upgradeGroupVersion touches each resource in a given groupVersion, incrementing its storage version.
func (um *Manager) upgradeGroupVersion(ctx context.Context, groupVersion string) error {
	// new client to get updated restmapper
	c, err := client.New(um.mgr.GetConfig(), client.Options{Scheme: um.mgr.GetScheme(), Mapper: nil})
	if err != nil {
		return err
	}
	um.client = c
	if err := um.ensureCRDExists(ctx); err != nil {
		log.Info("required crd has not been deployed ", "CRD", crdName)
		return err
	}
	// get all resource kinds
	resourceList, err := um.getAllKinds(groupVersion)
	if err != nil {
		// If the resource doesn't exist, it doesn't need upgrading
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	resourceGV := strings.Split(resourceList.GroupVersion, "/")
	group := resourceGV[0]
	version := resourceGV[1]

	// For some reason we have seen duplicate kinds, suppress that
	uniqueKinds := make(map[string]bool)
	for i := range resourceList.APIResources {
		uniqueKinds[resourceList.APIResources[i].Kind] = true
	}

	// get resource for each Kind
	for kind := range uniqueKinds {
		log.Info("resource", "kind", kind, "group", group, "version", version)
		resourceGvk := schema.GroupVersionKind{
			Group:   group,
			Version: version,
			Kind:    kind + "List",
		}
		instanceList := &unstructured.UnstructuredList{}
		instanceList.SetGroupVersionKind(resourceGvk)
		err := um.client.List(ctx, instanceList)
		if err != nil {
			return err
		}
		log.Info("resource count", "count", len(instanceList.Items))
		updateResources := make(map[util.KindVersionResource]unstructured.Unstructured, len(instanceList.Items))
		// get each resource
		for _, item := range instanceList.Items {
			key := util.GetUniqueKey(item)
			updateResources[key] = item
		}

		if len(updateResources) > 0 {
			urloop := &updateResourceLoop{
				ur:      updateResources,
				client:  um.client,
				stop:    make(chan struct{}),
				stopped: make(chan struct{}),
			}
			log.Info("starting update resources loop", "group", group, "version", version, "kind", kind)
			go urloop.update(ctx)
		}
	}
	return nil
}

type updateResourceLoop struct {
	ur      map[util.KindVersionResource]unstructured.Unstructured
	client  client.Client
	stop    chan struct{}
	stopped chan struct{}
}

func (urloop *updateResourceLoop) update(ctx context.Context) {
	defer close(urloop.stopped)
	updateLoop := func() (bool, error) {
		for _, item := range urloop.ur {
			select {
			case <-urloop.stop:
				return true, nil
			default:
				failure := false
				var latestItem unstructured.Unstructured
				item.DeepCopyInto(&latestItem)
				name := latestItem.GetName()
				namespace := latestItem.GetNamespace()
				namespacedName := types.NamespacedName{
					Name:      name,
					Namespace: namespace,
				}
				// get the latest constraint
				err := urloop.client.Get(ctx, namespacedName, &latestItem)
				if err != nil {
					failure = true
					log.Error(err, "could not get latest resource during update", "name", name, "namespace", namespace)
				}
				if err := urloop.client.Update(ctx, &latestItem); err != nil {
					failure = true
					log.Error(err, "could not update resource", "name", name, "namespace", namespace)
				}
				if !failure {
					key := util.GetUniqueKey(latestItem)
					delete(urloop.ur, key)
				}
			}
		}
		if len(urloop.ur) == 0 {
			return true, nil
		}
		return false, nil
	}

	if err := wait.ExponentialBackoff(wait.Backoff{
		Duration: 1 * time.Second,
		Factor:   2,
		Jitter:   1,
		Steps:    5,
	}, updateLoop); err != nil {
		log.Error(err, "could not update resource reached max retries", "remaining update resources", urloop.ur)
	}
}
