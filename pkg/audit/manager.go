package audit

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	constraintTypes "github.com/open-policy-agent/frameworks/constraint/pkg/types"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/target"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var log = logf.Log.WithName("controller").WithValues(logging.Process, "audit")

const (
	crdName                          = "constrainttemplates.templates.gatekeeper.sh"
	constraintsGV                    = "constraints.gatekeeper.sh/v1beta1"
	msgSize                          = 256
	defaultAuditInterval             = 60
	defaultConstraintViolationsLimit = 20
	defaultListLimit                 = 0
)

var (
	auditInterval             = flag.Uint("audit-interval", defaultAuditInterval, "interval to run audit in seconds. defaulted to 60 secs if unspecified, 0 to disable")
	constraintViolationsLimit = flag.Uint("constraint-violations-limit", defaultConstraintViolationsLimit, "limit of number of violations per constraint. defaulted to 20 violations if unspecified")
	auditChunkSize            = flag.Uint64("audit-chunk-size", defaultListLimit, "(alpha) Kubernetes API chunking List results when retrieving cluster resources using discovery client. defaulted to 0 if unspecified")
	auditFromCache            = flag.Bool("audit-from-cache", false, "pull resources from OPA cache when auditing")
	emitAuditEvents           = flag.Bool("emit-audit-events", false, "(alpha) emit Kubernetes events in gatekeeper namespace with detailed info for each violation from an audit")
	auditMatchKindOnly        = flag.Bool("audit-match-kind-only", false, "only use kinds specified in all constraints for auditing cluster resources. if kind is not specified in any of the constraints, it will audit all resources (same as setting this flag to false)")
	emptyAuditResults         []auditResult
)

// Manager allows us to audit resources periodically.
type Manager struct {
	client          client.Client
	opa             *opa.Client
	stopper         chan struct{}
	stopped         chan struct{}
	mgr             manager.Manager
	ctx             context.Context
	ucloop          *updateConstraintLoop
	reporter        *reporter
	log             logr.Logger
	processExcluder *process.Excluder
	eventRecorder   record.EventRecorder
	gkNamespace     string
}

type auditResult struct {
	cname             string
	cnamespace        string
	cgvk              schema.GroupVersionKind
	capiversion       string
	rkind             string
	rname             string
	rnamespace        string
	message           string
	enforcementAction string
	constraint        *unstructured.Unstructured
}

// StatusViolation represents each violation under status.
type StatusViolation struct {
	Kind              string `json:"kind"`
	Name              string `json:"name"`
	Namespace         string `json:"namespace,omitempty"`
	Message           string `json:"message"`
	EnforcementAction string `json:"enforcementAction"`
}

// nsCache is used for caching namespaces and their labels.
type nsCache struct {
	cache map[string]corev1.Namespace
}

func newNSCache() *nsCache {
	return &nsCache{
		cache: make(map[string]corev1.Namespace),
	}
}

func (c *nsCache) Get(ctx context.Context, client client.Client, namespace string) (corev1.Namespace, error) {
	if ns, ok := c.cache[namespace]; !ok {
		if err := client.Get(ctx, types.NamespacedName{Name: namespace}, &ns); err != nil {
			return corev1.Namespace{}, err
		}
		c.cache[namespace] = ns
	}

	return c.cache[namespace], nil
}

// New creates a new manager for audit.
func New(ctx context.Context, mgr manager.Manager, opa *opa.Client, processExcluder *process.Excluder) (*Manager, error) {
	reporter, err := newStatsReporter(ctx)
	if err != nil {
		log.Error(err, "StatsReporter could not start")
		return nil, err
	}
	eventBroadcaster := record.NewBroadcaster()
	kubeClient := kubernetes.NewForConfigOrDie(mgr.GetConfig())
	eventBroadcaster.StartRecordingToSink(&clientcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		corev1.EventSource{Component: "gatekeeper-audit"})

	am := &Manager{
		opa:             opa,
		stopper:         make(chan struct{}),
		stopped:         make(chan struct{}),
		mgr:             mgr,
		ctx:             ctx,
		reporter:        reporter,
		processExcluder: processExcluder,
		eventRecorder:   recorder,
		gkNamespace:     util.GetNamespace(),
	}
	return am, nil
}

// audit performs an audit then updates the status of all constraint resources with the results.
func (am *Manager) audit(ctx context.Context) error {
	startTime := time.Now()
	timestamp := startTime.UTC().Format(time.RFC3339)
	am.log = log.WithValues(logging.AuditID, timestamp)
	logStart(am.log)
	// record audit latency
	defer func() {
		logFinish(am.log)
		latency := time.Since(startTime)
		if err := am.reporter.reportLatency(latency); err != nil {
			am.log.Error(err, "failed to report latency")
		}
	}()

	if err := am.reporter.reportRunStart(startTime); err != nil {
		am.log.Error(err, "failed to report run start time")
	}

	// new client to get updated restmapper
	c, err := client.New(am.mgr.GetConfig(), client.Options{Scheme: am.mgr.GetScheme(), Mapper: nil})
	if err != nil {
		return err
	}
	am.client = c
	// don't audit anything until the constraintTemplate crd is in the cluster
	if err := am.ensureCRDExists(ctx); err != nil {
		am.log.Info("Audit exits, required crd has not been deployed ", "CRD", crdName)
		return nil
	}

	// get all constraint kinds
	constraintsGVKs, err := am.getAllConstraintKinds()
	if err != nil {
		// if no constraint is found with the constraint apiversion, then return
		am.log.Info("no constraint is found with apiversion", "constraint apiversion", constraintsGV)
		return nil
	}

	var resp *constraintTypes.Responses
	var res []*constraintTypes.Result

	updateLists := make(map[util.KindVersionResource][]auditResult)
	totalViolationsPerConstraint := make(map[util.KindVersionResource]int64)
	totalViolationsPerEnforcementAction := make(map[util.EnforcementAction]int64)
	// resetting total violations per enforcement action
	for _, action := range util.KnownEnforcementActions {
		totalViolationsPerEnforcementAction[action] = 0
	}

	if *auditFromCache {
		am.log.Info("Auditing from cache")
		resp, err = am.opa.Audit(ctx)
		if err != nil {
			return err
		}
		res = resp.Results()
		am.log.Info("Audit opa.Audit() results", "violations", len(res))

		err := am.addAuditResponsesToUpdateLists(updateLists, res, totalViolationsPerConstraint, totalViolationsPerEnforcementAction, timestamp)
		if err != nil {
			return err
		}
	} else {
		am.log.Info("Auditing via discovery client")
		err := am.auditResources(ctx, constraintsGVKs, updateLists, totalViolationsPerConstraint, totalViolationsPerEnforcementAction, timestamp)
		if err != nil {
			return err
		}
	}

	// log constraints with violations
	for link := range updateLists {
		ar := updateLists[link][0]
		logConstraint(am.log, ar.constraint, ar.enforcementAction, totalViolationsPerConstraint[link])
	}

	for k, v := range totalViolationsPerEnforcementAction {
		if err := am.reporter.reportTotalViolations(k, v); err != nil {
			am.log.Error(err, "failed to report total violations")
		}
	}

	// update constraints for each kind
	am.writeAuditResults(ctx, constraintsGVKs, updateLists, timestamp, totalViolationsPerConstraint)

	return nil
}

// Audits server resources via the discovery client, as an alternative to opa.Client.Audit().
func (am *Manager) auditResources(
	ctx context.Context,
	constraintsGVK []schema.GroupVersionKind,
	updateLists map[util.KindVersionResource][]auditResult,
	totalViolationsPerConstraint map[util.KindVersionResource]int64,
	totalViolationsPerEnforcementAction map[util.EnforcementAction]int64,
	timestamp string) error {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(am.mgr.GetConfig())
	if err != nil {
		return err
	}

	serverResourceLists, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		if discovery.IsGroupDiscoveryFailedError(err) {
			am.log.Error(err, "Kubernetes has an orphaned APIService. Delete orphaned APIService using kubectl delete apiservice <name>")
		} else {
			return err
		}
	}

	clusterAPIResources := make(map[metav1.GroupVersion]map[string]bool)
	for _, rl := range serverResourceLists {
		gvParsed, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			am.log.Error(err, "Error parsing groupversion", "groupversion", rl.GroupVersion)
			continue
		}

		gv := metav1.GroupVersion{
			Group:   gvParsed.Group,
			Version: gvParsed.Version,
		}
		if _, ok := clusterAPIResources[gv]; !ok {
			clusterAPIResources[gv] = make(map[string]bool)
		}
		for i := range rl.APIResources {
			for _, verb := range rl.APIResources[i].Verbs {
				if verb == "list" {
					clusterAPIResources[gv][rl.APIResources[i].Kind] = true
					break
				}
			}
		}
	}

	var errs opa.Errors
	nsCache := newNSCache()

	matchedKinds := make(map[string]bool)
	if *auditMatchKindOnly {
		constraintList := &unstructured.UnstructuredList{}
	constraintsLoop:
		for _, c := range constraintsGVK {
			constraintList.SetGroupVersionKind(c)
			if err = am.client.List(ctx, constraintList); err != nil {
				am.log.Error(err, "Unable to list objects for gvk", "group", c.Group, "version", c.Version, "kind", c.Kind)
				continue
			}
			for _, constraint := range constraintList.Items {
				kinds, found, err := unstructured.NestedSlice(constraint.Object, "spec", "match", "kinds")
				if err != nil {
					am.log.Error(err, "Unable to return spec.match.kinds field", "group", c.Group, "version", c.Version, "kind", c.Kind)
					// looking at all kinds if there is an error
					matchedKinds["*"] = true
					break constraintsLoop
				}
				if found {
					for _, k := range kinds {
						kind, ok := k.(map[string]interface{})
						if !ok {
							am.log.Error(errors.New("could not cast kind as map[string]"), "kind", k)
							continue
						}
						kindsKind, _, err := unstructured.NestedSlice(kind, "kinds")
						if err != nil {
							am.log.Error(err, "Unable to return kinds.kinds field", "group", c.Group, "version", c.Version, "kind", c.Kind)
							continue
						}
						for _, kk := range kindsKind {
							if kk.(string) == "" || kk.(string) == "*" {
								// no need to continue, all kinds are included
								matchedKinds["*"] = true
								break constraintsLoop
							}
							// adding constraint match kind to matchedKinds list
							matchedKinds[kk.(string)] = true
						}
					}
				} else {
					// if constraint doesn't have match kinds defined, we will look at all kinds
					matchedKinds["*"] = true
					break constraintsLoop
				}
			}
		}
	} else {
		matchedKinds["*"] = true
	}

	for gv, gvKinds := range clusterAPIResources {
	kindsLoop:
		for kind := range gvKinds {
			_, matchAll := matchedKinds["*"]
			if _, found := matchedKinds[kind]; !found && !matchAll {
				continue
			}

			objList := &unstructured.UnstructuredList{}
			opts := &client.ListOptions{
				Limit: int64(*auditChunkSize),
			}
			resourceVersion := ""

			for {
				objList.SetGroupVersionKind(schema.GroupVersionKind{
					Group:   gv.Group,
					Version: gv.Version,
					Kind:    kind + "List",
				})
				objList.SetResourceVersion(resourceVersion)

				err := am.client.List(ctx, objList, opts)
				if err != nil {
					am.log.Error(err, "Unable to list objects for gvk", "group", gv.Group, "version", gv.Version, "kind", kind)
					continue kindsLoop
				}

				for index := range objList.Items {
					objNamespace := objList.Items[index].GetNamespace()
					isExcludedNamespace, err := am.skipExcludedNamespace(&objList.Items[index])
					if err != nil {
						log.Error(err, "error while excluding namespaces")
					}

					if isExcludedNamespace {
						continue
					}

					ns := corev1.Namespace{}
					if objNamespace != "" {
						ns, err = nsCache.Get(ctx, am.client, objNamespace)
						if err != nil {
							am.log.Error(err, "Unable to look up object namespace", "group", gv.Group, "version", gv.Version, "kind", kind)
							continue
						}
					}

					augmentedObj := target.AugmentedUnstructured{
						Object:    objList.Items[index],
						Namespace: &ns,
					}
					resp, err := am.opa.Review(ctx, augmentedObj)
					if err != nil {
						errs = append(errs, err)
					} else if len(resp.Results()) > 0 {
						err = am.addAuditResponsesToUpdateLists(updateLists, resp.Results(), totalViolationsPerConstraint, totalViolationsPerEnforcementAction, timestamp)
						if err != nil {
							return err
						}
					}
				}

				resourceVersion = objList.GetResourceVersion()
				opts.Continue = objList.GetContinue()
				if opts.Continue == "" {
					break
				}
			}
		}
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

func (am *Manager) auditManagerLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(*auditInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("Audit Manager close")
			close(am.stopper)
			return
		case <-ticker.C:
			if err := am.audit(ctx); err != nil {
				log.Error(err, "audit manager audit() failed")
			}
		}
	}
}

// Start implements controller.Controller.
func (am *Manager) Start(ctx context.Context) error {
	log.Info("Starting Audit Manager")
	go am.auditManagerLoop(ctx)
	<-ctx.Done()
	log.Info("Stopping audit manager workers")
	return nil
}

func (am *Manager) ensureCRDExists(ctx context.Context) error {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	return am.client.Get(ctx, types.NamespacedName{Name: crdName}, crd)
}

func (am *Manager) getAllConstraintKinds() ([]schema.GroupVersionKind, error) {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(am.mgr.GetConfig())
	if err != nil {
		return nil, err
	}
	l, err := discoveryClient.ServerResourcesForGroupVersion(constraintsGV)
	if err != nil {
		return nil, err
	}
	resourceGV := strings.Split(constraintsGV, "/")
	group := resourceGV[0]
	version := resourceGV[1]
	// We have seen duplicate GVK entries on shifting to status client, remove them
	unique := make(map[schema.GroupVersionKind]bool)
	for i := range l.APIResources {
		unique[schema.GroupVersionKind{Group: group, Version: version, Kind: l.APIResources[i].Kind}] = true
	}
	var ret []schema.GroupVersionKind
	for gvk := range unique {
		ret = append(ret, gvk)
	}
	return ret, nil
}

func (am *Manager) addAuditResponsesToUpdateLists(
	updateLists map[util.KindVersionResource][]auditResult,
	res []*constraintTypes.Result,
	totalViolationsPerConstraint map[util.KindVersionResource]int64,
	totalViolationsPerEnforcementAction map[util.EnforcementAction]int64,
	timestamp string) error {
	for _, r := range res {
		key := util.GetUniqueKey(*r.Constraint)
		totalViolationsPerConstraint[key]++
		name := r.Constraint.GetName()
		namespace := r.Constraint.GetNamespace()
		apiVersion := r.Constraint.GetAPIVersion()
		gvk := r.Constraint.GroupVersionKind()
		enforcementAction := r.EnforcementAction
		message := r.Msg
		resource, ok := r.Resource.(*unstructured.Unstructured)
		if !ok {
			return errors.Errorf("could not cast resource as reviewResource: %v", r.Resource)
		}
		rname := resource.GetName()
		rkind := resource.GetKind()
		rnamespace := resource.GetNamespace()
		// append audit results only if it is below violations limit
		if uint(len(updateLists[key])) < *constraintViolationsLimit {
			result := auditResult{
				cgvk:              gvk,
				capiversion:       apiVersion,
				cname:             name,
				cnamespace:        namespace,
				rkind:             rkind,
				rname:             rname,
				rnamespace:        rnamespace,
				message:           message,
				enforcementAction: enforcementAction,
				constraint:        r.Constraint,
			}
			updateLists[key] = append(updateLists[key], result)
		}
		ea := util.EnforcementAction(enforcementAction)
		totalViolationsPerEnforcementAction[ea]++
		logViolation(am.log, r.Constraint, r.EnforcementAction, resource.GroupVersionKind(), rnamespace, rname, message)
		if *emitAuditEvents {
			emitEvent(r.Constraint, timestamp, enforcementAction, resource.GroupVersionKind(), rnamespace, rname, message, am.gkNamespace, am.eventRecorder)
		}
	}
	return nil
}

func (am *Manager) writeAuditResults(ctx context.Context, constraintsGVKs []schema.GroupVersionKind, updateLists map[util.KindVersionResource][]auditResult, timestamp string, totalViolations map[util.KindVersionResource]int64) {
	// if there is a previous reporting thread, close it before starting a new one
	if am.ucloop != nil {
		// this is closing the previous audit reporting thread
		am.log.Info("closing the previous audit reporting thread")
		close(am.ucloop.stop)
		select {
		case <-am.ucloop.stopped:
		case <-time.After(time.Duration(*auditInterval) * time.Second):
			// avoid deadlocking in cases where ucloop never stops
			// this creates potential leak of threads but avoids potential of deadlocking
			am.log.Info("timeout waiting for previous audit reporting thread to finish")
		}
	}

	am.ucloop = &updateConstraintLoop{
		client:  am.client,
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
		ul:      updateLists,
		ts:      timestamp,
		tv:      totalViolations,
		log:     am.log,
	}

	go am.ucloop.update(ctx, constraintsGVKs)
}

func (am *Manager) skipExcludedNamespace(obj *unstructured.Unstructured) (bool, error) {
	isNamespaceExcluded, err := am.processExcluder.IsNamespaceExcluded(process.Audit, obj)
	if err != nil {
		return false, err
	}

	return isNamespaceExcluded, err
}

func (ucloop *updateConstraintLoop) updateConstraintStatus(ctx context.Context, instance *unstructured.Unstructured, auditResults []auditResult, timestamp string, totalViolations int64) error {
	constraintName := instance.GetName()
	ucloop.log.Info("updating constraint status", "constraintName", constraintName)
	// create constraint status violations
	var statusViolations []interface{}
	for i := range auditResults {
		ar := &auditResults[i] // avoid large shallow copy in range loop
		// append statusViolations for this constraint until constraintViolationsLimit has reached
		if uint(len(statusViolations)) < *constraintViolationsLimit {
			msg := ar.message
			if len(msg) > msgSize {
				msg = truncateString(msg, msgSize)
			}
			statusViolations = append(statusViolations, StatusViolation{
				Kind:              ar.rkind,
				Name:              ar.rname,
				Namespace:         ar.rnamespace,
				Message:           msg,
				EnforcementAction: ar.enforcementAction,
			})
		}
	}
	raw, err := json.Marshal(statusViolations)
	if err != nil {
		return err
	}
	// need to convert to []interface{}
	violations := make([]interface{}, 0)
	err = json.Unmarshal(raw, &violations)
	if err != nil {
		return err
	}
	// update constraint status auditTimestamp
	if err = unstructured.SetNestedField(instance.Object, timestamp, "status", "auditTimestamp"); err != nil {
		return err
	}
	// update constraint status totalViolations
	if err = unstructured.SetNestedField(instance.Object, totalViolations, "status", "totalViolations"); err != nil {
		return err
	}
	// update constraint status violations
	if len(violations) == 0 {
		_, found, err := unstructured.NestedSlice(instance.Object, "status", "violations")
		if err != nil {
			return err
		}
		if found {
			unstructured.RemoveNestedField(instance.Object, "status", "violations")
			ucloop.log.Info("removed status violations", "constraintName", constraintName)
		}
		err = ucloop.client.Status().Update(ctx, instance)
		if err != nil {
			return err
		}
	} else {
		if err := unstructured.SetNestedSlice(instance.Object, violations, "status", "violations"); err != nil {
			return err
		}
		ucloop.log.Info("constraint status update", "object", instance)
		err = ucloop.client.Status().Update(ctx, instance)
		if err != nil {
			return err
		}
		ucloop.log.Info("updated constraint status violations", "constraintName", constraintName, "count", len(violations))
	}
	return nil
}

func truncateString(str string, size int) string {
	shortenStr := str
	if len(str) > size {
		if size > 3 {
			size -= 3
		}
		shortenStr = str[0:size] + "..."
	}
	return shortenStr
}

type updateConstraintLoop struct {
	uc      map[util.KindVersionResource]unstructured.Unstructured
	client  client.Client
	stop    chan struct{}
	stopped chan struct{}
	ul      map[util.KindVersionResource][]auditResult
	ts      string
	tv      map[util.KindVersionResource]int64
	log     logr.Logger
}

func (ucloop *updateConstraintLoop) update(ctx context.Context, constraintsGVKs []schema.GroupVersionKind) {
	defer close(ucloop.stopped)

	ucloop.uc = make(map[util.KindVersionResource]unstructured.Unstructured)

	// get constraints for each Kind
	for _, constraintGvk := range constraintsGVKs {
		select {
		case <-ucloop.stop:
			return
		default:
		}

		ucloop.log.Info("constraint", "resource kind", constraintGvk.Kind)
		instanceList := &unstructured.UnstructuredList{}
		instanceList.SetGroupVersionKind(constraintGvk)
		err := ucloop.client.List(ctx, instanceList)
		if err != nil {
			ucloop.log.Error(err, "error while listing constraints", "kind", constraintGvk.Kind)
			continue
		}
		ucloop.log.Info("constraint", "count of constraints", len(instanceList.Items))

		// get each constraint
		for _, item := range instanceList.Items {
			key := util.GetUniqueKey(item)
			ucloop.uc[key] = item
		}
	}

	if len(ucloop.uc) == 0 {
		return
	}

	ucloop.log.Info("starting update constraints loop", "constraints to update", fmt.Sprintf("%v", ucloop.uc))

	updateLoop := func() (bool, error) {
		for _, item := range ucloop.uc {
			select {
			case <-ucloop.stop:
				return true, nil
			default:
				var latestItem unstructured.Unstructured
				item.DeepCopyInto(&latestItem)
				name := latestItem.GetName()
				namespace := latestItem.GetNamespace()
				namespacedName := types.NamespacedName{
					Name:      name,
					Namespace: namespace,
				}
				key := util.GetUniqueKey(item)
				// get the latest constraint
				err := ucloop.client.Get(ctx, namespacedName, &latestItem)
				if err != nil {
					if k8serrors.IsNotFound(err) {
						ucloop.log.Info("could not find constraint", "name", name, "namespace", namespace)
						delete(ucloop.uc, key)
					} else {
						ucloop.log.Error(err, "could not get latest constraint during update", "name", name, "namespace", namespace)
						continue
					}
				}
				latestItemKey := util.GetUniqueKey(latestItem)
				totalViolations := ucloop.tv[latestItemKey]
				if constraintAuditResults, ok := ucloop.ul[latestItemKey]; !ok {
					err := ucloop.updateConstraintStatus(ctx, &latestItem, emptyAuditResults, ucloop.ts, totalViolations)
					if err != nil {
						ucloop.log.Error(err, "could not update constraint status", "name", name, "namespace", namespace)
						continue
					}
				} else {
					// update the constraint
					err := ucloop.updateConstraintStatus(ctx, &latestItem, constraintAuditResults, ucloop.ts, totalViolations)
					if err != nil {
						ucloop.log.Error(err, "could not update constraint status", "name", name, "namespace", namespace)
						continue
					}
				}
				delete(ucloop.uc, key)
			}
		}
		if len(ucloop.uc) == 0 {
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
		ucloop.log.Error(err, "could not update constraint reached max retries", "remaining update constraints", fmt.Sprintf("%v", ucloop.uc))
	}
}

func logStart(l logr.Logger) {
	l.Info(
		"auditing constraints and violations",
		logging.EventType, "audit_started",
	)
}

func logFinish(l logr.Logger) {
	l.Info(
		"auditing is complete",
		logging.EventType, "audit_finished",
	)
}

func logConstraint(l logr.Logger, constraint *unstructured.Unstructured, enforcementAction string, totalViolations int64) {
	l.Info(
		"audit results for constraint",
		logging.EventType, "constraint_audited",
		logging.ConstraintGroup, constraint.GroupVersionKind().Group,
		logging.ConstraintAPIVersion, constraint.GroupVersionKind().Version,
		logging.ConstraintKind, constraint.GetKind(),
		logging.ConstraintName, constraint.GetName(),
		logging.ConstraintNamespace, constraint.GetNamespace(),
		logging.ConstraintAction, enforcementAction,
		logging.ConstraintStatus, "enforced",
		logging.ConstraintViolations, strconv.FormatInt(totalViolations, 10),
	)
}

func logViolation(l logr.Logger,
	constraint *unstructured.Unstructured,
	enforcementAction string, resourceGroupVersionKind schema.GroupVersionKind, rnamespace, rname, message string) {
	l.Info(
		message,
		logging.EventType, "violation_audited",
		logging.ConstraintGroup, constraint.GroupVersionKind().Group,
		logging.ConstraintAPIVersion, constraint.GroupVersionKind().Version,
		logging.ConstraintKind, constraint.GetKind(),
		logging.ConstraintName, constraint.GetName(),
		logging.ConstraintNamespace, constraint.GetNamespace(),
		logging.ConstraintAction, enforcementAction,
		logging.ResourceGroup, resourceGroupVersionKind.Group,
		logging.ResourceAPIVersion, resourceGroupVersionKind.Version,
		logging.ResourceKind, resourceGroupVersionKind.Kind,
		logging.ResourceNamespace, rnamespace,
		logging.ResourceName, rname,
	)
}

func emitEvent(constraint *unstructured.Unstructured,
	timestamp, enforcementAction string, resourceGroupVersionKind schema.GroupVersionKind, rnamespace, rname, message, gkNamespace string,
	eventRecorder record.EventRecorder) {
	annotations := map[string]string{
		"process":                    "audit",
		"auditTimestamp":             timestamp,
		logging.EventType:            "violation_audited",
		logging.ConstraintGroup:      constraint.GroupVersionKind().Group,
		logging.ConstraintAPIVersion: constraint.GroupVersionKind().Version,
		logging.ConstraintKind:       constraint.GetKind(),
		logging.ConstraintName:       constraint.GetName(),
		logging.ConstraintNamespace:  constraint.GetNamespace(),
		logging.ConstraintAction:     enforcementAction,
		logging.ResourceGroup:        resourceGroupVersionKind.Group,
		logging.ResourceAPIVersion:   resourceGroupVersionKind.Version,
		logging.ResourceKind:         resourceGroupVersionKind.Kind,
		logging.ResourceNamespace:    rnamespace,
		logging.ResourceName:         rname,
	}
	reason := "AuditViolation"
	ref := getViolationRef(gkNamespace, resourceGroupVersionKind.Kind, rname, rnamespace, constraint.GetKind(), constraint.GetName(), constraint.GetNamespace())

	eventRecorder.AnnotatedEventf(ref, annotations, corev1.EventTypeWarning, reason, "Timestamp: %s, Resource Namespace: %s, Constraint: %s, Message: %s", timestamp, rnamespace, constraint.GetName(), message)
}

func getViolationRef(gkNamespace, rkind, rname, rnamespace, ckind, cname, cnamespace string) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind:      rkind,
		Name:      rname,
		UID:       types.UID(rkind + "/" + rnamespace + "/" + rname + "/" + ckind + "/" + cnamespace + "/" + cname),
		Namespace: gkNamespace,
	}
}
