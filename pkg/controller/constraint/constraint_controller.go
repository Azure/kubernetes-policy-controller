/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package constraint

import (
	"context"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/frameworks/constraint/pkg/core/constraints"
	constraintstatusv1beta1 "github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/constraintstatus"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller").WithValues(logging.Process, "constraint_controller")

const (
	finalizerName = "finalizers.gatekeeper.sh/constraint"
)

type Adder struct {
	Opa              *opa.Client
	ConstraintsCache *ConstraintsCache
	WatchManager     *watch.Manager
	ControllerSwitch *watch.ControllerSwitch
	Events           <-chan event.GenericEvent
	Tracker          *readiness.Tracker
	GetPod           func(context.Context) (*corev1.Pod, error)
	ProcessExcluder  *process.Excluder
}

func (a *Adder) InjectOpa(o *opa.Client) {
	a.Opa = o
}

func (a *Adder) InjectWatchManager(w *watch.Manager) {
	a.WatchManager = w
}

func (a *Adder) InjectControllerSwitch(cs *watch.ControllerSwitch) {
	a.ControllerSwitch = cs
}

func (a *Adder) InjectTracker(t *readiness.Tracker) {
	a.Tracker = t
}

func (a *Adder) InjectMutationCache(mutationCache *mutation.System) {}

// Add creates a new Constraint Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func (a *Adder) Add(mgr manager.Manager) error {
	reporter, err := newStatsReporter()
	if err != nil {
		log.Error(err, "StatsReporter could not start")
		return err
	}

	r := newReconciler(mgr, a.Opa, a.ControllerSwitch, reporter, a.ConstraintsCache, a.Tracker)
	if a.GetPod != nil {
		r.getPod = a.GetPod
	}
	return add(mgr, r, a.Events)
}

type ConstraintsCache struct {
	mux   sync.RWMutex
	cache map[string]tags
}

type tags struct {
	enforcementAction util.EnforcementAction
	status            metrics.Status
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(
	mgr manager.Manager,
	opa *opa.Client,
	cs *watch.ControllerSwitch,
	reporter StatsReporter,
	constraintsCache *ConstraintsCache,
	tracker *readiness.Tracker) *ReconcileConstraint {
	r := &ReconcileConstraint{
		// Separate reader and writer because manager's default client bypasses the cache for unstructured resources.
		writer:       mgr.GetClient(),
		statusClient: mgr.GetClient(),
		reader:       mgr.GetCache(),

		cs:               cs,
		scheme:           mgr.GetScheme(),
		opa:              opa,
		log:              log,
		reporter:         reporter,
		constraintsCache: constraintsCache,
		tracker:          tracker,
	}
	r.getPod = r.defaultGetPod
	return r
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler, events <-chan event.GenericEvent) error {
	// Create a new controller
	c, err := controller.New("constraint-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to the provided constraint
	err = c.Watch(
		&source.Channel{
			Source:         events,
			DestBufferSize: 1024,
		},
		handler.EnqueueRequestsFromMapFunc(util.EventPackerMapFunc()),
	)
	if err != nil {
		return err
	}

	// Watch for changes to ConstraintStatus
	err = c.Watch(
		&source.Kind{Type: &constraintstatusv1beta1.ConstraintPodStatus{}},
		handler.EnqueueRequestsFromMapFunc(constraintstatus.PodStatusToConstraintMapper(true, util.EventPackerMapFunc())),
	)
	if err != nil {
		return err
	}
	return nil
}

var _ reconcile.Reconciler = &ReconcileConstraint{}

// ReconcileConstraint reconciles an arbitrary constraint object described by Kind.
type ReconcileConstraint struct {
	reader       client.Reader
	writer       client.Writer
	statusClient client.StatusClient

	cs               *watch.ControllerSwitch
	scheme           *runtime.Scheme
	opa              *opa.Client
	log              logr.Logger
	reporter         StatsReporter
	constraintsCache *ConstraintsCache
	tracker          *readiness.Tracker
	getPod           func(context.Context) (*corev1.Pod, error)
}

// +kubebuilder:rbac:groups=constraints.gatekeeper.sh,resources=*,verbs=get;list;watch;create;update;patch;delete

// Reconcile reads that state of the cluster for a constraint object and makes changes based on the state read
// and what is in the constraint.Spec.
func (r *ReconcileConstraint) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	// Short-circuit if shutting down.
	if r.cs != nil {
		running := r.cs.Enter()
		defer r.cs.Exit()
		if !running {
			return reconcile.Result{}, nil
		}
	}

	gvk, unpackedRequest, err := util.UnpackRequest(request)
	if err != nil {
		// Unrecoverable, do not retry.
		// TODO(OREN) add metric
		log.Error(err, "unpacking request", "request", request)
		return reconcile.Result{}, nil
	}

	// Sanity - make sure it is a constraint resource.
	if gvk.Group != constraintstatusv1beta1.ConstraintsGroup {
		// Unrecoverable, do not retry.
		log.Error(err, "invalid constraint GroupVersion", "gvk", gvk)
		return reconcile.Result{}, nil
	}

	deleted := false
	instance := &unstructured.Unstructured{}
	instance.SetGroupVersionKind(gvk)
	if err := r.reader.Get(ctx, unpackedRequest.NamespacedName, instance); err != nil {
		if !errors.IsNotFound(err) && !meta.IsNoMatchError(err) {
			return reconcile.Result{}, err
		}
		deleted = true
		instance = &unstructured.Unstructured{}
		instance.SetGroupVersionKind(gvk)
		instance.SetNamespace(unpackedRequest.NamespacedName.Namespace)
		instance.SetName(unpackedRequest.NamespacedName.Name)
	}

	deleted = deleted || !instance.GetDeletionTimestamp().IsZero()

	constraintKey := strings.Join([]string{instance.GetKind(), instance.GetName()}, "/")
	enforcementAction, err := util.GetEnforcementAction(instance.Object)
	if err != nil {
		return reconcile.Result{}, err
	}

	reportMetrics := false
	defer func() {
		if reportMetrics {
			r.constraintsCache.reportTotalConstraints(ctx, r.reporter)
		}
	}()

	if HasFinalizer(instance) {
		RemoveFinalizer(instance)
		if err := r.writer.Update(ctx, instance); err != nil {
			return reconcile.Result{Requeue: true}, nil
		}
	}

	if !deleted {
		r.log.Info("handling constraint update", "instance", instance)
		status, err := r.getOrCreatePodStatus(ctx, instance)
		if err != nil {
			log.Info("could not get/create pod status object", "error", err)
			return reconcile.Result{}, err
		}
		status.Status.ConstraintUID = instance.GetUID()
		status.Status.ObservedGeneration = instance.GetGeneration()
		status.Status.Errors = nil
		if c, err := r.opa.GetConstraint(ctx, instance); err != nil || !constraints.SemanticEqual(instance, c) {
			if err := r.cacheConstraint(ctx, instance); err != nil {
				r.constraintsCache.addConstraintKey(constraintKey, tags{
					enforcementAction: enforcementAction,
					status:            metrics.ErrorStatus,
				})
				status.Status.Errors = append(status.Status.Errors, constraintstatusv1beta1.Error{Message: err.Error()})
				if err2 := r.writer.Update(ctx, status); err2 != nil {
					log.Error(err2, "could not report constraint error status")
				}
				reportMetrics = true
				return reconcile.Result{}, err
			}
			logAddition(r.log, instance, enforcementAction)
		}

		status.Status.Enforced = true
		if err = r.writer.Update(ctx, status); err != nil {
			return reconcile.Result{Requeue: true}, nil
		}

		// adding constraint to cache and sending metrics
		r.constraintsCache.addConstraintKey(constraintKey, tags{
			enforcementAction: enforcementAction,
			status:            metrics.ActiveStatus,
		})
		reportMetrics = true
	} else {
		r.log.Info("handling constraint delete", "instance", instance)
		if _, err := r.opa.RemoveConstraint(ctx, instance); err != nil {
			if _, ok := err.(*opa.UnrecognizedConstraintError); !ok {
				return reconcile.Result{}, err
			}
		}
		logRemoval(r.log, instance, enforcementAction)

		// cancel expectations
		t := r.tracker.For(instance.GroupVersionKind())
		t.CancelExpect(instance)

		r.constraintsCache.deleteConstraintKey(constraintKey)
		reportMetrics = true

		sName, err := constraintstatusv1beta1.KeyForConstraint(util.GetPodName(), instance)
		if err != nil {
			return reconcile.Result{}, err
		}
		statusObj := &constraintstatusv1beta1.ConstraintPodStatus{}
		statusObj.SetName(sName)
		statusObj.SetNamespace(util.GetNamespace())
		if err := r.writer.Delete(ctx, statusObj); err != nil {
			if !errors.IsNotFound(err) {
				return reconcile.Result{}, err
			}
		}
	}
	return reconcile.Result{}, nil
}

func (r *ReconcileConstraint) defaultGetPod(_ context.Context) (*corev1.Pod, error) {
	// require injection of GetPod in order to control what client we use to
	// guarantee we don't inadvertently create a watch
	panic("GetPod must be injected")
}

func (r *ReconcileConstraint) getOrCreatePodStatus(ctx context.Context, constraint *unstructured.Unstructured) (*constraintstatusv1beta1.ConstraintPodStatus, error) {
	statusObj := &constraintstatusv1beta1.ConstraintPodStatus{}
	sName, err := constraintstatusv1beta1.KeyForConstraint(util.GetPodName(), constraint)
	if err != nil {
		return nil, err
	}
	key := types.NamespacedName{Name: sName, Namespace: util.GetNamespace()}
	if err := r.reader.Get(ctx, key, statusObj); err != nil {
		if !errors.IsNotFound(err) {
			return nil, err
		}
	} else {
		return statusObj, nil
	}
	pod, err := r.getPod(ctx)
	if err != nil {
		return nil, err
	}
	statusObj, err = constraintstatusv1beta1.NewConstraintStatusForPod(pod, constraint, r.scheme)
	if err != nil {
		return nil, err
	}
	if err := r.writer.Create(ctx, statusObj); err != nil {
		return nil, err
	}
	return statusObj, nil
}

func logAddition(l logr.Logger, constraint *unstructured.Unstructured, enforcementAction util.EnforcementAction) {
	l.Info(
		"constraint added to OPA",
		logging.EventType, "constraint_added",
		logging.ConstraintGroup, constraint.GroupVersionKind().Group,
		logging.ConstraintAPIVersion, constraint.GroupVersionKind().Version,
		logging.ConstraintKind, constraint.GetKind(),
		logging.ConstraintName, constraint.GetName(),
		logging.ConstraintAction, string(enforcementAction),
		logging.ConstraintStatus, "enforced",
	)
}

func logRemoval(l logr.Logger, constraint *unstructured.Unstructured, enforcementAction util.EnforcementAction) {
	l.Info(
		"constraint removed from OPA",
		logging.EventType, "constraint_removed",
		logging.ConstraintGroup, constraint.GroupVersionKind().Group,
		logging.ConstraintAPIVersion, constraint.GroupVersionKind().Version,
		logging.ConstraintKind, constraint.GetKind(),
		logging.ConstraintName, constraint.GetName(),
		logging.ConstraintAction, string(enforcementAction),
		logging.ConstraintStatus, "unenforced",
	)
}

func (r *ReconcileConstraint) cacheConstraint(ctx context.Context, instance *unstructured.Unstructured) error {
	t := r.tracker.For(instance.GroupVersionKind())

	obj := instance.DeepCopy()
	// Remove the status field since we do not need it for OPA
	unstructured.RemoveNestedField(obj.Object, "status")
	_, err := r.opa.AddConstraint(ctx, obj)
	if err != nil {
		t.TryCancelExpect(obj)
		return err
	}

	// Track for readiness
	t.Observe(instance)
	log.Info("[readiness] observed Constraint", "name", instance.GetName())

	return nil
}

func RemoveFinalizer(instance *unstructured.Unstructured) {
	instance.SetFinalizers(removeString(finalizerName, instance.GetFinalizers()))
}

func HasFinalizer(instance *unstructured.Unstructured) bool {
	return containsString(finalizerName, instance.GetFinalizers())
}

func containsString(s string, items []string) bool {
	for _, item := range items {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(s string, items []string) []string {
	var rval []string
	for _, item := range items {
		if item != s {
			rval = append(rval, item)
		}
	}
	return rval
}

func NewConstraintsCache() *ConstraintsCache {
	return &ConstraintsCache{
		cache: make(map[string]tags),
	}
}

func (c *ConstraintsCache) addConstraintKey(constraintKey string, t tags) {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.cache[constraintKey] = tags{
		enforcementAction: t.enforcementAction,
		status:            t.status,
	}
}

func (c *ConstraintsCache) deleteConstraintKey(constraintKey string) {
	c.mux.Lock()
	defer c.mux.Unlock()

	delete(c.cache, constraintKey)
}

func (c *ConstraintsCache) reportTotalConstraints(ctx context.Context, reporter StatsReporter) {
	c.mux.RLock()
	defer c.mux.RUnlock()

	totals := make(map[tags]int)
	// report total number of constraints
	for _, v := range c.cache {
		totals[v]++
	}

	for _, enforcementAction := range util.KnownEnforcementActions {
		for _, status := range metrics.AllStatuses {
			if err := reporter.reportConstraints(ctx,
				tags{
					enforcementAction: enforcementAction,
					status:            status,
				},
				int64(totals[tags{
					enforcementAction: enforcementAction,
					status:            status,
				}])); err != nil {
				log.Error(err, "failed to report total constraints")
			}
		}
	}
}
