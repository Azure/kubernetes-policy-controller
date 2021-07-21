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

package config

import (
	"context"
	"fmt"

	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	configv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/config/v1alpha1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	syncc "github.com/open-policy-agent/gatekeeper/pkg/controller/sync"
	"github.com/open-policy-agent/gatekeeper/pkg/keys"
	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/target"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	ctrlName      = "config-controller"
	finalizerName = "finalizers.gatekeeper.sh/config"
)

var log = logf.Log.WithName("controller").WithValues("kind", "Config")

type Adder struct {
	Opa              *opa.Client
	WatchManager     *watch.Manager
	ControllerSwitch *watch.ControllerSwitch
	Tracker          *readiness.Tracker
	ProcessExcluder  *process.Excluder
}

// Add creates a new ConfigController and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func (a *Adder) Add(mgr manager.Manager) error {
	// Events will be used to receive events from dynamic watches registered
	// via the registrar below.
	events := make(chan event.GenericEvent, 1024)
	r, err := newReconciler(mgr, a.Opa, a.WatchManager, a.ControllerSwitch, a.Tracker, a.ProcessExcluder, events, events)
	if err != nil {
		return err
	}

	return add(mgr, r)
}

func (a *Adder) InjectOpa(o *opa.Client) {
	a.Opa = o
}

func (a *Adder) InjectWatchManager(wm *watch.Manager) {
	a.WatchManager = wm
}

func (a *Adder) InjectControllerSwitch(cs *watch.ControllerSwitch) {
	a.ControllerSwitch = cs
}

func (a *Adder) InjectTracker(t *readiness.Tracker) {
	a.Tracker = t
}

func (a *Adder) InjectProcessExcluder(m *process.Excluder) {
	a.ProcessExcluder = m
}

func (a *Adder) InjectMutationCache(mutationCache *mutation.System) {}

// newReconciler returns a new reconcile.Reconciler
// events is the channel from which sync controller will receive the events
// regEvents is the channel registered by Registrar to put the events in
// events and regEvents point to same event channel except for testing.
func newReconciler(mgr manager.Manager, opa syncc.OpaDataClient, wm *watch.Manager, cs *watch.ControllerSwitch, tracker *readiness.Tracker, processExcluder *process.Excluder, events <-chan event.GenericEvent, regEvents chan<- event.GenericEvent) (*ReconcileConfig, error) {
	watchSet := watch.NewSet()
	filteredOpa := syncc.NewFilteredOpaDataClient(opa, watchSet)
	syncMetricsCache := syncc.NewMetricsCache()

	syncAdder := syncc.Adder{
		Opa:             filteredOpa,
		Events:          events,
		MetricsCache:    syncMetricsCache,
		Tracker:         tracker,
		ProcessExcluder: processExcluder,
	}
	// Create subordinate controller - we will feed it events dynamically via watch
	if err := syncAdder.Add(mgr); err != nil {
		return nil, fmt.Errorf("registering sync controller: %w", err)
	}

	w, err := wm.NewRegistrar(
		ctrlName,
		regEvents)
	if err != nil {
		return nil, err
	}
	return &ReconcileConfig{
		reader:           mgr.GetCache(),
		writer:           mgr.GetClient(),
		statusClient:     mgr.GetClient(),
		scheme:           mgr.GetScheme(),
		opa:              filteredOpa,
		cs:               cs,
		watcher:          w,
		watched:          watchSet,
		syncMetricsCache: syncMetricsCache,
		tracker:          tracker,
		processExcluder:  processExcluder,
	}, nil
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New(ctrlName, mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to Config
	err = c.Watch(&source.Kind{Type: &configv1alpha1.Config{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileConfig{}

// ReconcileConfig reconciles a Config object.
type ReconcileConfig struct {
	reader       client.Reader
	writer       client.Writer
	statusClient client.StatusClient

	scheme           *runtime.Scheme
	opa              syncc.OpaDataClient
	syncMetricsCache *syncc.MetricsCache
	cs               *watch.ControllerSwitch
	watcher          *watch.Registrar
	watched          *watch.Set
	needsReplay      *watch.Set
	needsWipe        bool
	tracker          *readiness.Tracker
	processExcluder  *process.Excluder
}

// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=podsecuritypolicies,resourceNames=gatekeeper-admin,verbs=use
// +kubebuilder:rbac:groups=config.gatekeeper.sh,resources=configs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.gatekeeper.sh,resources=configs/status,verbs=get;update;patch

// Reconcile reads that state of the cluster for a Config object and makes changes based on the state read
// and what is in the Config.Spec
// Automatically generate RBAC rules to allow the Controller to read all things (for sync)
// update is needed for finalizers.
func (r *ReconcileConfig) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	// Short-circuit if shutting down.
	if r.cs != nil {
		running := r.cs.Enter()
		defer r.cs.Exit()
		if !running {
			return reconcile.Result{}, nil
		}
	}

	// Fetch the Config instance
	if request.NamespacedName != keys.Config {
		log.Info("Ignoring unsupported config name", "namespace", request.NamespacedName.Namespace, "name", request.NamespacedName.Name)
		return reconcile.Result{}, nil
	}
	exists := true
	instance := &configv1alpha1.Config{}
	err := r.reader.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		// if config is not found, we should remove cached data
		if errors.IsNotFound(err) {
			exists = false
		} else {
			// Error reading the object - requeue the request.
			return reconcile.Result{}, err
		}
	}

	// Actively remove config finalizer. This should automatically remove
	// the finalizer over time even if state teardown didn't work correctly
	// after a deprecation period, all finalizer code can be removed.
	if exists && hasFinalizer(instance) {
		removeFinalizer(instance)
		if err := r.writer.Update(ctx, instance); err != nil {
			return reconcile.Result{}, err
		}
	}

	newSyncOnly := watch.NewSet()
	newExcluder := process.New()
	var statsEnabled bool
	// If the config is being deleted the user is saying they don't want to
	// sync anything
	if exists && instance.GetDeletionTimestamp().IsZero() {
		for _, entry := range instance.Spec.Sync.SyncOnly {
			gvk := schema.GroupVersionKind{Group: entry.Group, Version: entry.Version, Kind: entry.Kind}
			newSyncOnly.Add(gvk)
		}

		newExcluder.Add(instance.Spec.Match)
		statsEnabled = instance.Spec.Readiness.StatsEnabled
	}

	// Enable verbose readiness stats if requested.
	if statsEnabled {
		log.Info("enabling readiness stats")
		r.tracker.EnableStats(ctx)
	} else {
		log.Info("disabling readiness stats")
		r.tracker.DisableStats(ctx)
	}

	// Remove expectations for resources we no longer watch.
	diff := r.watched.Difference(newSyncOnly)
	r.removeStaleExpectations(diff)

	// If the watch set has not changed, we're done here.
	if r.watched.Equals(newSyncOnly) && r.processExcluder.Equals(newExcluder) {
		// ...unless we have pending wipe / replay operations from a previous reconcile.
		if !(r.needsWipe || r.needsReplay != nil) {
			return reconcile.Result{}, nil
		}

		// If we reach here, the watch set hasn't changed since last reconcile, but we
		// have unfinished wipe/replay business from the last change.
	} else {
		// The watch set _has_ changed, so recalculate the replay set.
		r.needsReplay = nil
		r.needsWipe = true
	}

	// --- Start watching the new set ---

	// This must happen first - signals to the opa client in the sync controller
	// to drop events from no-longer-watched resources that may be in its queue.
	if r.needsReplay == nil {
		r.needsReplay = r.watched.Intersection(newSyncOnly)
	}
	r.watched.Replace(newSyncOnly)

	// swapping with the new excluder
	r.processExcluder.Replace(newExcluder)

	// *Note the following steps are not transactional with respect to admission control*

	// Wipe all data to avoid stale state if needed. Happens once per watch-set-change.
	if err := r.wipeCacheIfNeeded(ctx); err != nil {
		return reconcile.Result{}, fmt.Errorf("wiping opa data cache: %w", err)
	}

	// Important: dynamic watches update must happen *after* updating our watchSet.
	// Otherwise the sync controller will drop events for the newly watched kinds.
	// Defer error handling so object re-sync happens even if the watch is hard
	// errored due to a missing GVK in the watch set.
	if err := r.watcher.ReplaceWatch(ctx, newSyncOnly.Items()); err != nil {
		return reconcile.Result{}, err
	}

	// Replay cached data for any resources that were previously watched and still in the watch set.
	// This is necessary because we wipe their data from Opa above.
	// TODO(OREN): Improve later by selectively removing subtrees of data instead of a full wipe.
	if err := r.replayData(ctx); err != nil {
		return reconcile.Result{}, fmt.Errorf("replaying data: %w", err)
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileConfig) wipeCacheIfNeeded(ctx context.Context) error {
	if r.needsWipe {
		if _, err := r.opa.RemoveData(ctx, target.WipeData{}); err != nil {
			return err
		}

		// reset sync cache before sending the metric
		r.syncMetricsCache.ResetCache()
		r.syncMetricsCache.ReportSync(ctx, &syncc.Reporter{})

		r.needsWipe = false
	}
	return nil
}

// replayData replays all watched and cached data into Opa following a config set change.
// In the future we can rework this to avoid the full opa data cache wipe.
func (r *ReconcileConfig) replayData(ctx context.Context) error {
	if r.needsReplay == nil {
		return nil
	}
	for _, gvk := range r.needsReplay.Items() {
		u := &unstructured.UnstructuredList{}
		u.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   gvk.Group,
			Version: gvk.Version,
			Kind:    gvk.Kind + "List",
		})
		err := r.reader.List(ctx, u)
		if err != nil {
			return fmt.Errorf("replaying data for %+v: %w", gvk, err)
		}

		defer r.syncMetricsCache.ReportSync(ctx, &syncc.Reporter{})

		for i := range u.Items {
			syncKey := r.syncMetricsCache.GetSyncKey(u.Items[i].GetNamespace(), u.Items[i].GetName())

			isExcludedNamespace, err := r.skipExcludedNamespace(&u.Items[i])
			if err != nil {
				log.Error(err, "error while excluding namespaces")
			}

			if isExcludedNamespace {
				continue
			}

			if _, err := r.opa.AddData(ctx, &u.Items[i]); err != nil {
				r.syncMetricsCache.AddObject(syncKey, syncc.Tags{
					Kind:   u.Items[i].GetKind(),
					Status: metrics.ErrorStatus,
				})
				return fmt.Errorf("adding data for %+v: %w", gvk, err)
			}

			r.syncMetricsCache.AddObject(syncKey, syncc.Tags{
				Kind:   u.Items[i].GetKind(),
				Status: metrics.ActiveStatus,
			})
		}
		r.needsReplay.Remove(gvk)
	}
	r.needsReplay = nil
	return nil
}

// removeStaleExpectations stops tracking data for any resources that are no longer watched.
func (r *ReconcileConfig) removeStaleExpectations(stale *watch.Set) {
	for _, gvk := range stale.Items() {
		r.tracker.CancelData(gvk)
	}
}

func (r *ReconcileConfig) skipExcludedNamespace(obj *unstructured.Unstructured) (bool, error) {
	isNamespaceExcluded, err := r.processExcluder.IsNamespaceExcluded(process.Sync, obj)
	if err != nil {
		return false, err
	}

	return isNamespaceExcluded, err
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

func hasFinalizer(instance *configv1alpha1.Config) bool {
	return containsString(finalizerName, instance.GetFinalizers())
}

func removeFinalizer(instance *configv1alpha1.Config) {
	instance.SetFinalizers(removeString(finalizerName, instance.GetFinalizers()))
}
