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

package sync

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller").WithValues("metaKind", "Sync")

type Adder struct {
	Opa             OpaDataClient
	Events          <-chan event.GenericEvent
	MetricsCache    *MetricsCache
	Tracker         *readiness.Tracker
	ProcessExcluder *process.Excluder
}

// Add creates a new Sync Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func (a *Adder) Add(mgr manager.Manager) error {
	reporter, err := NewStatsReporter()
	if err != nil {
		log.Error(err, "Sync metrics reporter could not start")
		return err
	}

	r, err := newReconciler(mgr, a.Opa, *reporter, a.MetricsCache, a.Tracker, a.ProcessExcluder)
	if err != nil {
		return err
	}
	return add(mgr, r, a.Events)
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(
	mgr manager.Manager,
	opa OpaDataClient,
	reporter Reporter,
	metricsCache *MetricsCache,
	tracker *readiness.Tracker,
	processExcluder *process.Excluder) (reconcile.Reconciler, error) {
	return &ReconcileSync{
		reader:          mgr.GetCache(),
		scheme:          mgr.GetScheme(),
		opa:             opa,
		log:             log,
		reporter:        reporter,
		metricsCache:    metricsCache,
		tracker:         tracker,
		processExcluder: processExcluder,
	}, nil
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler, events <-chan event.GenericEvent) error {
	// Create a new controller
	c, err := controller.New("sync-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to the provided resource
	return c.Watch(
		&source.Channel{
			Source:         events,
			DestBufferSize: 1024,
		},
		handler.EnqueueRequestsFromMapFunc(util.EventPackerMapFunc()),
	)
}

var _ reconcile.Reconciler = &ReconcileSync{}

type MetricsCache struct {
	mux        sync.RWMutex
	Cache      map[string]Tags
	KnownKinds map[string]bool
}

type Tags struct {
	Kind   string
	Status metrics.Status
}

// ReconcileSync reconciles an arbitrary object described by Kind.
type ReconcileSync struct {
	reader client.Reader

	scheme          *runtime.Scheme
	opa             OpaDataClient
	log             logr.Logger
	reporter        Reporter
	metricsCache    *MetricsCache
	tracker         *readiness.Tracker
	processExcluder *process.Excluder
}

// +kubebuilder:rbac:groups=constraints.gatekeeper.sh,resources=*,verbs=get;list;watch;create;update;patch;delete

// Reconcile reads that state of the cluster for an object and makes changes based on the state read
// and what is in the constraint.Spec.
func (r *ReconcileSync) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	timeStart := time.Now()

	gvk, unpackedRequest, err := util.UnpackRequest(request)
	if err != nil {
		// Unrecoverable, do not retry.
		// TODO(OREN) add metric
		log.Error(err, "unpacking request", "request", request)
		return reconcile.Result{}, nil
	}

	syncKey := r.metricsCache.GetSyncKey(unpackedRequest.Namespace, unpackedRequest.Name)
	reportMetrics := false
	defer func() {
		if reportMetrics {
			if err := r.reporter.reportSyncDuration(ctx, time.Since(timeStart)); err != nil {
				log.Error(err, "failed to report sync duration")
			}

			r.metricsCache.ReportSync(ctx, &r.reporter)

			if err := r.reporter.reportLastSync(ctx); err != nil {
				log.Error(err, "failed to report last sync timestamp")
			}
		}
	}()

	instance := &unstructured.Unstructured{}
	instance.SetGroupVersionKind(gvk)

	if err := r.reader.Get(ctx, unpackedRequest.NamespacedName, instance); err != nil {
		if errors.IsNotFound(err) {
			// This is a deletion; remove the data
			instance.SetNamespace(unpackedRequest.Namespace)
			instance.SetName(unpackedRequest.Name)
			if _, err := r.opa.RemoveData(ctx, instance); err != nil {
				return reconcile.Result{}, err
			}

			// cancel expectations
			t := r.tracker.ForData(instance.GroupVersionKind())
			t.CancelExpect(instance)

			r.metricsCache.DeleteObject(syncKey)
			reportMetrics = true
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// namespace is excluded from sync
	isExcludedNamespace, err := r.skipExcludedNamespace(instance)
	if err != nil {
		log.Error(err, "error while excluding namespaces")
	}

	if isExcludedNamespace {
		// cancel expectations
		t := r.tracker.ForData(instance.GroupVersionKind())
		t.CancelExpect(instance)
		return reconcile.Result{}, nil
	}

	if !instance.GetDeletionTimestamp().IsZero() {
		if _, err := r.opa.RemoveData(ctx, instance); err != nil {
			return reconcile.Result{}, err
		}

		// cancel expectations
		t := r.tracker.ForData(instance.GroupVersionKind())
		t.CancelExpect(instance)

		r.metricsCache.DeleteObject(syncKey)
		reportMetrics = true
		return reconcile.Result{}, nil
	}

	r.log.V(logging.DebugLevel).Info(
		"data will be added",
		logging.ResourceAPIVersion, instance.GetAPIVersion(),
		logging.ResourceKind, instance.GetKind(),
		logging.ResourceNamespace, instance.GetNamespace(),
		logging.ResourceName, instance.GetName(),
	)

	if _, err := r.opa.AddData(ctx, instance); err != nil {
		r.metricsCache.AddObject(syncKey, Tags{
			Kind:   instance.GetKind(),
			Status: metrics.ErrorStatus,
		})
		reportMetrics = true

		return reconcile.Result{}, err
	}
	r.tracker.ForData(gvk).Observe(instance)
	log.V(1).Info("[readiness] observed data", "gvk", gvk, "namespace", instance.GetNamespace(), "name", instance.GetName())

	r.metricsCache.AddObject(syncKey, Tags{
		Kind:   instance.GetKind(),
		Status: metrics.ActiveStatus,
	})

	r.metricsCache.addKind(instance.GetKind())

	reportMetrics = true

	return reconcile.Result{}, nil
}

func (r *ReconcileSync) skipExcludedNamespace(obj *unstructured.Unstructured) (bool, error) {
	isNamespaceExcluded, err := r.processExcluder.IsNamespaceExcluded(process.Sync, obj)
	if err != nil {
		return false, err
	}

	return isNamespaceExcluded, err
}

func NewMetricsCache() *MetricsCache {
	return &MetricsCache{
		Cache:      make(map[string]Tags),
		KnownKinds: make(map[string]bool),
	}
}

func (c *MetricsCache) GetSyncKey(namespace string, name string) string {
	return strings.Join([]string{namespace, name}, "/")
}

// need to know encountered kinds to reset metrics for that kind
// this is a known memory leak
// footprint should naturally reset on Pod upgrade b/c the container restarts.
func (c *MetricsCache) addKind(key string) {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.KnownKinds[key] = true
}

func (c *MetricsCache) ResetCache() {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.Cache = make(map[string]Tags)
}

func (c *MetricsCache) AddObject(key string, t Tags) {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.Cache[key] = Tags{
		Kind:   t.Kind,
		Status: t.Status,
	}
}

func (c *MetricsCache) DeleteObject(key string) {
	c.mux.Lock()
	defer c.mux.Unlock()

	delete(c.Cache, key)
}

func (c *MetricsCache) ReportSync(ctx context.Context, reporter *Reporter) {
	c.mux.RLock()
	defer c.mux.RUnlock()

	totals := make(map[Tags]int)
	for _, v := range c.Cache {
		totals[v]++
	}

	for kind := range c.KnownKinds {
		for _, status := range metrics.AllStatuses {
			if err := reporter.reportSync(ctx,
				Tags{
					Kind:   kind,
					Status: status,
				},
				int64(totals[Tags{
					Kind:   kind,
					Status: status,
				}])); err != nil {
				log.Error(err, "failed to report sync")
			}
		}
	}
}
