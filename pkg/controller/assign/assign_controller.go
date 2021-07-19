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

package assign

import (
	"context"
	"fmt"
	"time"

	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	mutationsv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/mutations/v1alpha1"
	statusv1beta1 "github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/mutatorstatus"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/mutators"
	mutationReporter "github.com/open-policy-agent/gatekeeper/pkg/mutation/reporter"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiTypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller").WithValues(logging.Process, "assign_controller")

var gvkAssign = schema.GroupVersionKind{
	Group:   mutationsv1alpha1.GroupVersion.Group,
	Version: mutationsv1alpha1.GroupVersion.Version,
	Kind:    "Assign",
}

type Adder struct {
	MutationSystem *mutation.System
	Tracker        *readiness.Tracker
	GetPod         func() (*corev1.Pod, error)
}

// Add creates a new Assign Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func (a *Adder) Add(mgr manager.Manager) error {
	r := newReconciler(mgr, a.MutationSystem, a.Tracker, a.GetPod)
	return add(mgr, r)
}

func (a *Adder) InjectOpa(o *opa.Client) {}

func (a *Adder) InjectWatchManager(w *watch.Manager) {}

func (a *Adder) InjectControllerSwitch(cs *watch.ControllerSwitch) {}

func (a *Adder) InjectTracker(t *readiness.Tracker) {
	a.Tracker = t
}

func (a *Adder) InjectGetPod(getPod func() (*corev1.Pod, error)) {
	a.GetPod = getPod
}

func (a *Adder) InjectMutationCache(mutationCache *mutation.System) {
	a.MutationSystem = mutationCache
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, mutationCache *mutation.System, tracker *readiness.Tracker, getPod func() (*corev1.Pod, error)) *Reconciler {
	rep, err := mutationReporter.NewStatsReporter()
	if err != nil {
		log.Error(err, "Failed to create mutation stats reporter")
	}

	r := &Reconciler{
		system:   mutationCache,
		Client:   mgr.GetClient(),
		tracker:  tracker,
		getPod:   getPod,
		scheme:   mgr.GetScheme(),
		reporter: rep,
		cache:    mutation.NewMutationCache(),
	}
	if getPod == nil {
		r.getPod = r.defaultGetPod
	}
	return r
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	if !*mutation.MutationEnabled {
		return nil
	}

	// Create a new controller
	c, err := controller.New("assign-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to Assign
	if err = c.Watch(
		&source.Kind{Type: &mutationsv1alpha1.Assign{}},
		&handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	// Watch for changes to MutatorPodStatus
	err = c.Watch(
		&source.Kind{Type: &statusv1beta1.MutatorPodStatus{}},
		handler.EnqueueRequestsFromMapFunc(mutatorstatus.PodStatusToMutatorMapper(true, "Assign", util.EventPackerMapFunc())),
	)
	if err != nil {
		return err
	}
	return nil
}

// Reconciler reconciles a Assign object.
type Reconciler struct {
	client.Client
	system   *mutation.System
	tracker  *readiness.Tracker
	getPod   func() (*corev1.Pod, error)
	scheme   *runtime.Scheme
	reporter *mutationReporter.Reporter
	cache    *mutation.Cache
}

// +kubebuilder:rbac:groups=mutations.gatekeeper.sh,resources=*,verbs=get;list;watch;create;update;patch;delete

// Reconcile reads that state of the cluster for a Assign object and makes changes based on the state read
// and what is in the Assign.Spec.
// TODO (https://github.com/open-policy-agent/gatekeeper/issues/1449): DRY this and assignmetadata_controller.go .
func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Info("Reconcile", "request", request)
	timeStart := time.Now()

	deleted := false
	assign := &mutationsv1alpha1.Assign{}
	err := r.Get(ctx, request.NamespacedName, assign)
	if err != nil {
		if !errors.IsNotFound(err) {
			return reconcile.Result{}, err
		}

		deleted = true
		assign = &mutationsv1alpha1.Assign{
			ObjectMeta: metav1.ObjectMeta{
				Name:      request.NamespacedName.Name,
				Namespace: request.NamespacedName.Namespace,
			},
			TypeMeta: metav1.TypeMeta{
				Kind:       "Assign",
				APIVersion: fmt.Sprintf("%s/%s", mutationsv1alpha1.GroupVersion.Group, mutationsv1alpha1.GroupVersion.Version),
			},
		}
	}
	deleted = deleted || !assign.GetDeletionTimestamp().IsZero()
	tracker := r.tracker.For(gvkAssign)

	mID := types.MakeID(assign)

	if deleted {
		tracker.CancelExpect(assign)
		r.cache.Remove(mID)

		if err := r.system.Remove(mID); err != nil {
			log.Error(err, "Remove failed", "resource", request.NamespacedName)
			return reconcile.Result{}, err
		}

		sName, err := statusv1beta1.KeyForMutatorID(util.GetPodName(), mID)
		if err != nil {
			return reconcile.Result{}, err
		}
		status := &statusv1beta1.MutatorPodStatus{}
		status.SetName(sName)
		status.SetNamespace(util.GetNamespace())
		if err := r.Delete(ctx, status); err != nil {
			if !errors.IsNotFound(err) {
				return reconcile.Result{}, err
			}
		}

		return reconcile.Result{}, nil
	}

	// Since we aren't deleting the mutator, we are either adding or updating
	ingestionStatus := mutationReporter.MutatorStatusError
	defer func() {
		// Deferring the Upsert call gives us the correct ingestionStatus
		r.cache.Upsert(mID, ingestionStatus)

		if r.reporter == nil {
			return
		}

		if err := r.reporter.ReportMutatorIngestionRequest(ingestionStatus, time.Since(timeStart)); err != nil {
			log.Error(err, "failed to report mutator ingestion request")
		}

		for status, count := range r.cache.Tally() {
			if err = r.reporter.ReportMutatorsStatus(status, count); err != nil {
				log.Error(err, "failed to report mutator status request")
			}
		}
	}()

	status, err := r.getOrCreatePodStatus(mID)
	if err != nil {
		log.Info("could not get/create pod status object", "error", err)
		return reconcile.Result{}, err
	}
	status.Status.MutatorUID = assign.GetUID()
	status.Status.ObservedGeneration = assign.GetGeneration()
	status.Status.Errors = nil

	mutator, err := mutators.MutatorForAssign(assign)
	if err != nil {
		log.Error(err, "Creating mutator for resource failed", "resource", request.NamespacedName)
		tracker.TryCancelExpect(assign)
		status.Status.Errors = append(status.Status.Errors, statusv1beta1.MutatorError{Message: err.Error()})
		if err2 := r.Update(ctx, status); err != nil {
			log.Error(err2, "could not update mutator status")
		}
		return reconcile.Result{}, err
	}

	if err := r.system.Upsert(mutator); err != nil {
		log.Error(err, "Insert failed", "resource", request.NamespacedName)
		tracker.TryCancelExpect(assign)
		status.Status.Errors = append(status.Status.Errors, statusv1beta1.MutatorError{Message: err.Error()})
		if err2 := r.Update(ctx, status); err != nil {
			log.Error(err2, "could not update mutator status")
		}
		return reconcile.Result{}, err
	}

	tracker.Observe(assign)
	status.Status.Enforced = true

	if err := r.Update(ctx, status); err != nil {
		log.Error(err, "could not update mutator status")
		return reconcile.Result{}, err
	}

	ingestionStatus = mutationReporter.MutatorStatusActive
	return reconcile.Result{}, nil
}

func (r *Reconciler) getOrCreatePodStatus(mutatorID types.ID) (*statusv1beta1.MutatorPodStatus, error) {
	statusObj := &statusv1beta1.MutatorPodStatus{}
	sName, err := statusv1beta1.KeyForMutatorID(util.GetPodName(), mutatorID)
	if err != nil {
		return nil, err
	}
	key := apiTypes.NamespacedName{Name: sName, Namespace: util.GetNamespace()}
	if err := r.Get(context.TODO(), key, statusObj); err != nil {
		if !errors.IsNotFound(err) {
			return nil, err
		}
	} else {
		return statusObj, nil
	}
	pod, err := r.getPod()
	if err != nil {
		return nil, err
	}
	statusObj, err = statusv1beta1.NewMutatorStatusForPod(pod, mutatorID, r.scheme)
	if err != nil {
		return nil, err
	}
	if err := r.Create(context.TODO(), statusObj); err != nil {
		return nil, err
	}
	return statusObj, nil
}

func (r *Reconciler) defaultGetPod() (*corev1.Pod, error) {
	// require injection of GetPod in order to control what client we use to
	// guarantee we don't inadvertently create a watch
	panic("GetPod must be injected")
}
