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

package constrainttemplatestatus

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/go-logr/logr"
	constrainttemplatev1beta1 "github.com/open-policy-agent/frameworks/constraint/pkg/apis/templates/v1beta1"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller").WithValues(logging.Process, "constraint_template_status_controller")

type Adder struct {
	Opa              *opa.Client
	WatchManager     *watch.Manager
	ControllerSwitch *watch.ControllerSwitch
}

// Add creates a new Constraint Status Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func (a *Adder) Add(mgr manager.Manager) error {
	r := newReconciler(mgr, a.ControllerSwitch)
	return add(mgr, r)
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(
	mgr manager.Manager,
	cs *watch.ControllerSwitch) reconcile.Reconciler {
	return &ReconcileConstraintStatus{
		// Separate reader and writer because manager's default client bypasses the cache for unstructured resources.
		writer:       mgr.GetClient(),
		statusClient: mgr.GetClient(),
		reader:       mgr.GetCache(),

		cs:     cs,
		scheme: mgr.GetScheme(),
		log:    log,
	}
}

// PodStatusToConstraintTemplateMapper correlates a ConstraintTemplatePodStatus with its corresponding constraint template
// `selfOnly` tells the mapper to only map statuses corresponding to the current pod.
func PodStatusToConstraintTemplateMapper(selfOnly bool) handler.MapFunc {
	return func(obj client.Object) []reconcile.Request {
		labels := obj.GetLabels()
		name, ok := labels[v1beta1.ConstraintTemplateNameLabel]
		if !ok {
			log.Error(fmt.Errorf("constraint template status resource with no mapping label: %s", obj.GetName()), "missing label while attempting to map a constraint template status resource")
			return nil
		}
		if selfOnly {
			pod, ok := labels[v1beta1.PodLabel]
			if !ok {
				log.Error(fmt.Errorf("constraint template status resource with no pod label: %s", obj.GetName()), "missing label while attempting to map a constraint template status resource")
			}
			// Do not attempt to reconcile the resource when other pods have changed their status
			if pod != util.GetPodName() {
				return nil
			}
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name}}}
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("constraint-template-status-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to ConstraintTemplateStatus
	err = c.Watch(
		&source.Kind{Type: &v1beta1.ConstraintTemplatePodStatus{}},
		handler.EnqueueRequestsFromMapFunc(PodStatusToConstraintTemplateMapper(false)),
	)
	if err != nil {
		return err
	}

	// Watch for changes to the provided constraint
	// Watch for changes to ConstraintTemplate
	err = c.Watch(&source.Kind{Type: &constrainttemplatev1beta1.ConstraintTemplate{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}
	return nil
}

var _ reconcile.Reconciler = &ReconcileConstraintStatus{}

// ReconcileConstraintStatus reconciles an arbitrary constraint object described by Kind.
type ReconcileConstraintStatus struct {
	reader       client.Reader
	writer       client.Writer
	statusClient client.StatusClient

	cs     *watch.ControllerSwitch
	scheme *runtime.Scheme
	log    logr.Logger
}

// +kubebuilder:rbac:groups=constraints.gatekeeper.sh,resources=*,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=status.gatekeeper.sh,resources=*,verbs=get;list;watch;create;update;patch;delete

// Reconcile reads that state of the cluster for a constraint object and makes changes based on the state read
// and what is in the constraint.Spec.
func (r *ReconcileConstraintStatus) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	// Short-circuit if shutting down.
	if r.cs != nil {
		running := r.cs.Enter()
		defer r.cs.Exit()
		if !running {
			return reconcile.Result{}, nil
		}
	}
	template := &unstructured.Unstructured{}
	gv := constrainttemplatev1beta1.SchemeGroupVersion
	template.SetGroupVersionKind(gv.WithKind("ConstraintTemplate"))
	if err := r.reader.Get(ctx, request.NamespacedName, template); err != nil {
		// If the template does not exist, we are done
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	r.log.Info("handling constraint template status update", "instance", template)

	sObjs := &v1beta1.ConstraintTemplatePodStatusList{}
	if err := r.reader.List(
		ctx,
		sObjs,
		client.MatchingLabels{v1beta1.ConstraintTemplateNameLabel: request.Name},
		client.InNamespace(util.GetNamespace()),
	); err != nil {
		return reconcile.Result{}, err
	}
	statusObjs := make(sortableStatuses, len(sObjs.Items))
	copy(statusObjs, sObjs.Items)
	sort.Sort(statusObjs)

	var s []interface{}
	for i := range statusObjs {
		// Don't report status if it's not for the correct object. This can happen
		// if a watch gets interrupted, causing the constraint status to be deleted
		// out from underneath it
		if statusObjs[i].Status.TemplateUID != template.GetUID() {
			continue
		}
		j, err := json.Marshal(statusObjs[i].Status)
		if err != nil {
			return reconcile.Result{}, err
		}
		var o map[string]interface{}
		if err := json.Unmarshal(j, &o); err != nil {
			return reconcile.Result{}, err
		}
		s = append(s, o)
	}
	if err := unstructured.SetNestedSlice(template.Object, s, "status", "byPod"); err != nil {
		return reconcile.Result{}, err
	}

	if err := unstructured.SetNestedField(template.Object, len(s) > 0, "status", "created"); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.statusClient.Status().Update(ctx, template); err != nil {
		return reconcile.Result{Requeue: true}, nil
	}
	return reconcile.Result{}, nil
}

type sortableStatuses []v1beta1.ConstraintTemplatePodStatus

func (s sortableStatuses) Len() int {
	return len(s)
}

func (s sortableStatuses) Less(i, j int) bool {
	return s[i].Status.ID < s[j].Status.ID
}

func (s sortableStatuses) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
