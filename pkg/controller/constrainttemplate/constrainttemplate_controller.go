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

package constrainttemplate

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/open-policy-agent/frameworks/constraint/pkg/apis/templates/v1beta1"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/frameworks/constraint/pkg/core/templates"
	statusv1beta1 "github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/constraint"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/constraintstatus"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/constrainttemplatestatus"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"github.com/open-policy-agent/gatekeeper/pkg/operations"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	"github.com/open-policy-agent/opa/ast"
	errorpkg "github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	finalizerName = "constrainttemplate.finalizers.gatekeeper.sh"
	ctrlName      = "constrainttemplate-controller"
)

var log = logf.Log.WithName("controller").WithValues("kind", "ConstraintTemplate", logging.Process, "constraint_template_controller")

var gvkConstraintTemplate = schema.GroupVersionKind{
	Group:   v1beta1.SchemeGroupVersion.Group,
	Version: v1beta1.SchemeGroupVersion.Version,
	Kind:    "ConstraintTemplate",
}

type Adder struct {
	Opa              *opa.Client
	WatchManager     *watch.Manager
	ControllerSwitch *watch.ControllerSwitch
	Tracker          *readiness.Tracker
	GetPod           func(context.Context) (*corev1.Pod, error)
}

// Add creates a new ConstraintTemplate Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func (a *Adder) Add(mgr manager.Manager) error {
	// events will be used to receive events from dynamic watches registered
	events := make(chan event.GenericEvent, 1024)
	r, err := newReconciler(mgr, a.Opa, a.WatchManager, a.ControllerSwitch, a.Tracker, events, events, a.GetPod)
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

func (a *Adder) InjectGetPod(getPod func(context.Context) (*corev1.Pod, error)) {
	a.GetPod = getPod
}

func (a *Adder) InjectMutationCache(mutationCache *mutation.System) {}

// newReconciler returns a new reconcile.Reconciler
// cstrEvents is the channel from which constraint controller will receive the events
// regEvents is the channel registered by Registrar to put the events in
// cstrEvents and regEvents point to same event channel except for testing.
func newReconciler(mgr manager.Manager, opa *opa.Client, wm *watch.Manager, cs *watch.ControllerSwitch, tracker *readiness.Tracker, cstrEvents <-chan event.GenericEvent, regEvents chan<- event.GenericEvent, getPod func(context.Context) (*corev1.Pod, error)) (*ReconcileConstraintTemplate, error) {
	// constraintsCache contains total number of constraints and shared mutex
	constraintsCache := constraint.NewConstraintsCache()

	// via the registrar below.
	constraintAdder := constraint.Adder{
		Opa:              opa,
		ConstraintsCache: constraintsCache,
		WatchManager:     wm,
		ControllerSwitch: cs,
		Events:           cstrEvents,
		Tracker:          tracker,
		GetPod:           getPod,
	}
	// Create subordinate controller - we will feed it events dynamically via watch
	if err := constraintAdder.Add(mgr); err != nil {
		return nil, fmt.Errorf("registering constraint controller: %w", err)
	}

	if operations.IsAssigned(operations.Status) {
		// statusEvents will be used to receive events from dynamic watches registered
		// via the registrar below.
		statusEvents := make(chan event.GenericEvent, 1024)
		csAdder := constraintstatus.Adder{
			Opa:              opa,
			WatchManager:     wm,
			ControllerSwitch: cs,
			Events:           statusEvents,
		}
		if err := csAdder.Add(mgr); err != nil {
			return nil, err
		}

		ctsAdder := constrainttemplatestatus.Adder{
			Opa:              opa,
			WatchManager:     wm,
			ControllerSwitch: cs,
		}
		if err := ctsAdder.Add(mgr); err != nil {
			return nil, err
		}
	}

	w, err := wm.NewRegistrar(ctrlName, regEvents)
	if err != nil {
		return nil, err
	}
	statusW, err := wm.NewRegistrar(ctrlName+"-status", regEvents)
	if err != nil {
		return nil, err
	}
	r, err := newStatsReporter()
	if err != nil {
		return nil, err
	}
	reconciler := &ReconcileConstraintTemplate{
		Client:        mgr.GetClient(),
		scheme:        mgr.GetScheme(),
		opa:           opa,
		watcher:       w,
		statusWatcher: statusW,
		cs:            cs,
		metrics:       r,
		tracker:       tracker,
		getPod:        getPod,
	}
	if getPod == nil {
		reconciler.getPod = reconciler.defaultGetPod
	}
	return reconciler, nil
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New(ctrlName, mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to ConstraintTemplate
	err = c.Watch(&source.Kind{Type: &v1beta1.ConstraintTemplate{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to ConstraintTemplateStatus
	err = c.Watch(
		&source.Kind{Type: &statusv1beta1.ConstraintTemplatePodStatus{}},
		handler.EnqueueRequestsFromMapFunc(constrainttemplatestatus.PodStatusToConstraintTemplateMapper(true)),
	)
	if err != nil {
		return err
	}

	// Watch for changes to Constraint CRDs
	err = c.Watch(
		&source.Kind{Type: &apiextensionsv1.CustomResourceDefinition{}},
		&handler.EnqueueRequestForOwner{
			OwnerType:    &v1beta1.ConstraintTemplate{},
			IsController: true,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileConstraintTemplate{}

// ReconcileConstraintTemplate reconciles a ConstraintTemplate object.
type ReconcileConstraintTemplate struct {
	client.Client
	scheme        *runtime.Scheme
	watcher       *watch.Registrar
	statusWatcher *watch.Registrar
	opa           *opa.Client
	cs            *watch.ControllerSwitch
	metrics       *reporter
	tracker       *readiness.Tracker
	getPod        func(context.Context) (*corev1.Pod, error)
}

// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=templates.gatekeeper.sh,resources=constrainttemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=templates.gatekeeper.sh,resources=constrainttemplates/finalizers,verbs=get;update;patch;delete
// +kubebuilder:rbac:groups=templates.gatekeeper.sh,resources=constrainttemplates/status,verbs=get;update;patch

// Reconcile reads that state of the cluster for a ConstraintTemplate object and makes changes based on the state read
// and what is in the ConstraintTemplate.Spec.
func (r *ReconcileConstraintTemplate) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := log.WithValues("template_name", request.Name)
	// Short-circuit if shutting down.
	if r.cs != nil {
		running := r.cs.Enter()
		defer r.cs.Exit()
		if !running {
			return reconcile.Result{}, nil
		}
	}

	defer r.metrics.registry.report(ctx, r.metrics)

	// Fetch the ConstraintTemplate instance
	deleted := false
	ct := &v1beta1.ConstraintTemplate{}
	err := r.Get(ctx, request.NamespacedName, ct)
	if err != nil {
		if !errors.IsNotFound(err) {
			return reconcile.Result{}, err
		}
		deleted = true
		// be sure we are using a blank constraint template so that
		// we know finalizer removal code won't break (can be removed once that
		// code is removed)
		ct = &v1beta1.ConstraintTemplate{}
	}
	deleted = deleted || !ct.GetDeletionTimestamp().IsZero()

	if containsString(finalizerName, ct.GetFinalizers()) {
		RemoveFinalizer(ct)
		if err := r.Update(ctx, ct); err != nil && !errors.IsNotFound(err) {
			log.Error(err, "update error")
			return reconcile.Result{Requeue: true}, nil
		}
	}

	if deleted {
		ctRef := &templates.ConstraintTemplate{}
		ctRef.SetNamespace(request.Namespace)
		ctRef.SetName(request.Name)
		ctUnversioned, err := r.opa.GetTemplate(ctx, ctRef)
		result := reconcile.Result{}
		if err != nil {
			log.Info("missing constraint template in OPA cache, no deletion necessary")
			logAction(ctRef, deletedAction)
			r.metrics.registry.remove(request.NamespacedName)
		} else {
			result, err = r.handleDelete(ctx, ctUnversioned)
			if err != nil {
				log.Error(err, "deletion error")
				logError(request.NamespacedName.Name)
				r.metrics.registry.add(request.NamespacedName, metrics.ErrorStatus)
				return reconcile.Result{}, err
			} else if !result.Requeue {
				logAction(ct, deletedAction)
				r.metrics.registry.remove(request.NamespacedName)
			}
		}
		err = r.deleteAllStatus(ctx, request.Name)
		return result, err
	}

	status, err := r.getOrCreatePodStatus(ctx, ct.Name)
	if err != nil {
		log.Info("could not get/create pod status object", "error", err)
		return reconcile.Result{}, err
	}
	status.Status.TemplateUID = ct.GetUID()
	status.Status.ObservedGeneration = ct.GetGeneration()
	status.Status.Errors = nil

	unversionedCT := &templates.ConstraintTemplate{}
	if err := r.scheme.Convert(ct, unversionedCT, nil); err != nil {
		log.Error(err, "conversion error")
		r.metrics.registry.add(request.NamespacedName, metrics.ErrorStatus)
		logError(request.NamespacedName.Name)
		return reconcile.Result{}, err
	}

	unversionedProposedCRD, err := r.opa.CreateCRD(ctx, unversionedCT)
	if err != nil {
		log.Error(err, "CRD creation error")
		r.tracker.TryCancelTemplate(unversionedCT) // Don't track templates that failed compilation
		r.metrics.registry.add(request.NamespacedName, metrics.ErrorStatus)
		var createErr *v1beta1.CreateCRDError
		if parseErrs, ok := err.(ast.Errors); ok {
			for i := 0; i < len(parseErrs); i++ {
				createErr = &v1beta1.CreateCRDError{Code: parseErrs[i].Code, Message: parseErrs[i].Message, Location: parseErrs[i].Location.String()}
				status.Status.Errors = append(status.Status.Errors, createErr)
			}
		} else {
			createErr = &v1beta1.CreateCRDError{Code: "create_error", Message: err.Error()}
			status.Status.Errors = append(status.Status.Errors, createErr)
		}

		if updateErr := r.Update(ctx, status); updateErr != nil {
			log.Error(updateErr, "update error")
			return reconcile.Result{Requeue: true}, nil
		}
		logError(request.NamespacedName.Name)
		return reconcile.Result{}, nil
	}

	proposedCRD := &apiextensionsv1.CustomResourceDefinition{}
	if err := r.scheme.Convert(unversionedProposedCRD, proposedCRD, nil); err != nil {
		log.Error(err, "CRD conversion error")
		r.tracker.TryCancelTemplate(unversionedCT) // Don't track templates that failed compilation
		r.metrics.registry.add(request.NamespacedName, metrics.ErrorStatus)
		logError(request.NamespacedName.Name)
		err := r.reportErrorOnCTStatus(ctx, "conversion_error", "Could not convert from unversioned resource", status, err)
		return reconcile.Result{}, err
	}

	name := unversionedProposedCRD.GetName()
	namespace := unversionedProposedCRD.GetNamespace()
	// Check if the constraint CRD already exists
	action := updatedAction
	currentCRD := &apiextensionsv1.CustomResourceDefinition{}
	err = r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, currentCRD)
	switch {
	case err == nil:
		break

	case errors.IsNotFound(err):
		action = createdAction
		currentCRD = nil

	default:
		log.Error(err, "client.Get CRD error")
		logError(request.NamespacedName.Name)
		r.metrics.registry.add(request.NamespacedName, metrics.ErrorStatus)
		return reconcile.Result{}, err
	}

	result, err := r.handleUpdate(ctx, ct, unversionedCT, proposedCRD, currentCRD, status)
	if err != nil {
		log.Error(err, "update error")
		logError(request.NamespacedName.Name)
		r.metrics.registry.add(request.NamespacedName, metrics.ErrorStatus)
	} else if !result.Requeue {
		logAction(ct, action)
		r.metrics.registry.add(request.NamespacedName, metrics.ActiveStatus)
	}
	return result, err
}

func (r *ReconcileConstraintTemplate) reportErrorOnCTStatus(ctx context.Context, code, message string, status *statusv1beta1.ConstraintTemplatePodStatus, err error) error {
	status.Status.Errors = []*v1beta1.CreateCRDError{}
	createErr := &v1beta1.CreateCRDError{
		Code:    code,
		Message: fmt.Sprintf("%s: %s", message, err),
	}
	status.Status.Errors = append(status.Status.Errors, createErr)
	if err2 := r.Update(ctx, status); err2 != nil {
		return errorpkg.Wrap(err, fmt.Sprintf("Could not update status: %s", err2))
	}
	return err
}

func (r *ReconcileConstraintTemplate) handleUpdate(
	ctx context.Context,
	ct *v1beta1.ConstraintTemplate,
	unversionedCT *templates.ConstraintTemplate,
	proposedCRD, currentCRD *apiextensionsv1.CustomResourceDefinition,
	status *statusv1beta1.ConstraintTemplatePodStatus,
) (reconcile.Result, error) {
	name := proposedCRD.GetName()
	log := log.WithValues("name", ct.GetName(), "crdName", name)

	log.Info("loading code into OPA")
	beginCompile := time.Now()

	// It's important that opa.AddTemplate() is called first. That way we can
	// rely on a template's existence in OPA to know whether a watch needs
	// to be removed
	if _, err := r.opa.AddTemplate(ctx, unversionedCT); err != nil {
		if err := r.metrics.reportIngestDuration(ctx, metrics.ErrorStatus, time.Since(beginCompile)); err != nil {
			log.Error(err, "failed to report constraint template ingestion duration")
		}
		err := r.reportErrorOnCTStatus(ctx, "ingest_error", "Could not ingest Rego", status, err)
		r.tracker.TryCancelTemplate(unversionedCT) // Don't track templates that failed compilation
		return reconcile.Result{}, err
	}

	if err := r.metrics.reportIngestDuration(ctx, metrics.ActiveStatus, time.Since(beginCompile)); err != nil {
		log.Error(err, "failed to report constraint template ingestion duration")
	}

	// Mark for readiness tracking
	t := r.tracker.For(gvkConstraintTemplate)
	t.Observe(unversionedCT)
	log.Info("[readiness] observed ConstraintTemplate", "name", unversionedCT.GetName())

	var newCRD *apiextensionsv1.CustomResourceDefinition
	if currentCRD == nil {
		newCRD = proposedCRD.DeepCopy()
	} else {
		newCRD = currentCRD.DeepCopy()
		newCRD.Spec = proposedCRD.Spec
	}

	if err := controllerutil.SetControllerReference(ct, newCRD, r.scheme); err != nil {
		return reconcile.Result{}, err
	}

	if currentCRD == nil {
		log.Info("creating crd")
		if err := r.Create(ctx, newCRD); err != nil {
			err := r.reportErrorOnCTStatus(ctx, "create_error", "Could not create CRD", status, err)
			return reconcile.Result{}, err
		}
	} else if !reflect.DeepEqual(newCRD, currentCRD) {
		log.Info("updating crd")
		if err := r.Update(ctx, newCRD); err != nil {
			err := r.reportErrorOnCTStatus(ctx, "update_error", "Could not update CRD", status, err)
			return reconcile.Result{}, err
		}
	}
	// This must go after CRD creation/update as otherwise AddWatch will always fail
	log.Info("making sure constraint is in watcher registry")
	if err := r.addWatch(ctx, makeGvk(ct.Spec.CRD.Spec.Names.Kind)); err != nil {
		log.Error(err, "error adding template to watch registry")
		return reconcile.Result{}, err
	}
	if err := r.Update(ctx, status); err != nil {
		log.Error(err, "update error")
		return reconcile.Result{Requeue: true}, nil
	}
	return reconcile.Result{}, nil
}

func (r *ReconcileConstraintTemplate) handleDelete(
	ctx context.Context,
	ct *templates.ConstraintTemplate) (reconcile.Result, error) {
	log := log.WithValues("name", ct.GetName())
	log.Info("removing from watcher registry")
	gvk := makeGvk(ct.Spec.CRD.Spec.Names.Kind)
	if err := r.removeWatch(ctx, gvk); err != nil {
		return reconcile.Result{}, err
	}
	r.tracker.CancelTemplate(ct)

	// removing the template from the OPA cache must go last as we are relying
	// on that cache to derive the Kind to remove from the watch
	if _, err := r.opa.RemoveTemplate(ctx, ct); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *ReconcileConstraintTemplate) defaultGetPod(_ context.Context) (*corev1.Pod, error) {
	// require injection of GetPod in order to control what client we use to
	// guarantee we don't inadvertently create a watch
	panic("GetPod must be injected")
}

func (r *ReconcileConstraintTemplate) deleteAllStatus(ctx context.Context, ctName string) error {
	statusObj := &statusv1beta1.ConstraintTemplatePodStatus{}
	sName, err := statusv1beta1.KeyForConstraintTemplate(util.GetPodName(), ctName)
	if err != nil {
		return err
	}
	statusObj.SetName(sName)
	statusObj.SetNamespace(util.GetNamespace())
	if err := r.Delete(ctx, statusObj); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
	}

	cstrStatusObjs := &statusv1beta1.ConstraintPodStatusList{}
	if err := r.List(ctx, cstrStatusObjs, client.MatchingLabels(map[string]string{
		statusv1beta1.PodLabel:                    util.GetPodName(),
		statusv1beta1.ConstraintTemplateNameLabel: ctName,
	})); err != nil {
		return err
	}
	for index := range cstrStatusObjs.Items {
		if err := r.Delete(ctx, &cstrStatusObjs.Items[index]); err != nil {
			if !errors.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}

func (r *ReconcileConstraintTemplate) getOrCreatePodStatus(ctx context.Context, ctName string) (*statusv1beta1.ConstraintTemplatePodStatus, error) {
	statusObj := &statusv1beta1.ConstraintTemplatePodStatus{}
	sName, err := statusv1beta1.KeyForConstraintTemplate(util.GetPodName(), ctName)
	if err != nil {
		return nil, err
	}
	key := types.NamespacedName{Name: sName, Namespace: util.GetNamespace()}
	if err := r.Get(ctx, key, statusObj); err != nil {
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
	statusObj, err = statusv1beta1.NewConstraintTemplateStatusForPod(pod, ctName, r.scheme)
	if err != nil {
		return nil, err
	}
	if err := r.Create(ctx, statusObj); err != nil {
		return nil, err
	}
	return statusObj, nil
}

func (r *ReconcileConstraintTemplate) addWatch(ctx context.Context, kind schema.GroupVersionKind) error {
	if err := r.watcher.AddWatch(ctx, kind); err != nil {
		return err
	}
	return r.statusWatcher.AddWatch(ctx, kind)
}

func (r *ReconcileConstraintTemplate) removeWatch(ctx context.Context, kind schema.GroupVersionKind) error {
	if err := r.watcher.RemoveWatch(ctx, kind); err != nil {
		return err
	}
	return r.statusWatcher.RemoveWatch(ctx, kind)
}

type action string

const (
	createdAction = action("created")
	updatedAction = action("updated")
	deletedAction = action("deleted")
)

type namedObj interface {
	GetName() string
}

func logAction(template namedObj, a action) {
	log.Info(
		fmt.Sprintf("template was %s", string(a)),
		logging.EventType, fmt.Sprintf("template_%s", string(a)),
		logging.TemplateName, template.GetName(),
	)
}

func logError(name string) {
	log.Info(
		"unable to ingest template",
		logging.EventType, "template_ingest_error",
		logging.TemplateName, name,
	)
}

func RemoveFinalizer(instance *v1beta1.ConstraintTemplate) {
	instance.SetFinalizers(removeString(finalizerName, instance.GetFinalizers()))
}

func makeGvk(kind string) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   "constraints.gatekeeper.sh",
		Version: "v1beta1",
		Kind:    kind,
	}
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
