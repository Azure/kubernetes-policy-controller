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

package webhook

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/frameworks/constraint/pkg/core/templates"
	rtypes "github.com/open-policy-agent/frameworks/constraint/pkg/types"
	"github.com/open-policy-agent/gatekeeper/apis"
	mutationsv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/mutations/v1alpha1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/keys"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/mutators"
	"github.com/open-policy-agent/gatekeeper/pkg/target"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// httpStatusWarning is the HTTP return code for displaying warning messages in admission webhook (supported in Kubernetes v1.19+)
// https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#response
const httpStatusWarning = 299

var maxServingThreads = flag.Int("max-serving-threads", -1, "(alpha) cap the number of threads handling non-trivial requests, -1 means an infinite number of threads")

func init() {
	AddToManagerFuncs = append(AddToManagerFuncs, AddPolicyWebhook)
	if err := apis.AddToScheme(runtimeScheme); err != nil {
		log.Error(err, "unable to add to scheme")
		panic(err)
	}
}

// +kubebuilder:webhook:verbs=create;update,path=/v1/admit,mutating=false,failurePolicy=ignore,groups=*,resources=*,versions=*,name=validation.gatekeeper.sh,sideEffects=None,admissionReviewVersions=v1;v1beta1,matchPolicy=Exact
// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch

// AddPolicyWebhook registers the policy webhook server with the manager.
func AddPolicyWebhook(mgr manager.Manager, opa *opa.Client, processExcluder *process.Excluder, mutationCache *mutation.System) error {
	reporter, err := newStatsReporter()
	if err != nil {
		return err
	}
	eventBroadcaster := record.NewBroadcaster()
	kubeClient := kubernetes.NewForConfigOrDie(mgr.GetConfig())
	eventBroadcaster.StartRecordingToSink(&clientcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		corev1.EventSource{Component: "gatekeeper-webhook"})
	handler := &validationHandler{
		opa: opa,
		webhookHandler: webhookHandler{
			client:          mgr.GetClient(),
			reader:          mgr.GetAPIReader(),
			reporter:        reporter,
			processExcluder: processExcluder,
			eventRecorder:   recorder,
			gkNamespace:     util.GetNamespace(),
		},
	}
	if *maxServingThreads > 0 {
		handler.semaphore = make(chan struct{}, *maxServingThreads)
	}
	wh := &admission.Webhook{Handler: handler}
	// TODO(https://github.com/open-policy-agent/gatekeeper/issues/661): remove log injection if the race condition in the cited bug is eliminated.
	// Otherwise we risk having unstable logger names for the webhook.
	if err := wh.InjectLogger(log); err != nil {
		return err
	}
	mgr.GetWebhookServer().Register("/v1/admit", wh)
	return nil
}

var _ admission.Handler = &validationHandler{}

type validationHandler struct {
	webhookHandler
	opa       *opa.Client
	semaphore chan struct{}
}

// Handle the validation request
// nolint: gocritic // Must accept admission.Request as a struct to satisfy Handler interface.
func (h *validationHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := log.WithValues("hookType", "validation")

	timeStart := time.Now()

	if isGkServiceAccount(req.AdmissionRequest.UserInfo) {
		return admission.ValidationResponse(true, "Gatekeeper does not self-manage")
	}

	if req.AdmissionRequest.Operation == admissionv1.Delete {
		// oldObject is the existing object.
		// It is null for DELETE operations in API servers prior to v1.15.0.
		// https://github.com/kubernetes/website/pull/14671
		if req.AdmissionRequest.OldObject.Raw == nil {
			vResp := admission.ValidationResponse(false, "For admission webhooks registered for DELETE operations, please use Kubernetes v1.15.0+.")
			vResp.Result.Code = http.StatusInternalServerError
			return vResp
		}
		// For admission webhooks registered for DELETE operations on k8s built APIs or CRDs,
		// the apiserver now sends the existing object as admissionRequest.Request.OldObject to the webhook
		// object is the new object being admitted.
		// It is null for DELETE operations.
		// https://github.com/kubernetes/kubernetes/pull/76346
		req.AdmissionRequest.Object = req.AdmissionRequest.OldObject
	}

	if userErr, err := h.validateGatekeeperResources(ctx, &req); err != nil {
		vResp := admission.ValidationResponse(false, err.Error())
		if vResp.Result == nil {
			vResp.Result = &metav1.Status{}
		}
		if userErr {
			vResp.Result.Code = http.StatusUnprocessableEntity
		} else {
			vResp.Result.Code = http.StatusInternalServerError
		}
		return vResp
	}

	requestResponse := unknownResponse
	defer func() {
		if h.reporter != nil {
			if err := h.reporter.ReportValidationRequest(ctx, requestResponse, time.Since(timeStart)); err != nil {
				log.Error(err, "failed to report request")
			}
		}
	}()

	// namespace is excluded from webhook using config
	isExcludedNamespace, err := h.skipExcludedNamespace(&req.AdmissionRequest, process.Webhook)
	if err != nil {
		log.Error(err, "error while excluding namespace")
	}

	if isExcludedNamespace {
		requestResponse = allowResponse
		return admission.ValidationResponse(true, "Namespace is set to be ignored by Gatekeeper config")
	}

	resp, err := h.reviewRequest(ctx, &req)
	if err != nil {
		log.Error(err, "error executing query")
		vResp := admission.ValidationResponse(false, err.Error())
		if vResp.Result == nil {
			vResp.Result = &metav1.Status{}
		}
		vResp.Result.Code = http.StatusInternalServerError
		requestResponse = errorResponse
		return vResp
	}

	res := resp.Results()
	denyMsgs, warnMsgs := h.getValidationMessages(res, &req)

	if len(denyMsgs) > 0 {
		vResp := admission.ValidationResponse(false, strings.Join(denyMsgs, "\n"))
		if vResp.Result == nil {
			vResp.Result = &metav1.Status{}
		}
		if len(warnMsgs) > 0 {
			vResp.Warnings = warnMsgs
		}
		vResp.Result.Code = http.StatusForbidden
		requestResponse = denyResponse
		return vResp
	}

	requestResponse = allowResponse
	vResp := admission.ValidationResponse(true, "")
	if vResp.Result == nil {
		vResp.Result = &metav1.Status{}
	}
	if len(warnMsgs) > 0 {
		vResp.Warnings = warnMsgs
		vResp.Result.Code = httpStatusWarning
	}
	return vResp
}

func (h *validationHandler) getValidationMessages(res []*rtypes.Result, req *admission.Request) ([]string, []string) {
	var denyMsgs, warnMsgs []string
	var resourceName string
	if len(res) > 0 && (*logDenies || *emitAdmissionEvents) {
		resourceName = req.AdmissionRequest.Name
		if len(resourceName) == 0 && req.AdmissionRequest.Object.Raw != nil {
			// On a CREATE operation, the client may omit name and
			// rely on the server to generate the name.
			obj := &unstructured.Unstructured{}
			if _, _, err := deserializer.Decode(req.AdmissionRequest.Object.Raw, nil, obj); err == nil {
				resourceName = obj.GetName()
			}
		}
	}
	for _, r := range res {
		if err := util.ValidateEnforcementAction(util.EnforcementAction(r.EnforcementAction)); err != nil {
			continue
		}
		if *logDenies {
			log.WithValues(
				logging.Process, "admission",
				logging.EventType, "violation",
				logging.ConstraintName, r.Constraint.GetName(),
				logging.ConstraintGroup, r.Constraint.GroupVersionKind().Group,
				logging.ConstraintAPIVersion, r.Constraint.GroupVersionKind().Version,
				logging.ConstraintKind, r.Constraint.GetKind(),
				logging.ConstraintAction, r.EnforcementAction,
				logging.ResourceGroup, req.AdmissionRequest.Kind.Group,
				logging.ResourceAPIVersion, req.AdmissionRequest.Kind.Version,
				logging.ResourceKind, req.AdmissionRequest.Kind.Kind,
				logging.ResourceNamespace, req.AdmissionRequest.Namespace,
				logging.ResourceName, resourceName,
				logging.RequestUsername, req.AdmissionRequest.UserInfo.Username,
			).Info("denied admission")
		}
		if *emitAdmissionEvents {
			annotations := map[string]string{
				logging.Process:              "admission",
				logging.EventType:            "violation",
				logging.ConstraintName:       r.Constraint.GetName(),
				logging.ConstraintGroup:      r.Constraint.GroupVersionKind().Group,
				logging.ConstraintAPIVersion: r.Constraint.GroupVersionKind().Version,
				logging.ConstraintKind:       r.Constraint.GetKind(),
				logging.ConstraintAction:     r.EnforcementAction,
				logging.ResourceGroup:        req.AdmissionRequest.Kind.Group,
				logging.ResourceAPIVersion:   req.AdmissionRequest.Kind.Version,
				logging.ResourceKind:         req.AdmissionRequest.Kind.Kind,
				logging.ResourceNamespace:    req.AdmissionRequest.Namespace,
				logging.ResourceName:         resourceName,
				logging.RequestUsername:      req.AdmissionRequest.UserInfo.Username,
			}
			var eventMsg, reason string
			switch r.EnforcementAction {
			case string(util.Dryrun):
				eventMsg = "Dryrun violation"
				reason = "DryrunViolation"
			case string(util.Warn):
				eventMsg = "Admission webhook \"validation.gatekeeper.sh\" raised a warning for this request"
				reason = "WarningAdmission"
			default:
				eventMsg = "Admission webhook \"validation.gatekeeper.sh\" denied request"
				reason = "FailedAdmission"
			}
			ref := getViolationRef(
				h.gkNamespace,
				req.AdmissionRequest.Kind.Kind,
				resourceName,
				req.AdmissionRequest.Namespace,
				r.Constraint.GetKind(),
				r.Constraint.GetName(),
				r.Constraint.GetNamespace())
			h.eventRecorder.AnnotatedEventf(
				ref,
				annotations,
				corev1.EventTypeWarning,
				reason,
				"%s, Resource Namespace: %s, Constraint: %s, Message: %s",
				eventMsg,
				req.AdmissionRequest.Namespace,
				r.Constraint.GetName(),
				r.Msg)
		}

		if r.EnforcementAction == string(util.Deny) {
			denyMsgs = append(denyMsgs, fmt.Sprintf("[%s] %s", r.Constraint.GetName(), r.Msg))
		}

		if r.EnforcementAction == string(util.Warn) {
			warnMsgs = append(warnMsgs, fmt.Sprintf("[%s] %s", r.Constraint.GetName(), r.Msg))
		}
	}
	return denyMsgs, warnMsgs
}

// validateGatekeeperResources returns whether an issue is user error (vs internal) and any errors
// validating internal resources.
func (h *validationHandler) validateGatekeeperResources(ctx context.Context, req *admission.Request) (bool, error) {
	gvk := req.AdmissionRequest.Kind

	switch {
	case gvk.Group == "templates.gatekeeper.sh" && gvk.Kind == "ConstraintTemplate":
		return h.validateTemplate(ctx, req)
	case gvk.Group == "constraints.gatekeeper.sh":
		return h.validateConstraint(ctx, req)
	case gvk.Group == "config.gatekeeper.sh" && gvk.Kind == "Config":
		if err := h.validateConfigResource(req); err != nil {
			return true, err
		}
	case req.AdmissionRequest.Kind.Group == mutationsGroup && req.AdmissionRequest.Kind.Kind == "AssignMetadata":
		return h.validateAssignMetadata(req)
	case req.AdmissionRequest.Kind.Group == mutationsGroup && req.AdmissionRequest.Kind.Kind == "Assign":
		return h.validateAssign(req)
	}

	return false, nil
}

func (h *validationHandler) validateTemplate(ctx context.Context, req *admission.Request) (bool, error) {
	templ, _, err := deserializer.Decode(req.AdmissionRequest.Object.Raw, nil, nil)
	if err != nil {
		return false, err
	}
	unversioned := &templates.ConstraintTemplate{}
	if err := runtimeScheme.Convert(templ, unversioned, nil); err != nil {
		return false, err
	}
	if _, err := h.opa.CreateCRD(ctx, unversioned); err != nil {
		return true, err
	}
	return false, nil
}

func (h *validationHandler) validateConstraint(ctx context.Context, req *admission.Request) (bool, error) {
	obj := &unstructured.Unstructured{}
	if _, _, err := deserializer.Decode(req.AdmissionRequest.Object.Raw, nil, obj); err != nil {
		return false, err
	}
	if err := h.opa.ValidateConstraint(ctx, obj); err != nil {
		return true, err
	}

	enforcementActionString, found, err := unstructured.NestedString(obj.Object, "spec", "enforcementAction")
	if err != nil {
		return false, err
	}
	enforcementAction := util.EnforcementAction(enforcementActionString)
	if found && enforcementAction != "" {
		if !*disableEnforcementActionValidation {
			err = util.ValidateEnforcementAction(enforcementAction)
			if err != nil {
				return false, err
			}
		}
	} else {
		return true, nil
	}
	return false, nil
}

func (h *validationHandler) validateConfigResource(req *admission.Request) error {
	if req.Name != keys.Config.Name {
		return fmt.Errorf("config resource must have name 'config'")
	}
	return nil
}

func (h *validationHandler) validateAssignMetadata(req *admission.Request) (bool, error) {
	obj, _, err := deserializer.Decode(req.AdmissionRequest.Object.Raw, nil, &mutationsv1alpha1.AssignMetadata{})
	if err != nil {
		return false, err
	}
	assignMetadata, ok := obj.(*mutationsv1alpha1.AssignMetadata)
	if !ok {
		return false, fmt.Errorf("Deserialized object is not of type AssignMetadata")
	}
	err = mutators.IsValidAssignMetadata(assignMetadata)
	if err != nil {
		return true, err
	}

	return false, nil
}

func (h *validationHandler) validateAssign(req *admission.Request) (bool, error) {
	obj, _, err := deserializer.Decode(req.AdmissionRequest.Object.Raw, nil, &mutationsv1alpha1.Assign{})
	if err != nil {
		return false, err
	}
	assign, ok := obj.(*mutationsv1alpha1.Assign)
	if !ok {
		return false, fmt.Errorf("Deserialized object is not of type Assign")
	}

	err = mutators.IsValidAssign(assign)
	if err != nil {
		return true, err
	}

	return false, nil
}

// traceSwitch returns true if a request should be traced.
func (h *validationHandler) reviewRequest(ctx context.Context, req *admission.Request) (*rtypes.Responses, error) {
	// if we have a maximum number of concurrent serving goroutines, try to acquire
	// a lock and block until we succeed
	if h.semaphore != nil {
		select {
		case h.semaphore <- struct{}{}:
			defer func() {
				<-h.semaphore
			}()
		case <-ctx.Done():
			return nil, errors.New("serving context canceled, aborting request")
		}
	}
	trace, dump := h.tracingLevel(ctx, req)
	// Coerce server-side apply admission requests into treating namespaces
	// the same way as older admission requests. See
	// https://github.com/open-policy-agent/gatekeeper/issues/792
	if req.Kind.Kind == namespaceKind && req.Kind.Group == "" {
		req.Namespace = ""
	}
	review := &target.AugmentedReview{AdmissionRequest: &req.AdmissionRequest}
	if req.AdmissionRequest.Namespace != "" {
		ns := &corev1.Namespace{}
		if err := h.client.Get(ctx, types.NamespacedName{Name: req.AdmissionRequest.Namespace}, ns); err != nil {
			if !k8serrors.IsNotFound(err) {
				return nil, err
			}
			// bypass cached client and ask api-server directly
			err = h.reader.Get(ctx, types.NamespacedName{Name: req.AdmissionRequest.Namespace}, ns)
			if err != nil {
				return nil, err
			}
		}
		review.Namespace = ns
	}

	resp, err := h.opa.Review(ctx, review, opa.Tracing(trace))
	if trace {
		log.Info(resp.TraceDump())
	}
	if dump {
		dump, err := h.opa.Dump(ctx)
		if err != nil {
			log.Error(err, "dump error")
		} else {
			log.Info(dump)
		}
	}
	return resp, err
}

func getViolationRef(gkNamespace, rkind, rname, rnamespace, ckind, cname, cnamespace string) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind:      rkind,
		Name:      rname,
		UID:       types.UID(rkind + "/" + rnamespace + "/" + rname + "/" + ckind + "/" + cnamespace + "/" + cname),
		Namespace: gkNamespace,
	}
}
