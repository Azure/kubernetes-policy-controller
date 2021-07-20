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
	"net/http"
	"time"

	"github.com/open-policy-agent/cert-controller/pkg/rotator"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/gatekeeper/apis"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func init() {
	AddToManagerFuncs = append(AddToManagerFuncs, AddMutatingWebhook)

	if err := apis.AddToScheme(runtimeScheme); err != nil {
		log.Error(err, "unable to add to scheme")
		panic(err)
	}
}

// TODO enable this once mutation is beta +kubebuilder:webhook:verbs=create;update,path=/v1/mutate,mutating=true,failurePolicy=ignore,groups=*,resources=*,versions=*,name=mutation.gatekeeper.sh,sideEffects=None,admissionReviewVersions=v1;v1beta1,matchPolicy=Exact
// TODO enable this once mutation is beta +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch;update

// AddMutatingWebhook registers the mutating webhook server with the manager.
func AddMutatingWebhook(ctx context.Context, mgr manager.Manager, client *opa.Client, processExcluder *process.Excluder, mutationSystem *mutation.System) error {
	if !*mutation.MutationEnabled {
		return nil
	}
	reporter, err := newStatsReporter(ctx)
	if err != nil {
		return err
	}
	eventBroadcaster := record.NewBroadcaster()
	kubeClient := kubernetes.NewForConfigOrDie(mgr.GetConfig())

	eventBroadcaster.StartRecordingToSink(&clientcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		corev1.EventSource{Component: "gatekeeper-mutation-webhook"})

	wh := &admission.Webhook{
		Handler: &mutationHandler{
			webhookHandler: webhookHandler{
				client:          mgr.GetClient(),
				reader:          mgr.GetAPIReader(),
				reporter:        reporter,
				processExcluder: processExcluder,
				eventRecorder:   recorder,
				gkNamespace:     util.GetNamespace(),
			},
			mutationSystem: mutationSystem,
			deserializer:   codecs.UniversalDeserializer(),
		},
	}

	// TODO(https://github.com/open-policy-agent/gatekeeper/issues/661): remove log injection if the race condition in the cited bug is eliminated.
	// Otherwise we risk having unstable logger names for the webhook.
	if err := wh.InjectLogger(log); err != nil {
		return err
	}
	mgr.GetWebhookServer().Register("/v1/mutate", wh)

	return nil
}

var _ admission.Handler = &mutationHandler{}

type mutationHandler struct {
	webhookHandler
	mutationSystem *mutation.System
	deserializer   runtime.Decoder
}

// Handle the mutation request
// nolint: gocritic // Must accept admission.Request to satisfy interface.
func (h *mutationHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := log.WithValues("hookType", "mutation")
	timeStart := time.Now()

	if isGkServiceAccount(req.AdmissionRequest.UserInfo) {
		return admission.ValidationResponse(true, "Gatekeeper does not self-manage")
	}

	if req.AdmissionRequest.Operation != admissionv1.Create &&
		req.AdmissionRequest.Operation != admissionv1.Update {
		return admission.ValidationResponse(true, "Mutating only on create or update")
	}

	if h.isGatekeeperResource(ctx, &req) {
		return admission.ValidationResponse(true, "Not mutating gatekeeper resources")
	}

	requestResponse := unknownResponse
	defer func() {
		if h.reporter != nil {
			if err := h.reporter.ReportMutationRequest(requestResponse, time.Since(timeStart)); err != nil {
				log.Error(err, "failed to report request")
			}
		}
	}()

	// namespace is excluded from webhook using config
	isExcludedNamespace, err := h.skipExcludedNamespace(&req.AdmissionRequest, process.Mutation)
	if err != nil {
		log.Error(err, "error while excluding namespace")
	}

	if isExcludedNamespace {
		requestResponse = skipResponse
		return admission.ValidationResponse(true, "Namespace is set to be ignored by Gatekeeper config")
	}

	resp, err := h.mutateRequest(ctx, &req)
	if err != nil {
		requestResponse = errorResponse
		return admission.Errored(int32(http.StatusInternalServerError), err)
	}
	requestResponse = successResponse
	return resp
}

func (h *mutationHandler) mutateRequest(ctx context.Context, req *admission.Request) (admission.Response, error) {
	ns := &corev1.Namespace{}

	// if the object being mutated is a namespace itself, we use it as namespace
	switch {
	case req.Kind.Kind == namespaceKind && req.Kind.Group == "":
		req.Namespace = ""
		obj, _, err := deserializer.Decode(req.Object.Raw, nil, &corev1.Namespace{})
		if err != nil {
			return admission.Errored(int32(http.StatusInternalServerError), err), nil
		}
		ok := false
		ns, ok = obj.(*corev1.Namespace)
		if !ok {
			return admission.Errored(int32(http.StatusInternalServerError), errors.New("failed to cast namespace object")), nil
		}
	case req.AdmissionRequest.Namespace != "":
		if err := h.client.Get(ctx, types.NamespacedName{Name: req.AdmissionRequest.Namespace}, ns); err != nil {
			if !k8serrors.IsNotFound(err) {
				log.Error(err, "error retrieving namespace", "name", req.AdmissionRequest.Namespace)
				return admission.Errored(int32(http.StatusInternalServerError), err), nil
			}
			// bypass cached client and ask api-server directly
			err = h.reader.Get(ctx, types.NamespacedName{Name: req.AdmissionRequest.Namespace}, ns)
			if err != nil {
				log.Error(err, "error retrieving namespace from API server", "name", req.AdmissionRequest.Namespace)
				return admission.Errored(int32(http.StatusInternalServerError), err), nil
			}
		}
	default:
		ns = nil
	}
	obj := unstructured.Unstructured{}
	err := obj.UnmarshalJSON(req.Object.Raw)
	if err != nil {
		log.Error(err, "failed to unmarshal", "object", string(req.Object.Raw))
		return admission.Errored(int32(http.StatusInternalServerError), err), nil
	}

	mutated, err := h.mutationSystem.Mutate(&obj, ns)
	if err != nil {
		log.Error(err, "failed to mutate object", "object", string(req.Object.Raw))
		return admission.Errored(int32(http.StatusInternalServerError), err), nil
	}
	if !mutated {
		return admission.ValidationResponse(true, "Resource was not mutated"), nil
	}

	newJSON, err := obj.MarshalJSON()
	if err != nil {
		log.Error(err, "failed to marshal mutated object", "object", obj)
		return admission.Errored(int32(http.StatusInternalServerError), err), nil
	}
	resp := admission.PatchResponseFromRaw(req.Object.Raw, newJSON)
	return resp, nil
}

func AppendMutationWebhookIfEnabled(webhooks []rotator.WebhookInfo) []rotator.WebhookInfo {
	if *mutation.MutationEnabled {
		return append(webhooks, rotator.WebhookInfo{
			Name: MwhName,
			Type: rotator.Mutating,
		})
	}
	return webhooks
}
