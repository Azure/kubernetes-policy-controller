package webhook

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/open-policy-agent/gatekeeper/apis"
	"github.com/open-policy-agent/gatekeeper/apis/config/v1alpha1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/keys"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type requestResponse string

const (
	successResponse requestResponse = "success"
	errorResponse   requestResponse = "error"
	denyResponse    requestResponse = "deny"
	allowResponse   requestResponse = "allow"
	unknownResponse requestResponse = "unknown"
	skipResponse    requestResponse = "skip"
)

var log = logf.Log.WithName("webhook")

var (
	// VwhName is the metadata.name of the Gatekeeper ValidatingWebhookConfiguration.
	VwhName = "gatekeeper-validating-webhook-configuration"
	// MwhName is the metadata.name of the Gatekeeper MutatingWebhookConfiguration.
	MwhName = "gatekeeper-mutating-webhook-configuration"
)

const (
	serviceAccountName = "gatekeeper-admin"
	mutationsGroup     = "mutations.gatekeeper.sh"
	namespaceKind      = "Namespace"
)

var (
	runtimeScheme                      = k8sruntime.NewScheme()
	codecs                             = serializer.NewCodecFactory(runtimeScheme)
	deserializer                       = codecs.UniversalDeserializer()
	disableEnforcementActionValidation = flag.Bool("disable-enforcementaction-validation", false, "disable validation of the enforcementAction field of a constraint")
	logDenies                          = flag.Bool("log-denies", false, "log detailed info on each deny")
	emitAdmissionEvents                = flag.Bool("emit-admission-events", false, "(alpha) emit Kubernetes events in gatekeeper namespace for each admission violation")
	serviceaccount                     = fmt.Sprintf("system:serviceaccount:%s:%s", util.GetNamespace(), serviceAccountName)
	// webhookName is deprecated, set this on the manifest YAML if needed".
)

func init() {
	_ = apis.AddToScheme(runtimeScheme)
}

func isGkServiceAccount(user authenticationv1.UserInfo) bool {
	return user.Username == serviceaccount
}

type webhookHandler struct {
	client   client.Client
	reporter StatsReporter
	// reader that will be configured to use the API server
	// obtained from mgr.GetAPIReader()
	reader client.Reader
	// for testing
	injectedConfig  *v1alpha1.Config
	processExcluder *process.Excluder
	eventRecorder   record.EventRecorder
	gkNamespace     string
}

func (h *webhookHandler) getConfig(ctx context.Context) (*v1alpha1.Config, error) {
	if h.injectedConfig != nil {
		return h.injectedConfig, nil
	}
	if h.client == nil {
		return nil, errors.New("no client available to retrieve validation config")
	}
	cfg := &v1alpha1.Config{}
	return cfg, h.client.Get(ctx, keys.Config, cfg)
}

// isGatekeeperResource returns true if the request relates to a gatekeeper resource.
func (h *webhookHandler) isGatekeeperResource(req *admission.Request) bool {
	if req.AdmissionRequest.Kind.Group == "templates.gatekeeper.sh" ||
		req.AdmissionRequest.Kind.Group == "constraints.gatekeeper.sh" ||
		req.AdmissionRequest.Kind.Group == mutationsGroup ||
		req.AdmissionRequest.Kind.Group == "config.gatekeeper.sh" ||
		req.AdmissionRequest.Kind.Group == "status.gatekeeper.sh" {
		return true
	}

	return false
}

func (h *webhookHandler) tracingLevel(ctx context.Context, req *admission.Request) (bool, bool) {
	cfg, _ := h.getConfig(ctx)
	traceEnabled := false
	dump := false
	for _, trace := range cfg.Spec.Validation.Traces {
		if trace.User != req.AdmissionRequest.UserInfo.Username {
			continue
		}
		gvk := v1alpha1.GVK{
			Group:   req.AdmissionRequest.Kind.Group,
			Version: req.AdmissionRequest.Kind.Version,
			Kind:    req.AdmissionRequest.Kind.Kind,
		}
		if gvk == trace.Kind {
			traceEnabled = true
			if strings.EqualFold(trace.Dump, "All") {
				dump = true
			}
		}
	}
	return traceEnabled, dump
}

func (h *webhookHandler) skipExcludedNamespace(req *admissionv1.AdmissionRequest, excludedProcess process.Process) (bool, error) {
	obj := &unstructured.Unstructured{}
	if _, _, err := deserializer.Decode(req.Object.Raw, nil, obj); err != nil {
		return false, err
	}

	isNamespaceExcluded, err := h.processExcluder.IsNamespaceExcluded(excludedProcess, obj)
	if err != nil {
		return false, err
	}

	return isNamespaceExcluded, err
}
