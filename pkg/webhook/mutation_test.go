package webhook

import (
	"context"
	"reflect"
	"testing"

	configv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/config/v1alpha1"
	mutationsv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/mutations/v1alpha1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/match"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/mutators"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	atypes "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestWebhookAssign(t *testing.T) {
	sys := mutation.NewSystem()

	v := &mutationsv1alpha1.Assign{
		ObjectMeta: metav1.ObjectMeta{Name: "AddFoo"},
		Spec: mutationsv1alpha1.AssignSpec{
			ApplyTo:  []match.ApplyTo{{Groups: []string{""}, Versions: []string{"v1"}, Kinds: []string{"Pod"}}},
			Location: "spec.value",
			Parameters: mutationsv1alpha1.Parameters{
				Assign: runtime.RawExtension{Raw: []byte(`{"value": "foo"}`)},
			},
		},
	}
	if err := mutators.IsValidAssign(v); err != nil {
		t.Fatal(err)
	}

	m, err := mutators.MutatorForAssign(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := sys.Upsert(m); err != nil {
		t.Fatal(err)
	}

	h := &mutationHandler{
		webhookHandler: webhookHandler{
			injectedConfig: &configv1alpha1.Config{
				Spec: configv1alpha1.ConfigSpec{
					Validation: configv1alpha1.Validation{
						Traces: []configv1alpha1.Trace{},
					},
				},
			},
			client:          &nsGetter{},
			reader:          &nsGetter{},
			processExcluder: process.New(),
		},
		mutationSystem: sys,
		deserializer:   codecs.UniversalDeserializer(),
	}

	raw := []byte(`{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "acbd","namespace": "ns1"}}`)

	req := atypes.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Kind: metav1.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			},
			Object:    runtime.RawExtension{Raw: raw},
			Namespace: "ns1",
			Operation: admissionv1.Create,
		},
	}

	resp := h.Handle(context.Background(), req)

	expectedVal := []byte(`{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "acbd","namespace": "ns1"}, "spec": {"value": "foo"}}`)
	expected := admission.PatchResponseFromRaw(raw, expectedVal)

	if !reflect.DeepEqual(resp, expected) {
		t.Errorf("unexpected response: %+v\n\nexpected: %+v", resp, expected)
	}
}

func TestWebhookAssignMetadata(t *testing.T) {
	sys := mutation.NewSystem()

	v := &mutationsv1alpha1.AssignMetadata{
		ObjectMeta: metav1.ObjectMeta{Name: "AddFoo"},
		Spec: mutationsv1alpha1.AssignMetadataSpec{
			Location: "metadata.labels.foo",
			Parameters: mutationsv1alpha1.MetadataParameters{
				Assign: runtime.RawExtension{Raw: []byte(`{"value": "bar"}`)},
			},
		},
	}
	if err := mutators.IsValidAssignMetadata(v); err != nil {
		t.Fatal(err)
	}

	m, err := mutators.MutatorForAssignMetadata(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := sys.Upsert(m); err != nil {
		t.Fatal(err)
	}

	h := &mutationHandler{
		webhookHandler: webhookHandler{
			injectedConfig: &configv1alpha1.Config{
				Spec: configv1alpha1.ConfigSpec{
					Validation: configv1alpha1.Validation{
						Traces: []configv1alpha1.Trace{},
					},
				},
			},
			client:          &nsGetter{},
			reader:          &nsGetter{},
			processExcluder: process.New(),
		},
		mutationSystem: sys,
		deserializer:   codecs.UniversalDeserializer(),
	}

	raw := []byte(`{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "acbd","namespace": "ns1"}}`)

	req := atypes.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Kind: metav1.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			},
			Object:    runtime.RawExtension{Raw: raw},
			Namespace: "ns1",
			Operation: admissionv1.Create,
		},
	}

	resp := h.Handle(context.Background(), req)

	expectedVal := []byte(`{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "acbd", "namespace": "ns1", "labels": {"foo":"bar"}}}`)
	expected := admission.PatchResponseFromRaw(raw, expectedVal)

	if !reflect.DeepEqual(resp, expected) {
		t.Errorf("unexpected response: %+v\n\nexpected: %+v", resp, expected)
	}
}
