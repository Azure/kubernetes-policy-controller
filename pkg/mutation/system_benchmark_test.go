package mutation

import (
	"encoding/json"
	"testing"

	"github.com/open-policy-agent/gatekeeper/apis/mutations/v1alpha1"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/match"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/mutators"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func makeValue(v interface{}) runtime.RawExtension {
	v2 := map[string]interface{}{
		"value": v,
	}
	j, err := json.Marshal(v2)
	if err != nil {
		panic(err)
	}
	return runtime.RawExtension{Raw: j}
}

func assign(value interface{}, location string) *v1alpha1.Assign {
	result := &v1alpha1.Assign{
		Spec: v1alpha1.AssignSpec{
			ApplyTo: []match.ApplyTo{{
				Groups:   []string{"*"},
				Versions: []string{"*"},
				Kinds:    []string{"*"},
			}},
			Location: location,
			Parameters: v1alpha1.Parameters{
				Assign: makeValue(value),
			},
		},
	}

	return result
}

func BenchmarkSystem_Mutate(b *testing.B) {
	s := NewSystem()

	a := assign("", "spec")
	m, err := mutators.MutatorForAssign(a)
	if err != nil {
		b.Fatal(err)
	}

	err = s.Upsert(m)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		u := &unstructured.Unstructured{}

		_, _ = s.Mutate(u, nil)
	}
}
