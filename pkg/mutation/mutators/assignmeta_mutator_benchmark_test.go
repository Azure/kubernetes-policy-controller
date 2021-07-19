package mutators

import (
	"testing"

	"github.com/open-policy-agent/gatekeeper/apis/mutations/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func assignMetadata(value interface{}, location string) *v1alpha1.AssignMetadata {
	result := &v1alpha1.AssignMetadata{
		Spec: v1alpha1.AssignMetadataSpec{
			Location: location,
			Parameters: v1alpha1.MetadataParameters{
				Assign: makeValue(value),
			},
		},
	}

	return result
}

func BenchmarkAssignMetadataMutator_Always(b *testing.B) {
	mutator, err := MutatorForAssignMetadata(assignMetadata("bar", "metadata.labels.foo"))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a fresh object each time as AssignMetadata.Mutate does nothing if
		// a label/annotation already exists. Thus, this test is for when we do
		// actually make the mutation.

		// The performance cost of instantiating the Unstructured is negligible
		// compared to the time to perform Mutate.
		obj := &unstructured.Unstructured{
			Object: make(map[string]interface{}),
		}
		_, _ = mutator.Mutate(obj)
	}
}

func BenchmarkAssignMetadataMutator_Never(b *testing.B) {
	mutator, err := MutatorForAssignMetadata(assignMetadata("bar", "metadata.labels.foo"))
	if err != nil {
		b.Fatal(err)
	}

	obj := &unstructured.Unstructured{
		Object: make(map[string]interface{}),
	}
	_, err = mutator.Mutate(obj)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use the same object each time as AssignMetadata.Mutate does nothing if
		// a label/annotation already exists. Thus, this test is for the case where
		// no mutation is necessary.
		_, _ = mutator.Mutate(obj)
	}
}
