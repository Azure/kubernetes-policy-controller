package core_test

import (
	"encoding/json"
	"fmt"
	"testing"

	mutationsv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/mutations/v1alpha1"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/match"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/mutators"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/mutators/testhelpers"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	TestValue          = "testValue"
	ParameterTestValue = "\"testValue\""
)

func prepareTestPod(t *testing.T) *unstructured.Unstructured {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testpod1",
			Namespace: "foo",
			Labels:    map[string]string{"a": "b"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "testname1",
					Image: "image",
					Ports: []corev1.ContainerPort{{Name: "portName1"}},
				},
				{
					Name:  "testname2",
					Image: "image",
					Ports: []corev1.ContainerPort{
						{Name: "portName2A"},
						{Name: "portName2B"},
					},
				},
				{
					Name:  "testname3",
					Image: "image",
					Ports: []corev1.ContainerPort{{Name: "portName3"}},
				},
			},
		},
	}
	podObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		t.Errorf("Failed to convert pod to unstructured %v", err)
	}
	return &unstructured.Unstructured{Object: podObject}
}

func TestObjects(t *testing.T) {
	testFunc := func(unstr *unstructured.Unstructured) {
		element, found, err := unstructured.NestedString(unstr.Object, "metadata", "labels", "labelA")
		if err != nil {
			t.Error("Unexpected error", err)
		}
		if !found && element != TestValue {
			t.Errorf("Failed to update pod")
		}
	}
	if err := testAssignMetadataMutation(
		"metadata.labels.labelA",
		ParameterTestValue,
		prepareTestPod(t),
		testFunc,
		t,
	); err != nil {
		t.Errorf("Unexpected error: %+v", err)
	}
}

func TestObjectsAndLists(t *testing.T) {
	testFunc := func(u *unstructured.Unstructured) {
		containers, _, err := unstructured.NestedSlice(u.Object, "spec", "containers")
		if err != nil {
			t.Error("Unexpected error", err)
		}
		for _, container := range containers {
			containerAsMap, ok := container.(map[string]interface{})
			if !ok {
				t.Fatalf("got container type %T, want %T", container, map[string]interface{}{})
			}

			if containerAsMap["name"] == "testname2" {
				for _, port := range containerAsMap["ports"].([]interface{}) {
					portAsMap, ok := port.(map[string]interface{})
					if !ok {
						t.Fatalf("got port type %T, want %T", container, map[string]interface{}{})
					}

					if portAsMap["name"] == "portName2B" {
						if portAsMap["hostIP"] != TestValue {
							t.Errorf("Failed to update pod")
						}
					} else {
						if _, ok := port.(map[string]interface{})["hostIP"]; ok {
							t.Errorf("Unexpected pod was updated")
						}
					}
				}
			} else {
				for _, port := range container.(map[string]interface{})["ports"].([]interface{}) {
					if _, ok := port.(map[string]interface{})["hostIP"]; ok {
						t.Errorf("Unexpected pod was updated")
					}
				}
			}
		}
	}

	if err := testAssignMutation(
		"", "v1", "Pod",
		`spec.containers["name": "testname2"].ports["name": "portName2B"].hostIP`,
		ParameterTestValue,
		prepareTestPod(t),
		testFunc,
		t,
	); err != nil {
		t.Errorf("Unexpected error: %+v", err)
	}
}

func TestListsAsLastElementWithStringValue(t *testing.T) {
	testFunc := func(u *unstructured.Unstructured) {}

	if err := testDummyMutation(
		`spec.containers["name": "notExists"]`,
		ParameterTestValue,
		prepareTestPod(t),
		testFunc,
		t,
	); err == nil {
		t.Errorf("List path entry in last position should accept a string value")
	}
}

func TestListsAsLastElement(t *testing.T) {
	testFunc := func(u *unstructured.Unstructured) {
		containers, _, err := unstructured.NestedFieldNoCopy(u.Object, "spec", "containers")
		if err != nil {
			t.Error("Unexpected error", err)
		}
		for _, container := range containers.([]interface{}) {
			if container.(map[string]interface{})["name"] == "notExists" {
				if container.(map[string]interface{})["foo"] == "foovalue" {
					return
				}
			}
		}
		t.Errorf("Failed to update pod. Container element was not added")
	}

	if err := testAssignMutation(
		"", "v1", "Pod",
		`spec.containers["name": "notExists"]`,
		"{\"name\": \"notExists\", \"foo\": \"foovalue\"}",
		prepareTestPod(t),
		testFunc,
		t,
	); err != nil {
		t.Errorf("Unexpected error: %+v", err)
	}
}

func TestListsAsLastElementAlreadyExists(t *testing.T) {
	testFunc := func(u *unstructured.Unstructured) {
		containers, _, err := unstructured.NestedSlice(u.Object, "spec", "containers")
		if err != nil {
			t.Error("Unexpected error", err)
		}
		for _, container := range containers {
			if container.(map[string]interface{})["name"] == "testname1" {
				if container.(map[string]interface{})["foo"] == "bar" {
					return
				}
			}
		}
		t.Errorf("Pod's container was not replaced as expected")
	}

	if err := testAssignMutation(
		"", "v1", "Pod",
		`spec.containers["name": "testname1"]`,
		"{\"name\": \"testname1\", \"foo\": \"bar\"}",
		prepareTestPod(t),
		testFunc,
		t,
	); err != nil {
		t.Errorf("Unexpected error: %+v", err)
	}
}

func TestGlobbedList(t *testing.T) {
	testFunc := func(u *unstructured.Unstructured) {
		containers, _, err := unstructured.NestedSlice(u.Object, "spec", "containers")
		if err != nil {
			t.Error("Unexpected error", err)
		}
		for _, container := range containers {
			containerAsMap, ok := container.(map[string]interface{})
			if !ok {
				t.Fatalf("got container type %T, want %T", container, map[string]interface{}{})
			}

			ports := containerAsMap["ports"]
			for _, port := range ports.([]interface{}) {
				if value, ok := port.(map[string]interface{})["protocol"]; !ok || value != TestValue {
					t.Errorf("Expected value was not updated")
				}
			}
		}
	}

	if err := testAssignMutation(
		"", "v1", "Pod",
		`spec.containers["name": *].ports["name": *].protocol`,
		ParameterTestValue,
		prepareTestPod(t),
		testFunc,
		t,
	); err != nil {
		t.Errorf("Unexpected error: %+v", err)
	}
}

func TestNonExistingPathEntry(t *testing.T) {
	testFunc := func(unstr *unstructured.Unstructured) {
		element, found, err := unstructured.NestedString(unstr.Object, "spec", "element", "should", "be", "added")
		if err != nil {
			t.Error("Unexpected error", err)
		}
		if !found && element != TestValue {
			t.Errorf("Failed to update pod")
		}
	}
	if err := testAssignMutation(
		"", "v1", "Pod",
		"spec.element.should.be.added",
		ParameterTestValue,
		prepareTestPod(t),
		testFunc,
		t,
	); err != nil {
		t.Errorf("Unexpected error: %+v", err)
	}
}

func TestNonExistingListPathEntry(t *testing.T) {
	testFunc := func(u *unstructured.Unstructured) {
		element, found, err := unstructured.NestedSlice(u.Object, "spec", "element")
		if err != nil {
			t.Error("Unexpected error", err)
		}
		if !found {
			t.Fatal("resource not found")
		}

		element0, ok := element[0].(map[string]interface{})
		if !ok {
			t.Fatalf("got spec.element[0] type %T, want %T", element[0], map[string]interface{}{})
		}

		element2, ok := element0["element2"].(map[string]interface{})
		if !ok {
			t.Fatalf("got spec.element[0].element2 type %T, want %T", element0["element2"], map[string]interface{}{})
		}
		if element2["added"] != TestValue {
			t.Errorf("Failed to update pod")
		}
	}
	if err := testAssignMutation(
		"", "v1", "Pod",
		`spec.element["name": "value"].element2.added`,
		ParameterTestValue,
		prepareTestPod(t),
		testFunc,
		t,
	); err != nil {
		t.Errorf("Unexpected error: %+v", err)
	}
}

func TestAssignMetadataDoesNotUpdateExistingLabel(t *testing.T) {
	testFunc := func(u *unstructured.Unstructured) {
		element, _, err := unstructured.NestedString(u.Object, "metadata", "labels", "a")
		if err != nil {
			t.Error("Unexpected error", err)
		}
		if element != "b" {
			t.Errorf("Value should not be updated")
		}
	}
	if err := testAssignMetadataMutation(
		`metadata.labels.a`,
		ParameterTestValue,
		prepareTestPod(t),
		testFunc,
		t,
	); err != nil {
		t.Errorf("Unexpected error: %+v", err)
	}
}

func TestAssignDoesNotMatchObjectStructure(t *testing.T) {
	if err := testAssignMutation("", "v1", "Pod", `spec.containers.ports.protocol`, ParameterTestValue, prepareTestPod(t), nil, t); err == nil {
		t.Errorf("Error should be returned for mismatched path and object structure")
	}
}

func TestListsAsLastElementAlreadyExistsWithKeyConflict(t *testing.T) {
	testFunc := func(u *unstructured.Unstructured) {}
	var v interface{}
	err := json.Unmarshal([]byte("{\"name\": \"conflictingName\", \"foo\": \"bar\"}"), &v)
	if err != nil {
		panic(err)
	}
	if err := testDummyMutation(
		`spec.containers["name": "testname1"]`,
		v,
		prepareTestPod(t),
		testFunc,
		t,
	); err == nil {
		t.Errorf("Expected error not raised. Conflicting name must not be applied.")
	} else if err.Error() != "key value of replaced object must not change" {
		t.Errorf("Incorrect error message: %s", err.Error())
	}
}

func testDummyMutation(
	location string,
	value interface{},
	unstructured *unstructured.Unstructured,
	testFunc func(*unstructured.Unstructured),
	t *testing.T) error {
	mutator := testhelpers.NewDummyMutator("dummy", location, value)
	return testMutation(mutator, unstructured, testFunc, t)
}

func testAssignMutation(
	group, version, kind string,
	location string,
	value string,
	unstructured *unstructured.Unstructured,
	testFunc func(*unstructured.Unstructured),
	t *testing.T) error {
	assign := mutationsv1alpha1.Assign{
		ObjectMeta: metav1.ObjectMeta{},
		Spec: mutationsv1alpha1.AssignSpec{
			ApplyTo:  []match.ApplyTo{{Groups: []string{group}, Versions: []string{version}, Kinds: []string{kind}}},
			Location: location,
			Parameters: mutationsv1alpha1.Parameters{
				Assign: runtime.RawExtension{
					Raw: []byte(fmt.Sprintf("{\"value\": %s}", value)),
				},
			},
		},
	}
	mutator, err := mutators.MutatorForAssign(&assign)
	if err != nil {
		t.Error("Unexpected error", err)
	}
	return testMutation(mutator, unstructured, testFunc, t)
}

func testAssignMetadataMutation(
	location string,
	value string,
	unstructured *unstructured.Unstructured,
	testFunc func(*unstructured.Unstructured),
	t *testing.T) error {
	assignMetadata := mutationsv1alpha1.AssignMetadata{
		ObjectMeta: metav1.ObjectMeta{},
		Spec: mutationsv1alpha1.AssignMetadataSpec{
			Location: location,
			Parameters: mutationsv1alpha1.MetadataParameters{
				Assign: runtime.RawExtension{
					Raw: []byte(fmt.Sprintf("{\"value\":%s}", value)),
				},
			},
		},
	}
	mutator, err := mutators.MutatorForAssignMetadata(&assignMetadata)
	if err != nil {
		t.Error("Unexpected error", err)
	}
	return testMutation(mutator, unstructured, testFunc, t)
}

func testMutation(mutator types.Mutator, unstructured *unstructured.Unstructured, testFunc func(*unstructured.Unstructured), t *testing.T) error {
	_, err := mutator.Mutate(unstructured)
	if err != nil {
		return err
	}
	testFunc(unstructured)
	return nil
}
