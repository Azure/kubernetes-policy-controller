package mutation

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/path/parser"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Leveraging existing resource types to create custom mutators in order to validate
// the cache.
type fakeMutator struct {
	MID           types.ID
	MPath         parser.Path // relevant for comparison
	GVKs          []schema.GroupVersionKind
	Labels        map[string]string
	MutationCount int
	UnstableFor   int // makes the mutation unstable for the first n mutations
}

func (m *fakeMutator) Matches(obj client.Object, ns *corev1.Namespace) bool {
	return true // always matches
}

func (m *fakeMutator) Mutate(obj *unstructured.Unstructured) (bool, error) {
	if m.Labels == nil {
		return false, nil
	}
	m.MutationCount++

	current := obj.GetLabels()
	if current == nil {
		current = make(map[string]string)
	}

	for k, v := range m.Labels {
		if m.MutationCount < m.UnstableFor { // means we need to make the mutation unstable, adding the count
			v = fmt.Sprintf("%s%d", v, m.MutationCount)
		}
		current[k] = v
	}
	obj.SetLabels(current)

	return true, nil
}

func (m *fakeMutator) ID() types.ID {
	return m.MID
}

func (m *fakeMutator) Path() parser.Path {
	return m.MPath
}

func (m *fakeMutator) Value() (interface{}, error) {
	return nil, nil
}

func (m *fakeMutator) HasDiff(mutator types.Mutator) bool {
	return !cmp.Equal(m, mutator, cmpopts.EquateEmpty())
}

func (m *fakeMutator) DeepCopy() types.Mutator {
	res := &fakeMutator{
		MID:           m.MID,
		MPath:         m.MPath.DeepCopy(),
		GVKs:          make([]schema.GroupVersionKind, len(m.GVKs)),
		MutationCount: m.MutationCount,
		UnstableFor:   m.UnstableFor,
	}
	copy(res.GVKs, m.GVKs)

	if m.Labels != nil {
		if res.Labels == nil {
			res.Labels = make(map[string]string)
		}
		for k, v := range m.Labels {
			res.Labels[k] = v
		}
	}
	return res
}

func (m *fakeMutator) String() string {
	return ""
}

func (m *fakeMutator) SchemaBindings() []schema.GroupVersionKind {
	return m.GVKs
}

var initMutators = []types.Mutator{
	&fakeMutator{MID: types.ID{Group: "bbb", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
	&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "ddd"}},
	&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "aaa", Name: "aaa"}},
	&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "ccc", Name: "ddd"}},
	&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
	&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "aaa"}},
}

func TestSorting(t *testing.T) {
	table := []struct {
		tname    string
		initial  []types.Mutator
		expected []types.Mutator
		action   func(*System) error
	}{
		{
			tname:   "testsort",
			initial: initMutators,
			expected: []types.Mutator{
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "ccc", Name: "ddd"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "aaa", Name: "aaa"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "aaa"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "ddd"}},
				&fakeMutator{MID: types.ID{Group: "bbb", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
			},
			action: func(s *System) error { return nil },
		},
		{
			tname:   "testremove",
			initial: initMutators,
			expected: []types.Mutator{
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "ccc", Name: "ddd"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "aaa", Name: "aaa"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "ddd"}},
				&fakeMutator{MID: types.ID{Group: "bbb", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
			},
			action: func(s *System) error {
				return s.Remove(types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "aaa"})
			},
		},
		{
			tname:   "testaddingsame",
			initial: initMutators,
			expected: []types.Mutator{
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "ccc", Name: "ddd"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "aaa", Name: "aaa"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "aaa"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "ddd"}},
				&fakeMutator{MID: types.ID{Group: "bbb", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
			},
			action: func(s *System) error {
				return s.Upsert(&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "aaa"}})
			},
		},
		{
			tname:   "testaddingdifferent",
			initial: initMutators,
			expected: []types.Mutator{
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "ccc", Name: "ddd"}},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "aaa", Name: "aaa"}},
				&fakeMutator{
					MID:   types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "aaa"},
					MPath: mustParse("relevantvalue"), GVKs: []schema.GroupVersionKind{{Kind: "foo"}},
				},
				&fakeMutator{MID: types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "ddd"}},
				&fakeMutator{MID: types.ID{Group: "bbb", Kind: "aaa", Namespace: "aaa", Name: "aaa"}},
			},
			action: func(s *System) error {
				return s.Upsert(&fakeMutator{
					MID:   types.ID{Group: "aaa", Kind: "bbb", Namespace: "ccc", Name: "aaa"},
					MPath: mustParse("relevantvalue"), GVKs: []schema.GroupVersionKind{{Kind: "foo"}},
				})
			},
		},
	}

	for _, tc := range table {
		t.Run(tc.tname, func(t *testing.T) {
			c := NewSystem()
			for i, m := range tc.initial {
				err := c.Upsert(m)
				if err != nil {
					t.Errorf("%s: Failed inserting %dth object", tc.tname, i)
				}
			}
			err := tc.action(c)
			if err != nil {
				t.Errorf("%s: test action failed %v", tc.tname, err)
			}
			if len(c.orderedMutators) != len(tc.expected) {
				t.Errorf("%s: Expected %d object from the operator, found %d", tc.tname, len(c.orderedMutators), len(tc.expected))
			}

			if diff := cmp.Diff(tc.expected, c.orderedMutators, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s: Cache content is not consistent: %s", tc.tname, diff)
			}

			expectedMap := make(map[types.ID]types.Mutator)
			for _, m := range tc.expected {
				expectedMap[m.ID()] = m
			}
			if diff := cmp.Diff(expectedMap, c.mutatorsMap, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s: Cache content (map) is not consistent: %s", tc.tname, diff)
			}
		})
	}
}

func TestMutation(t *testing.T) {
	table := []struct {
		tname              string
		mutations          []*fakeMutator
		expectedLabels     map[string]string
		expectedIterations int
		expectError        bool
	}{
		{
			tname: "mutate",
			mutations: []*fakeMutator{
				{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "aaa"}, Labels: map[string]string{
					"ka": "va",
				}},
				{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "bbb"}, Labels: map[string]string{
					"kb": "vb",
				}},
			},
			expectedLabels: map[string]string{
				"ka": "va",
				"kb": "vb",
			},
			expectedIterations: 2,
		},
		{
			tname: "neverconverge",
			mutations: []*fakeMutator{
				{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "aaa"}, Labels: map[string]string{
					"ka": "va",
				}, UnstableFor: 5},
				{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "bbb"}, Labels: map[string]string{
					"kb": "vb",
				}},
			},
			expectError: true,
		},
		{
			tname: "convergeafter3",
			mutations: []*fakeMutator{
				{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "aaa"}, Labels: map[string]string{
					"ka": "va",
				}, UnstableFor: 3},
				{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "bbb"}, Labels: map[string]string{
					"kb": "vb",
				}},
				{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "ccc"}, Labels: map[string]string{
					"kb": "vb",
				}},
				{MID: types.ID{Group: "aaa", Kind: "aaa", Namespace: "aaa", Name: "ddd"}, Labels: map[string]string{
					"kb": "vb",
				}},
			},
			expectedLabels: map[string]string{
				"ka": "va",
				"kb": "vb",
			},
			expectedIterations: 4,
		},
	}
	for _, tc := range table {
		t.Run(tc.tname, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testpod",
					Namespace: "foo",
				},
			}

			converted, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
			if err != nil {
				t.Fatal(tc.tname, "Convert pod to unstructured failed")
			}
			toMutate := &unstructured.Unstructured{Object: converted}

			c := NewSystem()
			for i, m := range tc.mutations {
				err := c.Upsert(m)
				if err != nil {
					t.Errorf(tc.tname, "Failed inserting %dth object", i)
				}
			}
			mutated, err := c.Mutate(toMutate, nil)
			if tc.expectError && err == nil {
				t.Fatal(tc.tname, "Expecting error from mutate, did not fail")
			}

			if tc.expectError { // if error is expected, don't do additional checks
				return
			}

			if err != nil {
				t.Fatal(tc.tname, "Mutate failed unexpectedly", err)
			}

			newLabels := toMutate.GetLabels()

			if !mutated {
				t.Error(tc.tname, "Mutation not as expected", cmp.Diff(tc.expectedLabels, newLabels))
			}

			if diff := cmp.Diff(tc.expectedLabels, newLabels); diff != "" {
				t.Error(tc.tname, "Mutation not as expected", diff)
			}

			probe, ok := c.orderedMutators[0].(*fakeMutator) // fetching a mock mutator to check the number of iterations
			if !ok {
				t.Fatalf("mutator type %T, want %T", c.orderedMutators[0], &fakeMutator{})
			}
			if probe.MutationCount != tc.expectedIterations {
				t.Error(tc.tname, "Expected %d  mutation iterations, got", tc.expectedIterations, tc.mutations[0].MutationCount)
			}
		})
	}
}

func mustParse(s string) parser.Path {
	p, err := parser.Parse(s)
	if err != nil {
		panic(err)
	}
	return p
}

func TestSystem_DontApplyConflictingMutations(t *testing.T) {
	// Two conflicting mutators.
	foo := &fakeMutator{
		MID:    types.ID{Name: "foo"},
		MPath:  mustParse("spec.containers[name: foo].image"),
		GVKs:   []schema.GroupVersionKind{{Version: "v1", Kind: "Pod"}},
		Labels: map[string]string{"active": "true"},
	}
	fooConflict := &fakeMutator{
		MID:    types.ID{Name: "foo-conflict"},
		MPath:  mustParse("spec.containers.image"),
		GVKs:   []schema.GroupVersionKind{{Version: "v1", Kind: "Pod"}},
		Labels: map[string]string{"active": "true"},
	}

	s := NewSystem()
	err := s.Upsert(foo)
	if err != nil {
		t.Fatalf("got Upsert() error = %v, want <nil>", err)
	}

	// We can mutate objects before System is put in an inconsistent state.
	t.Run("mutate works on consistent state", func(t *testing.T) {
		u := &unstructured.Unstructured{}
		gotMutated, gotErr := s.Mutate(u, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "billing"}})
		if !gotMutated {
			t.Errorf("got Mutate() = %t, want true", gotMutated)
		}
		if gotErr != nil {
			t.Fatalf("got Mutate() error = %v, want <nil>", gotErr)
		}
	})

	// Put System in an inconsistent state.
	err = s.Upsert(fooConflict)
	if err == nil {
		t.Fatalf("got Upsert() error = %v, want error", err)
	}

	// Since foo and foo-conflict define conflicting schemas, neither is executed.
	// TODO(willbeason): Fix once System is updated to properly report conflicts (#1216).
	//  Should be "no mutation on inconsistent state".
	t.Run("mutation on inconsistent state", func(t *testing.T) {
		u2 := &unstructured.Unstructured{}
		gotMutated, gotErr := s.Mutate(u2, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "billing"}})
		if !gotMutated {
			t.Errorf("got Mutate() = %t, want true", gotMutated)
		}
		// if gotMutated {
		// 	t.Errorf("got Mutate() = %t, want false", gotMutated)
		// }
		if gotErr != nil {
			t.Fatalf("got Mutate() error = %v, want <nil>", gotErr)
		}
	})

	// Get the system back to a consistent state.
	err = s.Remove(types.ID{Name: "foo-conflict"})
	if err != nil {
		t.Fatalf("got Remove() error = %v, want <nil>", err)
	}

	// Mutations are performed again.
	t.Run("mutations performed after conflict removed", func(t *testing.T) {
		u3 := &unstructured.Unstructured{}
		gotMutated, gotErr := s.Mutate(u3, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "billing"}})
		if !gotMutated {
			t.Errorf("got Mutate() = %t, want true", gotMutated)
		}
		if gotErr != nil {
			t.Fatalf("got Mutate() error = %v, want <nil>", gotErr)
		}
	})
}

func TestSystem_DontApplyConflictingMutationsRemoveOriginal(t *testing.T) {
	// Two conflicting mutators.
	foo := &fakeMutator{
		MID:    types.ID{Name: "foo"},
		MPath:  mustParse("spec.containers[name: foo].image"),
		GVKs:   []schema.GroupVersionKind{{Version: "v1", Kind: "Pod"}},
		Labels: map[string]string{"active": "true"},
	}
	fooConflict := &fakeMutator{
		MID:    types.ID{Name: "foo-conflict"},
		MPath:  mustParse("spec.containers.image"),
		GVKs:   []schema.GroupVersionKind{{Version: "v1", Kind: "Pod"}},
		Labels: map[string]string{"active": "true"},
	}

	// Put System in an inconsistent state.
	s := NewSystem()
	err := s.Upsert(foo)
	if err != nil {
		t.Fatalf("got Upsert() error = %v, want <nil>", err)
	}
	err = s.Upsert(fooConflict)
	if err == nil {
		t.Fatalf("got Upsert() error = %v, want error", err)
	}
	gotErr := s.Remove(types.ID{Name: "foo"})
	if gotErr != nil {
		t.Fatalf("got Remove() error = %v, want <nil>", gotErr)
	}

	u := &unstructured.Unstructured{}
	gotMutated, gotErr := s.Mutate(u, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "billing"}})
	if gotMutated {
		t.Errorf("got Mutate() = %t, want false", gotMutated)
	}
	if gotErr != nil {
		t.Fatalf("got Mutate() error = %v, want <nil>", gotErr)
	}
}

func id(name string) types.ID {
	return types.ID{Name: name}
}

func TestSystem_EarliestConflictingMutatorWins(t *testing.T) {
	// Two conflicting mutators.
	foo := &fakeMutator{
		MID:    id("foo"),
		MPath:  mustParse("spec.containers[name: foo].image"),
		GVKs:   []schema.GroupVersionKind{{Version: "v1", Kind: "Pod"}},
		Labels: map[string]string{"active": "true"},
	}
	fooConflict := &fakeMutator{
		MID:    id("foo-conflict"),
		MPath:  mustParse("spec.containers.image"),
		GVKs:   []schema.GroupVersionKind{{Version: "v1", Kind: "Pod"}},
		Labels: map[string]string{"active": "true"},
	}
	// A non-conflicting mutator on the same type.
	bar := &fakeMutator{
		MID:    id("bar"),
		MPath:  mustParse("spec.images[name: nginx].tag"),
		GVKs:   []schema.GroupVersionKind{{Version: "v1", Kind: "Pod"}},
		Labels: map[string]string{"active": "true"},
	}

	// Put System in an inconsistent state.
	s := NewSystem()
	err := s.Upsert(foo)
	if err != nil {
		t.Fatalf("got Upsert() error = %v, want <nil>", err)
	}
	err = s.Upsert(fooConflict)
	if err == nil {
		t.Fatalf("got Upsert() error = %v, want error", err)
	}
	err = s.Upsert(bar)
	if err != nil {
		t.Fatalf("got Upsert() error = %v, want <nil>", err)
	}

	u := &unstructured.Unstructured{}
	gotMutated, gotErr := s.Mutate(u, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "billing"}})
	if !gotMutated {
		t.Errorf("got Mutate() = %t, want true", gotMutated)
	}
	if gotErr != nil {
		t.Fatalf("got Mutate() error = %v, want <nil>", gotErr)
	}
	if s.Get(id("foo")).(*fakeMutator).MutationCount != 2 {
		t.Errorf("got foo.MutationCount == %d, want 2", foo.MutationCount)
	}
	if s.Get(id("foo-conflict")) != nil {
		t.Errorf("got fooConflict.MutationCount == %d, want 0", fooConflict.MutationCount)
	}
	if s.Get(id("bar")).(*fakeMutator).MutationCount != 2 {
		t.Errorf("got bar.MutationCount == %d, want 2", bar.MutationCount)
	}
}
