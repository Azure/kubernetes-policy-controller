package mutators

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/google/go-cmp/cmp"
	mutationsv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/mutations/v1alpha1"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/match"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/mutators/core"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/path/parser"
	patht "github.com/open-policy-agent/gatekeeper/pkg/mutation/path/tester"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/schema"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("mutation").WithValues(logging.Process, "mutation")

// AssignMutator is a mutator object built out of a
// Assign instance.
type AssignMutator struct {
	id     types.ID
	assign *mutationsv1alpha1.Assign
	path   parser.Path

	// bindings are the set of GVKs this Mutator applies to.
	bindings  []runtimeschema.GroupVersionKind
	tester    *patht.Tester
	valueTest *mutationsv1alpha1.AssignIf
}

// AssignMutator implements mutatorWithSchema.
var _ schema.MutatorWithSchema = &AssignMutator{}

func (m *AssignMutator) Matches(obj client.Object, ns *corev1.Namespace) bool {
	if !match.AppliesTo(m.assign.Spec.ApplyTo, obj) {
		return false
	}
	matches, err := match.Matches(&m.assign.Spec.Match, obj, ns)
	if err != nil {
		log.Error(err, "AssignMutator.Matches failed", "assign", m.assign.Name)
		return false
	}
	return matches
}

func (m *AssignMutator) Mutate(obj *unstructured.Unstructured) (bool, error) {
	return core.Mutate(m, m.tester, m.testValue, obj)
}

// valueTest returns true if it is okay for the mutation func to override the value.
func (m *AssignMutator) testValue(v interface{}, exists bool) bool {
	if len(m.valueTest.In) != 0 {
		ifInMatched := false
		if !exists {
			// a missing value cannot satisfy the "In" test
			return false
		}
		for _, obj := range m.valueTest.In {
			if cmp.Equal(v, obj) {
				ifInMatched = true
				break
			}
		}
		if !ifInMatched {
			return false
		}
	}

	if !exists {
		// a missing value cannot violate NotIn
		return true
	}

	for _, obj := range m.valueTest.NotIn {
		if cmp.Equal(v, obj) {
			return false
		}
	}
	return true
}

func (m *AssignMutator) ID() types.ID {
	return m.id
}

func (m *AssignMutator) SchemaBindings() []runtimeschema.GroupVersionKind {
	return m.bindings
}

func (m *AssignMutator) Value() (interface{}, error) {
	return types.UnmarshalValue(m.assign.Spec.Parameters.Assign.Raw)
}

func (m *AssignMutator) HasDiff(mutator types.Mutator) bool {
	toCheck, ok := mutator.(*AssignMutator)
	if !ok { // different types, different
		return true
	}

	if !cmp.Equal(toCheck.id, m.id) {
		return true
	}
	if !cmp.Equal(toCheck.path, m.path) {
		return true
	}
	if !cmp.Equal(toCheck.bindings, m.bindings) {
		return true
	}

	// any difference in spec may be enough
	if !cmp.Equal(toCheck.assign.Spec, m.assign.Spec) {
		return true
	}

	return false
}

func (m *AssignMutator) Path() parser.Path {
	return m.path
}

func (m *AssignMutator) DeepCopy() types.Mutator {
	res := &AssignMutator{
		id:     m.id,
		assign: m.assign.DeepCopy(),
		path: parser.Path{
			Nodes: make([]parser.Node, len(m.path.Nodes)),
		},
		bindings: make([]runtimeschema.GroupVersionKind, len(m.bindings)),
	}
	copy(res.path.Nodes, m.path.Nodes)
	copy(res.bindings, m.bindings)
	res.tester = m.tester.DeepCopy()
	res.valueTest = m.valueTest.DeepCopy()
	return res
}

func (m *AssignMutator) String() string {
	return fmt.Sprintf("%s/%s/%s:%d", m.id.Kind, m.id.Namespace, m.id.Name, m.assign.GetGeneration())
}

// MutatorForAssign returns an AssignMutator built from
// the given assign instance.
func MutatorForAssign(assign *mutationsv1alpha1.Assign) (*AssignMutator, error) {
	path, err := parser.Parse(assign.Spec.Location)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid location format `%s` for Assign %s", assign.Spec.Location, assign.GetName())
	}

	if hasMetadataRoot(path) {
		return nil, fmt.Errorf("assign %s can't change metadata", assign.GetName())
	}

	err = checkKeyNotChanged(path, assign.GetName())
	if err != nil {
		return nil, err
	}

	toAssign := make(map[string]interface{})
	err = json.Unmarshal(assign.Spec.Parameters.Assign.Raw, &toAssign)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid format for parameters.assign %s for Assign %s", assign.Spec.Parameters.Assign.Raw, assign.GetName())
	}

	value, ok := toAssign["value"]
	if !ok {
		return nil, fmt.Errorf("spec.parameters.assign for Assign %s must have a value field", assign.GetName())
	}

	err = validateObjectAssignedToList(path, value, assign.GetName())
	if err != nil {
		return nil, err
	}

	id := types.MakeID(assign)

	pathTests, err := gatherPathTests(assign)
	if err != nil {
		return nil, err
	}
	tester, err := patht.New(path, pathTests)
	if err != nil {
		return nil, err
	}
	valueTests, err := assign.ValueTests()
	if err != nil {
		return nil, err
	}
	for _, applyTo := range assign.Spec.ApplyTo {
		if len(applyTo.Groups) == 0 || len(applyTo.Versions) == 0 || len(applyTo.Kinds) == 0 {
			return nil, fmt.Errorf("invalid applyTo for Assign mutator %s, all of group, version and kind must be specified", assign.GetName())
		}
	}

	gvks := getSortedGVKs(assign.Spec.ApplyTo)
	if len(gvks) == 0 {
		return nil, fmt.Errorf("applyTo required for Assign mutator %s", assign.GetName())
	}

	return &AssignMutator{
		id:        id,
		assign:    assign.DeepCopy(),
		bindings:  gvks,
		path:      path,
		tester:    tester,
		valueTest: &valueTests,
	}, nil
}

func gatherPathTests(assign *mutationsv1alpha1.Assign) ([]patht.Test, error) {
	pts := assign.Spec.Parameters.PathTests
	var pathTests []patht.Test
	for _, pt := range pts {
		p, err := parser.Parse(pt.SubPath)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("problem parsing sub path `%s` for Assign %s", pt.SubPath, assign.GetName()))
		}
		pathTests = append(pathTests, patht.Test{SubPath: p, Condition: pt.Condition})
	}
	return pathTests, nil
}

// IsValidAssign returns an error if the given assign object is not
// semantically valid.
func IsValidAssign(assign *mutationsv1alpha1.Assign) error {
	if _, err := MutatorForAssign(assign); err != nil {
		return err
	}
	return nil
}

func hasMetadataRoot(path parser.Path) bool {
	if len(path.Nodes) == 0 {
		return false
	}

	if reflect.DeepEqual(path.Nodes[0], &parser.Object{Reference: "metadata"}) {
		return true
	}
	return false
}

// checkKeyNotChanged does not allow to change the key field of
// a list element. A path like foo[name: bar].name is rejected.
func checkKeyNotChanged(p parser.Path, assignName string) error {
	if len(p.Nodes) == 0 {
		return errors.New("empty path")
	}
	if len(p.Nodes) < 2 {
		return nil
	}
	lastNode := p.Nodes[len(p.Nodes)-1]
	secondLastNode := p.Nodes[len(p.Nodes)-2]

	if secondLastNode.Type() != parser.ListNode {
		return nil
	}
	if lastNode.Type() != parser.ObjectNode {
		return fmt.Errorf("invalid path format in Assign %s: child of a list can't be a list", assignName)
	}
	addedObject, ok := lastNode.(*parser.Object)
	if !ok {
		return errors.New("failed converting an ObjectNodeType to Object in Assign " + assignName)
	}
	listNode, ok := secondLastNode.(*parser.List)
	if !ok {
		return errors.New("failed converting a ListNodeType to List in Assign " + assignName)
	}

	if addedObject.Reference == listNode.KeyField {
		return fmt.Errorf("invalid path format in Assign %s: changing the item key is not allowed", assignName)
	}
	return nil
}

func validateObjectAssignedToList(p parser.Path, value interface{}, assignName string) error {
	if len(p.Nodes) == 0 {
		return errors.New("empty path")
	}
	if p.Nodes[len(p.Nodes)-1].Type() != parser.ListNode {
		return nil
	}
	listNode, ok := p.Nodes[len(p.Nodes)-1].(*parser.List)
	if !ok {
		return errors.New("failed converting a ListNodeType to List, Assign: " + assignName)
	}
	if listNode.Glob {
		return errors.New("can't append to a globbed list, Assign: " + assignName)
	}
	if listNode.KeyValue == nil {
		return errors.New("invalid key value for a non globbed object, Assign: " + assignName)
	}
	valueMap, ok := value.(map[string]interface{})
	if !ok {
		return errors.New("only full objects can be appended to lists, Assign: " + assignName)
	}
	if listNode.KeyValue != valueMap[listNode.KeyField] {
		return fmt.Errorf("adding object to list with different key %s: list key %v, object key %v, assign: %s", listNode.KeyField, listNode.KeyValue, valueMap[listNode.KeyField], assignName)
	}

	return nil
}

func getSortedGVKs(bindings []match.ApplyTo) []runtimeschema.GroupVersionKind {
	// deduplicate GVKs
	gvksMap := map[runtimeschema.GroupVersionKind]struct{}{}
	for _, binding := range bindings {
		for _, gvk := range binding.Flatten() {
			gvksMap[gvk] = struct{}{}
		}
	}

	var gvks []runtimeschema.GroupVersionKind
	for gvk := range gvksMap {
		gvks = append(gvks, gvk)
	}

	// we iterate over the map in a stable order so that
	// unit tests won't be flaky.
	sort.Slice(gvks, func(i, j int) bool { return gvks[i].String() < gvks[j].String() })

	return gvks
}
