package tester

import (
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/open-policy-agent/gatekeeper/pkg/mutation/path/parser"
)

// Condition describes whether the path either MustExist or MustNotExist in the original object
// +kubebuilder:validation:Enum=MustExist;MustNotExist
type Condition string

const (
	// MustExist means that an object must exist at the given path entry.
	MustExist = Condition("MustExist")
	// MustNotExist means that an object must not exist at the given path entry.
	MustNotExist = Condition("MustNotExist")
)

var conditions = map[string]Condition{
	"MustExist":    MustExist,
	"MustNotExist": MustNotExist,
}

// Base errors for validating path tests.
var (
	ErrPrefix   = errors.New("all subpaths must be a prefix of the `location` value of the mutation")
	ErrConflict = errors.New("conflicting path test conditions")
)

// StringToCondition translates a user-provided string into a Test Condition.
func StringToCondition(s string) (Condition, error) {
	cond, ok := conditions[s]
	if !ok {
		return Condition(""), fmt.Errorf("%s is not a valid path test condition", s)
	}

	return cond, nil
}

// Test describes a condition that the object must satisfy.
type Test struct {
	SubPath   parser.Path
	Condition Condition
}

func isPrefix(short, long parser.Path) bool {
	if len(short.Nodes) > len(long.Nodes) {
		return false
	}

	for i, entry := range short.Nodes {
		if reflect.DeepEqual(entry, long.Nodes[i]) {
			continue
		}
		return false
	}
	return true
}

// validatePathTests returns whether a set of path tests are valid against the provided location.
func validatePathTests(location parser.Path, pathTests []Test) error {
	for _, pathTest := range pathTests {
		if !isPrefix(pathTest.SubPath, location) {
			return fmt.Errorf("%w: subpath %q is not a prefix of location %q", ErrPrefix, pathTest.SubPath, location)
		}
	}
	return nil
}

// New creates a new Tester object.
func New(location parser.Path, tests []Test) (*Tester, error) {
	err := validatePathTests(location, tests)
	if err != nil {
		return nil, err
	}

	paths := make(map[int]*parser.Path)
	idx := &Tester{
		tests: make(map[int]Condition),
	}

	// Read in all tests before checking for conflicts.
	idxLowestMustNot := math.MaxInt32
	idxHighestMust := 0
	for i, test := range tests {
		j := len(test.SubPath.Nodes) - 1
		idx.tests[j] = test.Condition
		paths[j] = &tests[i].SubPath

		if test.Condition == MustNotExist && i < idxLowestMustNot {
			idxLowestMustNot = i
		} else if test.Condition == MustExist && i > idxHighestMust {
			idxHighestMust = i
		}
	}

	// Check for conflicts.
	for _, test := range tests {
		i := len(test.SubPath.Nodes) - 1
		// Check that a single node must not be marked both MustExist and MustNotExist.
		v, ok := idx.tests[i]
		if ok && v != test.Condition {
			return nil, fmt.Errorf("%w: path %q is marked both MustExist and MustNotExist", ErrConflict, test.SubPath)
		}
	}

	// Check that child nodes are not required if they have a forbidden parent.
	if idxHighestMust > idxLowestMustNot {
		return nil, fmt.Errorf("%w: path %q is marked MustExist but parent %q MustNotExist",
			ErrConflict, paths[idxHighestMust], paths[idxLowestMustNot])
	}
	return idx, nil
}

// Tester knows whether it's okay that an object exists at a given path depth.
type Tester struct {
	tests map[int]Condition
}

// ExistsOkay returns true if it's okay that an object exists.
func (pt *Tester) ExistsOkay(depth int) bool {
	c, ok := pt.tests[depth]
	if !ok {
		return true
	}
	return c == MustExist
}

// MissingOkay returns true if it's okay that an object is missing.
func (pt *Tester) MissingOkay(depth int) bool {
	c, ok := pt.tests[depth]
	if !ok {
		return true
	}
	return c == MustNotExist
}

// DeepCopy returns a deep copy of the tester.
func (pt *Tester) DeepCopy() *Tester {
	if pt == nil {
		return nil
	}
	t := &Tester{}
	t.tests = make(map[int]Condition, len(pt.tests))
	for k, v := range pt.tests {
		t.tests[k] = v
	}
	return t
}
