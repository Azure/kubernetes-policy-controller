package gktest

// Filter filters tests and cases to run.
type Filter struct{}

// NewFilter parses run into a Filter for selecting constraint tests and
// individual cases to run.
//
// Empty string results in a Filter which matches all tests and their cases.
//
// Examples:
// 1) NewFiler("require-foo-label//missing-label")
// Matches tests containing the string "require-foo-label"
// and cases containing the string "missing-label". So this would match all of the
// following:
// - Test: "require-foo-label", Case: "missing-label"
// - Test: "not-require-foo-label, Case: "not-missing-label"
// - Test: "require-foo-label-and-bar-annotation", Case: "missing-label-and-annotation"
//
// 2) NewFilter("missing-label")
// Matches cases which either have a name containing "missing-label" or which
// are in a test named "missing-label". Matches the following:
// - Test: "forbid-missing-label", Case: "with-foo-label"
// - Test: "required-labels", Case: "missing-label"
//
// 3) NewFilter("^require-foo-label$//")
// Matches tests which exactly match "require-foo-label". Matches the following:
// - Test: "require-foo-label", Case: "with-foo-label"
// - Test: "require-foo-label", Case: "no-labels"
//
// 4) NewFilter("//empty-object")
// Matches tests whose names contain the string "empty-object". Matches the
// following:
// - Test: "forbid-foo-label", Case: "empty-object"
// - Test: "forbid-foo-label", Case: "another-empty-object"
// - Test: "require-bar-annotation", Case: "empty-object".
func NewFilter(run string) (Filter, error) {
	return Filter{}, nil
}

// MatchesTest filters the set of tests to run by test name
// and the cases contained in the test. Returns true if the Test should be run.
//
// If a test regex was not specified but a case regex was, returns true if
// at least one case in `c` matches the case regex.
func (f Filter) MatchesTest(c Test) bool {
	return true
}

// MatchesCase filters Cases to run by name.
//
// Returns true if the case regex matches c.
func (f Filter) MatchesCase(c Case) bool {
	return true
}
