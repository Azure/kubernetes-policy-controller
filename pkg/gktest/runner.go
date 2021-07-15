package gktest

import (
	"context"
	"fmt"
	"io/fs"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Runner defines logic independent of how tests are run and the results are
// printed.
type Runner struct {
	// FS is the filesystem the Runner interacts with to read Suites and objects.
	FS fs.FS

	// NewClient instantiates a Client for compiling Templates/Constraints, and
	// validating objects against them.
	NewClient func() (Client, error)

	// TODO: Add Printer.
}

// Run executes all Tests in the Suite and returns the results.
func (r *Runner) Run(ctx context.Context, filter Filter, s *Suite) SuiteResult {
	result := SuiteResult{
		TestResults: make([]TestResult, len(s.Tests)),
	}
	for i, t := range s.Tests {
		if filter.MatchesTest(t) {
			result.TestResults[i] = r.runTest(ctx, filter, t)
		}
	}
	return result
}

// runTest executes every Case in the Test. Returns the results for every Case.
func (r *Runner) runTest(ctx context.Context, filter Filter, t Test) TestResult {
	client, err := r.NewClient()
	if err != nil {
		return TestResult{Error: fmt.Errorf("%w: %v", ErrCreatingClient, err)}
	}

	if t.Template == "" {
		return TestResult{Error: fmt.Errorf("%w: missing template", ErrInvalidSuite)}
	}
	template, err := readTemplate(r.FS, t.Template)
	if err != nil {
		return TestResult{Error: err}
	}
	_, err = client.AddTemplate(ctx, template)
	if err != nil {
		return TestResult{Error: fmt.Errorf("%w: %v", ErrAddingTemplate, err)}
	}

	if t.Constraint == "" {
		return TestResult{Error: fmt.Errorf("%w: missing constraint", ErrInvalidSuite)}
	}
	cObj, err := readConstraint(r.FS, t.Constraint)
	if err != nil {
		return TestResult{Error: err}
	}
	_, err = client.AddConstraint(ctx, cObj)
	if err != nil {
		return TestResult{Error: fmt.Errorf("%w: %v", ErrAddingConstraint, err)}
	}

	results := make([]CaseResult, len(t.Cases))
	for i, c := range t.Cases {
		if !filter.MatchesCase(c) {
			continue
		}

		results[i] = r.runCase(ctx, client, c)
	}
	return TestResult{CaseResults: results}
}

func readCase(f fs.FS, path string) (*unstructured.Unstructured, error) {
	bytes, err := fs.ReadFile(f, path)
	if err != nil {
		return nil, err
	}

	return readUnstructured(bytes)
}

func runAllow(ctx context.Context, client Client, f fs.FS, path string) CaseResult {
	u, err := readCase(f, path)
	if err != nil {
		return CaseResult{Error: err}
	}

	result, err := client.Review(ctx, u)
	if err != nil {
		return CaseResult{Error: err}
	}

	results := result.Results()
	if len(results) > 0 {
		return CaseResult{Error: fmt.Errorf("%w: %v", ErrUnexpectedDeny, results)}
	}
	return CaseResult{}
}

func runDeny(ctx context.Context, client Client, f fs.FS, path string, assertions []Assertion) CaseResult {
	u, err := readCase(f, path)
	if err != nil {
		return CaseResult{Error: err}
	}

	result, err := client.Review(ctx, u)
	if err != nil {
		return CaseResult{Error: err}
	}

	results := result.Results()
	if len(results) == 0 {
		return CaseResult{Error: ErrUnexpectedAllow}
	}

	return CaseResult{}
}

// RunCase executes a Case and returns the result of the run.
func (r *Runner) runCase(ctx context.Context, client Client, c Case) CaseResult {
	switch {
	case c.Allow != "" && c.Deny == "":
		return runAllow(ctx, client, r.FS, c.Allow)
	case c.Allow == "" && c.Deny != "":
		return runDeny(ctx, client, r.FS, c.Deny, c.Assertions)
	default:
		return CaseResult{Error: fmt.Errorf("%w: must define exactly one of allow and deny", ErrInvalidCase)}
	}
}
