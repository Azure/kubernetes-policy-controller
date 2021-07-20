package gktest

import (
	"context"
	"fmt"
	"io/fs"
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

// RunCase executes a Case and returns the result of the run.
func (r *Runner) runCase(ctx context.Context, client Client, c Case) CaseResult {
	return CaseResult{}
}
