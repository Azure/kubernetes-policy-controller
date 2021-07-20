package test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/open-policy-agent/gatekeeper/pkg/gktest"
	"github.com/spf13/cobra"
)

const (
	examples = `  # Run all tests in label-tests.yaml
  gator test label-tests.yaml

  # Run all tests whose names contain "forbid-labels".
  gator test tests/... --run forbid-labels//

  # Run all cases whose names contain "nginx-deployment".
  gator test tests/... --run //nginx-deployment

  # Run all cases whose names exactly match "nginx-deployment".
  gator test tests/... --run '//^nginx-deployment$'

  # Run all cases that are either named "forbid-labels" or are
  # in tests named "forbid-labels".
  gator test tests/... --run '^forbid-labels$'`
)

var run string

func init() {
	Cmd.Flags().StringVarP(&run, "run", "r", "",
		`regular expression which filters tests to run by name`)
}

// Cmd is the gator test subcommand.
var Cmd = &cobra.Command{
	Use:     "test path [--run=name]",
	Short:   "test runs suites of tests on Gatekeeper Constraints",
	Example: examples,
	Args:    cobra.ExactArgs(1),
	RunE:    runE,
}

func runE(cmd *cobra.Command, args []string) error {
	path := args[0]

	// Convert path to be absolute. Allowing for relative and absolute paths
	// everywhere in the code leads to unnecessary complexity, so the first
	// thing we do on encountering a path is to convert it to an absolute path.
	var err error
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("getting absolute path: %w", err)
		}
	}

	// Create the base file system. We use fs.FS rather than direct calls to
	// os.ReadFile or filepath.WalkDir to make testing easier and keep logic
	// os-independent.
	fileSystem := getFS(path)

	recursive := false
	if strings.HasSuffix(path, "/...") {
		recursive = true
		path = strings.TrimSuffix(path, "...")
	}
	path = strings.TrimSuffix(path, "/")

	suites, err := gktest.ReadSuites(fileSystem, path, recursive)
	if err != nil {
		return fmt.Errorf("listing test files: %w", err)
	}
	filter, err := gktest.NewFilter(run)
	if err != nil {
		return fmt.Errorf("compiling filter: %w", err)
	}

	return runSuites(cmd.Context(), fileSystem, suites, filter)
}

func runSuites(ctx context.Context, fileSystem fs.FS, suites []gktest.Suite, filter gktest.Filter) error {
	isFailure := false

	runner := gktest.Runner{
		FS:        fileSystem,
		NewClient: gktest.NewOPAClient,
	}

	for i := range suites {
		s := &suites[i]

		suiteResult := runner.Run(ctx, filter, s)
		for _, testResult := range suiteResult.TestResults {
			if testResult.Error != nil {
				isFailure = true
			}
			for _, caseResult := range testResult.CaseResults {
				if caseResult.Error != nil {
					isFailure = true
				}
			}
		}
	}

	if isFailure {
		// At least one test failed or there was a problem executing tests in at
		// least one file.
		return errors.New("FAIL")
	}
	return nil
}

func getFS(path string) fs.FS {
	// TODO(#1397): Check that this produces the correct file system string on
	//  Windows. We may need to add a trailing `/` for fs.FS to function properly.
	root := filepath.VolumeName(path)
	if root == "" {
		// We are running on a unix-like filesystem without volume names, so the
		// file system root is `/`.
		root = "/"
	}

	return os.DirFS(root)
}
