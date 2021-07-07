package gktest

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestPrinterGo_Print(t *testing.T) {
	// TODO(#1430): Not final. While this will eventually exactly match the output
	//  of go test, this is a first pass.

	testCases := []struct {
		name        string
		result      []SuiteResult
		want        string
		wantVerbose string
	}{
		{
			name:   "no suites",
			result: []SuiteResult{},
			want: `PASS
`,
			wantVerbose: `PASS
`,
		},
		{
			name: "empty suite",
			result: []SuiteResult{{
				Path: "tests.go",
			}},
			want: `ok	tests.go	0s
PASS
`,
			wantVerbose: `ok	tests.go	0s
PASS
`,
		},
		{
			name: "empty constraint test",
			result: []SuiteResult{{
				Path:    "tests.go",
				Runtime: Duration(330 * time.Millisecond),
				TestResults: []TestResult{{
					Name:    "forbid-labels",
					Runtime: Duration(330 * time.Millisecond),
				}},
			}},
			want: `ok	tests.go	0.33s
PASS
`,
			wantVerbose: `=== RUN   forbid-labels
--- PASS: forbid-labels	(0.33s)
ok	tests.go	0.33s
PASS
`,
		},
		{
			name: "constraint test",
			result: []SuiteResult{{
				Path:    "tests.go",
				Runtime: Duration(330 * time.Millisecond),
				TestResults: []TestResult{{
					Name:    "forbid-labels",
					Runtime: Duration(330 * time.Millisecond),
					CaseResults: []CaseResult{{
						Name:    "forbid-labels/with label",
						Runtime: Duration(100 * time.Millisecond),
					}, {
						Name:    "forbid-labels/without label",
						Runtime: Duration(230 * time.Millisecond),
					}},
				}},
			}},
			want: `ok	tests.go	0.33s
PASS
`,
			wantVerbose: `=== RUN   forbid-labels
    === RUN   forbid-labels/with label
    --- PASS: forbid-labels/with label	(0.1s)
    === RUN   forbid-labels/without label
    --- PASS: forbid-labels/without label	(0.23s)
--- PASS: forbid-labels	(0.33s)
ok	tests.go	0.33s
PASS
`,
		},
		{
			name: "constraint test failure",
			result: []SuiteResult{{
				Path:    "tests.go",
				Runtime: Duration(330 * time.Millisecond),
				TestResults: []TestResult{{
					Name:    "forbid-labels",
					Runtime: Duration(330 * time.Millisecond),
					CaseResults: []CaseResult{{
						Name:    "forbid-labels/with label",
						Runtime: Duration(100 * time.Millisecond),
					}, {
						Name:    "forbid-labels/without label",
						Error:   errors.New("got violation but want allow"),
						Runtime: Duration(230 * time.Millisecond),
					}},
				}},
			}},
			want: `    --- FAIL: forbid-labels/without label	(0.23s)
        got violation but want allow
--- FAIL: forbid-labels	(0.33s)
FAIL	tests.go	0.33s
FAIL
`,
			wantVerbose: `=== RUN   forbid-labels
    === RUN   forbid-labels/with label
    --- PASS: forbid-labels/with label	(0.1s)
    === RUN   forbid-labels/without label
    --- FAIL: forbid-labels/without label	(0.23s)
        got violation but want allow
--- FAIL: forbid-labels	(0.33s)
FAIL	tests.go	0.33s
FAIL
`,
		},
		{
			name: "multiple suites",
			result: []SuiteResult{{
				Path:    "tests.go",
				Runtime: Duration(330 * time.Millisecond),
				TestResults: []TestResult{{
					Name:    "forbid-labels",
					Runtime: Duration(330 * time.Millisecond),
					CaseResults: []CaseResult{{
						Name:    "forbid-labels/with label",
						Runtime: Duration(100 * time.Millisecond),
					}, {
						Name:    "forbid-labels/without label",
						Runtime: Duration(230 * time.Millisecond),
					}},
				}},
			}, {
				Path:    "tests-2.go",
				Runtime: Duration(400 * time.Millisecond),
				TestResults: []TestResult{{
					Name:    "require-labels",
					Runtime: Duration(400 * time.Millisecond),
					CaseResults: []CaseResult{{
						Name:    "require-labels/with label",
						Runtime: Duration(170 * time.Millisecond),
					}, {
						Name:    "require-labels/without label",
						Runtime: Duration(230 * time.Millisecond),
					}},
				}},
			}},
			want: `ok	tests.go	0.33s
ok	tests-2.go	0.4s
PASS
`,
			wantVerbose: `=== RUN   forbid-labels
    === RUN   forbid-labels/with label
    --- PASS: forbid-labels/with label	(0.1s)
    === RUN   forbid-labels/without label
    --- PASS: forbid-labels/without label	(0.23s)
--- PASS: forbid-labels	(0.33s)
ok	tests.go	0.33s
=== RUN   require-labels
    === RUN   require-labels/with label
    --- PASS: require-labels/with label	(0.17s)
    === RUN   require-labels/without label
    --- PASS: require-labels/without label	(0.23s)
--- PASS: require-labels	(0.4s)
ok	tests-2.go	0.4s
PASS
`,
		},
		{
			name: "multiple constraints",
			result: []SuiteResult{{
				Path:    "tests.go",
				Runtime: Duration(730 * time.Millisecond),
				TestResults: []TestResult{{
					Name:    "forbid-labels",
					Runtime: Duration(330 * time.Millisecond),
					CaseResults: []CaseResult{{
						Name:    "forbid-labels/with label",
						Runtime: Duration(100 * time.Millisecond),
					}, {
						Name:    "forbid-labels/without label",
						Runtime: Duration(230 * time.Millisecond),
					}},
				}, {
					Name:    "require-labels",
					Runtime: Duration(400 * time.Millisecond),
					CaseResults: []CaseResult{{
						Name:    "require-labels/with label",
						Runtime: Duration(170 * time.Millisecond),
					}, {
						Name:    "require-labels/without label",
						Runtime: Duration(230 * time.Millisecond),
					}},
				}},
			}},
			want: `ok	tests.go	0.73s
PASS
`,
			wantVerbose: `=== RUN   forbid-labels
    === RUN   forbid-labels/with label
    --- PASS: forbid-labels/with label	(0.1s)
    === RUN   forbid-labels/without label
    --- PASS: forbid-labels/without label	(0.23s)
--- PASS: forbid-labels	(0.33s)
=== RUN   require-labels
    === RUN   require-labels/with label
    --- PASS: require-labels/with label	(0.17s)
    === RUN   require-labels/without label
    --- PASS: require-labels/without label	(0.23s)
--- PASS: require-labels	(0.4s)
ok	tests.go	0.73s
PASS
`,
		},
	}

	printer := PrinterGo{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := &strings.Builder{}
			gotErr := printer.Print(w, tc.result, false)

			if gotErr != nil {
				t.Fatal(gotErr)
			}
			wantLines := strings.Split(tc.want, "\n")
			gotLines := strings.Split(w.String(), "\n")
			if diff := cmp.Diff(wantLines, gotLines); diff != "" {
				t.Error(diff)
			}
		})

		t.Run(tc.name+" verbose", func(t *testing.T) {
			w := &strings.Builder{}
			gotErr := printer.Print(w, tc.result, true)

			if gotErr != nil {
				t.Fatal(gotErr)
			}
			wantLines := strings.Split(tc.wantVerbose, "\n")
			gotLines := strings.Split(w.String(), "\n")
			if diff := cmp.Diff(wantLines, gotLines); diff != "" {
				t.Error(diff)
			}
		})
	}
}
