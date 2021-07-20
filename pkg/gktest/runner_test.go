package gktest

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const (
	templateAlwaysValidate = `
kind: ConstraintTemplate
apiVersion: templates.gatekeeper.sh/v1beta1
metadata:
  name: alwaysvalidate
spec:
  crd:
    spec:
      names:
        kind: AlwaysValidate
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8sdisallowedtags
        violation {
          false
        }
`

	templateUnsupportedVersion = `
kind: ConstraintTemplate
apiVersion: templates.gatekeeper.sh/v1beta2
metadata:
  name: unsupportedversion
spec:
  crd:
    spec:
      names:
        kind: UnsupportedVersion
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8sdisallowedtags
        violation {
          false
        }
`

	templateInvalidYAML = `
kind: ConstraintTemplate
apiVersion: templates.gatekeeper.sh/v1beta1
metadata:
  name: alwaysvalidate
  {}: {}
spec:
  crd:
    spec:
      names:
        kind: AlwaysValidate
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8sdisallowedtags
        violation {
          false
        }
`

	templateMarshalError = `
kind: ConstraintTemplate
apiVersion: templates.gatekeeper.sh/v1beta1
metadata:
  name: alwaysvalidate
spec: [a, b, c]
`

	templateCompileError = `
kind: ConstraintTemplate
apiVersion: templates.gatekeeper.sh/v1beta1
metadata:
  name: compileerror
spec:
  crd:
    spec:
      names:
        kind: CompileError
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8sdisallowedtags
        violation {
          f
        }
`

	constraintAlwaysValidate = `
kind: AlwaysValidate
apiVersion: constraints.gatekeeper.sh/v1beta1
metadata:
  name: always-pass
`

	constraintInvalidYAML = `
kind: AlwaysValidate
apiVersion: constraints.gatekeeper.sh/v1beta1
metadata:
  name: always-pass
  {}: {}
`

	constraintWrongTemplate = `
kind: Other
apiVersion: constraints.gatekeeper.sh/v1beta1
metadata:
  name: other
`
)

func TestRunner_Run(t *testing.T) {
	testCases := []struct {
		name  string
		suite Suite
		f     fs.FS
		want  SuiteResult
	}{
		{
			name: "Suite missing Template",
			suite: Suite{
				Tests: []Test{{}},
			},
			f: fstest.MapFS{},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrInvalidSuite,
				}},
			},
		},
		{
			name: "Suite with template in nonexistent file",
			suite: Suite{
				Tests: []Test{{
					Template: "template.yaml",
				}},
			},
			f: fstest.MapFS{},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: fs.ErrNotExist,
				}},
			},
		},
		{
			name: "Suite with YAML parsing error",
			suite: Suite{
				Tests: []Test{{
					Template: "template.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateInvalidYAML),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrAddingTemplate,
				}},
			},
		},
		{
			name: "Suite with template unmarshalling error",
			suite: Suite{
				Tests: []Test{{
					Template: "template.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateMarshalError),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrAddingTemplate,
				}},
			},
		},
		{
			name: "Suite with rego compilation error",
			suite: Suite{
				Tests: []Test{{
					Template: "template.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateCompileError),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrAddingTemplate,
				}},
			},
		},
		{
			name: "Suite with unsupported template version",
			suite: Suite{
				Tests: []Test{{
					Template: "template.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateUnsupportedVersion),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrAddingTemplate,
				}},
			},
		},
		{
			name: "Suite pointing to non-template",
			suite: Suite{
				Tests: []Test{{
					Template: "template.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(constraintAlwaysValidate),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrNotATemplate,
				}},
			},
		},
		{
			name: "Suite missing Constraint",
			suite: Suite{
				Tests: []Test{{
					Template: "template.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateAlwaysValidate),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrInvalidSuite,
				}},
			},
		},
		{
			name: "valid Suite",
			suite: Suite{
				Tests: []Test{{
					Template:   "template.yaml",
					Constraint: "constraint.yaml",
					Cases:      []Case{{}},
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateAlwaysValidate),
				},
				"constraint.yaml": &fstest.MapFile{
					Data: []byte(constraintAlwaysValidate),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					CaseResults: []CaseResult{{}},
				}},
			},
		},
		{
			name: "constraint missing file",
			suite: Suite{
				Tests: []Test{{
					Template:   "template.yaml",
					Constraint: "constraint.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateAlwaysValidate),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: fs.ErrNotExist,
				}},
			},
		},
		{
			name: "constraint invalid YAML",
			suite: Suite{
				Tests: []Test{{
					Template:   "template.yaml",
					Constraint: "constraint.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateAlwaysValidate),
				},
				"constraint.yaml": &fstest.MapFile{
					Data: []byte(constraintInvalidYAML),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrAddingConstraint,
				}},
			},
		},
		{
			name: "constraint is not a constraint",
			suite: Suite{
				Tests: []Test{{
					Template:   "template.yaml",
					Constraint: "constraint.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateAlwaysValidate),
				},
				"constraint.yaml": &fstest.MapFile{
					Data: []byte(templateAlwaysValidate),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrNotAConstraint,
				}},
			},
		},
		{
			name: "constraint is for other template",
			suite: Suite{
				Tests: []Test{{
					Template:   "template.yaml",
					Constraint: "constraint.yaml",
				}},
			},
			f: fstest.MapFS{
				"template.yaml": &fstest.MapFile{
					Data: []byte(templateAlwaysValidate),
				},
				"constraint.yaml": &fstest.MapFile{
					Data: []byte(constraintWrongTemplate),
				},
			},
			want: SuiteResult{
				TestResults: []TestResult{{
					Error: ErrAddingConstraint,
				}},
			},
		},
	}

	for _, tc := range testCases {
		ctx := context.Background()

		runner := Runner{
			FS:        tc.f,
			NewClient: NewOPAClient,
		}

		t.Run(tc.name, func(t *testing.T) {
			got := runner.Run(ctx, Filter{}, &tc.suite)

			if diff := cmp.Diff(tc.want, got, cmpopts.EquateErrors(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf(diff)
			}
		})
	}
}

func TestRunner_Run_ClientError(t *testing.T) {
	want := SuiteResult{
		TestResults: []TestResult{{Error: ErrCreatingClient}},
	}

	runner := Runner{
		FS: fstest.MapFS{},
		NewClient: func() (Client, error) {
			return nil, errors.New("error")
		},
	}

	ctx := context.Background()

	suite := &Suite{
		Tests: []Test{{}},
	}
	got := runner.Run(ctx, Filter{}, suite)

	if diff := cmp.Diff(want, got, cmpopts.EquateErrors(), cmpopts.EquateEmpty()); diff != "" {
		t.Error(diff)
	}
}
