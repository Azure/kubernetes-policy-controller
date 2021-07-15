package gktest

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
)

func TestReadSuites(t *testing.T) {
	testCases := []struct {
		name       string
		target     string
		recursive  bool
		fileSystem fs.FS
		want       []Suite
		wantErr    error
	}{
		{
			name:       "no filesystem",
			target:     "test.yaml",
			recursive:  false,
			fileSystem: nil,
			want:       nil,
			wantErr:    ErrNoFileSystem,
		},
		{
			name:       "empty filesystem",
			target:     "test.yaml",
			recursive:  false,
			fileSystem: fstest.MapFS{},
			want:       nil,
			wantErr:    fs.ErrNotExist,
		},
		{
			name:       "empty target",
			target:     "",
			recursive:  false,
			fileSystem: fstest.MapFS{},
			want:       nil,
			wantErr:    ErrNoTarget,
		},
		{
			name:      "non yaml file",
			target:    "test.txt",
			recursive: false,
			fileSystem: fstest.MapFS{
				"test.txt": &fstest.MapFile{
					Data: []byte(""),
				},
			},
			want:    nil,
			wantErr: ErrUnsupportedExtension,
		},
		{
			name:      "single target",
			target:    "test.yaml",
			recursive: false,
			fileSystem: fstest.MapFS{
				"test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
			},
			want:    []Suite{{}},
			wantErr: nil,
		},
		{
			name:      "invalid filepath",
			target:    "test/../test/test.yaml",
			recursive: false,
			fileSystem: fstest.MapFS{
				"test/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
			},
			want:    nil,
			wantErr: fs.ErrNotExist,
		},
		{
			name:      "single target wrong Kind",
			target:    "test.yaml",
			recursive: false,
			fileSystem: fstest.MapFS{
				"test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Role
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
			},
			want:    nil,
			wantErr: nil,
		},
		{
			name:      "single target wrong Group",
			target:    "test.yaml",
			recursive: false,
			fileSystem: fstest.MapFS{
				"test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: v1alpha1
`),
				},
			},
			want:    nil,
			wantErr: nil,
		},
		{
			name:      "single target invalid yaml",
			target:    "test.yaml",
			recursive: false,
			fileSystem: fstest.MapFS{
				"test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
{a: b}: 3`),
				},
			},
			want:    nil,
			wantErr: ErrInvalidYAML,
		},
		{
			name:      "directory",
			target:    "tests",
			recursive: false,
			fileSystem: fstest.MapFS{
				"tests/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
			},
			want:    []Suite{{}},
			wantErr: nil,
		},
		{
			name:      "directory with tests and non-tests",
			target:    "tests",
			recursive: false,
			fileSystem: fstest.MapFS{
				"tests/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
				"tests/other-file": &fstest.MapFile{
					Data: []byte(`some data`),
				},
			},
			want:    []Suite{{}},
			wantErr: nil,
		},
		{
			name:      "non-recursive directory with subdirectory",
			target:    "tests",
			recursive: false,
			fileSystem: fstest.MapFS{
				"tests/labels/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
				"tests/annotations/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
				"tests/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
			},
			want:    []Suite{{}},
			wantErr: nil,
		},
		{
			name:      "recursive directory with subdirectory",
			target:    "tests",
			recursive: true,
			fileSystem: fstest.MapFS{
				"tests/labels/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
				"tests/annotations/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
				"tests/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
			},
			want:    []Suite{{}, {}, {}},
			wantErr: nil,
		},
		{
			name:      "recursive directory with subdirectory with '.yaml'",
			target:    "tests",
			recursive: true,
			fileSystem: fstest.MapFS{
				"tests/labels.yaml/test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
			},
			want:    []Suite{{}},
			wantErr: nil,
		},
		{
			name:      "recursive file is an error",
			target:    "test.yaml",
			recursive: true,
			fileSystem: fstest.MapFS{
				"test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
`),
				},
			},
			want:    nil,
			wantErr: ErrNotADirectory,
		},
		{
			name:      "suite with test and cases",
			target:    "test.yaml",
			recursive: false,
			fileSystem: fstest.MapFS{
				"test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
tests:
- template: template.yaml
  constraint: constraint.yaml
  cases:
  - allow: allow.yaml
  - deny: deny.yaml
`),
				},
			},
			want: []Suite{{
				Tests: []Test{{
					Template:   "template.yaml",
					Constraint: "constraint.yaml",
					Cases: []Case{{
						Allow: "allow.yaml",
					}, {
						Deny: "deny.yaml",
					}},
				}},
			}},
			wantErr: nil,
		},
		{
			name:      "suite with tests and no cases",
			target:    "test.yaml",
			recursive: false,
			fileSystem: fstest.MapFS{
				"test.yaml": &fstest.MapFile{
					Data: []byte(`
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
tests:
- template: template.yaml
  constraint: constraint.yaml
`),
				},
			},
			want: []Suite{{
				Tests: []Test{{
					Template:   "template.yaml",
					Constraint: "constraint.yaml",
				}},
			}},
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotErr := ReadSuites(tc.fileSystem, tc.target, tc.recursive)
			if !errors.Is(gotErr, tc.wantErr) {
				t.Errorf("got error %v, want error %v",
					gotErr, tc.wantErr)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Error(diff)
			}
		})
	}
}
