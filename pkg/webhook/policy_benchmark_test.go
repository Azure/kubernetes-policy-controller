/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"context"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	templv1beta1 "github.com/open-policy-agent/frameworks/constraint/pkg/apis/templates/v1beta1"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/frameworks/constraint/pkg/core/templates"
	"github.com/open-policy-agent/gatekeeper/apis/config/v1alpha1"
	testclient "github.com/open-policy-agent/gatekeeper/test/clients"
	"github.com/pkg/errors"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
	atypes "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/yaml"
)

type fakeNsGetter struct {
	testclient.NoopClient
	scheme *runtime.Scheme
}

// getFiles reads a directory and returns a list of files ending with .yaml/.yml
// returns an error if directory does not exist.
func getFiles(dir string) ([]string, error) {
	var filePaths []string
	var err error
	if _, err = os.Stat(dir); err != nil {
		return nil, err
	}
	var files []os.DirEntry
	if files, err = os.ReadDir(dir); err != nil {
		return nil, err
	}
	// white-list file extensions
	exts := sets.NewString(".yaml", ".yml")
	for _, file := range files {
		if !exts.Has(filepath.Ext(file.Name())) {
			continue
		}
		filePaths = append(filePaths, filepath.Join(dir, file.Name()))
	}
	return filePaths, nil
}

func (f *fakeNsGetter) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	if ns, ok := obj.(*corev1.Namespace); ok {
		ns.ObjectMeta = metav1.ObjectMeta{
			Name: key.Name,
		}
		return nil
	}

	return errors.New("not found")
}

// readTemplates reads templates from a directory
// all files ending with .yaml are loaded. One resource per .yaml file
// does not support recursive directory search
// fails if directory is not a valid path
// fails if any of the files is not a valid constraint template.
func readTemplates(dir string) ([]templates.ConstraintTemplate, error) {
	fileList, err := getFiles(dir)
	if err != nil {
		return nil, err
	}
	result := make([]templates.ConstraintTemplate, len(fileList))
	for i, file := range fileList {
		yamlString, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		cstr := &templv1beta1.ConstraintTemplate{}
		if err := yaml.Unmarshal(yamlString, cstr); err != nil {
			return nil, err
		}
		unversioned := templates.ConstraintTemplate{}
		if err := runtimeScheme.Convert(cstr, &unversioned, nil); err != nil {
			return nil, err
		}
		result[i] = unversioned
	}
	return result, nil
}

// readConstraints reads constraints from a directory
// all files ending with .yaml are loaded. One resource per .yaml file
// does not support recursive directory search
// fails if directory is not a valid path.
func readConstraints(dir string) ([]unstructured.Unstructured, error) {
	return readDirHelper(dir)
}

// readResources reads resources from a directory
// these resources would be transformed into admission requests ex: Pods, Deployments
// all files ending with .yaml are loaded. One resource per .yaml file
// does not support recursive directory search
// fails if directory is not a valid path.
func readResources(dir string) ([]unstructured.Unstructured, error) {
	return readDirHelper(dir)
}

// readDirHelper is a helper method to read YAML files and unmarshal them into unstructured.
func readDirHelper(dir string) ([]unstructured.Unstructured, error) {
	fileList, err := getFiles(dir)
	if err != nil {
		return nil, err
	}
	result := make([]unstructured.Unstructured, len(fileList))
	for i, file := range fileList {
		yamlString, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		cr := unstructured.Unstructured{}
		if err := yaml.Unmarshal(yamlString, &cr); err != nil {
			return nil, err
		}
		result[i] = cr
	}
	return result, nil
}

func addTemplates(opa *opa.Client, list []templates.ConstraintTemplate) error {
	for index := range list {
		_, err := opa.AddTemplate(context.TODO(), &list[index])
		if err != nil {
			return err
		}
	}
	return nil
}

func addConstraints(opa *opa.Client, list []unstructured.Unstructured) error {
	for index := range list {
		_, err := opa.AddConstraint(context.TODO(), &list[index])
		if err != nil {
			return err
		}
	}
	return nil
}

// generateConstraints generates m constraints based on representative constraint in crList.
func generateConstraints(m int, crList []unstructured.Unstructured) []unstructured.Unstructured {
	result := make([]unstructured.Unstructured, m)
	for i := 0; i < m; i++ {
		r := crList[i%len(crList)]
		result[i] = *(r.DeepCopy())
		r.SetName(genRandString(10))
	}
	return result
}

func genRandString(n int) string {
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		c := 'a' + rand.Intn(26) // #nosec G404
		out[i] = byte(c)
	}
	return string(out)
}

func createAdmissionRequests(resList []unstructured.Unstructured, n int) atypes.Request {
	dryRun := false
	res := resList[n%len(resList)]
	name := "res-name-" + strconv.Itoa(n)
	namespace := "res-namespace-" + strconv.Itoa(n)
	res.SetName(name)
	res.SetNamespace(namespace)
	oldRes := res.DeepCopy()
	res.SetResourceVersion("2")
	oldRes.SetResourceVersion("1")
	gvr, _ := meta.UnsafeGuessKindToResource(oldRes.GroupVersionKind())
	return atypes.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			UID:                types.UID(uuid.NewUUID()),
			Kind:               metav1.GroupVersionKind{Group: oldRes.GroupVersionKind().Group, Version: oldRes.GroupVersionKind().Version, Kind: oldRes.GroupVersionKind().Kind},
			Resource:           metav1.GroupVersionResource{Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource},
			SubResource:        "",
			RequestKind:        &metav1.GroupVersionKind{Group: oldRes.GroupVersionKind().Group, Version: oldRes.GroupVersionKind().Version, Kind: oldRes.GroupVersionKind().Kind},
			RequestResource:    &metav1.GroupVersionResource{Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource},
			RequestSubResource: "",
			Name:               name,
			Namespace:          namespace,
			Operation:          "UPDATE",
			UserInfo: authenticationv1.UserInfo{
				Username: "res-creator",
				UID:      "uid",
				Groups:   []string{"res-creator-group"},
				Extra:    map[string]authenticationv1.ExtraValue{"extraKey": {"value1", "value2"}},
			},
			Object:    runtime.RawExtension{Object: &resList[n%len(resList)]},
			OldObject: runtime.RawExtension{Object: oldRes},
			DryRun:    &dryRun,
			Options:   runtime.RawExtension{},
		},
	}
}

func BenchmarkValidationHandler(b *testing.B) {
	// setup test
	opa, err := makeOpaClient()
	if err != nil {
		b.Fatalf("could not initialize OPA: %s", err)
	}

	c := &fakeNsGetter{scheme: runtimeScheme, NoopClient: testclient.NoopClient{}}
	cfg := &v1alpha1.Config{
		Spec: v1alpha1.ConfigSpec{
			Validation: v1alpha1.Validation{
				Traces: []v1alpha1.Trace{},
			},
		},
	}
	h := validationHandler{opa: opa, webhookHandler: webhookHandler{client: c, injectedConfig: cfg}}

	benchmarks := map[string]struct {
		// description of the test
		description string
		// directory to load constraint templates from
		templateDir string
		// directory to load constraints from
		constraintDir string
		// directory to load resources to be evaluated // ex. Pods
		resourceDir string
		// number of constraints to load
		load []int
	}{
		"psp: 100% violations": {
			description:   "All constraints are applicable and all requests are violating",
			templateDir:   "testdata/psp-all-violations/psp-templates",
			constraintDir: "testdata/psp-all-violations/psp-constraints",
			// pod list
			resourceDir: "testdata/psp-all-violations/psp-pods",
			load:        []int{5, 10, 50, 100, 200, 1000, 2000},
		},
		// create template, constraint and resource directories and add a new test case with appropriate load values
	}
	for name, tc := range benchmarks {
		// read template
		ctList, err := readTemplates(tc.templateDir)
		if err != nil {
			b.Fatalf("failed to read template files: %s", err)
		}

		// read constraints
		crList, err := readConstraints(tc.constraintDir)
		if err != nil {
			b.Fatalf("failed to read constraint files: %s", err)
		}

		// read resources
		resList, err := readResources(tc.resourceDir)
		if err != nil {
			b.Fatalf("failed to read resources: %s", err)
		}

		for _, bm := range tc.load {
			// remove all data from OPA
			err = opa.Reset(context.TODO())
			if err != nil {
				b.Errorf("test %s, failed to reset OPA: %s", name, err)
			}
			// seed random generator
			rand.Seed(time.Now().UnixNano())

			// create T templates
			err = addTemplates(opa, ctList)
			if err != nil {
				b.Errorf("test %s, failed to load templates into OPA: %s", name, err)
			}
			// load constraints into OPA
			err = addConstraints(opa, generateConstraints(bm, crList))
			if err != nil {
				b.Errorf("test %s, failed to load constraints into OPA: %s", name, err)
			}

			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					req := createAdmissionRequests(resList, i)
					b.StartTimer()
					resp := h.Handle(context.TODO(), req)
					if resp.Result.Code == http.StatusInternalServerError || resp.Result.Code == http.StatusUnprocessableEntity {
						b.Errorf("expected a decision, received server error %d on test %s", resp.Result.Code, name)
					}
				}
			})
			// remove all data from OPA
			err = opa.Reset(context.TODO())
			if err != nil {
				b.Errorf("test %s, failed to reset OPA: %s", name, err)
			}
		}
	}
}
