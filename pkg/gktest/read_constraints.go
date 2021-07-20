package gktest

import (
	"encoding/json"
	"fmt"
	"io/fs"

	templatesv1 "github.com/open-policy-agent/frameworks/constraint/pkg/apis/templates/v1"
	"github.com/open-policy-agent/frameworks/constraint/pkg/core/templates"
	"github.com/open-policy-agent/gatekeeper/apis"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// scheme stores the k8s resource types we can instantiate as Templates.
var scheme = runtime.NewScheme()

func init() {
	_ = apis.AddToScheme(scheme)
}

// readTemplate reads the contents of the path and returns the
// ConstraintTemplate it defines. Returns an error if the file does not define
// a ConstraintTemplate.
func readTemplate(f fs.FS, path string) (*templates.ConstraintTemplate, error) {
	bytes, err := fs.ReadFile(f, path)
	if err != nil {
		return nil, fmt.Errorf("reading ConstraintTemplate from %q: %w", path, err)
	}

	u := unstructured.Unstructured{
		Object: make(map[string]interface{}),
	}
	err = yaml.Unmarshal(bytes, u.Object)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing ConstraintTemplate YAML from %q: %v", ErrAddingTemplate, path, err)
	}

	gvk := u.GroupVersionKind()
	if gvk.Group != templatesv1.SchemeGroupVersion.Group || gvk.Kind != "ConstraintTemplate" {
		return nil, fmt.Errorf("%w: %q", ErrNotATemplate, path)
	}

	t, err := scheme.New(gvk)
	if err != nil {
		// The type isn't registered in the scheme.
		return nil, fmt.Errorf("%w: %v", ErrAddingTemplate, err)
	}

	// YAML parsing doesn't properly handle ObjectMeta, so we must
	// marshal/unmashal through JSON.
	jsonBytes, err := u.MarshalJSON()
	if err != nil {
		// Indicates a bug in unstructured.MarshalJSON(). Any Unstructured
		// unmarshalled from YAML should be marshallable to JSON.
		return nil, fmt.Errorf("calling unstructured.MarshalJSON(): %w", err)
	}
	err = json.Unmarshal(jsonBytes, t)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAddingTemplate, err)
	}

	template := &templates.ConstraintTemplate{}
	err = scheme.Convert(t, template, nil)
	if err != nil {
		// This shouldn't happen unless there's a bug in the conversion functions.
		// Most likely it means the conversion functions weren't generated.
		return nil, err
	}

	return template, nil
}

func readConstraint(f fs.FS, path string) (*unstructured.Unstructured, error) {
	bytes, err := fs.ReadFile(f, path)
	if err != nil {
		return nil, fmt.Errorf("reading Constraint from %q: %w", path, err)
	}

	c := &unstructured.Unstructured{
		Object: make(map[string]interface{}),
	}

	err = yaml.Unmarshal(bytes, c.Object)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing Constraint from %q: %v", ErrAddingConstraint, path, err)
	}

	gvk := c.GroupVersionKind()
	if gvk.Group != "constraints.gatekeeper.sh" {
		return nil, ErrNotAConstraint
	}

	return c, nil
}
