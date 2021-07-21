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

package constrainttemplate

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/open-policy-agent/frameworks/constraint/pkg/apis/templates/v1beta1"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/frameworks/constraint/pkg/client/drivers/local"
	podstatus "github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	statusv1beta1 "github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/target"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	testclient "github.com/open-policy-agent/gatekeeper/test/clients"
	"github.com/open-policy-agent/gatekeeper/third_party/sigs.k8s.io/controller-runtime/pkg/dynamiccache"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const timeout = time.Second * 15

// setupManager sets up a controller-runtime manager with registered watch manager.
func setupManager(t *testing.T) (manager.Manager, *watch.Manager) {
	t.Helper()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	metrics.Registry = prometheus.NewRegistry()
	mgr, err := manager.New(cfg, manager.Options{
		MetricsBindAddress: "0",
		NewCache:           dynamiccache.New,
		MapperProvider: func(c *rest.Config) (meta.RESTMapper, error) {
			return apiutil.NewDynamicRESTMapper(c)
		},
	})
	if err != nil {
		t.Fatalf("setting up controller manager: %s", err)
	}
	c := mgr.GetCache()
	dc, ok := c.(watch.RemovableCache)
	if !ok {
		t.Fatalf("expected dynamic cache, got: %T", c)
	}
	wm, err := watch.New(dc)
	if err != nil {
		t.Fatalf("could not create watch manager: %s", err)
	}
	if err := mgr.Add(wm); err != nil {
		t.Fatalf("could not add watch manager to manager: %s", err)
	}
	return mgr, wm
}

func TestReconcile(t *testing.T) {
	crdKey := types.NamespacedName{Name: "denyall.constraints.gatekeeper.sh"}
	g := gomega.NewGomegaWithT(t)
	instance := &v1beta1.ConstraintTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "denyall"},
		Spec: v1beta1.ConstraintTemplateSpec{
			CRD: v1beta1.CRD{
				Spec: v1beta1.CRDSpec{
					Names: v1beta1.Names{
						Kind: "DenyAll",
					},
				},
			},
			Targets: []v1beta1.Target{
				{
					Target: "admission.k8s.gatekeeper.sh",
					Rego: `
package foo

violation[{"msg": "denied!"}] {
	1 == 1
}
`,
				},
			},
		},
	}

	// Uncommenting the below enables logging of K8s internals like watch.
	// fs := flag.NewFlagSet("", flag.PanicOnError)
	// klog.InitFlags(fs)
	// fs.Parse([]string{"--alsologtostderr", "-v=10"})
	// klog.SetOutput(os.Stderr)

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, wm := setupManager(t)
	c := testclient.NewRetryClient(mgr.GetClient())

	// creating the gatekeeper-system namespace is necessary because that's where
	// status resources live by default
	g.Expect(createGatekeeperNamespace(mgr.GetConfig())).NotTo(gomega.HaveOccurred())

	// initialize OPA
	driver := local.New(local.Tracing(true))
	backend, err := opa.NewBackend(opa.Driver(driver))
	if err != nil {
		t.Fatalf("unable to set up OPA backend: %s", err)
	}
	opa, err := backend.NewClient(opa.Targets(&target.K8sValidationTarget{}))
	if err != nil {
		t.Fatalf("unable to set up OPA client: %s", err)
	}

	os.Setenv("POD_NAME", "no-pod")
	podstatus.DisablePodOwnership()
	cs := watch.NewSwitch()
	tracker, err := readiness.SetupTracker(mgr, false)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	pod := &corev1.Pod{}
	pod.Name = "no-pod"
	// events will be used to receive events from dynamic watches registered
	events := make(chan event.GenericEvent, 1024)
	rec, _ := newReconciler(mgr, opa, wm, cs, tracker, events, events, func(context.Context) (*corev1.Pod, error) { return pod, nil })
	g.Expect(add(mgr, rec)).NotTo(gomega.HaveOccurred())

	cstr := newDenyAllCstr()

	ctx, cancelFunc := context.WithCancel(context.Background())
	mgrStopped := StartTestManager(ctx, mgr, g)
	once := sync.Once{}
	testMgrStopped := func() {
		once.Do(func() {
			cancelFunc()
			mgrStopped.Wait()
		})
	}

	defer testMgrStopped()
	// Clean up to remove the crd, constraint and constraint template
	defer func() {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		g.Expect(c.Get(ctx, crdKey, crd)).NotTo(gomega.HaveOccurred())

		g.Expect(deleteObject(ctx, c, cstr, timeout)).To(gomega.BeNil())
		g.Expect(deleteObject(ctx, c, crd, timeout)).To(gomega.BeNil())
		g.Expect(deleteObject(ctx, c, instance, timeout)).To(gomega.BeNil())
	}()

	log.Info("Running test: CRD Gets Created")
	t.Run("CRD Gets Created", func(t *testing.T) {
		err = c.Create(ctx, instance)
		g.Expect(err).NotTo(gomega.HaveOccurred())

		clientset := kubernetes.NewForConfigOrDie(cfg)
		g.Eventually(func() error {
			crd := &apiextensionsv1.CustomResourceDefinition{}
			if err := c.Get(ctx, crdKey, crd); err != nil {
				return err
			}
			rs, err := clientset.Discovery().ServerResourcesForGroupVersion("constraints.gatekeeper.sh/v1beta1")
			if err != nil {
				return err
			}
			for _, r := range rs.APIResources {
				if r.Kind == "DenyAll" {
					return nil
				}
			}
			return errors.New("DenyAll not found")
		}, timeout).Should(gomega.BeNil())
	})

	log.Info("Running test: Constraint is marked as enforced")
	t.Run("Constraint is marked as enforced", func(t *testing.T) {
		err = c.Create(ctx, cstr)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		constraintEnforced(c, g, timeout)
	})

	log.Info("Running test: Constraint actually enforced")
	t.Run("Constraint actually enforced", func(t *testing.T) {
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "testns",
			},
		}
		req := admissionv1.AdmissionRequest{
			Kind: metav1.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Namespace",
			},
			Operation: "Create",
			Name:      "FooNamespace",
			Object:    runtime.RawExtension{Object: ns},
		}
		resp, err := opa.Review(ctx, req)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		if len(resp.Results()) != 1 {
			fmt.Println(resp.TraceDump())
			fmt.Println(opa.Dump(ctx))
		}
		g.Expect(len(resp.Results())).Should(gomega.Equal(1))
	})

	log.Info("Running test: Deleted constraint CRDs are recreated")
	t.Run("Deleted constraint CRDs are recreated", func(t *testing.T) {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		g.Expect(c.Get(ctx, crdKey, crd)).NotTo(gomega.HaveOccurred())
		origUID := crd.GetUID()
		crd.Spec = apiextensionsv1.CustomResourceDefinitionSpec{}
		g.Expect(c.Delete(ctx, crd)).NotTo(gomega.HaveOccurred())

		g.Eventually(func() error {
			crd := &apiextensionsv1.CustomResourceDefinition{}
			if err := c.Get(ctx, crdKey, crd); err != nil {
				return err
			}
			if !crd.GetDeletionTimestamp().IsZero() {
				return errors.New("Still deleting")
			}
			if crd.GetUID() == origUID {
				return errors.New("Not yet deleted")
			}
			for _, cond := range crd.Status.Conditions {
				if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
					return nil
				}
			}
			return errors.New("Not established")
		}, timeout, time.Second).Should(gomega.BeNil())

		g.Eventually(func() error {
			sList := &podstatus.ConstraintPodStatusList{}
			if err := c.List(ctx, sList); err != nil {
				return err
			}
			if len(sList.Items) != 0 {
				return fmt.Errorf("Remaining status items: %+v", sList.Items)
			}
			return nil
		}, timeout).Should(gomega.BeNil())

		g.Eventually(func() error { return c.Create(ctx, newDenyAllCstr()) }, timeout).Should(gomega.BeNil())
		// we need a longer timeout because deleting the CRD interrupts the watch
		constraintEnforced(c, g, 50*timeout)
	})

	log.Info("Running test: Templates with Invalid Rego throw errors")
	t.Run("Templates with Invalid Rego throw errors", func(t *testing.T) {
		// Create template with invalid rego, should populate parse error in status
		instanceInvalidRego := &v1beta1.ConstraintTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "invalidrego"},
			Spec: v1beta1.ConstraintTemplateSpec{
				CRD: v1beta1.CRD{
					Spec: v1beta1.CRDSpec{
						Names: v1beta1.Names{
							Kind: "InvalidRego",
						},
					},
				},
				Targets: []v1beta1.Target{
					{
						Target: "admission.k8s.gatekeeper.sh",
						Rego: `
	package foo

	violation[{"msg": "hi"}] { 1 == 1 }

	anyrule[}}}//invalid//rego

	`,
					},
				},
			},
		}

		err = c.Create(ctx, instanceInvalidRego)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = c.Delete(ctx, instanceInvalidRego)
			g.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		g.Eventually(func() error {
			ct := &v1beta1.ConstraintTemplate{}
			if err := c.Get(ctx, types.NamespacedName{Name: "invalidrego"}, ct); err != nil {
				return err
			}
			if ct.Name == "invalidrego" {
				status, found := getCTByPodStatus(ct)
				if !found {
					return fmt.Errorf("could not retrieve CT status for pod, byPod status: %+v", ct.Status.ByPod)
				}
				if len(status.Errors) == 0 {
					j, err := json.Marshal(status)
					if err != nil {
						t.Fatal("could not parse JSON", err)
					}
					s := string(j)
					return fmt.Errorf("InvalidRego template should contain an error: %s", s)
				}
				if status.Errors[0].Code != "rego_parse_error" {
					return fmt.Errorf("InvalidRego template returning unexpected error %s", status.Errors[0].Code)
				}
				return nil
			}
			return errors.New("InvalidRego not found")
		}, timeout).Should(gomega.BeNil())
	})

	log.Info("Running test: Deleted constraint templates not enforced")
	t.Run("Deleted constraint templates not enforced", func(t *testing.T) {
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "testns",
			},
		}
		req := admissionv1.AdmissionRequest{
			Kind: metav1.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Namespace",
			},
			Operation: "Create",
			Name:      "FooNamespace",
			Object:    runtime.RawExtension{Object: ns},
		}
		resp, err := opa.Review(ctx, req)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		if len(resp.Results()) != 1 {
			fmt.Println(resp.TraceDump())
			fmt.Println(opa.Dump(ctx))
		}
		g.Expect(len(resp.Results())).Should(gomega.Equal(1))
		g.Expect(c.Delete(ctx, instance.DeepCopy())).Should(gomega.BeNil())
		g.Eventually(func() error {
			resp, err := opa.Review(ctx, req)
			if err != nil {
				return err
			}
			if len(resp.Results()) != 0 {
				dump, _ := opa.Dump(ctx)
				return fmt.Errorf("Results not yet zero\nOPA DUMP:\n%s", dump)
			}
			return nil
		}, timeout).Should(gomega.BeNil())
	})
}

// Tests that expectations for constraints are canceled if the corresponding constraint is deleted.
func TestReconcile_DeleteConstraintResources(t *testing.T) {
	log.Info("Running test: Cancel the expectations when constraint gets deleted")

	g := gomega.NewGomegaWithT(t)

	// Setup the Manager
	mgr, wm := setupManager(t)
	c := testclient.NewRetryClient(mgr.GetClient())
	ctx, cancelFunc := context.WithCancel(context.Background())

	// start manager that will start tracker and controller
	mgrStopped := StartTestManager(ctx, mgr, g)
	once := sync.Once{}
	testMgrStopped := func() {
		once.Do(func() {
			cancelFunc()
			mgrStopped.Wait()
		})
	}
	defer testMgrStopped()

	// Create the constraint template object and expect the Reconcile to be created when controller starts
	instance := &v1beta1.ConstraintTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "denyall"},
		Spec: v1beta1.ConstraintTemplateSpec{
			CRD: v1beta1.CRD{
				Spec: v1beta1.CRDSpec{
					Names: v1beta1.Names{
						Kind: "DenyAll",
					},
				},
			},
			Targets: []v1beta1.Target{
				{
					Target: "admission.k8s.gatekeeper.sh",
					Rego: `
package foo

violation[{"msg": "denied!"}] {
	1 == 1
}
`,
				},
			},
		},
	}

	err := c.Create(ctx, instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() { g.Expect(ignoreNotFound(c.Delete(ctx, instance))).To(gomega.BeNil()) }()

	gvk := schema.GroupVersionKind{
		Group:   "constraints.gatekeeper.sh",
		Version: "v1beta1",
		Kind:    "DenyAll",
	}

	// Install constraint CRD
	crd := makeCRD(gvk, "denyall")
	err = applyCRD(ctx, g, c, gvk, crd)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "applying CRD")

	// Create the constraint for constraint template
	cstr := newDenyAllCstr()
	err = c.Create(ctx, cstr)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// creating the gatekeeper-system namespace is necessary because that's where
	// status resources live by default
	g.Expect(createGatekeeperNamespace(mgr.GetConfig())).NotTo(gomega.HaveOccurred())

	// Set up tracker
	tracker, err := readiness.SetupTracker(mgr, false)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// initialize OPA
	driver := local.New(local.Tracing(true))
	backend, err := opa.NewBackend(opa.Driver(driver))
	if err != nil {
		t.Fatalf("unable to set up OPA backend: %s", err)
	}
	opa, err := backend.NewClient(opa.Targets(&target.K8sValidationTarget{}))
	if err != nil {
		t.Fatalf("unable to set up OPA client: %s", err)
	}

	_ = os.Setenv("POD_NAME", "no-pod")
	podstatus.DisablePodOwnership()
	cs := watch.NewSwitch()
	pod := &corev1.Pod{}
	pod.Name = "no-pod"
	// events will be used to receive events from dynamic watches registered
	events := make(chan event.GenericEvent, 1024)
	rec, _ := newReconciler(mgr, opa, wm, cs, tracker, events, nil, func(context.Context) (*corev1.Pod, error) { return pod, nil })
	g.Expect(add(mgr, rec)).NotTo(gomega.HaveOccurred())

	// get the object tracker for the constraint
	ot := tracker.For(gvk)
	tr, ok := ot.(testExpectations)
	if !ok {
		t.Fatalf("unexpected tracker, got %T", ot)
	}
	// ensure that expectations are set for the constraint gvk
	g.Eventually(func() bool {
		return tr.IsExpecting(gvk, types.NamespacedName{Name: "denyallconstraint"})
	}, timeout).Should(gomega.BeTrue(), "waiting for expectations")

	// Delete the constraint , the delete event will be reconciled by controller
	// to cancel the expectation set for it by tracker
	g.Expect(c.Delete(ctx, cstr)).NotTo(gomega.HaveOccurred())

	// set event channel to receive request for constraint
	events <- event.GenericEvent{
		Object: cstr,
	}

	// Check readiness tracker is satisfied post-reconcile
	g.Eventually(func() bool {
		return tracker.For(gvk).Satisfied()
	}, timeout).Should(gomega.BeTrue())
}

func constraintEnforced(c client.Client, g *gomega.GomegaWithT, timeout time.Duration) {
	g.Eventually(func() error {
		cstr := newDenyAllCstr()
		err := c.Get(context.TODO(), types.NamespacedName{Name: "denyallconstraint"}, cstr)
		if err != nil {
			return err
		}
		status, err := getCByPodStatus(cstr)
		if err != nil {
			return err
		}
		if !status.Enforced {
			obj, _ := json.MarshalIndent(cstr.Object, "", "  ")
			// return errors.New("constraint not enforced)
			return fmt.Errorf("constraint not enforced: \n%s", obj)
		}
		return nil
	}, timeout).Should(gomega.BeNil())
}

func newDenyAllCstr() *unstructured.Unstructured {
	cstr := &unstructured.Unstructured{}
	cstr.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "constraints.gatekeeper.sh",
		Version: "v1beta1",
		Kind:    "DenyAll",
	})
	cstr.SetName("denyallconstraint")
	return cstr
}

func getCTByPodStatus(templ *v1beta1.ConstraintTemplate) (v1beta1.ByPodStatus, bool) {
	statuses := templ.Status.ByPod
	for _, s := range statuses {
		if s.ID == util.GetID() {
			return s, true
		}
	}
	return v1beta1.ByPodStatus{}, false
}

func getCByPodStatus(obj *unstructured.Unstructured) (*statusv1beta1.ConstraintPodStatusStatus, error) {
	list, found, err := unstructured.NestedSlice(obj.Object, "status", "byPod")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("no byPod status is set")
	}
	var status *statusv1beta1.ConstraintPodStatusStatus
	for _, v := range list {
		j, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		s := &statusv1beta1.ConstraintPodStatusStatus{}
		if err := json.Unmarshal(j, s); err != nil {
			return nil, err
		}
		if s.ID == util.GetID() {
			status = s
			break
		}
	}
	if status == nil {
		return nil, errors.New("current pod is not listed in byPod status")
	}
	return status, nil
}

// makeCRD generates a CRD specified by GVK and plural for testing.
func makeCRD(gvk schema.GroupVersionKind, plural string) *apiextensionsv1.CustomResourceDefinition {
	trueBool := true
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s.%s", plural, gvk.Group),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "CustomResourceDefinition",
			APIVersion: "apiextensions/v1",
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: gvk.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   plural,
				Singular: strings.ToLower(gvk.Kind),
				Kind:     gvk.Kind,
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    gvk.Version,
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							XPreserveUnknownFields: &trueBool,
						},
					},
				},
			},
			Scope: apiextensionsv1.ClusterScoped,
		},
	}
}

// applyCRD applies a CRD and waits for it to register successfully.
func applyCRD(ctx context.Context, g *gomega.GomegaWithT, client client.Client, gvk schema.GroupVersionKind, crd client.Object) error {
	err := client.Create(ctx, crd)
	if err != nil {
		return err
	}

	u := &unstructured.UnstructuredList{}
	u.SetGroupVersionKind(gvk)
	g.Eventually(func() error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return client.List(ctx, u)
	}, 5*time.Second, 100*time.Millisecond).ShouldNot(gomega.HaveOccurred())
	return nil
}

func deleteObject(ctx context.Context, c client.Client, obj client.Object, timeout time.Duration) error {
	err := c.Delete(ctx, obj)
	if err != nil {
		if errors2.IsNotFound(err) {
			return nil
		}
		return err
	}

	err = wait.Poll(time.Second, timeout, func() (bool, error) {
		// Get the object name
		name := client.ObjectKeyFromObject(obj)
		err := c.Get(ctx, name, obj)
		if err != nil {
			if errors2.IsGone(err) || errors2.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, err
	})
	return err
}

func ignoreNotFound(err error) error {
	if err != nil && errors2.IsNotFound(err) {
		return nil
	}
	return err
}

// This interface is getting used by tests to check the private objects of objectTracker.
type testExpectations interface {
	IsExpecting(gvk schema.GroupVersionKind, nsName types.NamespacedName) bool
}
