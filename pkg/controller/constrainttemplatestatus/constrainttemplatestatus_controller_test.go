package constrainttemplatestatus_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/open-policy-agent/frameworks/constraint/pkg/apis/templates/v1beta1"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/frameworks/constraint/pkg/client/drivers/local"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	podstatus "github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/constrainttemplate"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/target"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	testclient "github.com/open-policy-agent/gatekeeper/test/clients"
	"github.com/open-policy-agent/gatekeeper/third_party/sigs.k8s.io/controller-runtime/pkg/dynamiccache"
)

const timeout = time.Second * 15

// setupManager sets up a controller-runtime manager with registered watch manager.
func setupManager(t *testing.T) (manager.Manager, *watch.Manager) {
	t.Helper()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
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
	g := gomega.NewGomegaWithT(t)
	template := &v1beta1.ConstraintTemplate{
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
	g.Expect(createGatekeeperNamespace(mgr.GetConfig())).To(gomega.BeNil())

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
	adder := constrainttemplate.Adder{
		Opa:              opa,
		WatchManager:     wm,
		ControllerSwitch: cs,
		Tracker:          tracker,
		GetPod:           func(context.Context) (*corev1.Pod, error) { return pod, nil },
	}
	g.Expect(adder.Add(mgr)).NotTo(gomega.HaveOccurred())

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

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	mgr.GetCache().WaitForCacheSync(waitCtx)

	templateCpy := template.DeepCopy()
	t.Run("Constraint template status gets created and reported", func(t *testing.T) {
		g.Eventually(verifyTStatusCount(ctx, c, 0), timeout).Should(gomega.BeNil())
		g.Expect(c.Create(ctx, templateCpy)).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyTStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
		g.Eventually(verifyTByPodStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
	})

	cstr := newDenyAllCstr()
	t.Run("Constraint status gets created and reported", func(t *testing.T) {
		g.Eventually(verifyCStatusCount(ctx, c, 0), timeout).Should(gomega.BeNil())
		g.Expect(c.Create(ctx, cstr)).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyCStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
		g.Eventually(verifyCByPodStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
	})

	fakePod := pod.DeepCopy()
	fakePod.SetName("fake-pod")
	t.Run("Multiple constraint template statuses are reported", func(t *testing.T) {
		fakeTStatus, err := podstatus.NewConstraintTemplateStatusForPod(fakePod, "denyall", mgr.GetScheme())
		fakeTStatus.Status.TemplateUID = templateCpy.UID
		g.Expect(err).To(gomega.BeNil())
		defer func() { g.Expect(ignoreNotFound(c.Delete(ctx, fakeTStatus))).To(gomega.BeNil()) }()
		g.Expect(c.Create(ctx, fakeTStatus)).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyTByPodStatusCount(ctx, c, 2), timeout).Should(gomega.BeNil())
		g.Expect(c.Delete(ctx, fakeTStatus)).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyTByPodStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
	})

	t.Run("Multiple constraint statuses are reported", func(t *testing.T) {
		fakeCStatus, err := podstatus.NewConstraintStatusForPod(fakePod, newDenyAllCstr(), mgr.GetScheme())
		g.Expect(err).To(gomega.BeNil())
		fakeCStatus.Status.ConstraintUID = cstr.GetUID()
		g.Expect(err).To(gomega.BeNil())
		g.Expect(c.Create(ctx, fakeCStatus)).NotTo(gomega.HaveOccurred())
		defer func() { g.Expect(ignoreNotFound(c.Delete(ctx, fakeCStatus))).To(gomega.BeNil()) }()
		g.Eventually(verifyCByPodStatusCount(ctx, c, 2), timeout).Should(gomega.BeNil())
		g.Expect(c.Delete(ctx, fakeCStatus)).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyCByPodStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
	})

	t.Run("Deleting a constraint deletes its status", func(t *testing.T) {
		g.Expect(c.Delete(ctx, cstr)).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyCStatusCount(ctx, c, 0), timeout).Should(gomega.BeNil())
		cstr = newDenyAllCstr()
		g.Expect(c.Create(ctx, cstr)).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyCStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
		g.Eventually(verifyCByPodStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
	})

	t.Run("Deleting a constraint template deletes all statuses", func(t *testing.T) {
		g.Expect(c.Delete(ctx, template.DeepCopy())).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyTStatusCount(ctx, c, 0), timeout).Should(gomega.BeNil())
		g.Eventually(verifyCStatusCount(ctx, c, 0), timeout).Should(gomega.BeNil())
		// need to manually delete constraint currently as garbage collection does not run on the test API server
		g.Expect(c.Delete(ctx, newDenyAllCstr())).NotTo(gomega.HaveOccurred())
	})

	templateCpy = template.DeepCopy()
	cstr = newDenyAllCstr()
	t.Run("Deleting a constraint template deletes all statuses for the current pod", func(t *testing.T) {
		g.Eventually(verifyTStatusCount(ctx, c, 0), timeout).Should(gomega.BeNil())
		g.Expect(c.Create(ctx, templateCpy)).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyTStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
		g.Eventually(verifyTByPodStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
		g.Eventually(verifyCStatusCount(ctx, c, 0), timeout).Should(gomega.BeNil())
		g.Expect(c.Create(ctx, cstr)).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyCStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
		g.Eventually(verifyCByPodStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())

		fakeTStatus, err := podstatus.NewConstraintTemplateStatusForPod(fakePod, "denyall", mgr.GetScheme())
		fakeTStatus.Status.TemplateUID = templateCpy.UID
		g.Expect(err).To(gomega.BeNil())
		g.Expect(c.Create(ctx, fakeTStatus)).NotTo(gomega.HaveOccurred())
		defer func() { g.Expect(ignoreNotFound(c.Delete(ctx, fakeTStatus))).NotTo(gomega.HaveOccurred()) }()

		fakeCStatus, err := podstatus.NewConstraintStatusForPod(fakePod, newDenyAllCstr(), mgr.GetScheme())
		fakeCStatus.Status.ConstraintUID = cstr.GetUID()
		g.Expect(err).To(gomega.BeNil())
		g.Expect(c.Create(ctx, fakeCStatus)).NotTo(gomega.HaveOccurred())
		defer func() { g.Expect(ignoreNotFound(c.Delete(ctx, fakeCStatus))).NotTo(gomega.HaveOccurred()) }()

		g.Eventually(verifyTByPodStatusCount(ctx, c, 2), 30*timeout).Should(gomega.BeNil())
		g.Eventually(verifyCByPodStatusCount(ctx, c, 2), timeout).Should(gomega.BeNil())
		g.Expect(c.Delete(ctx, template.DeepCopy())).NotTo(gomega.HaveOccurred())
		g.Eventually(verifyTStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
		g.Eventually(verifyCStatusCount(ctx, c, 1), timeout).Should(gomega.BeNil())
	})
}

func verifyTStatusCount(ctx context.Context, c client.Client, expected int) func() error {
	return func() error {
		statuses := &podstatus.ConstraintTemplatePodStatusList{}
		if err := c.List(ctx, statuses, client.InNamespace("gatekeeper-system")); err != nil {
			return err
		}
		if len(statuses.Items) != expected {
			return fmt.Errorf("status count = %d, wanted %d, statuses: %+v", len(statuses.Items), expected, statuses.Items)
		}
		return nil
	}
}

func verifyTByPodStatusCount(ctx context.Context, c client.Client, expected int) func() error {
	return func() error {
		ct := &v1beta1.ConstraintTemplate{}
		if err := c.Get(ctx, types.NamespacedName{Name: "denyall"}, ct); err != nil {
			return err
		}
		if len(ct.Status.ByPod) != expected {
			return fmt.Errorf("status count = %d, wanted %d", len(ct.Status.ByPod), expected)
		}
		return nil
	}
}

func verifyCStatusCount(ctx context.Context, c client.Client, expected int) func() error {
	return func() error {
		statuses := &podstatus.ConstraintPodStatusList{}
		if err := c.List(ctx, statuses, client.InNamespace("gatekeeper-system")); err != nil {
			return err
		}
		if len(statuses.Items) != expected {
			return fmt.Errorf("status count = %d, wanted %d, statuses: %+v", len(statuses.Items), expected, statuses.Items)
		}
		return nil
	}
}

func verifyCByPodStatusCount(ctx context.Context, c client.Client, expected int) func() error {
	return func() error {
		cstr := newDenyAllCstr()
		if err := c.Get(ctx, types.NamespacedName{Name: "denyallconstraint"}, cstr); err != nil {
			return err
		}
		statuses, _, err := unstructured.NestedSlice(cstr.Object, "status", "byPod")
		if err != nil {
			return err
		}
		if len(statuses) != expected {
			return fmt.Errorf("status count = %d, wanted %d", len(statuses), expected)
		}
		return nil
	}
}

func ignoreNotFound(err error) error {
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
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
