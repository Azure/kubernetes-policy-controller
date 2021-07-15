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

package readiness_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/open-policy-agent/frameworks/constraint/pkg/apis/templates/v1beta1"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/frameworks/constraint/pkg/client/drivers/local"
	podstatus "github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	mutationtypes "github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/target"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	"github.com/open-policy-agent/gatekeeper/third_party/sigs.k8s.io/controller-runtime/pkg/dynamiccache"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// setupManager sets up a controller-runtime manager with registered watch manager.
func setupManager(t *testing.T) (manager.Manager, *watch.Manager) {
	t.Helper()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	metrics.Registry = prometheus.NewRegistry()
	mgr, err := manager.New(cfg, manager.Options{
		HealthProbeBindAddress: "127.0.0.1:29090",
		MetricsBindAddress:     "0",
		NewCache:               dynamiccache.New,
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

func setupOpa(t *testing.T) *opa.Client {
	// initialize OPA
	driver := local.New(local.Tracing(false))
	backend, err := opa.NewBackend(opa.Driver(driver))
	if err != nil {
		t.Fatalf("setting up OPA backend: %v", err)
	}
	client, err := backend.NewClient(opa.Targets(&target.K8sValidationTarget{}))
	if err != nil {
		t.Fatalf("setting up OPA client: %v", err)
	}
	return client
}

func setupController(mgr manager.Manager, wm *watch.Manager, opa *opa.Client, mutationSystem *mutation.System) error {
	tracker, err := readiness.SetupTracker(mgr, mutationSystem != nil)
	if err != nil {
		return fmt.Errorf("setting up tracker: %w", err)
	}

	// ControllerSwitch will be used to disable controllers during our teardown process,
	// avoiding conflicts in finalizer cleanup.
	sw := watch.NewSwitch()

	pod := &corev1.Pod{}
	pod.Name = "no-pod"

	processExcluder := process.Get()

	// Setup all Controllers
	opts := controller.Dependencies{
		Opa:              opa,
		WatchManger:      wm,
		ControllerSwitch: sw,
		Tracker:          tracker,
		GetPod:           func() (*corev1.Pod, error) { return pod, nil },
		ProcessExcluder:  processExcluder,
		MutationCache:    mutationSystem,
	}
	if err := controller.AddToManager(mgr, opts); err != nil {
		return fmt.Errorf("registering controllers: %w", err)
	}
	return nil
}

func Test_AssignMetadata(t *testing.T) {
	g := gomega.NewWithT(t)

	defer func() {
		mutationEnabled := false
		mutation.MutationEnabled = &mutationEnabled
	}()

	mutationEnabled := true
	mutation.MutationEnabled = &mutationEnabled

	os.Setenv("POD_NAME", "no-pod")
	podstatus.DisablePodOwnership()

	// Apply fixtures *before* the controllers are setup.
	err := applyFixtures("testdata")
	g.Expect(err).NotTo(gomega.HaveOccurred(), "applying fixtures")

	// Wire up the rest.
	mgr, wm := setupManager(t)
	opaClient := setupOpa(t)
	mutationCache := mutation.NewSystem()
	if err := setupController(mgr, wm, opaClient, mutationCache); err != nil {
		t.Fatalf("setupControllers: %v", err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	mgrStopped := StartTestManager(ctx, mgr, g)
	defer func() {
		cancelFunc()
		mgrStopped.Wait()
	}()

	g.Eventually(func() (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return probeIsReady(ctx)
	}, 300*time.Second, 1*time.Second).Should(gomega.BeTrue())

	// Verify that the AssignMetadata is present in the cache
	for _, am := range testAssignMetadata {
		id := mutationtypes.MakeID(am)
		exptectedMutator := mutationCache.Get(id)
		g.Expect(exptectedMutator).NotTo(gomega.BeNil(), "expected mutator was not found")
	}
}

func Test_Assign(t *testing.T) {
	g := gomega.NewWithT(t)

	defer func() {
		mutationEnabled := false
		mutation.MutationEnabled = &mutationEnabled
	}()

	mutationEnabled := true
	mutation.MutationEnabled = &mutationEnabled

	os.Setenv("POD_NAME", "no-pod")
	podstatus.DisablePodOwnership()

	// Apply fixtures *before* the controllers are setup.
	err := applyFixtures("testdata")
	g.Expect(err).NotTo(gomega.HaveOccurred(), "applying fixtures")

	// Wire up the rest.
	mgr, wm := setupManager(t)
	opaClient := setupOpa(t)
	mutationCache := mutation.NewSystem()
	if err := setupController(mgr, wm, opaClient, mutationCache); err != nil {
		t.Fatalf("setupControllers: %v", err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	mgrStopped := StartTestManager(ctx, mgr, g)
	defer func() {
		cancelFunc()
		mgrStopped.Wait()
	}()

	g.Eventually(func() (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return probeIsReady(ctx)
	}, 300*time.Second, 1*time.Second).Should(gomega.BeTrue())

	// Verify that the Assign is present in the cache
	for _, am := range testAssign {
		id := mutationtypes.MakeID(am)
		exptectedMutator := mutationCache.Get(id)
		g.Expect(exptectedMutator).NotTo(gomega.BeNil(), "expected mutator was not found")
	}
}

// Test_Tracker verifies that once an initial set of fixtures are loaded into OPA,
// the readiness probe reflects that Gatekeeper is ready to enforce policy. Adding
// additional constraints afterwards will not change the readiness state.
//
// Fixtures are loaded from testdata/ and testdata/post.
// CRDs are loaded from testdata/crds (see TestMain).
// Corresponding expectations are in testdata_test.go.
func Test_Tracker(t *testing.T) {
	g := gomega.NewWithT(t)

	os.Setenv("POD_NAME", "no-pod")
	podstatus.DisablePodOwnership()

	// Apply fixtures *before* the controllers are setup.
	err := applyFixtures("testdata")
	g.Expect(err).NotTo(gomega.HaveOccurred(), "applying fixtures")

	// Wire up the rest.
	mgr, wm := setupManager(t)
	opaClient := setupOpa(t)

	if err := setupController(mgr, wm, opaClient, nil); err != nil {
		t.Fatalf("setupControllers: %v", err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	mgrStopped := StartTestManager(ctx, mgr, g)
	defer func() {
		cancelFunc()
		mgrStopped.Wait()
	}()

	// creating the gatekeeper-system namespace is necessary because that's where
	// status resources live by default
	g.Expect(createGatekeeperNamespace(mgr.GetConfig())).To(gomega.BeNil())

	g.Eventually(func() (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return probeIsReady(ctx)
	}, 300*time.Second, 1*time.Second).Should(gomega.BeTrue())

	// Verify cache (tracks testdata fixtures)
	for _, ct := range testTemplates {
		_, err := opaClient.GetTemplate(ctx, ct)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "checking cache for template")
	}
	for _, c := range testConstraints {
		_, err := opaClient.GetConstraint(ctx, c)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "checking cache for constraint")
	}
	// TODO: Verify data if we add the corresponding API to opa.Client.
	// for _, d := range testData {
	// 	_, err := opaClient.GetData(ctx, c)
	// 	g.Expect(err).NotTo(gomega.HaveOccurred(), "checking cache for constraint")
	// }

	// Add additional templates/constraints and verify that we remain satisfied
	err = applyFixtures("testdata/post")
	g.Expect(err).NotTo(gomega.HaveOccurred(), "applying post fixtures")

	g.Eventually(func() (bool, error) {
		// Verify cache (tracks testdata/post fixtures)
		ctx := context.Background()
		for _, ct := range postTemplates {
			_, err := opaClient.GetTemplate(ctx, ct)
			if err != nil {
				return false, err
			}
		}
		for _, c := range postConstraints {
			_, err := opaClient.GetConstraint(ctx, c)
			if err != nil {
				return false, err
			}
		}

		return true, nil
	}, 10*time.Second, 100*time.Millisecond).Should(gomega.BeTrue(), "verifying cache for post-fixtures")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g.Expect(probeIsReady(ctx)).Should(gomega.BeTrue(), "became unready after adding additional constraints")
}

// Verifies that a Config resource referencing bogus (unregistered) GVKs will not halt readiness.
func Test_Tracker_UnregisteredCachedData(t *testing.T) {
	g := gomega.NewWithT(t)

	// Apply fixtures *before* the controllers are setup.
	err := applyFixtures("testdata")
	g.Expect(err).NotTo(gomega.HaveOccurred(), "applying fixtures")

	// Apply config resource with bogus GVK reference
	err = applyFixtures("testdata/bogus-config")
	g.Expect(err).NotTo(gomega.HaveOccurred(), "applying config")

	// Wire up the rest.
	mgr, wm := setupManager(t)
	opaClient := setupOpa(t)
	if err := setupController(mgr, wm, opaClient, nil); err != nil {
		t.Fatalf("setupControllers: %v", err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	mgrStopped := StartTestManager(ctx, mgr, g)
	defer func() {
		cancelFunc()
		mgrStopped.Wait()
	}()

	g.Eventually(func() (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return probeIsReady(ctx)
	}, 300*time.Second, 1*time.Second).Should(gomega.BeTrue())
}

// Test_CollectDeleted adds resources and starts the readiness tracker, then
// deletes the expected resources and ensures that the trackers watching these
// resources correctly identify the deletions and remove the corresponding expectations.
// Note that the main controllers are not running in order to target testing to the
// readiness tracker.
func Test_CollectDeleted(t *testing.T) {
	type test struct {
		description string
		gvk         schema.GroupVersionKind
		tracker     readiness.Expectations
	}

	g := gomega.NewWithT(t)

	err := applyFixtures("testdata")
	g.Expect(err).NotTo(gomega.HaveOccurred(), "applying fixtures")

	mgr, _ := setupManager(t)
	tracker, err := readiness.SetupTracker(mgr, false)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "setting up tracker")

	ctx, cancelFunc := context.WithCancel(context.Background())
	mgrStopped := StartTestManager(ctx, mgr, g)
	defer func() {
		cancelFunc()
		mgrStopped.Wait()
	}()

	client := mgr.GetClient()

	g.Expect(tracker.Satisfied(ctx)).To(gomega.BeFalse(), "checking the overall tracker is unsatisfied")

	// set up expected GVKs for tests
	cgvk := schema.GroupVersionKind{
		Group:   "constraints.gatekeeper.sh",
		Version: "v1beta1",
		Kind:    "K8sRequiredLabels",
	}

	cm := &corev1.ConfigMap{}
	cmgvk, err := apiutil.GVKForObject(cm, mgr.GetScheme())
	g.Expect(err).NotTo(gomega.HaveOccurred(), "retrieving ConfigMap GVK")
	cmtracker := tracker.ForData(cmgvk)

	ct := &v1beta1.ConstraintTemplate{}
	ctgvk, err := apiutil.GVKForObject(ct, mgr.GetScheme())
	g.Expect(err).NotTo(gomega.HaveOccurred(), "retrieving ConstraintTemplate GVK")

	// note: state can leak between these test cases because we do not reset the environment
	// between them to keep the test short. Trackers are mostly independent per GVK.
	tests := []test{
		{description: "constraints", gvk: cgvk},
		{description: "data (configmaps)", gvk: cmgvk, tracker: cmtracker},
		{description: "templates", gvk: ctgvk},
		// no need to check Config here since it is not actually Expected for readiness
		// (the objects identified in a Config's syncOnly are Expected, tested in data case above)
	}

	for _, tc := range tests {
		var tt readiness.Expectations
		if tc.tracker != nil {
			tt = tc.tracker
		} else {
			tt = tracker.For(tc.gvk)
		}

		g.Eventually(func() (bool, error) {
			return tt.Populated() && !tt.Satisfied(), nil
		}, 10*time.Second, 1*time.Second).
			Should(gomega.BeTrue(), "checking the tracker is tracking %s correctly", tc.description)

		ul := &unstructured.UnstructuredList{}
		ul.SetGroupVersionKind(tc.gvk)
		err = client.List(ctx, ul)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "deleting all %s", tc.description)
		g.Expect(len(ul.Items)).To(gomega.BeNumerically(">=", 1), "expecting nonzero %s", tc.description)

		for index := range ul.Items {
			err = client.Delete(ctx, &ul.Items[index])
			g.Expect(err).NotTo(gomega.HaveOccurred(), "deleting %s %s", tc.description, ul.Items[index].GetName())
		}

		g.Eventually(func() (bool, error) {
			return tt.Satisfied(), nil
		}, 10*time.Second, 1*time.Second).
			Should(gomega.BeTrue(), "checking the tracker collects deletes of %s", tc.description)
	}
}

// probeIsReady checks whether expectations have been satisfied (via the readiness probe).
func probeIsReady(ctx context.Context) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:29090/readyz", http.NoBody)
	if err != nil {
		return false, fmt.Errorf("constructing http request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return false, err
	}

	return resp.StatusCode >= 200 && resp.StatusCode < 400, nil
}
