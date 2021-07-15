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

package assignmetadata

import (
	"os"
	gosync "sync"
	"testing"
	"time"

	"github.com/onsi/gomega"
	mutationsv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/mutations/v1alpha1"
	podstatus "github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/mutatorstatus"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const timeout = time.Second * 15

// setupManager sets up a controller-runtime manager with registered watch manager.
func setupManager(t *testing.T) manager.Manager {
	t.Helper()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	metrics.Registry = prometheus.NewRegistry()
	mgr, err := manager.New(cfg, manager.Options{
		MetricsBindAddress: "0",
		MapperProvider: func(c *rest.Config) (meta.RESTMapper, error) {
			return apiutil.NewDynamicRESTMapper(c)
		},
	})
	if err != nil {
		t.Fatalf("setting up controller manager: %s", err)
	}
	return mgr
}

func TestReconcile(t *testing.T) {
	name := "assignmetadata-test-obj"
	g := gomega.NewGomegaWithT(t)
	mutator := &mutationsv1alpha1.AssignMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: mutationsv1alpha1.AssignMetadataSpec{
			Location: "metadata.labels.test",
			Parameters: mutationsv1alpha1.MetadataParameters{
				Assign: runtime.RawExtension{Raw: []byte(`{"value": "works"}`)},
			},
		},
	}
	objName := types.NamespacedName{Name: name}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr := setupManager(t)
	c := mgr.GetClient()

	// creating the gatekeeper-system namespace is necessary because that's where
	// status resources live by default
	g.Expect(createGatekeeperNamespace(mgr.GetConfig())).To(gomega.BeNil())

	// force mutation to be enabled
	*mutation.MutationEnabled = true

	mSys := mutation.NewSystem()
	tracker, err := readiness.SetupTracker(mgr, true)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	os.Setenv("POD_NAME", "no-pod")
	podstatus.DisablePodOwnership()
	pod := &corev1.Pod{}
	pod.Name = "no-pod"
	rec := newReconciler(mgr, mSys, tracker, func() (*corev1.Pod, error) { return pod, nil })

	recFn, _ := SetupTestReconcile(rec)
	g.Expect(add(mgr, recFn)).NotTo(gomega.HaveOccurred())
	statusAdder := &mutatorstatus.Adder{}
	g.Expect(statusAdder.Add(mgr)).NotTo(gomega.HaveOccurred())

	ctx, cancelFunc := context.WithCancel(context.Background())
	mgrStopped := StartTestManager(ctx, mgr, g)
	once := gosync.Once{}
	testMgrStopped := func() {
		once.Do(func() {
			cancelFunc()
			mgrStopped.Wait()
		})
	}

	defer testMgrStopped()

	t.Run("Can add a mutator", func(t *testing.T) {
		g.Expect(c.Create(ctx, mutator.DeepCopy())).NotTo(gomega.HaveOccurred())
	})

	t.Run("Mutator is reported as enforced", func(t *testing.T) {
		g.Eventually(func() error {
			v := &mutationsv1alpha1.AssignMetadata{}
			v.SetName("assign-test-obj")
			if err := c.Get(ctx, objName, v); err != nil {
				return errors.Wrap(err, "cannot get mutator")
			}
			if len(v.Status.ByPod) < 1 {
				return errors.Errorf("no pod status reported: %+v", v)
			}
			if !v.Status.ByPod[0].Enforced {
				return errors.New("pod not reported as enforced")
			}
			return nil
		}, timeout).Should(gomega.Succeed())
	})

	t.Run("System mutates a resource", func(t *testing.T) {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
		g.Expect(func() error {
			_, err := mSys.Mutate(u, nil)
			return err
		}()).NotTo(gomega.HaveOccurred())
		g.Expect(func() error {
			v, exists, err := unstructured.NestedString(u.Object, "metadata", "labels", "test")
			if err != nil {
				return err
			}
			if !exists {
				return errors.New("mutated value is missing")
			}
			if v != "works" {
				return errors.Errorf(`value = %s, expected "works"`, v)
			}
			return nil
		}()).NotTo(gomega.HaveOccurred())
	})

	t.Run("Mutation deletion is honored", func(t *testing.T) {
		g.Expect(c.Delete(ctx, mutator.DeepCopy())).NotTo(gomega.HaveOccurred())
		g.Eventually(func() error {
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
			g.Expect(func() error {
				_, err := mSys.Mutate(u, nil)
				return err
			}()).NotTo(gomega.HaveOccurred())
			_, exists, err := unstructured.NestedString(u.Object, "metadata", "labels", "test")
			if err != nil {
				return err
			}
			if exists {
				return errors.New("mutated value still exists")
			}
			return nil
		}, timeout).Should(gomega.Succeed())
	})

	testMgrStopped()
}
