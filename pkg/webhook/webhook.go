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
	"github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// AddToManagerFuncs is a list of functions to add all Controllers to the Manager.
var AddToManagerFuncs []func(manager.Manager, *client.Client, *process.Excluder, *mutation.System) error

// The below autogen directive is currently disabled because controller-gen has
// no way of specifying the resource name restriction
// DISABLED +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingwebhookconfigurations,verbs=get;list;watch;create;update;patch;delete

// +kubebuilder:rbac:groups="",namespace=gatekeeper-system,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",namespace=gatekeeper-system,resources=events,verbs=create;patch

// AddToManager adds all Controllers to the Manager.
func AddToManager(m manager.Manager, opa *client.Client, processExcluder *process.Excluder, mutationCache *mutation.System) error {
	for _, f := range AddToManagerFuncs {
		if err := f(m, opa, processExcluder, mutationCache); err != nil {
			return err
		}
	}
	return nil
}
