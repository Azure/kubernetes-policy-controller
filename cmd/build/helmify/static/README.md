# Gatekeeper Helm Chart

## Get Repo Info

```console
helm repo add gatekeeper https://open-policy-agent.github.io/gatekeeper/charts
helm repo update
```

_See [helm repo](https://helm.sh/docs/helm/helm_repo/) for command documentation._

## Install Chart

```console
# Helm install with gatekeeper-system namespace already created
$ helm install -n gatekeeper-system [RELEASE_NAME] gatekeeper/gatekeeper

# Helm install and create namespace
$ helm install -n gatekeeper-system [RELEASE_NAME] gatekeeper/gatekeeper --create-namespace

```

_See [parameters](#parameters) below._

_See [helm install](https://helm.sh/docs/helm/helm_install/) for command documentation._

## Upgrade Chart

**Upgrading from < v3.4.0**
Chart 3.4.0 deprecates support for Helm 2 and also removes the creation of the `gatekeeper-system` Namespace from within the chart. This follows Helm 3 Best Practices.

Option 1:  
A simple way to upgrade is to uninstall first and re-install with 3.4.0 or greater. 

```console
$ helm uninstall gatekeeper
$ helm install -n gatekeeper-system [RELEASE_NAME] gatekeeper/gatekeeper --create-namespace

```

Option 2:  
Run the `helm_migrate.sh` script before installing the 3.4.0 or greater chart. This will remove the Helm secret for the original release, while keeping all of the resources. It then updates the annotations of the resources so that the new chart can import and manage them.

```console
$ helm_migrate.sh
$ helm install -n gatekeeper-system gatekeeper gatekeeper/gatekeeper
```

**Upgrading from >= v3.4.0**
```console
$ helm upgrade -n gatekeeper-system [RELEASE_NAME] gatekeeper/gatekeeper
```

_See [helm 2 to 3](https://helm.sh/docs/topics/v2_v3_migration/) for Helm 2 migration documentation._


## Exempting Namespace

The Helm chart automatically sets the Gatekeeper flag `--exempt-namespace={{ .Release.Namespace }}` in order to exempt the namespace where the chart is installed, and adds the `admission.gatekeeper.sh/ignore` label to the namespace during a post-install hook.

_See [Exempting Namespaces](https://open-policy-agent.github.io/gatekeeper/website/docs/exempt-namespaces) for more information._

## Parameters

| Parameter                                    | Description                                                                            | Default                                                                   |
| :--------------------------------------------| :--------------------------------------------------------------------------------------| :-------------------------------------------------------------------------|
| postInstall.labelNamespace.enabled           | Add labels to the namespace during post install hooks                                  | `true`                                                                    |
| postInstall.labelNamespace.image.repository  | Image with kubectl to label the namespace                                              | `line/kubectl-kustomize`                                                  |
| postInstall.labelNamespace.image.tag         | Image tag                                                                              | `1.20.4-4.0.5`                                                            |
| postInstall.labelNamespace.image.pullPolicy  | Image pullPolicy                                                                       | `IfNotPresent`                                                            |
| postInstall.labelNamespace.image.pullSecrets | Image pullSecrets                                                                      | `[]`                                                                      |
| auditInterval                                | The frequency with which audit is run                                                  | `60`                                                                      |
| constraintViolationsLimit                    | The maximum # of audit violations reported on a constraint                             | `20`                                                                      |
| auditFromCache                               | Take the roster of resources to audit from the OPA cache                               | `false`                                                                   |
| auditChunkSize                               | Chunk size for listing cluster resources for audit (alpha feature)                     | `0`                                                                       |
| auditMatchKindOnly                           | Only check resources of the kinds specified in all constraints defined in the cluster. | `false`                                                                        |
| disableValidatingWebhook                     | Disable the validating webhook                                                         | `false`                                                                   |
| validatingWebhookTimeoutSeconds              | The timeout for the validating webhook in seconds                                      | `3`                                                                       |
| validatingWebhookFailurePolicy               | The failurePolicy for the validating webhook                                           | `Ignore`                                                                       |
| validatingWebhookCheckIgnoreFailurePolicy    | The failurePolicy for the check-ignore-label validating webhook                        | `Fail`                                                                       |
| enableDeleteOperations                       | Enable validating webhook for delete operations                                        | `false`                                                                   |
| experimentalEnableMutation                   | Enable mutation  (alpha feature)                                                       | `false`                                                                   |
| emitAdmissionEvents                          | Emit K8s events in gatekeeper namespace for admission violations (alpha feature)       | `false`                                                                   |
| emitAuditEvents                              | Emit K8s events in gatekeeper namespace for audit violations (alpha feature)           | `false`                                                                   |
| logDenies                                    | Log detailed info on each deny                                                         | `false`                                                                   |
| logLevel                                     | Minimum log level                                                                      | `INFO`                                                                    |
| image.pullPolicy                             | The image pull policy                                                                  | `IfNotPresent`                                                            |
| image.repository                             | Image repository                                                                       | `openpolicyagent/gatekeeper`                                              |
| image.release                                | The image release tag to use                                                           | Current release version: `v3.6.0-beta.2`                                         |
| image.pullSecrets                            | Specify an array of imagePullSecrets                                                   | `[]`                                                                      |
| resources                                    | The resource request/limits for the container image                                    | limits: 1 CPU, 512Mi, requests: 100mCPU, 256Mi                            |
| nodeSelector                                 | The node selector to use for pod scheduling                                            | `kubernetes.io/os: linux`                                                 |
| affinity                                     | The node affinity to use for pod scheduling                                            | `{}`                                                                      |
| tolerations                                  | The tolerations to use for pod scheduling                                              | `[]`                                                                      |
| controllerManager.healthPort                 | Health port for controller manager                                                     | `9090`                                                                    |
| controllerManager.port                       | Webhook-server port for controller manager                                             | `8443`                                                                    |
| controllerManager.prometheusPort             | Metrics port for controller manager                                                    | `8888`                                                                    |
| controllerManager.priorityClassName          | Priority class name for controller manager                                             | `system-cluster-critical`                                                 |
| controllerManager.exemptNamespaces           | The namespaces to exempt                                                                                   | `[]`                                                 |
| controllerManager.hostNetwork                | Enables controllerManager to be deployed on hostNetwork                                | `false`                                                                   |
| audit.priorityClassName                      | Priority class name for audit controller                                               | `system-cluster-critical`                                                 |
| audit.hostNetwork                            | Enables audit to be deployed on hostNetwork                                            | `false`                                                                   |
| replicas                                     | The number of Gatekeeper replicas to deploy for the webhook                            | `3`                                                                       |
| podAnnotations                               | The annotations to add to the Gatekeeper pods                                          | `container.seccomp.security.alpha.kubernetes.io/manager: runtime/default` |
| podLabels                                    | The labels to add to the Gatekeeper pods                                               | `{}`                                                                      |
| podCountLimit                                | The maximum number of Gatekeeper pods to run                                           | `100`                                                                     |
| secretAnnotations                            | The annotations to add to the Gatekeeper secrets                                       | `{}`                                                                      |
| pdb.controllerManager.minAvailable           | The number of controller manager pods that must still be available after an eviction   | `1`                                                                       |
| service.type                                 | Service type                                                                           | `ClusterIP`                                                               |
| service.loadBalancerIP                       | The IP address of LoadBalancer service                                                 | ``                                                                        |

## Contributing Changes

This Helm chart is autogenerated from the Gatekeeper static manifest. The
generator code lives under `cmd/build/helmify`. To make modifications to this
template, please edit `kustomization.yaml`, `kustomize-for-helm.yaml` and
`replacements.go` under that directory and then run `make manifests`. Your
changes will show up in the `manifest_staging` directory and will be promoted
to the root `charts` directory the next time a Gatekeeper release is cut.
