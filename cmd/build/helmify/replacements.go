package main

var replacements = map[string]string{
	`HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_CONTAINER_RESOURCES: ""`: `{{- toYaml .Values.controllerManager.resources | nindent 10 }}`,

	`HELMSUBST_DEPLOYMENT_AUDIT_CONTAINER_RESOURCES: ""`: `{{- toYaml .Values.audit.resources | nindent 10 }}`,

	"HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_HOST_NETWORK": `{{ .Values.controllerManager.hostNetwork }}`,

	"HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_PORT": `{{ .Values.controllerManager.port }}`,

	"HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_HEALTH_PORT": `{{ .Values.controllerManager.healthPort }}`,

	"HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_PROMETHEUS_PORT": `{{ .Values.controllerManager.prometheusPort }}`,

	"HELMSUBST_DEPLOYMENT_AUDIT_HOST_NETWORK": `{{ .Values.audit.hostNetwork }}`,

	`HELMSUBST_DEPLOYMENT_AUDIT_NODE_SELECTOR: ""`: `{{- toYaml .Values.audit.nodeSelector | nindent 8 }}`,

	`HELMSUBST_DEPLOYMENT_AUDIT_AFFINITY: ""`: `{{- toYaml .Values.audit.affinity | nindent 8 }}`,

	`HELMSUBST_DEPLOYMENT_AUDIT_TOLERATIONS: ""`: `{{- toYaml .Values.audit.tolerations | nindent 8 }}`,

	`HELMSUBST_DEPLOYMENT_AUDIT_IMAGE_PULL_SECRETS: ""`: `{{- toYaml .Values.image.pullSecrets | nindent 8 }}`,

	"HELMSUBST_DEPLOYMENT_AUDIT_PRIORITY_CLASS_NAME": `{{ .Values.audit.priorityClassName }}`,

	`HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_NODE_SELECTOR: ""`: `{{- toYaml .Values.controllerManager.nodeSelector | nindent 8 }}`,

	`HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_AFFINITY: ""`: `{{- toYaml .Values.controllerManager.affinity | nindent 8 }}`,

	`HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_TOLERATIONS: ""`: `{{- toYaml .Values.controllerManager.tolerations | nindent 8 }}`,

	`HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_IMAGE_PULL_SECRETS: ""`: `{{- toYaml .Values.image.pullSecrets | nindent 8 }}`,

	"HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_PRIORITY_CLASS_NAME": `{{ .Values.controllerManager.priorityClassName }}`,

	"HELMSUBST_DEPLOYMENT_REPLICAS": `{{ .Values.replicas }}`,

	`HELMSUBST_ANNOTATIONS: ""`: `{{- toYaml .Values.podAnnotations | trim | nindent 8 }}`,

	"HELMSUBST_SECRET_ANNOTATIONS": `{{- toYaml .Values.secretAnnotations | trim | nindent 4 }}`,

	"HELMSUBST_VALIDATING_WEBHOOK_TIMEOUT": `{{ .Values.validatingWebhookTimeoutSeconds }}`,

	"HELMSUBST_VALIDATING_WEBHOOK_FAILURE_POLICY": `{{ .Values.validatingWebhookFailurePolicy }}`,

	"HELMSUBST_VALIDATING_WEBHOOK_CHECK_IGNORE_FAILURE_POLICY": `{{ .Values.validatingWebhookCheckIgnoreFailurePolicy }}`,

	"HELMSUBST_RESOURCEQUOTA_POD_LIMIT": `{{ .Values.podCountLimit }}`,

	"HELMSUBST_VALIDATING_WEBHOOK_OPERATION_RULES": `
    - CREATE
    - UPDATE
    {{- if .Values.enableDeleteOperations }}
    - DELETE
    {{- end}}`,

	"HELMSUBST_PDB_CONTROLLER_MANAGER_MINAVAILABLE": `{{ .Values.pdb.controllerManager.minAvailable }}`,

	`HELMSUBST_SERVICE_TYPE: ""`: `{{- if .Values.service }}
  type: {{  .Values.service.type | default "ClusterIP" }}
    {{- if .Values.service.loadBalancerIP }}
  loadBalancerIP: {{ .Values.service.loadBalancerIP }}
    {{- end }}
  {{- end }}`,
	"- HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_DISABLED_BUILTIN": `
        {{- range .Values.disabledBuiltins}}
        - --disable-opa-builtin={{ . }}
        {{- end }}`,
	"- HELMSUBST_DEPLOYMENT_CONTROLLER_MANAGER_EXEMPT_NAMESPACES": `
        {{- range .Values.controllerManager.exemptNamespaces}}
        - --exempt-namespace={{ . }}
        {{- end }}`,
}
