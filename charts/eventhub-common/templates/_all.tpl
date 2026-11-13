{{/*
Everything a service needs, in one include: `{{ include "eventhub-common.all" . }}`.

EVERY RESOURCE IS WRITTEN ONCE, AS YAML, AND EMITTED AS AN OBJECT. Each template below
is read back with fromYaml and printed with toYaml -- or with toJson when
`render=json`, which is how scripts/verify-helm-charts.mjs reads a chart without a YAML
parser. The two outputs come from the same object, so what the verifier checks is what
gets deployed.
*/}}
{{- define "eventhub-common.all" -}}
{{- /*
  The environment Secret only when the installer hands over an environment. ArgoCD
  installs from git, and passwords are not in git: there the Secret `<name>-env` is
  made beside the chart -- by infra/local/up.mjs from compose locally, by External
  Secrets on EKS -- and the Deployment names it either way.
*/}}
{{- if .Values.env }}
{{- include "eventhub-common.emit" (list . "eventhub-common.secret") }}
{{- end }}
{{- include "eventhub-common.emit" (list . "eventhub-common.deployment") }}
{{- if .Values.port }}
{{- include "eventhub-common.emit" (list . "eventhub-common.service") }}
{{- end }}
{{- if gt (int .Values.replicas) 1 }}
{{- include "eventhub-common.emit" (list . "eventhub-common.pdb") }}
{{- end }}
{{- end -}}

{{- define "eventhub-common.emit" -}}
{{- $root := index . 0 -}}
{{- $obj := include (index . 1) $root | fromYaml -}}
{{- if hasKey $obj "Error" }}{{ fail (printf "%s did not render as YAML: %s" (index . 1) $obj.Error) }}{{ end }}
---
{{ if eq ($root.Values.render | default "yaml") "json" }}{{ toJson $obj }}{{ else }}{{ toYaml $obj }}{{ end }}
{{ end -}}

{{- define "eventhub-common.labels" -}}
app.kubernetes.io/name: {{ .Values.name }}
app.kubernetes.io/part-of: eventhub
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "eventhub-common.selector" -}}
app.kubernetes.io/name: {{ .Values.name }}
{{- end -}}
