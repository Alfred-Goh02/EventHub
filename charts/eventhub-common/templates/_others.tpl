{{/* The Service in front of the pods, on the same port the service listens on. */}}
{{- define "eventhub-common.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ .Values.name }}
  labels:
    {{- include "eventhub-common.labels" . | nindent 4 }}
spec:
  selector:
    {{- include "eventhub-common.selector" . | nindent 4 }}
  ports:
    - name: http
      port: {{ .Values.port }}
      targetPort: http
{{- end -}}

{{/*
At least one replica through a voluntary disruption -- a node drain, a cluster
upgrade. Only rendered for more than one replica: with one, minAvailable 1 would block
every drain for good.
*/}}
{{- define "eventhub-common.pdb" -}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ .Values.name }}
  labels:
    {{- include "eventhub-common.labels" . | nindent 4 }}
spec:
  minAvailable: 1
  selector:
    matchLabels:
      {{- include "eventhub-common.selector" . | nindent 6 }}
{{- end -}}

{{/*
The service's environment, as a Secret: it carries database URLs with passwords. Values
come from whoever installs the chart -- locally infra/local/up.mjs, from compose's own
resolved configuration; on EKS, External Secrets from Secrets Manager.
*/}}
{{- define "eventhub-common.secret" -}}
apiVersion: v1
kind: Secret
metadata:
  name: {{ .Values.name }}-env
  labels:
    {{- include "eventhub-common.labels" . | nindent 4 }}
type: Opaque
stringData:
  {{- range $k, $val := .Values.env }}
  {{ $k }}: {{ toString $val | quote }}
  {{- end }}
{{- end -}}
