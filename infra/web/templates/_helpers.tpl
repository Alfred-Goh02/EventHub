{{/*
EVERY RESOURCE IS WRITTEN ONCE, AS YAML, AND EMITTED AS AN OBJECT, as the other charts do:
toYaml, or toJson when `render=json`, which is how scripts/verify-web-chart.mjs reads the
chart without a YAML parser.
*/}}
{{- define "web.emit" -}}
{{- $root := index . 0 -}}
{{- $obj := include (index . 1) $root | fromYaml -}}
{{- if hasKey $obj "Error" }}{{ fail (printf "%s did not render as YAML: %s" (index . 1) $obj.Error) }}{{ end }}
---
{{ if eq ($root.Values.render | default "yaml") "json" }}{{ toJson $obj }}{{ else }}{{ toYaml $obj }}{{ end }}
{{ end -}}

{{- define "web.labels" -}}
app.kubernetes.io/name: web
app.kubernetes.io/component: web
app.kubernetes.io/part-of: eventhub
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "web.selector" -}}
app.kubernetes.io/name: web
{{- end -}}

{{- define "web.deployment" -}}
{{- $v := .Values -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  labels:
    {{- include "web.labels" . | nindent 4 }}
spec:
  replicas: {{ $v.replicas }}
  selector:
    matchLabels:
      {{- include "web.selector" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "web.labels" . | nindent 8 }}
    spec:
      terminationGracePeriodSeconds: {{ $v.terminationGracePeriodSeconds }}
      # Nothing here reads its environment as configuration beyond PORT, HOST and the
      # address in envSecret, and a Service named web would add WEB_PORT and the rest for
      # nothing.
      enableServiceLinks: false
      # The web server never talks to the Kubernetes API.
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: {{ $v.runAsUser }}
        runAsGroup: {{ $v.runAsUser }}
      {{- if gt (int $v.replicas) 1 }}
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              {{- include "web.selector" . | nindent 14 }}
      {{- end }}
      containers:
        - name: web
          image: "{{ $v.image.repository }}:{{ $v.image.tag }}"
          imagePullPolicy: IfNotPresent
          # APP_URL_TEMPLATE, the cluster's address, which the installer puts here
          # (infra/eks/public.mjs). Not optional: without it every page would be served on the
          # sample data, which looks like a working site with nobody's events in it.
          envFrom:
            - secretRef:
                name: {{ $v.envSecret | quote }}
          ports:
            - name: http
              containerPort: {{ $v.port }}
          startupProbe:
            httpGet: { path: {{ $v.probes.liveness }}, port: http }
            periodSeconds: 2
            failureThreshold: 30
          livenessProbe:
            httpGet: { path: {{ $v.probes.liveness }}, port: http }
            periodSeconds: 10
            timeoutSeconds: 2
            failureThreshold: 3
          readinessProbe:
            httpGet: { path: {{ $v.probes.readiness }}, port: http }
            periodSeconds: 5
            timeoutSeconds: 2
            failureThreshold: 2
          lifecycle:
            preStop:
              exec:
                command: ["sleep", "{{ $v.preStopSeconds }}"]
          resources:
            {{- toYaml $v.resources | nindent 12 }}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
{{- end -}}

{{- define "web.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: web
  labels:
    {{- include "web.labels" . | nindent 4 }}
spec:
  # ClusterIP until the domain is decided, as the gateway's is.
  type: ClusterIP
  selector:
    {{- include "web.selector" . | nindent 4 }}
  ports:
    - name: http
      port: {{ .Values.port }}
      targetPort: http
      protocol: TCP
{{- end -}}

{{- define "web.pdb" -}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: web
  labels:
    {{- include "web.labels" . | nindent 4 }}
spec:
  minAvailable: 1
  selector:
    matchLabels:
      {{- include "web.selector" . | nindent 6 }}
{{- end -}}
