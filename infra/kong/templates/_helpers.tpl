{{/*
EVERY RESOURCE IS WRITTEN ONCE, AS YAML, AND EMITTED AS AN OBJECT, as charts/eventhub-common
and infra/stores do it: each template is read back with fromYaml and printed with toYaml, or
with toJson when `render=json`, which is how scripts/verify-gateway-chart.mjs and
scripts/verify-eks-install.mjs read the chart without a YAML parser. Both come from the same
object, so what they check is what gets deployed.
*/}}
{{- define "kong.emit" -}}
{{- $root := index . 0 -}}
{{- $obj := include (index . 1) $root | fromYaml -}}
{{- if hasKey $obj "Error" }}{{ fail (printf "%s did not render as YAML: %s" (index . 1) $obj.Error) }}{{ end }}
---
{{ if eq ($root.Values.render | default "yaml") "json" }}{{ toJson $obj }}{{ else }}{{ toYaml $obj }}{{ end }}
{{ end -}}

{{- define "kong.labels" -}}
app.kubernetes.io/name: kong
app.kubernetes.io/component: gateway
app.kubernetes.io/part-of: eventhub
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "kong.selector" -}}
app.kubernetes.io/name: kong
{{- end -}}

{{/*
The two files compose mounts, from this directory. Helm reads a chart's own files only, which
is why the chart is this directory: anywhere else it would need copies.

With LF line endings, whatever the checkout has: core.autocrlf gives a Windows checkout CRLF,
and the ConfigMap -- and the checksum that rolls the pods -- would then differ by the machine
that installed it, for the same commit.
*/}}
{{- define "kong.files" -}}
{{- $yml := .Files.Get "kong.yml" | replace "\r\n" "\n" -}}
{{- $lua := .Files.Get "render-consumer.lua" | replace "\r\n" "\n" -}}
{{- if not $yml }}{{ fail "kong.yml is missing from the chart: it is the gateway's whole configuration" }}{{ end -}}
{{- if not $lua }}{{ fail "render-consumer.lua is missing from the chart: Kong cannot start without the consumer it adds" }}{{ end -}}
{{- dict "kong.yml" $yml "render-consumer.lua" $lua | toJson -}}
{{- end -}}

{{- define "kong.configmap" -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: kong-declarative
  labels:
    {{- include "kong.labels" . | nindent 4 }}
data:
  {{- range $name, $text := include "kong.files" . | fromJson }}
  {{ $name }}: {{ $text | quote }}
  {{- end }}
{{- end -}}

{{- define "kong.deployment" -}}
{{- $v := .Values -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kong
  labels:
    {{- include "kong.labels" . | nindent 4 }}
spec:
  replicas: {{ $v.replicas }}
  selector:
    matchLabels:
      {{- include "kong.selector" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "kong.labels" . | nindent 8 }}
      annotations:
        # A changed kong.yml or render step rolls the pods: Kong reads both only as it
        # starts. A rotated key likewise reaches Kong only when Kong restarts
        # (render-consumer.lua).
        checksum/declarative: {{ include "kong.files" . | sha256sum }}
    spec:
      terminationGracePeriodSeconds: {{ $v.terminationGracePeriodSeconds }}
      # NO SERVICE LINKS. Kong reads its configuration from KONG_* variables, and the Service
      # this chart makes is named kong. Measured on kind with links on: eight of them in the
      # pod, KONG_PORT=tcp://10.96.24.230:8000, KONG_SERVICE_HOST and the rest. Kong 3.9
      # ignores those names today; a release that gave any of them a meaning would take a
      # value nobody chose.
      enableServiceLinks: false
      # Kong DB-less never talks to the Kubernetes API.
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
              {{- include "kong.selector" . | nindent 14 }}
      {{- end }}
      containers:
        - name: kong
          image: {{ $v.image }}
          args:
            {{- toYaml $v.args | nindent 12 }}
          # Compose's environment for Kong, made by infra/eks/up.mjs from
          # `docker compose config`, as every service's is.
          envFrom:
            - secretRef:
                name: kong-env
          ports:
            - name: proxy
              containerPort: {{ $v.proxyPort }}
            - name: status
              containerPort: {{ $v.statusPort }}
          # /status/ready answers 200 once the declarative configuration is loaded and every
          # worker serves it; /status answers while nginx runs at all.
          startupProbe:
            httpGet: { path: /status, port: status }
            periodSeconds: 2
            failureThreshold: 30
          livenessProbe:
            httpGet: { path: /status, port: status }
            periodSeconds: 10
            timeoutSeconds: 2
            failureThreshold: 3
          readinessProbe:
            httpGet: { path: /status/ready, port: status }
            periodSeconds: 5
            timeoutSeconds: 2
            failureThreshold: 2
          lifecycle:
            preStop:
              exec:
                command: ["kong", "quit", "--wait={{ $v.preStopWait }}", "--timeout={{ $v.quitTimeout }}"]
          resources:
            {{- toYaml $v.resources | nindent 12 }}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: declarative
              mountPath: {{ $v.declarativeDir }}
              readOnly: true
            - name: jwt-public
              mountPath: {{ $v.keysDir }}
              readOnly: true
      volumes:
        - name: declarative
          configMap:
            name: kong-declarative
        # The public half only, as every service but Auth gets it (charts/eventhub-common).
        - name: jwt-public
          secret:
            secretName: jwt-keys
            items:
              - key: public.pem
                path: public.pem
{{- end -}}

{{- define "kong.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: kong
  labels:
    {{- include "kong.labels" . | nindent 4 }}
spec:
  # ClusterIP until the domain is decided. With the AWS Load Balancer Controller in the
  # cluster, LoadBalancer would be an internet-facing NLB, and the step that adds one also
  # decides what Kong trusts as the client's address (kong.yml's rate limit says why).
  type: ClusterIP
  selector:
    {{- include "kong.selector" . | nindent 4 }}
  ports:
    - name: proxy
      port: {{ .Values.proxyPort }}
      targetPort: proxy
      protocol: TCP
{{- end -}}

{{- define "kong.pdb" -}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: kong
  labels:
    {{- include "kong.labels" . | nindent 4 }}
spec:
  minAvailable: 1
  selector:
    matchLabels:
      {{- include "kong.selector" . | nindent 6 }}
{{- end -}}
