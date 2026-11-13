{{/*
The service's Deployment.

PROBES ARE SPLIT, AND MUST BE. Liveness asks /health, which touches nothing: a database
outage that failed liveness would restart every replica at once and turn a slow
dependency into an outage. Readiness asks /ready, which asks the database as this
process: a pod that cannot serve leaves the Service's endpoints and comes back by itself.
scripts/verify-helm-charts.mjs requires both paths, different, and each a route the
service really registers.

terminationGracePeriodSeconds is above drainOnSignal's 10 s backstop in @eventhub/http,
so on SIGTERM the process finishes its requests and exits before the kubelet's SIGKILL.
*/}}
{{- define "eventhub-common.deployment" -}}
{{- $v := .Values -}}
{{- if not $v.image.tag }}{{ fail "image.tag is required; never latest" }}{{ end -}}
{{- if eq (toString $v.image.tag) "latest" }}{{ fail "image.tag must not be latest: a tag names one build" }}{{ end -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $v.name }}
  labels:
    {{- include "eventhub-common.labels" . | nindent 4 }}
spec:
  replicas: {{ $v.replicas }}
  selector:
    matchLabels:
      {{- include "eventhub-common.selector" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "eventhub-common.labels" . | nindent 8 }}
      annotations:
        # A changed environment rolls the pods, as compose recreates a container.
        checksum/env: {{ toJson $v.env | sha256sum }}
    spec:
      terminationGracePeriodSeconds: {{ $v.terminationGracePeriodSeconds }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
      {{- if gt (int $v.replicas) 1 }}
      # One replica per node where there are nodes enough: losing a node takes one, not all.
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              {{- include "eventhub-common.selector" . | nindent 14 }}
      {{- end }}
      containers:
        - name: {{ $v.name }}
          image: "{{ $v.image.repository }}:{{ $v.image.tag }}"
          imagePullPolicy: {{ $v.image.pullPolicy }}
          workingDir: {{ $v.workingDir }}
          command:
            {{- toYaml $v.command | nindent 12 }}
          envFrom:
            - secretRef:
                name: {{ $v.name }}-env
          {{- if eq $v.keys "public" }}
          env:
            # The public key as a path: publicKeyFromEnv takes either a value or a file.
            - name: AUTH_PUBLIC_KEY_FILE
              value: /run/eventhub/keys/public.pem
          {{- end }}
          {{- if $v.port }}
          ports:
            - name: http
              containerPort: {{ $v.port }}
          livenessProbe:
            httpGet:
              path: {{ $v.probes.liveness }}
              port: http
            periodSeconds: 10
            timeoutSeconds: 2
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: {{ $v.probes.readiness }}
              port: http
            periodSeconds: 5
            timeoutSeconds: 2
            failureThreshold: 2
          startupProbe:
            httpGet:
              path: {{ $v.probes.liveness }}
              port: http
            periodSeconds: 2
            failureThreshold: 30
          {{- end }}
          resources:
            {{- toYaml $v.resources | nindent 12 }}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          {{- if ne $v.keys "none" }}
          volumeMounts:
            - name: jwt-keys
              # Auth signs, so it reads both halves where jwt.ts looks for them. Every
              # other service gets the public half only, at the path AUTH_PUBLIC_KEY_FILE names.
              mountPath: {{ if eq $v.keys "private" }}{{ $v.workingDir }}/keys{{ else }}/run/eventhub/keys{{ end }}
              readOnly: true
          {{- end }}
      {{- if ne $v.keys "none" }}
      volumes:
        - name: jwt-keys
          secret:
            secretName: jwt-keys
            {{- if eq $v.keys "public" }}
            items:
              - key: public.pem
                path: public.pem
            {{- end }}
      {{- end }}
{{- end -}}
