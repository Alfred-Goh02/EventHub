{{/*
EVERY RESOURCE IS WRITTEN ONCE, AS YAML, AND EMITTED AS AN OBJECT, as charts/eventhub-common
does it: each template is read back with fromYaml and printed with toYaml, or with toJson
when `render=json`, which is how scripts/verify-store-chart.mjs and infra/stores/secrets.mjs
read the chart without a YAML parser. Both come from the same object, so what they check is
what gets deployed.

emit takes (list $root "template" $context).
*/}}
{{- define "stores.emit" -}}
{{- $root := index . 0 -}}
{{- $obj := include (index . 1) (index . 2) | fromYaml -}}
{{- if hasKey $obj "Error" }}{{ fail (printf "%s did not render as YAML: %s" (index . 1) $obj.Error) }}{{ end }}
---
{{ if eq ($root.Values.render | default "yaml") "json" }}{{ toJson $obj }}{{ else }}{{ toYaml $obj }}{{ end }}
{{ end -}}

{{- define "stores.labels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/component: store
app.kubernetes.io/part-of: eventhub
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end -}}

{{- define "stores.selector" -}}
app.kubernetes.io/name: {{ .name }}
{{- end -}}

{{- define "stores.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ .name }}
  labels:
    {{- include "stores.labels" . | nindent 4 }}
spec:
  # Never LoadBalancer or NodePort: no store has authentication worth the name, and
  # with the AWS Load Balancer Controller in the cluster a LoadBalancer Service would be
  # an internet-facing NLB.
  type: ClusterIP
  selector:
    {{- include "stores.selector" . | nindent 4 }}
  ports:
    {{- range .s.servicePorts }}
    - name: p{{ . }}
      port: {{ . }}
      targetPort: {{ . }}
      protocol: TCP
    {{- end }}
{{- end -}}

{{- /*
A StatefulSet's own Service, headless and publishing pods before they are ready, which
is what a StatefulSet's serviceName is for. The apps dial the ClusterIP Service above.
*/}}
{{- define "stores.headless" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ .name }}-headless
  labels:
    {{- include "stores.labels" . | nindent 4 }}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    {{- include "stores.selector" . | nindent 4 }}
  ports:
    {{- range .s.servicePorts }}
    - name: p{{ . }}
      port: {{ . }}
      targetPort: {{ . }}
      protocol: TCP
    {{- end }}
{{- end -}}

{{- define "stores.workload" -}}
{{- $s := .s -}}
apiVersion: apps/v1
kind: {{ $s.kind }}
metadata:
  name: {{ .name }}
  labels:
    {{- include "stores.labels" . | nindent 4 }}
spec:
  replicas: 1
  {{- if eq $s.kind "StatefulSet" }}
  serviceName: {{ .name }}-headless
  # A claim goes with its StatefulSet. Retained, every deleted StatefulSet would leave a
  # volume billing, and terraform destroy knows nothing about it.
  persistentVolumeClaimRetentionPolicy:
    whenDeleted: Delete
    whenScaled: Delete
  {{- else }}
  # One copy at a time: a second Redis or Mailpit beside the first is a second, empty one.
  strategy:
    type: Recreate
  {{- end }}
  selector:
    matchLabels:
      {{- include "stores.selector" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "stores.labels" . | nindent 8 }}
        {{- if $s.heavy }}
        eventhub/store-weight: heavy
        {{- end }}
    spec:
      # NO SERVICE LINKS. With them, a Service named kafka puts KAFKA_PORT=tcp://... into
      # every pod, and the Confluent images read every KAFKA_* and SCHEMA_REGISTRY_*
      # variable as configuration.
      enableServiceLinks: false
      # No store talks to the Kubernetes API.
      automountServiceAccountToken: false
      {{- if .root.Values.priorityClass.name }}
      priorityClassName: {{ .root.Values.priorityClass.name }}
      {{- end }}
      terminationGracePeriodSeconds: {{ $s.terminationGracePeriodSeconds }}
      {{- with $s.podSecurityContext }}
      securityContext:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- if $s.heavy }}
      # The three JVM-sized stores spread over the nodes where they can: preferred, not
      # required, because two nodes cannot hold three apart.
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                topologyKey: kubernetes.io/hostname
                labelSelector:
                  matchLabels:
                    eventhub/store-weight: heavy
      {{- end }}
      containers:
        - name: {{ .name }}
          image: {{ $s.image }}
          imagePullPolicy: IfNotPresent
          {{- with $s.args }}
          args:
            {{- range . }}
            - {{ . | quote }}
            {{- end }}
          {{- end }}
          ports:
            {{- range $s.containerPorts }}
            - containerPort: {{ . }}
              protocol: TCP
            {{- end }}
          {{- if or $s.env $s.secretEnv }}
          env:
            {{- range $k, $v := $s.env }}
            - name: {{ $k | quote }}
              value: {{ $v | quote }}
            {{- end }}
            {{- range $k, $secret := $s.secretEnv }}
            - name: {{ $k | quote }}
              valueFrom:
                secretKeyRef:
                  name: {{ $secret }}
                  key: {{ $k }}
            {{- end }}
          {{- end }}
          readinessProbe:
            {{- toYaml $s.readinessProbe | nindent 12 }}
          livenessProbe:
            {{- toYaml $s.livenessProbe | nindent 12 }}
          resources:
            {{- toYaml $s.resources | nindent 12 }}
          {{- if or $s.volume $s.initFrom }}
          volumeMounts:
            {{- if $s.volume }}
            - name: data
              mountPath: {{ $s.volume.mountPath }}
            {{- end }}
            {{- if $s.initFrom }}
            - name: init
              mountPath: {{ $s.initFrom.mountPath }}
              readOnly: true
            {{- end }}
          {{- end }}
      {{- if $s.initFrom }}
      volumes:
        - name: init
          secret:
            secretName: {{ $s.initFrom.secret }}
      {{- end }}
  {{- if and (eq $s.kind "StatefulSet") $s.volume }}
  volumeClaimTemplates:
    - metadata:
        name: data
        labels:
          {{- include "stores.labels" . | nindent 10 }}
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: {{ .root.Values.storageClass.name }}
        resources:
          requests:
            storage: {{ $s.volume.size }}
  {{- end }}
{{- end -}}

{{- define "stores.storageclass" -}}
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: {{ .root.Values.storageClass.name }}
  labels:
    app.kubernetes.io/part-of: eventhub
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  encrypted: "true"
  csi.storage.k8s.io/fstype: ext4
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
allowVolumeExpansion: true
{{- end -}}

{{- define "stores.priorityclass" -}}
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: {{ .root.Values.priorityClass.name }}
  labels:
    app.kubernetes.io/part-of: eventhub
value: {{ .root.Values.priorityClass.value }}
globalDefault: false
preemptionPolicy: PreemptLowerPriority
description: "EventHub's data stores: scheduled ahead of the app pods, which can be rescheduled anywhere."
{{- end -}}
