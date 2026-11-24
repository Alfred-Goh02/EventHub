{{/*
EVERY RESOURCE IS WRITTEN ONCE, AS YAML, AND EMITTED AS AN OBJECT, as the other charts do:
toYaml, or toJson when `render=json`, which is how the verifiers read the chart without a
YAML parser.
*/}}
{{- define "edge.emit" -}}
{{- $root := index . 0 -}}
{{- $obj := include (index . 1) $root | fromYaml -}}
{{- if hasKey $obj "Error" }}{{ fail (printf "%s did not render as YAML: %s" (index . 1) $obj.Error) }}{{ end }}
---
{{ if eq ($root.Values.render | default "yaml") "json" }}{{ toJson $obj }}{{ else }}{{ toYaml $obj }}{{ end }}
{{ end -}}

{{- define "edge.service" -}}
{{- $v := .Values -}}
{{- $tls := ne (toString $v.tls.certificateArn) "" -}}
apiVersion: v1
kind: Service
metadata:
  name: edge
  labels:
    app.kubernetes.io/name: edge
    app.kubernetes.io/component: edge
    app.kubernetes.io/part-of: eventhub
    app.kubernetes.io/managed-by: {{ .Release.Service }}
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing
    service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: ip
    service.beta.kubernetes.io/aws-load-balancer-target-group-attributes: preserve_client_ip.enabled=true
    service.beta.kubernetes.io/aws-load-balancer-attributes: load_balancing.cross_zone.enabled=true
    service.beta.kubernetes.io/aws-load-balancer-additional-resource-tags: {{ $v.tags | quote }}
    {{- if $tls }}
    service.beta.kubernetes.io/aws-load-balancer-ssl-cert: {{ $v.tls.certificateArn | quote }}
    service.beta.kubernetes.io/aws-load-balancer-ssl-ports: "web,gateway"
    service.beta.kubernetes.io/aws-load-balancer-ssl-negotiation-policy: {{ $v.tls.policy | quote }}
    {{- end }}
spec:
  type: LoadBalancer
  loadBalancerClass: service.k8s.aws/nlb
  selector:
    {{ $v.selectorLabel }}: "true"
  ports:
    # The page: the web app's container port is named http, and Kong has no port of that name.
    - name: web
      port: {{ if $tls }}443{{ else }}80{{ end }}
      targetPort: http
      protocol: TCP
    # The API: Kong's container port is named proxy, and the web app has no port of that name.
    - name: gateway
      port: {{ $v.ports.gateway }}
      targetPort: proxy
      protocol: TCP
{{- end -}}
