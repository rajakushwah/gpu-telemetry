{{/* Common labels; "component" selects a single workload. */}}
{{- define "gt.labels" -}}
app.kubernetes.io/part-of: gpu-telemetry
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "gt.image" -}}
image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
imagePullPolicy: {{ .Values.image.pullPolicy }}
{{- end }}

{{/* Hardened container settings for the Go services. */}}
{{- define "gt.securityContext" -}}
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities: { drop: ["ALL"] }
{{- end }}

{{- define "gt.brokerURL" -}}
http://{{ .Release.Name }}-broker:9000
{{- end }}

{{- define "gt.dbSecret" -}}
{{ .Values.timescaledb.existingSecret | default (printf "%s-db" .Release.Name) }}
{{- end }}

{{/* DB credentials come only from the Secret; DATABASE_URL expands $(POSTGRES_PASSWORD) at runtime. */}}
{{- define "gt.dbEnv" -}}
- name: STORE
  value: postgres
- name: POSTGRES_PASSWORD
  valueFrom: { secretKeyRef: { name: {{ include "gt.dbSecret" . }}, key: password } }
- name: DATABASE_URL
  value: "postgres://{{ .Values.timescaledb.user }}:$(POSTGRES_PASSWORD)@{{ .Release.Name }}-timescaledb:5432/{{ .Values.timescaledb.database }}?sslmode=disable"
{{- end }}
