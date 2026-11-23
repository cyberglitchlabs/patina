{{/* Chart name, truncated to the 63-character DNS label limit. */}}
{{- define "patina.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified resource name. */}}
{{- define "patina.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* Selector labels: stable across upgrades. */}}
{{- define "patina.selectorLabels" -}}
app.kubernetes.io/name: {{ include "patina.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Common labels. */}}
{{- define "patina.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "patina.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Name of the Secret holding the feed and admin tokens. */}}
{{- define "patina.secretName" -}}
{{- default (include "patina.fullname" .) .Values.existingSecret }}
{{- end }}

{{/* Image reference; the tag defaults to the chart appVersion. */}}
{{- define "patina.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{/* Validate .Values.config: it must parse as TOML, must not set keys the
chart owns, and must name the overrides file when `overrides` is set. */}}
{{- define "patina.validateConfig" -}}
{{- /* Helm has no mustFromToml: fromToml returns {"Error": msg} on bad input.
Only a sole string-valued Error key counts as failure; a config that is exactly
`Error = "x"` is a residual collision we accept. */ -}}
{{- $cfg := fromToml .Values.config -}}
{{- if and (eq (len $cfg) 1) (kindIs "string" (get $cfg "Error")) (hasKey $cfg "Error") }}
{{- fail (printf "config is not valid TOML: %s" (get $cfg "Error")) }}
{{- end }}
{{- if hasKey $cfg "listen" }}
{{- fail "config must not set listen: the chart owns it" }}
{{- end }}
{{- if hasKey $cfg "database_url" }}
{{- fail "config must not set database_url: the chart owns it (use database.urlSecret for Postgres)" }}
{{- end }}
{{- if .Values.overrides }}
{{- $found := false }}
{{- range $s := (get $cfg "sources" | default list) }}
{{- if and (kindIs "map" $s) (eq (toString (get $s "overrides_path" | default "")) "/etc/patina/overrides.toml") }}
{{- $found = true }}
{{- end }}
{{- end }}
{{- if not $found }}
{{- fail "overrides is set but config has no [[sources]] entry with overrides_path = \"/etc/patina/overrides.toml\"; set overrides_path = \"/etc/patina/overrides.toml\"" }}
{{- end }}
{{- end }}
{{- end }}
