{{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Expand the name of the chart.
*/}}
{{- define "superset-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "superset-operator.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "superset-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "superset-operator.labels" -}}
helm.sh/chart: {{ include "superset-operator.chart" . }}
{{ include "superset-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "superset-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "superset-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
control-plane: controller-manager
{{- end }}

{{/*
Service account name.
*/}}
{{- define "superset-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "superset-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Manager image.
*/}}
{{- define "superset-operator.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Validate watch.scope. Must be "cluster" or "namespaces".
*/}}
{{- define "superset-operator.validateWatchScope" -}}
{{- $scope := default "cluster" .Values.watch.scope -}}
{{- if not (has $scope (list "cluster" "namespaces")) -}}
{{- fail (printf "watch.scope must be \"cluster\" or \"namespaces\", got %q" $scope) -}}
{{- end -}}
{{- end }}

{{/*
Canonical comma-separated list of watched namespaces. Empty/whitespace
entries are skipped, duplicates removed. When the resulting list is empty
the release namespace is used.
*/}}
{{- define "superset-operator.watchNamespacesCSV" -}}
{{- $out := list -}}
{{- range default (list) .Values.watch.namespaces -}}
  {{- $t := trim . -}}
  {{- if $t -}}{{- $out = append $out $t -}}{{- end -}}
{{- end -}}
{{- if not $out -}}{{- $out = list .Release.Namespace -}}{{- end -}}
{{- $out | uniq | join "," -}}
{{- end }}

{{/*
Manager RBAC rules. Shared by the cluster-scoped ClusterRole and the
per-namespace Roles so the two render paths can't drift. Include with
{{- include "superset-operator.managerRules" . | nindent N }} at the
desired indentation.
*/}}
{{- define "superset-operator.managerRules" -}}
- apiGroups: [""]
  resources: [configmaps, serviceaccounts, services]
  verbs: [create, delete, get, list, patch, update, watch]
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch]
- apiGroups: [apps]
  resources: [deployments]
  verbs: [create, delete, get, list, patch, update, watch]
- apiGroups: [autoscaling]
  resources: [horizontalpodautoscalers]
  verbs: [create, delete, get, list, patch, update, watch]
- apiGroups: [batch]
  resources: [jobs]
  verbs: [create, delete, get, list, patch, update, watch]
- apiGroups: [events.k8s.io]
  resources: [events]
  verbs: [create, patch, update]
- apiGroups: [gateway.networking.k8s.io]
  resources: [httproutes]
  verbs: [create, delete, get, list, patch, update, watch]
- apiGroups: [monitoring.coreos.com]
  resources: [servicemonitors]
  verbs: [create, delete, get, list, patch, update, watch]
- apiGroups: [networking.k8s.io]
  resources: [ingresses, networkpolicies]
  verbs: [create, delete, get, list, patch, update, watch]
- apiGroups: [policy]
  resources: [poddisruptionbudgets]
  verbs: [create, delete, get, list, patch, update, watch]
- apiGroups: [superset.apache.org]
  resources: [supersets/status]
  verbs: [get, patch, update]
- apiGroups: [superset.apache.org]
  resources: [supersets]
  verbs: [get, list, watch]
{{- end }}
