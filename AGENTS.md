<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# CLAUDE.md

## Project Overview

Kubernetes operator for Apache Superset, built with the Go-based Operator SDK and Go 1.26+.
- Module: `github.com/apache/superset-kubernetes-operator`
- API group: `superset.apache.org/v1alpha1`
- License: Apache 2.0

## Developer Guide

See `docs/developer-guide.md` for development setup, make commands, testing philosophy, code generation workflow, linting, CI/supply chain, and contributing guidelines. Adhere to the conventions documented there.

## Architecture

The operator uses a **two-tier CRD architecture** where the parent `Superset` resource resolves shared top-level and per-component configuration into fully-flattened child CRDs. See `docs/architecture.md` for detailed design.

### CRD Hierarchy

- **Superset** (parent) — top-level CR with shared spec (top-level + per-component), environment, secretKey/secretKeyFrom, metastore (with uriFrom/passwordFrom), valkey (cache/broker/results), config, InitSpec, NetworkingSpec, MonitoringSpec
- **SupersetInit** — init lifecycle manager: bare Pods + ConfigMap (enabled by default, disable via `spec.init.disabled: true`)
- **SupersetWebServer** — gunicorn web server Deployment + Service + ConfigMap
- **SupersetCeleryWorker** — async task worker Deployment + ConfigMap
- **SupersetCeleryBeat** — periodic task scheduler Deployment + ConfigMap (singleton, always 1 replica)
- **SupersetCeleryFlower** — Celery monitoring UI Deployment + Service + ConfigMap
- **SupersetWebsocketServer** — websocket server Deployment + Service (Node.js, no ConfigMap)
- **SupersetMcpServer** — FastMCP server Deployment + Service + ConfigMap (Python, port 8088)

**Key principles:**
- **Parent resolves, children execute.** All layering logic lives in the parent controller. Child CRs are fully flattened — no inheritance to trace.
- **Presence = enabled.** No `enabled: true/false`. If `celeryWorker: {}` is set, workers deploy. Exception: `init` is enabled by default; disable via `spec.init.disabled: true`.
- **Secrets never touch ConfigMaps.** In prod mode, CRD CEL validation rejects inline `secretKey`, `metastore.uri`, `metastore.password`, and `valkey.password`. Use `secretKeyFrom`, `metastore.uriFrom`, `metastore.passwordFrom`, or `valkey.passwordFrom` to reference Kubernetes Secrets (operator injects `valueFrom.secretKeyRef` env vars). In dev mode, inline secrets are allowed.
- **Per-component config rendering.** All Python components get `SECRET_KEY` rendered from `SUPERSET_OPERATOR__SECRET_KEY`. Web gets port config. Structured metastore renders an f-string URI from `SUPERSET_OPERATOR__DB_*` env vars. When `spec.valkey` is set, operator renders all cache configs (`CACHE_CONFIG`, `DATA_CACHE_CONFIG`, etc.), `CeleryConfig`, and `RESULTS_BACKEND` from `SUPERSET_OPERATOR__VALKEY_*` env vars. Websocket gets nothing (Node.js).

## Directory Layout

- `api/v1alpha1/` — CRD type definitions
  - `shared_types.go` — ImageSpec, MetastoreSpec, ValkeySpec (ValkeySSLSpec, ValkeyCacheSpec, ValkeyCelerySpec, ValkeyResultsBackendSpec), FlatComponentSpec, DeploymentTemplate, PodTemplate, ContainerTemplate, ScalableComponentSpec, ComponentSpec, AutoscalingSpec, PDBSpec
  - `superset_types.go` — Parent CRD: SupersetSpec (environment, secretKey/secretKeyFrom, metastore with uriFrom/passwordFrom, valkey, config, autoscaling, podDisruptionBudget), component specs, InitSpec (adminUser, loadExamples), AdminUserSpec, NetworkingSpec, MonitoringSpec, status types
  - `supersetinit_types.go` — Flat child CRD (Config + checksums, Pods + ConfigMap)
  - `supersetwebserver_types.go` — Flat child CRD (Config + Service + checksums)
  - `supersetceleryworker_types.go` — Flat child CRD (Config + checksums)
  - `supersetcelerybeat_types.go` — Flat child CRD (Config + checksums, singleton)
  - `supersetceleryflower_types.go` — Flat child CRD (Config + Service + checksums)
  - `supersetwebsocketserver_types.go` — Flat child CRD (Service only, no Python config)
  - `supersetmcpserver_types.go` — Flat child CRD (Config + Service + checksums)

- `internal/resolution/` — Pure Go spec resolution engine (zero controller-runtime deps)
  - `merge.go` — MergeMaps, MergeEnvVars, MergeVolumes, MergeVolumeMounts, MergeHostAliases, MergeContainers
  - `resolve.go` — ResolveScalar, ResolveOverridableMap/Slice/Value
  - `resolver.go` — ResolveChildSpec() — core flattening engine
- `internal/config/` — Pure Go config rendering pipeline (zero controller-runtime deps)
  - `renderer.go` — Per-component superset_config.py generation
- `internal/common/` — Shared types (ComponentType, Ptr), naming functions (ChildName, ConfigMapName, ComponentLabels), constants (labels, suffixes, ports)
- `internal/controller/` — Reconciler implementations
  - `child_reconciler.go` — generic `ChildReconciler` with `ChildCR` interface: shared sub-resource lifecycle (ConfigMap, Deployment, Service, Scaling) used by all 6 child controllers
  - `component_descriptors.go` — table-driven component descriptors for parent→child conversion
  - `deployment_builder.go` — builds Deployment from FlatComponentSpec + DeploymentConfig
  - `initpod.go` — InitPod lifecycle helpers (backoff, retention, failure messages)
  - `supersetinit_controller.go` — SupersetInit reconciler (pod state machine, retries, timeout)
  - `helpers.go` — componentLabels(), mergeLabels(), mergeAnnotations()
  - `status.go` — condition helpers, ChildComponentStatus update
  - `scaling.go` — HPA (with custom metrics) + PDB reconciliation
  - `networking.go` — HTTPRoute/Ingress reconciliation
  - `monitoring.go` — ServiceMonitor reconciliation (unstructured)
  - `networkpolicy.go` — NetworkPolicy reconciliation
- `config/` — Kustomize manifests (CRDs, RBAC, manager, samples, prometheus)
- `cmd/main.go` — entrypoint, registers all reconcilers + Gateway API scheme
- `docs/` — installation, architecture overview, internals, user guide, developer guide
- `scripts/` — release automation (`release-rc.sh`, `release-finalize.sh`)
- `test/e2e/` — end-to-end tests (require Kind cluster)

## Key Patterns

- **Two-tier resolution**: Parent resolves top-level + per-component fields into flat child spec. `internal/resolution/ResolveChildSpec()` is the core engine.
- **Deployment template hierarchy**: All Deployment/Pod/Container configuration flows through `deploymentTemplate` (Deployment-level) and `podTemplate` (Pod-level with nested `container` for main container fields) as siblings on the component spec. Top-level values provide defaults; per-component values are field-level merged (scalars: component wins; named collections: merge by name; unnamed collections: append). Init uses `podTemplate` only (no Deployment-level). See `docs/user-guide.md#deployment-template` for full semantics.
- **ScalableComponentSpec**: Has `DeploymentTemplate`, `PodTemplate`, and scaling fields (`Replicas`, `Autoscaling`, `PDB`). Used by scalable components. CeleryBeat has `DeploymentTemplate` + `PodTemplate` directly (no scaling). Init has `PodTemplate` only.
- **ComponentSpec**: Per-component image override field (`Image`). Embedded by all component specs except InitSpec.
- **Per-component config**: `internal/config/RenderConfig()` generates component-appropriate Python. `SECRET_KEY` is rendered from the `SUPERSET_OPERATOR__SECRET_KEY` env var. Both passthrough and structured metastore modes render `SQLALCHEMY_DATABASE_URI` in the config from operator-internal env vars (`SUPERSET_OPERATOR__DB_URI` for passthrough, `SUPERSET_OPERATOR__DB_*` for structured). Web server gets `SUPERSET_WEBSERVER_PORT`. WebsocketServer returns empty (Node.js). All Python components get `config`.
- **Environment modes**: `environment: dev` allows inline `secretKey`, `metastore.uri`, `metastore.password`, `valkey.password`, `init.adminUser`, and `init.loadExamples`. `environment: prod` (default) rejects these via CRD validation; use `secretKeyFrom`, `metastore.uriFrom`, `metastore.passwordFrom`, or `valkey.passwordFrom` to reference Kubernetes Secrets (operator injects `valueFrom.secretKeyRef` env vars).
- **Env var tiers**: Operator-internal transport vars (`SUPERSET_OPERATOR__SECRET_KEY`, `SUPERSET_OPERATOR__DB_URI`, `SUPERSET_OPERATOR__DB_HOST`, `SUPERSET_OPERATOR__VALKEY_HOST`, `SUPERSET_OPERATOR__FORCE_RELOAD`, etc.) and standard env vars (`PYTHONPATH`).
- **SECRET_KEY validation**: CEL requires either `secretKey` (dev mode) or `secretKeyFrom` (any mode) to be set.
- **Deployment builder**: All child controllers use `buildDeploymentSpec()` with flat `FlatComponentSpec`. Reads all fields from the merged `DeploymentTemplate` hierarchy. No parent lookup needed.
- **Generic child reconciler**: 6 child controllers (all except SupersetInit) use a shared `ChildReconciler` with a `ChildCR` interface. Each child CRD type implements accessor methods (`GetFlatSpec`, `GetConfig`, `GetService`, etc.).
- **Idempotent reconciliation**: Controllers use `controllerutil.CreateOrUpdate` for all resources.
- **Ownership**: `controllerutil.SetControllerReference` for garbage collection cascade.
- **Operator labels protected**: Operator labels (`app.kubernetes.io/*`, `superset.apache.org/parent`) are merged last — users cannot override them. Child CRs, workload pods, and NetworkPolicies carry `superset.apache.org/parent` + `app.kubernetes.io/component` for label-based orphan discovery and instance-scoped NetworkPolicy isolation.
- **Child name resolution**: Child CRs always use the parent name (differentiated by Kind). Sub-resource names (Deployment, Service, ConfigMap) are `{parentName}-{componentType}`, computed locally by each child controller from its CR name and component type via `naming.ResourceBaseName()`.
- **Checksum-driven rollouts**: Config checksums stamped as pod annotations trigger rolling restarts. Use `forceReload` for Secret rotations.
- **HPA**: When `autoscaling` is set, Deployment replicas is nil (HPA manages). Supports custom metrics via `autoscalingv2.MetricSpec`. Top-level `autoscaling`/`podDisruptionBudget` provide defaults inherited by all scalable components; per-component values override (not merge). CeleryBeat and Init are excluded (singleton/bare pods).
- **Beat singleton**: CeleryBeat always forces replicas=1 regardless of spec.
- **Gateway API**: Uses `sigs.k8s.io/gateway-api` types. Graceful handling of missing CRDs via `meta.IsNoMatchError`.
- **Init lifecycle**: SupersetInit child CRD with dedicated controller. Manages bare Pods (restartPolicy: Never) with customizable command (default: `superset db upgrade && superset init`). Operator manages retries with exponential backoff. Init gates component deployment. Enabled by default; disable via `spec.init.disabled: true`. Dev-mode-only `adminUser` and `loadExamples` fields append `superset fab create-admin` and `superset load-examples` steps to the init command (mutually exclusive with custom `command`).
- **CRD validation**: All validation uses CEL (`x-kubernetes-validations`) on CRD types — no admission webhooks. Rules cover: environment mode restrictions, secret mutual exclusivity, metastore/valkey validation, networking constraints, monitoring constraints. Defaults (repository, pullPolicy, environment) use kubebuilder default markers.
- **Metrics**: Operator exposes controller-runtime default metrics (reconcile counts, durations, leader election) on HTTPS :8443 with Kubernetes auth/authz. No custom metrics — controller-runtime defaults are sufficient. Superset instance monitoring via optional `spec.monitoring.serviceMonitor` (creates a Prometheus ServiceMonitor targeting the web-server component using unstructured objects; gracefully skips if CRD is absent).
- **Config mount path**: `/app/superset/config` for superset_config.py.
- **All Go files must have the Apache 2.0 copyright header** (see `hack/boilerplate.go.txt`)

## Naming Conventions

| Parent field | CRD Kind | Component suffix | Container name |
|---|---|---|---|
| `init` | `SupersetInit` | `init` | `superset` |
| `webServer` | `SupersetWebServer` | `web-server` | `superset` |
| `celeryWorker` | `SupersetCeleryWorker` | `celery-worker` | `superset` |
| `celeryBeat` | `SupersetCeleryBeat` | `celery-beat` | `superset` |
| `celeryFlower` | `SupersetCeleryFlower` | `celery-flower` | `superset` |
| `websocketServer` | `SupersetWebsocketServer` | `websocket-server` | `superset` |
| `mcpServer` | `SupersetMcpServer` | `mcp-server` | `superset` |

**Two-level naming:** Child CRs always use the parent name (differentiated by Kind). Sub-resources (Deployments, Services, ConfigMaps) are named `{parentName}-{componentType}`. Each child controller computes sub-resource names locally from its CR name and known component type. Example: parent `my-superset` → child CR `SupersetWebServer/my-superset` → Deployment `my-superset-web-server`, Service `my-superset-web-server`.

All components use the reserved container name `superset` for the main container. Since each component runs in its own Pod, names never collide. This allows `kubectl exec -it <pod> -c superset` without needing to know the component type.

All CRD names (parent and child) are validated via CEL to be valid DNS labels (lowercase alphanumeric and hyphens, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, max 63 characters). DNS-label syntax is required because the operator derives Service names from the parent name + component suffix. Per-component CEL rules enforce that the parent name is short enough for each enabled component's suffix to fit within the 63-char Service name limit (e.g., `-websocket-server` = 17 chars limits parent to 46).