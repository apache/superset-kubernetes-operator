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

# LLM Context Guide for Apache Superset Kubernetes Operator

## Project Overview

Kubernetes operator for Apache Superset, built with the Go-based Operator SDK and Go 1.26+.
- Module: `github.com/apache/superset-kubernetes-operator`
- API group: `superset.apache.org/v1alpha1`
- License: Apache 2.0

## Security

- Security policy and vulnerability reporting: [SECURITY.md](SECURITY.md)
- Security posture, including the threat model (trust boundaries, in-scope / out-of-scope), secret handling, and RBAC justification: [docs/reference/security.md](docs/reference/security.md)

## Developer Guide

See `docs/contributing/development-setup.md` and `docs/contributing/development-guidelines.md` for development setup, make commands, testing philosophy, code generation workflow, linting, CI/supply chain, and contributing guidelines. Adhere to the conventions documented there.

## Architecture

The operator uses a **single public CRD architecture** where the parent `Superset` resource resolves shared top-level and per-component configuration into parent-owned Kubernetes resources. See `docs/architecture/overview.md` for detailed design.

### CRD Hierarchy

- **Superset** (parent) — top-level CR with shared spec (top-level + per-component), environment, secretKey/secretKeyFrom, metastore (with uriFrom/passwordFrom), valkey (cache/broker/results), config, LifecycleSpec, NetworkingSpec, MonitoringSpec
- **Lifecycle tasks** — parent-owned Jobs + ConfigMap. Four sequential tasks: "clone" (database snapshot from external source), "migrate" (`superset db upgrade`), "rotate" (`superset re-encrypt-secrets` for secret key rotation), and "init" (`superset init`). Each task can be independently disabled via `disabled: true`. Clone supports `cronSchedule` for periodic re-execution.
- **Deployment components** — web server, Celery worker, Celery beat, Flower, websocket, and MCP server Deployments with Services/ConfigMaps/HPA/PDB as applicable.

**Key principles:**
- **Parent resolves and reconciles.** All layering logic lives in the parent controller, which writes Kubernetes resources directly.
- **Presence = enabled.** No `enabled: true/false`. If `celeryWorker: {}` is set, workers deploy. Lifecycle tasks (migrate, init) run by default; disable individual tasks via `disabled: true`. Clone runs when `spec.lifecycle.clone` is set. Rotate runs when `spec.lifecycle.rotate` is set.
- **Secrets never touch ConfigMaps.** In prod mode, CRD CEL validation rejects inline `secretKey`, `metastore.uri`, `metastore.password`, and `valkey.password`. Use `secretKeyFrom`, `metastore.uriFrom`, `metastore.passwordFrom`, or `valkey.passwordFrom` to reference Kubernetes Secrets (operator injects `valueFrom.secretKeyRef` env vars). In dev mode, inline secrets are allowed.
- **Per-component config rendering.** All Python components get `SECRET_KEY` rendered from `SUPERSET_OPERATOR__SECRET_KEY`. Web gets port config. Structured metastore renders an f-string URI from `SUPERSET_OPERATOR__DB_*` env vars. When `spec.valkey` is set, operator renders all cache configs (`CACHE_CONFIG`, `DATA_CACHE_CONFIG`, etc.), `CeleryConfig`, and `RESULTS_BACKEND` from `SUPERSET_OPERATOR__VALKEY_*` env vars. Websocket gets nothing (Node.js).

## Directory Layout

- `api/v1alpha1/` — CRD type definitions
  - `shared_types.go` — ImageSpec, MetastoreSpec, ValkeySpec (ValkeySSLSpec, ValkeyCacheSpec, ValkeyCelerySpec, ValkeyResultsBackendSpec), GunicornSpec, CeleryWorkerProcessSpec, SQLAlchemyEngineOptionsSpec, FlatComponentSpec, DeploymentTemplate, PodTemplate, ContainerTemplate, ScalableComponentSpec, ComponentSpec, AutoscalingSpec, PDBSpec
  - `superset_types.go` — SupersetSpec (environment, secretKey/secretKeyFrom, metastore with uriFrom/passwordFrom, valkey, config, sqlaEngineOptions, autoscaling, podDisruptionBudget), component specs (GunicornSpec on webServer, CeleryWorkerProcessSpec on celeryWorker, SQLAlchemyEngineOptionsSpec on all Python components except Flower), LifecycleSpec (clone/migrate/rotate/init tasks, upgradeMode, maintenancePage), AdminUserSpec, NetworkingSpec, MonitoringSpec, status types (LifecycleStatus, ComponentStatusMap, LastLifecycleImage)

- `internal/resolution/` — Pure Go spec resolution engine (zero controller-runtime deps)
  - `merge.go` — MergeMaps, MergeEnvVars, MergeVolumes, MergeVolumeMounts, MergeHostAliases, MergeContainers
  - `resolve.go` — ResolveScalar, ResolveOverridableMap/Slice/Value
  - `resolver.go` — ResolveComponentSpec() — core flattening engine
- `internal/config/` — Pure Go config rendering pipeline (zero controller-runtime deps)
  - `renderer.go` — Per-component superset_config.py generation
  - `gunicorn.go` — Gunicorn preset resolution, env var generation
  - `celery.go` — Celery worker preset resolution, command construction
  - `engine_options.go` — SQLALCHEMY_ENGINE_OPTIONS computation (pool sizing from worker/thread counts)
- `internal/common/` — Shared types (ComponentType, Ptr), naming functions (DerivedName, ConfigMapName, ComponentLabels), constants (labels, suffixes, ports)
- `internal/controller/` — Reconciler implementations
  - `superset_controller.go` — Parent `SupersetReconciler`: top-level Reconcile loop, parent-owned resource reconciliation, cleanup, status
  - `lifecycle.go` — Lifecycle pipeline orchestration: task sequencing, checksum computation, upgrade gates
  - `drain.go` — Component drain logic: resource deletion, pod termination verification
  - `schedule.go` — Cron schedule handling: tick computation, requeue timing
  - `config_builder.go` — Spec conversion: top-level → SharedInput, config rendering, env var collection
  - `maintenance.go` — Maintenance page: parent-owned Deployment + ConfigMap, Service selector switching
  - `component_reconciler.go` — shared component resource lifecycle (ConfigMap, Deployment, Service, Scaling)
  - `component_resources.go` — `ComponentResourceDefs()`: per-component DeploymentConfig defaults (commands, ports, scaling flags)
  - `component_descriptors.go` — table-driven component descriptors for parent resource reconciliation
  - `deployment_builder.go` — builds Deployment from FlatComponentSpec + DeploymentConfig
  - `initpod.go` — Task Job PodSpec helpers
  - `version.go` — Version comparison logic (upgrade/downgrade detection)
  - `helpers.go` — componentLabels(), mergeLabels(), mergeAnnotations()
  - `status.go` — condition helpers
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

- **Component resolution**: Parent resolves top-level + per-component fields into a flat runtime spec. `internal/resolution/ResolveComponentSpec()` is the core engine.
- **Deployment template hierarchy**: All Deployment/Pod/Container configuration flows through `deploymentTemplate` (Deployment-level) and `podTemplate` (Pod-level with nested `container` for main container fields) as siblings on the component spec. Top-level values provide defaults; per-component values are field-level merged (scalars: component wins; named collections: merge by name; unnamed collections: append). Lifecycle task Jobs use `podTemplate` only (no Deployment-level). See `docs/user-guide/configuration.md#deployment-template` for full semantics.
- **ScalableComponentSpec**: Has `DeploymentTemplate`, `PodTemplate`, and scaling fields (`Replicas`, `Autoscaling`, `PDB`). Used by scalable components. CeleryBeat has `DeploymentTemplate` + `PodTemplate` directly (no scaling). Lifecycle task Jobs use `PodTemplate` only.
- **ComponentSpec**: Per-component image override field (`Image`). Embedded by all component specs except LifecycleSpec.
- **Per-component config**: `internal/config/RenderConfig()` generates component-appropriate Python. `SECRET_KEY` is rendered from the `SUPERSET_OPERATOR__SECRET_KEY` env var. Both passthrough and structured metastore modes render `SQLALCHEMY_DATABASE_URI` in the config from operator-internal env vars (`SUPERSET_OPERATOR__DB_URI` for passthrough, `SUPERSET_OPERATOR__DB_*` for structured). `SQLALCHEMY_ENGINE_OPTIONS` is computed per component from the `sqlaEngineOptions` preset and Gunicorn/Celery worker configuration. Web server gets `SUPERSET_WEBSERVER_PORT`. WebsocketServer returns empty (Node.js). All Python components get `config`.
- **Gunicorn configuration**: `spec.webServer.gunicorn` controls Gunicorn worker parameters. Presets (`conservative`/`balanced`/`performance`/`aggressive`) set workers, threads, workerClass. Static defaults for timeout, keepAlive, etc. Operator injects env vars (`SERVER_WORKER_AMOUNT`, `SERVER_THREADS_AMOUNT`, etc.) read by `run-server.sh`. `disabled` preset suppresses injection.
- **Celery worker configuration**: `spec.celeryWorker.celery` controls Celery worker command args. Presets set concurrency and pool. Operator constructs the `celery worker` command from resolved fields. `disabled` preset uses the hardcoded fallback command.
- **SQLAlchemy engine options**: `spec.sqlaEngineOptions` sets the baseline; per-component `sqlaEngineOptions` on webServer, celeryWorker, celeryBeat, mcpServer, lifecycle tasks replaces the top-level entirely (override semantics). Presets: `disabled` (no rendering), `conservative` (NullPool), `balanced` (pool_size=1, max_overflow=-1), `performance` (pool_size=workers), `aggressive` (pool_size=workers×threads). CeleryBeat and lifecycle tasks always default to NullPool. Pool sizing is computed from resolved Gunicorn workers/threads or Celery concurrency. Static defaults: pool_recycle=3600, pool_pre_ping=false.
- **Environment modes**: `environment: Development` allows inline `secretKey`, `metastore.uri`, `metastore.password`, `valkey.password`, `lifecycle.adminUser`, and `lifecycle.loadExamples`. `environment: Production` (default) rejects these via CRD validation; use `secretKeyFrom`, `metastore.uriFrom`, `metastore.passwordFrom`, or `valkey.passwordFrom` to reference Kubernetes Secrets (operator injects `valueFrom.secretKeyRef` env vars).
- **Env var tiers**: Operator-internal transport vars (`SUPERSET_OPERATOR__SECRET_KEY`, `SUPERSET_OPERATOR__DB_URI`, `SUPERSET_OPERATOR__DB_HOST`, `SUPERSET_OPERATOR__VALKEY_HOST`, `SUPERSET_OPERATOR__FORCE_RELOAD`, etc.).
- **SECRET_KEY validation**: CEL requires either `secretKey` (dev mode) or `secretKeyFrom` (any mode) to be set.
- **Deployment builder**: Component resource reconciliation uses `buildDeploymentSpec()` with flat `FlatComponentSpec`. Reads all fields from the merged `DeploymentTemplate` hierarchy.
- **Generic component reconciler**: Deployment components share helper functions for ConfigMap, Deployment, Service, HPA, and PDB reconciliation.
- **Idempotent reconciliation**: Controllers use `controllerutil.CreateOrUpdate` for all resources.
- **Ownership**: `controllerutil.SetControllerReference` for garbage collection cascade.
- **Operator labels protected**: Operator labels (`app.kubernetes.io/*`, `superset.apache.org/parent`) are merged last — users cannot override them. Workload pods and NetworkPolicies carry `superset.apache.org/parent` + `app.kubernetes.io/component` for label-based discovery and instance-scoped NetworkPolicy isolation.
- **Resource name resolution**: Component resources are named `{parentName}-{componentType}`. Lifecycle task Jobs use deterministic names based on `{parentName}-{taskName}`.
- **Checksum-driven rollouts**: Config checksums stamped as pod annotations trigger rolling restarts. Use `forceReload` for Secret rotations.
- **HPA**: When `autoscaling` is set, Deployment replicas is nil (HPA manages). Supports custom metrics via `autoscalingv2.MetricSpec`. Top-level `autoscaling`/`podDisruptionBudget` provide defaults inherited by all scalable components; per-component values override (not merge). CeleryBeat and lifecycle tasks are excluded (singleton/one-off Jobs).
- **Beat singleton**: CeleryBeat always forces replicas=1 regardless of spec.
- **Gateway API**: Uses `sigs.k8s.io/gateway-api` types. Graceful handling of missing CRDs via `meta.IsNoMatchError`.
- **Lifecycle tasks**: `spec.lifecycle` on the parent CRD (type `LifecycleSpec`) defines up to four sequential tasks: "clone" (database snapshot from external source), "migrate" (`superset db upgrade`), "rotate" (`superset re-encrypt-secrets` for secret key rotation), and "init" (`superset init`). The parent controller sequences them (migrate waits for clone, rotate waits for migrate, init waits for rotate), gates component deployment, and triggers re-runs by deleting and recreating deterministic task Jobs when checksums change. Tasks run as Jobs with `backoffLimit: 0`; operator-managed retry/backoff and durable state are stored in parent `status.lifecycle`. Each task can be independently disabled via `disabled: true`. Clone supports `cronSchedule` for periodic re-execution. Checksums cascade: a re-clone triggers re-migrate, which triggers re-rotate, which triggers re-init. Version comparison detects upgrade vs downgrade; downgrades are blocked (phase: `Blocked`). `upgradeMode: Automatic` (default) runs tasks immediately; `Supervised` waits for an annotation approval before proceeding (phase: `AwaitingApproval`). Lifecycle gates component deployment — components are not updated until all enabled tasks complete. Dev-mode-only `adminUser` and `loadExamples` fields append steps to the init task command. Parent status tracks `LastLifecycleImage` and `Lifecycle *LifecycleStatus` (with lifecycle `Phase` enum: `Cloning`, `Draining`, `Migrating`, `Rotating`, `Initializing`, `Restoring`, `Complete`, etc.). Drain only runs when a task that will run requires drain and at least one configured component has desired replicas greater than zero; when it runs, parent brings up maintenance Deployment only if `maintenancePage` is set and an existing web-server workload is present, switches the parent-owned web-server Service selector to maintenance-page labels, drains components, runs tasks, waits for web-server ready, switches selector back, and deletes maintenance resources.
- **CRD validation**: All validation uses CEL (`x-kubernetes-validations`) on CRD types — no admission webhooks. Rules cover: environment mode restrictions, secret mutual exclusivity, metastore/valkey validation, networking constraints, monitoring constraints. Defaults (repository, pullPolicy, environment) use kubebuilder default markers.
- **Metrics**: Operator exposes controller-runtime default metrics (reconcile counts, durations, leader election) on HTTPS :8443 with Kubernetes auth/authz. No custom metrics — controller-runtime defaults are sufficient. Superset instance monitoring via optional `spec.monitoring.serviceMonitor` (creates a Prometheus ServiceMonitor targeting the web-server component using unstructured objects; gracefully skips if CRD is absent).
- **Config mount path**: `/app/pythonpath` for superset_config.py.
- **All Go files must have the Apache 2.0 copyright header** (see `hack/boilerplate.go.txt`)

## Naming Conventions

| Parent field | Component suffix | Container name |
|---|---|---|
| `lifecycle` (clone) | `clone` | `superset` |
| `lifecycle` (migrate) | `migrate` | `superset` |
| `lifecycle` (rotate) | `rotate` | `superset` |
| `lifecycle` (init) | `init` | `superset` |
| `webServer` | `web-server` | `superset` |
| `celeryWorker` | `celery-worker` | `superset` |
| `celeryBeat` | `celery-beat` | `superset` |
| `celeryFlower` | `celery-flower` | `superset` |
| `websocketServer` | `websocket-server` | `superset` |
| `mcpServer` | `mcp-server` | `superset` |

**Resource naming:** Component resources (Deployments, Services, ConfigMaps) are named `{parentName}-{componentType}`. Lifecycle task Jobs use deterministic names based on `{parentName}-{taskName}`.

All components use the reserved container name `superset` for the main container. Since each component runs in its own Pod, names never collide. This allows `kubectl exec -it <pod> -c superset` without needing to know the component type.

Superset CR names are validated via CEL to be valid DNS labels (lowercase alphanumeric and hyphens, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, max 63 characters). DNS-label syntax is required because the operator derives Service names from the parent name + component suffix. Per-component CEL rules enforce that the parent name is short enough for each enabled component's suffix to fit within the 63-char Service name limit (e.g., `-websocket-server` = 17 chars limits parent to 46).

## Automation Principle

When committed state must stay in sync with something external (upstream releases, generated docs, tables derived from a source of truth), prefer **building a CI-enforced sync mechanism** over documenting a manual checklist. Manual steps rot; CI failures don't.

Concrete patterns to mirror:

- **One source of truth, generated outputs.** Keep canonical data in a single small file (`.github/supported-k8s.json`, Go type definitions, etc.) and derive everything else — docs tables, CI matrices, defaults — from it.
- **`make codegen` aggregates all generators.** Anything regenerable goes through it. CI's `Verify codegen` job runs `make codegen` and fails on diff.
- **Upstream drift checks.** When the source-of-truth file itself must track something external (a pinned dependency's release notes, the latest upstream minor), write a `make sync-<thing>` script that queries the upstream API and rewrites the file, plus a `make verify-<thing>` that runs it in `--check` mode in CI. See `scripts/sync-supported-versions.sh` for the canonical example.
- **Scheduled auto-sync PRs.** Pair the sync script with a daily scheduled GitHub Actions job that runs it and opens a PR if anything changed. Removes the "every open PR breaks until someone syncs" failure mode when upstream ticks.
- **Renovate for pinned versions.** Use `customManagers` for non-standard pin sites (regex-matched JSON/YAML fields). Use `prBodyNotes` + `addLabels` to steer the reviewer toward the right follow-up command on PRs that require manual judgment.
- **Sentinel-bounded generated blocks** in human-edited files (`<!-- BEGIN X -->` / `<!-- END X -->`) so generators can rewrite specific regions without disturbing surrounding hand-written content.

The bar: every new "remember to also update X when Y changes" instruction is a bug in the tooling. Fix the tooling instead.

## PR Conventions

- **Title format**: `type(scope): description` or `type: description` — enforced by CI (`amannn/action-semantic-pull-request`). Scope is optional but encouraged. Aim for 50 characters when practical, and avoid exceeding 72 characters because GitHub wraps longer titles in common views.
- **Types**: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`, `build`, `perf`, `style`, `revert`
- **Scopes** (when used): `api`, `controller`, `resolution`, `config`, `helm`, `ci`, `docs`, `deps`
- **Description**: Every PR must have a Summary section with at least one paragraph explaining what and why. Use the Details section for implementation notes. PR template pre-fills these sections.
- **Code coverage**: Codecov reports patch coverage and project delta on every PR (informational, no enforced targets).

## Documentation Style

- **README** is a landing page: project description, philosophy, quick start, link to docs. Keep it welcoming and free of jargon — don't reference specific knobs, internal config names, or implementation details that might intimidate newcomers.
- **docs/index.md** is the primary feature overview for the docs site. Keep feature descriptions high-level and outcome-focused. Implementation details belong in the user guide or architecture docs. Prefer consolidated bullets that group related capabilities (e.g., "Lifecycle automation — cloning, migrations, key rotation, and init" rather than a separate bullet per feature).
- **docs/user-guide/configuration.md** is the full configuration reference. Here it's appropriate to name specific fields, presets, env vars, and show concrete YAML examples.
- **docs/architecture/overview.md** explains design decisions and internal structure for contributors and advanced users.
- General principles: be concise and objective, avoid overselling or verbose language, reserve code blocks for real code (not ASCII art), minimize duplication between README and docs (README links to docs for details).
- **API reference** (`docs/reference/api-reference.md`) is generated from Go types via `make codegen`. Only operator-defined types are rendered; built-in Kubernetes types (e.g., `Affinity`, `Container`, `Volume`) are linked to [pkg.go.dev](https://pkg.go.dev) via `knownTypes` in `hack/api-ref-config.yaml`. When adding a field that references a new K8s type, add a `knownTypes` entry so it renders as a link rather than being inlined.
- **`make codegen`** regenerates all generated artifacts (CRDs, DeepCopy, Helm CRDs, API docs). Run it after modifying types in `api/v1alpha1/`. CI verifies nothing is stale.
