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

Kubernetes operator for Apache Superset, built with the Go-based Operator SDK. The Go version is pinned in `go.mod`.

- Module: `github.com/apache/superset-kubernetes-operator`
- API group: `superset.apache.org/v1alpha1`
- License: Apache 2.0

## Security

- Security policy and vulnerability reporting: [SECURITY.md](SECURITY.md)
- Security posture, including the threat model (trust boundaries, in-scope / out-of-scope), secret handling, and RBAC justification: [docs/reference/security.md](docs/reference/security.md)

## Development Guidelines

All changes must follow [docs/contributing/development-guidelines.md](docs/contributing/development-guidelines.md), which is the single source of truth for how to design, implement, test, document, and submit changes in this repository. Read it before making changes, and check your changes against it before finishing. For environment setup, make targets, and docs tooling, see [docs/contributing/development-setup.md](docs/contributing/development-setup.md).

This file only gives project context. Don't restate development guidance here: add or change it in the development guidelines, so there's a single place to keep up to date.

## Architecture

The operator uses a **single public CRD architecture** where the parent `Superset` resource resolves shared top-level and per-component configuration into parent-owned Kubernetes resources. See `docs/architecture/overview.md` for detailed design.

### CRD Hierarchy

- **Superset** (parent) — top-level CR with shared spec (top-level + per-component), environment, secretKey/secretKeyFrom, metastore (literal or Secret-backed connection fields), valkey (cache/broker/results with literal or Secret-backed connection fields), config, LifecycleSpec, NetworkingSpec, MonitoringSpec
- **Lifecycle tasks** — parent-owned Jobs + ConfigMap. Four sequential tasks: "seed" (database snapshot from external source), "migrate" (`superset db upgrade`), "rotate" (`superset re-encrypt-secrets` for secret key rotation), and "init" (`superset init`). Each task can be independently disabled via `disabled: true`. Seed supports `cronSchedule` for periodic re-execution.
- **Deployment components** — web server, Celery worker, Celery beat, Flower, websocket, and MCP server Deployments with Services/ConfigMaps/HPA/PDB as applicable.

**Key principles:**

- **Parent resolves and reconciles.** All layering logic lives in the parent controller, which writes Kubernetes resources directly.
- **Presence = enabled.** No `enabled: true/false`. If `celeryWorker: {}` is set, workers deploy. Lifecycle tasks (migrate, init) run by default; disable individual tasks via `disabled: true`. Seed runs when `spec.lifecycle.seed` is set. Rotate runs when `spec.lifecycle.rotate` is set.
- **Secrets never touch ConfigMaps.** Outside `Development` (that is, in `Staging` and `Production`), CRD CEL validation rejects inline `secretKey`, `previousSecretKey`, `metastore.uri`, `metastore.password`, `valkey.password`, `websocketServer.config`, and `lifecycle.seed.source.password`. Use the corresponding `*From` fields to reference Kubernetes Secrets (operator injects `valueFrom.secretKeyRef` env vars). Structured metastore, Valkey, and seed-source connection fields also have `*From` counterparts for provisioner-owned connection details. In `Development`, inline secrets are allowed.
- **Per-component config rendering.** All Python components get `SECRET_KEY` rendered from `SUPERSET_OPERATOR__SECRET_KEY`. Web gets port config. Structured metastore renders an f-string URI from `SUPERSET_OPERATOR__DB_*` env vars. When `spec.valkey` is set, operator renders all cache configs (`CACHE_CONFIG`, `DATA_CACHE_CONFIG`, etc.), `CeleryConfig`, and `RESULTS_BACKEND` from `SUPERSET_OPERATOR__VALKEY_*` env vars. Websocket gets nothing (Node.js).

## Directory Layout

Package responsibilities and the most important files are listed in the development guidelines' [Package Structure](docs/contributing/development-guidelines.md#package-structure) and [Key Files Reference](docs/contributing/development-guidelines.md#key-files-reference). Orientation:

- `api/v1alpha1/` — CRD types. `shared_types.go` holds the shared building blocks (image, metastore, Valkey, presets, `DeploymentTemplate`/`PodTemplate`/`ContainerTemplate`, `FlatComponentSpec`); `superset_types.go` holds `SupersetSpec`, component and lifecycle task specs, the CEL rules, and `SupersetStatus`.
- `internal/resolution/` — pure Go spec flattening (`merge.go` merge helpers, `resolve.go` scalar/override helpers, `resolver.go` `ResolveComponentSpec()`).
- `internal/config/` — pure Go `superset_config.py` rendering (`renderer.go`), Gunicorn/Celery presets (`gunicorn.go`, `celery.go`), `CeleryConfig` rendering (`celery_app.go`), and `SQLALCHEMY_ENGINE_OPTIONS` (`engine_options.go`).
- `internal/common/` — `ComponentType`, naming helpers, label keys, suffixes, and ports.
- `internal/schedule/` — pure Go cron tick computation.
- `internal/controller/` — the parent `SupersetReconciler` (`superset_controller.go`) and its helpers. File prefixes group responsibilities: `component_*` and `deployment_builder.go` for component resources, `lifecycle*.go` for the task pipeline (`lifecycle_cascade.go` computes task checksums; `lifecycle_<task>.go` holds per-task inputs and commands), plus `drain.go`, `maintenance.go`, `schedule.go`, `scaling.go`, `networking.go`, `networkpolicy.go`, `monitoring.go`, `websocket_config.go`, `bootstrap.go`, and `redact.go` (credential masking for task output written to status and Events).
- `config/` — Kustomize manifests (CRDs, RBAC, manager, samples, prometheus).
- `charts/superset-operator/` — Helm chart.
- `cmd/main.go` — entrypoint, registers the reconciler and the Gateway API scheme.
- `docs/` — user, architecture, reference, and contributor docs.
- `scripts/` — CI, docs, sync, and release automation.
- `test/e2e/` — end-to-end tests (require a Kind cluster).

## Key Patterns

- **Component resolution**: Parent resolves top-level + per-component fields into a flat runtime spec. `internal/resolution/ResolveComponentSpec()` is the core engine.
- **Deployment template hierarchy**: All Deployment/Pod/Container configuration flows through `deploymentTemplate` (Deployment-level) and `podTemplate` (Pod-level with nested `container` for main container fields) as siblings on the component spec. Top-level values provide defaults; per-component values are field-level merged (scalars: component wins; named collections: merge by name; unnamed collections: append). Lifecycle task Jobs use `podTemplate` only (no Deployment-level). See `docs/user-guide/configuration.md#deployment-template` for full semantics.
- **ScalableComponentSpec**: Has `DeploymentTemplate`, `PodTemplate`, and scaling fields (`Replicas`, `Autoscaling`, `PDB`). Used by scalable components. CeleryBeat has `DeploymentTemplate` + `PodTemplate` directly (no scaling). Lifecycle task Jobs use `PodTemplate` only.
- **ComponentSpec**: Per-component image override field (`Image`). Embedded by all component specs except LifecycleSpec.
- **Per-component config**: `internal/config/RenderConfig()` generates component-appropriate Python. `SECRET_KEY` is rendered from the `SUPERSET_OPERATOR__SECRET_KEY` env var. Both passthrough and structured metastore modes render `SQLALCHEMY_DATABASE_URI` in the config from operator-internal env vars (`SUPERSET_OPERATOR__DB_URI` for passthrough, `SUPERSET_OPERATOR__DB_*` for structured). `SQLALCHEMY_ENGINE_OPTIONS` is computed per component from the `sqlaEngineOptions` preset and Gunicorn/Celery worker configuration. Web server gets `SUPERSET_WEBSERVER_PORT`. WebsocketServer returns empty (Node.js). All Python components get `config`.
- **Gunicorn configuration**: `spec.webServer.gunicorn` controls Gunicorn worker parameters. Presets (`conservative`/`balanced`/`performance`/`aggressive`) set workers and threads; a non-gthread `workerClass` without explicit `threads` resolves to 1 thread. Static defaults for timeout, keepAlive, etc. Operator injects env vars (`SERVER_WORKER_AMOUNT`, `SERVER_THREADS_AMOUNT`, etc.) read by `run-server.sh`. `disabled` preset suppresses injection.
- **Celery worker configuration**: `spec.celeryWorker.celery` controls Celery worker command args. Presets set concurrency. Operator constructs the `celery worker` command from resolved fields. `disabled` preset uses the hardcoded fallback command.
- **SQLAlchemy engine options**: `spec.sqlaEngineOptions` sets the baseline; per-component `sqlaEngineOptions` on webServer, celeryWorker, celeryBeat, mcpServer, lifecycle tasks replaces the top-level entirely (override semantics). Presets size the pool from the resolved Gunicorn workers/threads or Celery concurrency; CeleryBeat and lifecycle tasks use NullPool unless the preset is `disabled`. See `docs/user-guide/configuration.md#sqlalchemy-engine-options` for the preset table and static defaults.
- **Environment modes**: `Production` (default) and `Staging` reject the inline secret fields listed under "Secrets never touch ConfigMaps"; `Staging` additionally allows `lifecycle.seed`. `Development` allows inline secrets, `lifecycle.seed`, `lifecycle.init.adminUser`, and `lifecycle.init.loadExamples`. A CEL transition rule blocks changing an existing `Production` resource to another environment in place; it must be deleted and recreated. See `docs/user-guide/configuration.md#environment-mode`.
- **Env var tiers**: Operator-internal transport vars (`SUPERSET_OPERATOR__SECRET_KEY`, `SUPERSET_OPERATOR__DB_URI`, `SUPERSET_OPERATOR__DB_HOST`, `SUPERSET_OPERATOR__VALKEY_HOST`, `SUPERSET_OPERATOR__FORCE_RELOAD`, etc.).
- **SECRET_KEY validation**: CEL requires exactly one of `secretKey` (`Development` only) or `secretKeyFrom` (any environment).
- **Deployment builder**: Component resource reconciliation uses `buildDeploymentSpec()` with flat `FlatComponentSpec`. Reads all fields from the merged `DeploymentTemplate` hierarchy.
- **Operator labels protected**: Operator labels (`app.kubernetes.io/*`, `superset.apache.org/parent`) are merged last — users cannot override them. Workload pods and NetworkPolicies carry `superset.apache.org/parent` + `app.kubernetes.io/component` for label-based discovery and instance-scoped NetworkPolicy isolation.
- **Checksum-driven rollouts**: Config checksums stamped as pod annotations trigger rolling restarts. Use `forceReload` for Secret rotations.
- **HPA**: When `autoscaling` is set, Deployment replicas is nil (HPA manages). Supports custom metrics via `autoscalingv2.MetricSpec`. Top-level `autoscaling`/`podDisruptionBudget` provide defaults inherited by all scalable components; per-component values override (not merge). CeleryBeat and lifecycle tasks are excluded (singleton/one-off Jobs).
- **Beat singleton**: CeleryBeat always forces replicas=1 regardless of spec.
- **Gateway API**: Uses `sigs.k8s.io/gateway-api` types. Graceful handling of missing CRDs via `meta.IsNoMatchError`.
- **Lifecycle tasks**: `spec.lifecycle` on the parent CRD (type `LifecycleSpec`) defines up to four sequential tasks: "seed" (database snapshot from external source), "migrate" (`superset db upgrade`), "rotate" (`superset re-encrypt-secrets` for secret key rotation), and "init" (`superset init`). The parent controller sequences them (migrate waits for seed, rotate waits for migrate, init waits for rotate), gates component deployment, and triggers re-runs by deleting and recreating deterministic task Jobs when checksums change. Tasks run as Jobs with `backoffLimit: 0`; operator-managed retry/backoff and durable state are stored in parent `status.lifecycle`. Each task can be independently disabled via `disabled: true`. Seed supports `cronSchedule` for periodic re-execution. Checksums cascade: a re-seed triggers re-migrate, which triggers re-rotate, which triggers re-init. Any change to the lifecycle image tag re-runs migrate (`superset db upgrade`) regardless of direction — downgrades are not blocked (Superset's `db downgrade` is poorly tested and often breaks, so the operator never runs down migrations; users should take a database backup before upgrading). `upgradeMode: Automatic` (default) runs tasks immediately; `Supervised` waits for an annotation approval before proceeding (phase: `AwaitingApproval`). Lifecycle gates component deployment — components are not updated until all enabled tasks complete. Dev-mode-only `adminUser` and `loadExamples` fields append steps to the init task command. Parent status tracks `LastLifecycleImage` and `Lifecycle *LifecycleStatus` (with lifecycle `Phase` enum: `Seeding`, `Draining`, `Migrating`, `Rotating`, `Initializing`, `Restoring`, `Complete`, etc.). Drain only runs when a task that will run requires drain and at least one configured component has desired replicas greater than zero; when it runs, parent brings up maintenance Deployment only if `maintenancePage` is set and an existing web-server workload is present, switches the parent-owned web-server Service selector to maintenance-page labels, drains components, runs tasks, waits for web-server ready, switches selector back, and deletes maintenance resources.
- **Config mount path**: `/app/pythonpath` for superset_config.py.

## Naming Conventions

| Parent field | Component suffix | Container name |
|---|---|---|
| `lifecycle` (seed) | `seed` | `superset` |
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
