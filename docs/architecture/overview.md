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

# Architecture Overview

For runtime behavior details — reconciliation lifecycle, parent-owned resource
management, and status reporting — see [Internals](internals.md). For lifecycle
task orchestration (migrations, upgrades, drain strategies), see
[Lifecycle](../user-guide/lifecycle.md).

## Single Superset CRD Architecture

The operator exposes one user-facing CRD: `Superset`. A single `Superset`
resource defines the complete deployment. The parent controller resolves shared
top-level configuration and per-component overrides into concrete Kubernetes
resources: Deployments, Services, ConfigMaps, HPAs, PDBs, lifecycle task Jobs,
networking, monitoring, and NetworkPolicies.

### Why one CRD?

`Superset` is the reconciliation boundary for one Superset installation. The
runtime components do not have independent desired state: they share instance
configuration, secret material, database migration ordering, drain/maintenance
behavior, rollout gates, and aggregate readiness. A lifecycle task can block or
replace every component, and a component rollout may depend on lifecycle state
that only the parent can evaluate.

Because of that coupling, separate component CRDs would mostly expose internal
controller decomposition as Kubernetes APIs. They would imply that components
can be created, updated, or observed as independently managed custom resources,
even though the controller cannot safely reconcile them without the parent's
configuration, lifecycle, and rollout context.

A single CRD matches the actual ownership model:

- Users declare one desired state: the `Superset` resource.
- The controller reconciles Deployments, Services, ConfigMaps, HPAs, PDBs,
  lifecycle task Jobs, networking, monitoring, and NetworkPolicies as
  parent-owned secondary resources.
- The `Superset` status subresource is the canonical visibility surface for
  component readiness, resource references, lifecycle task progress, and
  failure messages.

This keeps the public API small, avoids partially managed intermediate custom
resources, and makes `kubectl describe superset <name>` the place to inspect the
state of the whole installation.

The implementation remains modular internally. Component descriptors,
deployment defaults, config rendering, lifecycle task orchestration, resource
reconciliation, and status projection are separated in code and covered by
focused tests. The single CRD is an API and ownership decision, not a mandate for
one monolithic controller implementation.

### How it works

For each enabled component, the parent controller renders any needed
`superset_config.py`, merges top-level and per-component templates into a flat
runtime spec, reconciles the parent-owned Kubernetes resources, and projects
workload state back into `status.components`.

Lifecycle tasks follow the same model: the parent resolves the task Job Pod spec,
creates a parent-owned ConfigMap when needed, runs a parent-owned Job, and
stores durable task state in `status.lifecycle`.

---

## API Shape

Users create one top-level resource:

```yaml
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: my-superset
spec:
  image: { tag: "latest" }
  environment: Development
  secretKey: thisIsNotSecure_changeInProduction!
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
```

Components fall into two runtime categories:

**Scalable components** support replicas, HPA, and PodDisruptionBudgets:

| Parent field | Suffix | Creates |
|---|---|---|
| `webServer` | `-web-server` | Deployment, Service, ConfigMap, HPA, PDB |
| `celeryWorker` | `-celery-worker` | Deployment, ConfigMap, HPA, PDB |
| `celeryFlower` | `-celery-flower` | Deployment, Service, ConfigMap, HPA, PDB |
| `websocketServer` | `-websocket-server` | Deployment, Service, HPA, PDB |
| `mcpServer` | `-mcp-server` | Deployment, Service, ConfigMap, HPA, PDB |

**Singleton components** run exactly one instance and don't support scaling:

| Parent field | Suffix | Creates |
|---|---|---|
| `lifecycle` | `-clone`, `-migrate`, `-rotate`, `-init` | Jobs, ConfigMap for Superset-image tasks |
| `celeryBeat` | `-celery-beat` | Deployment, ConfigMap |

**Presence = enabled**: Setting `celeryWorker: {}` deploys workers with
defaults. Omitting `celeryWorker` entirely means no workers. No
`enabled: true/false` toggles. The exception is lifecycle tasks, which are
enabled by default even when `spec.lifecycle` is nil; disable them explicitly
with `spec.lifecycle.disabled: true`.

---

## Configuration Model

All Deployment, Pod, and container configuration flows through two sibling
template fields:

```
deploymentTemplate                     → DeploymentSpec-level (strategy, revisionHistoryLimit, ...)
podTemplate                            → PodSpec-level (affinity, tolerations, volumes, ...)
└── container                          → Container-level (resources, env, probes, ...)
```

**Top-level `deploymentTemplate` and `podTemplate`** provide defaults
inherited by all components. **Per-component** values are field-level merged
with the top-level — only specify what's different. Scaling fields
(`replicas`, `autoscaling`, `podDisruptionBudget`) are outside the templates
since they interact with operator logic (HPA, Beat singleton).

Merge semantics per field type:

- **Scalars/structs** (resources, affinity, securityContext, probes, etc.) — component wins if set
- **Named collections** (env, volumes, volumeMounts, sidecars) — merge by name, component wins on conflict
- **Maps** (annotations, labels, nodeSelector) — merge by key, component wins on conflict
- **Unnamed collections** (tolerations, topologySpreadConstraints) — append
- **command/args** — component-only, not inherited from top-level
- **Operator-managed labels** (`app.kubernetes.io/*`) — applied last, cannot be overridden

Lifecycle tasks use `podTemplate` only (no `deploymentTemplate`) since they
create Jobs, not Deployments. See the [Configuration guide](../user-guide/configuration.md#deployment-template) for
the full field reference and examples.

### Example: How resources resolve for celeryWorker

```yaml
spec:
  podTemplate:
    container:
      resources:
        limits:
          cpu: "2"
          memory: "4Gi"
  celeryWorker:
    podTemplate:
      container:
        resources:
          limits:
            cpu: "8"                   # component replaces entire resources struct
```

Result on the celery worker Deployment: `resources.limits = {cpu: "8"}`
(resources is a scalar/struct field — component replaces entirely).

### Example: How env vars resolve for webServer

```yaml
spec:
  podTemplate:
    container:
      env:
        - {name: LOG_LEVEL, value: INFO}
  webServer:
    podTemplate:
      container:
        env:
          - {name: GUNICORN_WORKERS, value: "4"}   # merged with top-level
```

Result on the web server Deployment: both env vars present.

---

## Configuration Philosophy: Typed Fields vs. Raw Python

Superset is configured through `superset_config.py` — a Python module that exposes hundreds of knobs across Flask, Flask-AppBuilder, Celery, caching, security, and Superset itself. Mirroring every one of those knobs as a typed CRD field would turn the operator into a partial Superset fork, balloon CRD size, and lag behind upstream changes. Hiding everything behind a single Python blob, on the other hand, gives up the validation, discoverability, and operator-side reasoning that a CRD makes possible.

The operator splits the difference:

- **Typed CRD fields** are reserved for settings where the operator adds clear value: anything tied to Kubernetes resources (images, ports, replicas, autoscaling), anything sourced from Secrets (`secretKey`, metastore credentials, Valkey passwords), and any setting that the operator can validate, render uniformly, or wire up across components (metastore URIs, Valkey-driven cache and Celery backends, lifecycle gating). Typed fields earn their place because they can't be expressed as plain Python without re-implementing what the operator already does.

- **Raw Python in `spec.config` and `spec.<component>.config`** is the default home for everything else: feature flags beyond a curated set, custom security managers, OAuth providers, thumbnail executors, custom Celery routes, beat schedules, task annotations, and anything else that is naturally a Python expression or class. Admins are comfortable writing Python in `superset_config.py`; forcing those settings through YAML schemas tends to obfuscate rather than clarify.

To make Python-side configuration ergonomic, the operator exposes a few resolved values as env vars (`SUPERSET_OPERATOR__INSTANCE_NAME`, `SUPERSET_OPERATOR__VALKEY_HOST`, etc.) so admins can reference them from raw Python without templating. Operator-rendered objects like the `CeleryConfig` class are regular Python — admins extend them by mutating attributes, subclassing, or replacing the assignment outright.

This split is intentionally a moving boundary. As specific knobs prove to be widely used, frequently misconfigured, or worth cross-component validation, they migrate from raw Python into typed CRD fields. The starting position favors raw Python; promotions are made deliberately and case by case. The first promotions are `spec.featureFlags` (a `map[string]bool` rendering `FEATURE_FLAGS = {...}`) and `spec.celery.imports` (the Celery worker import tuple, defaulting to upstream Superset's modules).

---

## Config Rendering Pipeline

The operator generates per-component `superset_config.py` files by
**concatenating** three sections in order. Both `spec.config` (base) and
`spec.<component>.config` (component) are appended — they are not mutually
exclusive. If both are set, the component receives all three sections:

### How config is built

1. **Operator-generated configs** — `SECRET_KEY` rendered from the
   `SUPERSET_OPERATOR__SECRET_KEY` env var, `SQLALCHEMY_DATABASE_URI` rendered
   from operator-internal env vars (both passthrough and structured metastore
   modes), plus `SUPERSET_WEBSERVER_PORT` for the web server.
2. **SQLAlchemy engine options** — `SQLALCHEMY_ENGINE_OPTIONS` dict, computed
   per component from the resolved `sqlaEngineOptions` preset and the
   component's worker/thread configuration (Gunicorn workers × threads for
   the web server, Celery concurrency for workers). Presets range from
   `conservative` (NullPool) through `balanced` (pool\_size=1, max\_overflow=-1)
   to `aggressive` (pool\_size=workers×threads). See
   [SQLAlchemy Engine Options](../user-guide/configuration.md#sqlalchemy-engine-options) for details.
3. **Valkey cache config** — When `spec.valkey` is set, the operator renders
   `CACHE_CONFIG`, `DATA_CACHE_CONFIG`, `FILTER_STATE_CACHE_CONFIG`,
   `EXPLORE_FORM_DATA_CACHE_CONFIG`, `THUMBNAIL_CACHE_CONFIG`,
   `CeleryConfig`, and `RESULTS_BACKEND` backed by Valkey. Connection details
   are read from `SUPERSET_OPERATOR__VALKEY_*` env vars at Python runtime.
   SSL/mTLS cert paths are baked directly into the rendered config.
4. **Base config (`spec.config`)** — Raw Python from the top-level `config`
   field, shared by all Python components. Appended after operator-generated
   configs.
5. **Component config (`spec.<component>.config`)** — Raw Python from the
   per-component `config` field. Appended last, so it can override anything
   above.

For example, given a structured metastore configuration:

```yaml
spec:
  metastore:
    host: db.example.com
    database: superset
    username: superset
    passwordFrom:
      name: db-credentials
      key: password
  config: |
    FEATURE_FLAGS = {"DASHBOARD_RBAC": True}
  celeryWorker:
    config: |
      CELERY_ANNOTATIONS = {"tasks.add": {"rate_limit": "10/s"}}
```

The celery worker's `superset_config.py` contains all three sections:

```python
# Operator-generated configs
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://..."  # assembled from env vars

# Base config (spec.config)
FEATURE_FLAGS = {"DASHBOARD_RBAC": True}

# Component config
CELERY_ANNOTATIONS = {"tasks.add": {"rate_limit": "10/s"}}
```

Note: All operator-managed settings (`SECRET_KEY`, `SQLALCHEMY_DATABASE_URI`,
web server port) are rendered into the config file from operator-internal
`SUPERSET_OPERATOR__*` env vars. Both passthrough and structured metastore
modes render `SQLALCHEMY_DATABASE_URI` from `SUPERSET_OPERATOR__DB_URI`
(passthrough) or `SUPERSET_OPERATOR__DB_*` (structured).

| Config section | WebServer | CeleryWorker | CeleryBeat | CeleryFlower | McpServer |
|---|---|---|---|---|---|
| SECRET_KEY | yes | yes | yes | yes | yes |
| Passthrough DB URI | if set | if set | if set | if set | if set |
| Structured DB URI (f-string) | if set | if set | if set | if set | if set |
| Web server port (8088) | yes | | | | |
| Top-level config | yes | yes | yes | yes | yes |
| Per-component config | yes | yes | yes | yes | yes |

**WebsocketServer** is Node.js-based -- it does NOT get `superset_config.py`.

### Secret Handling

In **dev mode** (`environment: Development`), `secretKey`, `metastore.uri`, and
`metastore.password` can be set as plain strings directly in the CR. The
operator injects them as environment variables on the container spec.

In **prod mode** (`environment: Production`, the default), CRD validation rejects
these inline fields. Instead, use the `*From` fields to reference Kubernetes
Secrets:

- `secretKeyFrom` — references a Secret key for the Flask secret key
- `metastore.uriFrom` — references a Secret key for the full database URI
- `metastore.passwordFrom` — references a Secret key for the database password (structured mode)

The operator injects the corresponding env vars (`SUPERSET_OPERATOR__SECRET_KEY`,
`SUPERSET_OPERATOR__DB_URI`, `SUPERSET_OPERATOR__DB_PASS`) with
`valueFrom.secretKeyRef` pointing at the referenced Secret. Secret values
never appear in ConfigMaps or CRD status fields.

### Config Mount Structure

- `/app/pythonpath/` — ConfigMap with `superset_config.py`

---

## Checksum-Driven Rollouts

The parent computes a per-component config checksum and stamps it on the
component Deployment pod template. When the checksum changes (due to config or
secret reference changes on the CR), Kubernetes triggers a rolling restart of the affected
component. Note: rotating a referenced Secret's value without changing the
CR does not trigger a rollout — use
[Force Reload](../user-guide/configuration.md#force-reload) for this case. See
[Internals](internals.md#checksum-driven-rollouts) for the full checksum
table and per-component isolation details.

---

## Resource Ownership

All resources use Kubernetes owner references for automatic cleanup. The parent
`Superset` CR owns component Deployments, Services, ConfigMaps, HPAs, PDBs,
lifecycle task Jobs, networking resources (Ingress/HTTPRoute), ServiceMonitor,
and NetworkPolicies. Deleting the parent cascades to everything. Removing a
component from the parent spec deletes the resources for that component.
