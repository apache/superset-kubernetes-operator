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

For runtime behavior details — reconciliation lifecycle, child controller
pattern, and status reporting — see [Internals](internals.md). For lifecycle
task orchestration (migrations, upgrades, drain strategies), see
[Lifecycle](../user-guide/lifecycle.md).

## Two-Tier CRD Architecture

The operator uses a two-tier CRD architecture. A single parent `Superset`
resource defines the complete deployment. The parent controller resolves all
configuration into fully-flattened child CRDs that each manage one component.

### Why two tiers?

A single-controller design would require one reconciliation loop to manage all
sub-resources (Deployments, Services, ConfigMaps, HPA, PDB) for every component.
This creates a large blast radius — a bug in Celery worker reconciliation can
block web server updates — and makes the controller harder to test and reason
about.

Splitting into dedicated child CRDs and controllers isolates each component's
lifecycle. The web server controller only watches `SupersetWebServer` resources;
it cannot interfere with Celery or init. Each child controller is simple and
generic (all six share `ChildReconciler`), while the parent controller focuses
solely on configuration resolution and child CR orchestration. This separation
also enables independent scaling of controller watches and makes `kubectl get`
output immediately useful — `kubectl get supersetwebservers` shows web server
status without filtering.

### How it works

Each child CRD contains the fully resolved spec — `kubectl get supersetwebserver -o yaml`
shows exactly what is running with no layering to trace. While child CRDs can
technically be created directly, doing so bypasses the parent's lifecycle
orchestration (task sequencing, drain strategies, component gating). Manual child
CRs are not recommended for production use. Because child CRs carry the same
fields as the parent (images, commands, env vars, volumes), their writers should
be treated as equally trusted — see [Security](../reference/security.md#trust-boundaries)
for details.

---

## CRD Hierarchy

### Parent: `Superset`

The top-level resource. Users create this to deploy Superset.

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

### Children (fully-flattened, operator-managed)

Components fall into two categories:

**Scalable components** support replicas, HPA, and PodDisruptionBudgets:

| CRD Kind | Parent field | Suffix | Creates |
|---|---|---|---|
| `SupersetWebServer` | `webServer` | `-web-server` | Deployment, Service, ConfigMap, HPA |
| `SupersetCeleryWorker` | `celeryWorker` | `-celery-worker` | Deployment, ConfigMap, HPA |
| `SupersetCeleryFlower` | `celeryFlower` | `-celery-flower` | Deployment, Service, ConfigMap |
| `SupersetWebsocketServer` | `websocketServer` | `-websocket-server` | Deployment, Service |
| `SupersetMcpServer` | `mcpServer` | `-mcp-server` | Deployment, Service, ConfigMap |

**Singleton components** run exactly one instance and don't support scaling:

| CRD Kind | Parent field | Suffix | Creates |
|---|---|---|---|
| `SupersetLifecycleTask` | `lifecycle` | `-clone`, `-migrate`, `-rotate`, `-init` | Pods, ConfigMap |
| `SupersetCeleryBeat` | `celeryBeat` | `-celery-beat` | Deployment, ConfigMap |

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
create bare Pods. See the [Configuration guide](../user-guide/configuration.md#deployment-template) for
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

Result on SupersetCeleryWorker: `resources.limits = {cpu: "8"}` (resources
is a scalar/struct field — component replaces entirely).

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

Result on SupersetWebServer: both env vars present.

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

Each child CR carries a config checksum stamped as a pod template
annotation. When the checksum changes (due to config or secret reference
changes on the CR), Kubernetes triggers a rolling restart of the affected
component. Note: rotating a referenced Secret's value without changing the
CR does not trigger a rollout — use
[Force Reload](../user-guide/configuration.md#force-reload) for this case. See
[Internals](internals.md#checksum-driven-rollouts) for the full checksum
table and per-component isolation details.

---

## Resource Ownership

All resources use Kubernetes owner references for automatic cleanup. The parent
`Superset` CR owns child CRDs (SupersetLifecycleTask, SupersetWebServer, etc.),
networking resources (Ingress/HTTPRoute), ServiceMonitor, and NetworkPolicies.
Each child CR in turn owns its managed resources (Deployment, ConfigMap, Service,
HPA, PDB for component CRDs; Pods and ConfigMap for SupersetLifecycleTask). Deleting
the parent cascades to everything. Removing a component from the parent spec
deletes its child CR, which cascades to all owned resources.
