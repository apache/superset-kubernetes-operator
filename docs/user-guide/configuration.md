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

# Configuration

This guide covers the full configuration reference for the Superset operator.
For installation instructions, see [Installation](installation.md).
For lifecycle (migrations, upgrades), see [Lifecycle](lifecycle.md).

## Environment Mode

The `environment` field controls validation strictness (enforced by
[CEL](https://kubernetes.io/docs/reference/using-api/cel/) rules in the CRD schema):

- **`Production`** (default) — inline `secretKey`, `metastore.uri`, `metastore.password`, and `valkey.password` are rejected by CRD validation. Use `secretKeyFrom`, `metastore.uriFrom`, `metastore.passwordFrom`, or `valkey.passwordFrom` to reference Kubernetes Secrets.
- **`Staging`** — same secret restrictions as Production, but allows `lifecycle.clone` for database cloning from an external source.
- **`Development`** — allows plain-text `secretKey`, `metastore.uri`, `metastore.password`, and `valkey.password` directly in the CR for quick local development. Also permits `lifecycle.clone`, `lifecycle.init.adminUser`, and `lifecycle.init.loadExamples`.

### Dev Mode Example

```yaml
spec:
  environment: Development
  secretKey: thisIsNotSecure_changeInProduction!
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  featureFlags:
    ENABLE_TEMPLATE_PROCESSING: true
  webServer: {}
  lifecycle:
    init:
      adminUser: {}
      loadExamples: true
```

### Prod Mode Example

Use `secretKeyFrom` and `metastore.uriFrom` to reference Kubernetes Secrets. The operator injects the corresponding env vars with `valueFrom.secretKeyRef`:

```yaml
spec:
  image:
    tag: "6.0.1"
  secretKeyFrom:
    name: superset-secret
    key: secret-key
  metastore:
    uriFrom:
      name: db-credentials
      key: connection-string
  featureFlags:
    DASHBOARD_RBAC: true
    ALERT_REPORTS: true
  config: |
    ROW_LIMIT = 10000
  webServer: {}
```

## Metastore

The `metastore` field provides database connection configuration. There are two modes:

**Passthrough URI** — provide the full SQLAlchemy connection string. In Development mode, use `uri` inline. In Staging or Production, use `uriFrom` to reference a Kubernetes Secret:

```yaml
# Development mode: inline URI
spec:
  environment: Development
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
```

```yaml
# Staging/Production: URI from Secret
spec:
  metastore:
    uriFrom:
      name: db-credentials
      key: connection-string
```

`uri` and `uriFrom` are mutually exclusive with each other and with the structured fields below.

**Structured fields** — the operator sets individual env vars (`SUPERSET_OPERATOR__DB_HOST`, `SUPERSET_OPERATOR__DB_PORT`, `SUPERSET_OPERATOR__DB_NAME`, `SUPERSET_OPERATOR__DB_USER`, `SUPERSET_OPERATOR__DB_PASS`) that the generated config assembles into a connection URI. In Staging or Production, use `passwordFrom` to reference a Secret for the password:

```yaml
# Development mode: inline password
spec:
  environment: Development
  metastore:
    type: PostgreSQL
    host: db.example.com
    port: 5432
    database: superset
    username: superset
    password: secret
```

```yaml
# Staging/Production: password from Secret
spec:
  metastore:
    type: PostgreSQL
    host: db.example.com
    port: 5432
    database: superset
    username: superset
    passwordFrom:
      name: db-credentials
      key: password
```

`password` and `passwordFrom` are mutually exclusive.

Structured mode defaults to `postgresql+psycopg2` for PostgreSQL and
`mysql+mysqldb` for MySQL. If your image installs a different SQLAlchemy
driver, set `metastore.driver`:

```yaml
spec:
  metastore:
    type: MySQL
    driver: pymysql
    host: mysql.example.com
    database: superset
    username: superset
    passwordFrom:
      name: db-credentials
      key: password
```

### Auto-creating the database

Setting `metastore.createDatabase: true` instructs the operator to attach a one-shot init container to the migrate Job that issues `CREATE DATABASE` against the server before `superset db upgrade` runs. The step is idempotent — existing databases are detected and the init container exits cleanly, so re-applying or re-running migrations is safe.

```yaml
spec:
  metastore:
    type: PostgreSQL
    host: db.example.com
    database: superset
    username: superset
    passwordFrom:
      name: db-credentials
      key: password
    createDatabase: true
```

Requirements and caveats:

- **Structured metastore only.** Rejected by CRD validation when `uri` or `uriFrom` is set — the operator needs the individual host/database/username fields to issue admin-level statements.
- **Privileges.** The configured metastore user must have `CREATEDB` (PostgreSQL) or `CREATE` (MySQL) privilege on the server. The init container connects to the `postgres` admin database (PostgreSQL) or runs `CREATE DATABASE IF NOT EXISTS` (MySQL).
- **Init container image.** The operator uses `postgres:17-alpine` or `mysql:8-alpine` (matching the clone task) — the Superset image is not assumed to ship database client tools.
- **Resources and securityContext are inherited from `spec.lifecycle.podTemplate.container`.** Whatever you set on `spec.lifecycle.podTemplate.container.resources` and `spec.lifecycle.podTemplate.container.securityContext` is applied to the create-database init container. This lets you satisfy strict admission policies (Pod Security Standards `restricted`, Kyverno, OPA) without a dedicated knob.
- **Redundant with `lifecycle.clone`.** Clone already drops and re-creates its target database every time it runs, so toggling `createDatabase` on alongside clone is harmless but does no extra work in practice — the init container detects the existing database (created by clone) and no-ops.

## Valkey

The `valkey` field configures Valkey (or Redis) as the cache backend, Celery message broker, and SQL Lab results backend. Setting `valkey.host` auto-generates all cache, Celery, and results backend configuration with sensible defaults:

```yaml
# Minimal: one field configures everything
spec:
  valkey:
    host: valkey.default.svc
```

This generates a `superset_config.py` with `CACHE_CONFIG`, `DATA_CACHE_CONFIG`, `FILTER_STATE_CACHE_CONFIG`, `EXPLORE_FORM_DATA_CACHE_CONFIG`, `THUMBNAIL_CACHE_CONFIG`, `DISTRIBUTED_COORDINATION_CONFIG`, a connectivity-only `CeleryConfig`, and `RESULTS_BACKEND` — each cache section backed by a separate Valkey database for isolation (Celery broker and result backend share database 0 by default). Celery application behavior such as imports, task routes, and beat schedules remains explicit Python config.

**Default database assignments:**

| Section | Superset Config Key | Valkey DB | Key Prefix | Timeout |
|---|---|---|---|---|
| `cache` | `CACHE_CONFIG` | 1 | `superset_` | 300s |
| `dataCache` | `DATA_CACHE_CONFIG` | 2 | `superset_data_` | 86400s |
| `filterStateCache` | `FILTER_STATE_CACHE_CONFIG` | 3 | `superset_filter_` | 3600s |
| `exploreFormDataCache` | `EXPLORE_FORM_DATA_CACHE_CONFIG` | 4 | `superset_explore_` | 3600s |
| `thumbnailCache` | `THUMBNAIL_CACHE_CONFIG` | 5 | `superset_thumbnail_` | 3600s |
| `distributedCoordination` | `DISTRIBUTED_COORDINATION_CONFIG` | 7 | `coordination_` | 300s |
| `celeryBroker` | `CeleryConfig.broker_url` | 0 | — | — |
| `celeryResultBackend` | `CeleryConfig.result_backend` | 0 | — | — |
| `resultsBackend` | `RESULTS_BACKEND` | 6 | `superset_results_` | — |

`distributedCoordination` (`DISTRIBUTED_COORDINATION_CONFIG`) backs Superset's real-time pub/sub messaging, atomic distributed locks (Redis `SET NX EX`), and Global Task Framework signaling. It is recommended for production deployments and will eventually replace `GLOBAL_ASYNC_QUERIES_CACHE_BACKEND` as the standard signaling backend.

**Instance-scoped key prefixes.** Every rendered `CACHE_KEY_PREFIX` (and the results backend's `key_prefix`) is automatically prefixed with the parent CR name at runtime — e.g. a `Superset` named `prod` produces `prod_superset_`, `prod_superset_data_`, `prod_coordination_`, etc. This prevents key collisions when multiple Superset deployments share a single Valkey instance. The prefix value you set on a section is appended after the instance name; setting `keyPrefix: "myapp_"` on `cache` yields `prod_myapp_`.

Each section can be individually tuned or disabled:

```yaml
spec:
  valkey:
    host: valkey.default.svc
    port: 6380
    passwordFrom:
      name: valkey-credentials
      key: password
    cache:
      defaultTimeout: 600
    dataCache:
      database: 10
      defaultTimeout: 43200
    filterStateCache:
      disabled: true    # fall back to Superset's built-in default
    celeryBroker:
      database: 14
    celeryResultBackend:
      database: 15
```

In Development mode, `valkey.password` can be set inline. In Staging or Production, use `valkey.passwordFrom` to reference a Kubernetes Secret — the operator injects the password via `valueFrom.secretKeyRef`.

### SSL/TLS

Enable SSL by setting the `ssl` field. For simple SSL (encrypted connection, no client certificates), set `ssl: {}`. For mTLS, provide certificate file paths:

```yaml
spec:
  valkey:
    host: valkey.default.svc
    passwordFrom:
      name: valkey-credentials
      key: password
    ssl:
      certRequired: required   # "required" (default), "optional", or "none"
      keyFile: /mnt/tls/client.key.pem
      certFile: /mnt/tls/client.crt.pem
      caCertFile: /mnt/tls/ca.pem
```

Mount the certificate files via the top-level `podTemplate` so they are available to all components:

```yaml
spec:
  podTemplate:
    volumes:
      - name: valkey-tls
        secret:
          secretName: valkey-tls-certs
    container:
      volumeMounts:
        - name: valkey-tls
          mountPath: /mnt/tls
          readOnly: true
  valkey:
    host: valkey.default.svc
    ssl:
      keyFile: /mnt/tls/tls.key
      certFile: /mnt/tls/tls.crt
      caCertFile: /mnt/tls/ca.crt
```

## Reserved Environment Variables

The operator sets certain env vars automatically based on the CR spec. These are organized into tiers:

| Env Var | Tier | Set by | Description |
|---|---|---|---|
| `SUPERSET_OPERATOR__INSTANCE_NAME` | Operator-internal | Operator (from parent CR `metadata.name`) | Parent CR name; available for use in raw `spec.config` (e.g. instance-scoped Celery queue names) |
| `SUPERSET_OPERATOR__SECRET_KEY` | Operator-internal | Operator (from `secretKey` or `secretKeyFrom`) | Superset session signing key |
| `SUPERSET_OPERATOR__DB_URI` | Operator-internal | Operator (from `metastore.uri` or `metastore.uriFrom`) | Full database connection URI |
| `SUPERSET_OPERATOR__DB_HOST`, `SUPERSET_OPERATOR__DB_PORT`, `SUPERSET_OPERATOR__DB_NAME` | Operator-internal | Operator (structured metastore) | Database connection fields |
| `SUPERSET_OPERATOR__DB_USER`, `SUPERSET_OPERATOR__DB_PASS` | Operator-internal | Operator (from metastore structured fields or `passwordFrom`) | Database credentials |
| `SUPERSET_OPERATOR__VALKEY_HOST`, `SUPERSET_OPERATOR__VALKEY_PORT` | Operator-internal | Operator (from `valkey`) | Valkey connection fields |
| `SUPERSET_OPERATOR__VALKEY_PASS` | Operator-internal | Operator (from `valkey.password` or `valkey.passwordFrom`) | Valkey password |
| `SUPERSET_OPERATOR__FORCE_RELOAD` | Operator-internal | Operator (from `spec.forceReload`) | Triggers rolling restart |
| `SUPERSET_WEBSERVER_PORT` | Standard | Rendered in config | Web server port (8088) |

The operator does **not** set `PYTHONPATH` — it relies on the upstream Superset image's default (which already includes `/app/pythonpath`, where the operator mounts the rendered `superset_config.py`). Custom images must preserve this entry on `PYTHONPATH` for the rendered config to be picked up.

**Tier descriptions:**

- **Operator-internal transport vars** (`SUPERSET_OPERATOR__` prefix) are used by the operator to pass values into the rendered `superset_config.py`. They are not recognized by Superset directly — the operator renders them as Python assignments (e.g., `SECRET_KEY = os.environ['SUPERSET_OPERATOR__SECRET_KEY']`).
- **Standard env vars** have no special prefix.

**Which env vars are set per metastore mode:**

| Env Var | `metastore.uri` | `metastore.uriFrom` | Structured (`host`, ...) |
|---|:---:|:---:|:---:|
| `SUPERSET_OPERATOR__DB_URI`  | Set (plain text) | Set (`valueFrom`) | — |
| `SUPERSET_OPERATOR__DB_HOST` | — | — | Set |
| `SUPERSET_OPERATOR__DB_PORT` | — | — | Set |
| `SUPERSET_OPERATOR__DB_NAME` | — | — | Set (if `database` provided) |
| `SUPERSET_OPERATOR__DB_USER` | — | — | Set (if `username` provided) |
| `SUPERSET_OPERATOR__DB_PASS` | — | — | Set (plain text or `valueFrom`) |

In both passthrough and structured modes, the operator renders `SQLALCHEMY_DATABASE_URI` in `superset_config.py` from the operator-internal env vars. Passthrough mode reads from `SUPERSET_OPERATOR__DB_URI`, while structured mode assembles an f-string URI from the `SUPERSET_OPERATOR__DB_*` env vars.

**Which env vars are set when `valkey` is configured:**

| Env Var | Set when |
|---|---|
| `SUPERSET_OPERATOR__VALKEY_HOST` | Always (from `valkey.host`) |
| `SUPERSET_OPERATOR__VALKEY_PORT` | Always (from `valkey.port`, default 6379) |
| `SUPERSET_OPERATOR__VALKEY_PASS` | `valkey.password` (dev, plain text) or `valkey.passwordFrom` (prod, `valueFrom`) |

## Custom Python Config

The `config` field accepts raw Python that is appended after the operator-generated config. It is available at the top level (base config, shared by all Python components) and per component (component config).

The operator exposes a curated set of knobs as typed CRD fields — Kubernetes resources, Kubernetes Secret references, and managed connectivity that the operator can safely validate and wire across components (for example metastore URIs, Valkey-backed caches, Celery broker/backend URLs, lifecycle gating, and feature presets). Application behavior stays in `config` as Python. Over time, settings that prove broadly useful or error-prone may graduate from raw Python to typed fields. See [Configuration Philosophy](../architecture/overview.md#configuration-philosophy-typed-fields-vs-raw-python) for the rationale.

```yaml
spec:
  # Base config: appended to ALL Python components
  config: |
    ROW_LIMIT = 10000
    LOG_LEVEL = "INFO"

  # Component config: appended after base config for this component only
  celeryWorker:
    config: |
      CELERY_ANNOTATIONS = {"tasks.add": {"rate_limit": "10/s"}}
```

Both fields are **concatenated**, not mutually exclusive. In this example, the celery worker's `superset_config.py` contains the operator-generated configs (`SECRET_KEY`, structured DB URI if applicable), then the base config (`ROW_LIMIT`, `LOG_LEVEL`), then the component config (`CELERY_ANNOTATIONS`). The web server receives only the operator-generated configs and the base config, since it has no component-specific `config` field set.

See [Config Rendering Pipeline](../architecture/overview.md#config-rendering-pipeline) for the full rendering order and an example of the generated output.

## Bootstrap Script

`bootstrapScript` is an escape hatch for the default Python component and
lifecycle task commands. When set, the operator writes it as
`superset_bootstrap.sh` in the component or lifecycle ConfigMap and sources it
before the default command starts.

```yaml
spec:
  bootstrapScript: |
    pip install my-superset-plugin
```

The top-level value applies to web server, Celery worker, Celery Beat, Celery
Flower, MCP server, and lifecycle `migrate`, `rotate`, and `init` task Jobs.
The websocket server is a Node.js component and does not use this script. Clone
tasks also do not use it because they run a database-tool image rather than the
Superset image.

Components and lifecycle tasks can override the top-level script. Set the
override to an empty string to disable inheritance:

```yaml
spec:
  bootstrapScript: |
    pip install my-superset-plugin
  celeryWorker:
    bootstrapScript: ""
  lifecycle:
    bootstrapScript: |
      pip install migration-only-helper
```

If you override `podTemplate.container.command` or a lifecycle task `command`,
that command is responsible for sourcing `/app/pythonpath/superset_bootstrap.sh`
if it still needs the script. `bootstrapScript` is trusted shell code and is
stored in the generated ConfigMap, so do not place secrets in it. For production
dependency installation, a custom image is usually more repeatable than
installing packages on every pod start.

## Feature Flags

`spec.featureFlags` is a typed map of Superset feature flags rendered into `superset_config.py` as `FEATURE_FLAGS = {...}`. Keys conventionally use `UPPER_SNAKE_CASE` (e.g. `ALERT_REPORTS`, `THUMBNAILS`); values are booleans.

```yaml
spec:
  featureFlags:
    ALERT_REPORTS: true
    THUMBNAILS: true
    DASHBOARD_NATIVE_FILTERS: true
```

Keys are emitted in alphabetical order so the rendered config is deterministic and config checksums stay stable across reconciles. Setting `featureFlags: {}` (or omitting the field) leaves `FEATURE_FLAGS` unrendered, falling back to upstream Superset defaults.

For feature flags whose values aren't booleans (rare), use raw `spec.config` instead.

## Celery Configuration

Enable Celery workers for background tasks (caching, scheduled reports, long-running queries) by setting `celeryWorker` and `celeryBeat`. When `spec.valkey` is configured, the operator renders the Celery connectivity fields it can derive from the CRD: broker URL, result backend URL, and optional SSL settings.

```yaml
# With Valkey: connectivity is rendered from the CRD.
spec:
  valkey:
    host: valkey.default.svc
  celeryWorker: {}
  celeryBeat: {}
```

### What the operator renders

When `spec.valkey` is set, the operator renders a `CeleryConfig` class assigned to `CELERY_CONFIG`. This class intentionally contains only managed connectivity:

| Field | Source | Notes |
|---|---|---|
| `broker_url` | `spec.valkey` | f-string from operator-internal Valkey env vars |
| `result_backend` | `spec.valkey` | same |
| `broker_use_ssl` / `redis_backend_use_ssl` | `spec.valkey.ssl` | rendered when SSL is configured |

Application-level Celery behavior is not defaulted by the operator. Define imports, task routes, task annotations, acknowledgement behavior, beat schedules, scheduler expiration, and other Celery app settings explicitly in `spec.config`. This keeps the CRD focused on managed connectivity and avoids freezing Superset application defaults into the Kubernetes API.

Because assigning `CELERY_CONFIG` replaces Superset's own Celery config class, production deployments that enable Celery should include the Celery app settings they rely on. Put settings needed by multiple Python components in top-level `spec.config`; put Beat-only settings such as `beat_schedule` in `spec.celeryBeat.config` so schedule changes roll only the Celery Beat Deployment. The example below is a starting point; review the `superset_config.py` from the Superset version you deploy and tune it for your environment.

```yaml
spec:
  valkey:
    host: valkey.default.svc
  celeryWorker: {}
  celeryBeat:
    config: |
      from celery.schedules import crontab

      CELERY_BEAT_SCHEDULER_EXPIRES = 7 * 24 * 60 * 60

      CeleryConfig.beat_schedule = {
          "reports.scheduler": {
              "task": "reports.scheduler",
              "schedule": crontab(minute="*", hour="*"),
              "options": {"expires": CELERY_BEAT_SCHEDULER_EXPIRES},
          },
          "reports.prune_log": {
              "task": "reports.prune_log",
              "schedule": crontab(minute=0, hour=0),
          },
      }
  config: |
    CeleryConfig.imports = (
        "superset.sql_lab",
        "superset.tasks.scheduler",
        "superset.tasks.thumbnails",
        "superset.tasks.cache",
        "superset.tasks.slack",
    )
    CeleryConfig.worker_prefetch_multiplier = 1
    CeleryConfig.task_acks_late = False
    CeleryConfig.task_annotations = {
        "sql_lab.get_sql_results": {
            "rate_limit": "100/s",
        },
    }
```

#### Without Valkey

When `spec.valkey` is unset, the operator emits no `CeleryConfig` class. Provide both connectivity and app behavior manually via `spec.config`:

```yaml
spec:
  config: |
    from celery.schedules import crontab
    class CeleryConfig:
        broker_url = "redis://valkey:6379/0"
        result_backend = "redis://valkey:6379/1"
        imports = ("superset.sql_lab",)
        beat_schedule = {}
    CELERY_CONFIG = CeleryConfig
  celeryWorker: {}
  celeryBeat: {}
```

### Custom Queues And Routes

The operator-rendered `CeleryConfig` is a regular Python class. Extend it from `spec.config` by mutating attributes, subclassing, or replacing `CELERY_CONFIG` outright. For instance-scoped queue naming (preventing cross-instance queue collisions on a shared broker), the operator exposes the parent CR name as `SUPERSET_OPERATOR__INSTANCE_NAME`:

```yaml
spec:
  valkey:
    host: valkey.default.svc
  celeryWorker: {}
  celeryBeat: {}
  config: |
    import os
    from kombu import Queue

    INSTANCE = os.environ["SUPERSET_OPERATOR__INSTANCE_NAME"]

    CeleryConfig.task_queues = (
        Queue(f"{INSTANCE}-prio1", routing_key=f"{INSTANCE}-prio1.tasks", priority=0),
        Queue(f"{INSTANCE}-prio2", routing_key=f"{INSTANCE}-prio2.tasks", priority=1),
        Queue(f"{INSTANCE}-prio3", routing_key=f"{INSTANCE}-prio3.tasks", priority=2),
    )
    CeleryConfig.task_default_queue = f"{INSTANCE}-prio1"
    CeleryConfig.task_routes = {
        "sql_lab*": {"queue": f"{INSTANCE}-prio2", "routing_key": f"{INSTANCE}-prio2.tasks"},
        "cache*":   {"queue": f"{INSTANCE}-prio3", "routing_key": f"{INSTANCE}-prio3.tasks"},
        "reports*": {"queue": f"{INSTANCE}-prio3", "routing_key": f"{INSTANCE}-prio3.tasks"},
    }
```

Use `SUPERSET_OPERATOR__INSTANCE_NAME` whenever you need the parent CR name inside your Python config.

## Gunicorn Configuration

The operator manages Gunicorn worker parameters for the web server by injecting environment variables that Superset's `run-server.sh` reads. By default, even without an explicit `gunicorn` field, the operator injects balanced defaults (2 workers, 8 threads, gthread worker class).

Presets control **workers**, **threads**, and **workerClass**. All other fields have static defaults that you can override individually.

| Field | conservative | balanced (default) | performance | aggressive |
|---|---|---|---|---|
| workers | 1 | 2 | 4 | 8 |
| threads | 4 | 8 | 8 | 16 |
| workerClass | gthread | gthread | gthread | gthread |

Set `preset: disabled` to suppress env var injection entirely — Superset's `run-server.sh` built-in defaults will apply instead.

```yaml
spec:
  webServer:
    gunicorn:
      preset: performance      # 4 workers, 8 threads
      timeout: 120             # override static default (60)
      maxRequests: 1000        # enable worker recycling
      maxRequestsJitter: 50
```

The full set of configurable fields (static defaults in parentheses): `timeout` (60), `keepAlive` (2), `maxRequests` (0 = disabled), `maxRequestsJitter` (0), `limitRequestLine` (0 = unlimited), `limitRequestFieldSize` (0 = unlimited), `logLevel` (info).

## Celery Worker Configuration

The operator constructs the celery worker command from structured fields instead of the hardcoded default. Presets control **concurrency** and **pool** type:

| Field | conservative | balanced (default) | performance | aggressive |
|---|---|---|---|---|
| concurrency | 2 | 4 | 8 | 16 |
| pool | prefork | prefork | prefork | prefork |

Set `preset: disabled` to use the operator's built-in fallback command (`--pool=prefork -O fair -c 4`).

```yaml
spec:
  celeryWorker:
    celery:
      preset: performance        # 8 concurrency, prefork
      maxTasksPerChild: 1000     # recycle workers after 1000 tasks
      softTimeLimit: 3600        # 1h soft limit (raises SoftTimeLimitExceeded)
      timeLimit: 7200            # 2h hard kill
```

Additional fields (static defaults in parentheses): `optimization` (fair), `maxTasksPerChild` (0 = unlimited), `maxMemoryPerChild` (0 = disabled), `prefetchMultiplier` (4), `softTimeLimit` (0 = disabled), `timeLimit` (0 = disabled).

## SQLAlchemy Engine Options

The operator renders `SQLALCHEMY_ENGINE_OPTIONS` in each component's `superset_config.py`, with pool sizing computed from the component's execution model. By default (balanced preset), all components get sensible pool settings without any explicit configuration.

Presets control **poolClass**, **poolSize**, and **maxOverflow**:

| Preset | Pool class | pool\_size | max\_overflow |
|---|---|---|---|
| disabled | *(no rendering)* | — | — |
| conservative | NullPool | — | — |
| balanced (default) | QueuePool | 1 (web/celery/flower), 5 (MCP) | -1 (unlimited) |
| performance | QueuePool | workers (web), concurrency (celery), 1 (flower), 10 (MCP) | -1 |
| aggressive | QueuePool | workers × threads (web), concurrency (celery), 1 (flower), 20 (MCP) | -1 |

CeleryBeat and lifecycle tasks always use NullPool regardless of preset (singleton/short-lived components with minimal DB interaction). CeleryFlower uses standard pool sizing (defaults to 1 for performance/aggressive since it has no worker configuration).

`spec.sqlaEngineOptions` sets the baseline for all Python components. Per-component `sqlaEngineOptions` on `webServer`, `celeryWorker`, `celeryBeat`, `mcpServer`, or `lifecycle` replaces the top-level entirely (override semantics, not merge).

```yaml
spec:
  sqlaEngineOptions:
    preset: balanced             # applies to all components
    poolRecycle: 1800            # override static default (3600)
  webServer:
    gunicorn:
      preset: performance        # 4 workers, 8 threads
    sqlaEngineOptions:
      preset: performance        # pool_size=4 (gunicorn workers)
  celeryWorker:
    celery:
      concurrency: 12
    sqlaEngineOptions:
      preset: aggressive         # pool_size=12 (celery concurrency)
  celeryBeat: {}                 # always NullPool
```

Static defaults (same regardless of preset, overridable per-field): `poolRecycle` (3600), `poolPrePing` (false), `poolTimeout` (omitted — SQLAlchemy default 30s).

Individual field overrides take precedence over the preset computation:

```yaml
spec:
  sqlaEngineOptions:
    preset: balanced
    poolSize: 10                 # explicit: overrides preset calculation
    poolPrePing: true            # explicit: overrides static default
```

## Websocket Server

Enable Superset's async event streaming by setting `websocketServer`. This
deploys a **Node.js** application (not Python) that pushes real-time updates to
dashboards via WebSocket connections.

!!! warning "Requires a dedicated image"
    The websocket server is a separate Node.js application and **does not run
    from the default Superset image**. You must provide an image that contains
    `websocket_server.js` — the CRD enforces this with a CEL rule that rejects
    `websocketServer` set without an `image.repository` override. A
    community-maintained image is available at
    [`oneacrefund/superset-websocket`](https://hub.docker.com/r/oneacrefund/superset-websocket)
    (experimental, not officially supported by Apache Superset).

```yaml
spec:
  websocketServer:
    image:
      repository: oneacrefund/superset-websocket
      tag: "latest"
```

Because the websocket server is Node.js-based, it does **not** receive a
`superset_config.py`, and `sqlaEngineOptions` is not available on this
component. Configuration can be provided with environment variables, inline
Development-only `config`, or a Secret-backed `configFrom`.

```yaml
spec:
  websocketServer:
    image:
      repository: oneacrefund/superset-websocket
      tag: "latest"
    podTemplate:
      container:
        env:
          - name: SUPERSET_WEBSERVER_URL
            value: "http://my-superset-web-server:8088"
```

Inline `config` renders `config.json` and mounts it at
`/home/superset-websocket/config.json`. It is allowed only in Development mode
because websocket config commonly contains `jwtSecret` or Redis credentials:

```yaml
spec:
  environment: Development
  websocketServer:
    image:
      repository: oneacrefund/superset-websocket
      tag: "latest"
    config:
      port: 8080
      logLevel: debug
      jwtSecret: CHANGE-ME
      jwtCookieName: async-token
      redis:
        host: redis.default.svc
        port: 6379
        db: 0
```

In Staging and Production, store `config.json` in a Secret and reference it:

```yaml
spec:
  websocketServer:
    image:
      repository: oneacrefund/superset-websocket
      tag: "latest"
    configFrom:
      name: superset-websocket-config
      key: config.json
```

The operator mounts the Secret key without reading or copying the Secret. If
the Secret content changes, update `spec.forceReload` to roll websocket pods.

The websocket server creates a Service (default port 8080) and supports the
same scaling, deployment template, and pod template fields as other scalable
components.

## MCP Server

Enable the [Model Context Protocol](https://modelcontextprotocol.io/) server by setting `mcpServer`. This deploys a Python-based FastMCP server that exposes Superset's API via MCP, allowing AI assistants and LLM-based tools to interact with Superset:

```yaml
spec:
  mcpServer: {}
```

The MCP server receives a `superset_config.py` with core config (`SECRET_KEY`, structured DB URI if applicable) and top-level/per-component `config` — but not web server port. It runs as a separate Deployment with its own Service (port 8088). The MCP server supports per-component `sqlaEngineOptions` with higher default pool sizes than other components (5 for balanced, 10 for performance, 20 for aggressive) to accommodate concurrent tool invocations.

## Health Probes

```yaml
spec:
  webServer:
    podTemplate:
      container:
        livenessProbe:
          httpGet:
            path: /health
            port: 8088
          initialDelaySeconds: 15
          periodSeconds: 15
        readinessProbe:
          httpGet:
            path: /health
            port: 8088
          initialDelaySeconds: 15
          periodSeconds: 15
```

## Security Context

Security context can be set at the top level (shared by all components) or overridden per component:

```yaml
spec:
  podTemplate:
    podSecurityContext:
      runAsUser: 1000
      runAsNonRoot: true
      fsGroup: 1000
    container:
      securityContext:
        readOnlyRootFilesystem: true
        allowPrivilegeEscalation: false
```

Or override per component (replaces the top-level value):

```yaml
spec:
  webServer:
    podTemplate:
      podSecurityContext:
        runAsUser: 1000
        runAsNonRoot: true
```

## Autoscaling (HPA)

```yaml
spec:
  webServer:
    autoscaling:
      minReplicas: 2
      maxReplicas: 10
      metrics:
        - type: Resource
          resource:
            name: cpu
            target:
              type: Utilization
              averageUtilization: 75
```

When HPA is configured, the `replicas` field is ignored (HPA manages scaling).

## Pod Disruption Budget

```yaml
spec:
  webServer:
    podDisruptionBudget:
      minAvailable: 1
```

## Component Enable/Disable

All components follow the same rule: presence = enabled, absence = disabled. Set a component's spec to enable it (use `{}` for defaults), omit it or set to null to disable:

```yaml
spec:
  webServer:
    replicas: 2          # enabled with 2 replicas
  celeryWorker:
    replicas: 3          # enabled with 3 replicas
  celeryBeat: {}         # enabled with defaults
  celeryFlower: {}       # enabled with defaults
  websocketServer: null  # disabled (or omit entirely)
  mcpServer: {}          # enabled with defaults
```

A Superset CR with no components enabled is valid — the operator will run
initialization (if not disabled) but deploy no workloads. The parent status
will report `Phase: Running` with condition reason `NoComponentsEnabled`.

### Resource Names

Component resources are named `{parentName}-{componentType}`. For example, a
parent named `my-superset` creates resources such as
`my-superset-web-server`, `my-superset-celery-worker`, and
`my-superset-mcp-server`. ConfigMaps add the `-config` suffix, for example
`my-superset-web-server-config`. Lifecycle task Jobs use deterministic names
based on `{parentName}-{taskName}`, such as `my-superset-migrate`.

The parent name must be a valid DNS label: lowercase alphanumeric and hyphens
only (`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`), at most 63 characters. Since
sub-resource names append a component suffix, the parent name is further
constrained by the longest enabled component's suffix. The longest suffix is
`-websocket-server` (17 characters), so parent names must be at most 46
characters when websocket-server is enabled. CRD validation enforces the
appropriate limit for each enabled component.

## Deployment Template

The `deploymentTemplate` and `podTemplate` fields configure the Kubernetes
Deployment and Pod for each component. They mirror the Kubernetes hierarchy
as siblings:

```
deploymentTemplate                  → DeploymentSpec-level
podTemplate                         → PodSpec-level
└── container                       → main container
```

### Three usage patterns

**1. Omit entirely** — use operator defaults (most users start here):

```yaml
spec:
  image:
    tag: "6.0.1"
  webServer:
    replicas: 2
```

**2. Set top-level defaults** — apply to all components:

```yaml
spec:
  deploymentTemplate:
    revisionHistoryLimit: 3
  podTemplate:
    terminationGracePeriodSeconds: 60
    nodeSelector:
      workload: superset
    container:
      resources:
        limits:
          cpu: "2"
          memory: "4Gi"
      env:
        - name: LOG_LEVEL
          value: INFO

  webServer:
    replicas: 2
  celeryWorker:
    replicas: 4
```

All components inherit the deployment template, pod template (node selector,
termination grace period), and container template (resources, env vars).

**3. Per-component customization** — field-level merge with top-level:

```yaml
spec:
  deploymentTemplate:
    revisionHistoryLimit: 3
  podTemplate:
    container:
      resources:
        limits:
          cpu: "2"
          memory: "4Gi"
      env:
        - name: LOG_LEVEL
          value: INFO

  webServer:
    replicas: 2
    deploymentTemplate:
      strategy:
        type: RollingUpdate
        rollingUpdate:
          maxSurge: 1
          maxUnavailable: 0
    podTemplate:
      container:
        resources:
          limits:
            cpu: "4"             # replaces entire resources struct
            memory: "8Gi"
        command: ["gunicorn"]
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sh", "-c", "sleep 15"]

  celeryWorker:
    replicas: 8
    podTemplate:
      container:
        env:
          - name: CELERY_CONCURRENCY
            value: "8"           # merged with top-level LOG_LEVEL env var
```

### Merge semantics

Per-component `deploymentTemplate` and `podTemplate` are each **field-level
merged** independently with the top-level — you only specify what's different.

| Behavior | Fields |
|----------|--------|
| **Component wins if set** | `resources` (both pod-level and container-level), `affinity`, `securityContext`, `podSecurityContext`, `priorityClassName`, `strategy`, `revisionHistoryLimit`, probes, `lifecycle`, `dnsPolicy`, `dnsConfig`, `runtimeClassName`, `shareProcessNamespace`, `enableServiceLinks`, `terminationGracePeriodSeconds`, `minReadySeconds`, `progressDeadlineSeconds` |
| **Merge by name** | `env`, `volumes`, `volumeMounts`, `sidecars`, `initContainers` |
| **Merge by key** | `annotations`, `labels`, `nodeSelector`, `hostAliases` (by IP) |
| **Append** | `tolerations`, `topologySpreadConstraints`, `envFrom` |
| **No inheritance** | `command`, `args` (component-only, not inherited from top-level) |

**Note on append fields:** `tolerations`, `topologySpreadConstraints`, and `envFrom` are
concatenated (top-level first, then component-level) without deduplication. To avoid
duplicates in the final pod spec, define each entry at one level only — typically
top-level for shared entries and component-level for component-specific ones.

### Available fields

**Deployment level** (`deploymentTemplate.*`):

| Field | Description |
|---|---|
| `revisionHistoryLimit` | Old ReplicaSets to retain for rollback |
| `minReadySeconds` | Seconds before a pod is considered available |
| `progressDeadlineSeconds` | Seconds before a rollout is considered failed |
| `strategy` | Update strategy (RollingUpdate or Recreate) |

**Pod level** (`podTemplate.*`):

| Field | Description |
|---|---|
| `annotations` | Pod annotations (merged with operator-managed annotations) |
| `labels` | Pod labels (merged; operator labels cannot be overridden) |
| `affinity` | Pod/node affinity and anti-affinity |
| `tolerations` | Node tolerations (appended) |
| `nodeSelector` | Node label selector (merged by key) |
| `topologySpreadConstraints` | Topology spread constraints (appended) |
| `hostAliases` | /etc/hosts entries (merged by IP) |
| `podSecurityContext` | Pod-level security context |
| `priorityClassName` | Pod priority class |
| `volumes` | Volumes (merged by name with operator-injected volumes) |
| `sidecars` | Sidecar containers (merged by name) |
| `initContainers` | Init containers (merged by name) |
| `terminationGracePeriodSeconds` | Grace period for pod shutdown |
| `dnsPolicy` | DNS resolution policy |
| `dnsConfig` | Custom DNS configuration |
| `runtimeClassName` | RuntimeClass (e.g., gVisor, Kata) |
| `shareProcessNamespace` | Share PID namespace between containers |
| `enableServiceLinks` | Inject service environment variables |
| `resources` | Pod-level resource requirements (Kubernetes 1.34+, requires PodLevelResources feature gate) |

**Container level** (`podTemplate.container.*`):

| Field | Description |
|---|---|
| `resources` | CPU/memory requests and limits |
| `env` | Environment variables (merged by name) |
| `envFrom` | ConfigMap/Secret env sources (appended) |
| `volumeMounts` | Volume mounts (merged by name) |
| `ports` | Container ports (replaces operator defaults when set; the first resolved port is used as the Service `targetPort`, the ingress port for the operator-managed NetworkPolicy, and as the target port for any default probes the user did not override) |
| `securityContext` | Container-level security context |
| `command` | Container entrypoint (no inheritance) |
| `args` | Container arguments (no inheritance) |
| `livenessProbe` | Liveness probe |
| `readinessProbe` | Readiness probe |
| `startupProbe` | Startup probe |
| `lifecycle` | preStop/postStart lifecycle hooks |

**Pod-level resources** (Kubernetes 1.34+): When `podTemplate.resources` is set,
it defines the total resource budget for the entire pod, enabling resource
sharing among containers (main + sidecars). Container-level
`podTemplate.container.resources` remains available for per-container limits.

```yaml
spec:
  podTemplate:
    resources:
      requests:
        cpu: "4"
        memory: "8Gi"
      limits:
        cpu: "8"
        memory: "16Gi"
    container:
      resources:
        requests:
          cpu: "2"
          memory: "4Gi"
```

## Force Reload

Trigger a rolling restart of all components:

```yaml
spec:
  forceReload: "2026-03-14T12:00:00Z"
```

Change the value to any new string to trigger a restart. This is primarily
useful for **secret rotation**: when you update a Kubernetes Secret's data, pods
don't automatically restart because the operator references secrets via
`valueFrom.secretKeyRef` (resolved at pod creation time). Changing `forceReload`
forces new pods that pick up the updated secret values.

## Suspend Reconciliation

Temporarily pause reconciliation without deleting resources:

```yaml
spec:
  suspend: true
```

When suspended, the operator stops all reconciliation — no lifecycle task Jobs
run, no component resources are created or updated, and no resources are deleted. Set
`suspend: false` (or remove the field) to resume.

## Connecting PostgreSQL and Valkey

The operator does not manage database or cache infrastructure. Use one of these approaches:

### Managed Services

Set connection details via `secretKeyFrom`, `metastore.uriFrom`, and `valkey`:

```yaml
spec:
  secretKeyFrom:
    name: superset-secrets
    key: secret-key
  metastore:
    uriFrom:
      name: superset-db
      key: uri
  valkey:
    host: valkey.default.svc
    passwordFrom:
      name: valkey-credentials
      key: password
```

### CloudNativePG

Use [CloudNativePG](https://cloudnative-pg.io/) for PostgreSQL:

```yaml
# CloudNativePG Cluster (separate CR)
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: superset-pg
spec:
  instances: 3
  storage:
    size: 10Gi
```

Then reference the connection secret via `metastore.uriFrom` on your Superset CR.

### Redis Operator

Use the [Redis Operator](https://github.com/spotahome/redis-operator) or [Bitnami Redis Helm chart](https://github.com/bitnami/charts/tree/main/bitnami/redis) for Redis or Valkey, and configure the connection via `valkey`.
