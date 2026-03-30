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

# User Guide

This guide covers the full configuration reference for the Superset operator.
For installation instructions, see [Installation](installation.md).

## Configuration

### Environment Mode

The `environment` field controls validation strictness (enforced by
[CEL](https://kubernetes.io/docs/reference/using-api/cel/) rules in the CRD schema):

- **`prod`** (default) — inline `secretKey`, `metastore.uri`, `metastore.password`, and `valkey.password` are rejected by CRD validation. Use `secretKeyFrom`, `metastore.uriFrom`, `metastore.passwordFrom`, or `valkey.passwordFrom` to reference Kubernetes Secrets.
- **`dev`** — allows plain-text `secretKey`, `metastore.uri`, `metastore.password`, and `valkey.password` directly in the CR for quick local development.

### Dev Mode Example

```yaml
spec:
  environment: dev
  secretKey: thisIsNotSecure_changeInProduction!
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  config: |
    FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
  webServer: {}
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
  config: |
    FEATURE_FLAGS = {"DASHBOARD_RBAC": True, "ALERT_REPORTS": True}
    ROW_LIMIT = 10000
  webServer: {}
```

### Metastore

The `metastore` field provides database connection configuration. There are two modes:

**Passthrough URI** — provide the full SQLAlchemy connection string. In dev mode, use `uri` inline. In prod mode, use `uriFrom` to reference a Kubernetes Secret:

```yaml
# Dev mode: inline URI
spec:
  environment: dev
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
```

```yaml
# Prod mode: URI from Secret
spec:
  metastore:
    uriFrom:
      name: db-credentials
      key: connection-string
```

`uri` and `uriFrom` are mutually exclusive with each other and with the structured fields below.

**Structured fields** — the operator sets individual env vars (`SUPERSET_OPERATOR__DB_HOST`, `SUPERSET_OPERATOR__DB_PORT`, `SUPERSET_OPERATOR__DB_NAME`, `SUPERSET_OPERATOR__DB_USER`, `SUPERSET_OPERATOR__DB_PASS`) that the generated config assembles into a connection URI. In prod mode, use `passwordFrom` to reference a Secret for the password:

```yaml
# Dev mode: inline password
spec:
  environment: dev
  metastore:
    type: postgresql
    host: db.example.com
    port: 5432
    database: superset
    username: superset
    password: secret
```

```yaml
# Prod mode: password from Secret
spec:
  metastore:
    type: postgresql
    host: db.example.com
    port: 5432
    database: superset
    username: superset
    passwordFrom:
      name: db-credentials
      key: password
```

`password` and `passwordFrom` are mutually exclusive.

### Valkey

The `valkey` field configures Valkey (or Redis) as the cache backend, Celery message broker, and SQL Lab results backend. Setting `valkey.host` auto-generates all cache, Celery, and results backend configuration with sensible defaults:

```yaml
# Minimal: one field configures everything
spec:
  valkey:
    host: valkey.default.svc
```

This generates a complete `superset_config.py` with `CACHE_CONFIG`, `DATA_CACHE_CONFIG`, `FILTER_STATE_CACHE_CONFIG`, `EXPLORE_FORM_DATA_CACHE_CONFIG`, `THUMBNAIL_CACHE_CONFIG`, `CeleryConfig`, and `RESULTS_BACKEND` — each cache section backed by a separate Valkey database for isolation (Celery broker and result backend share database 0 by default).

**Default database assignments:**

| Section | Superset Config Key | Valkey DB | Key Prefix | Timeout |
|---|---|---|---|---|
| `cache` | `CACHE_CONFIG` | 1 | `superset_` | 300s |
| `dataCache` | `DATA_CACHE_CONFIG` | 2 | `superset_data_` | 86400s |
| `filterStateCache` | `FILTER_STATE_CACHE_CONFIG` | 3 | `superset_filter_` | 3600s |
| `exploreFormDataCache` | `EXPLORE_FORM_DATA_CACHE_CONFIG` | 4 | `superset_explore_` | 3600s |
| `thumbnailCache` | `THUMBNAIL_CACHE_CONFIG` | 5 | `superset_thumbnail_` | 3600s |
| `celeryBroker` | `CeleryConfig.broker_url` | 0 | — | — |
| `celeryResultBackend` | `CeleryConfig.result_backend` | 0 | — | — |
| `resultsBackend` | `RESULTS_BACKEND` | 6 | `superset_results_` | — |

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

In dev mode, `valkey.password` can be set inline. In prod mode (default), use `valkey.passwordFrom` to reference a Kubernetes Secret — the operator injects the password via `valueFrom.secretKeyRef`.

#### SSL/TLS

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

### Reserved Environment Variables

The operator sets certain env vars automatically based on the CR spec. These are organized into tiers:

| Env Var | Tier | Set by | Description |
|---|---|---|---|
| `SUPERSET_OPERATOR__SECRET_KEY` | Operator-internal | Operator (from `secretKey` or `secretKeyFrom`) | Superset session signing key |
| `SUPERSET_OPERATOR__DB_URI` | Operator-internal | Operator (from `metastore.uri` or `metastore.uriFrom`) | Full database connection URI |
| `SUPERSET_OPERATOR__DB_HOST`, `SUPERSET_OPERATOR__DB_PORT`, `SUPERSET_OPERATOR__DB_NAME` | Operator-internal | Operator (structured metastore) | Database connection fields |
| `SUPERSET_OPERATOR__DB_USER`, `SUPERSET_OPERATOR__DB_PASS` | Operator-internal | Operator (from metastore structured fields or `passwordFrom`) | Database credentials |
| `SUPERSET_OPERATOR__VALKEY_HOST`, `SUPERSET_OPERATOR__VALKEY_PORT` | Operator-internal | Operator (from `valkey`) | Valkey connection fields |
| `SUPERSET_OPERATOR__VALKEY_PASS` | Operator-internal | Operator (from `valkey.password` or `valkey.passwordFrom`) | Valkey password |
| `SUPERSET_OPERATOR__FORCE_RELOAD` | Operator-internal | Operator (from `spec.forceReload`) | Triggers rolling restart |
| `PYTHONPATH` | Standard | Operator | Python module search path |
| `SUPERSET_WEBSERVER_PORT` | Standard | Rendered in config | Web server port (8088) |

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

### Custom Python Config

The `config` field accepts raw Python that is appended after the operator-generated config. It is available at the top level (base config, shared by all Python components) and per component (component config):

```yaml
spec:
  # Base config: appended to ALL Python components
  config: |
    FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
    ROW_LIMIT = 10000

  # Component config: appended after base config for this component only
  celeryWorker:
    config: |
      CELERY_ANNOTATIONS = {"tasks.add": {"rate_limit": "10/s"}}
```

Both fields are **concatenated**, not mutually exclusive. In this example, the celery worker's `superset_config.py` contains the operator-generated configs (`SECRET_KEY`, structured DB URI if applicable), then the base config (FEATURE_FLAGS, ROW_LIMIT), then the component config (CELERY_ANNOTATIONS). The web server receives only the operator-generated configs and the base config, since it has no component-specific `config` field set.

See [Config Rendering Pipeline](architecture.md#config-rendering-pipeline) for the full rendering order and an example of the generated output.

### Celery Configuration

Enable Celery workers for background tasks (caching, scheduled reports, long-running queries) by setting `celeryWorker` and `celeryBeat`. When `spec.valkey` is configured, the Celery broker and result backend are auto-generated. Otherwise, provide Celery config manually via `config`:

```yaml
# With valkey (recommended): Celery config auto-generated
spec:
  valkey:
    host: valkey.default.svc
  celeryWorker: {}
  celeryBeat: {}
```

```yaml
# Without valkey: manual Celery config
spec:
  config: |
    class CeleryConfig:
        broker_url = "redis://valkey:6379/0"
        result_backend = "redis://valkey:6379/1"
    CELERY_CONFIG = CeleryConfig
  celeryWorker: {}
  celeryBeat: {}
```

### MCP Server

Enable the [Model Context Protocol](https://modelcontextprotocol.io/) server by setting `mcpServer`. This deploys a Python-based FastMCP server that exposes Superset's API via MCP, allowing AI assistants and LLM-based tools to interact with Superset:

```yaml
spec:
  mcpServer: {}
```

The MCP server receives a `superset_config.py` with core config (`SECRET_KEY`, structured DB URI if applicable) and top-level/per-component `config` — but not web server port. It runs as a separate Deployment with its own Service.

### Init Configuration

The `spec.init` section supports `podTemplate` with the same Pod and container
fields as other components (tolerations, nodeSelector, volumes, etc. on
`podTemplate`; env, resources, securityContext, etc. on `podTemplate.container`),
so init pods inherit top-level scheduling and security settings and can be
customized independently. Init is enabled by default even when `spec.init` is
nil; disable it explicitly with `spec.init.disabled: true`.

#### Admin User (Dev Mode Only)

In dev mode, the operator can create an admin user during initialization:

```yaml
spec:
  environment: dev
  init:
    adminUser:
      username: admin           # default
      password: admin           # default
      firstName: Superset       # default
      lastName: Admin           # default
      email: admin@example.com  # default
```

All fields have defaults, so `adminUser: {}` creates a user with
username/password `admin`/`admin`. The operator passes credentials as env vars
and appends a `superset fab create-admin` step to the init command. This field
is rejected in prod mode by CRD validation.

#### Load Examples (Dev Mode Only)

Load Superset's example dashboards and datasets during initialization:

```yaml
spec:
  environment: dev
  init:
    loadExamples: true
```

The operator appends a `superset load-examples` step to the init command. This
field is rejected in prod mode by CRD validation. Note that Superset's built-in
examples require an admin user with username `admin` — if you customize
`adminUser.username`, example loading may fail.

Both `adminUser` and `loadExamples` are mutually exclusive with a custom
`init.command` — when using these fields, the operator constructs the full
init command automatically.

### Health Probes

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

### Security Context

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

### Autoscaling (HPA)

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

### Pod Disruption Budget

```yaml
spec:
  webServer:
    podDisruptionBudget:
      minAvailable: 1
```

## Networking

### Gateway API (Recommended)

Requires [Gateway API CRDs](https://gateway-api.sigs.k8s.io/) installed on the cluster. Gateway API is not included in Kubernetes and must be [installed separately](https://gateway-api.sigs.k8s.io/guides/#installing-gateway-api). If the CRDs are absent, the operator logs a message and skips HTTPRoute management.

```yaml
spec:
  networking:
    gateway:
      gatewayRef:
        name: my-gateway
        namespace: gateway-system
      hostnames:
        - superset.example.com
```

The operator creates an `HTTPRoute` with:
- `/ws` -> websocket-server Service (if enabled)
- `/mcp` -> mcp-server Service (if enabled)
- `/flower` -> celery-flower Service (if enabled)
- `/` -> web-server Service (if enabled)

Paths are configurable via `service.gatewayPath` on each component. For
example, to serve Celery Flower under `/monitoring`:

```yaml
spec:
  celeryFlower:
    service:
      gatewayPath: /monitoring
```

### Ingress (Legacy)

Gateway API and Ingress are mutually exclusive — set one or the other, not both.

```yaml
spec:
  networking:
    ingress:
      className: nginx
      annotations:
        nginx.ingress.kubernetes.io/proxy-body-size: "100m"
      hosts:
        - host: superset.example.com
          paths:
            - path: /
              pathType: Prefix
      tls:
        - secretName: superset-tls
          hosts:
            - superset.example.com
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

### Child CR and Sub-Resource Names

Child CRs share the parent's name (differentiated by Kind). For example, a
parent named `my-superset` creates `SupersetWebServer/my-superset`,
`SupersetCeleryWorker/my-superset`, etc. Sub-resources (Deployments, Services,
ConfigMaps) are named `{parentName}-{componentType}` (e.g.
`my-superset-web-server`).

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
| **Component wins if set** | `resources`, `affinity`, `securityContext`, `podSecurityContext`, `priorityClassName`, `strategy`, `revisionHistoryLimit`, probes, `lifecycle`, `dnsPolicy`, `dnsConfig`, `runtimeClassName`, `shareProcessNamespace`, `enableServiceLinks`, `terminationGracePeriodSeconds`, `minReadySeconds`, `progressDeadlineSeconds` |
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

**Container level** (`podTemplate.container.*`):

| Field | Description |
|---|---|
| `resources` | CPU/memory requests and limits |
| `env` | Environment variables (merged by name) |
| `envFrom` | ConfigMap/Secret env sources (appended) |
| `volumeMounts` | Volume mounts (merged by name) |
| `securityContext` | Container-level security context |
| `command` | Container entrypoint (no inheritance) |
| `args` | Container arguments (no inheritance) |
| `livenessProbe` | Liveness probe |
| `readinessProbe` | Readiness probe |
| `startupProbe` | Startup probe |
| `lifecycle` | preStop/postStart lifecycle hooks |

### Init pod

The init pod uses `podTemplate` instead of `deploymentTemplate` (since it creates
bare Pods, not Deployments):

```yaml
spec:
  init:
    podTemplate:
      container:
        resources:
          limits:
            memory: "2Gi"
    command: ["/bin/sh", "-c", "superset db upgrade && superset init"]
```

## Monitoring

### Prometheus ServiceMonitor

Requires [prometheus-operator](https://prometheus-operator.dev/) CRDs. The operator gracefully skips if they are not installed.

```yaml
spec:
  monitoring:
    serviceMonitor:
      interval: 30s
      labels:
        release: prometheus
```

## Network Policies

```yaml
spec:
  networkPolicy:
    extraIngress: []
    extraEgress: []
```

Creates per-component NetworkPolicies that:
- Allow ingress from other components of the same Superset instance (matched by `app.kubernetes.io/name: superset` + `superset.apache.org/parent` labels — multiple Superset instances in the same namespace are isolated from each other)
- Allow ingress on the service port from any source for externally-facing components (web server, Celery Flower, websocket server, MCP server) — this is necessary because ingress controllers and load balancers typically reside outside the namespace and cannot be matched with a pod selector
- Allow all egress (for database/cache access)
- Support custom `extraIngress` and `extraEgress` rules

If you need to restrict external ingress to specific sources, disable the built-in
network policy and create your own NetworkPolicy resources with the desired `from`
selectors.

## Force Reload

Trigger a rolling restart of all components:

```yaml
spec:
  forceReload: "2026-03-14T12:00:00Z"
```

Change the value to any new string to trigger a restart.

## Suspend Reconciliation

Temporarily pause reconciliation without deleting resources:

```yaml
spec:
  suspend: true
```

When suspended, the operator stops all reconciliation — no init pods run, no
child CRs are created or updated, and no resources are deleted. Set
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

Use the [Redis Operator](https://github.com/spotahome/redis-operator) or [Bitnami Redis Helm chart](https://github.com/bitnami/charts/tree/main/bitnami/redis) for Redis, and configure the connection via `config`.

## Migration from Helm Chart

| Helm Chart Value | Operator Equivalent |
|-----------------|---------------------|
| `image.repository` + `image.tag` | `spec.image.repository` + `spec.image.tag` |
| `supersetNode.connections` | `spec.metastore` (with `uriFrom` or structured fields) |
| `supersetNode.replicaCount` | `spec.webServer.replicas` |
| `supersetWorker.replicaCount` | `spec.celeryWorker.replicas` |
| `supersetCeleryBeat.enabled` | `spec.celeryBeat: {}` (set) or omit (disabled) |
| `supersetCeleryFlower.enabled` | `spec.celeryFlower: {}` (set) or omit (disabled) |
| `supersetWebsockets.enabled` | `spec.websocketServer: {}` (set) or omit (disabled) |
| `configOverrides` | `spec.config` |
| `ingress.*` | `spec.networking.ingress` |
| `service.*` | `spec.webServer.service` |
| `resources.*` | `spec.podTemplate.container.resources` (top-level) or per-component |
| `nodeSelector` | `spec.podTemplate.nodeSelector` (top-level) or per-component (merged) |
| `tolerations` | `spec.podTemplate.tolerations` (top-level) or per-component (appended) |
| `affinity` | `spec.podTemplate.affinity` (top-level) or per-component (replaces) |
| `extraEnv` | `spec.podTemplate.container.env` (top-level) or per-component (merged) |
| `postgresql.*` | Not managed -- use CloudNativePG or managed services |
| `redis.*` | Not managed -- use Redis Operator or managed services |