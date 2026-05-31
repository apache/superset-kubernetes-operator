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

# Migration from Helm Chart

This guide helps translate a deployment from the official Apache Superset Helm
chart to a `Superset` custom resource. It focuses on feature parity and on the
places where the operator intentionally uses a different model.

> **Comparison target:** This guide is written against the upstream
> [`apache/superset` Helm chart](https://github.com/apache/superset/tree/superset-helm-chart-0.15.5/helm/superset)
> at chart version `0.15.5` / `appVersion: 5.0.0`. Older Helm releases share
> most field names, but some values differ across chart versions.

The operator covers the chart's core application features: web server, Celery
workers, Celery Beat, Celery Flower, websocket server, database migration,
application initialization, Python configuration, env vars, services, ingress,
autoscaling, disruption budgets, probes, security contexts, scheduling
constraints, sidecars, init containers, and custom volumes.

The main differences are deliberate:

- The operator does not install PostgreSQL or Redis/Valkey. Bring your own
  database and cache, then reference them from the `Superset` CR.
- Production mode rejects inline operator-managed secrets. Use Kubernetes
  Secrets through `secretKeyFrom`, `metastore.uriFrom`,
  `metastore.passwordFrom`, and `valkey.passwordFrom`.
- Lifecycle work is split into managed `migrate` and `init` tasks instead of a
  Helm hook Job. This gives the operator status, retries, upgrade gating, and
  maintenance-page support.
- Gateway API and Ingress are both first-class. Either one routes the web
  server plus the websocket, Flower, and MCP services from a single object —
  the operator expands a bare host into one rule per present component. Gateway
  API is preferred only when you need multi-gateway topologies or richer
  routing than Ingress can express.

## Migration Workflow

1. Export the Helm values used by the existing release:

   ```bash
   helm get values <release> -n <namespace> -o yaml > superset-values.yaml
   ```

2. Identify external dependencies. If the Helm release currently relied on the
   bundled PostgreSQL or Redis subcharts, install those dependencies as
   standalone Helm releases or otherwise provision equivalent services before
   moving Superset to the operator. Do not delete the Helm release until
   database persistence and backups are understood.

3. Create Kubernetes Secrets for the Superset `SECRET_KEY`, database
   connection string or password, and Valkey/Redis password. Prefer a full
   SQLAlchemy URI in `metastore.uriFrom` when migrating from Helm because it
   preserves the exact connection string.

   Check which Secret and key currently hold each credential before wiring the
   operator CR. With the upstream chart's default `secretEnv.create: true`, the
   chart creates a Helm-owned Secret named `<helm-fullname>-env` containing
   `DB_PASS`. If the bundled PostgreSQL subchart is enabled, Bitnami also
   creates a database Secret, typically `<release>-postgresql` with key
   `password`. Before uninstalling Helm, copy the credentials you intend to keep
   into Secrets that will remain after the release is removed, or point the
   operator at existing externally managed Secrets.

4. Translate application configuration from `configOverrides` into
   `spec.config`. Move sensitive values out of Python source and into Secret
   backed env vars.

5. Translate enabled chart components into operator components. In the
   operator, presence means enabled:

   ```yaml
   spec:
     webServer: {}
     celeryWorker: {}
     celeryBeat: {}
     celeryFlower: {}
     websocketServer:
       image:
         repository: oneacrefund/superset-websocket   # or your own image
         tag: latest
   ```

6. Decide how the first operator reconciliation should handle lifecycle tasks.
   If the existing Helm deployment already migrated and initialized the
   database, either allow the operator to run its idempotent `migrate` and
   `init` tasks during a planned window, or disable lifecycle initially:

   ```yaml
   spec:
     lifecycle:
       disabled: true
   ```

   Re-enable lifecycle after the first cutover if you want the operator to
   manage future upgrades.

7. Create the `Superset` CR with a name that will not collide with existing
   Helm resources, wait for it to become ready, then move traffic to the
   operator-managed Service, Ingress, or HTTPRoute.

8. Remove the old Helm release only after the operator-managed deployment is
   serving traffic and background workers are healthy.

## Value Mapping

### Global Settings

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `nameOverride`, `fullnameOverride` | `metadata.name` | The operator derives workload names from the CR name. Generated names are not independently overridden. |
| `extraLabels` | Resource-specific labels | Use `podTemplate.labels`, component `service.labels`, `networking.ingress.labels`, or `networking.gateway.labels`. There is no single global label field for every generated object. |
| `image.repository`, `image.tag`, `image.pullPolicy` | `spec.image.repository`, `spec.image.tag`, `spec.image.pullPolicy` | Per-component image overrides — including `pullPolicy` — are available under each component's `image` field; unset fields inherit from `spec.image`. |
| `imagePullSecrets` | `spec.image.pullSecrets` | Applied to all component pods. |
| `runAsUser` | `spec.podTemplate.podSecurityContext.runAsUser` | Prefer a full pod or container security context for production hardening. |
| `serviceAccountName`, `serviceAccount.create`, `serviceAccount.annotations` | `spec.serviceAccount.name`, `spec.serviceAccount.create`, `spec.serviceAccount.annotations` | The operator can create or reference a ServiceAccount. With `create: false` you must set `name` to an existing ServiceAccount in the namespace; the operator does not silently fall back to `default`. |
| `bootstrapScript` | `spec.bootstrapScript` or component `bootstrapScript` | The operator mounts `superset_bootstrap.sh` and sources it before default Python component and lifecycle task commands. Component and lifecycle values override the top-level value; set an override to `""` to disable inheritance. |
| `forceReload` on chart components | `spec.forceReload` | Operator `forceReload` is global. For component-only restarts, change a component pod annotation. |

### Secrets, Env Vars, and Config Files

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `secretEnv.create` | External Kubernetes Secret | The operator does not create a general-purpose env Secret from CR fields. |
| `envFromSecret`, `envFromSecrets` | `spec.podTemplate.container.envFrom` | Can also be set per component. |
| `extraEnv`, `extraEnvRaw` | `spec.podTemplate.container.env` | `env` entries merge by name between top-level and per-component templates. |
| `extraSecretEnv` | Kubernetes Secret plus `envFrom` or `env.valueFrom.secretKeyRef` | For operator-owned secrets, use first-class fields such as `secretKeyFrom`, `metastore.uriFrom`, and `valkey.passwordFrom`. |
| `configOverrides`, `configOverridesFiles` | `spec.config` or per-component `config` | Copy the Python snippets into operator config. Helm treats each `configOverrides` child key as an arbitrary label/comment, not as a typed setting. The operator appends `spec.config` after generated `SECRET_KEY`, metastore, Valkey, Celery, and SQLAlchemy config. |
| `configFromSecret` | `spec.config` plus Secret-backed env vars | The operator renders `superset_config.py` into ConfigMaps. Avoid putting secret values directly in Python config. |
| `extraConfigs` | External ConfigMap plus `podTemplate.volumes` and `volumeMounts` | If you used `import_datasources.yaml`, add a custom lifecycle init command to import it. |
| `extraSecrets` | External Secret plus `podTemplate.volumes` and `volumeMounts` | Mount additional files explicitly where your config expects them. |
| `extraVolumes`, `extraVolumeMounts` | `spec.podTemplate.volumes`, `spec.podTemplate.container.volumeMounts` | Can be shared top-level or overridden per component. |

### Database and Valkey/Redis

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `supersetNode.connections.db_*` | `spec.metastore` | Use `uriFrom` for an exact SQLAlchemy URI, or structured `host`, `database`, `username`, and `passwordFrom`. The Helm `db_pass` value is rendered into `DB_PASS` in the chart env Secret; for `passwordFrom`, reference whichever long-lived Secret/key will hold that database password after migration. |
| `supersetNode.connections.redis_*` | `spec.valkey` | `valkey` works with Redis-compatible services and renders cache, Celery broker/backend, and SQL Lab results backend config. `redis_user` maps to `username`. |
| `supersetNode.connections.redis_ssl` | `spec.valkey.ssl` | Set `ssl: {}` for Redis/Valkey TLS. Mount client certs or CA bundles with `podTemplate.volumes` if needed; when certificate paths are configured, translate Helm `ssl_cert_reqs` values from `CERT_NONE`/`CERT_OPTIONAL`/`CERT_REQUIRED` to `certRequired: none`/`optional`/`required`. |
| `postgresql.*` | Not managed by this operator | Provide an existing PostgreSQL endpoint and reference it from `spec.metastore`. |
| `redis.*` | Not managed by this operator | Provide an existing Redis/Valkey-compatible endpoint and reference it from `spec.valkey`. |

The Helm chart exposes one cache DB (`redis_cache_db`) and one Celery DB
(`redis_celery_db`). The operator gives each Superset cache role its own
default DB and key prefix. To preserve Helm-like sharing, set the relevant
`spec.valkey.*.database` fields to the same DB numbers you used in Helm.

`spec.valkey` renders managed connectivity for Celery (`broker_url`,
`result_backend`, and SSL settings), but it does not recreate Celery application
behavior such as imports, task annotations, routes, beat schedules, or scheduler
expiration. Carry those settings over explicitly in `spec.config` based on the
Superset version you deploy.

### Components

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `supersetNode.*` | `spec.webServer.*` | Web server Deployment, Service, config, probes, resources, HPA, and PDB. |
| `supersetNode.replicas.replicaCount` | `spec.webServer.replicas` | If `autoscaling` is set, the HPA manages replicas. |
| `supersetNode.command` | `spec.webServer.podTemplate.container.command` | The operator default is `/usr/bin/run-server.sh`. Gunicorn settings are structured under `webServer.gunicorn`. |
| `supersetWorker.*` | `spec.celeryWorker.*` | Worker Deployment and config. Celery process settings are structured under `celeryWorker.celery`. |
| `supersetWorker.replicas.replicaCount` | `spec.celeryWorker.replicas` | Top-level `spec.replicas` can provide a shared default. |
| `supersetCeleryBeat.enabled` | `spec.celeryBeat: {}` | Celery Beat is always a singleton. |
| `supersetCeleryFlower.enabled` | `spec.celeryFlower: {}` | Flower gets its own Deployment and Service. |
| `supersetCeleryFlower.service.*` | `spec.celeryFlower.service.*` | Supports service type, port, nodePort, labels, and annotations. |
| `supersetWebsockets.enabled` | `spec.websocketServer.image.{repository,tag}` | An image override is required (CEL-validated): the default Superset image does not include `websocket_server.js`. Use a community image such as `oneacrefund/superset-websocket` or your own. |
| `supersetWebsockets.config` | `spec.websocketServer.config` or `configFrom` | Inline `config` is Development-only. In Staging/Production, create a Secret with `config.json` and reference it with `configFrom`. |
| `init.enabled` | `spec.lifecycle.disabled` or task-level `disabled` | Lifecycle is enabled by default. Set `lifecycle.disabled: true` to skip all tasks. |
| `init.command`, `init.initscript` | `spec.lifecycle.migrate.command`, `spec.lifecycle.init.command` | The operator splits database migration and application initialization into separate tasks. |
| `init.createAdmin`, `init.adminUser`, `init.loadExamples` | `spec.lifecycle.init.adminUser`, `spec.lifecycle.init.loadExamples` | These are allowed only in `environment: Development`. In production, create users through your normal identity and admin process or a custom task command using Secret-backed env vars. |
| `init.jobAnnotations` | Not applicable | Lifecycle tasks are CRD-managed pods, not Helm hook Jobs. |

### Pod and Deployment Customization

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `resources` | `spec.podTemplate.container.resources` | Top-level resources apply to all components unless overridden. |
| Component `resources` | Component `podTemplate.container.resources` | For example, `spec.celeryWorker.podTemplate.container.resources`. |
| `nodeSelector`, `tolerations`, `affinity`, `hostAliases`, `topologySpreadConstraints`, `priorityClassName` | `spec.podTemplate.*` | Top-level values merge with per-component values according to the configuration guide. |
| Component `podAnnotations`, `podLabels` | Component `podTemplate.annotations`, `podTemplate.labels` | Operator-managed labels are protected and cannot be overridden. |
| Component `deploymentAnnotations`, `deploymentLabels` | Component `deploymentTemplate.annotations`, `deploymentTemplate.labels` | Set top-level or per component; field-level merged (component wins on key conflict). Operator-managed labels are applied last and cannot be overridden. |
| Component `strategy` | Component `deploymentTemplate.strategy` | Also supports `revisionHistoryLimit`, `minReadySeconds`, and `progressDeadlineSeconds`. |
| Component `startupProbe`, `livenessProbe`, `readinessProbe` | Component `podTemplate.container.startupProbe`, `livenessProbe`, `readinessProbe` | The operator provides defaults for served components. |
| Component `podSecurityContext`, `containerSecurityContext` | Component `podTemplate.podSecurityContext`, `podTemplate.container.securityContext` | Can also be set top-level. |
| Component `extraContainers` | Component `podTemplate.sidecars` | Sidecars merge by container name. |
| Component `initContainers` | Component `podTemplate.initContainers` | Init containers merge by container name. |
| Default `wait-for-postgres` / `wait-for-postgres-redis` initContainers | Component `podTemplate.initContainers` | The chart injects `dockerize` initContainers that wait for the metastore and Redis before each component starts. The operator does not inject these because the metastore lifecycle and `migrate` task gate component startup independently. If you rely on this behavior — for example, on environments where the database may briefly be unavailable — re-add the same `dockerize` init container under `spec.podTemplate.initContainers`. |

### Services and Networking

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `service.type`, `service.port`, `service.nodePort.http`, `service.annotations` | `spec.webServer.service.type`, `port`, `nodePort`, `annotations` | `loadBalancerIP` is not modeled because the Kubernetes field is deprecated; use provider annotations where possible. |
| `ingress.enabled`, `ingress.ingressClassName`, `ingress.annotations`, `ingress.hosts`, `ingress.tls`, `ingress.path`, `ingress.pathType` | `spec.networking.ingress` | A host with no explicit `paths` fans out into one rule per present component, mirroring Gateway API (see [Networking & Monitoring](networking-and-monitoring.md#gateway-api-recommended) for the routing table). Use `className` for `ingressClassName`, or keep legacy `kubernetes.io/ingress.class` under `annotations`. Helm's top-level `path`/`pathType` move to `hosts[].paths[]`; setting explicit paths routes them to the web server only. |
| `ingress.extraHostsRaw` | `spec.networking.ingress.hosts` for normal web routes, or a custom Ingress | Use a separate Ingress for non-web backends or unusual raw rules. |
| `supersetWebsockets.ingress.*` | `spec.networking.ingress` or `spec.networking.gateway` | Both reconcilers expose the websocket service automatically when `websocketServer` is present; set its path with `websocketServer.service.gatewayPath`. |
| Flower or MCP external paths | `spec.networking.ingress` or `spec.networking.gateway` | Both reconcilers expose every present component on its own subpath; see the routing table in [Networking & Monitoring](networking-and-monitoring.md#gateway-api-recommended). |

Both reconcilers expand a host with no explicit `paths` into one rule per
present component, each served under its own subpath (overridable via
`service.gatewayPath`). Requests are forwarded as-is, with no path rewriting, so
each component owns its subpath: the operator configures Flower for this
automatically (its `--url_prefix`), and the web server serves at the root. This
differs from the Helm chart, where the web server sat at `/` and the websocket
was a separate `/ws` Ingress rule. If a component must be reached at the root
instead of a subpath, give it its own host or a prefix-stripping rewrite on your
controller. See
[Networking & Monitoring](networking-and-monitoring.md#gateway-api-recommended)
for the routing table.

Helm's web Ingress shape:

```yaml
ingress:
  path: /
  pathType: Prefix
  hosts:
    - superset.example.com
  annotations:
    kubernetes.io/ingress.class: alb
    alb.ingress.kubernetes.io/target-type: ip
```

becomes:

```yaml
spec:
  networking:
    ingress:
      annotations:
        kubernetes.io/ingress.class: alb  # legacy class annotation
        alb.ingress.kubernetes.io/target-type: ip
      hosts:
        - host: superset.example.com
          paths:
            - path: /
              pathType: Prefix
```

For controllers that support `spec.ingressClassName`, use `className` instead:

```yaml
spec:
  networking:
    ingress:
      className: alb
```

### Scaling and Availability

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `supersetNode.autoscaling.*` | `spec.webServer.autoscaling` | Uses Kubernetes `autoscaling/v2` metrics, so CPU, memory, custom, and external metrics are supported. |
| `supersetWorker.autoscaling.*` | `spec.celeryWorker.autoscaling` | Same HPA model as web server. |
| Component `podDisruptionBudget.*` | Component `podDisruptionBudget` | Supported for web server, Celery worker, Flower, websocket server, and MCP server. |
| `supersetCeleryBeat.podDisruptionBudget.*` | No direct equivalent | Celery Beat is a singleton in the operator and currently has no PDB field. |

## Known Parity Gaps

The operator covers the chart's commonly used features. The items below have
no direct equivalent today; each lists the recommended workaround:

- **Celery Beat PDB.** The chart exposes `supersetCeleryBeat.podDisruptionBudget`.
  The operator pins Beat to a single replica and does not surface a PDB field;
  a PDB on a 1-replica workload is advisory only. If Beat downtime during
  voluntary disruptions is unacceptable, use `priorityClassName` and node
  affinity to influence scheduling instead.
- **Bundled PostgreSQL and Redis subcharts.** Unlike the Helm chart, the
  operator does not bundle support for managing PostgreSQL or Redis/Valkey
  resources. Helm-based deployments that used the bundled subcharts should move
  those dependencies to standalone Helm releases or equivalent separately
  managed services, then configure `spec.metastore` and `spec.valkey` to point
  at those endpoints.
- **`loadBalancerIP`.** Intentional — the Kubernetes field is deprecated. Use
  cloud-provider Service annotations to influence load-balancer placement.

## Example Translation

Helm values:

```yaml
image:
  tag: 5.0.0

supersetNode:
  replicas:
    replicaCount: 2
  connections:
    db_host: superset-db
    db_port: "5432"
    db_user: superset
    db_pass: superset
    db_name: superset
    redis_host: valkey
    redis_port: "6379"

supersetWorker:
  replicas:
    replicaCount: 2

supersetCeleryBeat:
  enabled: true

configOverrides:
  enable_alert_reports: |
    FEATURE_FLAGS = {"ALERT_REPORTS": True}
```

The `enable_alert_reports` key above is only a Helm override label. The chart
appends its value to `superset_config.py`; it does not define a special
`feature_flags` values field.

Create or choose a Secret that will remain after Helm is removed:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: superset-secrets
stringData:
  secret-key: "<copy the current Superset SECRET_KEY>"
  db-password: "<copy Helm DB_PASS or your external database password>"
```

Operator CR:

```yaml
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: superset
spec:
  image:
    tag: "5.0.0"
  secretKeyFrom:
    name: superset-secrets
    key: secret-key
  metastore:
    type: PostgreSQL
    host: superset-db
    port: 5432
    database: superset
    username: superset
    passwordFrom:
      name: superset-secrets
      key: db-password
  valkey:
    host: valkey
    port: 6379
  featureFlags:
    ALERT_REPORTS: true
  webServer:
    replicas: 2
  celeryWorker:
    replicas: 2
  celeryBeat: {}
  lifecycle:
    migrate: {}
    init: {}
```

## Importing Datasources

The Helm chart's default init script imports
`/app/configs/import_datasources.yaml` when that file exists. The operator does
not run this import automatically. To keep the behavior, mount the file and add
the import to the init command:

```yaml
spec:
  lifecycle:
    podTemplate:
      volumes:
        - name: datasource-import
          configMap:
            name: superset-datasources
      container:
        volumeMounts:
          - name: datasource-import
            mountPath: /app/configs
            readOnly: true
    migrate: {}
    init:
      command:
        - /bin/sh
        - -c
        - |
          superset init
          if [ -f /app/configs/import_datasources.yaml ]; then
            superset import_datasources -p /app/configs/import_datasources.yaml
          fi
```

## Websocket Config

In Development, Helm's `supersetWebsockets.config` map can be copied under
`spec.websocketServer.config`:

```yaml
spec:
  environment: Development
  websocketServer:
    image:
      repository: oneacrefund/superset-websocket
      tag: latest
    config:
      port: 8080
      logLevel: debug
      jwtSecret: CHANGE-ME
      jwtCookieName: async-token
      redis:
        host: redis.example.com
        port: 6379
        db: 0
```

In Staging and Production, put the JSON in a Secret and reference the key:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: superset-websocket-config
stringData:
  config.json: |
    {"port":8080,"jwtSecret":"...","jwtCookieName":"async-token"}
---
apiVersion: superset.apache.org/v1alpha1
kind: Superset
spec:
  webServer: {}
  websocketServer:
    image:
      repository: oneacrefund/superset-websocket
      tag: latest
    configFrom:
      name: superset-websocket-config
      key: config.json
```

The operator mounts this at `/home/superset-websocket/config.json`. When the
referenced Secret content changes, update `spec.forceReload` to roll the
websocket Deployment.

## Websocket Routing

The Helm chart's `supersetWebsockets.ingress` injects a websocket rule alongside
the web server. The operator does the same automatically: whenever
`websocketServer` is present, both the Ingress and Gateway API reconcilers add a
rule pointing at the `<superset-name>-websocket-server` Service. A bare Ingress
host is enough:

```yaml
spec:
  websocketServer:
    image:
      repository: oneacrefund/superset-websocket
      tag: latest
  networking:
    ingress:
      hosts:
        - host: superset.example.com   # fans out to every present component on its subpath
```

The equivalent with Gateway API, plus a customized websocket path via
`service.gatewayPath`:

```yaml
spec:
  websocketServer:
    image:
      repository: oneacrefund/superset-websocket
      tag: latest
    service:
      gatewayPath: /ws
  networking:
    gateway:
      gatewayRef:
        name: shared-gateway
        namespace: gateway-system
      hostnames:
        - superset.example.com
```

Adding explicit `hosts[].paths[]` to the Ingress turns off the per-component
fan-out for that host and routes the listed paths to the web server only.
