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
- Gateway API is the preferred way to route multiple Superset services. The
  built-in Ingress reconciler targets the web server service only.

## Migration Workflow

1. Export the Helm values used by the existing release:

   ```bash
   helm get values <release> -n <namespace> -o yaml > superset-values.yaml
   ```

2. Identify external dependencies. If the Helm release currently installed
   Bitnami PostgreSQL or Redis, provision replacement services before removing
   the chart. Do not delete the Helm release until database persistence and
   backups are understood.

3. Create Kubernetes Secrets for the Superset `SECRET_KEY`, database
   connection string or password, and Valkey/Redis password. Prefer a full
   SQLAlchemy URI in `metastore.uriFrom` when migrating from Helm because it
   preserves the exact connection string.

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
     websocketServer: {}
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
| `image.repository`, `image.tag`, `image.pullPolicy` | `spec.image.repository`, `spec.image.tag`, `spec.image.pullPolicy` | Per-component image overrides are available under each component's `image` field. |
| `imagePullSecrets` | `spec.image.pullSecrets` | Applied to all component pods. |
| `runAsUser` | `spec.podTemplate.podSecurityContext.runAsUser` | Prefer a full pod or container security context for production hardening. |
| `serviceAccountName`, `serviceAccount.create`, `serviceAccount.annotations` | `spec.serviceAccount.name`, `spec.serviceAccount.create`, `spec.serviceAccount.annotations` | The operator can create or reference a ServiceAccount. |
| `bootstrapScript` | Custom image, init container, or command override | The operator does not generate a bootstrap script. Build dependencies into the image when possible. |
| `forceReload` on chart components | `spec.forceReload` | Operator `forceReload` is global. For component-only restarts, change a component pod annotation. |

### Secrets, Env Vars, and Config Files

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `secretEnv.create` | External Kubernetes Secret | The operator does not create a general-purpose env Secret from CR fields. |
| `envFromSecret`, `envFromSecrets` | `spec.podTemplate.container.envFrom` | Can also be set per component. |
| `extraEnv`, `extraEnvRaw` | `spec.podTemplate.container.env` | `env` entries merge by name between top-level and per-component templates. |
| `extraSecretEnv` | Kubernetes Secret plus `envFrom` or `env.valueFrom.secretKeyRef` | For operator-owned secrets, use first-class fields such as `secretKeyFrom`, `metastore.uriFrom`, and `valkey.passwordFrom`. |
| `configOverrides`, `configOverridesFiles` | `spec.config` or per-component `config` | The operator appends this Python after generated `SECRET_KEY`, metastore, Valkey, Celery, and SQLAlchemy config. |
| `configFromSecret` | `spec.config` plus Secret-backed env vars | The operator renders `superset_config.py` into ConfigMaps. Avoid putting secret values directly in Python config. |
| `extraConfigs` | External ConfigMap plus `podTemplate.volumes` and `volumeMounts` | If you used `import_datasources.yaml`, add a custom lifecycle init command to import it. |
| `extraSecrets` | External Secret plus `podTemplate.volumes` and `volumeMounts` | Mount additional files explicitly where your config expects them. |
| `extraVolumes`, `extraVolumeMounts` | `spec.podTemplate.volumes`, `spec.podTemplate.container.volumeMounts` | Can be shared top-level or overridden per component. |

### Database and Valkey/Redis

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `supersetNode.connections.db_*` | `spec.metastore` | Use `uriFrom` for an exact SQLAlchemy URI, or structured `host`, `database`, `username`, and `passwordFrom`. |
| `supersetNode.connections.redis_*` | `spec.valkey` | `valkey` works with Redis-compatible services and renders cache, Celery broker/backend, and SQL Lab results backend config. |
| `supersetNode.connections.redis_ssl` | `spec.valkey.ssl` | Mount client certs or CA bundles with `podTemplate.volumes` if needed. |
| `postgresql.*` | Not managed by this operator | Use a managed database, CloudNativePG, another PostgreSQL operator, or keep an existing PostgreSQL instance. |
| `redis.*` | Not managed by this operator | Use a managed Redis/Valkey service, Redis/Valkey operator, or separate Helm chart. |

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
| `supersetWebsockets.enabled` | `spec.websocketServer: {}` | Use a websocket image such as `oneacrefund/superset-websocket`, or your own image. |
| `supersetWebsockets.config` | Env vars or mounted config file | The operator does not render websocket `config.json`. Mount one with `podTemplate.volumes`, or configure the websocket server through env vars. |
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
| Component `deploymentAnnotations`, `deploymentLabels` | No direct equivalent | Prefer pod labels/annotations, service labels/annotations, or a cluster policy that mutates Deployments if you need Deployment metadata. |
| Component `strategy` | Component `deploymentTemplate.strategy` | Also supports `revisionHistoryLimit`, `minReadySeconds`, and `progressDeadlineSeconds`. |
| Component `startupProbe`, `livenessProbe`, `readinessProbe` | Component `podTemplate.container.startupProbe`, `livenessProbe`, `readinessProbe` | The operator provides defaults for served components. |
| Component `podSecurityContext`, `containerSecurityContext` | Component `podTemplate.podSecurityContext`, `podTemplate.container.securityContext` | Can also be set top-level. |
| Component `extraContainers` | Component `podTemplate.sidecars` | Sidecars merge by container name. |
| Component `initContainers` | Component `podTemplate.initContainers` | Init containers merge by container name. |

### Services and Networking

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `service.type`, `service.port`, `service.nodePort.http`, `service.annotations` | `spec.webServer.service.type`, `port`, `nodePort`, `annotations` | `loadBalancerIP` is not currently modeled; use provider annotations where possible. |
| `ingress.enabled`, `ingress.ingressClassName`, `ingress.annotations`, `ingress.hosts`, `ingress.tls`, `ingress.path`, `ingress.pathType` | `spec.networking.ingress` | Operator-managed Ingress targets the web server. |
| `ingress.extraHostsRaw` | `spec.networking.ingress.hosts` for normal web routes, or a custom Ingress | Use a separate Ingress for non-web backends or unusual raw rules. |
| `supersetWebsockets.ingress.*` | `spec.networking.gateway` or a custom Ingress | The operator's Gateway API integration routes `/ws` to the websocket service. Built-in Ingress does not route websocket paths. |
| Flower or MCP external paths | `spec.networking.gateway` | Gateway API can route `/flower` and `/mcp` to their services. |

### Scaling and Availability

| Helm chart value | Operator equivalent | Notes |
|---|---|---|
| `supersetNode.autoscaling.*` | `spec.webServer.autoscaling` | Uses Kubernetes `autoscaling/v2` metrics, so CPU, memory, custom, and external metrics are supported. |
| `supersetWorker.autoscaling.*` | `spec.celeryWorker.autoscaling` | Same HPA model as web server. |
| Component `podDisruptionBudget.*` | Component `podDisruptionBudget` | Supported for web server, Celery worker, Flower, websocket server, and MCP server. |
| `supersetCeleryBeat.podDisruptionBudget.*` | No direct equivalent | Celery Beat is a singleton in the operator and currently has no PDB field. |

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
  feature_flags: |
    FEATURE_FLAGS = {"ALERT_REPORTS": True}
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
      name: superset-db
      key: password
  valkey:
    host: valkey
    port: 6379
  config: |
    FEATURE_FLAGS = {"ALERT_REPORTS": True}
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

## Websocket Routing

If you used the Helm chart's `supersetWebsockets.ingress` path, prefer Gateway
API with the operator:

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

If your cluster only supports Ingress, create a separate Ingress for the
websocket service (`<superset-name>-websocket-server`) or route websocket
traffic outside the operator-managed Ingress.
