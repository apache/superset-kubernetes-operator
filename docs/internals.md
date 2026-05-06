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

# Internals — Reconciliation & Runtime

This document describes how the operator behaves at runtime: the
reconciliation lifecycle, task pod management, retry semantics, status
reporting, and resource cleanup. For the structural overview (CRD hierarchy,
configuration model, config rendering), see [Architecture](architecture.md).

---

## Reconciliation Lifecycle

When a `Superset` CR is created or updated, the parent controller runs through
five sequential phases:

1. **Preflight** — Fetch the Superset CR, check the suspend flag
2. **Shared Resources** — ServiceAccount
3. **Lifecycle Tasks** — Create/update SupersetTask child CRs (gates everything below)
4. **Component Reconciliation** — Resolve shared spec (top-level + per-component) into flat child specs, create/update/delete child CRs, reconcile networking/monitoring/network policies
5. **Status Aggregation** — Read child CR statuses, set conditions and phase

### Phase 1: Preflight

The controller fetches the `Superset` CR. If it no longer exists, the
reconciler returns gracefully — Kubernetes garbage collection handles cleanup
via owner references.

If `spec.suspend` is `true`, the controller sets the `Suspended` condition to
`True`, updates status, and returns immediately. No task pods run, no child CRs
are created or updated, and no resources are deleted. This allows users to
pause reconciliation without removing the CR.

### Phase 2: Shared Resources

**ServiceAccount** — Created if `spec.serviceAccount.create` is true (the
default). Uses the name from `spec.serviceAccount.name` or falls back to the
parent CR name. Owned by the parent CR and garbage-collected on parent deletion.

### Phase 3: Lifecycle Tasks

The parent controller creates or updates two `SupersetTask` child CRs:
`{parentName}-migrate` and `{parentName}-init`. The dedicated
`SupersetTaskReconciler` manages the Pod lifecycle for each task (ConfigMap
creation, bare Pod creation, retry with backoff, timeout, retention). See
[Init Pod Lifecycle](#init-pod-lifecycle) below for the full state machine.

Tasks run sequentially: migrate must complete before init starts. The task
strategy (default: `VersionChange`) determines whether tasks are triggered —
with the default strategy, tasks only run when the Superset image changes.

When `upgradeStrategy: Drain` is set, the operator deletes all component child
CRs before running tasks. This cascades to Deployments, Services, and HPAs via
garbage collection, ensuring no application pods access the metastore during
schema changes. After tasks complete, Phase 4 recreates all components fresh.

Components do not deploy until both lifecycle tasks complete (or lifecycle is
explicitly disabled via `spec.lifecycle.disabled: true`). If a task is in
progress or has failed, `Reconcile()` returns early with a requeue, skipping
Phase 4.

### Phase 4: Component Reconciliation

For each of the six deployment components, the parent controller:

1. Checks if the component is enabled (field present in spec)
2. If disabled, deletes the child CR (cascade-deletes all owned resources)
3. If enabled:
    - Renders component-appropriate `superset_config.py` from the parent's
      `secretKey`/`secretKeyFrom`, `metastore`, `config`, and per-component
      `config` fields via `RenderConfig()`
    - Collects secret env vars: when `secretKeyFrom`, `metastore.uriFrom`, or
      `metastore.passwordFrom` are set, the operator produces env vars with
      `valueFrom.secretKeyRef` pointing at the referenced Secret. In dev mode,
      inline values produce plain `value` env vars instead.
    - Resolves the shared spec (top-level + per-component) into a
      flat `FlatComponentSpec` via `ResolveChildSpec()`
    - Computes a config checksum from shared inputs and rendered config
    - Creates or updates the child CR with the fully-flattened spec

After components, the controller reconciles cluster-scoped resources:
networking (Ingress or HTTPRoute), monitoring (ServiceMonitor), and network
policies (one NetworkPolicy per enabled component).

### Phase 5: Status Aggregation

The controller reads each child CR's status via unstructured GET (using the
correct GVK per component type), extracts the `ready` field (format:
`"readyReplicas/desiredReplicas"`), and aggregates into the parent status.

| All components ready | Phase | Available condition |
|---|---|---|
| Yes | `Running` | `True` |
| No | `Degraded` | `False` |

---

## Init Pod Lifecycle

The parent controller creates `SupersetTask` child CRs, and the dedicated
`SupersetTaskReconciler` manages bare Pods (`restartPolicy: Never`). The task
controller acts as the retry controller, giving it full control over backoff,
timeout, naming, and cleanup.

### Default Commands

The lifecycle is split into two sequential tasks:

- **migrate** — `superset db upgrade` (database schema migration)
- **init** — `superset init` (application initialization: roles, permissions, app state)

Each task's command is independently customizable via `spec.lifecycle.migrate.command`
and `spec.lifecycle.init.command`:

```yaml
spec:
  lifecycle:
    migrate:
      command: ["/bin/sh", "-c", "superset db upgrade && custom-migrate"]
    init:
      command: ["/bin/sh", "-c", "superset init && custom-seed"]
```

### Task Strategies

Each task has a `strategy` field controlling when it runs:

| Strategy | Behavior |
|---|---|
| `VersionChange` (default) | Runs only when the Superset image tag changes |
| `Always` | Runs on any spec change (image, config, or command) |
| `Never` | Never runs (task effectively disabled) |

With `VersionChange`, config-only changes trigger rolling restarts via checksum
annotations but do not spawn task pods.

### Image-Change Detection

The operator tracks the last successfully deployed image. When an image change
is detected:

- In **Automatic** upgrade mode (default), tasks run immediately
- In **Supervised** upgrade mode, tasks wait for annotation-based approval

### Downgrade Blocking

The operator performs semver comparison on image tags. If the new tag is lower
than the currently deployed version, the reconciler blocks the change and sets
an error condition to prevent accidental database downgrades.

### Why Bare Pods

- **Controlled retries** — The operator decides when and how to retry, with
  configurable max attempts and exponential backoff
- **Clean audit trail** — Each attempt creates a new Pod with a unique
  `generateName` suffix, making it easy to inspect history
- **Sidecar handling** — The operator manages pod lifecycle directly, avoiding
  the Job controller's sidecar termination issues

### Gating

If the lifecycle tasks have not completed successfully, the
reconciler returns early and no child CRs are created or updated. Set
`spec.lifecycle.disabled: true` to skip lifecycle tasks entirely.

### Pod State Machine

Task pods transition through these states:

- **Pending** — No pod exists yet. The operator creates one.
- **Running** — Pod is executing. If it exceeds the timeout, it counts as a failed attempt.
- **Succeeded** → **Complete** — Task is done; the next task (or components) can proceed.
- **Failed** — If `attempts < maxRetries`, the operator deletes the pod and requeues with exponential backoff. If `attempts >= maxRetries`, the task is permanently failed.

### Retry and Backoff

| Setting | Default | Description |
|---|---|---|
| `spec.lifecycle.maxRetries` | `3` | Maximum attempts before permanent failure |
| `spec.lifecycle.timeout` | `300s` | Maximum time per attempt |

**Backoff calculation:**

Exponential backoff: `10s * 2^(attempt-1)`, capped at 300s (10s, 20s, 40s, 80s, 160s, 300s).

If a pod stays in Running or Pending state beyond the timeout, it counts as a
failed attempt.

### Pod Naming and Discovery

Pods use `generateName` (`{parent}-{task}-{random}`, e.g. `my-superset-migrate-x7k2m`)
for unique names per attempt. The operator discovers pods by label
(`superset.apache.org/instance` and `superset.apache.org/task`) and uses
the most recently created one when multiple exist.

### Pod Retention

After a task completes (successfully or permanently fails), the retention
policy determines what happens to the pod:

| Policy | On Success | On Failure |
|---|---|---|
| `Delete` (default) | Delete pod | Delete pod |
| `Retain` | Keep pod | Keep pod |
| `RetainOnFailure` | Delete pod | Keep pod |

Configured via `spec.lifecycle.podRetention.policy`. Retaining failed pods is useful
for debugging migration failures.

### Task Pod Spec

Task pods inherit scheduling, security, volumes, and env from the top-level
`podTemplate`, just like other components. Key fields:

- **Image**: From `spec.image`
- **Command**: From `spec.lifecycle.migrate.command` or `spec.lifecycle.init.command` (defaults: `superset db upgrade` and `superset init`)
- **Config**: Mounted from the task ConfigMap (`{parent}-{task}-config`)
- **Env vars**: Database credentials, secret key (via plain env vars in dev mode, or `valueFrom.secretKeyRef` when `*From` fields are used)
- **Resources**: From `spec.lifecycle.podTemplate.container.resources` if set
- **Service account**: Inherited from parent spec
- **Restart policy**: Always `Never` — the operator handles retries

---

## Child Controller Pattern

Each child CRD (SupersetTask, SupersetWebServer, SupersetCeleryWorker, etc.)
has its own controller that reconciles the Kubernetes resources for that
component.

**Scalable components** (WebServer, CeleryWorker, CeleryFlower, WebsocketServer,
McpServer) manage a Deployment and support replicas, HPA, and PDB. Their specs
embed `ScalableComponentSpec`, which has `DeploymentTemplate`, `PodTemplate`,
and scaling fields.

**Singleton components** (SupersetTask, CeleryBeat) run exactly one instance.
SupersetTask manages bare Pods with retry logic (uses `PodTemplate` only).
CeleryBeat manages a Deployment but forces `replicas: 1` (has both
`DeploymentTemplate` and `PodTemplate` but no scaling fields).

All deployment controllers follow the same pattern: reconcile ConfigMap (if
applicable), reconcile Deployment, reconcile Service (if the component exposes
a port), reconcile scaling (HPA + PDB for scalable components), and update
status. The task controller reconciles a ConfigMap and manages bare Pods.

### Why ConfigMaps

Superset imports `superset_config` as a standard Python module, which means the
config must exist as a `.py` file on the filesystem. A ConfigMap volume mount is
the standard Kubernetes mechanism for projecting files into containers:

- **Python import requirement** — `superset_config.py` must be a real file on
  disk; environment variables and downward API projections cannot serve as
  importable Python modules
- **Operability** — `kubectl get cm` shows exactly what config each component is
  running, making debugging straightforward
- **Clean pod manifests** — Without ConfigMaps, the rendered Python config
  would need to be inlined on the pod spec (as annotations or env vars),
  making Deployment manifests difficult to read. ConfigMaps keep pod specs
  focused on container configuration

The rendered config is already stored on the child CR's `spec.config` field, so
the ConfigMap is technically a derived resource. The child controller creates it
from the spec and mounts it at `/app/superset/config/`.

### What Each Component Creates

| Component | ConfigMap | Workload | Service | HPA | PDB |
|---|---|---|---|---|---|
| Migrate (task) | superset_config.py | bare Pod | — | — | — |
| Init (task) | superset_config.py | bare Pod | — | — | — |
| WebServer | superset_config.py | Deployment (gunicorn) | port 8088 | if set | if set |
| CeleryWorker | superset_config.py | Deployment (celery worker) | — | if set | if set |
| CeleryBeat | superset_config.py | Deployment (celery beat) | — | — | — |
| CeleryFlower | superset_config.py | Deployment (celery flower) | port 5555 | if set | if set |
| WebsocketServer | — | Deployment (node.js) | port 8080 | if set | if set |
| McpServer | superset_config.py | Deployment (fastmcp) | port 8088 | if set | if set |

**CeleryBeat** is a singleton — the controller forces `replicas: 1` regardless
of the spec, and does not create an HPA or PDB.

**WebsocketServer** is Node.js-based and does not get a `superset_config.py`
ConfigMap.

### Deployment Builder

All child controllers delegate to `buildDeploymentSpec()`, which constructs a
complete Deployment spec from the flat `FlatComponentSpec` and a
component-specific `DeploymentConfig`:

```go
type DeploymentConfig struct {
    ContainerName  string                 // e.g., "superset-web-server"
    DefaultCommand []string               // e.g., ["/usr/bin/run-server.sh"]
    DefaultArgs    []string               // optional
    DefaultPorts   []corev1.ContainerPort // e.g., [{Name: "http", Port: 8088}]
    ForceReplicas  *int32                 // non-nil only for beat (=1)
}
```

**Replicas resolution order:**

1. `ForceReplicas` (beat singleton) — always wins
2. `nil` if HPA is configured — HPA manages scaling
3. `spec.Replicas` otherwise

### Idempotent Reconciliation

All resource creation uses `controllerutil.CreateOrUpdate()`: creates the
resource if it doesn't exist, updates it if the spec has drifted. This makes
every reconciliation cycle safe to re-run.

---

## Labels and Annotations

The operator sets reserved labels on child CRs (SupersetTask, SupersetWebServer,
etc.) and NetworkPolicies for resource discovery and orphan cleanup.

### Operator-Managed Labels

| Label | Value | Purpose |
|---|---|---|
| `app.kubernetes.io/name` | `superset` | Application identity |
| `app.kubernetes.io/component` | Component type (e.g., `web-server`) | Component type filtering |
| `superset.apache.org/parent` | Parent Superset CR name | Parent-scoped discovery |

These labels are set by the operator on every reconciliation and **cannot be
overridden** — operator-managed labels are applied last, taking precedence over
any existing values.

Sub-resources (Deployments, Services, ConfigMaps) created by child controllers
use the standard `app.kubernetes.io/*` labels with `app.kubernetes.io/instance`
set to the child CR name for selector matching.

### Orphan Cleanup

When a component is disabled, the operator uses label-based discovery to find
and delete orphaned child CRs. On each reconcile, it lists all child CRs
matching the parent and component type labels, then deletes any whose name does
not match the currently desired name. Deleting a child CR cascades to all its
owned sub-resources via owner references.

---

## Checksum-Driven Rollouts

Config changes must trigger pod restarts for the new config to take effect.
The operator achieves this through **checksum annotations** on the pod template.

### How It Works

1. Parent controller computes checksums when building child CRs
2. Checksums are stored on the child CR spec
3. Child controller stamps them as pod template annotations
4. When a checksum changes, the pod template changes, and Kubernetes triggers a
   rolling restart

### Checksum Types

| Annotation | Source | Scope |
|---|---|---|
| `superset.apache.org/config-checksum` | Rendered superset_config.py | Per-component |

**Per-component isolation:** Changing a component's `config` only
changes that component's config checksum -- only its pods restart. Other
components are unaffected.

**Secret safety:** In prod mode, operator-managed secret values (`secretKeyFrom`,
`metastore.uriFrom`, `metastore.passwordFrom`, `valkey.passwordFrom`) are never
read by the operator and therefore never appear in checksums, annotations, or
ConfigMaps. In dev mode, inline secret values (`secretKey`, `metastore.password`,
`valkey.password`) influence the shared config checksum (as a hash, not
plaintext) because changes to these values must trigger a rollout.

---

## Networking

The operator supports two mutually exclusive networking modes for external
access to the web server.

### Gateway API (HTTPRoute)

When `spec.networking.gateway` is set, the controller creates an `HTTPRoute`
with path-based routing:

| Priority | Path | Target | Condition |
|---|---|---|---|
| 1 (most specific) | `/ws` | websocket-server Service | websocketServer enabled |
| 2 | `/mcp` | mcp-server Service | mcpServer enabled |
| 3 | `/flower` | celery-flower Service | celeryFlower enabled |
| 4 (catch-all) | `/` | web-server Service | webServer enabled |

More specific paths are listed first to ensure correct routing priority.
Paths are configurable via `service.gatewayPath` on each component spec.

### Ingress

When `spec.networking.ingress` is set, the controller creates a standard
`networkingv1.Ingress`. Supports multiple hosts with per-host path rules.
All paths route to the web-server Service.

### Graceful CRD Handling

Gateway API is not included in Kubernetes and must be
[installed separately](https://gateway-api.sigs.k8s.io/guides/#installing-gateway-api).
If the CRDs are not present, the controller skips HTTPRoute watch registration
and catches `meta.IsNoMatchError` at reconciliation time. The operator runs
with reduced functionality rather than failing.

---

## Monitoring

When `spec.monitoring.serviceMonitor` is set, the controller creates a
Prometheus `ServiceMonitor` targeting the web-server component.

- Uses **unstructured objects** because the ServiceMonitor CRD is external
  (monitoring.coreos.com/v1)
- Default scrape interval: 30s (configurable)
- Targets pods with `app.kubernetes.io/component: web-server`
- If the ServiceMonitor CRD is not installed, the controller logs an info
  message and continues — monitoring is optional

---

## Network Policies

When `spec.networkPolicy` is set, the controller creates one `NetworkPolicy`
per enabled component:

| Component | Ingress from Superset pods | Ingress from external | Egress |
|---|---|---|---|
| WebServer | port 8088 | port 8088 | all |
| CeleryWorker | any port | — | all |
| CeleryBeat | any port | — | all |
| CeleryFlower | port 5555 | port 5555 | all |
| WebsocketServer | port 8080 | port 8080 | all |
| McpServer | port 8088 | port 8088 | all |

**Base rules:**

- All components allow ingress from pods belonging to the same Superset instance
  (matched by `app.kubernetes.io/name: superset` + `superset.apache.org/parent` labels)
- Components with external ports (web server, flower, websocket, mcp) also
  allow ingress on that port from any source (enables load balancers and
  ingress controllers)
- All components allow unrestricted egress (they need access to databases,
  caches, and external APIs)

**User-defined rules** can be added via `spec.networkPolicy.extraIngress` and
`spec.networkPolicy.extraEgress`.

---

## Garbage Collection

The operator uses Kubernetes owner references for automatic cleanup. The parent
`Superset` CR owns child CRDs (SupersetTask, SupersetWebServer, etc.),
networking resources, ServiceMonitor, and NetworkPolicies. Each child CR owns
its managed resources — deployment CRDs own their Deployment, ConfigMap,
Service, HPA, and PDB; the SupersetTask CRDs own their ConfigMap and Pods.
Deleting the parent cascades to all child CRs, which cascade to all their
owned resources. Removing a component from the parent spec (e.g. deleting
`spec.celeryWorker`) deletes its child CR, cascading to all owned resources.

---

## Status and Conditions

### Parent Status

The parent `Superset` CR reports aggregate status:

```yaml
status:
  phase: Running          # Initializing | Running | Degraded | Suspended
  observedGeneration: 3
  version: "latest"
  components:
    webServer:
      ready: "2/2"
    celeryWorker:
      ready: "4/4"
    celeryBeat:
      ready: "1/1"
  conditions:
    - type: Available
      status: "True"
      reason: AllComponentsReady
    - type: InitComplete
      status: "True"
      reason: InitComplete
    - type: Suspended
      status: "False"
```

### Child Status

Each child CR reports its own status:

```yaml
status:
  ready: "2/3"
  observedGeneration: 5
  conditions:
    - type: Ready
      status: "False"
      reason: PartiallyReady
      message: "2 of 3 replicas ready"
    - type: Progressing
      status: "True"
      reason: RolloutInProgress
```

**Ready condition states:**

| State | Meaning |
|---|---|
| `True` / `AllReplicasReady` | readyReplicas >= desiredReplicas and > 0 |
| `False` / `PartiallyReady` | Some replicas ready, not all |
| `False` / `NotReady` | Zero replicas ready |

**Progressing condition states:**

| State | Meaning |
|---|---|
| `True` / `RolloutInProgress` | Deployment is rolling out new pods |
| `False` / `RolloutComplete` | New ReplicaSet is fully available |

### Init Status

Lifecycle task progress is tracked per-task:

```yaml
status:
  lifecycle:
    migrate:
      state: Complete       # Pending | Running | Complete | Failed
      podName: my-superset-migrate-x7k2m
      startedAt: "2026-03-16T10:00:00Z"
      completedAt: "2026-03-16T10:00:12Z"
      duration: "12s"
      attempts: 1
      image: apache/superset:latest
    init:
      state: Complete
      podName: my-superset-init-a9b3k
      startedAt: "2026-03-16T10:00:13Z"
      completedAt: "2026-03-16T10:00:22Z"
      duration: "9s"
      attempts: 1
      image: apache/superset:latest
```

---

## Error Handling Summary

| Scenario | Behavior |
|---|---|
| Superset CR deleted during reconcile | Graceful return (not found) |
| Init pod fails | Retry with backoff up to maxRetries, then permanent failure |
| Init pod times out | Counts as failed attempt, same retry logic |
| Child CR creation fails | Error propagated, reconcile retried by controller-runtime |
| Optional CRD missing (Gateway API, ServiceMonitor) | Log and continue — feature disabled gracefully |
| Referenced Secret values change | Pods see new values only after restart; update `forceReload` to trigger rollout |
| Component removed from spec | Child CR deleted, cascade cleans up all resources |
| Suspend enabled | All reconciliation paused, no resources created or deleted |