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
reconciliation lifecycle, child controller pattern, status reporting, and
resource cleanup. For the structural overview (CRD hierarchy, configuration
model, config rendering), see [Architecture](overview.md). For the full lifecycle
task reference (pod state machine, retry semantics, upgrade modes), see
[Lifecycle](../user-guide/lifecycle.md).

---

## Reconciliation Lifecycle

When a `Superset` CR is created or updated, the parent controller runs through
five sequential phases:

1. **Preflight** — Fetch the Superset CR, check the suspend flag
2. **Shared Resources** — ServiceAccount
3. **Lifecycle Tasks** — Create/update SupersetLifecycleTask child CRs (gates everything below)
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

The parent controller creates `SupersetLifecycleTask` child CRs:
`{parentName}-clone`, `{parentName}-migrate`, and `{parentName}-init`. The parent
uses a Get+Create/Delete pattern (never CreateOrUpdate) to avoid races with the
task controller's status writes. When a task needs to re-run (checksum mismatch),
the parent deletes the old CR and creates a fresh one on the next reconcile.

Tasks run sequentially: clone → migrate → init. Each task can be independently
disabled via `disabled: true`. Clone also supports periodic re-execution via
`cronSchedule`. Checksums cascade downstream: a re-clone forces re-migrate,
which forces re-init.

When a task requires drain (`requiresDrain: true`, the default for clone and
migrate), the operator deletes all component child CRs before running that task.
The parent verifies all component pods have terminated (not just Deployments
deleted) before proceeding to task execution. This ensures no application pods
access the metastore during schema changes. If `maintenancePage` is configured,
the parent brings up a maintenance Deployment and switches the web-server Service
selector before draining. After tasks complete, Phase 4 recreates all components
fresh.

Components do not deploy until all enabled lifecycle tasks complete (or lifecycle is
explicitly disabled via `spec.lifecycle.disabled: true`). If a task is in
progress or has failed, `Reconcile()` returns early with a requeue, skipping
Phase 4.

For the full lifecycle reference including pod state machine, retry/backoff
semantics, upgrade modes, and drain verification, see
[Lifecycle](../user-guide/lifecycle.md).

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

## Child Controller Pattern

Each child CRD (SupersetLifecycleTask, SupersetWebServer, SupersetCeleryWorker, etc.)
has its own controller that reconciles the Kubernetes resources for that
component.

**Scalable components** (WebServer, CeleryWorker, CeleryFlower, WebsocketServer,
McpServer) manage a Deployment and support replicas, HPA, and PDB. Their specs
embed `ScalableComponentSpec`, which has `DeploymentTemplate`, `PodTemplate`,
and scaling fields.

**Singleton components** (SupersetLifecycleTask, CeleryBeat) run exactly one instance.
SupersetLifecycleTask manages bare Pods with retry logic (uses `PodTemplate` only).
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

### Ownership and Checksum Flow

ConfigMaps are created and owned by the parent Superset controller (not by
child CRs). This means:

- ConfigMaps survive child CR deletion (e.g., during drain)
- The parent is the single writer of config content
- Child controllers mount ConfigMaps by conventional name without managing them

The parent computes a `ConfigChecksum` and passes it to child CRs via
`spec.configChecksum`. Child controllers stamp this as a pod template annotation
to trigger rolling restarts when config changes. This design follows the
principle that the checksum should be computed by whoever writes the data — since
the parent renders and writes the ConfigMap, it is the authority on when content
changed. Passing the checksum to child CRs avoids requiring child controllers to
watch or read ConfigMaps they don't own.

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

The operator sets reserved labels on child CRs (SupersetLifecycleTask, SupersetWebServer,
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

## Garbage Collection

The operator uses Kubernetes owner references for automatic cleanup. The parent
`Superset` CR owns child CRDs (SupersetLifecycleTask, SupersetWebServer, etc.),
the web-server Service, networking resources, ServiceMonitor, and NetworkPolicies.
Each child CR owns its managed resources — deployment CRDs own their Deployment,
ConfigMap, Service (except web-server, which is parent-owned), HPA, and PDB; the
SupersetLifecycleTask CRDs own their ConfigMap and Pods.
Deleting the parent cascades to all child CRs, which cascade to all their
owned resources. Removing a component from the parent spec (e.g. deleting
`spec.celeryWorker`) deletes its child CR, cascading to all owned resources.

---

## Maintenance Page (Parent-Owned Service Selector Switch)

When `spec.lifecycle.maintenancePage` is set, the operator serves a maintenance
page during drain and lifecycle tasks. This section documents the design decision
behind the traffic switchover mechanism.

### Problem

During drain, component child CRs are deleted. GC cascades this to Deployments
and Pods. Without intervention, users experience connection errors instead of a
friendly maintenance message.

### Solution: Parent-Owned Web-Server Service

The parent controller owns the web-server Service directly (not the child CR).
During lifecycle drain, the parent:

1. Creates a maintenance Deployment (parent-owned) running a lightweight HTTP
   server (nginx:alpine by default or a user-provided image).
2. Switches the web-server Service's selector to match the maintenance-page pod
   labels, instantly routing traffic to maintenance pods.
3. Drains all component child CRs (GC cascades to Deployments and Pods, but the
   Service is unaffected because it belongs to the parent).
4. Runs lifecycle tasks (clone → migrate → init).
5. After tasks complete and the web-server child CR is recreated, waits for the
   web-server Deployment to become ready.
6. Switches the Service selector back to the web-server pod labels.
7. Deletes the maintenance Deployment and its ConfigMap.

### Why Parent-Owned Service

- Service selector changes propagate in ~1 second via the endpoints controller,
  giving instant traffic switchover regardless of ingress implementation
- Works for all access patterns: Ingress, Gateway API, direct Service
- No orphan deletion complexity — the Service is always owned by the parent,
  so GC of child CRs never affects it
- The child `SupersetWebServer` reconciler skips Service management (the parent
  handles it), keeping the child controller simple

> **Note for developers using `kubectl port-forward`:** port-forward establishes a
> tunnel to a specific pod, not through the Service selector. When that pod is
> deleted during drain, the tunnel breaks with a "lost connection to pod" error.
> This does not affect Ingress/Gateway users — they route through EndpointSlices
> and see seamless transitions. Restart port-forward to reconnect to the
> maintenance pod.

### Alternatives Considered

**Orphan deletion + selector patch** (previous design): Used `propagationPolicy:
Orphan` when deleting the SupersetWebServer child CR to preserve the Service,
then patched the selector. Rejected because orphan lifecycle was fragile — race
conditions between GC finalization and reconciliation, plus the child had to
detect and re-adopt the orphaned Service on recreation.

**Separate maintenance Service + Ingress/HTTPRoute backend swap**: Architecturally
pure (clean separation, no interaction with web-server resources), but rejected
because Ingress/HTTPRoute propagation latency varies significantly by controller
implementation — from ~1s (Envoy-based) to 1-3 minutes (cloud load balancers like
GCP/AWS). This creates an unacceptable error window where users hit the draining
backend. Also doesn't work for users without networking configured.

---

## Status and Conditions

### Parent Status

The parent `Superset` CR reports aggregate status:

```yaml
status:
  phase: Running
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

### Parent Phase

The top-level `status.phase` reflects the overall instance state:

| Phase | Meaning |
|---|---|
| `Initializing` | First deployment — lifecycle tasks running for the first time |
| `Upgrading` | Image change detected — lifecycle tasks running against new version |
| `Draining` | Drain strategy active — components being removed before running tasks |
| `Running` | All enabled components are ready and lifecycle is complete |
| `Degraded` | One or more components are not fully ready |
| `Suspended` | `spec.suspend: true` — all reconciliation paused |
| `Blocked` | Downgrade detected — lifecycle tasks will not run (manual intervention required) |
| `AwaitingApproval` | Supervised upgrade mode — waiting for approval annotation before proceeding |

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
