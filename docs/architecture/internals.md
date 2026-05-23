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
reconciliation lifecycle, parent-owned resource reconciliation, status reporting, and
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
3. **Lifecycle Tasks** — Run parent-owned task Jobs and update `status.lifecycle`
4. **Component Reconciliation** — Resolve shared spec (top-level + per-component) into flat runtime specs, create/update/delete parent-owned Kubernetes resources, reconcile networking/monitoring/network policies
5. **Status Aggregation** — Read workload state, update `status.components`, set conditions and phase

### Phase 1: Preflight

The controller fetches the `Superset` CR. If it no longer exists, the
reconciler returns gracefully — Kubernetes garbage collection handles cleanup
via owner references.

If `spec.suspend` is `true`, the controller sets the `Suspended` condition to
`True`, updates status, and returns immediately. No task Jobs run, no component
resources are created or updated, and no resources are deleted. This allows
users to pause reconciliation without removing the CR.

### Phase 2: Shared Resources

**ServiceAccount** — Created if `spec.serviceAccount.create` is true (the
default). Uses the name from `spec.serviceAccount.name` or falls back to the
parent CR name. Owned by the parent CR and garbage-collected on parent deletion.

### Phase 3: Lifecycle Tasks

The parent controller runs deterministic lifecycle task Jobs:
`{parentName}-clone`, `{parentName}-migrate`, `{parentName}-rotate`, and
`{parentName}-init`. Durable task state lives on the parent
`status.lifecycle` field, so a completed task is still visible after successful
Jobs and Pods are removed by retention policy.

Tasks run sequentially: clone → migrate → rotate → init. Each task can be independently
disabled via `disabled: true`. Clone also supports periodic re-execution via
`cronSchedule`. Checksums cascade downstream: a re-clone forces re-migrate,
which forces re-rotate, which forces re-init.

When a pending task requires drain (`requiresDrain: true`, the default for
clone, migrate, and rotate), the operator deletes component Deployments, HPAs,
PDBs, and routable Services before running that task, but only when at least one
configured component has desired replicas greater than zero. Tasks whose current
checksum is already complete do not contribute to the drain decision. The parent
verifies all component pods have terminated before proceeding to task execution.
This ensures no application pods access the metastore during schema changes. If
`maintenancePage` is configured, the parent brings up a maintenance Deployment
and switches the web-server Service selector before draining. After tasks
complete, Phase 4 recreates all components fresh.

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
2. If disabled, deletes the parent-owned resources for that component
3. If enabled:
    - Renders component-appropriate `superset_config.py` from the parent's
      `secretKey`/`secretKeyFrom`, `metastore`, `config`, and per-component
      `config` fields via `RenderConfig()`
    - Collects secret env vars: when `secretKeyFrom`, `metastore.uriFrom`, or
      `metastore.passwordFrom` are set, the operator produces env vars with
      `valueFrom.secretKeyRef` pointing at the referenced Secret. In dev mode,
      inline values produce plain `value` env vars instead.
      Always injects `SUPERSET_OPERATOR__INSTANCE_NAME` (the parent CR name) so
      raw `spec.config` Python can reference the instance — for example to
      compute instance-scoped Celery queue names that won't collide across
      Superset CRs sharing a broker.
    - Resolves the shared spec (top-level + per-component) into a
      flat `FlatComponentSpec` via `ResolveComponentSpec()`
    - Computes a config checksum from shared inputs and rendered config
    - Creates or updates the component ConfigMap, Deployment, Service, HPA, and PDB

After components, the controller reconciles cluster-scoped resources:
networking (Ingress or HTTPRoute), monitoring (ServiceMonitor), and network
policies (one NetworkPolicy per enabled component).

### Phase 5: Status Aggregation

The controller reads each enabled component's Deployment status, checks the
expected supporting resources, and aggregates the result into the parent status.
Each component reports a phase, ready count, replica details, image, config
checksum, and observed resource list.

During lifecycle and maintenance return, status still reflects observed
component workloads. If the maintenance page is active, only the web-server
component is treated as not ready because the web-server Service is still routed
to maintenance; non-web components such as Celery workers report their actual
Deployment readiness.

| All components ready | Phase | Available condition |
|---|---|---|
| Yes | `Running` | `True` |
| No | `Degraded` | `False` |

---

## Component Resource Pattern

The parent controller reconciles each component through table-driven component
descriptors and shared resource helpers. This keeps per-component variation
explicit while avoiding intermediate CRDs in the public API.

**Scalable components** (WebServer, CeleryWorker, CeleryFlower, WebsocketServer,
McpServer) manage a Deployment and support replicas, HPA, and PDB. Their specs
embed `ScalableComponentSpec`, which has `DeploymentTemplate`, `PodTemplate`,
and scaling fields.

**Singleton components** (lifecycle tasks and CeleryBeat) run exactly one instance.
Lifecycle tasks are Jobs with operator-managed retry logic (uses `PodTemplate` only).
CeleryBeat uses a Deployment but forces `replicas: 1` (has both
`DeploymentTemplate` and `PodTemplate` but no scaling fields).

All deployment components follow the same pattern: reconcile ConfigMap (if
applicable), reconcile Deployment, reconcile Service (if the component exposes
a port), reconcile scaling (HPA + PDB for scalable components), and project
status onto the parent. Lifecycle task reconciliation creates a ConfigMap when
needed and manages deterministic Jobs directly.

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

ConfigMaps are created and owned by the parent Superset controller. This means:

- ConfigMaps are written by the same controller that renders their content
- The parent is the single writer of config content
- Component Deployments mount ConfigMaps by conventional name

The parent computes a config checksum and stamps it directly on component
Deployment pod templates to trigger rolling restarts when config changes. This design follows the
principle that the checksum should be computed by whoever writes the data — since
the parent renders and writes the ConfigMap, it is the authority on when content
changed.

### What Each Component Creates

| Component | ConfigMap | Workload | Service | HPA | PDB |
|---|---|---|---|---|---|
| Clone (task) | — | Job (database tool) | — | — | — |
| Migrate (task) | superset_config.py | Job | — | — | — |
| Rotate (task) | superset_config.py | Job | — | — | — |
| Init (task) | superset_config.py | Job | — | — | — |
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

All deployment component reconcilers delegate to `buildDeploymentSpec()`, which constructs a
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

The operator sets reserved labels on parent-owned resources and NetworkPolicies
for resource discovery and cleanup.

### Operator-Managed Labels

| Label | Value | Purpose |
|---|---|---|
| `app.kubernetes.io/name` | `superset` | Application identity |
| `app.kubernetes.io/component` | Component type (e.g., `web-server`) | Component type filtering |
| `superset.apache.org/parent` | Parent Superset CR name | Parent-scoped discovery |

These labels are set by the operator on every reconciliation and **cannot be
overridden** — operator-managed labels are applied last, taking precedence over
any existing values.

Component resources use the standard `app.kubernetes.io/*` labels with
`app.kubernetes.io/instance` set to the component instance name, currently the
parent `Superset` name, for selector matching.

### Orphan Cleanup

When a component is removed from the parent spec, the parent reconciler invokes
the component descriptor's cleanup path, which deletes the component's
parent-owned resources (Deployment, Service, ConfigMap, HPA, PDB) by their
deterministic `{parent}-{component}` names. The parent CR's owner references
also cascade-delete the same resources if the CR itself is deleted.

Operator-managed labels remain available on parent-owned resources for human
discovery — e.g. `kubectl get deploy,svc,cm,hpa,pdb -l
app.kubernetes.io/instance=<parent>` lists every parent-owned workload.

---

## Checksum-Driven Rollouts

Config changes must trigger pod restarts for the new config to take effect.
The operator achieves this through **checksum annotations** on the pod template.

### How It Works

1. Parent controller computes checksums while rendering component config
2. The checksum is stamped on the Deployment pod template annotation
3. When a checksum changes, the pod template changes, and Kubernetes triggers a
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
`Superset` CR owns component Deployments, Services, ConfigMaps, HPAs, PDBs,
lifecycle task Jobs, the web-server Service, networking resources,
ServiceMonitor, and NetworkPolicies. Deleting the parent cascades to all owned
resources. Removing a component from the parent spec (e.g. deleting
`spec.celeryWorker`) deletes that component's resources.

---

## Maintenance Page (Parent-Owned Service Selector Switch)

When `spec.lifecycle.maintenancePage` is set, the operator serves a maintenance
page during drain and lifecycle tasks. The page is only started when a task
that will run requires drain, at least one configured component has desired
replicas greater than zero, and an existing web-server workload is present.
This section documents the design decision behind the traffic switchover
mechanism.

### Problem

During drain, component Deployments and Pods are removed. Without intervention,
users experience connection errors instead of a friendly maintenance message.

### Solution: Parent-Owned Web-Server Service

The parent controller owns the web-server Service directly. During lifecycle
drain, the parent:

1. Creates a maintenance Deployment (parent-owned) running a lightweight HTTP
   server (nginx:alpine by default or a user-provided image).
2. Switches the web-server Service's selector to match the maintenance-page pod
   labels, instantly routing traffic to maintenance pods.
3. Drains all component workloads, but the Service is unaffected because it
   belongs to the parent.
4. Runs lifecycle tasks (clone → migrate → rotate → init).
5. After tasks complete and the web-server Deployment is recreated, waits for the
   web-server Deployment to become ready.
6. Switches the Service selector back to the web-server pod labels.
7. Deletes the maintenance Deployment and its ConfigMap.

### Why Parent-Owned Service

- Service selector changes propagate in ~1 second via the endpoints controller,
  giving instant traffic switchover regardless of ingress implementation
- Works for all access patterns: Ingress, Gateway API, direct Service
- No orphan deletion complexity — the Service is always owned by the parent, so
  drain never affects it
- The same parent controller owns both Service selector switches and component
  rollout state

> **Note for developers using `kubectl port-forward`:** port-forward establishes a
> tunnel to a specific pod, not through the Service selector. When that pod is
> deleted during drain, the tunnel breaks with a "lost connection to pod" error.
> This does not affect Ingress/Gateway users — they route through EndpointSlices
> and are unaffected. Restart port-forward to reconnect to the
> maintenance pod.

### Alternatives Considered

**Separate component owner + selector patch**: Preserved the Service while
deleting the web-server workload owner, then patched the selector. Rejected
because splitting ownership made drain ordering and re-adoption fragile.

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
  ready: "7/7"
  components:
    webServer:
      phase: Ready
      ready: "2/2"
      ref: Deployment/example-web-server
      image: apache/superset:latest
      replicas: 2
      readyReplicas: 2
      resources:
        - kind: Deployment
          name: example-web-server
          status: Present
        - kind: Service
          name: example-web-server
          status: Present
        - kind: ConfigMap
          name: example-web-server-config
          status: Present
    celeryWorker:
      phase: Ready
      ready: "4/4"
    celeryBeat:
      phase: Ready
      ready: "1/1"
  lifecycle:
    phase: Complete
    migrate:
      state: Complete
      ref: Job/example-migrate
      desiredChecksum: sha256:...
      completedChecksum: sha256:...
  conditions:
    - type: Available
      status: "True"
      reason: AllComponentsReady
    - type: LifecycleComplete
      status: "True"
      reason: LifecycleComplete
    - type: Suspended
      status: "False"
```

### Parent Phase

The top-level `status.phase` reflects the overall instance state:

| Phase | Meaning |
|---|---|
| `Initializing` | First deployment — lifecycle tasks running for the first time |
| `Upgrading` | Image change detected — lifecycle tasks running against new version |
| `Running` | All enabled components are ready and lifecycle is complete |
| `Degraded` | One or more components are not fully ready |
| `Suspended` | `spec.suspend: true` — all reconciliation paused |
| `Blocked` | Downgrade detected — lifecycle tasks will not run (manual intervention required) |
| `AwaitingApproval` | Supervised upgrade mode — waiting for approval annotation before proceeding |

Drain progress is reported on `status.lifecycle.phase=Draining`; it does not
replace the top-level parent phase.

After lifecycle tasks complete, `status.lifecycle.phase=Restoring` covers the
component rollout and maintenance switchback window. The lifecycle phase becomes
`Complete` only after enabled components report ready.

### Component Status

Each enabled component reports status under `status.components`:

```yaml
status:
  components:
    webServer:
      phase: Progressing
      ready: "1/3"
      replicas: 3
      readyReplicas: 1
      updatedReplicas: 3
      availableReplicas: 1
      message: "1 of 3 replicas are ready"
```

**Component phase states:**

| Phase | Meaning |
|---|---|
| `Pending` | Expected Deployment has not been observed yet |
| `Progressing` | Deployment exists and some rollout progress is visible |
| `Ready` | Desired replicas are ready, updated, and available |
| `Unavailable` | Deployment exists but no ready replicas are available or rollout has exceeded progress deadline |
| `Drained` | Component reconciliation is paused while lifecycle tasks run and the workload has been removed |

---

## Error Handling Summary

| Scenario | Behavior |
|---|---|
| Superset CR deleted during reconcile | Graceful return (not found) |
| Init pod fails | Retry with backoff up to maxRetries, then permanent failure |
| Init pod times out | Counts as failed attempt, same retry logic |
| Resource creation fails | Error propagated, reconcile retried by controller-runtime |
| Optional CRD missing (Gateway API, ServiceMonitor) | Log and continue — feature disabled gracefully |
| Referenced Secret values change | Pods see new values only after restart; update `forceReload` to trigger rollout |
| Component removed from spec | Parent-owned resources for that component are deleted |
| Suspend enabled | All reconciliation paused, no resources created or deleted |
