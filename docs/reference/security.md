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

# Security

This document defines the security boundaries, assumptions, and scope for the
Superset Kubernetes Operator. It covers trust boundaries, secret handling,
RBAC justification, and vulnerability reporting.

## Trust Boundaries

The operator operates across three trust levels:

| Role | What they can do | Trust level |
|---|---|---|
| **Cluster admin** | Installs the operator, manages its RBAC and namespace | Full trust |
| **Namespace admin** | Creates and modifies `Superset` CRs in their namespace | Trusted — can deploy arbitrary workloads |
| **Superset end-user** | Accesses the Superset web UI and API | Untrusted — no operator interaction |

**Key assumption:** Granting `create` or `update` on the `Superset` CRD is
equivalent to granting the ability to create Pods, Deployments,
Services (including NodePort and LoadBalancer if cluster policy allows),
ConfigMaps, ServiceAccounts, Ingresses, HTTPRoutes, NetworkPolicies,
HorizontalPodAutoscalers, PodDisruptionBudgets, and ServiceMonitors in that
namespace, including choosing container images, commands, arguments, environment
variables, volumes, and ServiceAccount references. This is inherent to the
Kubernetes operator pattern and is not a vulnerability.

A `Superset` CR controls all of the above plus arbitrary Python configuration
via `spec.config`. Restrict access to the `supersets` resource using Kubernetes
RBAC.

### Single Public CRD

The operator exposes one public custom resource, `Superset`. Component
Deployments, Services, ConfigMaps, HPAs, PDBs, lifecycle task Pods, networking,
monitoring, and NetworkPolicies are reconciled as parent-owned Kubernetes
resources. The bundled `superset-editor-role`, `superset-admin-role`, and
`superset-viewer-role` therefore only need permissions for the `supersets`
resource and its status subresource.

### ServiceAccount Selection Is Part of CR Write Access

CR authors can set `serviceAccount.name` with `serviceAccount.create: false`
to bind workloads to any existing ServiceAccount in the namespace — including
ServiceAccounts linked to cloud IAM or workload-identity setups (IRSA, GKE
Workload Identity, Azure AD). This is intentional and enables legitimate
integration patterns, but it means a CR author inherits whatever permissions
the selected ServiceAccount has. Cluster and namespace admins should treat
"can create Superset CRs" as equivalent to "can run workloads under any
ServiceAccount in this namespace" and restrict ServiceAccount distribution
accordingly.

## Security Model

### Prod vs Dev Mode

The operator enforces a strict separation between production and development
modes:

- **Prod mode** (default): Inline `secretKey`, `metastore.uri`,
  `metastore.password`, and `valkey.password` are rejected by CRD CEL validation
  rules. Users reference secrets via `secretKeyFrom`, `metastore.uriFrom`,
  `metastore.passwordFrom`, or `valkey.passwordFrom` (which the operator wires
  as `valueFrom.secretKeyRef` env vars).
- **Dev mode** (`environment: Development`): Inline secrets are allowed for local
  development convenience. Additionally, `lifecycle.init.adminUser` and
  `lifecycle.init.loadExamples` are permitted — these create a default admin account and
  load sample data during initialization. Admin credentials from `adminUser`
  are stored as plain-text environment variables on the parent Superset CR and
  the resulting task Pod spec (visible to anyone with read access to these
  resources in the namespace). The admin password also appears in the Pod's
  process arguments via shell expansion.

Dev mode is intentionally less secure. It exists for local development with Kind
or Minikube where secret management infrastructure is not available. This is not
a vulnerability — it is a documented design decision.

### Secret Handling

In prod mode, secrets follow this path:

1. User creates a Kubernetes `Secret` containing the secret key and database
   credentials
2. User references the Secret via `secretKeyFrom`, `metastore.uriFrom`,
   `metastore.passwordFrom`, or `valkey.passwordFrom` on the Superset CR. The
   operator injects the corresponding env vars with `valueFrom.secretKeyRef` —
   the actual secret value is resolved by the kubelet at pod startup, not by
   the operator.
3. The operator generates `superset_config.py` that renders
   `SECRET_KEY = os.environ['SUPERSET_OPERATOR__SECRET_KEY']` — the actual
   secret value is resolved at Python runtime from the env var, so it never
   appears in the ConfigMap
4. The operator does not need Kubernetes Secret read permissions for this flow
   and never reads, logs, writes, or stores secret values in ConfigMaps, CRD
   status fields, or Events

**Task pod caveat:** When a task pod fails, the operator records a truncated
version of the container's termination message in the parent Superset status and
Kubernetes Events for debugging. If the task command writes sensitive data to
its termination message (e.g., a database connection error that includes
credentials), a truncated form may appear in status. This is bounded to 256
characters and only applies to the task container's own output, not to
operator-managed secret references.

**Scope of this guarantee:** The above applies to operator-managed secret
references (`secretKeyFrom`, `metastore.uriFrom`, `metastore.passwordFrom`,
`valkey.passwordFrom`). User-authored fields — raw Python in `spec.config`,
component-level `config`, and `podTemplate.container.env` — are trusted input
and may contain arbitrary values including secrets. Users with read access to
Superset CRs or the generated ConfigMaps will see any values placed in these
fields. These are out
of scope for the operator's secret handling guarantees
(see [What Is Generally Out of Scope](#what-is-generally-out-of-scope)).

### Raw Python Configuration

The `spec.config` field accepts arbitrary Python code that is appended to the
generated `superset_config.py`. This is by design — Superset's configuration
system is Python-based and requires arbitrary Python for features like custom
security managers, database drivers, and feature flags.

Since CR creators can already deploy arbitrary containers (via `image`,
`command`, `args`), the ability to inject Python does not expand the attack
surface. Restrict who can create `Superset` CRs using Kubernetes RBAC.

### CRD Validation

All validation is enforced via [CEL](https://kubernetes.io/docs/reference/using-api/cel/)
rules embedded in the CRD schema (`x-kubernetes-validations`). No admission
webhooks are used. This avoids the operational complexity of cert-manager and
ensures validation is always active regardless of how the operator is deployed.

Key rules:

- **Prod-mode secret rejection:** Inline `secretKey`, `metastore.uri`,
  `metastore.password`, and `valkey.password` are rejected in prod mode
- **Mutual exclusivity:** `secretKey`/`secretKeyFrom`, metastore URI vs
  structured fields, gateway vs ingress
- **Networking requires webServer:** Routes target the web server service
- **Monitoring requires webServer:** ServiceMonitor scrapes the web server service
- **Defaulting:** `environment` defaults to `prod`, image repository and pull
  policy default via kubebuilder markers

## Design Decisions

The following design choices are intentional and documented here to avoid
repeat review cycles:

- **Ingress `CreateOrUpdate` replaces the full spec.** The `reconcileIngress`
  mutate function assigns `ingress.Spec = networkingv1.IngressSpec{...}` before
  building rules, so rules are rebuilt from scratch on every reconcile — they do
  not accumulate.
- **`setCondition` ignores message-only changes.** Condition updates are
  triggered by Status, Reason, or ObservedGeneration changes. In all current
  call sites, Status and Reason change together, so message-only changes are a
  no-op by design. `LastTransitionTime` is only updated when Status changes
  (per Kubernetes API conventions), not on Reason or generation changes alone.
- **`FlatComponentSpec` is shared across component Deployments and lifecycle tasks.**
  Lifecycle tasks use bare Pods (no Deployment), so fields like Autoscaling,
  PDB, and Replicas are unused. The parent controller nils these fields before
  creating task Pods. A dedicated `FlatInitSpec` may be introduced in a future
  API version, but the shared struct avoids duplicating Image, PodTemplate, and
  ServiceAccountName today.
- **`computeChecksum` has an unreachable fallback.** The `fmt.Sprintf("%v")`
  fallback after `json.Marshal` cannot fire for CRD types (which always marshal
  successfully). It exists as a defensive guard, not as an expected code path.
- **WebServer and McpServer share port 8088.** These are separate Pods and
  Services, so identical port numbers do not conflict.
- **Generated Python uses operator-controlled values.** String fields
  interpolated into `superset_config.py` (key prefixes, SSL cert paths) come
  from CRD fields whose values are set by CR authors — trusted actors who can
  already deploy arbitrary containers. Raw Python in `spec.config` is appended
  verbatim by design and is out of scope (see "What Is Generally Out of Scope"
  below).
- **Celery Flower uses shell expansion for `--url_prefix`.** The Flower default
  command uses `/bin/sh -c` to expand `$SUPERSET_OPERATOR__FLOWER_URL_PREFIX`,
  which is set from `service.gatewayPath`. The `gatewayPath` field is restricted
  to `^/[a-zA-Z0-9/_.-]+$` by CRD validation, preventing shell metacharacter
  injection. Additionally, CR creators can already override the command entirely
  via `podTemplate.container.command`, so this does not expand the attack
  surface.
- **ServiceAccount ownership.** When `serviceAccount.create` is true (the
  default), the operator creates and owns the ServiceAccount. If a
  ServiceAccount with the specified name already exists and is not owned by the
  Superset CR, the operator refuses to adopt it and reports an error. Users who
  want to reference a pre-existing ServiceAccount should set `create: false`.
- **Managed resource adoption and cleanup.** The operator reconciles managed
  resources at deterministic names derived from the Superset name.
  Kubernetes controller-owner semantics prevent adopting resources already
  controlled by another controller. Unowned resources with the same managed
  name, or resources carrying the operator's cleanup labels, may be adopted or
  deleted during reconciliation. This is within the trust model: users who can
  create or update Superset CRs are trusted namespace operators and already have
  the effective ability to manage the corresponding workloads and resources.
- **NetworkPolicy provides baseline ingress segmentation, not egress
  restriction.** When the built-in NetworkPolicy is enabled, the operator
  installs policies that isolate ingress between Superset instances and allow
  external clients to reach user-facing components (web, websocket, flower,
  MCP). Egress is intentionally unrestricted so workloads can reach the
  metastore database, Valkey, SMTP servers, object stores, and any other
  user-configured dependencies. Users who require strict egress isolation
  should disable the built-in policy and author their own.
- **Metrics endpoint ships with a permissive TLS default.** The bundled
  ServiceMonitor defaults to `insecureSkipVerify: true` against the manager's
  self-signed serving certificate so that Prometheus can scrape metrics
  out-of-the-box on clusters without cert-manager. Authentication and
  authorization are still enforced via bearer tokens validated by
  `TokenReview`/`SubjectAccessReview` (see [RBAC Justification](#rbac-justification)),
  so the endpoint is not anonymously accessible. Production deployments
  should switch to cert-manager-issued certificates and set
  `insecureSkipVerify: false` — `charts/superset-operator/values.yaml`
  documents the flip.

## RBAC Justification

The operator runs with a `ClusterRole` to support managing `Superset` instances
across namespaces. Each permission is justified below:

| Resource | Verbs | Reason |
|---|---|---|
| `configmaps` | CRUD | Stores generated `superset_config.py` per component |
| `services` | CRUD | Exposes web server, Flower, websocket, MCP server |
| `serviceaccounts` | CRUD | Creates per-instance ServiceAccount for pod identity |
| `pods` | create, delete, get, list, watch | Manages bare lifecycle task pods (not Deployments) |
| `events` | create, patch | Records reconciliation events |
| `deployments` | CRUD | Manages component Deployments |
| `horizontalpodautoscalers` | CRUD | Manages HPA for scalable components |
| `poddisruptionbudgets` | CRUD | Manages PDBs for availability |
| `ingresses`, `networkpolicies` | CRUD | Optional networking features |
| `httproutes` | CRUD | Optional Gateway API support |
| `servicemonitors` | CRUD | Optional Prometheus integration |
| `tokenreviews`, `subjectaccessreviews` | create | Metrics endpoint auth/authz (controller-runtime secure metrics) |
| `supersets` + `supersets/status` | get, list, watch, update, patch + status update | Reads `Superset` CRs and updates status |

The operator does **not** request:

- `*` (wildcard) on any resource or verb
- `impersonate` or RBAC management permissions
- `cluster-admin` or equivalent
- Kubernetes Secret read or write permissions

### Install Scope

The operator supports two install modes, selectable at deploy time:

- **Cluster-scoped (default).** The manager ServiceAccount is bound to a
  `ClusterRole` via a `ClusterRoleBinding`, and the cache watches every
  namespace. Appropriate when a cluster admin administers the operator
  centrally. Helm: `watch.scope: cluster`.
- **Namespace-scoped.** The manager watches only the namespaces listed in
  `WATCH_NAMESPACE` (comma-separated). Appropriate for restricted clusters
  that forbid `ClusterRole` creation, or for single-tenant installs that
  want a tighter blast radius.

The RBAC shape differs between Helm and Kustomize for namespace-scoped
installs:

- **Helm (`watch.scope: namespaces`)** renders one `Role` and one
  `RoleBinding` per watched namespace, and does **not** create a manager
  `ClusterRole`/`ClusterRoleBinding` at all. With CRDs preinstalled by a
  cluster admin and `metrics.enabled: false`, this install succeeds on
  clusters that deny cluster-scoped RBAC to the installer (see the
  Constraints list below).
- **Kustomize (`config/components/watch-namespace/`)** retains the
  controller-gen–generated `ClusterRole` but replaces the
  `ClusterRoleBinding` with a namespaced `RoleBinding` pointing at that
  same `ClusterRole`. A `RoleBinding` → `ClusterRole` pairing restricts
  the granted permissions to the binding's namespace. The Kustomize path
  therefore still requires cluster-scoped RBAC *at install time* for the
  `ClusterRole`; its runtime footprint is namespace-scoped.

Constraints common to both paths:

- **CRD installation always needs cluster-admin.** CRDs are cluster-scoped
  resources; watch-scope does not change that.
- **Secure metrics auth still needs cluster-scoped RBAC.** The metrics
  endpoint uses `TokenReview`/`SubjectAccessReview`, which are cluster-level
  APIs. On clusters that forbid `ClusterRole` entirely, disable metrics
  (`metrics.enabled: false` in Helm values).
- **Changing the watched-namespace list requires a manager restart.** The
  manager cache is built at startup; dynamic reconfiguration is not
  supported.
- **Superset CRs in unwatched namespaces are silently ignored.** The
  operator logs the watched set at startup but does not detect stray CRs
  elsewhere — confirm by tailing the startup log or listing `Supersets`
  across namespaces manually if users report missing reconciliation.

## What Is In Scope

The following are valid security concerns for this project:

- Secrets leaking into ConfigMaps, Events, logs, or CRD status in **prod mode**
- Privilege escalation via the operator's RBAC permissions
- CRD validation bypass (e.g., crafting a CR that evades CEL rules)
- The operator container's own security posture (it runs as non-root,
  read-only, all capabilities dropped)
- Supply chain issues in the operator's build and release pipeline
- Vulnerabilities in the operator binary itself

**Note on workload security contexts:** The operator does not enforce default
security contexts on Superset workload pods — it propagates whatever the user
configures via `podTemplate.podSecurityContext` and
`podTemplate.container.securityContext`. The
[production sample](https://github.com/apache/superset-kubernetes-operator/blob/main/config/samples/superset_v1alpha1_superset_prod.yaml)
shows recommended settings. Workload pod security is the user's responsibility
(see [What Is Generally Out of Scope](#what-is-generally-out-of-scope)).

## What Is Generally Out of Scope

The following areas are usually outside this project's security scope. Reports
are still welcome when they show that the operator changes the expected trust
boundary, weakens Kubernetes controls, leaks operator-managed secrets, or makes
one of these conditions materially worse:

- **Superset application vulnerabilities** — report these to the
  [Apache Superset project](https://github.com/apache/superset/security/policy)
- **Database or cache security** — the operator does not manage PostgreSQL or
  Valkey instances; their security is the user's responsibility
- **Kubernetes control plane vulnerabilities** — report these to the
  [Kubernetes security team](https://kubernetes.io/docs/reference/issues-security/security/)
- **Dev mode allows inline secrets** — this is intentional and documented for
  local development; prod mode is the enforced default. `lifecycle.init.adminUser` and
  `lifecycle.init.loadExamples` are also dev-mode-only features, rejected by CRD
  validation in prod mode
- **CR creators can deploy arbitrary workloads** — creating or updating any
  Superset CRD is equivalent to creating Pods with chosen images, commands,
  env vars, volumes, and ServiceAccounts; this is inherent to the operator
  pattern and is the expected trust model (see [Trust Boundaries](#trust-boundaries))
- **Arbitrary Python via `spec.config`** — this field accepts raw Python by
  design; CR creators can already deploy arbitrary containers, so Python
  configuration does not expand the attack surface
- **Lifecycle clone task command is trusted input** — the `lifecycle.clone`
  task runs whatever image and command the CR author configures, so shell
  and SQL content embedded in that command is trusted input. CR authors
  already deploy arbitrary containers, so the clone task does not expand the
  attack surface. Review clone commands as part of CR review, not as a
  separable vulnerability class.
- **Container image vulnerabilities** — the operator does not control the
  contents of the Superset container image
- **Workload pod security contexts** — the operator propagates user-configured
  security contexts but does not enforce defaults; workload pod hardening is
  the user's responsibility (see the production sample for recommended settings)
- **Network-level attacks** (MITM, DNS spoofing) — these are infrastructure
  concerns outside the operator's control
- **Missing features** (e.g., "should support Vault integration") — these are
  normally handled as feature requests unless the missing behavior creates a
  concrete security regression in the operator

## Reporting Vulnerabilities

The Apache Superset Kubernetes Operator project follows the
[Apache Software Foundation vulnerability handling process](https://apache.org/security/).

To report a security vulnerability, please email **security@apache.org**.

Please do **not** file a public GitHub issue for security vulnerabilities.

### Supported Versions

| Version | Supported |
|---|---|
| v1alpha1 (latest) | Yes |

### Component Scope

This policy covers the Superset Kubernetes Operator and its components:

- CRD definitions and CEL validation rules
- Controller reconciliation logic
- RBAC and resource management
- Helm chart and deployment manifests
