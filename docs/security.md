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
Superset Kubernetes Operator. It covers the threat model, trust boundaries,
RBAC justification, and vulnerability reporting process.

## Trust Boundaries

The operator operates across three trust levels:

| Role | What they can do | Trust level |
|---|---|---|
| **Cluster admin** | Installs the operator, manages its RBAC and namespace | Full trust |
| **Namespace admin** | Creates and modifies `Superset` CRs in their namespace | Trusted — can deploy arbitrary workloads |
| **Superset end-user** | Accesses the Superset web UI and API | Untrusted — no operator interaction |

**Key assumption:** Granting `create` or `update` on any Superset CRD — parent
or child — is equivalent to granting the ability to create Pods, Deployments,
Services (including NodePort and LoadBalancer if cluster policy allows),
ConfigMaps, ServiceAccounts, Ingresses, HTTPRoutes, NetworkPolicies,
HorizontalPodAutoscalers, PodDisruptionBudgets, and ServiceMonitors in that
namespace, including choosing container images, commands, arguments, environment
variables, volumes, and ServiceAccount references. This is inherent to the
Kubernetes operator pattern and is not a vulnerability.

A `Superset` CR controls all of the above plus arbitrary Python configuration
via `spec.config`. The same applies to child CRDs (`SupersetWebServer`,
`SupersetCeleryWorker`, etc.) which can be created directly and carry the same
fields. Restrict access to all Superset CRDs using Kubernetes RBAC.

## Security Model

### Prod vs Dev Mode

The operator enforces a strict separation between production and development
modes:

- **Prod mode** (default): Inline `secretKey`, `metastore.uri`,
  `metastore.password`, and `valkey.password` are rejected by CRD CEL validation
  rules. Users reference secrets via `secretKeyFrom`, `metastore.uriFrom`,
  `metastore.passwordFrom`, or `valkey.passwordFrom` (which the operator wires
  as `valueFrom.secretKeyRef` env vars).
- **Dev mode** (`environment: dev`): Inline secrets are allowed for local
  development convenience. Additionally, `init.adminUser` and
  `init.loadExamples` are permitted — these create a default admin account and
  load sample data during initialization. Admin credentials from `adminUser`
  are stored as plain-text environment variables on the parent Superset CR, the
  child SupersetInit CR, and the resulting init Pod spec (visible to anyone with
  read access to these resources in the namespace). The admin password also
  appears in the Pod's process arguments via shell expansion.

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

**Init pod caveat:** When an init pod fails, the operator records a truncated
version of the container's termination message in the SupersetInit CR status and
Kubernetes Events for debugging. If the init command writes sensitive data to
its termination message (e.g., a database connection error that includes
credentials), a truncated form may appear in status. This is bounded to 256
characters and only applies to the init container's own output, not to
operator-managed secret references.

**Scope of this guarantee:** The above applies to operator-managed secret
references (`secretKeyFrom`, `metastore.uriFrom`, `metastore.passwordFrom`,
`valkey.passwordFrom`). User-authored fields — raw Python in `spec.config`,
component-level `config`, `podTemplate.container.env`, and directly-created
child CRs — are trusted input and may contain arbitrary values including
secrets. Users with read access to Superset CRs (parent or child) or the
generated ConfigMaps will see any values placed in these fields. These are out
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
- **`FlatComponentSpec` is shared across all child CRDs including Init.** The
  Init controller uses bare Pods (no Deployment), so fields like Autoscaling,
  PDB, and Replicas are unused. The parent controller nils these fields before
  writing the Init child CR. A dedicated `FlatInitSpec` may be introduced in a
  future API version, but the shared struct avoids duplicating Image,
  PodTemplate, and ServiceAccountName today.
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
  resources at deterministic names derived from the Superset or child CR name.
  Kubernetes controller-owner semantics prevent adopting resources already
  controlled by another controller. Unowned resources with the same managed
  name, or resources carrying the operator's cleanup labels, may be adopted or
  deleted during reconciliation. This is within the trust model: users who can
  create or update Superset parent or child CRs are trusted namespace operators
  and already have the effective ability to manage the corresponding workloads
  and resources.

## RBAC Justification

The operator runs with a `ClusterRole` to support managing `Superset` instances
across namespaces. Each permission is justified below:

| Resource | Verbs | Reason |
|---|---|---|
| `configmaps` | CRUD | Stores generated `superset_config.py` per component |
| `services` | CRUD | Exposes web server, Flower, websocket, MCP server |
| `serviceaccounts` | CRUD | Creates per-instance ServiceAccount for pod identity |
| `pods` | create, delete, get, list, watch | Manages bare init pods (not Deployments) for `SupersetInit` |
| `events` | create, patch | Records reconciliation events |
| `deployments` | CRUD | Manages component Deployments |
| `horizontalpodautoscalers` | CRUD | Manages HPA for scalable components |
| `poddisruptionbudgets` | CRUD | Manages PDBs for availability |
| `ingresses`, `networkpolicies` | CRUD | Optional networking features |
| `httproutes` | CRUD | Optional Gateway API support |
| `servicemonitors` | CRUD | Optional Prometheus integration |
| `tokenreviews`, `subjectaccessreviews` | create | Metrics endpoint auth/authz (controller-runtime secure metrics) |
| Superset parent CRD | get, list, watch + status | Reads the parent `Superset` CR and updates its status |
| Superset child CRDs + status | CRUD | Creates, updates, and deletes child CRs; updates child status |

The operator does **not** request:

- `*` (wildcard) on any resource or verb
- `impersonate` or RBAC management permissions
- `cluster-admin` or equivalent
- Kubernetes Secret read or write permissions

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
  local development; prod mode is the enforced default. `init.adminUser` and
  `init.loadExamples` are also dev-mode-only features, rejected by CRD
  validation in prod mode
- **CR creators can deploy arbitrary workloads** — creating or updating any
  Superset CRD is equivalent to creating Pods with chosen images, commands,
  env vars, volumes, and ServiceAccounts; this is inherent to the operator
  pattern and is the expected trust model (see [Trust Boundaries](#trust-boundaries))
- **Arbitrary Python via `spec.config`** — this field accepts raw Python by
  design; CR creators can already deploy arbitrary containers, so Python
  configuration does not expand the attack surface
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
