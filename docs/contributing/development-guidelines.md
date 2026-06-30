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

# Development Guidelines

This guide covers the patterns, conventions, and testing philosophy that govern
contributions to the operator. For environment setup, see
[Development Setup](development-setup.md).

## Architecture

See [Architecture](../architecture/overview.md) for the structural overview (CRD
hierarchy, configuration model, config rendering) and
[Internals](../architecture/internals.md) for runtime behavior (reconciliation
lifecycle, parent-owned resource reconciliation, status reporting). Key points:

- **Single public CRD**: `Superset` resolves shared spec (top-level + per-component) into parent-owned Kubernetes resources
- **6 deployment components + lifecycle tasks**: web server, Celery worker, Celery beat, Flower, websocket, MCP, and task Jobs for seed/migrate/rotate/init
- **3 pure Go packages**: `internal/resolution/` (spec flattening), `internal/config/` (Python rendering), `internal/common/` (shared types)
- **Parent resolves and executes**: All layering, lifecycle orchestration, resource reconciliation, and status projection live in the parent controller

---

## Testing Philosophy

### Guiding principle: assert on observable outputs

For a Kubernetes operator, the "user" is the person writing a CR and
`kubectl apply`-ing it. **Integration and e2e tests** should mirror this
directly: apply a CR and assert on what the user observes — Deployments,
Services, ConfigMaps, lifecycle task Jobs, and parent status conditions.

**Unit tests** serve a different purpose: they provide rich permutation
coverage of business logic (merge semantics, config rendering, preset
resolution) that would be too slow or combinatorially expensive to exercise
at higher tiers. They are inherently non-user-facing, but the same principle
applies — assert on the *output* of a function (the resolved spec, the
rendered config) rather than on *how* the function arrived at that output.

Across all tiers:

- **Avoid testing private functions in isolation** unless they contain
  genuinely complex logic (merge semantics, backoff math). If a behavior is
  only meaningful through reconciliation, test it through reconciliation.
- **Refactor freely without rewriting tests.** If renaming an internal
  helper or restructuring a package breaks tests, those tests were coupled
  to implementation, not behavior, and should be rewritten to assert on
  observable outputs or removed entirely.

### Security and the threat model

The [security reference](../reference/security.md) is the source of truth for the
operator's security guarantees. Keep it and the tests that guard it in lockstep:

- **When you change the threat model**, update the corresponding security
  regression tests in the same PR (e.g. the ConfigMap no-leak guard, the RBAC
  scope guard, and the CEL secret-gating specs) so the guarantees and their
  guards never drift apart.
- **When you add a feature**, confirm the threat model still holds — update it
  if the change introduces a new trust boundary, secret, or risk surface — and
  add tests covering that new security surface.

### Test pyramid

We use a **pyramid testing strategy** where the vast majority of logic is
covered by fast, deterministic unit tests. Integration and e2e tests are
reserved for verifying the system works end-to-end, not for testing
individual behaviors.

- **Unit tests** — Fast, deterministic, fake client or pure functions. Cover all business logic and permutations.
- **Integration tests** — Minimal envtest tests (real API server). Verify CRD registration, CEL validation, reconciler lifecycle.
- **E2E tests** — 1-2 comprehensive scenarios on a Kind cluster. Verify full operator lifecycle.

| Concern | Unit test | Integration test |
|---|---|---|
| Speed | <1 second | 10-30 seconds (envtest startup) |
| Reliability | Deterministic | Flaky (port binding, timing) |
| Dependencies | None (fake client) | envtest binaries |
| IDE support | Works everywhere | Needs KUBEBUILDER_ASSETS |
| CI cost | Negligible | Moderate |

**Rule of thumb**: If you can test it with a fake client, do. Reserve
envtest/e2e for things that genuinely need a real API server (CEL
validation, CRD defaulting, multi-controller interaction).

> **Why CEL/schema tests can't move down.** The controller-runtime fake client
> does **not** enforce CRD OpenAPI schema or CEL (`x-kubernetes-validations`)
> rules — those only fire on a real API server. So tests for schema/CEL
> validation must live at the integration tier (envtest) and cannot be rewritten
> as fake-client unit tests, even though they look like "just" a create call.
> Conversely, anything that only builds or reconciles objects and asserts on the
> result belongs at the unit tier. (envtest/Kind cost is dominated by fixed suite
> startup, not per-spec, so moving a spec between tiers rarely changes CI time.)

### Test granularity

- **Prefer broad happy-path tests** that cover critical assertions in a single test function. For example, one comprehensive test that creates all components and verifies config, env vars, and status is better than 10 separate tests each checking one field.
- **Use granular table tests for helpers with their own semantic contract** (e.g., merge functions, backoff calculation, condition management, field-preservation helpers). When a regression spans both reconciliation flow and helper semantics, pair one behavior-level happy-path test with a focused helper table. Keep simple glue covered through the behavior it supports.
- **Every assertion should protect against a plausible regression.** If you can't name the scenario where removing it would let a bug through, it doesn't belong. Avoid adding narrow standalone tests when the assertion fits naturally in an existing comprehensive test.
- **When fixing a regression, always add a test that would have caught it.** Regressions that broke silently indicate a gap in test coverage — close the gap as part of the fix.
- **Use subtests (`t.Run`)** to group related scenarios within a single test function instead of creating separate top-level tests.

### What goes where

**Unit tests** (`*_test.go` with `testing` package + `fake.NewClientBuilder`):
- Resolution engine: merge semantics, override behavior, beat singleton
- Config rendering: per-component Python output, metastore URI, config
- Parent controller: reconciliation logic, parent-owned resource creation/deletion,
  config env var injection, image overrides, status aggregation, suspend
- Component resource helpers: ConfigMap, Deployment, Service reconciliation
- Lifecycle task Jobs: PodSpec building, retention policy, backoff calculation

**Integration tests** (Ginkgo + envtest):
- CRD schema validation works (kubebuilder markers produce correct OpenAPI)
- CEL validation rules reject invalid CRs
- Controller manager starts and registers all controllers

**E2E tests** (Ginkgo + Kind cluster):
- Operator health: controller pod running, metrics endpoint serving
- CR lifecycle: apply Superset CR → Deployments + ConfigMaps exist → parent status seedd
- Multi-component: all component types reconciled with correct sub-resources

### Running E2E tests locally

`make test-e2e` creates a throwaway Kind cluster (`superset-kubernetes-operator-test-e2e`), builds the operator image, loads it into the cluster, runs the Ginkgo specs, and tears the cluster down.

```bash
make test-e2e
```

The Kind node image is pinned in the Makefile (`KIND_NODE_IMAGE`) and matches the Kind binary version used in CI. Override `E2E_PROJECT_IMAGE` or `E2E_CURL_IMAGE` if your environment requires a mirror registry, or set `E2E_SKIP_BUILD_LOAD=1` to reuse an image you've already loaded into Kind during iteration.

### Writing a new unit test

Use the standard pattern from `superset_controller_test.go`:

```go
func TestReconcile_MyScenario(t *testing.T) {
    scheme := testScheme(t)

    superset := &supersetv1alpha1.Superset{
        ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
        Spec: supersetv1alpha1.SupersetSpec{
            Image:       supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
            Environment: strPtr("dev"),
            SecretKey:   strPtr("test-secret-key"),
            Lifecycle: &supersetv1alpha1.LifecycleSpec{
                Disabled: boolPtr(true),
            },
        },
    }

    c := fake.NewClientBuilder().
        WithScheme(scheme).
        WithObjects(superset).
        WithStatusSubresource(superset).
        Build()

    r := &SupersetReconciler{
        Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10),
    }

    _, err := r.Reconcile(context.Background(), reconcile.Request{
        NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
    })
    if err != nil {
        t.Fatalf("reconcile: %v", err)
    }

    // Assert on the result...
}
```

Key patterns:
- Use `boolPtr(true)` for `Lifecycle.Disabled` to bypass lifecycle task execution
- Register all types in the scheme via `testScheme(t)` helper
- Use `WithStatusSubresource` for objects whose status is updated
- Assert on parent-owned resources and parent status, not on internal helper state

### Testing pure packages

`internal/resolution/` and `internal/config/` are pure Go with zero
controller-runtime dependencies. Test them directly:

```go
func TestMergeEnvVars_ConflictResolution(t *testing.T) {
    result := resolution.MergeEnvVars(
        []corev1.EnvVar{{Name: "A", Value: "1"}},
        []corev1.EnvVar{{Name: "A", Value: "2"}},
    )
    // Later slice wins on name conflict.
    if result[0].Value != "2" { ... }
}
```

### Fuzzing

We fuzz a small set of pure functions with Go's built-in fuzzing
(`go test -fuzz`).

Fuzz targets live in `*_fuzz_test.go` files beside the code they exercise, in
the same package and with no build tag (so they compile under the default
`go test`). Run all of them, bounded, with:

```sh
make fuzz              # 30s per target
make fuzz FUZZTIME=2m  # longer local run
```

Each target seeds a corpus with `f.Add(...)` cases and asserts an invariant
beyond "does not panic" — e.g. `CompareVersions` is antisymmetric, `RenderConfig`
is deterministic, `pyQuote` round-trips, `MergeMaps` is a last-writer-wins union.
The seed corpus (plus any committed `testdata/fuzz/...` reproducers) replays as
ordinary subtests during `make test-unit`, so regressions are caught on every PR;
the scheduled `fuzz.yaml` workflow runs the targets for longer to explore new
inputs.

**When to add a target:** a pure, deterministic function that parses strings,
generates code/config, or merges collections from CR-author-controlled input.
Skip trivial scalar or preset math already covered by table tests.

**Scope — robustness, not a security boundary.** As noted under
[Security and the threat model](#security-and-the-threat-model), `Superset` CR
input comes from a *trusted* namespace admin. Fuzzing here guards against panics
and non-determinism on awkward-but-valid input; it is not defending a trust
boundary. Frame findings accordingly.

**On a finding:** `go test` writes a reproducer to `testdata/fuzz/<target>/<id>`.
Commit it as a permanent regression seed, then fix the bug.

---

## License Headers

All source files must carry the Apache License 2.0 header. This is enforced in CI
by [Apache Rat](https://creadur.apache.org/rat/).

To check locally (requires Java):

```sh
make check-license
```

The script downloads the Rat jar to `/tmp/lib/` on first run. Files that are
generated, scaffolded by Operator SDK, or not user-authored are excluded via
`.rat-excludes`.

When adding new source files (`.go`, `.yaml`, `.sh`, `Dockerfile`, etc.), include
the appropriate license header. Go files get this automatically from
`hack/boilerplate.go.txt` when using `make generate`. For other file types, use
the `#`-comment form:

```
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
```

---

## Do's and Don'ts

### Do

- **Use shared helpers** from `component_reconciler.go` (`reconcileComponentDeployment`, `reconcileComponentService`, `reconcileScaling`, `buildChecksumAnnotations`). ConfigMaps are owned by the parent — use `reconcileParentOwnedConfigMap` from `superset_controller.go`.
- **Use `componentLabels(component, instance)`** for consistent label generation
- **Stamp `parentLabels(parentName)` on all parent-owned resources** (ServiceAccount, Ingress, HTTPRoute, ServiceMonitor) — this enables label-based cleanup
- **Use `controllerutil.CreateOrUpdate` as the live-object boundary**: keep `build*Spec` helpers pure, build desired specs from resolved CR/operator inputs, then preserve API-server/defaulted or other-controller-owned fields before assignment. Examples include Service `ClusterIP`/`HealthCheckNodePort` and HPA-managed Deployment `replicas`.
- **Set OwnerReferences** via `controllerutil.SetControllerReference`
- **Record events** via `r.Recorder.Eventf()` for errors and state changes
- **Add Go doc comments** to all exported types — they become CRD descriptions
- **Run `make manifests generate`** after any change to `api/v1alpha1/`
- **Write unit tests first** — every new feature should have tests before integration
- **Test with fake client** — only use envtest when you genuinely need a real API server

### Don't

- **Don't hardcode resource names** — use `componentDescriptor.resourceBaseName()` and naming helpers. Component resources are named `{parentName}-{componentType}`.
- **Don't fetch resources by name for cleanup** — use `deleteByLabels` with parent/component labels to discover and clean up. Name-based `CreateOrUpdate` and status reads are fine — those address resources whose names the operator controls.
- **Don't hardcode commands/ports** — use `DeploymentConfig` defaults
- **Don't duplicate controller logic** — use `component_reconciler.go` helpers
- **Don't copy derived facts between docs** — values defined in code or in one canonical doc (e.g. the per-component routing paths in [`networking-and-monitoring.md`](../user-guide/networking-and-monitoring.md)) should be stated once and cross-linked from elsewhere, not re-listed on other pages where they silently go stale. Same spirit as the Automation Principle: one source of truth.
- **Don't add fields without doc comments** — they become CRD descriptions
- **Don't use `bool` with `omitempty`** — use `*bool` to distinguish false from unset
- **Don't write integration tests for unit-testable logic** — use fake client
- **Don't put secret values in ConfigMaps** — in prod mode, secrets are mounted via env vars from Kubernetes Secrets
- **Don't use admission webhooks for validation** — use [CEL](https://kubernetes.io/docs/reference/using-api/cel/) (`x-kubernetes-validations`) on CRD types instead. Webhooks add operational complexity (cert-manager dependency, TLS setup) and are not installed by default. All validation rules should be expressed as CEL where feasible.
- **Cross-reference security docs** — any change that affects trust boundaries, secret handling, RBAC, CRD validation, or the operator's attack surface must be reflected in [`docs/reference/security.md`](../reference/security.md). Review the threat model, design decisions, and in-scope/out-of-scope sections to ensure they stay accurate.

## How to Add a New Field

New Deployment/Pod/Container fields go into the template hierarchy:

1. Determine the Kubernetes level: `DeploymentTemplate` (Deployment-level),
   `PodTemplate` (PodSpec-level), or `ContainerTemplate` (Container-level)
2. Add the field to the appropriate template type in `api/v1alpha1/shared_types.go`
3. Add the merge logic in `internal/resolution/merge.go` (`MergeDeploymentTemplate`,
   `MergePodTemplate`, or `MergeContainerTemplate`) using the field's natural
   semantics (scalar → `ResolveOverridableValue`, named slice → `Merge*ByName`,
   map → `MergeMaps`, unnamed slice → `append`). All merge functions follow the
   convention `Merge*(topLevel, component[, operatorInjected])`: the top-level
   value establishes ordering, the component value overrides by name in place,
   and where applicable (env vars, volume mounts, labels) operator-injected
   values are applied last so they cannot be overridden by user configuration.
   Any code consuming resolved ports, env vars, or other merged collections
   must respect this same ordering.
4. Wire the field in `internal/controller/deployment_builder.go` (`buildDeploymentSpec`)
5. Run `make manifests generate`
6. Add assertions to existing comprehensive tests
7. Update sample CRs if helpful

## How to Add a New Component

1. Add the component spec to the parent `SupersetSpec` in `superset_types.go`.
2. Add resource defaults in `internal/controller/component_resources.go` via
   `ComponentResourceDefs()` with component name, `DeploymentConfig`
   (container name, default command, ports), `hasConfig`, and `hasScaling`.
3. Register the component in `internal/controller/component_descriptors.go`:
   - Add a `componentDescriptor` entry with `extract`, optional `adjustSpec`,
     and `statusAccessor` functions.
   - Add the descriptor to the `componentDescriptors` slice.
4. Add status fields if the component needs a dedicated slot under
   `ComponentStatusMap`.
5. Run `make codegen`.
6. Add sample CR coverage, focused unit tests, and e2e assertions for the
   parent-owned resources.

## Package Structure

```
internal/
├── resolution/       # Pure Go — spec flattening engine
│                     # Zero controller-runtime deps, fully unit-testable
│                     # MergeMaps, MergeEnvVars, ResolveComponentSpec(), etc.
├── config/           # Pure Go — Python config renderer
│                     # Per-component rendering, metastore URI,
│                     # config appending
├── common/           # Shared types (ComponentType, Ptr helper)
└── controller/       # controller-runtime — reconcilers
                      # Parent controller, component resources,
                      # task job lifecycle, status, scaling, networking
```

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `api/v1alpha1/shared_types.go` | ImageSpec, MetastoreSpec, DeploymentTemplate, PodTemplate, ContainerTemplate, FlatComponentSpec |
| `api/v1alpha1/superset_types.go` | Parent SupersetSpec, component specs, InitSpec, CEL validation rules, status |
| `internal/common/types.go` | Shared ComponentType, Ptr helper |
| `internal/resolution/resolver.go` | ResolveComponentSpec — core flattening engine |
| `internal/config/renderer.go` | RenderConfig — per-component Python generation |
| `internal/controller/component_reconciler.go` | Shared helpers for component resources |
| `internal/controller/component_resources.go` | `ComponentResourceDefs()` — table-driven component resource defaults |
| `internal/controller/component_descriptors.go` | Parent-side component descriptors for resource reconciliation and status |
| `internal/controller/superset_controller.go` | Parent reconciler (orchestrates everything) |
| `internal/controller/deployment_builder.go` | Deployment construction from flat spec |
| `internal/controller/initpod.go` | Lifecycle task Job PodSpec building, retention, backoff |
| `internal/controller/reconcile_parent_resources_test.go` | Parent controller resource tests (fake client) |

---

## Metrics

The operator exposes metrics at two levels:

### Operator metrics (own process)

Controller-runtime provides default metrics (reconcile counts/durations, leader
election, work queue depth) on HTTPS port 8443. The metrics endpoint is
protected by Kubernetes authentication and authorization — only clients with
the `metrics-reader` ClusterRole can scrape.

Key files:

| File | Purpose |
|------|---------|
| `config/default/metrics_service.yaml` | Service exposing :8443 |
| `config/default/manager_metrics_patch.yaml` | Injects `--metrics-bind-address` |
| `config/rbac/metrics_auth_role.yaml` | TokenReview/SubjectAccessReview for auth |
| `config/rbac/metrics_reader_role.yaml` | Grants GET on `/metrics` |
| `config/prometheus/monitor.yaml` | ServiceMonitor for operator metrics (optional) |
| `config/network-policy/allow-metrics-traffic.yaml` | NetworkPolicy restricting scrape access (optional) |

The Helm chart enables metrics by default (`metrics.enabled: true`, port 8443).

**No custom metrics.** Controller-runtime defaults are sufficient for operator
health. Don't add custom metrics unless there's a concrete alerting or
dashboarding need that can't be met by the defaults.

### Superset instance monitoring

When `spec.monitoring.serviceMonitor` is set on a Superset CR, the parent
controller creates a Prometheus ServiceMonitor targeting the web-server
component (port 8088). This uses unstructured objects because the
ServiceMonitor CRD is external (`monitoring.coreos.com/v1`). If the CRD is
not installed, the controller logs an info message and continues.

See `internal/controller/monitoring.go` for the implementation.

---

## Logging

The operator logs through `logf.FromContext(ctx)` (controller-runtime's
logr/zap). Verbosity is a single knob, `--zap-log-level`, configured in
`cmd/main.go`:

| Flag | Levels emitted | Audience |
|------|----------------|----------|
| `--zap-log-level=info` (default) | V(0) | Operators in production |
| `--zap-log-level=debug` (alias `=1`) | V(0)+V(1) | Debugging a reconcile |
| `--zap-log-level=2` | V(0)+V(1)+V(2) | Deep tracing internals |

### Three tiers

- **Info / V(0)** — `log.Info(msg, kv...)`. Low-frequency, meaningful state
  *transitions* an operator wants at default verbosity (lifecycle task
  created/completed/failed, downgrade blocked, awaiting approval, maintenance
  cleared, a component actually changed). **Must not fire on every reconcile.**
- **V(1) / debug** — `log.V(1).Info(msg, kv...)`. Per-reconcile progress and
  decisions: reconcile entry, phase markers, checksum-skip decisions,
  "waiting for X" requeues, schedule next-requeue time, CRD-absent skips, no-op
  results.
- **V(2) / trace** — `log.V(2).Info(msg, kv...)`. Fine-grained internals:
  rendered config size/checksum, version-comparison result, per-resource
  CreateOrUpdate op even when unchanged.

### Error logging

`log.Error(err, msg, kv...)` is reserved for **non-fatal errors that are
swallowed** — not returned up the stack and not surfaced as a Kubernetes Event —
so they are not silently lost. Do **not** `log.Error` an error you also return:
controller-runtime already logs returned reconcile errors at the top of the
loop, and the code records a Warning Event via `r.Recorder.Eventf`. Double
logging just creates noise.

### Cross-cutting rule

Anything that fires on **every** reconcile regardless of state change is V(1) or
deeper. V(0) is for genuine transitions only. When in doubt, ask "would this line
appear in a steady-state cluster where nothing is changing?" — if yes, it is not
V(0).

### Conventions

- Structured keys are **camelCase**: `name`, `namespace`, `component`, `task`,
  `image`, `from`, `to`, `operation`, `reason`, `phase`. Reuse these; don't
  invent synonyms.
- Never log secret values or rendered config bodies. Log sizes, counts, or
  checksums instead (e.g. `configBytes`, `envVars`, `checksum`).
- Pure packages (`internal/resolution/`, `internal/config/`, `internal/common/`,
  `internal/schedule/`) do **zero** logging by design — they have no
  controller-runtime dependency and return errors only. Keep it that way.

### Enabling verbose logs when debugging

```sh
make run ARGS="--zap-log-level=debug"   # V(1)
make run ARGS="--zap-log-level=2"        # V(1)+V(2)
# in-cluster (Helm): --set logLevel=debug (or 2) — see Installation › Debugging the operator
```

---

## Pull Requests

### Title format

PR titles must follow the conventional commits format:

```
type(scope): description
type: description
```

Scope is optional but encouraged when the change is scoped to a single area.
Keep titles concise: aim for 50 characters when practical, and avoid exceeding
72 characters because GitHub wraps longer titles in common views.

CI validates this on every PR via the `PR / Validate PR title` check.

**Allowed types:**

| Type | Use for |
|------|---------|
| `feat` | New functionality |
| `fix` | Bug fixes |
| `refactor` | Code restructuring without behavior change |
| `docs` | Documentation only |
| `test` | Adding or updating tests |
| `chore` | Maintenance (config, tooling, dependencies) |
| `ci` | CI/CD workflow changes |
| `build` | Build system or external dependency changes |
| `perf` | Performance improvements |
| `style` | Formatting, whitespace, linting |
| `revert` | Reverting a previous commit |

**Allowed scopes:**

| Scope | Covers |
|-------|--------|
| `api` | CRD type definitions (`api/v1alpha1/`) |
| `controller` | Reconciler logic (`internal/controller/`) |
| `resolution` | Spec resolution/merge engine (`internal/resolution/`) |
| `config` | Config rendering (`internal/config/`) |
| `helm` | Helm chart (`charts/`) |
| `ci` | CI workflows, tooling (`.github/`, `Makefile`) |
| `docs` | Documentation (`docs/`) |
| `deps` | Dependency updates |

**Examples:**

```
feat(api): add tolerations field to PodTemplate
fix(controller): handle nil deployment template in scaling reconciler
docs: add valkey configuration examples to user guide
chore(deps): bump controller-runtime to v0.20.0
```

### Description

Every PR must include a **Summary** section with at least one paragraph
explaining what the change does and why. A reviewer should understand the
motivation and scope from the summary alone, without reading the diff.

Use the optional **Details** section for implementation notes, design decisions,
alternatives considered, or migration steps.

The PR template (`PULL_REQUEST_TEMPLATE.md`) pre-fills these sections.

### Changelog entry

Add a bullet under `## Unreleased` in
[`docs/reference/releases.md`](../reference/releases.md)
for noteworthy changes — new features,
new CRD fields, behavior changes, breaking changes, deprecations, and
critical bug fixes. Skip the entry for routine work that a user wouldn't
care about: dependency bumps, CI tweaks, internal refactors, test-only
changes, and documentation-only updates.

Group entries under `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, or
`Security` (per [Keep a Changelog](https://keepachangelog.com/en/1.1.0/));
create the subheading on first use. Lead each bullet with the user-facing
effect, not the implementation:

```markdown
## Unreleased

### Added
- New `webServer.gunicorn.keepAlive` field for tuning Gunicorn keepalive timeouts.

### Fixed
- Lifecycle `migrate` task no longer retries indefinitely when the metastore
  rejects credentials; the parent surfaces an `AuthenticationFailed` reason.
```

The release manager does a final review pass over `## Unreleased` before
tagging — see [Releasing](releasing.md#reviewing-the-changelog) — so it's
fine to err on the side of including an entry; missing or duplicate ones can
be cleaned up there.

### Code coverage

CI uploads test coverage to [Codecov](https://codecov.io) on every PR. The
Codecov bot posts a comment showing:

- **Patch coverage** — what percentage of new/changed lines are covered by tests
- **Project coverage delta** — how overall coverage changed
