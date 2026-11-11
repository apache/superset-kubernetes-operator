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

This guide covers the patterns, conventions, and testing philosophy that govern contributions to the operator. For environment setup, see
[Development Setup](development-setup.md).

## Architecture

See [Architecture](../architecture/overview.md) for the structural overview (CRD hierarchy, configuration model, config rendering) and [Internals](../architecture/internals.md) for runtime behavior (reconciliation lifecycle, parent-owned resource reconciliation, status reporting). Key points:

- **Single public CRD**: `Superset` resolves shared spec (top-level + per-component) into parent-owned Kubernetes resources
- **6 deployment components + lifecycle tasks**: web server, Celery worker, Celery beat, Flower, websocket, MCP, and task Jobs for seed/migrate/rotate/init
- **Pure Go packages** (no controller-runtime dependency): `internal/resolution/` (spec flattening), `internal/config/` (Python rendering), `internal/common/` (shared types), `internal/schedule/` (cron tick computation)
- **Parent resolves and executes**: All layering, lifecycle orchestration, resource reconciliation, and status projection live in the parent controller

### Top-level defaults and per-component overrides

The `Superset` spec has two levels. A top-level field is a default that every component it applies to inherits, and the same field on a component overrides it. Users set shared settings once and only spell out per component what differs.

When a field applies to more than one component, declare it at both levels and resolve it in `internal/resolution` according to its shape:

- **Whole values** (`replicas`, `autoscaling`, `podDisruptionBudget`, `sqlaEngineOptions`): the component value replaces the top-level value (`ResolveScalar`, `ResolveOverridableValue`).
- **Templates** (`deploymentTemplate`, `podTemplate`): field-level merge (see [How to Add a New Field](#how-to-add-a-new-field)).
- **Image**: `tag` and `repository` override independently.
- **Raw config** (`config`): the component value is appended after the top-level value.

Keep a field per-component only when its meaning is specific to one component (`webServer.gunicorn`, `celeryWorker.celery`), or when inheriting it would be wrong (`command`/`args`). Components a top-level value doesn't apply to ignore it: Celery beat and lifecycle Jobs ignore `autoscaling` and `podDisruptionBudget`.

Validation that depends on an inherited value must check the effective value, not just the field as written on the component. Put such CEL rules on `SupersetSpec`, where both levels are visible.

### External connection values

Typed connection interfaces must compose with Kubernetes-native provisioners. For every literal connection field that a database, cache, or other service operator may publish after reconciliation—such as host, port, database, or username—provide a sibling `*From` field using `corev1.SecretKeySelector`. The literal and selector must be mutually exclusive through CEL validation, and both must produce the same operator-internal environment variable. Keep type-appropriate defaults when neither optional form is set.

Use per-field selectors rather than assuming a whole-Secret key convention: connection Secret keys differ across provisioners, and endpoint data and credentials may be owned by separate resources. The operator must pass selectors through as `valueFrom.secretKeyRef`; it must not read referenced Secret values or render them into ConfigMaps, checksums, status, or logs. Apply this convention consistently to every connection tuple, including lifecycle-only source or target connections, and cover both generated schema validation and observable Pod env output.

CRD defaulting runs before CEL validation. Do not put a kubebuilder/OpenAPI default on the literal side of a literal/`*From` pair: when a user sets only the selector, admission would first materialize the literal default and then the mutual-exclusion rule would reject the resource as if both fields had been supplied. Apply the effective default in the resolution or env-building layer only when neither form is set. Preserve typed validation for explicit literals, document the runtime default, and use an envtest case with the `*From` field to verify the generated CRD—not just the Go helper.

### Enumerated fields

Only use an enum when the operator defines the set of values: modes and policies it implements (`environment`, `upgradeMode`, presets, retention policy), choices it maps to its own behavior (`metastore.type` selects the driver, default port, and database-tool image), and status values it writes. Model each one as a named string type in `api/v1alpha1` with a typed constant per value. Put the `+kubebuilder:validation:Enum` marker on the type, not the field, so the Go constants and the CRD schema are declared in one place:

```go
// UpgradeMode controls whether lifecycle upgrades require manual approval.
// +kubebuilder:validation:Enum=Automatic;Supervised
type UpgradeMode string

const (
	// UpgradeModeAutomatic runs lifecycle tasks immediately on image change.
	UpgradeModeAutomatic UpgradeMode = "Automatic"
	// UpgradeModeSupervised waits for an approval annotation before running lifecycle tasks.
	UpgradeModeSupervised UpgradeMode = "Supervised"
)
```

Use these constants in controllers and config rendering, and keep the typed value through helper signatures. Convert with `string(...)` only where the value leaves Go, such as an env var or a command argument. Don't redeclare enum values as untyped string constants in internal packages. Document each constant: the API reference renders the comments as the table of allowed values. Tests may build inputs from the constants but assert expected values as literals; see [Testing Philosophy](#guiding-principle-assert-on-observable-outputs).

Don't use an enum for values the operator passes through to software it doesn't control, such as the Gunicorn worker class and log level, the Celery pool and optimization, or the Valkey client's `ssl_cert_reqs`. Upstream can add values at any time, and an enum would reject them until the operator is released again. Keep these fields plain `*string`, name a few common values and the default in the doc comment, and keep the operator's default as an unexported constant in the package that applies it.

When a Kubernetes type already exists (`corev1.PullPolicy`, `corev1.ServiceType`), reuse it, and narrow it with a field-level `Enum` marker if the operator only supports some values. Prefer an enum over a `bool` for operator-defined modes that may gain another value later.

---

## Testing Philosophy

### Guiding principle: assert on observable outputs

For a Kubernetes operator, the "user" is the person writing a CR and `kubectl apply`-ing it. **Integration and e2e tests** should mirror this directly: apply a CR and assert on what the user observes — Deployments, Services, ConfigMaps, lifecycle task Jobs, and parent status conditions.

**Unit tests** serve a different purpose: they provide rich permutation coverage of business logic (merge semantics, config rendering, preset resolution) that would be too slow or combinatorially expensive to exercise at higher tiers. They are inherently non-user-facing, but the same principle applies — assert on the *output* of a function (the resolved spec, the rendered config) rather than on *how* the function arrived at that output.

Across all tiers:

- **Avoid testing private functions in isolation** unless they contain genuinely complex logic (merge semantics, backoff math). If a behavior is only meaningful through reconciliation, test it through reconciliation.
- **Refactor freely without rewriting tests.** If renaming an internal helper or restructuring a package breaks tests, those tests were coupled to implementation, not behavior, and should be rewritten to assert on observable outputs or removed entirely.
- **Assert expected values as literals, not production constants.** A user sees `Running`, `GUNICORN_LOGLEVEL`, or `prefork`, not the Go constant behind it. Write the expected value out (`assert.EqualValues(t, "Running", superset.Status.Phase)`) so an accidental change to a constant, enum value, env var name, or label key fails a test instead of silently moving the expectation with it. Building test inputs from constants is fine.
- **Don't test non-actions.** A test should pin a behavior the code actively produces, not assert the absence of behavior the code never had (or no longer has). When you delete a feature, delete its tests — don't invert them into "X no longer happens" assertions. The positive tests for the surviving behavior already cover it: e.g. after removing downgrade blocking, a "downgrade proceeds" test is redundant because the general "an image change proceeds and re-runs migrate" test already exercises that path. (Mirrors the documentation rule: don't describe what the default already precludes.)

### Security and the threat model

The [security reference](../reference/security.md) is the source of truth for the operator's security guarantees. Keep it and the tests that guard it in lockstep:

- **When you change the threat model**, update the corresponding security regression tests in the same PR (e.g. the ConfigMap no-leak guard, the RBAC scope guard, and the CEL secret-gating specs) so the guarantees and their guards never drift apart.
- **When you add a feature**, confirm the threat model still holds — update it if the change introduces a new trust boundary, secret, or risk surface — and add tests covering that new security surface.

### Test pyramid

We use a **pyramid testing strategy** where the vast majority of logic is covered by fast, deterministic unit tests. Integration and e2e tests are reserved for verifying the system works end-to-end, not for testing individual behaviors.

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

**Rule of thumb**: If you can test it with a fake client, do. Reserve envtest/e2e for things that genuinely need a real API server (CEL validation, CRD defaulting, multi-controller interaction).

> **Why CEL/schema tests can't move down.** The controller-runtime fake client does **not** enforce CRD OpenAPI schema or CEL (`x-kubernetes-validations`) rules — those only fire on a real API server. So tests for schema/CEL validation must live at the integration tier (envtest) and cannot be rewritten as fake-client unit tests, even though they look like "just" a create call. Conversely, anything that only builds or reconciles objects and asserts on the result belongs at the unit tier. (envtest/Kind cost is dominated by fixed suite startup, not per-spec, so moving a spec between tiers rarely changes CI time.)

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
- Parent controller: reconciliation logic, parent-owned resource creation/deletion, config env var injection, image overrides, status aggregation, suspend
- Component resource helpers: ConfigMap, Deployment, Service reconciliation
- Lifecycle task Jobs: PodSpec building, retention policy, backoff calculation

**Integration tests** (Ginkgo + envtest):

- CRD schema validation works (kubebuilder markers produce correct OpenAPI)
- CEL validation rules reject invalid CRs
- Controller manager starts and registers all controllers

**E2E tests** (Ginkgo + Kind cluster):

- Operator health: controller pod running, metrics endpoint serving
- CR lifecycle: apply Superset CR → Deployments + ConfigMaps exist → parent status populated
- Multi-component: all component types reconciled with correct sub-resources

### Running E2E tests locally

`make test-e2e` creates a throwaway Kind cluster (`superset-kubernetes-operator-test-e2e`), builds the operator image, loads it into the cluster, runs the Ginkgo specs, and tears the cluster down.

```bash
make test-e2e
```

The Kind node image (`KIND_NODE_IMAGE`) is read from the first entry in `.github/supported-k8s.json` and matches the Kind binary version used in CI. Override `E2E_PROJECT_IMAGE` or `E2E_CURL_IMAGE` if your environment requires a mirror registry, or set `E2E_SKIP_BUILD_LOAD=1` to reuse an image you've already loaded into Kind during iteration.

### Writing a new unit test

Use the standard pattern from `reconcile_parent_resources_test.go`:

```go
func TestReconcile_MyScenario(t *testing.T) {
    scheme := testScheme(t)

    superset := &supersetv1alpha1.Superset{
        Name:      "test",
        Namespace: "default",
        Spec: supersetv1alpha1.SupersetSpec{
            Image:       supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.1.0"},
            Environment: new(common.EnvironmentDev),
            SecretKey:   new("test-secret-key"),
            Lifecycle: &supersetv1alpha1.LifecycleSpec{
                Disabled: new(true),
            },
        },
    }

    c := fake.NewClientBuilder().
        WithScheme(scheme).
        WithObjects(superset).
        WithStatusSubresource(superset).
        Build()

    r := &SupersetReconciler{
        Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10),
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

- Use `new(true)` for `Lifecycle.Disabled` to bypass lifecycle task execution
- Register all types in the scheme via `testScheme(t)` helper
- Use `WithStatusSubresource` for objects whose status is updated
- Assert on parent-owned resources and parent status, not on internal helper state

### Testing pure packages

`internal/resolution/` and `internal/config/` are pure Go with zero controller-runtime dependencies. Test them directly:

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

We fuzz a small set of pure functions with Go's built-in fuzzing (`go test -fuzz`).

Fuzz targets live in `*_fuzz_test.go` files beside the code they exercise, in the same package and with no build tag (so they compile under the default `go test`). Run all of them, bounded, with:

```sh
make fuzz              # 30s per target
make fuzz FUZZTIME=2m  # longer local run
```

Each target seeds a corpus with `f.Add(...)` cases and asserts an invariant beyond "does not panic" — e.g. `RenderConfig` is deterministic, `pyQuote` round-trips, `MergeMaps` is a last-writer-wins union. The seed corpus replays as ordinary subtests during `make test-unit`, so regressions are caught on every PR; the scheduled `fuzz.yaml` workflow runs the targets for longer to explore new inputs.

**When to add a target:** a pure, deterministic function that parses strings, generates code/config, or merges collections from CR-author-controlled input. Skip trivial scalar or preset math already covered by table tests.

**Scope — robustness, not a security boundary.** As noted under [Security and the threat model](#security-and-the-threat-model), `Superset` CR input comes from a *trusted* namespace admin. Fuzzing here guards against panics and non-determinism on awkward-but-valid input; it is not defending a trust boundary. Frame findings accordingly.

**On a finding:** `go test` writes a reproducer to `testdata/fuzz/<target>/<id>`. Capture its minimized input and add it back as an `f.Add(...)` seed in the target (with a short comment noting it is a fuzz-discovered regression), then delete the `testdata/fuzz/<target>/<id>` file and fix the bug. Committing the seed as an `f.Add(...)` line — rather than the raw `testdata/fuzz` reproducer — keeps it replaying on every `make test-unit` while staying license-headed Go source: the generated corpus files carry no Apache license header and fail the `check-license` (Apache Rat) job.

### Helm chart tests

The Helm chart (`charts/superset-operator/`) has its own [helm-unittest](https://github.com/helm-unittest/helm-unittest) suite under `charts/superset-operator/tests/`. It renders the chart with different values and asserts on the output — the same "assert on observable outputs" principle as the Go tests, applied to rendered manifests. CI runs it in the `Helm lint & test` job.

Install the tooling once (the plugin binary is pinned; `yq` is only needed for the coverage gate):

```sh
bash scripts/install-helm-unittest.sh
bash scripts/install-yq.sh   # or: brew install yq
```

Run locally:

```sh
make helm-test              # run the chart unit tests
helm unittest -u charts/superset-operator   # update snapshots after an intended change
make helm-values-covered    # verify every values.yaml knob is exercised
```

Coverage comes in two complementary forms. Full-render **snapshots** catch drift across the whole rendered output — `minimal_test.yaml` (default `values.yaml`) and `full_options_test.yaml` (every knob set via `tests/values/full-options.yaml`), stored in `tests/__snapshot__/`. Targeted assertions (e.g. `optout_and_helpers_test.yaml`) cover opt-out and template-helper branches a single combined snapshot cannot systematically reach, varying one knob at a time. Chart/release identity is pinned in each suite so snapshots stay stable across chart version bumps.

**Any change to the chart — especially a new values knob or feature — must be exercised in `tests/values/full-options.yaml` and its rendered effect captured in the snapshot (`helm unittest -u`).** This is enforced: `make helm-values-covered` (`scripts/check-chart-values-covered.sh`) fails CI when a `values.yaml` key is not set in the comprehensive values file. It closes the gap a snapshot alone cannot — a knob that defaults to null and renders nothing under defaults would otherwise leave both suites green. The gate keys off `values.yaml`, so a template that reads an *undocumented* `.Values.x` is not caught by it; that already violates the chart's documented-values contract (the README is generated from `values.yaml` via helm-docs) and is expected to be caught in review.

---

## License Headers

All source files must carry the Apache License 2.0 header. This is enforced in CI by [Apache Rat](https://creadur.apache.org/rat/).

To check locally (requires Java):

```sh
make check-license
```

The script downloads the Rat jar to `${XDG_CACHE_HOME:-$HOME/.cache}/superset-kubernetes-operator/lib/` on first run. Files that are generated, scaffolded by Operator SDK, or not user-authored are excluded via `.rat-excludes`.

When adding new source files (`.go`, `.yaml`, `.sh`, `Dockerfile`, etc.), include the appropriate license header. Go files get this automatically from `hack/boilerplate.go.txt` when using `make generate`. For other file types, use the `#`-comment form:

```text
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

## Automation Principle

When committed state must stay in sync with something external (upstream releases, generated docs, tables derived from a source of truth), build a CI-enforced sync mechanism instead of documenting a manual checklist. Manual steps rot; CI failures don't.

Patterns to follow:

- **One source of truth, generated outputs.** Keep canonical data in a single small file (`.github/supported-k8s.json`, Go type definitions, etc.) and derive everything else (docs tables, CI matrices, defaults) from it.
- **`make codegen` aggregates all generators.** Anything regenerable goes through it. CI's `Verify codegen` job runs `make codegen` and fails on diff.
- **Upstream drift checks.** When the source-of-truth file must track something external (a pinned dependency's release notes, the latest upstream minor), write a `make sync-<thing>` script that queries the upstream API and rewrites the file, plus a `make verify-<thing>` that runs it in `--check` mode in CI. See `scripts/sync-supported-versions.sh` for the canonical example.
- **Scheduled auto-sync PRs.** Pair the sync script with a daily scheduled GitHub Actions job that runs it and opens a PR if anything changed, so open PRs don't all break when upstream moves.
- **Renovate for pinned versions.** Use `customManagers` for non-standard pin sites (regex-matched JSON/YAML fields). Use `prBodyNotes` and `addLabels` to point the reviewer to the right follow-up command on PRs that need manual judgment.
- **Sentinel-bounded generated blocks** in human-edited files (`<!-- BEGIN X -->` / `<!-- END X -->`), so generators can rewrite specific regions without disturbing the hand-written content around them.

The bar: every new "remember to also update X when Y changes" instruction is a bug in the tooling. Fix the tooling instead.

---

## Documentation Style

- **README** is a landing page: project description, philosophy, quick start, link to docs. Keep it welcoming and free of jargon. Don't reference specific knobs, internal config names, or implementation details that might intimidate newcomers.
- **`docs/index.md`** is the primary feature overview for the docs site. Keep feature descriptions high-level and outcome-focused; implementation details belong in the user guide or architecture docs. Prefer consolidated bullets that group related capabilities (e.g., "Lifecycle automation — seeding, migrations, key rotation, and init" rather than a separate bullet per feature).
- **`docs/user-guide/configuration.md`** is the full configuration reference. Here it's appropriate to name specific fields, presets, and env vars, and to show concrete YAML examples.
- **`docs/architecture/overview.md`** explains design decisions and internal structure for contributors and advanced users.
- **General principles:** be concise and objective, avoid overselling or verbose language, reserve code blocks for real code (not ASCII art), and minimize duplication between the README and the docs (the README links to the docs for details).
- **Generated docs:** the API reference and the Helm chart README are generated; see [Development Setup](development-setup.md#documentation) for how to regenerate them and how to link built-in Kubernetes types.

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
- **Don't copy derived facts between docs** — values defined in code or in one canonical doc (e.g. the per-component routing paths in [`networking-and-monitoring.md`](../user-guide/networking-and-monitoring.md)) should be stated once and cross-linked from elsewhere, not re-listed on other pages where they silently go stale. Same spirit as the [Automation Principle](#automation-principle): one source of truth.
- **Don't echo upstream defaults in Helm values or docs** — when a Helm value passes through to a Kubernetes field, leave it unset (`~`) so the platform default applies, and don't restate the current default value in comments, `values.yaml`, or release notes. Defaults can change upstream, and repeating them creates a second source of truth that silently goes stale.
- **Don't add fields without doc comments** — they become CRD descriptions
- **Don't use `bool` with `omitempty`** — use `*bool` to distinguish false from unset
- **Don't write integration tests for unit-testable logic** — use fake client
- **Don't put secret values in ConfigMaps** — in prod mode, secrets are mounted via env vars from Kubernetes Secrets
- **Don't use admission webhooks for validation** — use [CEL](https://kubernetes.io/docs/reference/using-api/cel/) (`x-kubernetes-validations`) on CRD types instead. Webhooks add operational complexity (cert-manager dependency, TLS setup) and are not installed by default. All validation rules should be expressed as CEL where feasible.
- **Cross-reference security docs** — any change that affects trust boundaries, secret handling, RBAC, CRD validation, or the operator's attack surface must be reflected in [`docs/reference/security.md`](../reference/security.md). Review the threat model, design decisions, and in-scope/out-of-scope sections to ensure they stay accurate.

## How to Add a New Field

New Deployment/Pod/Container fields go into the template hierarchy:

1. Determine the Kubernetes level: `DeploymentTemplate` (Deployment-level), `PodTemplate` (PodSpec-level), or `ContainerTemplate` (Container-level)
2. Add the field to the appropriate template type in `api/v1alpha1/shared_types.go`
3. Add the merge logic in `internal/resolution/merge.go` using the field's natural semantics: scalars are overridden by the component value, named collections merge by name, maps merge by key, and unnamed collections append. Values are applied in order: the top-level value establishes ordering, the component value overrides in place, and where applicable (env vars, volume mounts, labels) operator-injected values are applied last so they cannot be overridden by user configuration. Any code consuming resolved env vars, volume mounts, or other merged collections must respect this same ordering.
4. Wire the field in `internal/controller/deployment_builder.go` (`buildDeploymentSpec`)
5. Run `make manifests generate`
6. Add assertions to existing comprehensive tests
7. Update sample CRs if helpful

Fields outside the templates follow the same layering: if a field applies to more than one component, add it at the top level and on each component it applies to, and resolve it as described in [Top-level defaults and per-component overrides](#top-level-defaults-and-per-component-overrides).

## How to Add a New Component

1. Add the component spec to the parent `SupersetSpec` in `superset_types.go`.
2. Add resource defaults in `internal/controller/component_resources.go` via `ComponentResourceDefs()` with component name, `DeploymentConfig` (container name, default command, ports, probes), `defaultPort`, and `hasScaling`.
3. Register the component in `internal/controller/component_descriptors.go`:
    - Add a `componentDescriptor` entry with `hasPythonConfig`, `extract`, optional `adjustSpec`, and `statusAccessor` functions.
    - Add the descriptor to the `componentDescriptors` slice.
4. Add status fields if the component needs a dedicated slot under `ComponentStatusMap`.
5. Run `make codegen`.
6. Add sample CR coverage, focused unit tests, and e2e assertions for the parent-owned resources.

## Package Structure

```text
internal/
├── resolution/       # Pure Go — spec flattening engine
│                     # Zero controller-runtime deps, fully unit-testable
│                     # MergeMaps, MergeEnvVars, ResolveComponentSpec(), etc.
├── config/           # Pure Go — Python config renderer
│                     # Per-component rendering, metastore URI,
│                     # config appending
├── common/           # Shared types (ComponentType, environment constants)
├── schedule/         # Pure Go — cron tick computation
└── controller/       # controller-runtime — reconcilers
                      # Parent controller, component resources,
                      # task job lifecycle, status, scaling, networking
```

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `api/v1alpha1/shared_types.go` | ImageSpec, MetastoreSpec, DeploymentTemplate, PodTemplate, ContainerTemplate, FlatComponentSpec |
| `api/v1alpha1/superset_types.go` | Parent SupersetSpec, component specs, LifecycleSpec, CEL validation rules, status |
| `internal/common/types.go` | Shared ComponentType, environment constants |
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

Controller-runtime provides default metrics (reconcile counts/durations, leader election, work queue depth) on HTTPS port 8443. The metrics endpoint is protected by Kubernetes authentication and authorization — only clients with the `metrics-reader` ClusterRole can scrape.

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

**No custom metrics.** Controller-runtime defaults are sufficient for operator health. Don't add custom metrics unless there's a concrete alerting or dashboarding need that can't be met by the defaults.

### Superset instance monitoring

When `spec.monitoring.serviceMonitor` is set on a Superset CR, the parent controller creates a Prometheus ServiceMonitor targeting the web-server component (port 8088). This uses unstructured objects because the ServiceMonitor CRD is external (`monitoring.coreos.com/v1`). If the CRD is not installed, the controller logs an info message and continues.

See `internal/controller/monitoring.go` for the implementation.

---

## Logging

The operator logs through `logf.FromContext(ctx)` (controller-runtime's logr/zap). Verbosity is a single knob, `--zap-log-level`, configured in `cmd/main.go`:

| Flag | Levels emitted | Audience |
|------|----------------|----------|
| `--zap-log-level=info` (default) | V(0) | Operators in production |
| `--zap-log-level=debug` (alias `=1`) | V(0)+V(1) | Debugging a reconcile |
| `--zap-log-level=2` | V(0)+V(1)+V(2) | Deep tracing internals |

### Three tiers

- **Info / V(0)** — `log.Info(msg, kv...)`. Low-frequency, meaningful state *transitions* an operator wants at default verbosity (lifecycle task created/completed/failed, awaiting approval, maintenance cleared, a component actually changed). **Must not fire on every reconcile.**
- **V(1) / debug** — `log.V(1).Info(msg, kv...)`. Per-reconcile progress and decisions: reconcile entry, phase markers, checksum-skip decisions, "waiting for X" requeues, schedule next-requeue time, CRD-absent skips, no-op results.
- **V(2) / trace** — `log.V(2).Info(msg, kv...)`. Fine-grained internals: rendered config size/checksum, version-comparison result, per-resource CreateOrUpdate op even when unchanged.

### Error logging

`log.Error(err, msg, kv...)` is reserved for **non-fatal errors that are swallowed** — not returned up the stack and not surfaced as a Kubernetes Event — so they are not silently lost. Do **not** `log.Error` an error you also return: controller-runtime already logs returned reconcile errors at the top of the loop, and the code records a Warning Event via `r.Recorder.Eventf`. Double logging just creates noise.

### Cross-cutting rule

Anything that fires on **every** reconcile regardless of state change is V(1) or deeper. V(0) is for genuine transitions only. When in doubt, ask "would this line appear in a steady-state cluster where nothing is changing?" — if yes, it is not V(0).

### Conventions

- Structured keys are **camelCase**: `name`, `namespace`, `component`, `task`, `image`, `from`, `to`, `operation`, `reason`, `phase`. Reuse these; don't invent synonyms.
- Never log secret values or rendered config bodies. Log sizes, counts, or checksums instead (e.g. `configBytes`, `envVars`, `checksum`).
- Pure packages (`internal/resolution/`, `internal/config/`, `internal/common/`, `internal/schedule/`) do **zero** logging by design — they have no controller-runtime dependency and return errors only. Keep it that way.

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

```text
type(scope): description
type: description
```

Scope is optional but encouraged when the change is scoped to a single area. Any scope is accepted, but prefer short, recognizable repository areas such as `api`, `controller`, `helm`, `ci`, `docs`, or `deps`. Keep titles concise: aim for 50 characters when practical, and avoid exceeding 72 characters because GitHub wraps longer titles in common views.

CI validates this on every PR via the `PR / Validate PR title` check.

**Allowed types** are defined in [`.github/workflows/pr.yaml`](https://github.com/apache/superset-kubernetes-operator/blob/main/.github/workflows/pr.yaml) (the `types` input to the semantic-PR action) and follow the conventional-commit set. Check that file rather than copying the list, which would go stale.

**Examples:**

```text
feat(api): add tolerations field to PodTemplate
fix(controller): handle nil deployment template in scaling reconciler
docs: add valkey configuration examples to user guide
chore(deps): bump controller-runtime to v0.20.0
```

### Description

Every PR must include a **Summary** section with at least one paragraph explaining what the change does and why. A reviewer should understand the motivation and scope from the summary alone, without reading the diff.

Use the optional **Details** section for implementation notes, design decisions, alternatives considered, or migration steps.

The PR template (`PULL_REQUEST_TEMPLATE.md`) pre-fills these sections.

### Changelog entry

Add a bullet under `## Unreleased` in
[`docs/reference/releases.md`](../reference/releases.md)
for noteworthy changes — new features, new CRD fields, behavior changes, breaking changes, deprecations, and critical bug fixes. Skip the entry for routine work that a user wouldn't care about: dependency bumps, CI tweaks, internal refactors, test-only changes, and documentation-only updates.

Group entries under `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, or `Security` (per [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)); create the subheading on first use. These are the only subheadings — Keep a Changelog has no dedicated "Breaking" section. Lead each bullet with the user-facing effect, not the implementation.

Mark a breaking change with a bold `**Breaking:**` prefix under the most fitting category (usually `Changed` or `Removed`) and state the required migration in the same bullet. The `Added` section and the `**Breaking:**` markers are what operators scan first when deciding whether — and how carefully — to upgrade, so keep both accurate:

```markdown
## Unreleased

### Added
- New `webServer.gunicorn.keepAlive` field for tuning Gunicorn keepalive timeouts.

### Changed
- **Breaking:** renamed `spec.oldField` to `spec.newField`; rename it in
  existing Superset resources before upgrading.

### Fixed
- Lifecycle `migrate` task no longer retries indefinitely when the metastore
  rejects credentials; the parent surfaces an `AuthenticationFailed` reason.
```

The release manager does a final review pass over `## Unreleased` before tagging — see [Releasing](releasing.md#reviewing-the-changelog) — so it's fine to err on the side of including an entry; missing or duplicate ones can be cleaned up there.

### Code coverage

CI uploads test coverage to [Codecov](https://codecov.io) on every PR. The Codecov bot posts a comment showing:

- **Patch coverage** — what percentage of new/changed lines are covered by tests
- **Project coverage delta** — how overall coverage changed
