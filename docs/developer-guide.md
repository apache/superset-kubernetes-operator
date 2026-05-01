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

# Developer Guide

This guide covers everything you need to contribute to the Superset Kubernetes Operator.

## Development Setup

This section walks through setting up a local development environment with
Kind, PostgreSQL, and Valkey. By the end you will have a working Superset
instance running on your laptop.

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker (or Podman) | 20+ | [docs.docker.com](https://docs.docker.com/get-docker/) |
| kubectl | 1.27+ | [kubernetes.io](https://kubernetes.io/docs/tasks/tools/) |
| Kind | 0.20+ | `go install sigs.k8s.io/kind@latest` or `brew install kind` |
| Go | 1.26+ | [go.dev](https://go.dev/dl/) |

### 1. Create a Kind cluster

```bash
kind create cluster --name superset
```

### 2. Deploy PostgreSQL

```bash
kubectl create deployment postgres --image=postgres:16 \
  --port=5432 \
  -- sh -c 'exec postgres'
kubectl set env deployment/postgres \
  POSTGRES_USER=superset \
  POSTGRES_PASSWORD=superset \
  POSTGRES_DB=superset
kubectl expose deployment postgres --port=5432
```

Wait for the pod to be ready:

```bash
kubectl wait --for=condition=available deployment/postgres --timeout=60s
```

### 3. Deploy Valkey

```bash
kubectl create deployment valkey --image=valkey/valkey:8 --port=6379
kubectl expose deployment valkey --port=6379
```

### 4. Run the operator

There are two ways to run the operator:

**Option A: Run locally (outside the cluster)**

```bash
make run
```

This compiles and runs the operator process on your machine, connecting to the Kind cluster via your kubeconfig. Leave this terminal open — it streams reconciliation logs.

**Option B: Deploy in-cluster**

```bash
make docker-build IMG=superset-operator:dev
kind load docker-image superset-operator:dev --name superset
make deploy IMG=superset-operator:dev
```

This builds the operator image, loads it into Kind, and deploys it as a Deployment with all RBAC and CRDs. View logs with:

```bash
kubectl logs -n superset-operator-system deployment/superset-operator-controller-manager -f
```

`make deploy` is a superset of `make install` — it installs CRDs, RBAC, and the operator Deployment in one step.

### 5. Deploy Superset

```bash
kubectl apply -f config/samples/superset_v1alpha1_superset.yaml
```

The sample manifest deploys a web server in dev mode with init disabled,
pointing at the Postgres instance created above.

### 6. Access Superset

```bash
kubectl port-forward svc/superset-sample-web-server 8088:8088
```

Open [http://localhost:8088](http://localhost:8088).

### 7. Clean up

```bash
kubectl delete superset superset-sample
kind delete cluster --name superset
```

---

## Make Commands

| Command | Description |
|---|---|
| `make run` | Run the operator locally (connects to cluster via kubeconfig) |
| `make deploy IMG=<image>` | Deploy operator in-cluster (CRDs + RBAC + Deployment — superset of `make install`) |
| `make install` | Install CRDs only |
| `make undeploy` | Remove the in-cluster operator deployment |
| `make uninstall` | Remove CRDs only |
| `make manifests` | Regenerate CRD YAML and RBAC from kubebuilder markers |
| `make generate` | Regenerate DeepCopy methods |
| `make build` | Build the operator binary |
| `make docker-build IMG=<image>` | Build container image for the local platform |
| `make docker-buildx IMG=<image>` | Build and push multi-platform image (linux/arm64, linux/amd64) |
| `make test` | Run all tests (unit + integration + e2e) |
| `make test-unit` | Run unit tests (no envtest or cluster required) |
| `make test-integration` | Run integration tests (requires envtest) |
| `make test-e2e` | Run e2e tests (requires Kind cluster) |
| `make lint` | Run golangci-lint |
| `make hooks` | Configure git to use `.githooks/` for pre-commit hooks |
| `make helm` | Sync CRDs into Helm chart and package it |
| `make helm-lint` | Lint the Helm chart |
| `make docs-serve` | Serve docs locally at http://localhost:8000 |
| `make docs-build` | Build docs site (runs in CI with `--strict`) |
| `make docs-api` | Regenerate API reference from Go types |
| `make check-license` | Check Apache license headers (requires Java) |
| `make clean` | Remove build artifacts, downloaded tools, and test cache |

---

## Pre-Commit Hooks

The project includes a lightweight git hook at `.githooks/pre-commit` that runs `make lint` before each commit. No external tools required — it uses `golangci-lint`, which covers formatting (`gofmt`, `goimports`), `go vet`, and all configured linters.

### Setup

```sh
make hooks
```

This sets `core.hooksPath` to `.githooks/` for this repository. To bypass the hook for a specific commit, use `git commit --no-verify`.

---

## Documentation

The docs are built with [mkdocs-material](https://squidfunk.github.io/mkdocs-material/). To set up and preview locally:

```sh
virtualenv venv
source venv/bin/activate
pip install -r docs-requirements.txt
make docs-serve
```

This starts a live-reloading server at `http://localhost:8000`. Edit Markdown files in `docs/` and changes appear instantly.

To verify the docs build cleanly (same check that runs in CI):

```sh
make docs-build
```

## Architecture

See [Architecture](architecture.md) for the structural overview (CRD hierarchy,
configuration model, config rendering) and [Internals](internals.md) for runtime
behavior (reconciliation lifecycle, init pod state machine, retry semantics).
Key points:

- **Two-tier CRD**: Parent `Superset` resolves shared spec (top-level + per-component) into flat child CRDs
- **7 child types**: SupersetInit, SupersetWebServer, SupersetCeleryWorker, SupersetCeleryBeat, SupersetCeleryFlower, SupersetWebsocketServer, SupersetMcpServer
- **3 pure Go packages**: `internal/resolution/` (spec flattening), `internal/config/` (Python rendering), `internal/common/` (shared types)
- **Parent resolves, children execute**: All layering logic in the parent controller; child CRs are fully flattened

The parent controller orchestrates all reconciliation:

- `reconcileInit()` — SupersetInit CR → bare Pod (db upgrade + superset init) + ConfigMap
- `reconcileServiceAccount()` — ServiceAccount
- `reconcileComponent()` — Table-driven loop over `componentDescriptors`: resolves each enabled component into a flat child CR (WebServer, CeleryWorker, CeleryBeat, CeleryFlower, WebsocketServer, McpServer). Each child controller then reconciles its own sub-resources (Deployment, ConfigMap, Service, HPA, PDB).
- `reconcileNetworking()` — HTTPRoute or Ingress
- `reconcileMonitoring()` — ServiceMonitor (unstructured)
- `reconcileNetworkPolicies()` — NetworkPolicy per component
- `updateStatus()` — Aggregate child statuses into phase

---

## Testing Philosophy

We use a **pyramid testing strategy** where the vast majority of logic is
covered by fast, deterministic unit tests. Integration and e2e tests are
reserved for verifying the system works end-to-end, not for testing
individual behaviors.

### Test granularity

- **Prefer broad happy-path tests** that cover critical assertions in a single test function. For example, one comprehensive test that creates all components and verifies config, env vars, and status is better than 10 separate tests each checking one field.
- **Use granular tests only for complex utilities** with many edge cases (e.g., merge functions, backoff calculation, condition management). These benefit from table-driven tests covering boundary conditions.
- **Every assertion should protect against a plausible regression.** If you can't name the scenario where removing it would let a bug through, it doesn't belong. Avoid adding narrow standalone tests when the assertion fits naturally in an existing comprehensive test.
- **When fixing a regression, always add a test that would have caught it.** Regressions that broke silently indicate a gap in test coverage — close the gap as part of the fix.
- **Use subtests (`t.Run`)** to group related scenarios within a single test function instead of creating separate top-level tests.

### Test Pyramid

- **Unit tests** — Fast, deterministic, fake client or pure functions. Cover all business logic.
- **Integration tests** — Minimal envtest tests (real API server). Verify CRD registration, CEL validation, reconciler lifecycle.
- **E2E tests** — 1-2 comprehensive scenarios on a Kind cluster. Verify full operator lifecycle.

### Why unit tests over integration tests

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

### What goes where

**Unit tests** (`*_test.go` with `testing` package + `fake.NewClientBuilder`):
- Resolution engine: merge semantics, override behavior, beat singleton
- Config rendering: per-component Python output, metastore URI, config
- Parent controller: reconciliation logic, child CR creation/deletion,
  config env var injection, image overrides, status aggregation, suspend
- Child controller helpers: ConfigMap, Deployment, Service reconciliation
- InitPod: pod spec building, retention policy, backoff calculation

**Integration tests** (Ginkgo + envtest):
- CRD schema validation works (kubebuilder markers produce correct OpenAPI)
- CEL validation rules reject invalid CRs
- Controller manager starts and registers all controllers

**E2E tests** (Ginkgo + Kind cluster):
- Operator health: controller pod running, metrics endpoint serving
- CR lifecycle: apply Superset CR → child CRs created → Deployments + ConfigMaps exist → status populated
- Multi-component: all component types reconciled with correct sub-resources

### Writing a new unit test

Use the standard pattern from `reconcile_test.go`:

```go
func TestReconcile_MyScenario(t *testing.T) {
    scheme := testScheme(t)

    superset := &supersetv1alpha1.Superset{
        ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
        Spec: supersetv1alpha1.SupersetSpec{
            Image:       supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
            Environment: strPtr("dev"),
            SecretKey:   strPtr("test-secret-key"),
            Init: &supersetv1alpha1.InitSpec{
                Disabled: boolPtr(true),
            },
        },
    }

    c := fake.NewClientBuilder().
        WithScheme(scheme).
        WithObjects(superset).
        WithStatusSubresource(superset, &supersetv1alpha1.SupersetWebServer{}).
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
- Use `boolPtr(true)` for `Init.Disabled` to bypass init pod lifecycle
- Register all types in the scheme via `testScheme(t)` helper
- Use `WithStatusSubresource` for objects whose status is updated
- Assert on child CR fields, not on intermediate state

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

- **Use shared helpers** from `child_reconciler.go` (`reconcileChildConfigMap`, `reconcileChildDeployment`, `reconcileChildService`, `reconcileScaling`, `updateChildStatus`, `buildChecksumAnnotations`)
- **Use `componentLabels(component, instance)`** for consistent label generation
- **Stamp `parentLabels(parentName)` on all parent-owned resources** (ServiceAccount, Ingress, HTTPRoute, ServiceMonitor) — this enables label-based cleanup
- **Use `controllerutil.CreateOrUpdate`** for idempotent reconciliation
- **Set OwnerReferences** via `controllerutil.SetControllerReference`
- **Record events** via `r.Recorder.Eventf()` for errors and state changes
- **Add Go doc comments** to all exported types — they become CRD descriptions
- **Run `make manifests generate`** after any change to `api/v1alpha1/`
- **Write unit tests first** — every new feature should have tests before integration
- **Test with fake client** — only use envtest when you genuinely need a real API server

### Don't

- **Don't hardcode child CR names** — always use `componentDescriptor.childName()` to resolve names. Child CRs use the parent name (differentiated by Kind); sub-resources are named `{parentName}-{componentType}`.
- **Don't fetch resources by name for cleanup** — use `deleteByLabels` with `parentLabels(parentName)` (parent-owned resources) or `componentLabels` (child-owned resources) to discover and clean up. Name-based `CreateOrUpdate` and status reads are fine — those address resources whose names the operator controls.
- **Don't hardcode commands/ports** — use `DeploymentConfig` defaults
- **Don't duplicate controller logic** — use `child_reconciler.go` helpers
- **Don't add fields without doc comments** — they become CRD descriptions
- **Don't use `bool` with `omitempty`** — use `*bool` to distinguish false from unset
- **Don't write integration tests for unit-testable logic** — use fake client
- **Don't put secret values in ConfigMaps** — in prod mode, secrets are mounted via env vars from Kubernetes Secrets
- **Don't use admission webhooks for validation** — use [CEL](https://kubernetes.io/docs/reference/using-api/cel/) (`x-kubernetes-validations`) on CRD types instead. Webhooks add operational complexity (cert-manager dependency, TLS setup) and are not installed by default. All validation rules should be expressed as CEL where feasible.
- **Cross-reference security docs** — any change that affects trust boundaries, secret handling, RBAC, CRD validation, or the operator's attack surface must be reflected in [`docs/security.md`](security.md). Review the threat model, design decisions, and in-scope/out-of-scope sections to ensure they stay accurate.

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

1. Create `api/v1alpha1/supersetnewcomponent_types.go`:
   - Define flat spec type embedding `FlatComponentSpec`
   - Add `Config`/checksum fields (if Python) or just `Service` (if not)
   - Register in `init()` via `SchemeBuilder.Register`

2. Add component spec to parent in `superset_types.go`

3. Create `internal/controller/supersetnewcomponent_controller.go`:
   - Follow the pattern in `supersetceleryflower_controller.go`
   - Define a `DeploymentConfig` with component-specific defaults
   - Use shared helpers from `child_reconciler.go`
   - Add RBAC markers

4. Register in parent controller:
   - Add `reconcileXxx` method and call in `Reconcile()`
   - Add `convertXxxComponent` function
   - Add status check in `updateStatus()`
   - Add `Owns()` in `SetupWithManager()`

5. Register in `cmd/main.go`
6. Run `make manifests generate`
7. Add sample CR and unit tests

## Package Structure

```
internal/
├── resolution/       # Pure Go — spec flattening engine
│                     # Zero controller-runtime deps, fully unit-testable
│                     # MergeMaps, MergeEnvVars, ResolveChildSpec(), etc.
├── config/           # Pure Go — Python config renderer
│                     # Per-component rendering, metastore URI,
│                     # config appending
├── common/           # Shared types (ComponentType, Ptr helper)
└── controller/       # controller-runtime — reconcilers
                      # Parent controller, 7 child controllers,
                      # InitPod lifecycle, status, scaling, networking
```

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `api/v1alpha1/shared_types.go` | ImageSpec, MetastoreSpec, DeploymentTemplate, PodTemplate, ContainerTemplate, FlatComponentSpec |
| `api/v1alpha1/superset_types.go` | Parent SupersetSpec, component specs, InitSpec, CEL validation rules, status |
| `internal/common/types.go` | Shared ComponentType, Ptr helper |
| `internal/resolution/resolver.go` | ResolveChildSpec — core flattening engine |
| `internal/config/renderer.go` | RenderConfig — per-component Python generation |
| `internal/controller/child_reconciler.go` | Shared helpers for all child controllers |
| `internal/controller/superset_controller.go` | Parent reconciler (orchestrates everything) |
| `internal/controller/deployment_builder.go` | Deployment construction from flat spec |
| `internal/controller/initpod.go` | InitPod lifecycle manager |
| `internal/controller/reconcile_test.go` | Parent controller unit tests (fake client) |

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

## Pull Requests

### Title format

PR titles must follow the conventional commits format:

```
type(scope): description
type: description
```

Scope is optional but encouraged when the change is scoped to a single area.

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

### Code coverage

CI uploads test coverage to [Codecov](https://codecov.io) on every PR. The
Codecov bot posts a comment showing:

- **Patch coverage** — what percentage of new/changed lines are covered by tests
- **Project coverage delta** — how overall coverage changed

---

## CI & Supply Chain

All CI workflows live in `.github/workflows/`. When adding or modifying
workflows, follow these conventions:

### GitHub Actions pinning

Pin all `uses:` references by **full commit SHA**, not version tag. Add a
version comment for readability:

```yaml
- uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683 # v4.2.2
```

**Why**: Version tags are mutable — a compromised upstream can retag a
malicious commit to an existing version. SHA pinning makes the reference
immutable. Dependabot automatically proposes SHA updates when new versions
are released, so maintenance overhead is minimal.

### Tool binary pinning

When downloading binaries in CI (e.g., `kind`, `helm`), always:

1. Pin to a specific version (never `latest`)
2. Verify with a SHA256 checksum

### Workflow permissions

Every workflow must declare `permissions:` at the top level. Default to the
minimum required:

```yaml
permissions:
  contents: read
```

Only add broader permissions when needed (e.g., `packages: write` for image
publishing, `security-events: write` for CodeQL).

### Renovate

`renovate.json` is configured to propose weekly dependency updates for Go
modules and GitHub Actions. A **7-day minimum release age** is enforced — Renovate
will not propose a version until it has been published for at least 7 days. This
reduces the risk of adopting a compromised release before the community detects
it. Review and merge these PRs promptly to stay current on security patches.

---

## Releasing

### Versioning

The project follows [Semantic Versioning](https://semver.org/). Two versions are tracked:

| Version | Location | Purpose |
|---------|----------|---------|
| **Operator version** | `VERSION` in `Makefile` | Operator image tag. Single source of truth. |
| **Chart version** | `version` in `charts/superset-operator/Chart.yaml` | Helm chart version. Can diverge for chart-only fixes. |

The Chart.yaml `appVersion` is injected from the Makefile `VERSION` at package time
(`make helm` passes `--app-version`), so it does not need to be updated manually.

While the project is pre-1.0, all versions use `0.x.y` to signal instability per semver.

### Release checklist

The release workflow (`.github/workflows/release.yml`) builds multi-platform
images and pushes them to GHCR. It runs automatically on pushes to `main`
(producing `dev` and `sha-<short>` tags) and on version tags (producing semver
tags). It can also be triggered manually via `workflow_dispatch`.

**Image tagging:**

| Trigger | Image tag | Example |
|---|---|---|
| Push to `main` | `dev` + `sha-<short-sha>` | `dev`, `sha-abc1234` |
| RC tag | Semver without `v` prefix | `0.1.0-rc1` |
| Release tag | Semver without `v` prefix + `latest` | `0.1.0`, `latest` |

See [Downloads](downloads.md) for full details on published images and
registries.

**Creating a release candidate:**

The `scripts/release-rc.sh` script automates the full RC preparation: creates a
release branch, bumps the operator version, regenerates manifests, runs tests
and linting, commits, and tags.

```sh
# First RC for 0.2.0 — creates release/0.2.0 branch and v0.2.0-rc1 tag
scripts/release-rc.sh 0.2.0

# Optionally bump the Helm chart version too
scripts/release-rc.sh 0.2.0 --chart-version 0.2.0

# Push branch + tag to trigger the release workflow
git push origin release/0.2.0 v0.2.0-rc1
```

Running the script again from the same release branch increments the RC number
automatically (rc1, rc2, ...).

**Finalizing a release:**

After the ASF vote passes, the `scripts/release-finalize.sh` script tags the final
release on the release branch:

```sh
# From the release/0.2.0 branch
scripts/release-finalize.sh 0.2.0

# Push the tag to trigger the release workflow
git push origin v0.2.0
```

The release workflow pushes the `0.2.0` and `latest` images to GHCR.