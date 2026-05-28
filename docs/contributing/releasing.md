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

# Releasing

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

Use `scripts/install-helm.sh` for CI Helm installs so the pinned Helm version
and checksum stay in one place.

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

## Versioning

The project follows [Semantic Versioning](https://semver.org/). Two versions are tracked:

| Version | Location | Purpose |
|---------|----------|---------|
| **Operator version** | `VERSION` in `Makefile` | Operator image tag. Single source of truth. |
| **Chart version** | `version` in `charts/superset-operator/Chart.yaml` | Helm chart version. Can diverge for chart-only fixes. |

The Chart.yaml `appVersion` is injected from the Makefile `VERSION` at package time
(`make helm` passes `--app-version`), so it does not need to be updated manually.

While the project is pre-1.0, all versions use `0.x.y` to signal instability per semver.

## Release Checklist

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

See [Downloads](../reference/downloads.md) for full details on published images
and registries.

Before creating the first RC for a minor release, run or verify:

- `make codegen` leaves no diff
- `make lint`
- `make test`
- `make helm-lint`
- `make docs-build`
- `make check-license`
- `make test-e2e` on a working Kind or equivalent Kubernetes cluster
- The release workflow is using pinned/checksum-verified tool downloads

## Creating a Release Candidate

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

## Finalizing a Release

After the ASF vote passes, the `scripts/release-finalize.sh` script tags the final
release on the release branch:

```sh
# From the release/0.2.0 branch
scripts/release-finalize.sh 0.2.0

# Push the tag to trigger the release workflow
git push origin v0.2.0
```

The release workflow pushes the `0.2.0` and `latest` images to GHCR.
