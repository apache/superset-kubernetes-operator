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

## Reviewing the Changelog

Contributors add bullets to `## [Unreleased]` in `CHANGELOG.md` as PRs land
(see the
[changelog convention](development-guidelines.md#changelog-entry)). Before
tagging the RC, the release manager does one review pass to make sure the
section accurately reflects the release:

1. Skim `git log v<previous>..HEAD` for noteworthy changes that nobody added
   an entry for, and add them. Skim for changes that were added but aren't
   actually noteworthy (typo fixes, internal refactors that snuck in), and
   drop them.
2. Merge bullets that describe the same user-visible change — separate PRs
   often touch on a single feature.
3. Make sure each bullet leads with the user-facing effect and reads in a
   consistent voice.
4. Group bullets under the standard Keep a Changelog subheadings — `Added`,
   `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security` — dropping any
   that end up empty.
5. Rename the section header from `## [Unreleased]` to
   `## [<version>] — <YYYY-MM-DD>` and add a fresh empty `## [Unreleased]`
   above it. Update the comparison links at the bottom of the file.

Two flows depending on whether the release branch already exists:

- **First RC of a minor release.** The `release/<version>` branch does not
  exist yet, and `release-rc.sh` will create it from `main`. Land the
  reviewed `CHANGELOG.md` on `main` first (a normal PR), then run
  `release-rc.sh` so the new branch and RC tag pick it up.
- **Subsequent RCs (`rc2`, `rc3`, …).** The `release/<version>` branch
  already exists. Commit the `CHANGELOG.md` polish on that branch directly,
  then run `release-rc.sh` to bump the RC number.

## Creating a Release Candidate

The `scripts/release-rc.sh` script automates the full RC preparation: creates
the release branch (first RC only), bumps the operator and Helm chart
versions, regenerates manifests, runs the lint/license/test/docs/helm-lint
checks, commits, and tags.

```sh
# First RC for 0.2.0 — creates release/0.2.0 branch and v0.2.0-rc1 tag.
# Chart version defaults to the operator version unless overridden.
scripts/release-rc.sh 0.2.0

# Pin the Helm chart to a different version (rare; only for chart-only fixes).
scripts/release-rc.sh 0.2.0 --chart-version 0.2.1

# Push branch + tag to trigger the release workflow
git push origin release/0.2.0 v0.2.0-rc1
```

Running the script again from the same release branch increments the RC
number automatically (rc1, rc2, ...).

## ASF Source Release Artifacts

Per the [ASF Release Policy](https://www.apache.org/legal/release-policy.html),
the **signed source archive is the official release**. The OCI images and
Helm chart published to GHCR by the release workflow are convenience binaries
and cannot be voted on in isolation.

A release candidate therefore needs three artifacts staged on
[dist.apache.org](https://dist.apache.org/repos/dist/dev/superset/):

| Artifact | Filename pattern | Notes |
|---|---|---|
| Source archive | `apache-superset-kubernetes-operator-<version>-source.tar.gz` | A `git archive` of the RC tag, prefixed with the project directory. |
| Detached PGP signature | `apache-superset-kubernetes-operator-<version>-source.tar.gz.asc` | Generated with a key in [`KEYS`](https://dist.apache.org/repos/dist/release/superset/KEYS). |
| SHA-512 checksum | `apache-superset-kubernetes-operator-<version>-source.tar.gz.sha512` | `shasum -a 512` output, with a bare filename so verifiers can run `shasum -c` after a plain download. |

### Pre-requisites for the release manager

1. Be a Superset PMC member (binding vote), or coordinate with one if you are
   a committer driving the release.
2. Have a PGP key registered in
   `https://dist.apache.org/repos/dist/release/superset/KEYS` and uploaded to
   the public keyservers. To add a new key, append the output of
   `gpg --list-sigs <fingerprint> && gpg --armor --export <fingerprint>` to
   `KEYS` in the SVN checkout below and commit.
3. Have `svn` checkouts of both ASF dist locations:

   ```sh
   svn co https://dist.apache.org/repos/dist/dev/superset/ ~/asf/dev-superset
   svn co https://dist.apache.org/repos/dist/release/superset/ ~/asf/release-superset
   ```

### Producing the source tarball

`scripts/release-source.sh` wraps `git archive`, `gpg`, and `shasum` and
self-verifies before exiting:

```sh
scripts/release-source.sh 0.2.0 --rc 1
# → dist/apache-superset-kubernetes-operator-0.2.0-rc1-source.tar.gz{,.asc,.sha512}
```

Pass `--gpg-key <id>` to pick a non-default signing key, and `--out-dir
<path>` to write the artifacts somewhere other than `./dist/`.

> **Why a script, not raw `git archive` + `gpg` + `shasum`.** The script
> avoids a small set of subtle errors that are easy to make manually:
>
> - `shasum -a 512 path/to/file` writes the path into the checksum file. If
>   the file is later renamed (RC → final), `shasum -c` fails. The script
>   always runs `shasum` from the file's own directory so the checksum line
>   contains a bare filename.
> - Detached PGP signatures verify the tarball **contents**, not the
>   filename. RC → final promotion only needs to copy the `.asc`; no
>   re-signing is required. The `--finalize` mode below relies on this.

### Staging the artifacts

```sh
cd ~/asf/dev-superset
mkdir kubernetes-operator-${VERSION}-rc${RC}
cp /path/to/dist/apache-superset-kubernetes-operator-${VERSION}-rc${RC}-source.tar.gz{,.asc,.sha512} \
   kubernetes-operator-${VERSION}-rc${RC}/
svn add kubernetes-operator-${VERSION}-rc${RC}
svn commit -m "Stage Superset Kubernetes Operator ${VERSION}-rc${RC}"
```

After the commit lands, the artifacts appear at
`https://dist.apache.org/repos/dist/dev/superset/kubernetes-operator-<version>-rc<n>/`.

### Vote email template

Send the vote thread to `dev@superset.apache.org` with `[VOTE]` in the
subject. The thread must stay open for **at least 72 hours** and pass with at
least three `+1` votes from PMC members and no `-1` votes (per
[ASF voting rules](https://www.apache.org/foundation/voting.html#ReleaseVotes)).

```text
Subject: [VOTE] Release Apache Superset Kubernetes Operator <version>-rc<n>

Hello,

This is a vote to release Apache Superset Kubernetes Operator <version> based
on candidate rc<n>.

The release candidate artifacts are staged at:
  https://dist.apache.org/repos/dist/dev/superset/kubernetes-operator-<version>-rc<n>/

The git tag is:
  https://github.com/apache/superset-kubernetes-operator/releases/tag/v<version>-rc<n>

PGP keys used for signing are listed in:
  https://dist.apache.org/repos/dist/release/superset/KEYS

To verify the candidate (Go 1.26+, Helm, and Apache Rat must be installed
locally; `make` will fetch additional Go modules and tools on first use):

  curl -O https://dist.apache.org/repos/dist/dev/superset/kubernetes-operator-<version>-rc<n>/apache-superset-kubernetes-operator-<version>-rc<n>-source.tar.gz{,.asc,.sha512}
  curl -O https://dist.apache.org/repos/dist/release/superset/KEYS
  gpg --import KEYS
  gpg --verify apache-superset-kubernetes-operator-<version>-rc<n>-source.tar.gz{.asc,}
  shasum -a 512 -c apache-superset-kubernetes-operator-<version>-rc<n>-source.tar.gz.sha512
  tar -xzf apache-superset-kubernetes-operator-<version>-rc<n>-source.tar.gz
  cd apache-superset-kubernetes-operator-<version>/
  make test-unit helm-lint check-license

Notable changes are documented in:
  https://github.com/apache/superset-kubernetes-operator/blob/v<version>-rc<n>/CHANGELOG.md

The vote will be open for at least 72 hours.

[ ] +1 release this package
[ ]  0 no opinion
[ ] -1 do not release this package because ...

Thanks,
<release manager>
```

After the vote thread closes, post a `[RESULT][VOTE]` summary to the same list
with the tally and the binding/non-binding breakdown.

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

After the binary release workflow finishes, promote the source artifacts.
`release-source.sh --finalize` reuses the staged RC tarball bytes (so the
detached signature stays valid) and regenerates the SHA-512 file under the
final filename:

```sh
scripts/release-source.sh 0.2.0 --finalize \
  --rc-dir ~/asf/dev-superset/kubernetes-operator-0.2.0-rc${RC}
# → dist/apache-superset-kubernetes-operator-0.2.0-source.tar.gz{,.asc,.sha512}

cd ~/asf/release-superset
mkdir kubernetes-operator-0.2.0
cp /path/to/dist/apache-superset-kubernetes-operator-0.2.0-source.tar.gz{,.asc,.sha512} \
   kubernetes-operator-0.2.0/
svn add kubernetes-operator-0.2.0
svn commit -m "Release Apache Superset Kubernetes Operator 0.2.0"

# Clean up the dev/ staging area (and any earlier RCs).
cd ~/asf/dev-superset
svn rm kubernetes-operator-0.2.0-rc*
svn commit -m "Clean up Superset Kubernetes Operator 0.2.0 release candidates"
```

The release announcement should go to `announce@apache.org` and
`dev@superset.apache.org` after the artifacts have propagated to the ASF
mirror network (typically within 24 hours).
