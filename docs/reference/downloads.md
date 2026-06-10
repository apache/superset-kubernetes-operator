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

# Downloads

!!! note
    The **signed source archive is the official Apache release**. The operator
    image and Helm chart are convenience binaries published to the GitHub
    Container Registry: `dev` / `sha-<commit>` tags and the `0.0.0-dev` chart on
    every merge to `main`, `<version>-rc<N>` tags for release candidates, and
    `<version>` + `latest` for final releases.

## Source Release

Per the [ASF Release Policy](https://www.apache.org/legal/release-policy.html),
the **signed source archive is the official Apache release**; the operator image
and Helm chart below are convenience binaries built from it.

Signed source archives, detached PGP signatures, and SHA-512 checksums are
published to the ASF distribution site under
`https://downloads.apache.org/superset/kubernetes-operator-<version>/`, for
example `kubernetes-operator-0.1.0/`:

| File | Description |
|------|-------------|
| `apache-superset-kubernetes-operator-<version>.tar.gz` | Source archive (a `git archive` of the release tag). |
| `apache-superset-kubernetes-operator-<version>.tar.gz.asc` | Detached PGP signature. |
| `apache-superset-kubernetes-operator-<version>.tar.gz.sha512` | SHA-512 checksum. |

### Verifying the Source Release

```bash
VERSION=0.1.0
BASE=https://downloads.apache.org/superset/kubernetes-operator-${VERSION}
curl -O ${BASE}/apache-superset-kubernetes-operator-${VERSION}.tar.gz{,.asc,.sha512}
curl -O https://downloads.apache.org/superset/KEYS
gpg --import KEYS
gpg --verify apache-superset-kubernetes-operator-${VERSION}.tar.gz{.asc,}
shasum -a 512 -c apache-superset-kubernetes-operator-${VERSION}.tar.gz.sha512
```

## Operator Image

Multi-architecture operator images (`linux/amd64`, `linux/arm64`) are published
to the GitHub Container Registry on every merge to `main` and on version tags:

```
ghcr.io/apache/superset-kubernetes-operator
```

### Pull

```bash
docker pull ghcr.io/apache/superset-kubernetes-operator:dev
```

### Image Tags

| Tag | Example | Description |
|-----|---------|-------------|
| `dev` | `dev` | Floating tag tracking the latest commit on `main`. Rebuilt on every merge. |
| `sha-<commit>` | `sha-1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b` | Immutable tag for a specific commit (full Git SHA). |
| `<version>` | `0.1.0` | Semver release. Published when a version tag is pushed. |
| `<version>-rc<N>` | `0.1.0-rc1` | Release candidate. |
| `latest` | `latest` | Points to the highest stable (non-prerelease) release. |

### Choosing a Tag

- **Production**: Pin to a semver tag (e.g., `0.1.0`) or a `sha-` tag for
  full reproducibility.
- **Testing pre-release features**: Use an RC tag (e.g., `0.1.0-rc1`) or `dev`.
- **Avoid** using `latest` or `dev` in production — these are mutable and will
  change without notice.

### Image Signing

All images are signed with [cosign](https://docs.sigstore.dev/cosign/overview/)
using keyless OIDC signing via GitHub Actions. To verify:

```bash
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp 'github\.com/apache/superset-kubernetes-operator' \
  ghcr.io/apache/superset-kubernetes-operator:<tag>
```

## Helm Chart

The Helm chart is published as an OCI artifact to the GitHub Container Registry
on every merge to `main` and on version tags.

### Install from OCI

```bash
helm install superset-operator \
  oci://ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator \
  --version <version> \
  --namespace superset-operator-system \
  --create-namespace
```

Replace `<version>` with a chart version from the table below. For the latest
development build, use `0.0.0-dev`.

### Chart Versions

| Tag | Description |
|-----|-------------|
| `0.0.0-dev` | Floating tag tracking the latest commit on `main`. |
| `<version>` (e.g., `0.1.0`) | Semver release. |
| `<version>-rc<N>` (e.g., `0.1.0-rc1`) | Release candidate. |

### Install from Source

For development or to test unreleased changes, install directly from a source
checkout:

```bash
helm install superset-operator charts/superset-operator \
  --namespace superset-operator-system \
  --create-namespace
```
