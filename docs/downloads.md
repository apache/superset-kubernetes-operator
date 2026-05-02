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
    There are no official releases yet. Only `dev` and `sha-<short>` image tags
    and the `0.0.0-dev` Helm chart are currently available.

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
| `sha-<short>` | `sha-abc1234` | Immutable tag for a specific commit. |
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
