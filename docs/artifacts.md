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

## Docker Images

Operator images are published to the GitHub Container Registry (GHCR) and
available via a Scarf analytics proxy that provides download metrics.

### Registries

| Registry | URL | Notes |
|----------|-----|-------|
| **GHCR (direct)** | `ghcr.io/apache/superset-kubernetes-operator` | Primary registry |
| **Scarf proxy** | `apachesuperset.docker.scarf.sh/apache/superset-kubernetes-operator` | Default in Helm chart. Transparently redirects to GHCR. |

Both URLs serve the same images. The Scarf proxy is used by default in the
Helm chart and kustomize manifests to provide anonymous download metrics.

### Architectures

All images are multi-architecture manifests supporting:

- `linux/amd64`
- `linux/arm64`

### Image Tags

| Tag | Example | Description |
|-----|---------|-------------|
| `dev` | `dev` | Floating tag tracking the latest commit on `main`. Rebuilt on every merge. |
| `sha-<short>` | `sha-abc1234` | Immutable tag for a specific commit. |
| `<version>` | `0.2.0` | Semver release (no `v` prefix). Published when a version tag is pushed. |
| `<version>-rc<N>` | `0.2.0-rc1` | Release candidate. Does not receive the `latest` tag. |
| `latest` | `latest` | Points to the highest stable (non-prerelease) release. |

### Choosing a Tag

- **Production**: Pin to a semver tag (e.g., `0.2.0`) or a `sha-` tag for
  full reproducibility.
- **Testing pre-release features**: Use an RC tag (e.g., `0.2.0-rc1`) or `dev`.
- **Avoid** using `latest` or `dev` in production — these are mutable and will
  change without notice.

### Image Signing

All images are signed with [cosign](https://docs.sigstore.dev/cosign/overview/)
using keyless OIDC signing via GitHub Actions. To verify an image:

```bash
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp 'github\.com/apache/superset-kubernetes-operator' \
  ghcr.io/apache/superset-kubernetes-operator:<tag>
```

### Pulling an Image

```bash
# Via GHCR directly
docker pull ghcr.io/apache/superset-kubernetes-operator:dev

# Via Scarf proxy (default in Helm chart)
docker pull apachesuperset.docker.scarf.sh/apache/superset-kubernetes-operator:dev
```

## Helm Chart

The Helm chart is currently installed from a source checkout. See the
[Installation](installation.md) guide for instructions.

A hosted Helm repository (via GitHub Pages) and OCI registry distribution are
planned for a future release.
