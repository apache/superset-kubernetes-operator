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

!!! warning "Work in progress"
    Container image and Helm chart publishing are being set up. Registry URLs
    and pull commands will be added here once the infrastructure is confirmed.

## Docker Images

Multi-architecture operator images (`linux/amd64`, `linux/arm64`) will be
published on every merge to `main` and on version tags.

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
using keyless OIDC signing via GitHub Actions.

## Helm Chart

The Helm chart is currently installed from a source checkout. See the
[Installation](installation.md) guide for instructions.

A hosted Helm repository (via GitHub Pages) and OCI registry distribution are
planned for a future release.
