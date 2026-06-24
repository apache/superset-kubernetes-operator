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

# Releases

This page tracks notable changes in Apache Superset Kubernetes Operator
releases.

## Unreleased

## 0.1.1 - 2026-06-29

### Fixed

- Honor HPA-managed replica counts: the operator no longer overwrites the
  replica count on Deployments whose scaling is owned by a
  HorizontalPodAutoscaler ([#152](https://github.com/apache/superset-kubernetes-operator/pull/152), [@pashtet04](https://github.com/pashtet04)).

## 0.1.0 - 2026-06-10

### Added

- Initial release ([@villebro](https://github.com/villebro)).

### Known limitations

- **Websocket server is experimental.** The websocket server is not yet well
  supported and is pending security hardening; it is not recommended for
  production use.
- **Downgrade protection requires semver image tags.** Downgrades are detected
  and blocked only when both image tags are valid semver. Non-semver tags
  (`latest`, date stamps, digest pins) cannot be ordered, so the operator emits a
  `VersionComparisonSkipped` warning and proceeds without blocking. See
  [Lifecycle](../user-guide/lifecycle.md).
- **Task failure messages may include credential fragments.** Lifecycle task
  failure output is truncated into `status` and could contain fragments of task
  stdout, including credentials. See [security.md](security.md).
