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

This page tracks notable changes in Apache Superset Kubernetes Operator releases.

## Unreleased

### Added

- **Global Async Queries and the realtime websocket transport (stable).** A new `spec.realtime` block brings first-class support for Global Async Queries (`realtime.asyncQueries`) and the realtime websocket push transport (`realtime.webSocket`). The websocket server is now GA: it ships in the official Superset image and launches via an alternate entrypoint, so it inherits `spec.image` and is configured entirely by operator-injected environment variables — no separate websocket image. The operator sets the `GLOBAL_ASYNC_QUERIES` feature flag, wires the coordination backend (`spec.valkey.distributedCoordination`), shares the `WEBSOCKET_JWT_SECRET` between the Flask app and the server, and renders the `superset.tasks.async_queries` Celery import plus the `reap_orphaned_tasks` beat schedule. Requires a Superset image with Global Async Queries on the Global Task Framework (Superset 7.0+, [apache/superset#43407](https://github.com/apache/superset/pull/43407)) ([@villebro](https://github.com/villebro)).
- **Centralized `spec.baseUrl`.** The external, browser-visible base URL now has a single home. The operator derives the websocket URL, the websocket `ALLOWED_ORIGINS` allowlist (defaulting to that one origin to mitigate Cross-Site WebSocket Hijacking), and `WEBDRIVER_BASEURL_USER_FRIENDLY` (Alerts & Reports hyperlinks) from it. The internal `WEBDRIVER_BASEURL` render target is now operator-managed and points at the web-server Service ([@villebro](https://github.com/villebro)).

### Changed

- **Breaking:** the `websocketServer` component is reconfigured for the GA websocket server (previously marked experimental). It now inherits `spec.image` and runs from the official Superset image, so the custom-image requirement and the `websocketServer.config`/`configFrom` (`config.json`) fields are removed — configure the transport through `spec.realtime.webSocket` (shared JWT secret, URL, allowed origins) instead, and run a Superset 7.0+ image. Its readiness probe now targets `/ready` (liveness stays `/health`) ([@villebro](https://github.com/villebro)).
- Kubernetes support now covers the three newest `kind`-published minor versions instead of two. CI tests Kubernetes 1.37, 1.36, and 1.35 natively, with the experimental `next` lane disabled again ([#317](https://github.com/apache/superset-kubernetes-operator/pull/317)).

### Security

- **Breaking:** Celery Flower is no longer published on the Ingress/Gateway surface in `Production` by default. Flower's default command ships without authentication and its dashboard discloses task names and arguments — for Superset, other users' async SQL Lab statements and alert/report payloads. In `Production` (the default) Flower is fanned out onto the end-user host — and its port opened in the built-in NetworkPolicy — only when you explicitly set `celeryFlower.service.gatewayPath`; `Development` and `Staging` continue to publish it by default. To keep exposing Flower in Production, set `celeryFlower.service.gatewayPath` (e.g. `/flower`) after placing authentication in front of it (e.g. `FLOWER_BASIC_AUTH` injected from a Secret via `celeryFlower.podTemplate.container.env`) ([#279](https://github.com/apache/superset-kubernetes-operator/pull/279), [@villebro](https://github.com/villebro)).

## 0.2.0 - 2026-08-11

### Added

- **Seconds and year precision in `cronSchedule`.** Lifecycle task `cronSchedule` fields now accept 6- and 7-field cron expressions in addition to the classic 5-field form — an optional leading seconds field and/or trailing year field (e.g. `*/30 * * * * *` every 30 seconds, `0 0 2 * * * 2027`). Existing 5-field schedules are unaffected ([#238](https://github.com/apache/superset-kubernetes-operator/pull/238), [@villebro](https://github.com/villebro)).
- **Helm `imagePullSecrets`.** The Helm chart now exposes an `imagePullSecrets` value that sets `spec.template.spec.imagePullSecrets` on the operator Deployment, letting operators pull the manager image from a private registry. The referenced Secrets must already exist in the release namespace — the chart does not create them ([#240](https://github.com/apache/superset-kubernetes-operator/pull/240), [@younsl](https://github.com/younsl)).
- **Helm `topologySpreadConstraints`.** The Helm chart now exposes a `topologySpreadConstraints` value that sets `spec.template.spec.topologySpreadConstraints` on the operator Deployment, letting operators distribute manager pods across failure domains such as nodes and zones ([#213](https://github.com/apache/superset-kubernetes-operator/pull/213), [@younsl](https://github.com/younsl)).
- **Helm `revisionHistoryLimit`.** The Helm chart now exposes a `revisionHistoryLimit` value that allows setting `spec.revisionHistoryLimit` on the operator Deployment, capping the number of old ReplicaSets retained for rollback ([@younsl](https://github.com/younsl)).
- **Helm extra manifests.** The Helm chart now supports `extraManifests` for rendering trusted, release-scoped Kubernetes manifests with Helm `tpl`. Use it for companion resources owned by the operator release, not shared cluster infrastructure such as Gateway API controllers, CRDs, or shared Gateways ([#196](https://github.com/apache/superset-kubernetes-operator/pull/196), [@younsl](https://github.com/younsl)).
- **Helm resize policy.** The Helm chart now supports `resizePolicy` on the manager container, controlling whether an in-place pod resize (InPlacePodVerticalScaling) restarts the container ([#200](https://github.com/apache/superset-kubernetes-operator/pull/200), [@younsl](https://github.com/younsl)).

### Changed

- **Breaking:** renamed the "version" status fields to "tag". `status.version` → `status.tag` (the `kubectl get` column is renamed **Version** → **Tag**), and `status.lifecycle.upgrade.fromVersion`/`toVersion` → `fromTag`/`toTag`. No behavior change.
- **Breaking:** downgrade blocking is removed. Any change to the lifecycle image tag now re-runs the migrate task (`superset db upgrade`) regardless of direction — the operator no longer performs semver comparison or sets `status.phase: Blocked` on a version decrease. The migrate task only runs `superset db upgrade` (Superset's down migrations are poorly tested and often break, so the operator never runs them), so take a database backup before an upgrade if you may need to revert. The `direction` field is removed from `status.lifecycle.upgrade`, and the `VersionComparisonSkipped` warning event no longer fires.
- **Breaking:** the lifecycle `clone` task is renamed to `seed`. Rename `spec.lifecycle.clone` to `spec.lifecycle.seed` (and its `postCloneSQL` field to `postSeedSQL`) in your Superset resources. The task Job name suffix changes from `-clone` to `-seed`, and the lifecycle status phase from `Cloning` to `Seeding`. Custom `seed.command` scripts must read the renamed `SUPERSET_OPERATOR__SEED_SRC_*` environment variables.

### Security

- **Credential redaction in task failure output.** Lifecycle task failure messages are now scrubbed with best-effort pattern-based redaction before being persisted to the parent Superset status and Kubernetes Events: passwords embedded in connection URIs and common credential assignments (`password=...`, `token=...`, etc.) are masked. This mitigates the known limitation where a failing task command could leak credential fragments into status ([@younsl](https://github.com/younsl)).

## 0.1.1 - 2026-06-29

### Fixed

- Honor HPA-managed replica counts: the operator no longer overwrites the replica count on Deployments whose scaling is owned by a HorizontalPodAutoscaler ([#152](https://github.com/apache/superset-kubernetes-operator/pull/152), [@pashtet04](https://github.com/pashtet04)).

## 0.1.0 - 2026-06-10

### Added

- Initial release ([@villebro](https://github.com/villebro)).

### Known limitations

- **Websocket server is experimental.** The websocket server is not yet well supported and is pending security hardening; it is not recommended for production use.
- **Downgrade protection requires semver image tags.** Downgrades are detected and blocked only when both image tags are valid semver. Non-semver tags (`latest`, date stamps, digest pins) cannot be ordered, so the operator emits a `VersionComparisonSkipped` warning and proceeds without blocking. See [Lifecycle](../user-guide/lifecycle.md).
- **Task failure messages may include credential fragments.** Lifecycle task failure output is truncated into `status` and could contain fragments of task stdout, including credentials. See [security.md](security.md).
