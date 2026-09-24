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

- **Pre-upgrade metastore backup.** A new optional `spec.lifecycle.backup` task dumps the metastore (`pg_dump -Fc` or `mysqldump`) to an existing PersistentVolumeClaim, or through a custom command, immediately before a migrate or rotate run, so a failed or unwanted upgrade can be reverted by restoring the dump. It runs at most once per lifecycle run, after drain; it is not part of the task checksum cascade, so editing it never re-runs another task, and a retried migrate reuses the snapshot taken before the first attempt. A failed backup blocks the upgrade before the database is touched. Dumps are written owner-only (`0600`), verified, and never overwritten. It is mutually exclusive with `seed` and requires no additional RBAC. Status gains `status.lifecycle.backup`, `status.lifecycle.settledChecksum`, and the `BackingUp` lifecycle phase.
- Structured metastore, Valkey, and lifecycle seed-source connection fields can now be sourced individually from Kubernetes Secret keys, allowing `Superset` resources to consume provisioner-owned endpoints and credentials without copying discovered values or assembling a separate connection URI ([#369](https://github.com/apache/superset-kubernetes-operator/pull/369)).
- **Helm `podDisruptionBudget`.** The Helm chart now exposes a `podDisruptionBudget` value that renders a [PodDisruptionBudget](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/#pod-disruption-budgets) for the operator manager pods. It is disabled by default and falls back to `maxUnavailable: 1` when no bound is set. Rendering fails for integer or percentage bounds that would leave zero allowed disruptions at the configured `replicas` (for example `minAvailable: 1` with one replica, or `maxUnavailable: 0`), so the budget never blocks node drains. See [High availability](../user-guide/installation.md#high-availability) for the recommended multi-replica setup. [`unhealthyPodEvictionPolicy`](https://kubernetes.io/docs/tasks/run-application/configure-pdb/#unhealthy-pod-eviction-policy) can be set to `AlwaysAllow` so drains can evict a crash-looping manager ([#386](https://github.com/apache/superset-kubernetes-operator/pull/386), [@younsl](https://github.com/younsl)).

### Changed

- **Helm chart requires leader election for multiple replicas.** Rendering now fails when `replicas` is greater than 1 and `leaderElection.enabled` is `false`. Replicas are active/standby behind the [leader election Lease](https://kubernetes.io/docs/concepts/architecture/leases/#leader-election); without it every replica reconciles the same `Superset` resources and races on Jobs, drains, and status. Existing releases with that combination fail `helm upgrade` until leader election is enabled or the release is scaled to one replica ([#390](https://github.com/apache/superset-kubernetes-operator/pull/390), [@younsl](https://github.com/younsl)).
- Valkey's default port is now applied at runtime, aligning it with the existing structured-metastore behavior, instead of being stored by CRD defaulting. Resources that omit both `valkey.port` and `valkey.portFrom` therefore no longer materialize `port: 6379`, while generated configuration continues to use port 6379. This permits `portFrom` without a defaulted literal conflicting at admission ([#369](https://github.com/apache/superset-kubernetes-operator/pull/369)).
- Kubernetes support now covers the three newest `kind`-published minor versions instead of two. CI tests Kubernetes 1.37, 1.36, and 1.35 natively, with the experimental `next` lane disabled again ([#317](https://github.com/apache/superset-kubernetes-operator/pull/317)).

### Fixed

- The default MySQL tool image for the seed task and the `createDatabase` init container is now `mysql:8.4`. The previous default, `mysql:8-alpine`, does not exist on Docker Hub, so MySQL seeding and database auto-creation failed with an image pull error unless an image was set explicitly. Staging instances that seed from MySQL with the default image re-seed once after upgrading, because the seed image is part of the seed task checksum ([#389](https://github.com/apache/superset-kubernetes-operator/pull/389), [@younsl](https://github.com/younsl)).
- The operator-managed maintenance page now sets `seccompProfile: RuntimeDefault` on its container, so it is admitted in namespaces enforcing the restricted Pod Security Standard (its other hardened defaults were already set, but the missing seccomp profile caused rejection) ([#362](https://github.com/apache/superset-kubernetes-operator/pull/362), [@villebro](https://github.com/villebro)).
- The operator no longer crash-loops on clusters where Gateway API is installed but serves only `v1beta1`: optional-API detection now pins the version the operator actually uses (`HTTPRoute` v1, `ServiceMonitor` v1) instead of matching any version of the Kind ([#361](https://github.com/apache/superset-kubernetes-operator/pull/361), [@villebro](https://github.com/villebro)).
- Component Services of type `NodePort` or `LoadBalancer` that do not pin `service.nodePort` no longer churn: the operator now preserves the apiserver-allocated node port across reconciles instead of triggering a re-allocation each time, which previously changed the external port and broke external access ([#360](https://github.com/apache/superset-kubernetes-operator/pull/360), [@villebro](https://github.com/villebro)).
- The operator now requests `update` on `supersets/finalizers`, so reconciliation no longer fails on clusters with the `OwnerReferencesPermissionEnforcement` admission plugin enabled (e.g. OpenShift), where the API server previously rejected the `blockOwnerDeletion` owner references the operator sets on child resources ([#359](https://github.com/apache/superset-kubernetes-operator/pull/359), [@villebro](https://github.com/villebro)).
- The chart no longer rejects top-level values it does not declare, so shared values files and deploy tooling that injects release metadata no longer fail schema validation. Nested value blocks are still strictly validated.

### Security

- Every operator-created container (component Deployments, lifecycle task Jobs, and the create-database init container) now defaults to a hardened securityContext — `allowPrivilegeEscalation: false`, all capabilities dropped, and `seccompProfile: RuntimeDefault` — filled per-field only where you have not set them yourself. This hardens workloads on permissive clusters and satisfies several of the restricted Pod Security Standard's requirements out of the box; full restricted compatibility still requires user-provided non-root settings (`runAsNonRoot`/`runAsUser` are not defaulted, because the Superset image declares a named user and a numeric UID cannot be safely defaulted across vanilla and OpenShift clusters). This changes pod templates, so workloads roll once on upgrade ([#363](https://github.com/apache/superset-kubernetes-operator/pull/363), [@villebro](https://github.com/villebro)).
- **Breaking:** CRs with `serviceAccount.create=true` (or unset) and `serviceAccount.name != metadata.name` are now rejected at admission. To use a pre-existing ServiceAccount with a different name, set `serviceAccount.create=false` ([#324](https://github.com/apache/superset-kubernetes-operator/pull/324), [@villebro](https://github.com/villebro)).
- **Breaking:** Valkey cache and results-backend `keyPrefix` values are now constrained to `^[A-Za-z0-9._:-]*$` (max 128 characters); values containing spaces, slashes, or other characters outside that set are rejected. The safer rendering also causes a one-time config-checksum change and pod roll on upgrade ([#325](https://github.com/apache/superset-kubernetes-operator/pull/325), [@villebro](https://github.com/villebro)).
- Seed `excludeTables`, `excludeTableData`, and `postSeedSQL` values are now passed literally to generated seed scripts; values that previously relied on shell expansion no longer expand ([#325](https://github.com/apache/superset-kubernetes-operator/pull/325), [@villebro](https://github.com/villebro)).
- **Breaking:** Celery Flower is no longer published on the Ingress/Gateway surface in `Production` by default. Flower's default command ships without authentication and its dashboard discloses task names and arguments — for Superset, other users' async SQL Lab statements and alert/report payloads. In `Production` (the default) Flower is fanned out onto the end-user host — and its port opened in the built-in NetworkPolicy — only when you explicitly set `celeryFlower.service.gatewayPath`; `Development` and `Staging` continue to publish it by default. To keep exposing Flower in Production, set `celeryFlower.service.gatewayPath` (e.g. `/flower`) after placing authentication in front of it (e.g. `FLOWER_BASIC_AUTH` injected from a Secret via `celeryFlower.podTemplate.container.env`) ([#279](https://github.com/apache/superset-kubernetes-operator/pull/279), [@villebro](https://github.com/villebro)).
- **Breaking:** A `Production` Superset CR can no longer be changed in place to `Development` or `Staging`; delete and recreate the instance to change environment out of `Production`. Upgrades toward `Production` and non-Production transitions are unaffected ([#327](https://github.com/apache/superset-kubernetes-operator/pull/327), [@villebro](https://github.com/villebro)).

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
