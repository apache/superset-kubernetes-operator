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

# Migration from Helm Chart

| Helm Chart Value | Operator Equivalent |
|-----------------|---------------------|
| `image.repository` + `image.tag` | `spec.image.repository` + `spec.image.tag` |
| `supersetNode.connections` | `spec.metastore` (with `uriFrom` or structured fields) |
| `supersetNode.replicaCount` | `spec.webServer.replicas` |
| `supersetWorker.replicaCount` | `spec.celeryWorker.replicas` |
| `supersetCeleryBeat.enabled` | `spec.celeryBeat: {}` (set) or omit (disabled) |
| `supersetCeleryFlower.enabled` | `spec.celeryFlower: {}` (set) or omit (disabled) |
| `supersetWebsockets.enabled` | `spec.websocketServer: {}` (set) or omit (disabled) |
| `configOverrides` | `spec.config` |
| `ingress.*` | `spec.networking.ingress` |
| `service.*` | `spec.webServer.service` |
| `resources.*` | `spec.podTemplate.container.resources` (top-level) or per-component |
| `nodeSelector` | `spec.podTemplate.nodeSelector` (top-level) or per-component (merged) |
| `tolerations` | `spec.podTemplate.tolerations` (top-level) or per-component (appended) |
| `affinity` | `spec.podTemplate.affinity` (top-level) or per-component (replaces) |
| `extraEnv` | `spec.podTemplate.container.env` (top-level) or per-component (merged) |
| `postgresql.*` | Not managed -- use CloudNativePG or managed services |
| `redis.*` | Not managed -- use Redis Operator or managed services |
