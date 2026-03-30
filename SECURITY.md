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

# Security Policy

## Security Model

The operator defaults to production mode. CRD validation rejects inline secrets
and credentials must be referenced from Kubernetes Secrets via `secretKeyFrom`,
`metastore.uriFrom`, `metastore.passwordFrom`, or `valkey.passwordFrom`. The operator never reads,
logs, or stores secret values in ConfigMaps or CRD status fields. The operator
runs as a non-root, distroless container with read-only root filesystem, dropped
capabilities, and least-privilege RBAC.

**Init pod caveat:** When an init pod fails, a truncated termination message
(max 256 characters) may appear in the SupersetInit status and Events. If the
init command's error output includes credentials, a fragment could be exposed.
This only applies to the init container's own output, not to operator-managed
secret references.

Users who can create or modify `Superset` custom resources are trusted — they can
deploy arbitrary containers and Python configuration. Restrict access to Superset
CRs using Kubernetes RBAC.

For a detailed description of trust boundaries, security assumptions, and
scope, see the [Security](docs/security.md) documentation. If you are unsure
whether something crosses the operator's trust boundary, please report it
privately and the maintainers will help triage it.

## Supported Versions

| Version | Supported |
|---|---|
| v1alpha1 (latest) | Yes |

## Reporting a Vulnerability

The Apache Superset Kubernetes Operator project follows the
[Apache Software Foundation vulnerability handling process](https://apache.org/security/).

To report a security vulnerability, please email **security@apache.org**.

Please do **not** file a public GitHub issue for security vulnerabilities.

## Scope

This policy covers the Superset Kubernetes Operator and its components:

- CRD definitions and CEL validation rules
- Controller reconciliation logic
- RBAC and resource management
- Helm chart and deployment manifests
