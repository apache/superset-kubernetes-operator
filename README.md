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

# Apache Superset Kubernetes Operator

> **Warning**: This project is under active development and is not yet stable. APIs, CRD schemas, and behavior may change without notice between releases. Do not use in production.

[![CI](https://github.com/apache/superset-kubernetes-operator/actions/workflows/ci.yaml/badge.svg)](https://github.com/apache/superset-kubernetes-operator/actions/workflows/ci.yaml)

A Kubernetes operator for deploying and managing [Apache Superset](https://superset.apache.org/) on Kubernetes. Built with the Go-based [Operator SDK](https://sdk.operatorframework.io/), the operator is designed to be simple to get started with — a minimal deployment requires just a few lines of YAML — while giving advanced users full control over every component through shared top-level defaults, per-component overrides, and direct access to flattened child CRDs.

- **Typed config with secret safety** — structured config fields rendered to per-component `superset_config.py`; secrets resolve to `os.environ["..."]` and never appear in ConfigMaps
- **Flat configuration** — shared top-level defaults inherited by all components, with per-component overrides (primitives replace, collections merge)
- **Component toggle** — enable CeleryWorker, CeleryBeat, CeleryFlower, WebsocketServer, or McpServer by setting their spec; omit to disable
- **Init lifecycle** — database migration and custom scripts run as managed Pods before components deploy
- **Checksum-driven rollouts** — config changes automatically trigger rolling restarts of affected components
- **Networking** — Gateway API (HTTPRoute) and Ingress support
- **Production hardening** — HPA with custom metrics, PodDisruptionBudgets, NetworkPolicies, Prometheus ServiceMonitor, health probes, customizable security contexts via deployment templates

## CRD Hierarchy

The operator manages eight CRDs under the `superset.apache.org/v1alpha1` API group:

| CRD Kind | Parent field | Suffix | Creates |
|---|---|---|---|
| **Superset** | *(parent)* | | Child CRs, Ingress/HTTPRoute, ServiceMonitor, NetworkPolicies |
| **SupersetInit** | `init` | `-init` | Pods, ConfigMap |
| **SupersetWebServer** | `webServer` | `-web-server` | Deployment, Service, ConfigMap, HPA |
| **SupersetCeleryWorker** | `celeryWorker` | `-celery-worker` | Deployment, ConfigMap, HPA |
| **SupersetCeleryBeat** | `celeryBeat` | `-celery-beat` | Deployment, ConfigMap |
| **SupersetCeleryFlower** | `celeryFlower` | `-celery-flower` | Deployment, Service, ConfigMap |
| **SupersetWebsocketServer** | `websocketServer` | `-websocket-server` | Deployment, Service |
| **SupersetMcpServer** | `mcpServer` | `-mcp-server` | Deployment, Service, ConfigMap |

## Prerequisites

- Kubernetes v1.28+ cluster
- Helm 3 (for Helm-based installation) or `kubectl` + `kustomize`
- (Optional) Gateway API CRDs for HTTPRoute support
- (Optional) prometheus-operator CRDs for ServiceMonitor support

## Quick Start

Install the operator via Helm:

```sh
helm install superset-operator charts/superset-operator \
  --namespace superset-operator-system \
  --create-namespace
```

Then create a minimal Superset instance:

```yaml
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: my-superset
spec:
  image:
    tag: "latest"
  environment: dev
  secretKey: thisIsNotSecure_changeInProduction!
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  webServer: {}
```

> **Note**: The example above uses `environment: dev` for simplicity. In production (the default), use `secretKeyFrom` and `metastore.uriFrom` to reference Kubernetes Secrets. See the [User Guide](docs/user-guide.md) and the [sample manifests](config/samples/) for production-ready examples.

## Documentation

- [User Guide](docs/user-guide.md) — installation, configuration, migration from Helm chart
- [Architecture](docs/architecture.md) — two-tier CRD design, config rendering, init lifecycle
- [Developer Guide](docs/developer-guide.md) — contributing, testing, releasing

## Development

```sh
make build            # Build operator binary
make test             # Run unit/integration tests
make lint             # Run golangci-lint
make helm-lint        # Lint the Helm chart
make docs-serve       # Serve docs locally (http://localhost:8000)
make manifests        # Regenerate CRDs + RBAC from markers
make generate         # Regenerate DeepCopy methods
```

After editing type definitions in `api/v1alpha1/`, run `make manifests generate` and commit the generated files alongside your changes.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.