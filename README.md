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
[![codecov](https://codecov.io/gh/apache/superset-kubernetes-operator/branch/main/graph/badge.svg)](https://codecov.io/gh/apache/superset-kubernetes-operator)
[![Go Report Card](https://goreportcard.com/badge/github.com/apache/superset-kubernetes-operator)](https://goreportcard.com/report/github.com/apache/superset-kubernetes-operator)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Docs](https://img.shields.io/badge/docs-apache.github.io-blue)](https://apache.github.io/superset-kubernetes-operator/)

A Kubernetes operator for deploying and managing [Apache Superset](https://superset.apache.org/) on Kubernetes. Read the [documentation](https://apache.github.io/superset-kubernetes-operator/) to get started. Built with the Go-based [Operator SDK](https://sdk.operatorframework.io/).

The operator is designed to make running Superset on Kubernetes as painless as possible. It works well out of the box with production-ready defaults, and every default is overridable when you need more control.

## Quick Start

Install the operator via Helm:

```sh
helm install superset-operator \
  oci://ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator \
  --version <version> \
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

> **Note**: The example above uses `environment: dev` for simplicity. In production (the default), use `secretKeyFrom` and `metastore.uriFrom` to reference Kubernetes Secrets. See the [User Guide](https://apache.github.io/superset-kubernetes-operator/user-guide/) and the [sample manifests](config/samples/) for production-ready examples.

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