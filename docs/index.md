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

!!! warning "Under Development"
    This project is under active development and is not yet stable. APIs, CRD schemas, and behavior may change without notice between releases. Do not use in production.

A Kubernetes operator for deploying and managing [Apache Superset](https://superset.apache.org/). Built with the Go-based [Operator SDK](https://sdk.operatorframework.io/).

The operator manages the full Superset lifecycle: database migrations, configuration rendering, component deployment, scaling, and networking. Users define a single `Superset` custom resource, and the operator resolves it into per-component child CRDs that each manage their own Deployment, ConfigMap, and Service.

## Features

- **Sane defaults** — production-ready settings out of the box that adapt automatically to your workload
- **Painless management** — structured configuration fields with per-component config generated automatically
- **Full control** — every default is overridable, from high-level presets down to individual fields, with a raw Python escape hatch for anything not covered
- **Flat configuration** — shared top-level defaults inherited by all components, with per-component overrides (primitives replace, collections merge)
- **Component toggle** — enable CeleryWorker, CeleryBeat, CeleryFlower, WebsocketServer, or McpServer by setting their spec; omit to disable
- **Init lifecycle** — database migration and initialization run as managed Pods before components deploy
- **Checksum-driven rollouts** — config changes automatically trigger rolling restarts of affected components
- **Networking** — Gateway API (HTTPRoute) and Ingress support
- **HPA with custom metrics**, PodDisruptionBudgets, NetworkPolicies, Prometheus ServiceMonitor

## What it looks like

A typical Superset deployment for getting started in dev mode:

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
    host: postgres
    port: 5432
    database: superset
    username: superset
    password: superset
  config: |
    FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
  webServer:
    replicas: 2
  mcpServer: {}
  init:
    adminUser: {}
    loadExamples: true
```

For production, use `secretKeyFrom` and `metastore.uriFrom` to reference Kubernetes Secrets instead of inline values:

```yaml
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: my-superset
spec:
  image:
    tag: "6.0.1"
  secretKeyFrom:
    name: superset-secret
    key: secret-key
  metastore:
    uriFrom:
      name: db-credentials
      key: connection-string
  config: |
    FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
  webServer:
    replicas: 2
  mcpServer: {}
```

The operator resolves this into child CRDs and their underlying resources. The init child CR runs database migrations before components deploy:

```
$ kubectl get supersets
NAME           PHASE     VERSION   AGE
my-superset    Running   latest     5m

$ kubectl get supersetinits
NAME                PHASE      ATTEMPTS   AGE
my-superset         Complete   1          5m

$ kubectl get pods -l app.kubernetes.io/name=superset
NAME                                          READY   STATUS    AGE
my-superset-web-server-6d4b8c7f9-k2x8m        1/1     Running   4m
my-superset-web-server-6d4b8c7f9-p9f3n        1/1     Running   4m
my-superset-mcp-server-5c6d7e8f9-x9y1z        1/1     Running   4m
```

## Next steps

- [Installation](installation.md) — install the operator and deploy Superset
- [User Guide](user-guide.md) — full configuration reference
- [Architecture](architecture.md) — how the two-tier CRD design works
- [Internals](internals.md) — reconciliation lifecycle and runtime behavior
- [Security](security.md) — trust boundaries, threat model, vulnerability reporting
- [Developer Guide](developer-guide.md) — contributing and development setup
- [API Reference](api-reference.md) — auto-generated CRD type documentation

## License

Apache License 2.0