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

The operator manages the full Superset lifecycle: database migrations, configuration rendering, component deployment, scaling, and networking. Users define a single `Superset` custom resource, and the operator reconciles the Deployments, ConfigMaps, Services, lifecycle task Jobs, and supporting resources for that instance.

## Features

- **Sane defaults** — production-ready settings out of the box that adapt automatically to your workload
- **Automatic config rendering** — structured fields for metastore, Valkey, Gunicorn, and Celery generate correct `superset_config.py` per component; config changes trigger rolling restarts
- **Full control** — every default is overridable, from high-level presets down to individual container fields, with a raw Python escape hatch for anything not covered
- **Component toggle** — enable CeleryWorker, CeleryBeat, CeleryFlower, WebsocketServer, or McpServer by setting their spec; omit to disable
- **Zero-downtime upgrades** — maintenance page serves users during database migrations; the operator drains components gracefully, runs lifecycle tasks, and restores traffic only after the new version is healthy
- **Lifecycle automation** — database cloning, schema migrations, secret key rotation, and application init run as sequenced tasks with automatic change detection and checksum-based re-execution
- **Networking** — Gateway API (HTTPRoute) and Ingress support with per-component routing
- **Production hardening** — HPA with custom metrics, PodDisruptionBudgets, NetworkPolicies, Prometheus ServiceMonitor
- **Flexible install scope** — cluster-scoped (default) or namespace-scoped; the namespace-scoped Helm install renders no manager `ClusterRole` at all, so it works on restricted clusters that forbid cluster-scoped RBAC

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
  environment: Development
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
  lifecycle:
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

The operator resolves this into parent-owned resources. Lifecycle task Jobs run database migrations before components deploy, and their durable state is projected onto the parent status:

```
$ kubectl get supersets
NAME           VERSION   PHASE     READY   AVAILABLE   AGE
my-superset    latest    Running   3/3     True        5m

$ kubectl get superset my-superset -o jsonpath='{.status.lifecycle.migrate.state}'
Complete

$ kubectl get pods -l app.kubernetes.io/name=superset
NAME                                          READY   STATUS    AGE
my-superset-web-server-6d4b8c7f9-k2x8m        1/1     Running   4m
my-superset-web-server-6d4b8c7f9-p9f3n        1/1     Running   4m
my-superset-mcp-server-5c6d7e8f9-x9y1z        1/1     Running   4m
```

## Where to start

**Deploying Superset?**

- [Getting Started](getting-started.md) — quickstart with dev mode
- [Installation](user-guide/installation.md) — production deployment with secrets

**Configuring an existing deployment?**

- [Configuration](user-guide/configuration.md) — full configuration reference
- [Lifecycle](user-guide/lifecycle.md) — database migrations and upgrade management
- [Networking & Monitoring](user-guide/networking-and-monitoring.md) — external access and observability

**Understanding how the operator works?**

- [Architecture](architecture/overview.md) — CRD hierarchy, config rendering pipeline
- [Internals](architecture/internals.md) — reconciliation phases, resource ownership, status

**Contributing code?**

- [Development Setup](contributing/development-setup.md) — local environment with Kind
- [Development Guidelines](contributing/development-guidelines.md) — testing, patterns, PR conventions

**Reference:**

- [API Reference](reference/api-reference.md) — auto-generated CRD type documentation
- [Security](reference/security.md) — trust boundaries, threat model, vulnerability reporting
- [Downloads](reference/downloads.md) — images, Helm chart, verification

## License

Apache License 2.0
