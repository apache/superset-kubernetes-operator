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

# Getting Started

This guide walks through installing the operator and deploying a minimal
Superset instance using dev mode.

## Prerequisites

- Kubernetes v1.28+ cluster
- Helm 3
- A PostgreSQL database accessible from the cluster

## 1. Install the operator

```bash
helm install superset-operator \
  oci://ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator \
  --version 0.0.0-dev \
  --namespace superset-operator-system \
  --create-namespace \
  --set image.pullPolicy=Always
```

## 2. Deploy Superset

Create a minimal Superset instance in dev mode (inline credentials for simplicity):

```bash
kubectl apply -f - <<'EOF'
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: my-superset
spec:
  environment: Development
  image:
    tag: "6.0.1"
  secretKey: thisIsNotSecure_changeInProduction!
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  webServer: {}
  lifecycle:
    migrate: {}
    init:
      adminUser: {}
      loadExamples: true
EOF
```

The operator runs database migrations and initialization, then deploys the web server.

## 3. Watch it come up

```bash
kubectl get superset my-superset -w
```

Wait for `PHASE: Running`. You can also inspect lifecycle task state on the
parent:

```bash
kubectl describe superset my-superset
```

## 4. Access Superset

```bash
kubectl port-forward svc/my-superset-web-server 8088:8088
```

Open [http://localhost:8088](http://localhost:8088) and log in with `admin` / `admin`.

## Next steps

- [Installation](user-guide/installation.md) — production deployment with proper secret management
- [Configuration](user-guide/configuration.md) — full configuration reference
- [Architecture](architecture/overview.md) — how the operator works under the hood
