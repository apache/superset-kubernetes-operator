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

# Installation

This guide covers installing the operator and deploying a Superset instance.

## Prerequisites

- Kubernetes v1.28+ cluster
- Helm 3 (for Helm-based installation) or `kubectl` + `kustomize`
- PostgreSQL or MySQL database
- (Optional) [Valkey](https://valkey.io/) (or Redis) — required when enabling caching, Celery task broker, or result backend
- (Optional) [Gateway API CRDs](https://gateway-api.sigs.k8s.io/guides/#installing-gateway-api) for HTTPRoute support — not included in Kubernetes, must be installed separately
- (Optional) prometheus-operator CRDs for ServiceMonitor support

## 1. Install the operator

Install from the OCI Helm registry:

```bash
helm install superset-operator \
  oci://ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator \
  --version <version> \
  --namespace superset-operator-system \
  --create-namespace
```

Or from a source checkout:

```bash
helm install superset-operator charts/superset-operator \
  --namespace superset-operator-system \
  --create-namespace
```

See `charts/superset-operator/values.yaml` for all available Helm values and
[Downloads](downloads.md) for published images and tag conventions.

## 2. Create secrets

Superset requires a secret key for session signing. In production, mount it
as an environment variable:

```bash
kubectl create secret generic superset-secrets \
  --from-literal=secret-key="$(openssl rand -hex 32)"
```

If your database credentials should not appear in the CR, mount the full
connection URI as a Secret too:

```bash
kubectl create secret generic superset-db \
  --from-literal=uri="postgresql+psycopg2://<user>:<password>@<host>:5432/<database>"
```

Replace the placeholders with your PostgreSQL connection details.

## 3. Deploy Superset

Create a `Superset` custom resource. In production (`environment: prod`, the
default), CRD validation rejects inline `secretKey`, `metastore.uri`,
`metastore.password`, and `valkey.password`. Reference secrets via
`secretKeyFrom`, `metastore.uriFrom`, `metastore.passwordFrom`, or
`valkey.passwordFrom` — the operator wires these as `valueFrom.secretKeyRef`
env vars automatically:

```bash
kubectl apply -f - <<'EOF'
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: my-superset
spec:
  image:
    tag: "6.0.1"

  secretKeyFrom:
    name: superset-secrets
    key: secret-key

  metastore:
    uriFrom:
      name: superset-db
      key: uri

  webServer:
    replicas: 2
    service:
      type: ClusterIP
      port: 8088
EOF
```

The operator will create a SupersetInit child CR to perform database migrations,
then create the web server child CR and its resources. Check init status with
`kubectl get supersetinits`.

## 4. Watch it come up

```bash
# Watch the parent CR status
kubectl get superset my-superset -w

# Watch init pods
kubectl get pods -l superset.apache.org/init-task -w

# Watch all pods
kubectl get pods -w
```

## 5. Access Superset

Port-forward to the web server service:

```bash
kubectl port-forward svc/my-superset-web-server 8088:8088
```

Open [http://localhost:8088](http://localhost:8088) in your browser.

### Authentication

The operator does not manage Superset user accounts. Production deployments
typically use an external authentication provider (OIDC, OAuth, LDAP) configured
via `spec.config`. See the
[Superset security documentation](https://superset.apache.org/docs/configuration/configuring-superset/#custom-oauth2-configuration)
for details.

## 6. Add Celery workers

Celery workers handle background tasks such as chart data caching, scheduled
reports, and long-running queries. Add the Celery components and point them at
your Valkey (or Redis) instance via `spec.valkey`:

```bash
kubectl apply -f - <<'EOF'
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: my-superset
spec:
  image:
    tag: "6.0.1"

  secretKeyFrom:
    name: superset-secrets
    key: secret-key

  metastore:
    uriFrom:
      name: superset-db
      key: uri

  valkey:
    host: valkey.default.svc

  webServer:
    replicas: 2
    service:
      type: ClusterIP
      port: 8088

  celeryWorker:
    replicas: 2

  celeryBeat: {}
EOF
```

The operator will create the CeleryWorker and CeleryBeat child CRs and their
Deployments automatically.

## Next steps

- [User Guide](user-guide.md) — configuration, networking, monitoring,
  and full configuration reference
- [Architecture](architecture.md) — how the two-tier CRD design works
- [Developer Guide](developer-guide.md) — contributing, development setup,
  testing, and code structure