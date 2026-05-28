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

# Networking & Monitoring

## Services

Routable components expose a Kubernetes Service. The service spec supports
`type`, `port`, `nodePort`, labels, and annotations:

```yaml
spec:
  webServer:
    service:
      type: LoadBalancer
      port: 443
      annotations:
        service.beta.kubernetes.io/aws-load-balancer-internal: "true"
```

The operator does not expose Kubernetes `Service.spec.loadBalancerIP` because
that field is deprecated. Prefer controller-specific annotations when your
provider documents them.

## Gateway API (Recommended)

Requires [Gateway API CRDs](https://gateway-api.sigs.k8s.io/) installed on the cluster. Gateway API is not included in Kubernetes and must be [installed separately](https://gateway-api.sigs.k8s.io/guides/#installing-gateway-api). If the CRDs are absent, the operator logs a message and skips HTTPRoute management.

```yaml
spec:
  networking:
    gateway:
      gatewayRef:
        name: my-gateway
        namespace: gateway-system
      hostnames:
        - superset.example.com
```

The operator creates an `HTTPRoute` with path-based routing:

| Priority | Path | Target | Condition |
|---|---|---|---|
| 1 (most specific) | `/ws` | websocket-server Service | websocketServer enabled |
| 2 | `/mcp` | mcp-server Service | mcpServer enabled |
| 3 | `/flower` | celery-flower Service | celeryFlower enabled |
| 4 (catch-all) | `/` | web-server Service | webServer enabled |

More specific paths are listed first to ensure correct routing priority.
Paths are configurable via `service.gatewayPath` on each component spec.

For example, to serve Celery Flower under `/monitoring`:

```yaml
spec:
  celeryFlower:
    service:
      gatewayPath: /monitoring
```

## Ingress (Legacy)

Gateway API and Ingress are mutually exclusive — set one or the other, not both.

```yaml
spec:
  networking:
    ingress:
      className: nginx
      annotations:
        nginx.ingress.kubernetes.io/proxy-body-size: "100m"
      hosts:
        - host: superset.example.com
          paths:
            - path: /
              pathType: Prefix
      tls:
        - secretName: superset-tls
          hosts:
            - superset.example.com
```

Use `className` for controllers that support `spec.ingressClassName`. For
legacy controllers, put `kubernetes.io/ingress.class` under `annotations`
instead:

```yaml
spec:
  networking:
    ingress:
      annotations:
        kubernetes.io/ingress.class: alb
        alb.ingress.kubernetes.io/target-type: ip
      hosts:
        - host: superset.example.com
          paths:
            - path: /
              pathType: Prefix
```

### Graceful CRD Handling

If Gateway API CRDs are not present, the controller skips HTTPRoute watch
registration and catches `meta.IsNoMatchError` at reconciliation time. The
operator runs with reduced functionality rather than failing.

## Superset Instance Metrics

Requires [prometheus-operator](https://prometheus-operator.dev/) CRDs. The operator gracefully skips if they are not installed.

```yaml
spec:
  monitoring:
    serviceMonitor:
      interval: 30s
      labels:
        release: prometheus
```

The controller creates a Prometheus `ServiceMonitor` targeting the web-server
component using unstructured objects (because the ServiceMonitor CRD is
external: `monitoring.coreos.com/v1`). Default scrape interval is 30s
(configurable). Targets pods with `app.kubernetes.io/component: web-server`.

## Operator Metrics

The operator itself exposes [controller-runtime](https://book.kubebuilder.io/reference/metrics.html)
default metrics — reconcile counts and durations, work-queue depth, leader
election state. These are served over HTTPS on port 8443, guarded by
Kubernetes bearer-token authentication and authorization. No custom
lifecycle metrics are emitted today; condition and event streams cover the
per-instance lifecycle state.

**RBAC.** Any scraper (typically Prometheus) needs the `metrics-reader`
ClusterRole bound to its own ServiceAccount. Both install paths ship this
role; bind it to your Prometheus ServiceAccount, for example:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: prometheus-superset-operator-metrics
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: superset-operator-metrics-reader  # Kustomize. For Helm, check the installed role name.
subjects:
  - kind: ServiceAccount
    name: prometheus
    namespace: monitoring
```

**TLS by default.** The operator generates a self-signed certificate at
startup, so scrapers connect over HTTPS with `insecureSkipVerify: true`. This
is the default in both the Kustomize ServiceMonitor (`config/prometheus/monitor.yaml`)
and the Helm chart. It's fine for a trusted cluster but not for
zero-trust environments. Authentication and authorization are always enforced
via bearer tokens (`TokenReview`/`SubjectAccessReview`) regardless of TLS
setup — see
[Design Decisions](../reference/security.md#design-decisions) in the security
reference for the trust-model context.

### Enable via Helm

The chart ships a ServiceMonitor template, off by default. The minimal
opt-in looks like:

```yaml
metrics:
  enabled: true
  serviceMonitor:
    enabled: true
    interval: 30s
    labels:
      release: prometheus           # selector your Prometheus picks up
```

That keeps the default self-signed + `insecureSkipVerify: true` behavior.

For real TLS via cert-manager, provision a `metrics-server-cert` Secret
(typically via a `cert-manager.io/v1` Certificate keyed to a self-signed
Issuer) and point the chart at it:

```yaml
metrics:
  enabled: true
  certSecretName: metrics-server-cert   # mounts Secret into manager pod
  serviceMonitor:
    enabled: true
    labels:
      release: prometheus
    tlsConfig:                           # free-form ServiceMonitor tlsConfig
      insecureSkipVerify: false
      serverName: <release>-superset-operator-metrics.<namespace>.svc
      ca:
        secret:
          name: metrics-server-cert
          key: ca.crt
```

The operator authenticates scrapers via bearer token (TokenReview), not
mTLS, so `ca` + `serverName` are sufficient for the scraper to verify the
server. Add `cert`/`keySecret` only if you've separately configured the
metrics server to require client certs.

Setting `metrics.certSecretName` mounts the Secret into the manager pod
and adds `--metrics-cert-path` args so the metrics server presents that
certificate. The full set of knobs is documented in
`charts/superset-operator/values.yaml`.

The chart does not ship the cert-manager `Certificate` or `Issuer`
resources themselves; you create them (for example via
`cert-manager.io/v1` manifests alongside your values). See the Kustomize
section below for a working example of those manifests.

### Enable via Kustomize

Uncomment `- ../prometheus` in `config/default/kustomization.yaml` and
re-apply. This adds the shipped ServiceMonitor in
`config/prometheus/monitor.yaml`, which scrapes the operator's self-signed
metrics endpoint with `insecureSkipVerify: true`.

For a cert-manager-managed certificate (real TLS, no `insecureSkipVerify`),
install [cert-manager](https://cert-manager.io/) on the cluster, then
uncomment three more sections in `config/default/kustomization.yaml`:

- `[CERTMANAGER]` — pulls in `config/certmanager` (a `selfsigned-issuer`
  Issuer plus a `metrics-certs` Certificate that issues the
  `metrics-server-cert` Secret).
- `[METRICS-WITH-CERTS]` — mounts that Secret into the manager pod and
  adds the `--metrics-cert-path` args.
- `[PROMETHEUS-WITH-CERTS]` — a `replacements` block that substitutes the
  real Service name and namespace into the Certificate's dnsNames and the
  ServiceMonitor's `tlsConfig.serverName`.

Also uncomment the patch reference in `config/prometheus/kustomization.yaml`
so the ServiceMonitor picks up the real-TLS `tlsConfig` from
`monitor_tls_patch.yaml`.

## Network Policies

```yaml
spec:
  networkPolicy:
    extraIngress: []
    extraEgress: []
```

Creates per-component NetworkPolicies that:

- Allow ingress from other pods of the same Superset instance on **any port** (matched by `app.kubernetes.io/name: superset` + `superset.apache.org/parent` labels — multiple Superset instances in the same namespace are isolated from each other). The same-instance rule is intentionally port-unrestricted so internal traffic between components (sidecar metrics scraping, the websocket server fanning out to web pods, etc.) is not silently blocked.
- Allow ingress on the service port from any source for externally-facing components (web server, Celery Flower, websocket server, MCP server) — this is necessary because ingress controllers and load balancers typically reside outside the namespace and cannot be matched with a pod selector.
- Allow all egress (for database/cache access)
- Support custom `extraIngress` and `extraEgress` rules

**Per-component rules:**

| Component | Ingress from same-instance Superset pods | Ingress from external | Egress |
|---|---|---|---|
| WebServer | any port | port 8088 | all |
| CeleryWorker | any port | — | all |
| CeleryBeat | any port | — | all |
| CeleryFlower | any port | port 5555 | all |
| WebsocketServer | any port | port 8080 | all |
| McpServer | any port | port 8088 | all |

If you need to restrict external ingress to specific sources, disable the built-in
network policy and create your own NetworkPolicy resources with the desired `from`
selectors.

The built-in policy is ingress segmentation only — egress is intentionally
unrestricted so workloads can reach the metastore database, Valkey, SMTP
servers, and other user-configured dependencies. For the rationale and
hardening path, see
[Design Decisions](../reference/security.md#design-decisions) in the security
reference.
