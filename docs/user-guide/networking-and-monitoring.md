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

### Graceful CRD Handling

If Gateway API CRDs are not present, the controller skips HTTPRoute watch
registration and catches `meta.IsNoMatchError` at reconciliation time. The
operator runs with reduced functionality rather than failing.

## Prometheus ServiceMonitor

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

## Network Policies

```yaml
spec:
  networkPolicy:
    extraIngress: []
    extraEgress: []
```

Creates per-component NetworkPolicies that:

- Allow ingress from other components of the same Superset instance (matched by `app.kubernetes.io/name: superset` + `superset.apache.org/parent` labels — multiple Superset instances in the same namespace are isolated from each other)
- Allow ingress on the service port from any source for externally-facing components (web server, Celery Flower, websocket server, MCP server) — this is necessary because ingress controllers and load balancers typically reside outside the namespace and cannot be matched with a pod selector
- Allow all egress (for database/cache access)
- Support custom `extraIngress` and `extraEgress` rules

**Per-component rules:**

| Component | Ingress from Superset pods | Ingress from external | Egress |
|---|---|---|---|
| WebServer | port 8088 | port 8088 | all |
| CeleryWorker | any port | — | all |
| CeleryBeat | any port | — | all |
| CeleryFlower | port 5555 | port 5555 | all |
| WebsocketServer | port 8080 | port 8080 | all |
| McpServer | port 8088 | port 8088 | all |

If you need to restrict external ingress to specific sources, disable the built-in
network policy and create your own NetworkPolicy resources with the desired `from`
selectors.
