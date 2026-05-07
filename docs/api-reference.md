# API Reference

## Packages
- [superset.apache.org/v1alpha1](#supersetapacheorgv1alpha1)


## superset.apache.org/v1alpha1

Package v1alpha1 contains API Schema definitions for the superset v1alpha1 API group.

### Resource Types
- [Superset](#superset)
- [SupersetCeleryBeat](#supersetcelerybeat)
- [SupersetCeleryFlower](#supersetceleryflower)
- [SupersetCeleryWorker](#supersetceleryworker)
- [SupersetLifecycleTask](#supersetlifecycletask)
- [SupersetMcpServer](#supersetmcpserver)
- [SupersetWebServer](#supersetwebserver)
- [SupersetWebsocketServer](#supersetwebsocketserver)



#### AdminUserSpec



AdminUserSpec defines admin user credentials for dev-mode initialization.



_Appears in:_
- [InitTaskSpec](#inittaskspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `username` _string_ | Admin username. | admin | Optional: \{\} <br /> |
| `password` _string_ | Admin password. Stored as plain-text env var in dev mode. | admin | Optional: \{\} <br /> |
| `firstName` _string_ | Admin first name. | Superset | Optional: \{\} <br /> |
| `lastName` _string_ | Admin last name. | Admin | Optional: \{\} <br /> |
| `email` _string_ | Admin email. | admin@example.com | Optional: \{\} <br /> |


#### AutoscalingSpec



AutoscalingSpec configures a HorizontalPodAutoscaler.



_Appears in:_
- [CeleryFlowerComponentSpec](#celeryflowercomponentspec)
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)
- [FlatComponentSpec](#flatcomponentspec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [ScalableComponentSpec](#scalablecomponentspec)
- [SupersetCeleryBeatSpec](#supersetcelerybeatspec)
- [SupersetCeleryFlowerSpec](#supersetceleryflowerspec)
- [SupersetCeleryWorkerSpec](#supersetceleryworkerspec)
- [SupersetLifecycleTaskSpec](#supersetlifecycletaskspec)
- [SupersetMcpServerSpec](#supersetmcpserverspec)
- [SupersetSpec](#supersetspec)
- [SupersetWebServerSpec](#supersetwebserverspec)
- [SupersetWebsocketServerSpec](#supersetwebsocketserverspec)
- [WebServerComponentSpec](#webservercomponentspec)
- [WebsocketServerComponentSpec](#websocketservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `minReplicas` _integer_ | Minimum replica count (defaults to 1). |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `maxReplicas` _integer_ | Maximum replica count; HPA will not scale above this. |  | Maximum: 100 <br />Minimum: 1 <br /> |
| `metrics` _[MetricSpec](https://pkg.go.dev/k8s.io/api/autoscaling/v2#MetricSpec) array_ | Metrics for the HPA. Supports CPU, memory, custom, and external metrics.<br />When empty, Kubernetes defaults to 80% average CPU utilization. |  | Optional: \{\} <br /> |


#### CeleryBeatComponentSpec



CeleryBeatComponentSpec defines the celery beat component on the parent CRD.
The controller forces replicas=1 regardless of spec.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Deployment-level overrides (strategy, revision history). Always enforces replicas=1. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod and container template for Celery beat pods. |  | Optional: \{\} <br /> |
| `image` _[ImageOverrideSpec](#imageoverridespec)_ | Image tag and/or repository overrides; inherits from spec.image if unset. |  | Optional: \{\} <br /> |
| `config` _string_ | Per-component raw Python appended after top-level config. |  | Optional: \{\} <br /> |
| `sqlaEngineOptions` _[SQLAlchemyEngineOptionsSpec](#sqlalchemyengineoptionsspec)_ | Per-component SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely). |  | Optional: \{\} <br /> |


#### CeleryFlowerComponentSpec



CeleryFlowerComponentSpec defines the celery flower component on the parent CRD.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Deployment template (Deployment-level configuration). |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod template (Pod and container configuration). |  | Optional: \{\} <br /> |
| `replicas` _integer_ | Desired replica count; overridden by autoscaling when active. Defaults to spec.replicas if unset. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | HorizontalPodAutoscaler configuration. When set, the HPA manages replica count. Overrides spec.autoscaling. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget for protecting availability during voluntary disruptions. Overrides spec.podDisruptionBudget. |  | Optional: \{\} <br /> |
| `image` _[ImageOverrideSpec](#imageoverridespec)_ | Image tag and/or repository overrides; inherits from spec.image if unset. |  | Optional: \{\} <br /> |
| `config` _string_ | Per-component raw Python appended after top-level config. |  | Optional: \{\} <br /> |
| `service` _[ComponentServiceSpec](#componentservicespec)_ | Service configuration (type, port, annotations). |  | Optional: \{\} <br /> |


#### CeleryWorkerComponentSpec



CeleryWorkerComponentSpec defines the celery worker component on the parent CRD.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Deployment template (Deployment-level configuration). |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod template (Pod and container configuration). |  | Optional: \{\} <br /> |
| `replicas` _integer_ | Desired replica count; overridden by autoscaling when active. Defaults to spec.replicas if unset. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | HorizontalPodAutoscaler configuration. When set, the HPA manages replica count. Overrides spec.autoscaling. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget for protecting availability during voluntary disruptions. Overrides spec.podDisruptionBudget. |  | Optional: \{\} <br /> |
| `image` _[ImageOverrideSpec](#imageoverridespec)_ | Image tag and/or repository overrides; inherits from spec.image if unset. |  | Optional: \{\} <br /> |
| `config` _string_ | Per-component raw Python appended after top-level config. |  | Optional: \{\} <br /> |
| `celery` _[CeleryWorkerProcessSpec](#celeryworkerprocessspec)_ | Celery worker execution configuration. Controls concurrency, pool type, and related parameters. |  | Optional: \{\} <br /> |
| `sqlaEngineOptions` _[SQLAlchemyEngineOptionsSpec](#sqlalchemyengineoptionsspec)_ | Per-component SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely). |  | Optional: \{\} <br /> |


#### CeleryWorkerProcessSpec



CeleryWorkerProcessSpec configures Celery worker execution parameters.
Fields controlled by presets: concurrency, pool.
All other fields have static defaults independent of preset.



_Appears in:_
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `preset` _string_ | Preset controlling concurrency and pool defaults.<br />Individual fields override preset-computed values. |  | Enum: [disabled conservative balanced performance aggressive] <br />Optional: \{\} <br /> |
| `concurrency` _integer_ | Number of concurrent task workers (maps to celery -c flag). |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `pool` _string_ | Celery pool implementation. |  | Enum: [prefork threads gevent eventlet solo] <br />Optional: \{\} <br /> |
| `optimization` _string_ | Task distribution optimization strategy. |  | Enum: [default fair] <br />Optional: \{\} <br /> |
| `maxTasksPerChild` _integer_ | Maximum tasks a worker process handles before being replaced (prefork only; 0 = unlimited). |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `maxMemoryPerChild` _integer_ | Maximum resident memory in bytes per worker before being replaced (prefork only; 0 = disabled). |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `prefetchMultiplier` _integer_ | Task prefetch multiplier — number of tasks prefetched per worker. |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `softTimeLimit` _integer_ | Soft time limit in seconds — raises SoftTimeLimitExceeded (0 = disabled). |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `timeLimit` _integer_ | Hard time limit in seconds — kills the task (0 = disabled). |  | Minimum: 0 <br />Optional: \{\} <br /> |


#### ChildComponentStatus



ChildComponentStatus reports the operational state of a child component.



_Appears in:_
- [SupersetCeleryBeatStatus](#supersetcelerybeatstatus)
- [SupersetCeleryFlowerStatus](#supersetceleryflowerstatus)
- [SupersetCeleryWorkerStatus](#supersetceleryworkerstatus)
- [SupersetMcpServerStatus](#supersetmcpserverstatus)
- [SupersetWebServerStatus](#supersetwebserverstatus)
- [SupersetWebsocketServerStatus](#supersetwebsocketserverstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ready` _string_ | "2/2" format showing ready vs desired replicas. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### ComponentRefStatus



ComponentRefStatus holds the status summary of a child component.



_Appears in:_
- [ComponentStatusMap](#componentstatusmap)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ready` _string_ | "2/2" format showing ready vs desired replicas. |  |  |
| `ref` _string_ | Reference to the child CR. |  |  |
| `configChecksum` _string_ | Config checksum on the child. |  | Optional: \{\} <br /> |


#### ComponentServiceSpec



ComponentServiceSpec defines the Service configuration for a component.



_Appears in:_
- [CeleryFlowerComponentSpec](#celeryflowercomponentspec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [SupersetCeleryFlowerSpec](#supersetceleryflowerspec)
- [SupersetMcpServerSpec](#supersetmcpserverspec)
- [SupersetWebServerSpec](#supersetwebserverspec)
- [SupersetWebsocketServerSpec](#supersetwebsocketserverspec)
- [WebServerComponentSpec](#webservercomponentspec)
- [WebsocketServerComponentSpec](#websocketservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _[ServiceType](https://pkg.go.dev/k8s.io/api/core/v1#ServiceType)_ | Service type (ClusterIP, NodePort, LoadBalancer). | ClusterIP | Enum: [ClusterIP NodePort LoadBalancer] <br />Optional: \{\} <br /> |
| `port` _integer_ | Service port exposed to clients. Defaults to the component's standard port (8088 for web server, 5555 for Flower). |  | Optional: \{\} <br /> |
| `nodePort` _integer_ | Fixed NodePort number when type=NodePort (30000-32767). If omitted, Kubernetes auto-assigns. |  | Optional: \{\} <br /> |
| `annotations` _object (keys:string, values:string)_ | Service annotations (e.g., for cloud load balancer configuration). |  | Optional: \{\} <br /> |
| `labels` _object (keys:string, values:string)_ | Service labels; merged with operator-managed labels. |  | Optional: \{\} <br /> |
| `gatewayPath` _string_ | URL path prefix for this component's HTTPRoute rule.<br />Only used when spec.networking.gateway is set.<br />Defaults: /ws (websocket), /mcp (MCP server), /flower (Celery Flower). |  | Pattern: `^/[a-zA-Z0-9/_.-]+$` <br />Optional: \{\} <br /> |


#### ComponentSpec



ComponentSpec defines per-component identity fields.
Embedded by all component specs except InitSpec.



_Appears in:_
- [CeleryBeatComponentSpec](#celerybeatcomponentspec)
- [CeleryFlowerComponentSpec](#celeryflowercomponentspec)
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [WebServerComponentSpec](#webservercomponentspec)
- [WebsocketServerComponentSpec](#websocketservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageOverrideSpec](#imageoverridespec)_ | Image tag and/or repository overrides; inherits from spec.image if unset. |  | Optional: \{\} <br /> |


#### ComponentStatusMap



ComponentStatusMap holds status for each component.



_Appears in:_
- [SupersetStatus](#supersetstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `webServer` _[ComponentRefStatus](#componentrefstatus)_ |  |  | Optional: \{\} <br /> |
| `celeryWorker` _[ComponentRefStatus](#componentrefstatus)_ |  |  | Optional: \{\} <br /> |
| `celeryBeat` _[ComponentRefStatus](#componentrefstatus)_ |  |  | Optional: \{\} <br /> |
| `celeryFlower` _[ComponentRefStatus](#componentrefstatus)_ |  |  | Optional: \{\} <br /> |
| `websocketServer` _[ComponentRefStatus](#componentrefstatus)_ |  |  | Optional: \{\} <br /> |
| `mcpServer` _[ComponentRefStatus](#componentrefstatus)_ |  |  | Optional: \{\} <br /> |


#### ContainerTemplate



ContainerTemplate configures fields on the main Superset container.



_Appears in:_
- [PodTemplate](#podtemplate)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `resources` _[ResourceRequirements](https://pkg.go.dev/k8s.io/api/core/v1#ResourceRequirements)_ | Resource requirements (CPU, memory). |  | Optional: \{\} <br /> |
| `env` _[EnvVar](https://pkg.go.dev/k8s.io/api/core/v1#EnvVar) array_ | Environment variables. |  | Optional: \{\} <br /> |
| `envFrom` _[EnvFromSource](https://pkg.go.dev/k8s.io/api/core/v1#EnvFromSource) array_ | Environment variable sources (ConfigMaps, Secrets). |  | Optional: \{\} <br /> |
| `volumeMounts` _[VolumeMount](https://pkg.go.dev/k8s.io/api/core/v1#VolumeMount) array_ | Volume mounts for the main container. |  | Optional: \{\} <br /> |
| `ports` _[ContainerPort](https://pkg.go.dev/k8s.io/api/core/v1#ContainerPort) array_ | Container ports. Replaces operator defaults when set. |  | Optional: \{\} <br /> |
| `securityContext` _[SecurityContext](https://pkg.go.dev/k8s.io/api/core/v1#SecurityContext)_ | Container-level security context. |  | Optional: \{\} <br /> |
| `command` _string array_ | Container entrypoint override. |  | Optional: \{\} <br /> |
| `args` _string array_ | Container arguments override. |  | Optional: \{\} <br /> |
| `livenessProbe` _[Probe](https://pkg.go.dev/k8s.io/api/core/v1#Probe)_ | Liveness probe; container is restarted when the probe fails. |  | Optional: \{\} <br /> |
| `readinessProbe` _[Probe](https://pkg.go.dev/k8s.io/api/core/v1#Probe)_ | Readiness probe; pod is removed from Service endpoints when the probe fails. |  | Optional: \{\} <br /> |
| `startupProbe` _[Probe](https://pkg.go.dev/k8s.io/api/core/v1#Probe)_ | Startup probe; liveness and readiness probes are deferred until this probe succeeds. |  | Optional: \{\} <br /> |
| `lifecycle` _[Lifecycle](https://pkg.go.dev/k8s.io/api/core/v1#Lifecycle)_ | Lifecycle hooks for the main container. |  | Optional: \{\} <br /> |


#### DeploymentTemplate



DeploymentTemplate configures Kubernetes Deployment-level fields for
operator-managed Deployments. Pod and container configuration is in
the sibling PodTemplate field.



_Appears in:_
- [CeleryBeatComponentSpec](#celerybeatcomponentspec)
- [CeleryFlowerComponentSpec](#celeryflowercomponentspec)
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)
- [FlatComponentSpec](#flatcomponentspec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [ScalableComponentSpec](#scalablecomponentspec)
- [SupersetCeleryBeatSpec](#supersetcelerybeatspec)
- [SupersetCeleryFlowerSpec](#supersetceleryflowerspec)
- [SupersetCeleryWorkerSpec](#supersetceleryworkerspec)
- [SupersetLifecycleTaskSpec](#supersetlifecycletaskspec)
- [SupersetMcpServerSpec](#supersetmcpserverspec)
- [SupersetSpec](#supersetspec)
- [SupersetWebServerSpec](#supersetwebserverspec)
- [SupersetWebsocketServerSpec](#supersetwebsocketserverspec)
- [WebServerComponentSpec](#webservercomponentspec)
- [WebsocketServerComponentSpec](#websocketservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `revisionHistoryLimit` _integer_ | Number of old ReplicaSets to retain for rollback. |  | Optional: \{\} <br /> |
| `minReadySeconds` _integer_ | Minimum seconds a pod must be ready before considered available. |  | Optional: \{\} <br /> |
| `progressDeadlineSeconds` _integer_ | Maximum seconds for a deployment to make progress before considered failed. |  | Optional: \{\} <br /> |
| `strategy` _[DeploymentStrategy](https://pkg.go.dev/k8s.io/api/apps/v1#DeploymentStrategy)_ | Deployment update strategy. |  | Optional: \{\} <br /> |


#### FlatComponentSpec



FlatComponentSpec defines the common fields for all fully-resolved child specs.
This is embedded (inlined) in each child CRD spec type.



_Appears in:_
- [SupersetCeleryBeatSpec](#supersetcelerybeatspec)
- [SupersetCeleryFlowerSpec](#supersetceleryflowerspec)
- [SupersetCeleryWorkerSpec](#supersetceleryworkerspec)
- [SupersetLifecycleTaskSpec](#supersetlifecycletaskspec)
- [SupersetMcpServerSpec](#supersetmcpserverspec)
- [SupersetWebServerSpec](#supersetwebserverspec)
- [SupersetWebsocketServerSpec](#supersetwebsocketserverspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Container image configuration. |  |  |
| `replicas` _integer_ | Desired replica count. | 1 | Optional: \{\} <br /> |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Fully-resolved deployment template. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Fully-resolved pod template. |  | Optional: \{\} <br /> |
| `serviceAccountName` _string_ | ServiceAccountName to set on the pod. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling configuration. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget configuration. |  | Optional: \{\} <br /> |


#### GatewaySpec



GatewaySpec defines HTTPRoute configuration.



_Appears in:_
- [NetworkingSpec](#networkingspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `gatewayRef` _[ParentReference](https://gateway-api.sigs.k8s.io/reference/spec/#gateway.networking.k8s.io/v1.ParentReference)_ | Reference to the Gateway resource to attach the HTTPRoute to. |  |  |
| `hostnames` _[Hostname](https://gateway-api.sigs.k8s.io/reference/spec/#gateway.networking.k8s.io/v1.Hostname) array_ | Hostnames for the HTTPRoute (e.g., "superset.example.com"). |  | Optional: \{\} <br /> |
| `annotations` _object (keys:string, values:string)_ | HTTPRoute annotations. |  | Optional: \{\} <br /> |
| `labels` _object (keys:string, values:string)_ | HTTPRoute labels. |  | Optional: \{\} <br /> |


#### GunicornSpec



GunicornSpec configures Gunicorn worker parameters for the web server.
Fields controlled by presets: workers, threads, workerClass.
All other fields have static defaults independent of preset.



_Appears in:_
- [WebServerComponentSpec](#webservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `preset` _string_ | Preset controlling workers, threads, and workerClass defaults.<br />Individual fields override preset-computed values. |  | Enum: [disabled conservative balanced performance aggressive] <br />Optional: \{\} <br /> |
| `workers` _integer_ | Number of Gunicorn worker processes. |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `threads` _integer_ | Number of threads per worker (only effective with gthread worker class). |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `workerClass` _string_ | Gunicorn worker class. |  | Enum: [sync gthread gevent eventlet] <br />Optional: \{\} <br /> |
| `timeout` _integer_ | Request timeout in seconds. |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `keepAlive` _integer_ | Keep-alive timeout in seconds for waiting for requests on a connection. |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `maxRequests` _integer_ | Maximum requests per worker before recycling (0 = disabled). |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `maxRequestsJitter` _integer_ | Random jitter added to maxRequests to prevent thundering herd on worker recycling. |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `limitRequestLine` _integer_ | Maximum size of HTTP request line in bytes (0 = unlimited). |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `limitRequestFieldSize` _integer_ | Maximum size of HTTP request header field in bytes (0 = unlimited). |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `logLevel` _string_ | Gunicorn log level. |  | Enum: [debug info warning error critical] <br />Optional: \{\} <br /> |


#### ImageOverrideSpec



ImageOverrideSpec allows a component to override specific image fields.
Unset fields inherit from spec.image.



_Appears in:_
- [CeleryBeatComponentSpec](#celerybeatcomponentspec)
- [CeleryFlowerComponentSpec](#celeryflowercomponentspec)
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)
- [ComponentSpec](#componentspec)
- [LifecycleSpec](#lifecyclespec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [WebServerComponentSpec](#webservercomponentspec)
- [WebsocketServerComponentSpec](#websocketservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `tag` _string_ | Override the image tag for this component; inherits from spec.image.tag if omitted. |  | Optional: \{\} <br /> |
| `repository` _string_ | Override the image repository for this component; inherits from spec.image.repository if omitted. |  | Optional: \{\} <br /> |


#### ImageSpec



ImageSpec defines the container image configuration.



_Appears in:_
- [FlatComponentSpec](#flatcomponentspec)
- [SupersetCeleryBeatSpec](#supersetcelerybeatspec)
- [SupersetCeleryFlowerSpec](#supersetceleryflowerspec)
- [SupersetCeleryWorkerSpec](#supersetceleryworkerspec)
- [SupersetLifecycleTaskSpec](#supersetlifecycletaskspec)
- [SupersetMcpServerSpec](#supersetmcpserverspec)
- [SupersetSpec](#supersetspec)
- [SupersetWebServerSpec](#supersetwebserverspec)
- [SupersetWebsocketServerSpec](#supersetwebsocketserverspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `repository` _string_ | Container image repository. | apachesuperset.docker.scarf.sh/apache/superset | Optional: \{\} <br /> |
| `tag` _string_ | Image tag. |  | MinLength: 1 <br /> |
| `pullPolicy` _[PullPolicy](https://pkg.go.dev/k8s.io/api/core/v1#PullPolicy)_ | Image pull policy (IfNotPresent, Always, Never). | IfNotPresent | Optional: \{\} <br /> |
| `pullSecrets` _[LocalObjectReference](https://pkg.go.dev/k8s.io/api/core/v1#LocalObjectReference) array_ | References to Secrets for pulling images from private registries. |  | Optional: \{\} <br /> |


#### IngressHost



IngressHost defines a host rule for the Ingress.



_Appears in:_
- [IngressSpec](#ingressspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `host` _string_ |  |  | Optional: \{\} <br /> |
| `paths` _[IngressPath](#ingresspath) array_ |  |  | Optional: \{\} <br /> |


#### IngressPath



IngressPath defines a path rule for an Ingress host.



_Appears in:_
- [IngressHost](#ingresshost)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `path` _string_ |  | / |  |
| `pathType` _[PathType](https://pkg.go.dev/k8s.io/api/networking/v1#PathType)_ |  | Prefix | Optional: \{\} <br /> |


#### IngressSpec



IngressSpec defines Ingress configuration.



_Appears in:_
- [NetworkingSpec](#networkingspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `className` _string_ | IngressClass name (e.g., "nginx") that determines which controller processes this Ingress. |  | Optional: \{\} <br /> |
| `host` _string_ | Primary hostname for the Ingress rule (e.g., "superset.example.com"). |  | Optional: \{\} <br /> |
| `annotations` _object (keys:string, values:string)_ | Ingress annotations (e.g., for TLS, auth, or controller-specific configuration). |  | Optional: \{\} <br /> |
| `labels` _object (keys:string, values:string)_ | Ingress labels. |  | Optional: \{\} <br /> |
| `hosts` _[IngressHost](#ingresshost) array_ | Additional host/path rules beyond the primary host. |  | Optional: \{\} <br /> |
| `tls` _[IngressTLS](https://pkg.go.dev/k8s.io/api/networking/v1#IngressTLS) array_ | TLS configuration (certificate secrets and hostnames). |  | Optional: \{\} <br /> |


#### InitTaskSpec



InitTaskSpec defines when and how the application initialization task runs.



_Appears in:_
- [LifecycleSpec](#lifecyclespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `strategy` _string_ | Strategy controls when the init task runs.<br />VersionChange: only on image changes (default).<br />Always: on any spec change (image, config, command).<br />Never: skip entirely. | VersionChange | Enum: [VersionChange Always Never] <br />Optional: \{\} <br /> |
| `command` _string array_ | Command override for the init task.<br />Default: ["sh", "-c", "superset init"]<br />Mutually exclusive with adminUser and loadExamples. |  |  |
| `timeout` _[Duration](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration)_ | Maximum timeout per attempt. |  | Optional: \{\} <br /> |
| `maxRetries` _integer_ | Maximum number of retries before permanent failure. | 3 | Minimum: 1 <br />Optional: \{\} <br /> |
| `adminUser` _[AdminUserSpec](#adminuserspec)_ | Admin user to create during initialization. Only allowed in dev mode.<br />When set, the operator appends a superset fab create-admin step to the init command. |  | Optional: \{\} <br /> |
| `loadExamples` _boolean_ | Load example dashboards and data during initialization. Only allowed in dev mode.<br />When true, the operator appends a superset load-examples step to the init command. |  | Optional: \{\} <br /> |


#### LifecycleSpec



LifecycleSpec defines lifecycle management configuration for database migrations
and application initialization tasks.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `upgradeMode` _string_ | UpgradeMode controls whether upgrades require manual approval.<br />Automatic runs immediately on image change; Supervised waits for an<br />approval annotation before proceeding. | Automatic | Enum: [Automatic Supervised] <br />Optional: \{\} <br /> |
| `upgradeStrategy` _string_ | UpgradeStrategy controls component behavior during database migrations.<br />Rolling (default): tasks run while existing components stay up.<br />Drain: all components scale to 0 before tasks run, avoiding metastore deadlocks. | Rolling | Enum: [Rolling Drain] <br />Optional: \{\} <br /> |
| `disabled` _boolean_ | Set to true to skip all lifecycle tasks entirely. |  | Optional: \{\} <br /> |
| `image` _[ImageOverrideSpec](#imageoverridespec)_ | Image override for lifecycle task pods. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod and container template for lifecycle task pods. |  | Optional: \{\} <br /> |
| `podRetention` _[PodRetentionSpec](#podretentionspec)_ | Pod retention policy for completed task pods. |  | Optional: \{\} <br /> |
| `config` _string_ | Per-lifecycle raw Python appended after top-level config. |  | Optional: \{\} <br /> |
| `sqlaEngineOptions` _[SQLAlchemyEngineOptionsSpec](#sqlalchemyengineoptionsspec)_ | Per-lifecycle SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely). |  | Optional: \{\} <br /> |
| `migrate` _[MigrateTaskSpec](#migratetaskspec)_ | Database migration task configuration. |  | Optional: \{\} <br /> |
| `init` _[InitTaskSpec](#inittaskspec)_ | Application initialization task configuration. |  | Optional: \{\} <br /> |


#### LifecycleStatus



LifecycleStatus tracks the current lifecycle task execution state.



_Appears in:_
- [SupersetStatus](#supersetstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `phase` _string_ | Phase of the lifecycle: Idle, Migrating, Initializing, Complete, Blocked, AwaitingApproval. |  | Optional: \{\} <br /> |
| `migrate` _[TaskRefStatus](#taskrefstatus)_ | Migrate task status summary. |  | Optional: \{\} <br /> |
| `init` _[TaskRefStatus](#taskrefstatus)_ | Init task status summary. |  | Optional: \{\} <br /> |
| `upgrade` _[UpgradeContext](#upgradecontext)_ | Upgrade context (populated during active upgrade). |  | Optional: \{\} <br /> |


#### McpServerComponentSpec



McpServerComponentSpec defines the MCP server component on the parent CRD.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Deployment template (Deployment-level configuration). |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod template (Pod and container configuration). |  | Optional: \{\} <br /> |
| `replicas` _integer_ | Desired replica count; overridden by autoscaling when active. Defaults to spec.replicas if unset. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | HorizontalPodAutoscaler configuration. When set, the HPA manages replica count. Overrides spec.autoscaling. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget for protecting availability during voluntary disruptions. Overrides spec.podDisruptionBudget. |  | Optional: \{\} <br /> |
| `image` _[ImageOverrideSpec](#imageoverridespec)_ | Image tag and/or repository overrides; inherits from spec.image if unset. |  | Optional: \{\} <br /> |
| `config` _string_ | Per-component raw Python appended after top-level config. |  | Optional: \{\} <br /> |
| `service` _[ComponentServiceSpec](#componentservicespec)_ | Service configuration (type, port, annotations). |  | Optional: \{\} <br /> |
| `sqlaEngineOptions` _[SQLAlchemyEngineOptionsSpec](#sqlalchemyengineoptionsspec)_ | Per-component SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely). |  | Optional: \{\} <br /> |


#### MetastoreSpec



MetastoreSpec defines the database connection for Superset's metastore.
Either a URI (passthrough) or structured fields (host, database, etc.) can be used.
They are mutually exclusive.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `uri` _string_ | Full SQLAlchemy database URI. Mutually exclusive with structured fields and uriFrom.<br />In prod mode, CRD validation rejects plain text URIs — use uriFrom to reference a Kubernetes Secret. |  | Optional: \{\} <br /> |
| `uriFrom` _[SecretKeySelector](https://pkg.go.dev/k8s.io/api/core/v1#SecretKeySelector)_ | Reference to a Secret key containing the full SQLAlchemy URI.<br />Mutually exclusive with uri and structured fields. |  | Optional: \{\} <br /> |
| `type` _string_ | Database type. Determines the SQLAlchemy driver. | postgresql | Enum: [postgresql mysql] <br />Optional: \{\} <br /> |
| `host` _string_ | Database hostname. |  | Optional: \{\} <br /> |
| `port` _integer_ | Database port. Defaults per driver (5432 for postgresql, 3306 for mysql). |  | Optional: \{\} <br /> |
| `database` _string_ | Database name. |  | Optional: \{\} <br /> |
| `username` _string_ | Database username. |  | Optional: \{\} <br /> |
| `password` _string_ | Database password. In prod mode, CRD validation rejects plain text passwords — use passwordFrom to reference a Kubernetes Secret. |  | Optional: \{\} <br /> |
| `passwordFrom` _[SecretKeySelector](https://pkg.go.dev/k8s.io/api/core/v1#SecretKeySelector)_ | Reference to a Secret key containing the database password.<br />Mutually exclusive with password. |  | Optional: \{\} <br /> |


#### MigrateTaskSpec



MigrateTaskSpec defines when and how the database migration task runs.



_Appears in:_
- [LifecycleSpec](#lifecyclespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `strategy` _string_ | Strategy controls when the migrate task runs.<br />VersionChange: only on image changes (default).<br />Always: on any spec change (image, config, command).<br />Never: skip (user manages migrations externally). | VersionChange | Enum: [VersionChange Always Never] <br />Optional: \{\} <br /> |
| `command` _string array_ | Command override for the migration task.<br />Default: ["sh", "-c", "superset db upgrade"] |  |  |
| `timeout` _[Duration](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration)_ | Maximum timeout per attempt. |  | Optional: \{\} <br /> |
| `maxRetries` _integer_ | Maximum number of retries before permanent failure. | 3 | Minimum: 1 <br />Optional: \{\} <br /> |


#### MonitoringSpec



MonitoringSpec defines Prometheus monitoring configuration.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `serviceMonitor` _[ServiceMonitorSpec](#servicemonitorspec)_ |  |  | Optional: \{\} <br /> |


#### NetworkPolicySpec



NetworkPolicySpec defines network segmentation configuration.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `extraIngress` _[NetworkPolicyIngressRule](https://pkg.go.dev/k8s.io/api/networking/v1#NetworkPolicyIngressRule) array_ | Additional ingress rules appended to the operator-generated NetworkPolicy (e.g., allow traffic from monitoring namespace). |  | Optional: \{\} <br /> |
| `extraEgress` _[NetworkPolicyEgressRule](https://pkg.go.dev/k8s.io/api/networking/v1#NetworkPolicyEgressRule) array_ | Additional egress rules appended to the operator-generated NetworkPolicy. |  | Optional: \{\} <br /> |


#### NetworkingSpec



NetworkingSpec defines external access configuration.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `gateway` _[GatewaySpec](#gatewayspec)_ | Gateway API HTTPRoute configuration. |  | Optional: \{\} <br /> |
| `ingress` _[IngressSpec](#ingressspec)_ | Ingress configuration. |  | Optional: \{\} <br /> |


#### PDBSpec



PDBSpec configures a PodDisruptionBudget.



_Appears in:_
- [CeleryFlowerComponentSpec](#celeryflowercomponentspec)
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)
- [FlatComponentSpec](#flatcomponentspec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [ScalableComponentSpec](#scalablecomponentspec)
- [SupersetCeleryBeatSpec](#supersetcelerybeatspec)
- [SupersetCeleryFlowerSpec](#supersetceleryflowerspec)
- [SupersetCeleryWorkerSpec](#supersetceleryworkerspec)
- [SupersetLifecycleTaskSpec](#supersetlifecycletaskspec)
- [SupersetMcpServerSpec](#supersetmcpserverspec)
- [SupersetSpec](#supersetspec)
- [SupersetWebServerSpec](#supersetwebserverspec)
- [SupersetWebsocketServerSpec](#supersetwebsocketserverspec)
- [WebServerComponentSpec](#webservercomponentspec)
- [WebsocketServerComponentSpec](#websocketservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `minAvailable` _[IntOrString](https://pkg.go.dev/k8s.io/apimachinery/pkg/util/intstr#IntOrString)_ | Minimum pods that must remain available during voluntary disruptions. Mutually exclusive with maxUnavailable. |  | Optional: \{\} <br /> |
| `maxUnavailable` _[IntOrString](https://pkg.go.dev/k8s.io/apimachinery/pkg/util/intstr#IntOrString)_ | Maximum pods allowed to be unavailable during voluntary disruptions. Mutually exclusive with minAvailable. |  | Optional: \{\} <br /> |


#### PodRetentionSpec



PodRetentionSpec defines retention behavior for init pods.



_Appears in:_
- [LifecycleSpec](#lifecyclespec)
- [SupersetLifecycleTaskSpec](#supersetlifecycletaskspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `policy` _string_ | Retention policy: Delete removes pods after completion, Retain keeps all,<br />RetainOnFailure keeps only failed pods for debugging. | Delete | Enum: [Delete Retain RetainOnFailure] <br />Optional: \{\} <br /> |


#### PodTemplate



PodTemplate configures Kubernetes PodSpec fields for the pod template.



_Appears in:_
- [CeleryBeatComponentSpec](#celerybeatcomponentspec)
- [CeleryFlowerComponentSpec](#celeryflowercomponentspec)
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)
- [FlatComponentSpec](#flatcomponentspec)
- [LifecycleSpec](#lifecyclespec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [ScalableComponentSpec](#scalablecomponentspec)
- [SupersetCeleryBeatSpec](#supersetcelerybeatspec)
- [SupersetCeleryFlowerSpec](#supersetceleryflowerspec)
- [SupersetCeleryWorkerSpec](#supersetceleryworkerspec)
- [SupersetLifecycleTaskSpec](#supersetlifecycletaskspec)
- [SupersetMcpServerSpec](#supersetmcpserverspec)
- [SupersetSpec](#supersetspec)
- [SupersetWebServerSpec](#supersetwebserverspec)
- [SupersetWebsocketServerSpec](#supersetwebsocketserverspec)
- [WebServerComponentSpec](#webservercomponentspec)
- [WebsocketServerComponentSpec](#websocketservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `annotations` _object (keys:string, values:string)_ | Pod annotations. |  | Optional: \{\} <br /> |
| `labels` _object (keys:string, values:string)_ | Pod labels (merged with operator-managed labels which cannot be overridden). |  | Optional: \{\} <br /> |
| `affinity` _[Affinity](https://pkg.go.dev/k8s.io/api/core/v1#Affinity)_ | Pod affinity and anti-affinity rules for scheduling. |  | Optional: \{\} <br /> |
| `tolerations` _[Toleration](https://pkg.go.dev/k8s.io/api/core/v1#Toleration) array_ | Tolerations for scheduling on tainted nodes. |  | Optional: \{\} <br /> |
| `nodeSelector` _object (keys:string, values:string)_ | Node labels for constraining pod scheduling. |  | Optional: \{\} <br /> |
| `topologySpreadConstraints` _[TopologySpreadConstraint](https://pkg.go.dev/k8s.io/api/core/v1#TopologySpreadConstraint) array_ | Topology spread constraints for distributing pods across failure domains. |  | Optional: \{\} <br /> |
| `hostAliases` _[HostAlias](https://pkg.go.dev/k8s.io/api/core/v1#HostAlias) array_ | Entries added to /etc/hosts in pod containers. |  | Optional: \{\} <br /> |
| `podSecurityContext` _[PodSecurityContext](https://pkg.go.dev/k8s.io/api/core/v1#PodSecurityContext)_ | Pod-level security context (runAsUser, fsGroup, seccomp, etc.). |  | Optional: \{\} <br /> |
| `priorityClassName` _string_ | Priority class name for pod scheduling priority and preemption. |  | Optional: \{\} <br /> |
| `volumes` _[Volume](https://pkg.go.dev/k8s.io/api/core/v1#Volume) array_ | Additional volumes for the pod (mounted via container.volumeMounts). |  | Optional: \{\} <br /> |
| `sidecars` _[Container](https://pkg.go.dev/k8s.io/api/core/v1#Container) array_ | Sidecar containers added alongside the main Superset container. |  | Optional: \{\} <br /> |
| `initContainers` _[Container](https://pkg.go.dev/k8s.io/api/core/v1#Container) array_ | Init containers run before the main container starts. |  | Optional: \{\} <br /> |
| `terminationGracePeriodSeconds` _integer_ | Grace period for pod termination in seconds. |  | Optional: \{\} <br /> |
| `dnsPolicy` _[DNSPolicy](https://pkg.go.dev/k8s.io/api/core/v1#DNSPolicy)_ | DNS policy for pods. |  | Optional: \{\} <br /> |
| `dnsConfig` _[PodDNSConfig](https://pkg.go.dev/k8s.io/api/core/v1#PodDNSConfig)_ | Custom DNS configuration for pods. |  | Optional: \{\} <br /> |
| `runtimeClassName` _string_ | RuntimeClass for pods. |  | Optional: \{\} <br /> |
| `shareProcessNamespace` _boolean_ | Share a single process namespace between all containers in a pod. |  | Optional: \{\} <br /> |
| `enableServiceLinks` _boolean_ | Controls whether service environment variables are injected into pods. |  | Optional: \{\} <br /> |
| `resources` _[ResourceRequirements](https://pkg.go.dev/k8s.io/api/core/v1#ResourceRequirements)_ | Pod-level resource requirements (CPU, memory). When set, defines the total<br />resources for the entire pod, enabling resource sharing among containers.<br />Requires Kubernetes 1.34+ with the PodLevelResources feature gate. |  | Optional: \{\} <br /> |
| `container` _[ContainerTemplate](#containertemplate)_ | Main container configuration. |  | Optional: \{\} <br /> |


#### SQLAlchemyEngineOptionsSpec



SQLAlchemyEngineOptionsSpec configures the SQLAlchemy connection pool.
Fields controlled by presets: poolClass (NullPool vs QueuePool), poolSize, maxOverflow.
Static defaults: poolRecycle=3600, poolPrePing=false.



_Appears in:_
- [CeleryBeatComponentSpec](#celerybeatcomponentspec)
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)
- [LifecycleSpec](#lifecyclespec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [SupersetSpec](#supersetspec)
- [WebServerComponentSpec](#webservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `preset` _string_ | Preset for connection pool behavior. "disabled" suppresses rendering entirely.<br />"conservative" uses NullPool (no persistent connections).<br />"balanced" through "aggressive" use QueuePool with increasing pool sizes.<br />Individual fields override preset-computed values. |  | Enum: [disabled conservative balanced performance aggressive] <br />Optional: \{\} <br /> |
| `poolSize` _integer_ | Number of persistent connections in the pool. Overrides preset calculation. |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `maxOverflow` _integer_ | Maximum overflow connections beyond poolSize (-1 = unlimited). |  | Optional: \{\} <br /> |
| `poolRecycle` _integer_ | Connection max-age in seconds before recycling. |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `poolPrePing` _boolean_ | Verify connections are alive before use. |  | Optional: \{\} <br /> |
| `poolTimeout` _integer_ | Seconds to wait for a connection from the pool before giving up. |  | Minimum: 0 <br />Optional: \{\} <br /> |


#### ScalableComponentSpec



ScalableComponentSpec provides deployment template and scaling fields.
Embedded by scalable components (WebServer, CeleryWorker, CeleryFlower,
WebsocketServer, McpServer). Non-scalable components (CeleryBeat, Init)
use DeploymentTemplate or PodTemplate directly.



_Appears in:_
- [CeleryFlowerComponentSpec](#celeryflowercomponentspec)
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [WebServerComponentSpec](#webservercomponentspec)
- [WebsocketServerComponentSpec](#websocketservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Deployment template (Deployment-level configuration). |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod template (Pod and container configuration). |  | Optional: \{\} <br /> |
| `replicas` _integer_ | Desired replica count; overridden by autoscaling when active. Defaults to spec.replicas if unset. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | HorizontalPodAutoscaler configuration. When set, the HPA manages replica count. Overrides spec.autoscaling. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget for protecting availability during voluntary disruptions. Overrides spec.podDisruptionBudget. |  | Optional: \{\} <br /> |


#### ServiceAccountSpec



ServiceAccountSpec defines ServiceAccount configuration.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `create` _boolean_ | When true (default), the operator creates a ServiceAccount. When false, it references an existing one. |  | Optional: \{\} <br /> |
| `name` _string_ | ServiceAccount name. Created by the operator when create=true; must pre-exist when create=false. |  | Optional: \{\} <br /> |
| `annotations` _object (keys:string, values:string)_ | ServiceAccount annotations (e.g., for IAM role bindings on cloud platforms). |  | Optional: \{\} <br /> |


#### ServiceMonitorSpec



ServiceMonitorSpec defines the ServiceMonitor configuration.



_Appears in:_
- [MonitoringSpec](#monitoringspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `interval` _string_ | Scrape interval (e.g., "30s"). How often Prometheus scrapes the web server metrics endpoint. | 30s | Optional: \{\} <br /> |
| `labels` _object (keys:string, values:string)_ | Labels for Prometheus ServiceMonitor discovery (must match your Prometheus selector). |  | Optional: \{\} <br /> |
| `scrapeTimeout` _string_ | Maximum time to wait for a scrape response before timing out. |  | Optional: \{\} <br /> |


#### Superset



Superset is the top-level resource representing a complete Superset deployment.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `Superset` | | |
| `metadata` _[ObjectMeta](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetSpec](#supersetspec)_ |  |  |  |
| `status` _[SupersetStatus](#supersetstatus)_ |  |  |  |


#### SupersetCeleryBeat



SupersetCeleryBeat is the Schema for the supersetcelerybeats API.
It manages the Celery beat scheduler Deployment (singleton).





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetCeleryBeat` | | |
| `metadata` _[ObjectMeta](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetCeleryBeatSpec](#supersetcelerybeatspec)_ |  |  |  |
| `status` _[SupersetCeleryBeatStatus](#supersetcelerybeatstatus)_ |  |  |  |


#### SupersetCeleryBeatSpec



SupersetCeleryBeatSpec is the fully-resolved, flat spec for celery beat.
Beat is always a singleton (1 replica).



_Appears in:_
- [SupersetCeleryBeat](#supersetcelerybeat)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Container image configuration. |  |  |
| `replicas` _integer_ | Desired replica count. | 1 | Optional: \{\} <br /> |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Fully-resolved deployment template. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Fully-resolved pod template. |  | Optional: \{\} <br /> |
| `serviceAccountName` _string_ | ServiceAccountName to set on the pod. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling configuration. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget configuration. |  | Optional: \{\} <br /> |
| `config` _string_ | The fully rendered superset_config.py content. |  | Optional: \{\} <br /> |
| `configChecksum` _string_ | Checksum for rolling restarts. |  | Optional: \{\} <br /> |


#### SupersetCeleryBeatStatus



SupersetCeleryBeatStatus defines the observed state of SupersetCeleryBeat.



_Appears in:_
- [SupersetCeleryBeat](#supersetcelerybeat)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ready` _string_ | "2/2" format showing ready vs desired replicas. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### SupersetCeleryFlower



SupersetCeleryFlower is the Schema for the supersetceleryflowers API.
It manages the Celery Flower monitoring UI Deployment and Service.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetCeleryFlower` | | |
| `metadata` _[ObjectMeta](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetCeleryFlowerSpec](#supersetceleryflowerspec)_ |  |  |  |
| `status` _[SupersetCeleryFlowerStatus](#supersetceleryflowerstatus)_ |  |  |  |


#### SupersetCeleryFlowerSpec



SupersetCeleryFlowerSpec is the fully-resolved, flat spec for celery flower.



_Appears in:_
- [SupersetCeleryFlower](#supersetceleryflower)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Container image configuration. |  |  |
| `replicas` _integer_ | Desired replica count. | 1 | Optional: \{\} <br /> |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Fully-resolved deployment template. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Fully-resolved pod template. |  | Optional: \{\} <br /> |
| `serviceAccountName` _string_ | ServiceAccountName to set on the pod. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling configuration. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget configuration. |  | Optional: \{\} <br /> |
| `config` _string_ | The fully rendered superset_config.py content. |  | Optional: \{\} <br /> |
| `configChecksum` _string_ | Checksum for rolling restarts. |  | Optional: \{\} <br /> |
| `service` _[ComponentServiceSpec](#componentservicespec)_ | Service configuration. |  | Optional: \{\} <br /> |


#### SupersetCeleryFlowerStatus



SupersetCeleryFlowerStatus defines the observed state of SupersetCeleryFlower.



_Appears in:_
- [SupersetCeleryFlower](#supersetceleryflower)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ready` _string_ | "2/2" format showing ready vs desired replicas. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### SupersetCeleryWorker



SupersetCeleryWorker is the Schema for the supersetceleryworkers API.
It manages the Celery worker Deployment.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetCeleryWorker` | | |
| `metadata` _[ObjectMeta](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetCeleryWorkerSpec](#supersetceleryworkerspec)_ |  |  |  |
| `status` _[SupersetCeleryWorkerStatus](#supersetceleryworkerstatus)_ |  |  |  |


#### SupersetCeleryWorkerSpec



SupersetCeleryWorkerSpec is the fully-resolved, flat spec for a celery worker.



_Appears in:_
- [SupersetCeleryWorker](#supersetceleryworker)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Container image configuration. |  |  |
| `replicas` _integer_ | Desired replica count. | 1 | Optional: \{\} <br /> |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Fully-resolved deployment template. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Fully-resolved pod template. |  | Optional: \{\} <br /> |
| `serviceAccountName` _string_ | ServiceAccountName to set on the pod. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling configuration. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget configuration. |  | Optional: \{\} <br /> |
| `config` _string_ | The fully rendered superset_config.py content. |  | Optional: \{\} <br /> |
| `configChecksum` _string_ | Checksum for rolling restarts. |  | Optional: \{\} <br /> |


#### SupersetCeleryWorkerStatus



SupersetCeleryWorkerStatus defines the observed state of SupersetCeleryWorker.



_Appears in:_
- [SupersetCeleryWorker](#supersetceleryworker)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ready` _string_ | "2/2" format showing ready vs desired replicas. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### SupersetLifecycleTask



SupersetLifecycleTask is the Schema for the supersetlifecycletasks API.
It manages lifecycle tasks (database migrations, init commands).





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetLifecycleTask` | | |
| `metadata` _[ObjectMeta](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetLifecycleTaskSpec](#supersetlifecycletaskspec)_ |  |  |  |
| `status` _[SupersetLifecycleTaskStatus](#supersetlifecycletaskstatus)_ |  |  |  |


#### SupersetLifecycleTaskSpec



SupersetLifecycleTaskSpec defines the fully-resolved spec for a lifecycle task.



_Appears in:_
- [SupersetLifecycleTask](#supersetlifecycletask)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Container image configuration. |  |  |
| `replicas` _integer_ | Desired replica count. | 1 | Optional: \{\} <br /> |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Fully-resolved deployment template. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Fully-resolved pod template. |  | Optional: \{\} <br /> |
| `serviceAccountName` _string_ | ServiceAccountName to set on the pod. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling configuration. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget configuration. |  | Optional: \{\} <br /> |
| `type` _string_ | Type identifies the task purpose. Future task types will require schema additions. |  | Enum: [Migrate Init] <br /> |
| `command` _string array_ | Command to execute in the task pod. |  |  |
| `config` _string_ | Rendered superset_config.py content. |  | Optional: \{\} <br /> |
| `configChecksum` _string_ | Config checksum for detecting changes that require re-run. |  | Optional: \{\} <br /> |
| `timeout` _[Duration](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration)_ | Maximum timeout per task pod attempt. |  | Optional: \{\} <br /> |
| `maxRetries` _integer_ | Maximum number of retries before permanent failure. | 3 | Minimum: 1 <br />Optional: \{\} <br /> |
| `podRetention` _[PodRetentionSpec](#podretentionspec)_ | Pod retention policy for completed task pods. |  | Optional: \{\} <br /> |


#### SupersetLifecycleTaskStatus



SupersetLifecycleTaskStatus reports the status of a lifecycle task.



_Appears in:_
- [SupersetLifecycleTask](#supersetlifecycletask)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `state` _string_ |  |  | Enum: [Pending Running Complete Failed] <br />Optional: \{\} <br /> |
| `podName` _string_ |  |  | Optional: \{\} <br /> |
| `startedAt` _[Time](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Time)_ |  |  | Optional: \{\} <br /> |
| `completedAt` _[Time](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Time)_ |  |  | Optional: \{\} <br /> |
| `duration` _string_ |  |  | Optional: \{\} <br /> |
| `attempts` _integer_ |  |  | Optional: \{\} <br /> |
| `image` _string_ |  |  | Optional: \{\} <br /> |
| `message` _string_ |  |  | Optional: \{\} <br /> |
| `configChecksum` _string_ | Config checksum that was active when the task last completed.<br />Used to detect changes and trigger re-execution. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition) array_ |  |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ |  |  | Optional: \{\} <br /> |


#### SupersetMcpServer



SupersetMcpServer is the Schema for the supersetmcpservers API.
It manages the FastMCP server Deployment and Service.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetMcpServer` | | |
| `metadata` _[ObjectMeta](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetMcpServerSpec](#supersetmcpserverspec)_ |  |  |  |
| `status` _[SupersetMcpServerStatus](#supersetmcpserverstatus)_ |  |  |  |


#### SupersetMcpServerSpec



SupersetMcpServerSpec is the fully-resolved, flat spec for the MCP server.



_Appears in:_
- [SupersetMcpServer](#supersetmcpserver)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Container image configuration. |  |  |
| `replicas` _integer_ | Desired replica count. | 1 | Optional: \{\} <br /> |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Fully-resolved deployment template. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Fully-resolved pod template. |  | Optional: \{\} <br /> |
| `serviceAccountName` _string_ | ServiceAccountName to set on the pod. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling configuration. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget configuration. |  | Optional: \{\} <br /> |
| `config` _string_ | The fully rendered superset_config.py content. |  | Optional: \{\} <br /> |
| `configChecksum` _string_ | Checksum for rolling restarts. |  | Optional: \{\} <br /> |
| `service` _[ComponentServiceSpec](#componentservicespec)_ | Service configuration. |  | Optional: \{\} <br /> |


#### SupersetMcpServerStatus



SupersetMcpServerStatus defines the observed state of SupersetMcpServer.



_Appears in:_
- [SupersetMcpServer](#supersetmcpserver)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ready` _string_ | "2/2" format showing ready vs desired replicas. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### SupersetSpec



SupersetSpec defines the desired state of a Superset deployment.



_Appears in:_
- [Superset](#superset)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Image configuration inherited by all components. |  |  |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Deployment template defaults inherited by all components (field-level merge). |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod template defaults inherited by all components (field-level merge). |  | Optional: \{\} <br /> |
| `replicas` _integer_ | Default replica count for all scalable components; per-component replicas override this. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Default autoscaling for all scalable components (component-level overrides this). |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | Default pod disruption budget for all scalable components (component-level overrides this). |  | Optional: \{\} <br /> |
| `environment` _string_ | Environment mode: "dev" or "prod". Controls validation strictness.<br />In prod mode, CRD validation rejects plain text secrets (secretKey, metastore.uri, metastore.password). | prod | Enum: [dev prod] <br />Optional: \{\} <br /> |
| `secretKey` _string_ | Plain text secret key for session signing. Only allowed in dev mode.<br />In prod, use secretKeyFrom to reference a Kubernetes Secret. |  | Optional: \{\} <br /> |
| `secretKeyFrom` _[SecretKeySelector](https://pkg.go.dev/k8s.io/api/core/v1#SecretKeySelector)_ | Reference to a Secret key containing the secret key for session signing.<br />Mutually exclusive with secretKey. |  | Optional: \{\} <br /> |
| `metastore` _[MetastoreSpec](#metastorespec)_ | Metastore database connection configuration. |  | Optional: \{\} <br /> |
| `valkey` _[ValkeySpec](#valkeyspec)_ | Valkey cache, broker, and results backend configuration. |  | Optional: \{\} <br /> |
| `config` _string_ | Raw Python appended after operator-generated superset_config.py. |  | Optional: \{\} <br /> |
| `sqlaEngineOptions` _[SQLAlchemyEngineOptionsSpec](#sqlalchemyengineoptionsspec)_ | SQLAlchemy engine options for connection pooling. Inherited by all Python<br />components; per-component sqlaEngineOptions overrides this entirely.<br />When unset, the operator computes balanced defaults per component. |  | Optional: \{\} <br /> |
| `webServer` _[WebServerComponentSpec](#webservercomponentspec)_ | Web server (gunicorn) component. Presence enables it; absence disables. |  | Optional: \{\} <br /> |
| `celeryWorker` _[CeleryWorkerComponentSpec](#celeryworkercomponentspec)_ | Celery async task worker component. Requires Valkey for broker/backend. |  | Optional: \{\} <br /> |
| `celeryBeat` _[CeleryBeatComponentSpec](#celerybeatcomponentspec)_ | Celery periodic task scheduler (singleton, always 1 replica). Requires Valkey. |  | Optional: \{\} <br /> |
| `celeryFlower` _[CeleryFlowerComponentSpec](#celeryflowercomponentspec)_ | Celery Flower monitoring UI component. |  | Optional: \{\} <br /> |
| `websocketServer` _[WebsocketServerComponentSpec](#websocketservercomponentspec)_ | WebSocket server for real-time updates (Node.js, no Python config). |  | Optional: \{\} <br /> |
| `mcpServer` _[McpServerComponentSpec](#mcpservercomponentspec)_ | FastMCP server component for AI tooling integration. |  | Optional: \{\} <br /> |
| `lifecycle` _[LifecycleSpec](#lifecyclespec)_ | Lifecycle configuration (database migration, init, upgrade mode). |  | Optional: \{\} <br /> |
| `networking` _[NetworkingSpec](#networkingspec)_ | Networking configuration (Ingress or Gateway API). |  | Optional: \{\} <br /> |
| `monitoring` _[MonitoringSpec](#monitoringspec)_ | Monitoring configuration. |  | Optional: \{\} <br /> |
| `networkPolicy` _[NetworkPolicySpec](#networkpolicyspec)_ | Network policy configuration. |  | Optional: \{\} <br /> |
| `serviceAccount` _[ServiceAccountSpec](#serviceaccountspec)_ | ServiceAccount configuration. |  | Optional: \{\} <br /> |
| `suspend` _boolean_ | Suspend stops reconciliation when true. |  | Optional: \{\} <br /> |
| `forceReload` _string_ | ForceReload is an opaque string injected into all pod templates. Changing its value<br />triggers a rolling restart of all components. Use a timestamp or incrementing value<br />(e.g. "2026-04-24T12:00:00Z") to force a restart after rotating referenced Secrets. |  | Optional: \{\} <br /> |


#### SupersetStatus



SupersetStatus defines the observed state of Superset.



_Appears in:_
- [Superset](#superset)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `conditions` _[Condition](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition) array_ |  |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ |  |  | Optional: \{\} <br /> |
| `components` _[ComponentStatusMap](#componentstatusmap)_ |  |  | Optional: \{\} <br /> |
| `lifecycle` _[LifecycleStatus](#lifecyclestatus)_ | Lifecycle tracks the current lifecycle state. |  | Optional: \{\} <br /> |
| `lastLifecycleImage` _string_ | Last image (repository:tag) that successfully completed the lifecycle.<br />Used to detect image changes on subsequent reconciles. |  | Optional: \{\} <br /> |
| `version` _string_ |  |  | Optional: \{\} <br /> |
| `configChecksum` _string_ |  |  | Optional: \{\} <br /> |
| `phase` _string_ | High-level phase. |  | Enum: [Initializing Upgrading Running Degraded Suspended Blocked AwaitingApproval] <br />Optional: \{\} <br /> |


#### SupersetWebServer



SupersetWebServer is the Schema for the supersetwebservers API.
It manages the Superset web server (gunicorn) Deployment.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetWebServer` | | |
| `metadata` _[ObjectMeta](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetWebServerSpec](#supersetwebserverspec)_ |  |  |  |
| `status` _[SupersetWebServerStatus](#supersetwebserverstatus)_ |  |  |  |


#### SupersetWebServerSpec



SupersetWebServerSpec is the fully-resolved, flat spec for a web server.



_Appears in:_
- [SupersetWebServer](#supersetwebserver)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Container image configuration. |  |  |
| `replicas` _integer_ | Desired replica count. | 1 | Optional: \{\} <br /> |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Fully-resolved deployment template. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Fully-resolved pod template. |  | Optional: \{\} <br /> |
| `serviceAccountName` _string_ | ServiceAccountName to set on the pod. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling configuration. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget configuration. |  | Optional: \{\} <br /> |
| `config` _string_ | The fully rendered superset_config.py content. |  | Optional: \{\} <br /> |
| `configChecksum` _string_ | Checksum stamped as pod template annotation for rolling restarts. |  | Optional: \{\} <br /> |
| `service` _[ComponentServiceSpec](#componentservicespec)_ | Service configuration. |  | Optional: \{\} <br /> |


#### SupersetWebServerStatus



SupersetWebServerStatus defines the observed state of SupersetWebServer.



_Appears in:_
- [SupersetWebServer](#supersetwebserver)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ready` _string_ | "2/2" format showing ready vs desired replicas. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### SupersetWebsocketServer



SupersetWebsocketServer is the Schema for the supersetwebsocketservers API.
It manages the Superset websocket server Deployment.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetWebsocketServer` | | |
| `metadata` _[ObjectMeta](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetWebsocketServerSpec](#supersetwebsocketserverspec)_ |  |  |  |
| `status` _[SupersetWebsocketServerStatus](#supersetwebsocketserverstatus)_ |  |  |  |


#### SupersetWebsocketServerSpec



SupersetWebsocketServerSpec is the fully-resolved, flat spec for a websocket server.
The websocket server is a Node.js application — it does NOT use superset_config.py.



_Appears in:_
- [SupersetWebsocketServer](#supersetwebsocketserver)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Container image configuration. |  |  |
| `replicas` _integer_ | Desired replica count. | 1 | Optional: \{\} <br /> |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Fully-resolved deployment template. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Fully-resolved pod template. |  | Optional: \{\} <br /> |
| `serviceAccountName` _string_ | ServiceAccountName to set on the pod. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling configuration. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget configuration. |  | Optional: \{\} <br /> |
| `service` _[ComponentServiceSpec](#componentservicespec)_ | Service configuration. |  | Optional: \{\} <br /> |


#### SupersetWebsocketServerStatus



SupersetWebsocketServerStatus defines the observed state of SupersetWebsocketServer.



_Appears in:_
- [SupersetWebsocketServer](#supersetwebsocketserver)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ready` _string_ | "2/2" format showing ready vs desired replicas. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### TaskRefStatus



TaskRefStatus holds the projected status summary of a lifecycle task.



_Appears in:_
- [LifecycleStatus](#lifecyclestatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `state` _string_ |  |  | Enum: [Pending Running Complete Failed] <br />Optional: \{\} <br /> |
| `startedAt` _[Time](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Time)_ |  |  | Optional: \{\} <br /> |
| `completedAt` _[Time](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Time)_ |  |  | Optional: \{\} <br /> |
| `duration` _string_ |  |  | Optional: \{\} <br /> |
| `attempts` _integer_ |  |  | Optional: \{\} <br /> |
| `podName` _string_ |  |  | Optional: \{\} <br /> |
| `image` _string_ |  |  | Optional: \{\} <br /> |
| `message` _string_ |  |  | Optional: \{\} <br /> |


#### UpgradeContext



UpgradeContext tracks the current upgrade operation.



_Appears in:_
- [LifecycleStatus](#lifecyclestatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `fromVersion` _string_ |  |  | Optional: \{\} <br /> |
| `toVersion` _string_ |  |  | Optional: \{\} <br /> |
| `direction` _string_ |  |  | Enum: [Upgrade Downgrade Unknown] <br />Optional: \{\} <br /> |
| `startedAt` _[Time](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Time)_ |  |  | Optional: \{\} <br /> |


#### ValkeyCacheSpec



ValkeyCacheSpec tunes a Superset Flask-Caching backend backed by Valkey.



_Appears in:_
- [ValkeySpec](#valkeyspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `disabled` _boolean_ | Disable this cache section. When true, the operator does not render<br />this config — Superset falls back to its built-in default. |  | Optional: \{\} <br /> |
| `database` _integer_ | Valkey database number. |  | Optional: \{\} <br /> |
| `keyPrefix` _string_ | Cache key prefix. |  | Optional: \{\} <br /> |
| `defaultTimeout` _integer_ | Default cache timeout in seconds. |  | Optional: \{\} <br /> |


#### ValkeyCelerySpec



ValkeyCelerySpec tunes a Celery Valkey connection.



_Appears in:_
- [ValkeySpec](#valkeyspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `disabled` _boolean_ | Disable this Celery backend. When true, the operator does not render this config. |  | Optional: \{\} <br /> |
| `database` _integer_ | Valkey database number. |  | Optional: \{\} <br /> |


#### ValkeyResultsBackendSpec



ValkeyResultsBackendSpec tunes the SQL Lab async results backend.



_Appears in:_
- [ValkeySpec](#valkeyspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `disabled` _boolean_ | Disable the results backend. When true, the operator does not render this config. |  | Optional: \{\} <br /> |
| `database` _integer_ | Valkey database number. |  | Optional: \{\} <br /> |
| `keyPrefix` _string_ | Cache key prefix for results. |  | Optional: \{\} <br /> |


#### ValkeySSLSpec



ValkeySSLSpec configures TLS for the Valkey connection.



_Appears in:_
- [ValkeySpec](#valkeyspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `certRequired` _string_ | Certificate verification mode. | required | Enum: [required optional none] <br />Optional: \{\} <br /> |
| `keyFile` _string_ | Path to the client private key file (for mTLS). |  | Optional: \{\} <br /> |
| `certFile` _string_ | Path to the client certificate file (for mTLS). |  | Optional: \{\} <br /> |
| `caCertFile` _string_ | Path to the CA certificate file for server verification. |  | Optional: \{\} <br /> |


#### ValkeySpec



ValkeySpec configures Valkey as the shared cache backend, Celery message
broker, and SQL Lab results backend for Superset. When set, all sections
are enabled with sensible defaults — only host is required.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `host` _string_ | Valkey server hostname. |  |  |
| `port` _integer_ | Valkey server port. | 6379 | Optional: \{\} <br /> |
| `password` _string_ | Plain text password. Only allowed in dev mode — use passwordFrom in prod. |  | Optional: \{\} <br /> |
| `passwordFrom` _[SecretKeySelector](https://pkg.go.dev/k8s.io/api/core/v1#SecretKeySelector)_ | Reference to a Secret key containing the Valkey password.<br />Mutually exclusive with password. |  | Optional: \{\} <br /> |
| `ssl` _[ValkeySSLSpec](#valkeysslspec)_ | SSL/TLS configuration. When set, enables SSL for the Valkey connection. |  | Optional: \{\} <br /> |
| `cache` _[ValkeyCacheSpec](#valkeycachespec)_ | General cache (CACHE_CONFIG). Default: db=1, prefix="superset_", timeout=300s. |  | Optional: \{\} <br /> |
| `dataCache` _[ValkeyCacheSpec](#valkeycachespec)_ | Data/query results cache (DATA_CACHE_CONFIG). Default: db=2, prefix="superset_data_", timeout=86400s. |  | Optional: \{\} <br /> |
| `filterStateCache` _[ValkeyCacheSpec](#valkeycachespec)_ | Dashboard filter state cache (FILTER_STATE_CACHE_CONFIG). Default: db=3, prefix="superset_filter_", timeout=3600s. |  | Optional: \{\} <br /> |
| `exploreFormDataCache` _[ValkeyCacheSpec](#valkeycachespec)_ | Chart builder form state cache (EXPLORE_FORM_DATA_CACHE_CONFIG). Default: db=4, prefix="superset_explore_", timeout=3600s. |  | Optional: \{\} <br /> |
| `thumbnailCache` _[ValkeyCacheSpec](#valkeycachespec)_ | Thumbnail cache (THUMBNAIL_CACHE_CONFIG). Default: db=5, prefix="superset_thumbnail_", timeout=3600s. |  | Optional: \{\} <br /> |
| `celeryBroker` _[ValkeyCelerySpec](#valkeyceleryspec)_ | Celery broker (CeleryConfig.broker_url). Default: db=0. |  | Optional: \{\} <br /> |
| `celeryResultBackend` _[ValkeyCelerySpec](#valkeyceleryspec)_ | Celery result backend (CeleryConfig.result_backend). Default: db=0. |  | Optional: \{\} <br /> |
| `resultsBackend` _[ValkeyResultsBackendSpec](#valkeyresultsbackendspec)_ | SQL Lab async results backend (RESULTS_BACKEND). Default: db=6, prefix="superset_results_". |  | Optional: \{\} <br /> |


#### WebServerComponentSpec



WebServerComponentSpec defines the web server component on the parent CRD.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Deployment template (Deployment-level configuration). |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod template (Pod and container configuration). |  | Optional: \{\} <br /> |
| `replicas` _integer_ | Desired replica count; overridden by autoscaling when active. Defaults to spec.replicas if unset. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | HorizontalPodAutoscaler configuration. When set, the HPA manages replica count. Overrides spec.autoscaling. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget for protecting availability during voluntary disruptions. Overrides spec.podDisruptionBudget. |  | Optional: \{\} <br /> |
| `image` _[ImageOverrideSpec](#imageoverridespec)_ | Image tag and/or repository overrides; inherits from spec.image if unset. |  | Optional: \{\} <br /> |
| `config` _string_ | Per-component raw Python appended after top-level config. |  | Optional: \{\} <br /> |
| `service` _[ComponentServiceSpec](#componentservicespec)_ | Service configuration (type, port, annotations). |  | Optional: \{\} <br /> |
| `gunicorn` _[GunicornSpec](#gunicornspec)_ | Gunicorn worker configuration. Controls worker processes, threads, and related parameters. |  | Optional: \{\} <br /> |
| `sqlaEngineOptions` _[SQLAlchemyEngineOptionsSpec](#sqlalchemyengineoptionsspec)_ | Per-component SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely). |  | Optional: \{\} <br /> |


#### WebsocketServerComponentSpec



WebsocketServerComponentSpec defines the websocket server component on the parent CRD.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Deployment template (Deployment-level configuration). |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod template (Pod and container configuration). |  | Optional: \{\} <br /> |
| `replicas` _integer_ | Desired replica count; overridden by autoscaling when active. Defaults to spec.replicas if unset. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | HorizontalPodAutoscaler configuration. When set, the HPA manages replica count. Overrides spec.autoscaling. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget for protecting availability during voluntary disruptions. Overrides spec.podDisruptionBudget. |  | Optional: \{\} <br /> |
| `image` _[ImageOverrideSpec](#imageoverridespec)_ | Image tag and/or repository overrides; inherits from spec.image if unset. |  | Optional: \{\} <br /> |
| `service` _[ComponentServiceSpec](#componentservicespec)_ | Service configuration (type, port, annotations). |  | Optional: \{\} <br /> |


