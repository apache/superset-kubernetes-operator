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
- [SupersetInit](#supersetinit)
- [SupersetMcpServer](#supersetmcpserver)
- [SupersetWebServer](#supersetwebserver)
- [SupersetWebsocketServer](#supersetwebsocketserver)



#### AdminUserSpec



AdminUserSpec defines admin user credentials for dev-mode initialization.



_Appears in:_
- [InitSpec](#initspec)

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
- [SupersetInitSpec](#supersetinitspec)
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
| `metrics` _[MetricSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#metricspec-v2-autoscaling) array_ | Metrics for the HPA. Supports CPU, memory, custom, and external metrics.<br />When empty, Kubernetes defaults to 80% average CPU utilization. |  | Optional: \{\} <br /> |


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
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta) array_ | Standard conditions. |  | Optional: \{\} <br /> |
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
| `type` _[ServiceType](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#servicetype-v1-core)_ | Service type (ClusterIP, NodePort, LoadBalancer). | ClusterIP | Enum: [ClusterIP NodePort LoadBalancer] <br />Optional: \{\} <br /> |
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
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#resourcerequirements-v1-core)_ | Resource requirements (CPU, memory). |  | Optional: \{\} <br /> |
| `env` _[EnvVar](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#envvar-v1-core) array_ | Environment variables. |  | Optional: \{\} <br /> |
| `envFrom` _[EnvFromSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#envfromsource-v1-core) array_ | Environment variable sources (ConfigMaps, Secrets). |  | Optional: \{\} <br /> |
| `volumeMounts` _[VolumeMount](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#volumemount-v1-core) array_ | Volume mounts for the main container. |  | Optional: \{\} <br /> |
| `ports` _[ContainerPort](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#containerport-v1-core) array_ | Container ports. Replaces operator defaults when set. |  | Optional: \{\} <br /> |
| `securityContext` _[SecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#securitycontext-v1-core)_ | Container-level security context. |  | Optional: \{\} <br /> |
| `command` _string array_ | Container entrypoint override. |  | Optional: \{\} <br /> |
| `args` _string array_ | Container arguments override. |  | Optional: \{\} <br /> |
| `livenessProbe` _[Probe](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#probe-v1-core)_ | Liveness probe; container is restarted when the probe fails. |  | Optional: \{\} <br /> |
| `readinessProbe` _[Probe](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#probe-v1-core)_ | Readiness probe; pod is removed from Service endpoints when the probe fails. |  | Optional: \{\} <br /> |
| `startupProbe` _[Probe](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#probe-v1-core)_ | Startup probe; liveness and readiness probes are deferred until this probe succeeds. |  | Optional: \{\} <br /> |
| `lifecycle` _[Lifecycle](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#lifecycle-v1-core)_ | Lifecycle hooks for the main container. |  | Optional: \{\} <br /> |


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
- [SupersetInitSpec](#supersetinitspec)
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
| `strategy` _[DeploymentStrategy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#deploymentstrategy-v1-apps)_ | Deployment update strategy. |  | Optional: \{\} <br /> |


#### FlatComponentSpec



FlatComponentSpec defines the common fields for all fully-resolved child specs.
This is embedded (inlined) in each child CRD spec type.



_Appears in:_
- [SupersetCeleryBeatSpec](#supersetcelerybeatspec)
- [SupersetCeleryFlowerSpec](#supersetceleryflowerspec)
- [SupersetCeleryWorkerSpec](#supersetceleryworkerspec)
- [SupersetInitSpec](#supersetinitspec)
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


#### ImageOverrideSpec



ImageOverrideSpec allows a component to override specific image fields.
Unset fields inherit from spec.image.



_Appears in:_
- [CeleryBeatComponentSpec](#celerybeatcomponentspec)
- [CeleryFlowerComponentSpec](#celeryflowercomponentspec)
- [CeleryWorkerComponentSpec](#celeryworkercomponentspec)
- [ComponentSpec](#componentspec)
- [InitSpec](#initspec)
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
- [SupersetInitSpec](#supersetinitspec)
- [SupersetMcpServerSpec](#supersetmcpserverspec)
- [SupersetSpec](#supersetspec)
- [SupersetWebServerSpec](#supersetwebserverspec)
- [SupersetWebsocketServerSpec](#supersetwebsocketserverspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `repository` _string_ | Container image repository. | apachesuperset.docker.scarf.sh/apache/superset | Optional: \{\} <br /> |
| `tag` _string_ | Image tag. |  | MinLength: 1 <br /> |
| `pullPolicy` _[PullPolicy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#pullpolicy-v1-core)_ | Image pull policy (IfNotPresent, Always, Never). | IfNotPresent | Optional: \{\} <br /> |
| `pullSecrets` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#localobjectreference-v1-core) array_ | References to Secrets for pulling images from private registries. |  | Optional: \{\} <br /> |


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
| `pathType` _[PathType](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#pathtype-v1-networking)_ |  | Prefix | Optional: \{\} <br /> |


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
| `tls` _[IngressTLS](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#ingresstls-v1-networking) array_ | TLS configuration (certificate secrets and hostnames). |  | Optional: \{\} <br /> |


#### InitSpec



InitSpec defines initialization configuration. The init pod runs a single
command (default: superset db upgrade && superset init) that must complete
before any component is deployed.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Pod and container template for the init pod. |  | Optional: \{\} <br /> |
| `config` _string_ | Per-init raw Python appended after top-level config. |  | Optional: \{\} <br /> |
| `image` _[ImageOverrideSpec](#imageoverridespec)_ | Image override for init pods. |  | Optional: \{\} <br /> |
| `command` _string array_ | Command to execute during initialization. When empty, the operator<br />constructs the command from base steps (db upgrade + init) and any<br />adminUser/loadExamples options. Mutually exclusive with adminUser<br />and loadExamples. |  |  |
| `disabled` _boolean_ | Set to true to skip initialization entirely. |  | Optional: \{\} <br /> |
| `adminUser` _[AdminUserSpec](#adminuserspec)_ | Admin user to create during initialization. Only allowed in dev mode.<br />When set, the operator appends a superset fab create-admin step to the init command. |  | Optional: \{\} <br /> |
| `loadExamples` _boolean_ | Load example dashboards and data during initialization. Only allowed in dev mode.<br />When true, the operator appends a superset load-examples step to the init command. |  | Optional: \{\} <br /> |
| `timeout` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#duration-v1-meta)_ | Maximum timeout for the init pod. Default: 300s. |  | Optional: \{\} <br /> |
| `maxRetries` _integer_ | Maximum number of retries before permanent failure. Default: 3. | 3 | Minimum: 1 <br />Optional: \{\} <br /> |
| `podRetention` _[PodRetentionSpec](#podretentionspec)_ | Pod retention policy for completed init pods. |  | Optional: \{\} <br /> |


#### InitTaskStatus



InitTaskStatus reports the status of the init task.



_Appears in:_
- [SupersetStatus](#supersetstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `state` _string_ |  |  | Enum: [Pending Running Complete Failed] <br />Optional: \{\} <br /> |
| `revision` _string_ |  |  | Optional: \{\} <br /> |
| `previousRevision` _string_ |  |  | Optional: \{\} <br /> |
| `startedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta)_ |  |  | Optional: \{\} <br /> |
| `completedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta)_ |  |  | Optional: \{\} <br /> |
| `duration` _string_ |  |  | Optional: \{\} <br /> |
| `attempts` _integer_ |  |  | Optional: \{\} <br /> |
| `podName` _string_ |  |  | Optional: \{\} <br /> |
| `image` _string_ |  |  | Optional: \{\} <br /> |
| `message` _string_ |  |  | Optional: \{\} <br /> |


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


#### MetastoreSpec



MetastoreSpec defines the database connection for Superset's metastore.
Either a URI (passthrough) or structured fields (host, database, etc.) can be used.
They are mutually exclusive.



_Appears in:_
- [SupersetSpec](#supersetspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `uri` _string_ | Full SQLAlchemy database URI. Mutually exclusive with structured fields and uriFrom.<br />In prod mode, CRD validation rejects plain text URIs — use uriFrom to reference a Kubernetes Secret. |  | Optional: \{\} <br /> |
| `uriFrom` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#secretkeyselector-v1-core)_ | Reference to a Secret key containing the full SQLAlchemy URI.<br />Mutually exclusive with uri and structured fields. |  | Optional: \{\} <br /> |
| `type` _string_ | Database type. Determines the SQLAlchemy driver. | postgresql | Enum: [postgresql mysql] <br />Optional: \{\} <br /> |
| `host` _string_ | Database hostname. |  | Optional: \{\} <br /> |
| `port` _integer_ | Database port. Defaults per driver (5432 for postgresql, 3306 for mysql). |  | Optional: \{\} <br /> |
| `database` _string_ | Database name. |  | Optional: \{\} <br /> |
| `username` _string_ | Database username. |  | Optional: \{\} <br /> |
| `password` _string_ | Database password. In prod mode, CRD validation rejects plain text passwords — use passwordFrom to reference a Kubernetes Secret. |  | Optional: \{\} <br /> |
| `passwordFrom` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#secretkeyselector-v1-core)_ | Reference to a Secret key containing the database password.<br />Mutually exclusive with password. |  | Optional: \{\} <br /> |


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
| `extraIngress` _[NetworkPolicyIngressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#networkpolicyingressrule-v1-networking) array_ | Additional ingress rules appended to the operator-generated NetworkPolicy (e.g., allow traffic from monitoring namespace). |  | Optional: \{\} <br /> |
| `extraEgress` _[NetworkPolicyEgressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#networkpolicyegressrule-v1-networking) array_ | Additional egress rules appended to the operator-generated NetworkPolicy. |  | Optional: \{\} <br /> |


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
- [SupersetInitSpec](#supersetinitspec)
- [SupersetMcpServerSpec](#supersetmcpserverspec)
- [SupersetSpec](#supersetspec)
- [SupersetWebServerSpec](#supersetwebserverspec)
- [SupersetWebsocketServerSpec](#supersetwebsocketserverspec)
- [WebServerComponentSpec](#webservercomponentspec)
- [WebsocketServerComponentSpec](#websocketservercomponentspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `minAvailable` _[IntOrString](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#intorstring-intstr-util)_ | Minimum pods that must remain available during voluntary disruptions. Mutually exclusive with maxUnavailable. |  | Optional: \{\} <br /> |
| `maxUnavailable` _[IntOrString](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#intorstring-intstr-util)_ | Maximum pods allowed to be unavailable during voluntary disruptions. Mutually exclusive with minAvailable. |  | Optional: \{\} <br /> |


#### PodRetentionSpec



PodRetentionSpec defines retention behavior for init pods.



_Appears in:_
- [InitSpec](#initspec)
- [SupersetInitSpec](#supersetinitspec)

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
- [InitSpec](#initspec)
- [McpServerComponentSpec](#mcpservercomponentspec)
- [ScalableComponentSpec](#scalablecomponentspec)
- [SupersetCeleryBeatSpec](#supersetcelerybeatspec)
- [SupersetCeleryFlowerSpec](#supersetceleryflowerspec)
- [SupersetCeleryWorkerSpec](#supersetceleryworkerspec)
- [SupersetInitSpec](#supersetinitspec)
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
| `affinity` _[Affinity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#affinity-v1-core)_ | Pod affinity and anti-affinity rules for scheduling. |  | Optional: \{\} <br /> |
| `tolerations` _[Toleration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#toleration-v1-core) array_ | Tolerations for scheduling on tainted nodes. |  | Optional: \{\} <br /> |
| `nodeSelector` _object (keys:string, values:string)_ | Node labels for constraining pod scheduling. |  | Optional: \{\} <br /> |
| `topologySpreadConstraints` _[TopologySpreadConstraint](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#topologyspreadconstraint-v1-core) array_ | Topology spread constraints for distributing pods across failure domains. |  | Optional: \{\} <br /> |
| `hostAliases` _[HostAlias](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#hostalias-v1-core) array_ | Entries added to /etc/hosts in pod containers. |  | Optional: \{\} <br /> |
| `podSecurityContext` _[PodSecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#podsecuritycontext-v1-core)_ | Pod-level security context (runAsUser, fsGroup, seccomp, etc.). |  | Optional: \{\} <br /> |
| `priorityClassName` _string_ | Priority class name for pod scheduling priority and preemption. |  | Optional: \{\} <br /> |
| `volumes` _[Volume](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#volume-v1-core) array_ | Additional volumes for the pod (mounted via container.volumeMounts). |  | Optional: \{\} <br /> |
| `sidecars` _[Container](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#container-v1-core) array_ | Sidecar containers added alongside the main Superset container. |  | Optional: \{\} <br /> |
| `initContainers` _[Container](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#container-v1-core) array_ | Init containers run before the main container starts. |  | Optional: \{\} <br /> |
| `terminationGracePeriodSeconds` _integer_ | Grace period for pod termination in seconds. |  | Optional: \{\} <br /> |
| `dnsPolicy` _[DNSPolicy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#dnspolicy-v1-core)_ | DNS policy for pods. |  | Optional: \{\} <br /> |
| `dnsConfig` _[PodDNSConfig](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#poddnsconfig-v1-core)_ | Custom DNS configuration for pods. |  | Optional: \{\} <br /> |
| `runtimeClassName` _string_ | RuntimeClass for pods. |  | Optional: \{\} <br /> |
| `shareProcessNamespace` _boolean_ | Share a single process namespace between all containers in a pod. |  | Optional: \{\} <br /> |
| `enableServiceLinks` _boolean_ | Controls whether service environment variables are injected into pods. |  | Optional: \{\} <br /> |
| `container` _[ContainerTemplate](#containertemplate)_ | Main container configuration. |  | Optional: \{\} <br /> |


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
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetSpec](#supersetspec)_ |  |  |  |
| `status` _[SupersetStatus](#supersetstatus)_ |  |  |  |


#### SupersetCeleryBeat



SupersetCeleryBeat is the Schema for the supersetcelerybeats API.
It manages the Celery beat scheduler Deployment (singleton).





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetCeleryBeat` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
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
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### SupersetCeleryFlower



SupersetCeleryFlower is the Schema for the supersetceleryflowers API.
It manages the Celery Flower monitoring UI Deployment and Service.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetCeleryFlower` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
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
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### SupersetCeleryWorker



SupersetCeleryWorker is the Schema for the supersetceleryworkers API.
It manages the Celery worker Deployment.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetCeleryWorker` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
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
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### SupersetInit



SupersetInit is the Schema for the supersetinits API.
It manages the initialization lifecycle (database migrations, init commands).





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetInit` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SupersetInitSpec](#supersetinitspec)_ |  |  |  |
| `status` _[SupersetInitStatus](#supersetinitstatus)_ |  |  |  |


#### SupersetInitSpec



SupersetInitSpec defines the fully-resolved spec for initialization.



_Appears in:_
- [SupersetInit](#supersetinit)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _[ImageSpec](#imagespec)_ | Container image configuration. |  |  |
| `replicas` _integer_ | Desired replica count. | 1 | Optional: \{\} <br /> |
| `deploymentTemplate` _[DeploymentTemplate](#deploymenttemplate)_ | Fully-resolved deployment template. |  | Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | Fully-resolved pod template. |  | Optional: \{\} <br /> |
| `serviceAccountName` _string_ | ServiceAccountName to set on the pod. |  | Optional: \{\} <br /> |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling configuration. |  | Optional: \{\} <br /> |
| `podDisruptionBudget` _[PDBSpec](#pdbspec)_ | PodDisruptionBudget configuration. |  | Optional: \{\} <br /> |
| `config` _string_ | Rendered superset_config.py content. |  | Optional: \{\} <br /> |
| `configChecksum` _string_ | Config checksum for detecting config changes. |  | Optional: \{\} <br /> |
| `maxRetries` _integer_ | Maximum number of retries before permanent failure. | 3 | Minimum: 1 <br />Optional: \{\} <br /> |
| `timeout` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#duration-v1-meta)_ | Maximum timeout per init pod attempt. |  | Optional: \{\} <br /> |
| `podRetention` _[PodRetentionSpec](#podretentionspec)_ | Pod retention policy for completed init pods. |  | Optional: \{\} <br /> |


#### SupersetInitStatus



SupersetInitStatus reports the status of initialization.



_Appears in:_
- [SupersetInit](#supersetinit)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `state` _string_ |  |  | Enum: [Pending Running Complete Failed] <br />Optional: \{\} <br /> |
| `podName` _string_ |  |  | Optional: \{\} <br /> |
| `startedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta)_ |  |  | Optional: \{\} <br /> |
| `completedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta)_ |  |  | Optional: \{\} <br /> |
| `duration` _string_ |  |  | Optional: \{\} <br /> |
| `attempts` _integer_ |  |  | Optional: \{\} <br /> |
| `image` _string_ |  |  | Optional: \{\} <br /> |
| `message` _string_ |  |  | Optional: \{\} <br /> |
| `configChecksum` _string_ | Config checksum that was active when init last completed.<br />Used to detect config changes and trigger re-initialization. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta) array_ |  |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ |  |  | Optional: \{\} <br /> |


#### SupersetMcpServer



SupersetMcpServer is the Schema for the supersetmcpservers API.
It manages the FastMCP server Deployment and Service.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetMcpServer` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
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
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta) array_ | Standard conditions. |  | Optional: \{\} <br /> |
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
| `secretKeyFrom` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#secretkeyselector-v1-core)_ | Reference to a Secret key containing the secret key for session signing.<br />Mutually exclusive with secretKey. |  | Optional: \{\} <br /> |
| `metastore` _[MetastoreSpec](#metastorespec)_ | Metastore database connection configuration. |  | Optional: \{\} <br /> |
| `valkey` _[ValkeySpec](#valkeyspec)_ | Valkey cache, broker, and results backend configuration. |  | Optional: \{\} <br /> |
| `config` _string_ | Raw Python appended after operator-generated superset_config.py. |  | Optional: \{\} <br /> |
| `webServer` _[WebServerComponentSpec](#webservercomponentspec)_ | Web server (gunicorn) component. Presence enables it; absence disables. |  | Optional: \{\} <br /> |
| `celeryWorker` _[CeleryWorkerComponentSpec](#celeryworkercomponentspec)_ | Celery async task worker component. Requires Valkey for broker/backend. |  | Optional: \{\} <br /> |
| `celeryBeat` _[CeleryBeatComponentSpec](#celerybeatcomponentspec)_ | Celery periodic task scheduler (singleton, always 1 replica). Requires Valkey. |  | Optional: \{\} <br /> |
| `celeryFlower` _[CeleryFlowerComponentSpec](#celeryflowercomponentspec)_ | Celery Flower monitoring UI component. |  | Optional: \{\} <br /> |
| `websocketServer` _[WebsocketServerComponentSpec](#websocketservercomponentspec)_ | WebSocket server for real-time updates (Node.js, no Python config). |  | Optional: \{\} <br /> |
| `mcpServer` _[McpServerComponentSpec](#mcpservercomponentspec)_ | FastMCP server component for AI tooling integration. |  | Optional: \{\} <br /> |
| `init` _[InitSpec](#initspec)_ | Initialization configuration. |  | Optional: \{\} <br /> |
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
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta) array_ |  |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ |  |  | Optional: \{\} <br /> |
| `components` _[ComponentStatusMap](#componentstatusmap)_ |  |  | Optional: \{\} <br /> |
| `init` _[InitTaskStatus](#inittaskstatus)_ |  |  | Optional: \{\} <br /> |
| `version` _string_ |  |  | Optional: \{\} <br /> |
| `migrationRevision` _string_ |  |  | Optional: \{\} <br /> |
| `configChecksum` _string_ |  |  | Optional: \{\} <br /> |
| `phase` _string_ | High-level phase. |  | Enum: [Initializing Running Degraded Suspended] <br />Optional: \{\} <br /> |


#### SupersetWebServer



SupersetWebServer is the Schema for the supersetwebservers API.
It manages the Superset web server (gunicorn) Deployment.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetWebServer` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
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
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


#### SupersetWebsocketServer



SupersetWebsocketServer is the Schema for the supersetwebsocketservers API.
It manages the Superset websocket server Deployment.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `superset.apache.org/v1alpha1` | | |
| `kind` _string_ | `SupersetWebsocketServer` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
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
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta) array_ | Standard conditions. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration for leader election consistency. |  | Optional: \{\} <br /> |


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
| `passwordFrom` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#secretkeyselector-v1-core)_ | Reference to a Secret key containing the Valkey password.<br />Mutually exclusive with password. |  | Optional: \{\} <br /> |
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


