/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Condition type constants for Superset resources.
const (
	ConditionTypeReady        = "Ready"
	ConditionTypeProgressing  = "Progressing"
	ConditionTypeDegraded     = "Degraded"
	ConditionTypeSuspended    = "Suspended"
	ConditionTypeInitComplete = "InitComplete"
	ConditionTypeAvailable    = "Available"
)

// --- Image types ---

// ImageSpec defines the container image configuration.
type ImageSpec struct {
	// Container image repository.
	// +optional
	// +kubebuilder:default="apachesuperset.docker.scarf.sh/apache/superset"
	Repository string `json:"repository,omitempty"`

	// Image tag.
	// +kubebuilder:validation:MinLength=1
	Tag string `json:"tag"`

	// Image pull policy (IfNotPresent, Always, Never).
	// +optional
	// +kubebuilder:default=IfNotPresent
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`

	// References to Secrets for pulling images from private registries.
	// +optional
	PullSecrets []corev1.LocalObjectReference `json:"pullSecrets,omitempty"`
}

// ImageOverrideSpec allows a component to override specific image fields.
// Unset fields inherit from spec.image.
type ImageOverrideSpec struct {
	// Override the image tag for this component; inherits from spec.image.tag if omitted.
	// +optional
	Tag *string `json:"tag,omitempty"`
	// Override the image repository for this component; inherits from spec.image.repository if omitted.
	// +optional
	Repository *string `json:"repository,omitempty"`
}

// --- Metastore types ---

// MetastoreSpec defines the database connection for Superset's metastore.
// Either a URI (passthrough) or structured fields (host, database, etc.) can be used.
// They are mutually exclusive.
// +kubebuilder:validation:XValidation:rule="!(has(self.uri) && has(self.uriFrom))",message="uri and uriFrom are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!(has(self.password) && has(self.passwordFrom))",message="password and passwordFrom are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!(has(self.uri) && (has(self.host) || has(self.database) || has(self.username) || has(self.password) || has(self.passwordFrom) || has(self.port)))",message="uri and structured fields are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!(has(self.uriFrom) && (has(self.host) || has(self.database) || has(self.username) || has(self.password) || has(self.passwordFrom) || has(self.port)))",message="uriFrom and structured fields are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!((has(self.database) || has(self.username) || has(self.password) || has(self.passwordFrom) || has(self.port)) && !has(self.host))",message="structured fields (database, username, password, passwordFrom, port) require host to be set"
// +kubebuilder:validation:XValidation:rule="!has(self.host) || (has(self.database) && has(self.username))",message="structured metastore requires database and username when host is set"
type MetastoreSpec struct {
	// Full SQLAlchemy database URI. Mutually exclusive with structured fields and uriFrom.
	// In prod mode, CRD validation rejects plain text URIs — use uriFrom to reference a Kubernetes Secret.
	// +optional
	URI *string `json:"uri,omitempty"`

	// Reference to a Secret key containing the full SQLAlchemy URI.
	// Mutually exclusive with uri and structured fields.
	// +optional
	URIFrom *corev1.SecretKeySelector `json:"uriFrom,omitempty"`

	// Database type. Determines the SQLAlchemy driver.
	// +optional
	// +kubebuilder:validation:Enum=postgresql;mysql
	// +kubebuilder:default=postgresql
	Type *string `json:"type,omitempty"`

	// Database hostname.
	// +optional
	Host *string `json:"host,omitempty"`

	// Database port. Defaults per driver (5432 for postgresql, 3306 for mysql).
	// +optional
	Port *int32 `json:"port,omitempty"`

	// Database name.
	// +optional
	Database *string `json:"database,omitempty"`

	// Database username.
	// +optional
	Username *string `json:"username,omitempty"`

	// Database password. In prod mode, CRD validation rejects plain text passwords — use passwordFrom to reference a Kubernetes Secret.
	// +optional
	Password *string `json:"password,omitempty"`

	// Reference to a Secret key containing the database password.
	// Mutually exclusive with password.
	// +optional
	PasswordFrom *corev1.SecretKeySelector `json:"passwordFrom,omitempty"`
}

// --- Valkey cache configuration ---

// ValkeySpec configures Valkey as the shared cache backend, Celery message
// broker, and SQL Lab results backend for Superset. When set, all sections
// are enabled with sensible defaults — only host is required.
// +kubebuilder:validation:XValidation:rule="!(has(self.password) && has(self.passwordFrom))",message="password and passwordFrom are mutually exclusive"
type ValkeySpec struct {
	// Valkey server hostname.
	Host string `json:"host"`

	// Valkey server port.
	// +optional
	// +kubebuilder:default=6379
	Port *int32 `json:"port,omitempty"`

	// Plain text password. Only allowed in dev mode — use passwordFrom in prod.
	// +optional
	Password *string `json:"password,omitempty"`

	// Reference to a Secret key containing the Valkey password.
	// Mutually exclusive with password.
	// +optional
	PasswordFrom *corev1.SecretKeySelector `json:"passwordFrom,omitempty"`

	// SSL/TLS configuration. When set, enables SSL for the Valkey connection.
	// +optional
	SSL *ValkeySSLSpec `json:"ssl,omitempty"`

	// General cache (CACHE_CONFIG). Default: db=1, prefix="superset_", timeout=300s.
	// +optional
	Cache *ValkeyCacheSpec `json:"cache,omitempty"`

	// Data/query results cache (DATA_CACHE_CONFIG). Default: db=2, prefix="superset_data_", timeout=86400s.
	// +optional
	DataCache *ValkeyCacheSpec `json:"dataCache,omitempty"`

	// Dashboard filter state cache (FILTER_STATE_CACHE_CONFIG). Default: db=3, prefix="superset_filter_", timeout=3600s.
	// +optional
	FilterStateCache *ValkeyCacheSpec `json:"filterStateCache,omitempty"`

	// Chart builder form state cache (EXPLORE_FORM_DATA_CACHE_CONFIG). Default: db=4, prefix="superset_explore_", timeout=3600s.
	// +optional
	ExploreFormDataCache *ValkeyCacheSpec `json:"exploreFormDataCache,omitempty"`

	// Thumbnail cache (THUMBNAIL_CACHE_CONFIG). Default: db=5, prefix="superset_thumbnail_", timeout=3600s.
	// +optional
	ThumbnailCache *ValkeyCacheSpec `json:"thumbnailCache,omitempty"`

	// Celery broker (CeleryConfig.broker_url). Default: db=0.
	// +optional
	CeleryBroker *ValkeyCelerySpec `json:"celeryBroker,omitempty"`

	// Celery result backend (CeleryConfig.result_backend). Default: db=0.
	// +optional
	CeleryResultBackend *ValkeyCelerySpec `json:"celeryResultBackend,omitempty"`

	// SQL Lab async results backend (RESULTS_BACKEND). Default: db=6, prefix="superset_results_".
	// +optional
	ResultsBackend *ValkeyResultsBackendSpec `json:"resultsBackend,omitempty"`
}

// ValkeySSLSpec configures TLS for the Valkey connection.
type ValkeySSLSpec struct {
	// Certificate verification mode.
	// +optional
	// +kubebuilder:validation:Enum=required;optional;none
	// +kubebuilder:default=required
	CertRequired *string `json:"certRequired,omitempty"`

	// Path to the client private key file (for mTLS).
	// +optional
	KeyFile *string `json:"keyFile,omitempty"`

	// Path to the client certificate file (for mTLS).
	// +optional
	CertFile *string `json:"certFile,omitempty"`

	// Path to the CA certificate file for server verification.
	// +optional
	CACertFile *string `json:"caCertFile,omitempty"`
}

// ValkeyCacheSpec tunes a Superset Flask-Caching backend backed by Valkey.
type ValkeyCacheSpec struct {
	// Disable this cache section. When true, the operator does not render
	// this config — Superset falls back to its built-in default.
	// +optional
	Disabled *bool `json:"disabled,omitempty"`

	// Valkey database number.
	// +optional
	Database *int32 `json:"database,omitempty"`

	// Cache key prefix.
	// +optional
	KeyPrefix *string `json:"keyPrefix,omitempty"`

	// Default cache timeout in seconds.
	// +optional
	DefaultTimeout *int32 `json:"defaultTimeout,omitempty"`
}

// ValkeyCelerySpec tunes a Celery Valkey connection.
type ValkeyCelerySpec struct {
	// Disable this Celery backend. When true, the operator does not render this config.
	// +optional
	Disabled *bool `json:"disabled,omitempty"`

	// Valkey database number.
	// +optional
	Database *int32 `json:"database,omitempty"`
}

// ValkeyResultsBackendSpec tunes the SQL Lab async results backend.
type ValkeyResultsBackendSpec struct {
	// Disable the results backend. When true, the operator does not render this config.
	// +optional
	Disabled *bool `json:"disabled,omitempty"`

	// Valkey database number.
	// +optional
	Database *int32 `json:"database,omitempty"`

	// Cache key prefix for results.
	// +optional
	KeyPrefix *string `json:"keyPrefix,omitempty"`
}

// --- Process configuration types ---

// GunicornSpec configures Gunicorn worker parameters for the web server.
// Fields controlled by presets: workers, threads, workerClass.
// All other fields have static defaults independent of preset.
// +kubebuilder:validation:XValidation:rule="!has(self.threads) || self.threads <= 1 || !has(self.workerClass) || self.workerClass == 'gthread'",message="threads > 1 requires workerClass=gthread"
type GunicornSpec struct {
	// Preset controlling workers, threads, and workerClass defaults.
	// Individual fields override preset-computed values.
	// +optional
	// +kubebuilder:validation:Enum=disabled;conservative;balanced;performance;aggressive
	Preset *string `json:"preset,omitempty"`

	// Number of Gunicorn worker processes.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Workers *int32 `json:"workers,omitempty"`

	// Number of threads per worker (only effective with gthread worker class).
	// +optional
	// +kubebuilder:validation:Minimum=1
	Threads *int32 `json:"threads,omitempty"`

	// Gunicorn worker class.
	// +optional
	// +kubebuilder:validation:Enum=sync;gthread;gevent;eventlet
	WorkerClass *string `json:"workerClass,omitempty"`

	// Request timeout in seconds.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Timeout *int32 `json:"timeout,omitempty"`

	// Keep-alive timeout in seconds for waiting for requests on a connection.
	// +optional
	// +kubebuilder:validation:Minimum=0
	KeepAlive *int32 `json:"keepAlive,omitempty"`

	// Maximum requests per worker before recycling (0 = disabled).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxRequests *int32 `json:"maxRequests,omitempty"`

	// Random jitter added to maxRequests to prevent thundering herd on worker recycling.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxRequestsJitter *int32 `json:"maxRequestsJitter,omitempty"`

	// Maximum size of HTTP request line in bytes (0 = unlimited).
	// +optional
	// +kubebuilder:validation:Minimum=0
	LimitRequestLine *int32 `json:"limitRequestLine,omitempty"`

	// Maximum size of HTTP request header field in bytes (0 = unlimited).
	// +optional
	// +kubebuilder:validation:Minimum=0
	LimitRequestFieldSize *int32 `json:"limitRequestFieldSize,omitempty"`

	// Gunicorn log level.
	// +optional
	// +kubebuilder:validation:Enum=debug;info;warning;error;critical
	LogLevel *string `json:"logLevel,omitempty"`
}

// CeleryWorkerProcessSpec configures Celery worker execution parameters.
// Fields controlled by presets: concurrency, pool.
// All other fields have static defaults independent of preset.
// +kubebuilder:validation:XValidation:rule="(!has(self.maxTasksPerChild) || self.maxTasksPerChild == 0) || !has(self.pool) || self.pool == 'prefork'",message="maxTasksPerChild only applies to pool=prefork"
// +kubebuilder:validation:XValidation:rule="(!has(self.maxMemoryPerChild) || self.maxMemoryPerChild == 0) || !has(self.pool) || self.pool == 'prefork'",message="maxMemoryPerChild only applies to pool=prefork"
type CeleryWorkerProcessSpec struct {
	// Preset controlling concurrency and pool defaults.
	// Individual fields override preset-computed values.
	// +optional
	// +kubebuilder:validation:Enum=disabled;conservative;balanced;performance;aggressive
	Preset *string `json:"preset,omitempty"`

	// Number of concurrent task workers (maps to celery -c flag).
	// +optional
	// +kubebuilder:validation:Minimum=1
	Concurrency *int32 `json:"concurrency,omitempty"`

	// Celery pool implementation.
	// +optional
	// +kubebuilder:validation:Enum=prefork;threads;gevent;eventlet;solo
	Pool *string `json:"pool,omitempty"`

	// Task distribution optimization strategy.
	// +optional
	// +kubebuilder:validation:Enum=default;fair
	Optimization *string `json:"optimization,omitempty"`

	// Maximum tasks a worker process handles before being replaced (prefork only; 0 = unlimited).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxTasksPerChild *int32 `json:"maxTasksPerChild,omitempty"`

	// Maximum resident memory in bytes per worker before being replaced (prefork only; 0 = disabled).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxMemoryPerChild *int32 `json:"maxMemoryPerChild,omitempty"`

	// Task prefetch multiplier — number of tasks prefetched per worker.
	// +optional
	// +kubebuilder:validation:Minimum=0
	PrefetchMultiplier *int32 `json:"prefetchMultiplier,omitempty"`

	// Soft time limit in seconds — raises SoftTimeLimitExceeded (0 = disabled).
	// +optional
	// +kubebuilder:validation:Minimum=0
	SoftTimeLimit *int32 `json:"softTimeLimit,omitempty"`

	// Hard time limit in seconds — kills the task (0 = disabled).
	// +optional
	// +kubebuilder:validation:Minimum=0
	TimeLimit *int32 `json:"timeLimit,omitempty"`
}

// SQLAlchemyEngineOptionsSpec configures the SQLAlchemy connection pool.
// Fields controlled by presets: poolClass (NullPool vs QueuePool), poolSize, maxOverflow.
// Static defaults: poolRecycle=3600, poolPrePing=false.
type SQLAlchemyEngineOptionsSpec struct {
	// Preset for connection pool behavior. "disabled" suppresses rendering entirely.
	// "conservative" uses NullPool (no persistent connections).
	// "balanced" through "aggressive" use QueuePool with increasing pool sizes.
	// Individual fields override preset-computed values.
	// +optional
	// +kubebuilder:validation:Enum=disabled;conservative;balanced;performance;aggressive
	Preset *string `json:"preset,omitempty"`

	// Number of persistent connections in the pool. Overrides preset calculation.
	// +optional
	// +kubebuilder:validation:Minimum=0
	PoolSize *int32 `json:"poolSize,omitempty"`

	// Maximum overflow connections beyond poolSize (-1 = unlimited).
	// +optional
	MaxOverflow *int32 `json:"maxOverflow,omitempty"`

	// Connection max-age in seconds before recycling.
	// +optional
	// +kubebuilder:validation:Minimum=0
	PoolRecycle *int32 `json:"poolRecycle,omitempty"`

	// Verify connections are alive before use.
	// +optional
	PoolPrePing *bool `json:"poolPrePing,omitempty"`

	// Seconds to wait for a connection from the pool before giving up.
	// +optional
	// +kubebuilder:validation:Minimum=0
	PoolTimeout *int32 `json:"poolTimeout,omitempty"`
}

// --- Deployment template hierarchy ---
//
// Mirrors Kubernetes Deployment → PodTemplateSpec → Container structure.
// Set at top level to apply to all components; per-component values are
// field-level merged with the top-level (component wins on conflict for
// scalars/structs, collections merge by name or append).

// DeploymentTemplate configures Kubernetes Deployment-level fields for
// operator-managed Deployments. Pod and container configuration is in
// the sibling PodTemplate field.
type DeploymentTemplate struct {
	// Number of old ReplicaSets to retain for rollback.
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty"`
	// Minimum seconds a pod must be ready before considered available.
	// +optional
	MinReadySeconds *int32 `json:"minReadySeconds,omitempty"`
	// Maximum seconds for a deployment to make progress before considered failed.
	// +optional
	ProgressDeadlineSeconds *int32 `json:"progressDeadlineSeconds,omitempty"`
	// Deployment update strategy.
	// +optional
	Strategy *appsv1.DeploymentStrategy `json:"strategy,omitempty"`
}

// PodTemplate configures Kubernetes PodSpec fields for the pod template.
type PodTemplate struct {
	// Pod annotations.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// Pod labels (merged with operator-managed labels which cannot be overridden).
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Pod affinity and anti-affinity rules for scheduling.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// Tolerations for scheduling on tainted nodes.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// Node labels for constraining pod scheduling.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Topology spread constraints for distributing pods across failure domains.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	// Entries added to /etc/hosts in pod containers.
	// +optional
	HostAliases []corev1.HostAlias `json:"hostAliases,omitempty"`
	// Pod-level security context (runAsUser, fsGroup, seccomp, etc.).
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`
	// Priority class name for pod scheduling priority and preemption.
	// +optional
	PriorityClassName *string `json:"priorityClassName,omitempty"`
	// Additional volumes for the pod (mounted via container.volumeMounts).
	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`
	// Sidecar containers added alongside the main Superset container.
	// +optional
	Sidecars []corev1.Container `json:"sidecars,omitempty"`
	// Init containers run before the main container starts.
	// +optional
	InitContainers []corev1.Container `json:"initContainers,omitempty"`
	// Grace period for pod termination in seconds.
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// DNS policy for pods.
	// +optional
	DNSPolicy *corev1.DNSPolicy `json:"dnsPolicy,omitempty"`
	// Custom DNS configuration for pods.
	// +optional
	DNSConfig *corev1.PodDNSConfig `json:"dnsConfig,omitempty"`
	// RuntimeClass for pods.
	// +optional
	RuntimeClassName *string `json:"runtimeClassName,omitempty"`
	// Share a single process namespace between all containers in a pod.
	// +optional
	ShareProcessNamespace *bool `json:"shareProcessNamespace,omitempty"`
	// Controls whether service environment variables are injected into pods.
	// +optional
	EnableServiceLinks *bool `json:"enableServiceLinks,omitempty"`
	// Main container configuration.
	// +optional
	Container *ContainerTemplate `json:"container,omitempty"`
}

// ContainerTemplate configures fields on the main Superset container.
type ContainerTemplate struct {
	// Resource requirements (CPU, memory).
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	// Environment variables.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
	// Environment variable sources (ConfigMaps, Secrets).
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`
	// Volume mounts for the main container.
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
	// Container ports. Replaces operator defaults when set.
	// +optional
	Ports []corev1.ContainerPort `json:"ports,omitempty"`
	// Container-level security context.
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`
	// Container entrypoint override.
	// +optional
	Command []string `json:"command,omitempty"`
	// Container arguments override.
	// +optional
	Args []string `json:"args,omitempty"`
	// Liveness probe; container is restarted when the probe fails.
	// +optional
	LivenessProbe *corev1.Probe `json:"livenessProbe,omitempty"`
	// Readiness probe; pod is removed from Service endpoints when the probe fails.
	// +optional
	ReadinessProbe *corev1.Probe `json:"readinessProbe,omitempty"`
	// Startup probe; liveness and readiness probes are deferred until this probe succeeds.
	// +optional
	StartupProbe *corev1.Probe `json:"startupProbe,omitempty"`
	// Lifecycle hooks for the main container.
	// +optional
	Lifecycle *corev1.Lifecycle `json:"lifecycle,omitempty"`
}

// --- Scaling and component types ---

// ScalableComponentSpec provides deployment template and scaling fields.
// Embedded by scalable components (WebServer, CeleryWorker, CeleryFlower,
// WebsocketServer, McpServer). Non-scalable components (CeleryBeat, Init)
// use DeploymentTemplate or PodTemplate directly.
type ScalableComponentSpec struct {
	// Deployment template (Deployment-level configuration).
	// +optional
	DeploymentTemplate *DeploymentTemplate `json:"deploymentTemplate,omitempty"`
	// Pod template (Pod and container configuration).
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`
	// Desired replica count; overridden by autoscaling when active. Defaults to spec.replicas if unset.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
	// HorizontalPodAutoscaler configuration. When set, the HPA manages replica count. Overrides spec.autoscaling.
	// +optional
	Autoscaling *AutoscalingSpec `json:"autoscaling,omitempty"`
	// PodDisruptionBudget for protecting availability during voluntary disruptions. Overrides spec.podDisruptionBudget.
	// +optional
	PodDisruptionBudget *PDBSpec `json:"podDisruptionBudget,omitempty"`
}

// ComponentSpec defines per-component identity fields.
// Embedded by all component specs except InitSpec.
type ComponentSpec struct {
	// Image tag and/or repository overrides; inherits from spec.image if unset.
	// +optional
	Image *ImageOverrideSpec `json:"image,omitempty"`
}

// --- Component service and scaling types ---

// ComponentServiceSpec defines the Service configuration for a component.
type ComponentServiceSpec struct {
	// Service type (ClusterIP, NodePort, LoadBalancer).
	// +optional
	// +kubebuilder:default=ClusterIP
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	Type corev1.ServiceType `json:"type,omitempty"`
	// Service port exposed to clients. Defaults to the component's standard port (8088 for web server, 5555 for Flower).
	// +optional
	Port *int32 `json:"port,omitempty"`
	// Fixed NodePort number when type=NodePort (30000-32767). If omitted, Kubernetes auto-assigns.
	// +optional
	NodePort *int32 `json:"nodePort,omitempty"`
	// Service annotations (e.g., for cloud load balancer configuration).
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// Service labels; merged with operator-managed labels.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// URL path prefix for this component's HTTPRoute rule.
	// Only used when spec.networking.gateway is set.
	// Defaults: /ws (websocket), /mcp (MCP server), /flower (Celery Flower).
	// +optional
	// +kubebuilder:validation:Pattern=`^/[a-zA-Z0-9/_.-]+$`
	GatewayPath *string `json:"gatewayPath,omitempty"`
}

// AutoscalingSpec configures a HorizontalPodAutoscaler.
// +kubebuilder:validation:XValidation:rule="!has(self.minReplicas) || self.maxReplicas >= self.minReplicas",message="maxReplicas must be >= minReplicas"
type AutoscalingSpec struct {
	// Minimum replica count (defaults to 1).
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinReplicas *int32 `json:"minReplicas,omitempty"`
	// Maximum replica count; HPA will not scale above this.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	MaxReplicas int32 `json:"maxReplicas"`
	// Metrics for the HPA. Supports CPU, memory, custom, and external metrics.
	// When empty, Kubernetes defaults to 80% average CPU utilization.
	// +optional
	Metrics []autoscalingv2.MetricSpec `json:"metrics,omitempty"`
}

// PDBSpec configures a PodDisruptionBudget.
// +kubebuilder:validation:XValidation:rule="!(has(self.minAvailable) && has(self.maxUnavailable))",message="minAvailable and maxUnavailable are mutually exclusive"
type PDBSpec struct {
	// Minimum pods that must remain available during voluntary disruptions. Mutually exclusive with maxUnavailable.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
	// Maximum pods allowed to be unavailable during voluntary disruptions. Mutually exclusive with minAvailable.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// --- Flat child spec base (embedded by all child CRDs) ---

// FlatComponentSpec defines the common fields for all fully-resolved child specs.
// This is embedded (inlined) in each child CRD spec type.
type FlatComponentSpec struct {
	// Container image configuration.
	Image ImageSpec `json:"image"`

	// Desired replica count.
	// +optional
	// +kubebuilder:default=1
	Replicas *int32 `json:"replicas,omitempty"`

	// Fully-resolved deployment template.
	// +optional
	DeploymentTemplate *DeploymentTemplate `json:"deploymentTemplate,omitempty"`

	// Fully-resolved pod template.
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`

	// ServiceAccountName to set on the pod.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// Autoscaling configuration.
	// +optional
	Autoscaling *AutoscalingSpec `json:"autoscaling,omitempty"`

	// PodDisruptionBudget configuration.
	// +optional
	PodDisruptionBudget *PDBSpec `json:"podDisruptionBudget,omitempty"`
}

// ChildComponentStatus reports the operational state of a child component.
type ChildComponentStatus struct {
	// "2/2" format showing ready vs desired replicas.
	// +optional
	Ready string `json:"ready,omitempty"`
	// Standard conditions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// ObservedGeneration for leader election consistency.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}
