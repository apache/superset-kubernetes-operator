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
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// SupersetSpec defines the desired state of a Superset deployment.
// +kubebuilder:validation:XValidation:rule="has(self.secretKey) != has(self.secretKeyFrom)",message="exactly one of secretKey (dev only) or secretKeyFrom must be set"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'dev') || !has(self.secretKey)",message="secretKey is only allowed when environment is dev; use secretKeyFrom in prod"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'dev') || !has(self.metastore) || !has(self.metastore.uri)",message="metastore.uri is only allowed when environment is dev; use metastore.uriFrom in prod"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'dev') || !has(self.metastore) || !has(self.metastore.password)",message="metastore.password is only allowed when environment is dev; use metastore.passwordFrom in prod"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'dev') || !has(self.valkey) || !has(self.valkey.password)",message="valkey.password is only allowed when environment is dev; use valkey.passwordFrom in prod"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'dev') || !has(self.init) || !has(self.init.adminUser)",message="init.adminUser is only allowed when environment is dev"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'dev') || !has(self.init) || !has(self.init.loadExamples)",message="init.loadExamples is only allowed when environment is dev"
// +kubebuilder:validation:XValidation:rule="!has(self.networking) || !has(self.networking.ingress) || has(self.webServer)",message="spec.networking.ingress requires spec.webServer to be set (all Ingress rules target the web server service)"
// +kubebuilder:validation:XValidation:rule="!has(self.networking) || !has(self.networking.gateway) || has(self.webServer) || has(self.websocketServer) || has(self.mcpServer) || has(self.celeryFlower)",message="spec.networking.gateway requires at least one component with a routable service (webServer, websocketServer, mcpServer, or celeryFlower)"
// +kubebuilder:validation:XValidation:rule="!has(self.monitoring) || !has(self.monitoring.serviceMonitor) || has(self.webServer)",message="spec.monitoring.serviceMonitor requires spec.webServer to be set (scrapes the web server service)"
type SupersetSpec struct {
	// Image configuration inherited by all components.
	Image ImageSpec `json:"image"`

	// Deployment template defaults inherited by all components (field-level merge).
	// +optional
	DeploymentTemplate *DeploymentTemplate `json:"deploymentTemplate,omitempty"`
	// Pod template defaults inherited by all components (field-level merge).
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`
	// Default replica count for all scalable components; per-component replicas override this.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
	// Default autoscaling for all scalable components (component-level overrides this).
	// +optional
	Autoscaling *AutoscalingSpec `json:"autoscaling,omitempty"`
	// Default pod disruption budget for all scalable components (component-level overrides this).
	// +optional
	PodDisruptionBudget *PDBSpec `json:"podDisruptionBudget,omitempty"`

	// Environment mode: "dev" or "prod". Controls validation strictness.
	// In prod mode, CRD validation rejects plain text secrets (secretKey, metastore.uri, metastore.password).
	// +optional
	// +kubebuilder:validation:Enum=dev;prod
	// +kubebuilder:default=prod
	Environment *string `json:"environment,omitempty"`

	// Plain text secret key for session signing. Only allowed in dev mode.
	// In prod, use secretKeyFrom to reference a Kubernetes Secret.
	// +optional
	SecretKey *string `json:"secretKey,omitempty"`

	// Reference to a Secret key containing the secret key for session signing.
	// Mutually exclusive with secretKey.
	// +optional
	SecretKeyFrom *corev1.SecretKeySelector `json:"secretKeyFrom,omitempty"`

	// Metastore database connection configuration.
	// +optional
	Metastore *MetastoreSpec `json:"metastore,omitempty"`

	// Valkey cache, broker, and results backend configuration.
	// +optional
	Valkey *ValkeySpec `json:"valkey,omitempty"`

	// Raw Python appended after operator-generated superset_config.py.
	// +optional
	Config *string `json:"config,omitempty"`

	// SQLAlchemy engine options for connection pooling. Inherited by all Python
	// components; per-component sqlaEngineOptions overrides this entirely.
	// When unset, the operator computes balanced defaults per component.
	// +optional
	SQLAlchemyEngineOptions *SQLAlchemyEngineOptionsSpec `json:"sqlaEngineOptions,omitempty"`

	// Web server (gunicorn) component. Presence enables it; absence disables.
	// +optional
	WebServer *WebServerComponentSpec `json:"webServer,omitempty"`
	// Celery async task worker component. Requires Valkey for broker/backend.
	// +optional
	CeleryWorker *CeleryWorkerComponentSpec `json:"celeryWorker,omitempty"`
	// Celery periodic task scheduler (singleton, always 1 replica). Requires Valkey.
	// +optional
	CeleryBeat *CeleryBeatComponentSpec `json:"celeryBeat,omitempty"`
	// Celery Flower monitoring UI component.
	// +optional
	CeleryFlower *CeleryFlowerComponentSpec `json:"celeryFlower,omitempty"`
	// WebSocket server for real-time updates (Node.js, no Python config).
	// +optional
	WebsocketServer *WebsocketServerComponentSpec `json:"websocketServer,omitempty"`
	// FastMCP server component for AI tooling integration.
	// +optional
	McpServer *McpServerComponentSpec `json:"mcpServer,omitempty"`

	// Initialization configuration.
	// +optional
	Init *InitSpec `json:"init,omitempty"`

	// Networking configuration (Ingress or Gateway API).
	// +optional
	Networking *NetworkingSpec `json:"networking,omitempty"`

	// Monitoring configuration.
	// +optional
	Monitoring *MonitoringSpec `json:"monitoring,omitempty"`

	// Network policy configuration.
	// +optional
	NetworkPolicy *NetworkPolicySpec `json:"networkPolicy,omitempty"`

	// ServiceAccount configuration.
	// +optional
	ServiceAccount *ServiceAccountSpec `json:"serviceAccount,omitempty"`

	// Suspend stops reconciliation when true.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// ForceReload is an opaque string injected into all pod templates. Changing its value
	// triggers a rolling restart of all components. Use a timestamp or incrementing value
	// (e.g. "2026-04-24T12:00:00Z") to force a restart after rotating referenced Secrets.
	// +optional
	ForceReload string `json:"forceReload,omitempty"`
}

// --- Component specs (on parent CRD) ---

// WebServerComponentSpec defines the web server component on the parent CRD.
type WebServerComponentSpec struct {
	ScalableComponentSpec `json:",inline"`
	ComponentSpec         `json:",inline"`
	// Per-component raw Python appended after top-level config.
	// +optional
	Config *string `json:"config,omitempty"`
	// Service configuration (type, port, annotations).
	// +optional
	Service *ComponentServiceSpec `json:"service,omitempty"`
	// Gunicorn worker configuration. Controls worker processes, threads, and related parameters.
	// +optional
	Gunicorn *GunicornSpec `json:"gunicorn,omitempty"`
	// Per-component SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely).
	// +optional
	SQLAlchemyEngineOptions *SQLAlchemyEngineOptionsSpec `json:"sqlaEngineOptions,omitempty"`
}

// CeleryWorkerComponentSpec defines the celery worker component on the parent CRD.
type CeleryWorkerComponentSpec struct {
	ScalableComponentSpec `json:",inline"`
	ComponentSpec         `json:",inline"`
	// Per-component raw Python appended after top-level config.
	// +optional
	Config *string `json:"config,omitempty"`
	// Celery worker execution configuration. Controls concurrency, pool type, and related parameters.
	// +optional
	Celery *CeleryWorkerProcessSpec `json:"celery,omitempty"`
	// Per-component SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely).
	// +optional
	SQLAlchemyEngineOptions *SQLAlchemyEngineOptionsSpec `json:"sqlaEngineOptions,omitempty"`
}

// CeleryBeatComponentSpec defines the celery beat component on the parent CRD.
// The controller forces replicas=1 regardless of spec.
type CeleryBeatComponentSpec struct {
	// Deployment-level overrides (strategy, revision history). Always enforces replicas=1.
	// +optional
	DeploymentTemplate *DeploymentTemplate `json:"deploymentTemplate,omitempty"`
	// Pod and container template for Celery beat pods.
	// +optional
	PodTemplate   *PodTemplate `json:"podTemplate,omitempty"`
	ComponentSpec `json:",inline"`
	// Per-component raw Python appended after top-level config.
	// +optional
	Config *string `json:"config,omitempty"`
	// Per-component SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely).
	// +optional
	SQLAlchemyEngineOptions *SQLAlchemyEngineOptionsSpec `json:"sqlaEngineOptions,omitempty"`
}

// CeleryFlowerComponentSpec defines the celery flower component on the parent CRD.
type CeleryFlowerComponentSpec struct {
	ScalableComponentSpec `json:",inline"`
	ComponentSpec         `json:",inline"`
	// Per-component raw Python appended after top-level config.
	// +optional
	Config *string `json:"config,omitempty"`
	// Service configuration (type, port, annotations).
	// +optional
	Service *ComponentServiceSpec `json:"service,omitempty"`
}

// WebsocketServerComponentSpec defines the websocket server component on the parent CRD.
type WebsocketServerComponentSpec struct {
	ScalableComponentSpec `json:",inline"`
	ComponentSpec         `json:",inline"`
	// Service configuration (type, port, annotations).
	// +optional
	Service *ComponentServiceSpec `json:"service,omitempty"`
}

// McpServerComponentSpec defines the MCP server component on the parent CRD.
type McpServerComponentSpec struct {
	ScalableComponentSpec `json:",inline"`
	ComponentSpec         `json:",inline"`
	// Per-component raw Python appended after top-level config.
	// +optional
	Config *string `json:"config,omitempty"`
	// Service configuration (type, port, annotations).
	// +optional
	Service *ComponentServiceSpec `json:"service,omitempty"`
	// Per-component SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely).
	// +optional
	SQLAlchemyEngineOptions *SQLAlchemyEngineOptionsSpec `json:"sqlaEngineOptions,omitempty"`
}

// --- Init spec ---

// InitSpec defines initialization configuration. The init pod runs a single
// command (default: superset db upgrade && superset init) that must complete
// before any component is deployed.
// +kubebuilder:validation:XValidation:rule="!has(self.command) || size(self.command) == 0 || (!has(self.adminUser) && !has(self.loadExamples))",message="command is mutually exclusive with adminUser and loadExamples"
type InitSpec struct {
	// Pod and container template for the init pod.
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`

	// Per-init raw Python appended after top-level config.
	// +optional
	Config *string `json:"config,omitempty"`

	// Image override for init pods.
	// +optional
	Image *ImageOverrideSpec `json:"image,omitempty"`

	// Command to execute during initialization. When empty, the operator
	// constructs the command from base steps (db upgrade + init) and any
	// adminUser/loadExamples options. Mutually exclusive with adminUser
	// and loadExamples.
	// +listType=atomic
	Command []string `json:"command,omitempty"`

	// Set to true to skip initialization entirely.
	// +optional
	Disabled *bool `json:"disabled,omitempty"`

	// Admin user to create during initialization. Only allowed in dev mode.
	// When set, the operator appends a superset fab create-admin step to the init command.
	// +optional
	AdminUser *AdminUserSpec `json:"adminUser,omitempty"`

	// Load example dashboards and data during initialization. Only allowed in dev mode.
	// When true, the operator appends a superset load-examples step to the init command.
	// +optional
	LoadExamples *bool `json:"loadExamples,omitempty"`

	// Per-component SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely).
	// +optional
	SQLAlchemyEngineOptions *SQLAlchemyEngineOptionsSpec `json:"sqlaEngineOptions,omitempty"`

	// Maximum timeout for the init pod. Default: 300s.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// Maximum number of retries before permanent failure. Default: 3.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// Pod retention policy for completed init pods.
	// +optional
	PodRetention *PodRetentionSpec `json:"podRetention,omitempty"`
}

// AdminUserSpec defines admin user credentials for dev-mode initialization.
type AdminUserSpec struct {
	// Admin username.
	// +optional
	// +kubebuilder:default="admin"
	Username *string `json:"username,omitempty"`
	// Admin password. Stored as plain-text env var in dev mode.
	// +optional
	// +kubebuilder:default="admin"
	Password *string `json:"password,omitempty"`
	// Admin first name.
	// +optional
	// +kubebuilder:default="Superset"
	FirstName *string `json:"firstName,omitempty"`
	// Admin last name.
	// +optional
	// +kubebuilder:default="Admin"
	LastName *string `json:"lastName,omitempty"`
	// Admin email.
	// +optional
	// +kubebuilder:default="admin@example.com"
	Email *string `json:"email,omitempty"`
}

// PodRetentionSpec defines retention behavior for init pods.
type PodRetentionSpec struct {
	// Retention policy: Delete removes pods after completion, Retain keeps all,
	// RetainOnFailure keeps only failed pods for debugging.
	// +optional
	// +kubebuilder:validation:Enum=Delete;Retain;RetainOnFailure
	// +kubebuilder:default=Delete
	Policy *string `json:"policy,omitempty"`
}

// --- Networking spec ---

// NetworkingSpec defines external access configuration.
// +kubebuilder:validation:XValidation:rule="!(has(self.gateway) && has(self.ingress))",message="gateway and ingress are mutually exclusive"
type NetworkingSpec struct {
	// Gateway API HTTPRoute configuration.
	// +optional
	Gateway *GatewaySpec `json:"gateway,omitempty"`

	// Ingress configuration.
	// +optional
	Ingress *IngressSpec `json:"ingress,omitempty"`
}

// GatewaySpec defines HTTPRoute configuration.
type GatewaySpec struct {
	// Reference to the Gateway resource to attach the HTTPRoute to.
	GatewayRef gatewayv1.ParentReference `json:"gatewayRef"`
	// Hostnames for the HTTPRoute (e.g., "superset.example.com").
	// +optional
	Hostnames []gatewayv1.Hostname `json:"hostnames,omitempty"`
	// HTTPRoute annotations.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// HTTPRoute labels.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// IngressSpec defines Ingress configuration.
type IngressSpec struct {
	// IngressClass name (e.g., "nginx") that determines which controller processes this Ingress.
	// +optional
	ClassName *string `json:"className,omitempty"`
	// Primary hostname for the Ingress rule (e.g., "superset.example.com").
	// +optional
	Host string `json:"host,omitempty"`
	// Ingress annotations (e.g., for TLS, auth, or controller-specific configuration).
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// Ingress labels.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Additional host/path rules beyond the primary host.
	// +optional
	Hosts []IngressHost `json:"hosts,omitempty"`
	// TLS configuration (certificate secrets and hostnames).
	// +optional
	TLS []networkingv1.IngressTLS `json:"tls,omitempty"`
}

// IngressHost defines a host rule for the Ingress.
type IngressHost struct {
	// +optional
	Host string `json:"host,omitempty"`
	// +optional
	Paths []IngressPath `json:"paths,omitempty"`
}

// IngressPath defines a path rule for an Ingress host.
type IngressPath struct {
	// +kubebuilder:default="/"
	Path string `json:"path,omitempty"`
	// +optional
	// +kubebuilder:default="Prefix"
	PathType *networkingv1.PathType `json:"pathType,omitempty"`
}

// --- Monitoring and NetworkPolicy ---

// MonitoringSpec defines Prometheus monitoring configuration.
type MonitoringSpec struct {
	// +optional
	ServiceMonitor *ServiceMonitorSpec `json:"serviceMonitor,omitempty"`
}

// ServiceMonitorSpec defines the ServiceMonitor configuration.
type ServiceMonitorSpec struct {
	// Scrape interval (e.g., "30s"). How often Prometheus scrapes the web server metrics endpoint.
	// +optional
	// +kubebuilder:default="30s"
	Interval *string `json:"interval,omitempty"`
	// Labels for Prometheus ServiceMonitor discovery (must match your Prometheus selector).
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Maximum time to wait for a scrape response before timing out.
	// +optional
	ScrapeTimeout *string `json:"scrapeTimeout,omitempty"`
}

// NetworkPolicySpec defines network segmentation configuration.
type NetworkPolicySpec struct {
	// Additional ingress rules appended to the operator-generated NetworkPolicy (e.g., allow traffic from monitoring namespace).
	// +optional
	ExtraIngress []networkingv1.NetworkPolicyIngressRule `json:"extraIngress,omitempty"`
	// Additional egress rules appended to the operator-generated NetworkPolicy.
	// +optional
	ExtraEgress []networkingv1.NetworkPolicyEgressRule `json:"extraEgress,omitempty"`
}

// --- ServiceAccount ---

// ServiceAccountSpec defines ServiceAccount configuration.
type ServiceAccountSpec struct {
	// When true (default), the operator creates a ServiceAccount. When false, it references an existing one.
	// +optional
	Create *bool `json:"create,omitempty"`
	// ServiceAccount name. Created by the operator when create=true; must pre-exist when create=false.
	// +optional
	Name string `json:"name,omitempty"`
	// ServiceAccount annotations (e.g., for IAM role bindings on cloud platforms).
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// --- Parent status ---

// SupersetStatus defines the observed state of Superset.
type SupersetStatus struct {
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Components *ComponentStatusMap `json:"components,omitempty"`
	// +optional
	Init *InitTaskStatus `json:"init,omitempty"`
	// +optional
	Version string `json:"version,omitempty"`
	// +optional
	MigrationRevision string `json:"migrationRevision,omitempty"`
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`
	// High-level phase.
	// +optional
	// +kubebuilder:validation:Enum=Initializing;Running;Degraded;Suspended
	Phase string `json:"phase,omitempty"`
}

// ComponentStatusMap holds status for each component.
type ComponentStatusMap struct {
	// +optional
	WebServer *ComponentRefStatus `json:"webServer,omitempty"`
	// +optional
	CeleryWorker *ComponentRefStatus `json:"celeryWorker,omitempty"`
	// +optional
	CeleryBeat *ComponentRefStatus `json:"celeryBeat,omitempty"`
	// +optional
	CeleryFlower *ComponentRefStatus `json:"celeryFlower,omitempty"`
	// +optional
	WebsocketServer *ComponentRefStatus `json:"websocketServer,omitempty"`
	// +optional
	McpServer *ComponentRefStatus `json:"mcpServer,omitempty"`
}

// ComponentRefStatus holds the status summary of a child component.
type ComponentRefStatus struct {
	// "2/2" format showing ready vs desired replicas.
	Ready string `json:"ready"`
	// Reference to the child CR.
	Ref string `json:"ref"`
	// Config checksum on the child.
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`
}

// InitTaskStatus reports the status of the init task.
type InitTaskStatus struct {
	// +optional
	// +kubebuilder:validation:Enum=Pending;Running;Complete;Failed
	State string `json:"state,omitempty"`
	// +optional
	Revision string `json:"revision,omitempty"`
	// +optional
	PreviousRevision string `json:"previousRevision,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	Duration string `json:"duration,omitempty"`
	// +optional
	Attempts int32 `json:"attempts,omitempty"`
	// +optional
	PodName string `json:"podName,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.status.version`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Available",type=string,JSONPath=`.status.conditions[?(@.type=="Available")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 63",message="metadata.name must be at most 63 characters (label values and Service names are limited to 63 characters)"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a valid DNS label (lowercase alphanumeric and hyphens only, no dots or underscores); the operator derives Service names from CR names"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.webServer) || size(self.metadata.name) <= 52",message="metadata.name must be at most 52 characters when webServer is enabled (sub-resource suffix '-web-server' is 11 chars)"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.celeryWorker) || size(self.metadata.name) <= 49",message="metadata.name must be at most 49 characters when celeryWorker is enabled (sub-resource suffix '-celery-worker' is 14 chars)"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.celeryBeat) || size(self.metadata.name) <= 51",message="metadata.name must be at most 51 characters when celeryBeat is enabled (sub-resource suffix '-celery-beat' is 12 chars)"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.celeryFlower) || size(self.metadata.name) <= 49",message="metadata.name must be at most 49 characters when celeryFlower is enabled (sub-resource suffix '-celery-flower' is 14 chars)"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.websocketServer) || size(self.metadata.name) <= 46",message="metadata.name must be at most 46 characters when websocketServer is enabled (sub-resource suffix '-websocket-server' is 17 chars)"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.mcpServer) || size(self.metadata.name) <= 52",message="metadata.name must be at most 52 characters when mcpServer is enabled (sub-resource suffix '-mcp-server' is 11 chars)"
// +kubebuilder:validation:XValidation:rule="(has(self.spec.init) && has(self.spec.init.disabled) && self.spec.init.disabled == true) || size(self.metadata.name) <= 58",message="metadata.name must be at most 58 characters when init is enabled (sub-resource suffix '-init' is 5 chars)"

// Superset is the top-level resource representing a complete Superset deployment.
type Superset struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SupersetSpec   `json:"spec,omitempty"`
	Status            SupersetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SupersetList contains a list of Superset.
type SupersetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Superset `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Superset{}, &SupersetList{})
}
