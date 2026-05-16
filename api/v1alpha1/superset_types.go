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
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'Development') || !has(self.secretKey)",message="secretKey is only allowed when environment is Development; use secretKeyFrom in Production"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'Development') || !has(self.metastore) || !has(self.metastore.uri)",message="metastore.uri is only allowed when environment is Development; use metastore.uriFrom in Production"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'Development') || !has(self.metastore) || !has(self.metastore.password)",message="metastore.password is only allowed when environment is Development; use metastore.passwordFrom in Production"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'Development') || !has(self.valkey) || !has(self.valkey.password)",message="valkey.password is only allowed when environment is Development; use valkey.passwordFrom in Production"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'Development') || !has(self.lifecycle) || !has(self.lifecycle.init) || !has(self.lifecycle.init.adminUser)",message="lifecycle.init.adminUser is only allowed when environment is Development"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'Development') || !has(self.lifecycle) || !has(self.lifecycle.init) || !has(self.lifecycle.init.loadExamples)",message="lifecycle.init.loadExamples is only allowed when environment is Development"
// +kubebuilder:validation:XValidation:rule="!has(self.networking) || !has(self.networking.ingress) || has(self.webServer)",message="spec.networking.ingress requires spec.webServer to be set (all Ingress rules target the web server service)"
// +kubebuilder:validation:XValidation:rule="!has(self.networking) || !has(self.networking.gateway) || has(self.webServer) || has(self.websocketServer) || has(self.mcpServer) || has(self.celeryFlower)",message="spec.networking.gateway requires at least one component with a routable service (webServer, websocketServer, mcpServer, or celeryFlower)"
// +kubebuilder:validation:XValidation:rule="!has(self.monitoring) || !has(self.monitoring.serviceMonitor) || has(self.webServer)",message="spec.monitoring.serviceMonitor requires spec.webServer to be set (scrapes the web server service)"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && (self.environment == 'Development' || self.environment == 'Staging')) || !has(self.lifecycle) || !has(self.lifecycle.clone)",message="lifecycle.clone is only allowed when environment is Development or Staging; cloning performs a destructive DROP DATABASE on the target metastore"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'Development') || !has(self.lifecycle) || !has(self.lifecycle.clone) || !has(self.lifecycle.clone.source) || !has(self.lifecycle.clone.source.password)",message="lifecycle.clone.source.password is only allowed when environment is Development; use lifecycle.clone.source.passwordFrom in Staging"
// +kubebuilder:validation:XValidation:rule="!has(self.lifecycle) || !has(self.lifecycle.clone) || (has(self.metastore) && has(self.metastore.host))",message="lifecycle.clone requires structured metastore configuration (host must be set)"
// +kubebuilder:validation:XValidation:rule="(has(self.environment) && self.environment == 'Development') || !has(self.previousSecretKey)",message="previousSecretKey is only allowed when environment is Development; use previousSecretKeyFrom in Production"
// +kubebuilder:validation:XValidation:rule="!has(self.previousSecretKey) || !has(self.previousSecretKeyFrom)",message="previousSecretKey and previousSecretKeyFrom are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.lifecycle) || !has(self.lifecycle.rotate) || has(self.previousSecretKey) || has(self.previousSecretKeyFrom)",message="lifecycle.rotate requires previousSecretKey (dev) or previousSecretKeyFrom to be set"
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

	// Environment mode: "Development", "Staging", or "Production". Controls validation strictness.
	// In Production mode, CRD validation rejects plain text secrets and disallows cloning.
	// In Staging mode, secrets are enforced (like Production) but cloning is allowed.
	// In Development mode, plain text secrets, cloning, admin user, and load examples are all permitted.
	// +optional
	// +kubebuilder:validation:Enum=Development;Staging;Production
	// +kubebuilder:default=Production
	Environment *string `json:"environment,omitempty"`

	// Plain text secret key for session signing. Only allowed in dev mode.
	// In prod, use secretKeyFrom to reference a Kubernetes Secret.
	// +optional
	SecretKey *string `json:"secretKey,omitempty"`

	// Reference to a Secret key containing the secret key for session signing.
	// Mutually exclusive with secretKey.
	// +optional
	SecretKeyFrom *corev1.SecretKeySelector `json:"secretKeyFrom,omitempty"`

	// Plain text previous secret key for key rotation. Only allowed in dev mode.
	// When set, rendered as PREVIOUS_SECRET_KEY in superset_config.py for all
	// Python components, enabling fallback decryption during key transitions.
	// +optional
	PreviousSecretKey *string `json:"previousSecretKey,omitempty"`

	// Reference to a Secret key containing the previous secret key for rotation.
	// Mutually exclusive with previousSecretKey.
	// +optional
	PreviousSecretKeyFrom *corev1.SecretKeySelector `json:"previousSecretKeyFrom,omitempty"`

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

	// Lifecycle configuration (database migration, init, upgrade mode).
	// +optional
	Lifecycle *LifecycleSpec `json:"lifecycle,omitempty"`

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
// CeleryBeat is a singleton: it always runs with one replica, and the
// inherited `spec.replicas` value (if any) is ignored. The spec exposes no
// replicas field, no autoscaling, and no PodDisruptionBudget by design.
type CeleryBeatComponentSpec struct {
	// Deployment-level overrides (strategy, revision history). Replica count
	// is fixed at 1 by the controller and cannot be overridden.
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

// --- Lifecycle spec ---

// BaseTaskSpec contains fields shared by all lifecycle task types.
type BaseTaskSpec struct {
	// Command override for the task Job.
	// +optional
	// +listType=atomic
	Command []string `json:"command,omitempty"`

	// Trigger is an opaque string. Changing its value forces a re-run of this
	// task and all downstream tasks. Use a timestamp, UUID, or CI build ID.
	// +optional
	Trigger *string `json:"trigger,omitempty"`

	// RequiresDrain controls whether components must be drained before this
	// task runs. When true, the operator removes component workloads before
	// executing the task Job, preventing database connection conflicts. Drain is
	// skipped when the task is already complete for the current checksum, or when
	// no configured component has desired replicas greater than zero.
	// Defaults vary per task type: true for clone, migrate, and rotate; false for init.
	// +optional
	RequiresDrain *bool `json:"requiresDrain,omitempty"`

	// Maximum timeout per attempt.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// Maximum number of retries before permanent failure.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// Disabled skips this task entirely when true.
	// +optional
	Disabled *bool `json:"disabled,omitempty"`
}

// SchedulableBaseTaskSpec extends BaseTaskSpec with cron-based scheduling.
// Tasks that embed this type can be periodically re-executed without external
// triggers. The schedule is additive to the manual trigger field.
type SchedulableBaseTaskSpec struct {
	BaseTaskSpec `json:",inline"`

	// CronSchedule is a 5-field cron expression (minute hour day-of-month month
	// day-of-week) that triggers periodic re-execution of this task and all
	// downstream tasks. When the clock crosses a cron boundary, the task
	// checksum changes and the lifecycle pipeline re-runs.
	//
	// Uses standard cron syntax. Examples: "0 2 * * *" (daily 2 AM UTC),
	// "0 */6 * * *" (every 6 hours), "30 1 * * 1" (Mondays 1:30 AM UTC).
	// Predefined schedules (e.g. "@daily") are not accepted; use the explicit
	// 5-field form. Pattern validation rejects malformed expressions at admission;
	// the controller still parses with robfig/cron at runtime as defense in depth.
	// +optional
	// +kubebuilder:validation:MinLength=9
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9*/,?-]+(\s+[A-Za-z0-9*/,?-]+){4}$`
	CronSchedule *string `json:"cronSchedule,omitempty"`
}

// LifecycleSpec defines lifecycle management configuration for database migrations
// and application initialization tasks.
// +kubebuilder:validation:XValidation:rule="!has(self.init) || !has(self.init.command) || size(self.init.command) == 0 || (!has(self.init.adminUser) && !has(self.init.loadExamples))",message="init.command is mutually exclusive with init.adminUser and init.loadExamples"
// +kubebuilder:validation:XValidation:rule="!has(self.clone) || !has(self.clone.source.password) || !has(self.clone.source.passwordFrom)",message="clone.source.password and clone.source.passwordFrom are mutually exclusive"
type LifecycleSpec struct {
	// UpgradeMode controls whether upgrades require manual approval.
	// Automatic runs immediately on image change; Supervised waits for an
	// approval annotation before proceeding.
	// +optional
	// +kubebuilder:validation:Enum=Automatic;Supervised
	// +kubebuilder:default=Automatic
	UpgradeMode *string `json:"upgradeMode,omitempty"`

	// Set to true to skip all lifecycle tasks entirely.
	// +optional
	Disabled *bool `json:"disabled,omitempty"`

	// Image override for lifecycle task Jobs.
	// +optional
	Image *ImageOverrideSpec `json:"image,omitempty"`

	// Pod and container template for lifecycle task Jobs.
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`

	// Retention policy for completed lifecycle task Jobs and their Pods.
	// +optional
	PodRetention *PodRetentionSpec `json:"podRetention,omitempty"`

	// Per-lifecycle raw Python appended after top-level config.
	// +optional
	Config *string `json:"config,omitempty"`

	// Per-lifecycle SQLAlchemy engine options (overrides spec.sqlaEngineOptions entirely).
	// +optional
	SQLAlchemyEngineOptions *SQLAlchemyEngineOptionsSpec `json:"sqlaEngineOptions,omitempty"`

	// MaintenancePage configures a lightweight maintenance page served during
	// lifecycle drain and task execution. Presence enables the feature when a
	// drain will actually run and an existing web-server workload is present.
	// In managed mode (no image override), an nginx:alpine container serves
	// a default or custom HTML page. In custom mode (image set), the user's
	// image handles serving, and content fields are passed as env vars.
	// +optional
	MaintenancePage *MaintenancePageSpec `json:"maintenancePage,omitempty"`

	// Clone configures database cloning from an external source before running
	// migrations. The clone target is always spec.metastore. Only allowed in
	// Development or Staging mode.
	// +optional
	Clone *CloneTaskSpec `json:"clone,omitempty"`

	// Database migration task configuration.
	// +optional
	Migrate *MigrateTaskSpec `json:"migrate,omitempty"`

	// Secret key rotation task configuration. Runs after migrate and before init.
	// Presence enables the task; absence disables it.
	// +optional
	Rotate *RotateTaskSpec `json:"rotate,omitempty"`

	// Application initialization task configuration.
	// +optional
	Init *InitTaskSpec `json:"init,omitempty"`
}

// MigrateTaskSpec defines the database migration task.
// Triggers on image (version) changes and upstream task re-execution.
type MigrateTaskSpec struct {
	BaseTaskSpec `json:",inline"`
}

// RotateTaskSpec defines the secret key rotation task.
// Runs superset re-encrypt-secrets between migrate and init when the
// secret key is rotated. Requires previousSecretKey or previousSecretKeyFrom
// to be set on the parent spec.
type RotateTaskSpec struct {
	BaseTaskSpec `json:",inline"`
}

// InitTaskSpec defines the application initialization task.
// Triggers on config changes and upstream task re-execution.
type InitTaskSpec struct {
	BaseTaskSpec `json:",inline"`

	// Admin user to create during initialization. Only allowed in dev mode.
	// When set, the operator appends a superset fab create-admin step to the init command.
	// +optional
	AdminUser *AdminUserSpec `json:"adminUser,omitempty"`

	// Load example dashboards and data during initialization. Only allowed in dev mode.
	// When true, the operator appends a superset load-examples step to the init command.
	// +optional
	LoadExamples *bool `json:"loadExamples,omitempty"`
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

// PodRetentionSpec defines retention behavior for lifecycle task Jobs and their Pods.
type PodRetentionSpec struct {
	// Retention policy: Delete removes Jobs and Pods after completion, Retain keeps all,
	// RetainOnFailure (the default) keeps only failed Jobs and Pods for debugging and
	// deletes successful ones to reduce noise. Retained Jobs and Pods are automatically
	// deleted when the task is reset or disabled, and garbage-collected when the
	// parent Superset CR is deleted.
	// +optional
	// +kubebuilder:validation:Enum=Delete;Retain;RetainOnFailure
	// +kubebuilder:default=RetainOnFailure
	Policy *string `json:"policy,omitempty"`
}

// MaintenancePageSpec configures a lightweight maintenance page served while
// components are drained for lifecycle tasks. The page is only started when a
// drain will actually run and an existing web-server workload is present.
// Supports two modes:
//   - Managed (default): uses nginx:alpine with operator-generated HTML and nginx config.
//   - Custom (image set): user provides their own image/command; content fields
//     are passed as SUPERSET_OPERATOR__MAINTENANCE_* env vars.
type MaintenancePageSpec struct {
	// Title displayed on the maintenance page heading (managed mode).
	// In custom mode, passed as env var SUPERSET_OPERATOR__MAINTENANCE_TITLE.
	// +optional
	Title *string `json:"title,omitempty"`

	// Message displayed below the title (managed mode).
	// In custom mode, passed as env var SUPERSET_OPERATOR__MAINTENANCE_MESSAGE.
	// +optional
	Message *string `json:"message,omitempty"`

	// Full HTML page content. When set in managed mode, title and message are
	// ignored and this value is served as the complete page.
	// In custom mode, passed as env var SUPERSET_OPERATOR__MAINTENANCE_BODY.
	// +optional
	Body *string `json:"body,omitempty"`

	// Image for the maintenance page container. When set, switches to custom
	// mode: no nginx config is injected, and the user's image is responsible
	// for serving HTTP traffic on the web-server port (default 8088). The port
	// must match the web-server Service's target port since the maintenance page
	// takes over that Service during lifecycle tasks.
	// When unset, defaults to nginx:alpine (managed mode).
	// +optional
	Image *ImageSpec `json:"image,omitempty"`

	// Number of maintenance page pod replicas.
	// +optional
	// +kubebuilder:default=1
	Replicas *int32 `json:"replicas,omitempty"`

	// Deployment-level overrides for the maintenance page (strategy, revision history).
	// For pod-level settings, use PodTemplate.
	// +optional
	DeploymentTemplate *DeploymentTemplate `json:"deploymentTemplate,omitempty"`

	// Pod template for the maintenance page pod.
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`
}

// --- Clone spec ---

// CloneTaskSpec configures database cloning from an external source into
// this CR's metastore. Runs before migrate and init tasks. The clone target
// is always spec.metastore — the metastore user must have CREATEDB rights.
// Only allowed in Development or Staging mode.
// Triggers on source config changes and the trigger field (inherited from BaseTaskSpec).
type CloneTaskSpec struct {
	SchedulableBaseTaskSpec `json:",inline"`

	// Source database to clone from (typically production, read-only user).
	Source CloneSourceSpec `json:"source"`

	// Tables to exclude entirely from the dump (schema and data).
	// +optional
	ExcludeTables []string `json:"excludeTables,omitempty"`

	// Tables where schema is dumped but data is not. Useful for large tables
	// needed by migrations but not for testing (e.g., "logs", "query").
	// +optional
	ExcludeTableData []string `json:"excludeTableData,omitempty"`

	// SQL statements to execute against the target database after cloning.
	// Useful for sanitizing cloned data (e.g., disabling alerts, deleting
	// OAuth tokens, masking PII).
	// +optional
	PostCloneSQL []string `json:"postCloneSQL,omitempty"`

	// Image for the clone Job. Defaults to postgres:17-alpine (PostgreSQL)
	// or mysql:8-alpine (MySQL) based on source.type.
	// +optional
	Image *ImageSpec `json:"image,omitempty"`

	// Pod and container template for the clone task Job.
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`

	// Retention policy for completed clone Jobs and their Pods.
	// +optional
	PodRetention *PodRetentionSpec `json:"podRetention,omitempty"`
}

// CloneSourceSpec defines the source database connection for cloning.
// +kubebuilder:validation:XValidation:rule="has(self.password) || has(self.passwordFrom)",message="one of password or passwordFrom must be set"
// +kubebuilder:validation:XValidation:rule="!has(self.password) || !has(self.passwordFrom)",message="password and passwordFrom are mutually exclusive"
type CloneSourceSpec struct {
	// Database type: PostgreSQL (default) or MySQL.
	// +optional
	// +kubebuilder:validation:Enum=PostgreSQL;MySQL
	// +kubebuilder:default=PostgreSQL
	Type *string `json:"type,omitempty"`

	// Source database hostname.
	Host string `json:"host"`

	// Source database port. Defaults to 5432 (postgresql) or 3306 (mysql).
	// +optional
	Port *int32 `json:"port,omitempty"`

	// Database name on the source server.
	Database string `json:"database"`

	// Username for the source database (should have read-only access).
	Username string `json:"username"`

	// Password for the source database (Development mode only). In Staging,
	// use passwordFrom to reference a Kubernetes Secret.
	// +optional
	Password *string `json:"password,omitempty"`

	// PasswordFrom references a Secret containing the source database password.
	// +optional
	PasswordFrom *corev1.SecretKeySelector `json:"passwordFrom,omitempty"`
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
	// Ready summarizes ready component replicas across all enabled components
	// in "ready/desired" format.
	// +optional
	Ready string `json:"ready,omitempty"`
	// +optional
	Components *ComponentStatusMap `json:"components,omitempty"`
	// Lifecycle tracks the current lifecycle state.
	// +optional
	Lifecycle *LifecycleStatus `json:"lifecycle,omitempty"`
	// Last image (repository:tag) that successfully completed the lifecycle.
	// Used to detect image changes on subsequent reconciles.
	// +optional
	LastLifecycleImage string `json:"lastLifecycleImage,omitempty"`
	// +optional
	Version string `json:"version,omitempty"`
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`
	// High-level phase.
	// +optional
	// +kubebuilder:validation:Enum=Initializing;Upgrading;Running;Degraded;Suspended;Blocked;AwaitingApproval
	Phase string `json:"phase,omitempty"`
}

// LifecycleStatus tracks the current lifecycle task execution state.
type LifecycleStatus struct {
	// Phase of the lifecycle: Cloning, Draining, Migrating, Rotating, Initializing, Restoring, Complete, Blocked, AwaitingApproval.
	// +optional
	Phase string `json:"phase,omitempty"`
	// MaintenanceActive indicates the maintenance page is currently serving traffic
	// via the web-server Service.
	// +optional
	MaintenanceActive bool `json:"maintenanceActive,omitempty"`
	// LastCompletedChecksums maps task type to its task checksum at last
	// successful completion. Used to detect input drift when task status refs
	// are absent.
	// +optional
	LastCompletedChecksums map[string]string `json:"lastCompletedChecksums,omitempty"`
	// Clone task status summary.
	// +optional
	Clone *TaskRefStatus `json:"clone,omitempty"`
	// Migrate task status summary.
	// +optional
	Migrate *TaskRefStatus `json:"migrate,omitempty"`
	// Rotate task status summary.
	// +optional
	Rotate *TaskRefStatus `json:"rotate,omitempty"`
	// Init task status summary.
	// +optional
	Init *TaskRefStatus `json:"init,omitempty"`
	// Upgrade context (populated during active upgrade).
	// +optional
	Upgrade *UpgradeContext `json:"upgrade,omitempty"`
}

// TaskRefStatus holds the projected status summary of a lifecycle task.
type TaskRefStatus struct {
	// +optional
	// +kubebuilder:validation:Enum=Pending;Running;Complete;Failed
	State string `json:"state,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	Attempts int32 `json:"attempts,omitempty"`
	// Maximum number of attempts before the task is considered permanently failed.
	// +optional
	MaxRetries int32 `json:"maxRetries,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// NextAttemptAt is the earliest time the operator may retry this task after
	// a failure or timeout.
	// +optional
	NextAttemptAt *metav1.Time `json:"nextAttemptAt,omitempty"`
	// DesiredChecksum is the checksum for the task inputs the operator is
	// currently trying to execute.
	// +optional
	DesiredChecksum string `json:"desiredChecksum,omitempty"`
	// CompletedChecksum is the task input checksum that last reached a terminal
	// Complete or Failed state.
	// +optional
	CompletedChecksum string `json:"completedChecksum,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// LastScheduledAt is the cron tick that triggered the most recent scheduled run.
	// +optional
	LastScheduledAt *metav1.Time `json:"lastScheduledAt,omitempty"`
	// NextScheduleAt is the next future cron tick when the schedule will fire.
	// +optional
	NextScheduleAt *metav1.Time `json:"nextScheduleAt,omitempty"`
}

// UpgradeContext tracks the current upgrade operation.
type UpgradeContext struct {
	// +optional
	FromVersion string `json:"fromVersion,omitempty"`
	// +optional
	ToVersion string `json:"toVersion,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=Upgrade;Downgrade;Unknown
	Direction string `json:"direction,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
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

// ComponentRefStatus holds the status summary of a managed component.
type ComponentRefStatus struct {
	// Phase summarizes the component workload state.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Progressing;Ready;Unavailable;Drained
	Phase string `json:"phase,omitempty"`
	// Resources lists the Kubernetes resources currently expected for this
	// component and whether the operator can observe them.
	// +optional
	// +listType=map
	// +listMapKey=kind
	// +listMapKey=name
	Resources []ComponentResourceStatus `json:"resources,omitempty"`
	// Image currently configured on the component's main container.
	// +optional
	Image string `json:"image,omitempty"`
	// Desired replica count used for status reporting.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// ReadyReplicas is the number of ready component pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// UpdatedReplicas is the number of pods updated to the current template.
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`
	// AvailableReplicas is the number of available component pods.
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`
	// Checksum stamped on the component pod template by the parent. Drives
	// rolling restarts; surfaced here so users can see which revision each
	// component is reconciling against.
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`
	// Message gives a short human-oriented reason when the component is not ready.
	// +optional
	Message string `json:"message,omitempty"`
}

// ComponentResourceStatus describes one Kubernetes resource managed for a
// component.
type ComponentResourceStatus struct {
	// Resource kind, for example Deployment, Service, ConfigMap, HorizontalPodAutoscaler.
	Kind string `json:"kind"`
	// Resource name.
	Name string `json:"name"`
	// Observed status: Present or Missing.
	// +kubebuilder:validation:Enum=Present;Missing
	Status string `json:"status"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.status.version`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Lifecycle",type=string,JSONPath=`.status.lifecycle.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.ready`
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
// +kubebuilder:validation:XValidation:rule="!has(self.spec.lifecycle) || !has(self.spec.lifecycle.maintenancePage) || size(self.metadata.name) <= 46",message="metadata.name must be at most 46 characters when lifecycle.maintenancePage is enabled (sub-resource suffix '-maintenance-page' is 17 chars)"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.lifecycle) || !has(self.spec.lifecycle.rotate) || size(self.metadata.name) <= 49",message="metadata.name must be at most 49 characters when lifecycle.rotate is enabled (task name '{parent}-rotate' + ConfigMap suffix '-config' must fit within 63 chars)"
// +kubebuilder:validation:XValidation:rule="(has(self.spec.lifecycle) && has(self.spec.lifecycle.disabled) && self.spec.lifecycle.disabled == true) || size(self.metadata.name) <= 48",message="metadata.name must be at most 48 characters when lifecycle is enabled (task name '{parent}-migrate' + ConfigMap suffix '-config' must fit within 63 chars)"

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
