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

package common

// Label keys.
const (
	LabelKeyName      = "app.kubernetes.io/name"
	LabelKeyComponent = "app.kubernetes.io/component"
	LabelKeyInstance  = "app.kubernetes.io/instance"
	LabelKeyParent    = "superset.apache.org/parent"
)

// Label values.
const (
	LabelValueApp = "superset"
)

// Resource name suffixes. These map 1:1 to ComponentType values.
const (
	SuffixWebServer       = "-web-server"       // matches ComponentWebServer
	SuffixCeleryWorker    = "-celery-worker"    // matches ComponentCeleryWorker
	SuffixCeleryBeat      = "-celery-beat"      // matches ComponentCeleryBeat
	SuffixCeleryFlower    = "-celery-flower"    // matches ComponentCeleryFlower
	SuffixWebsocketServer = "-websocket-server" // matches ComponentWebsocketServer
	SuffixMcpServer       = "-mcp-server"       // matches ComponentMcpServer
	SuffixMaintenancePage = "-maintenance"      // matches ComponentMaintenancePage
	SuffixInit            = "-init"             // matches ComponentInit
	SuffixClone           = "-clone"
	SuffixRotate          = "-rotate"
	SuffixConfig          = "-config"
	SuffixNetworkPolicy   = "-netpol"
)

// Container name (uniform across all components — pods never collide).
const Container = "superset"

// Label keys for lifecycle task Jobs and their Pods.
const (
	LabelKeyInitTask     = "superset.apache.org/init-task"
	LabelKeyInitInstance = "superset.apache.org/instance"
)

// Annotation keys.
const (
	AnnotationConfigChecksum = "superset.apache.org/config-checksum"
)

// Default ports.
const (
	PortWebServer    int32 = 8088
	PortCeleryFlower int32 = 5555
	PortWebsocket    int32 = 8080
	PortMcpServer    int32 = 8088
)

// Port names.
const (
	PortNameHTTP      = "http"
	PortNameWebsocket = "ws"
	PortNameMcp       = "mcp"
)

// Config paths.
const (
	ConfigVolumeName = "superset-config"
	ConfigMountPath  = "/app/pythonpath"
)

// Init task names.
const (
	InitTaskInit = "init"
)

// Clone default images.
const (
	CloneImagePostgres = "postgres:17-alpine"
	CloneImageMySQL    = "mysql:8-alpine"
)

// Env var names for operator-managed environment variables.
const (
	// Operator-internal transport vars (used by rendered superset_config.py).
	EnvInstanceName      = "SUPERSET_OPERATOR__INSTANCE_NAME"
	EnvSecretKey         = "SUPERSET_OPERATOR__SECRET_KEY"
	EnvPreviousSecretKey = "SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY"
	EnvDatabaseURI       = "SUPERSET_OPERATOR__DB_URI"
	EnvDBHost            = "SUPERSET_OPERATOR__DB_HOST"
	EnvDBPort            = "SUPERSET_OPERATOR__DB_PORT"
	EnvDBName            = "SUPERSET_OPERATOR__DB_NAME"
	EnvDBUser            = "SUPERSET_OPERATOR__DB_USER"
	EnvDBPass            = "SUPERSET_OPERATOR__DB_PASS"
	EnvForceReload       = "SUPERSET_OPERATOR__FORCE_RELOAD"

	// Valkey operator-internal transport vars.
	EnvValkeyHost = "SUPERSET_OPERATOR__VALKEY_HOST"
	EnvValkeyPort = "SUPERSET_OPERATOR__VALKEY_PORT"
	EnvValkeyUser = "SUPERSET_OPERATOR__VALKEY_USER"
	EnvValkeyPass = "SUPERSET_OPERATOR__VALKEY_PASS"

	// Celery Flower env vars.
	EnvFlowerURLPrefix = "SUPERSET_OPERATOR__FLOWER_URL_PREFIX"

	// Admin user operator-internal transport vars (used by init command construction).
	EnvAdminUsername  = "SUPERSET_OPERATOR__ADMIN_USERNAME"
	EnvAdminPassword  = "SUPERSET_OPERATOR__ADMIN_PASSWORD"
	EnvAdminFirstName = "SUPERSET_OPERATOR__ADMIN_FIRST_NAME"
	EnvAdminLastName  = "SUPERSET_OPERATOR__ADMIN_LAST_NAME"
	EnvAdminEmail     = "SUPERSET_OPERATOR__ADMIN_EMAIL"

	// Clone source operator-internal transport vars.
	EnvCloneSrcHost = "SUPERSET_OPERATOR__CLONE_SRC_HOST"
	EnvCloneSrcPort = "SUPERSET_OPERATOR__CLONE_SRC_PORT"
	EnvCloneSrcDB   = "SUPERSET_OPERATOR__CLONE_SRC_DB"
	EnvCloneSrcUser = "SUPERSET_OPERATOR__CLONE_SRC_USER"
	EnvCloneSrcPass = "SUPERSET_OPERATOR__CLONE_SRC_PASS"

	// Maintenance page content transport vars.
	EnvMaintenanceTitle   = "SUPERSET_OPERATOR__MAINTENANCE_TITLE"
	EnvMaintenanceMessage = "SUPERSET_OPERATOR__MAINTENANCE_MESSAGE"
	EnvMaintenanceBody    = "SUPERSET_OPERATOR__MAINTENANCE_BODY"
)

// DerivedName constructs a derived resource name from parent name and suffix.
func DerivedName(parentName string, suffix string) string {
	return parentName + suffix
}

// SubResourceName constructs a resource name from a base name and suffix.
func SubResourceName(baseName string, suffix string) string {
	return baseName + "-" + suffix
}

// ResourceBaseName returns the component resource base name: {parentName}-{componentType}.
func ResourceBaseName(parentName string, componentType ComponentType) string {
	return SubResourceName(parentName, string(componentType))
}

// ConfigMapName constructs a ConfigMap name for a component or task resource.
func ConfigMapName(baseName string) string {
	return baseName + SuffixConfig
}

// ComponentLabels returns the standard Kubernetes labels for a Superset component.
func ComponentLabels(component ComponentType, instance string) map[string]string {
	return map[string]string{
		LabelKeyName:      LabelValueApp,
		LabelKeyComponent: string(component),
		LabelKeyInstance:  instance,
	}
}
