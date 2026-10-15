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

package controller

import (
	"fmt"
	"maps"
	"strings"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

func convertTopLevelSpec(spec *supersetv1alpha1.SupersetSpec) *resolution.SharedInput {
	return &resolution.SharedInput{
		Replicas:            spec.Replicas,
		DeploymentTemplate:  spec.DeploymentTemplate,
		PodTemplate:         spec.PodTemplate,
		Autoscaling:         spec.Autoscaling,
		PodDisruptionBudget: spec.PodDisruptionBudget,
	}
}

// buildInitCommand constructs the init shell command from InitTaskSpec fields.
func buildInitCommand(init *supersetv1alpha1.InitTaskSpec) []string {
	script := "superset init"

	if init != nil && init.AdminUser != nil {
		script += ` && (superset fab create-admin` +
			` --username "$SUPERSET_OPERATOR__ADMIN_USERNAME"` +
			` --password "$SUPERSET_OPERATOR__ADMIN_PASSWORD"` +
			` --firstname "$SUPERSET_OPERATOR__ADMIN_FIRST_NAME"` +
			` --lastname "$SUPERSET_OPERATOR__ADMIN_LAST_NAME"` +
			` --email "$SUPERSET_OPERATOR__ADMIN_EMAIL"` +
			` || true)`
	}

	if init != nil && init.LoadExamples != nil && *init.LoadExamples {
		script += " && superset load-examples"
	}

	return []string{bootstrapShell, "-c", script}
}

func convertTaskComponent(lifecycle *supersetv1alpha1.LifecycleSpec, command []string) *resolution.ComponentInput {
	var pt *supersetv1alpha1.PodTemplate
	if lifecycle != nil {
		pt = lifecycle.PodTemplate
	}

	var ct *supersetv1alpha1.ContainerTemplate
	if pt != nil && pt.Container != nil {
		copied := *pt.Container
		ct = &copied
	} else {
		ct = &supersetv1alpha1.ContainerTemplate{}
	}
	ct.Command = command

	if pt != nil {
		copied := *pt
		copied.Container = ct
		pt = &copied
	} else {
		pt = &supersetv1alpha1.PodTemplate{Container: ct}
	}

	return &resolution.ComponentInput{
		PodTemplate: pt,
	}
}

// collectLifecycleInitEnvVars returns env vars for the init task (admin user credentials).
func collectLifecycleInitEnvVars(lifecycle *supersetv1alpha1.LifecycleSpec) []corev1.EnvVar {
	if lifecycle == nil || lifecycle.Init == nil || lifecycle.Init.AdminUser == nil {
		return nil
	}
	admin := lifecycle.Init.AdminUser
	return []corev1.EnvVar{
		{Name: naming.EnvAdminUsername, Value: derefOrDefault(admin.Username, "admin")},
		{Name: naming.EnvAdminPassword, Value: derefOrDefault(admin.Password, "admin")},
		{Name: naming.EnvAdminFirstName, Value: derefOrDefault(admin.FirstName, "Superset")},
		{Name: naming.EnvAdminLastName, Value: derefOrDefault(admin.LastName, "Admin")},
		{Name: naming.EnvAdminEmail, Value: derefOrDefault(admin.Email, "admin@example.com")},
	}
}

// --- Config input building ---

func buildConfigInput(spec *supersetv1alpha1.SupersetSpec) *supersetconfig.ConfigInput {
	input := &supersetconfig.ConfigInput{}

	if spec.Metastore != nil {
		if spec.Metastore.URI != nil || spec.Metastore.URIFrom != nil {
			input.MetastoreMode = supersetconfig.MetastorePassthrough
		} else if isStructuredMetastore(spec.Metastore) {
			input.MetastoreMode = supersetconfig.MetastoreStructured
			dbType := dbTypePostgresql
			if spec.Metastore.Type != nil {
				dbType = *spec.Metastore.Type
			}
			input.DBType = dbType
			if spec.Metastore.Driver != nil {
				input.DBDriver = *spec.Metastore.Driver
			}
		}
	}

	if spec.Valkey != nil {
		input.Valkey = buildValkeyInput(spec.Valkey)
	}

	input.FeatureFlags = spec.FeatureFlags

	if spec.Realtime != nil {
		if spec.Realtime.AsyncQueries != nil {
			input.AsyncQueriesEnabled = true
			input.FeatureFlags = withFeatureFlag(spec.FeatureFlags, featureFlagGlobalAsyncQueries, true)
		}
		if ws := spec.Realtime.WebSocket; ws != nil {
			input.WebSocketEnabled = true
			input.WebSocketURL = deriveWebSocketURL(spec)
			input.WebSocketCookieName = websocketCookieName(ws)
			if ws.JWTExpirationSeconds != nil {
				input.WebSocketJWTExpirationSeconds = *ws.JWTExpirationSeconds
			}
		}
	}

	if spec.Config != nil {
		input.Config = *spec.Config
	}

	if spec.BaseURL != nil {
		input.BaseURL = *spec.BaseURL
	}

	input.HasPreviousSecretKey = spec.PreviousSecretKey != nil || spec.PreviousSecretKeyFrom != nil

	return input
}

// withFeatureFlag returns a copy of flags with key set to val, leaving the
// caller's map untouched (spec.FeatureFlags is shared across components).
func withFeatureFlag(flags map[string]bool, key string, val bool) map[string]bool {
	merged := make(map[string]bool, len(flags)+1)
	maps.Copy(merged, flags)
	merged[key] = val
	return merged
}

// buildValkeyInput converts the CRD ValkeySpec into a resolved ValkeyInput with defaults applied.
func buildValkeyInput(v *supersetv1alpha1.ValkeySpec) *supersetconfig.ValkeyInput {
	vi := &supersetconfig.ValkeyInput{
		Cache:                   resolveValkeyCache(v.Cache, 1, "superset_", 300),
		DataCache:               resolveValkeyCache(v.DataCache, 2, "superset_data_", 86400),
		FilterStateCache:        resolveValkeyCache(v.FilterStateCache, 3, "superset_filter_", 3600),
		ExploreFormDataCache:    resolveValkeyCache(v.ExploreFormDataCache, 4, "superset_explore_", 3600),
		ThumbnailCache:          resolveValkeyCache(v.ThumbnailCache, 5, "superset_thumbnail_", 3600),
		DistributedCoordination: resolveValkeyCoordination(v),
		CeleryBroker:            resolveValkeyCelery(v.CeleryBroker, 0),
		CeleryResultBackend:     resolveValkeyCelery(v.CeleryResultBackend, 0),
		ResultsBackend:          resolveValkeyResults(v.ResultsBackend, 6, "superset_results_"),
	}

	if v.SSL != nil {
		vi.SSL = true
		// Resolved here, not left to each consumer, so the Python config and the
		// websocket server always verify the server the same way.
		vi.SSLCertRequired = derefOrDefault(v.SSL.CertRequired, "required")
		if v.SSL.KeyFile != nil {
			vi.SSLKeyFile = *v.SSL.KeyFile
		}
		if v.SSL.CertFile != nil {
			vi.SSLCertFile = *v.SSL.CertFile
		}
		if v.SSL.CACertFile != nil {
			vi.SSLCACertFile = *v.SSL.CACertFile
		}
	}

	return vi
}

// resolveValkeyCoordination resolves the distributed coordination backend
// settings. The app (DISTRIBUTED_COORDINATION_CONFIG) and the websocket server
// (REDIS_DB) must agree on them, so both read them from here.
func resolveValkeyCoordination(v *supersetv1alpha1.ValkeySpec) supersetconfig.ValkeyCacheInput {
	return resolveValkeyCache(v.DistributedCoordination, 7, "coordination_", 300)
}

func resolveValkeyCache(spec *supersetv1alpha1.ValkeyCacheSpec, defaultDB int32, defaultPrefix string, defaultTimeout int32) supersetconfig.ValkeyCacheInput {
	input := supersetconfig.ValkeyCacheInput{
		Database:       defaultDB,
		KeyPrefix:      defaultPrefix,
		DefaultTimeout: defaultTimeout,
	}
	if spec == nil {
		return input
	}
	if spec.Disabled != nil && *spec.Disabled {
		input.Disabled = true
	}
	if spec.Database != nil {
		input.Database = *spec.Database
	}
	if spec.KeyPrefix != nil {
		input.KeyPrefix = *spec.KeyPrefix
	}
	if spec.DefaultTimeout != nil {
		input.DefaultTimeout = *spec.DefaultTimeout
	}
	return input
}

func resolveValkeyCelery(spec *supersetv1alpha1.ValkeyCelerySpec, defaultDB int32) supersetconfig.ValkeyCeleryInput {
	input := supersetconfig.ValkeyCeleryInput{Database: defaultDB}
	if spec == nil {
		return input
	}
	if spec.Disabled != nil && *spec.Disabled {
		input.Disabled = true
	}
	if spec.Database != nil {
		input.Database = *spec.Database
	}
	return input
}

func resolveValkeyResults(spec *supersetv1alpha1.ValkeyResultsBackendSpec, defaultDB int32, defaultPrefix string) supersetconfig.ValkeyResultsInput {
	input := supersetconfig.ValkeyResultsInput{
		Database:  defaultDB,
		KeyPrefix: defaultPrefix,
	}
	if spec == nil {
		return input
	}
	if spec.Disabled != nil && *spec.Disabled {
		input.Disabled = true
	}
	if spec.Database != nil {
		input.Database = *spec.Database
	}
	if spec.KeyPrefix != nil {
		input.KeyPrefix = *spec.KeyPrefix
	}
	return input
}

// --- Secret env var collection ---

// collectSecretEnvVars gathers env vars for SECRET_KEY, metastore fields, valkey, and the
// instance name. The instance name (spec.valkey.keyPrefix, default instanceDefault, which is
// <namespace>:<name>) is the Valkey namespace: it prefixes cache/results keys and coordination/realtime channels, and is exposed
// so admins can compute instance-scoped values (e.g. Celery queue names) from raw Python in spec.config.

// isDevEnvironment reports whether the CR is in Development mode. Inline secret
// literals are only honored in Development. Re-checking here (not only via CEL)
// keeps the controller defensive against a CR that slips past admission —
// upgrades from older CRD versions, direct etcd writes, or an apiserver without
// CEL — so such a CR can never materialize an inline secret into a pod spec.
// Mirrors the defensive createDatabaseEnabled precedent.
func isDevEnvironment(spec *supersetv1alpha1.SupersetSpec) bool {
	return spec.Environment != nil && *spec.Environment == naming.EnvironmentDev
}
func collectSecretEnvVars(spec *supersetv1alpha1.SupersetSpec, instanceDefault string) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{Name: naming.EnvInstanceName, Value: valkeyKeyPrefix(spec, instanceDefault)},
	}
	isDev := isDevEnvironment(spec)

	// SUPERSET_OPERATOR__SECRET_KEY — rendered into superset_config.py as SECRET_KEY.
	if isDev && spec.SecretKey != nil {
		envs = append(envs, corev1.EnvVar{
			Name:  naming.EnvSecretKey,
			Value: *spec.SecretKey,
		})
	} else if spec.SecretKeyFrom != nil {
		envs = append(envs, corev1.EnvVar{
			Name:      naming.EnvSecretKey,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.SecretKeyFrom},
		})
	}

	// SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY — for key rotation.
	if isDev && spec.PreviousSecretKey != nil {
		envs = append(envs, corev1.EnvVar{
			Name:  naming.EnvPreviousSecretKey,
			Value: *spec.PreviousSecretKey,
		})
	} else if spec.PreviousSecretKeyFrom != nil {
		envs = append(envs, corev1.EnvVar{
			Name:      naming.EnvPreviousSecretKey,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.PreviousSecretKeyFrom},
		})
	}

	// Metastore env vars.
	if spec.Metastore != nil {
		if isDev && spec.Metastore.URI != nil {
			envs = append(envs, corev1.EnvVar{
				Name:  naming.EnvDatabaseURI,
				Value: *spec.Metastore.URI,
			})
		} else if spec.Metastore.URIFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvDatabaseURI,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Metastore.URIFrom},
			})
		} else if isStructuredMetastore(spec.Metastore) {
			envs = append(envs, structuredMetastoreEnvVars(spec.Metastore, isDev)...)
		}
	}

	// Valkey env vars.
	if spec.Valkey != nil {
		envs = append(envs, valkeyConnectionEnvVars(spec.Valkey, isDev, valkeyEnvNames{
			host: naming.EnvValkeyHost, port: naming.EnvValkeyPort, user: naming.EnvValkeyUser, pass: naming.EnvValkeyPass,
		})...)
	}

	// SUPERSET_OPERATOR__WS_JWT_SECRET — the app mints the cookie the websocket
	// server validates, so it needs the same secret.
	if ws := realtimeWebSocket(spec); ws != nil {
		if env, ok := devLiteralOrSecretEnv(naming.EnvWSJWTSecret, isDev, ws.JWTSecret, ws.JWTSecretFrom); ok {
			envs = append(envs, env)
		}
	}

	return envs
}

// valkeyEnvNames names the env vars a Valkey connection is projected into.
type valkeyEnvNames struct{ host, port, user, pass string }

// valkeyConnectionEnvVars projects spec.valkey's connection into env vars, each
// field from its literal or its Secret selector. The Python components and the
// websocket server both use it, so they always honor the same fields.
func valkeyConnectionEnvVars(v *supersetv1alpha1.ValkeySpec, isDev bool, names valkeyEnvNames) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		literalOrSecretEnv(names.host, &v.Host, v.HostFrom),
		literalInt32OrSecretEnv(names.port, v.Port, v.PortFrom, 6379),
	}
	if v.Username != nil || v.UsernameFrom != nil {
		envs = append(envs, literalOrSecretEnv(names.user, v.Username, v.UsernameFrom))
	}
	if env, ok := devLiteralOrSecretEnv(names.pass, isDev, v.Password, v.PasswordFrom); ok {
		envs = append(envs, env)
	}
	return envs
}

// devLiteralOrSecretEnv returns the env var for a secret that is either an
// inline literal (honored only in Development) or a Secret selector passed
// through as a secretKeyRef. ok is false when neither applies.
func devLiteralOrSecretEnv(name string, isDev bool, literal *string, selector *corev1.SecretKeySelector) (corev1.EnvVar, bool) {
	if isDev && literal != nil {
		return corev1.EnvVar{Name: name, Value: *literal}, true
	}
	if selector != nil {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: selector}}, true
	}
	return corev1.EnvVar{}, false
}

// valkeyKeyPrefix returns the Valkey namespace for this deployment: the explicit
// spec.valkey.keyPrefix, or instanceDefault (typically <namespace>:<name>) otherwise.
// It prefixes cache/results keys and the coordination/realtime channels so instances
// can share one Valkey/Redis.
func valkeyKeyPrefix(spec *supersetv1alpha1.SupersetSpec, instanceDefault string) string {
	if spec.Valkey != nil && spec.Valkey.KeyPrefix != nil && *spec.Valkey.KeyPrefix != "" {
		return *spec.Valkey.KeyPrefix
	}
	return instanceDefault
}

// defaultInstanceName is the default Valkey namespace for a CR: <namespace>:<name>.
// (namespace, name) is Kubernetes' uniqueness key, and the ":" separator is injective
// since neither a namespace nor a name may contain it — so two CRs can never collide
// on a shared Valkey/Redis by default.
func defaultInstanceName(superset *supersetv1alpha1.Superset) string {
	return superset.Namespace + ":" + superset.Name
}

// Environment variables read by the bundled websocket server (superset-websocket).
// These are the server's own contract, distinct from the SUPERSET_OPERATOR__* vars.
const (
	envWSPort                     = "PORT"
	envWSJWTCookie                = "JWT_COOKIE_NAME"
	envWSJWTSecret                = "JWT_SECRET"
	envWSAllowedOrigins           = "ALLOWED_ORIGINS"
	envWSRedisHost                = "REDIS_HOST"
	envWSRedisPort                = "REDIS_PORT"
	envWSRedisDB                  = "REDIS_DB"
	envWSRedisUser                = "REDIS_USERNAME"
	envWSRedisPass                = "REDIS_PASSWORD"
	envWSRedisSSL                 = "REDIS_SSL"
	envWSRedisSSLCertReqs         = "REDIS_SSL_CERT_REQS"
	envWSRedisSSLCACerts          = "REDIS_SSL_CA_CERTS"
	envWSRedisSSLCertfile         = "REDIS_SSL_CERTFILE"
	envWSRedisSSLKeyfile          = "REDIS_SSL_KEYFILE"
	envWSRealtimeChannelPrefix    = "REALTIME_CHANNEL_PREFIX"
	defaultWSCookie               = "superset-ws-token"
	featureFlagGlobalAsyncQueries = "GLOBAL_ASYNC_QUERIES"
)

// collectWebsocketEnvVars builds the env for the websocket server: the shared
// JWT secret plus a Redis connection pointing at the same coordination backend
// (DISTRIBUTED_COORDINATION_CONFIG) the Python app uses.
func collectWebsocketEnvVars(superset *supersetv1alpha1.Superset) []corev1.EnvVar {
	spec := &superset.Spec
	isDev := isDevEnvironment(spec)

	envs := []corev1.EnvVar{
		{Name: envWSPort, Value: fmt.Sprintf("%d", resolveWebsocketPort(superset))},
		// Namespace the realtime Pub/Sub channel to match the Python side
		// (REALTIME_CHANNEL_PREFIX) so instances sharing one Valkey/Redis don't
		// cross-deliver.
		{Name: envWSRealtimeChannelPrefix, Value: valkeyKeyPrefix(spec, defaultInstanceName(superset)) + ":"},
	}

	if origins := deriveWebSocketAllowedOrigins(spec); len(origins) > 0 {
		envs = append(envs, corev1.EnvVar{Name: envWSAllowedOrigins, Value: strings.Join(origins, ",")})
	}

	if ws := realtimeWebSocket(spec); ws != nil {
		envs = append(envs, corev1.EnvVar{Name: envWSJWTCookie, Value: websocketCookieName(ws)})
		if env, ok := devLiteralOrSecretEnv(envWSJWTSecret, isDev, ws.JWTSecret, ws.JWTSecretFrom); ok {
			envs = append(envs, env)
		}
	}

	if v := spec.Valkey; v != nil {
		envs = append(envs, valkeyConnectionEnvVars(v, isDev, valkeyEnvNames{
			host: envWSRedisHost, port: envWSRedisPort, user: envWSRedisUser, pass: envWSRedisPass,
		})...)
		// The same resolved input the Python components render
		// DISTRIBUTED_COORDINATION_CONFIG from, so the DB and TLS settings
		// (CACHE_REDIS_DB, CACHE_REDIS_SSL_*) match on both sides.
		vi := buildValkeyInput(v)
		envs = append(envs, corev1.EnvVar{Name: envWSRedisDB, Value: fmt.Sprintf("%d", vi.DistributedCoordination.Database)})
		if vi.SSL {
			envs = append(envs,
				corev1.EnvVar{Name: envWSRedisSSL, Value: "true"},
				corev1.EnvVar{Name: envWSRedisSSLCertReqs, Value: vi.SSLCertRequired},
			)
			for _, f := range []struct{ name, path string }{
				{envWSRedisSSLCACerts, vi.SSLCACertFile},
				{envWSRedisSSLCertfile, vi.SSLCertFile},
				{envWSRedisSSLKeyfile, vi.SSLKeyFile},
			} {
				if f.path != "" {
					envs = append(envs, corev1.EnvVar{Name: f.name, Value: f.path})
				}
			}
		}
	}

	return envs
}

// websocketCookieName returns the JWT cookie name shared by the app and the
// websocket server. Both sides are given this value explicitly so they cannot
// drift apart.
func websocketCookieName(ws *supersetv1alpha1.WebSocketTransportSpec) string {
	if ws.CookieName != nil && *ws.CookieName != "" {
		return *ws.CookieName
	}
	return defaultWSCookie
}

// realtimeWebSocket returns spec.realtime.webSocket, or nil when unset.
func realtimeWebSocket(spec *supersetv1alpha1.SupersetSpec) *supersetv1alpha1.WebSocketTransportSpec {
	if spec.Realtime == nil {
		return nil
	}
	return spec.Realtime.WebSocket
}

func isStructuredMetastore(metastore *supersetv1alpha1.MetastoreSpec) bool {
	return metastore != nil && (metastore.Host != nil || metastore.HostFrom != nil)
}

func literalOrSecretEnv(name string, literal *string, selector *corev1.SecretKeySelector) corev1.EnvVar {
	if selector != nil {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: selector}}
	}
	return corev1.EnvVar{Name: name, Value: derefOrDefault(literal, "")}
}

func literalInt32OrSecretEnv(name string, literal *int32, selector *corev1.SecretKeySelector, defaultValue int32) corev1.EnvVar {
	if selector != nil {
		return literalOrSecretEnv(name, nil, selector)
	}
	if literal != nil {
		defaultValue = *literal
	}
	return corev1.EnvVar{Name: name, Value: fmt.Sprintf("%d", defaultValue)}
}

// structuredMetastoreEnvVars builds the connection env vars shared by
// Superset components, lifecycle Jobs, and the create-database init container.
func structuredMetastoreEnvVars(metastore *supersetv1alpha1.MetastoreSpec, isDev bool) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		literalOrSecretEnv(naming.EnvDBHost, metastore.Host, metastore.HostFrom),
	}
	envs = append(envs, literalInt32OrSecretEnv(naming.EnvDBPort, metastore.Port, metastore.PortFrom, defaultDBPort(metastore.Type)))
	envs = append(envs,
		literalOrSecretEnv(naming.EnvDBName, metastore.Database, metastore.DatabaseFrom),
		literalOrSecretEnv(naming.EnvDBUser, metastore.Username, metastore.UsernameFrom),
	)
	if isDev && metastore.Password != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPass, Value: *metastore.Password})
	} else if metastore.PasswordFrom != nil {
		envs = append(envs, literalOrSecretEnv(naming.EnvDBPass, nil, metastore.PasswordFrom))
	}
	return envs
}

func derefOrDefault(ptr *string, def string) string {
	if ptr != nil {
		return *ptr
	}
	return def
}

func defaultDBPort(driver *string) int32 {
	if driver != nil && *driver == dbTypeMySQL {
		return 3306
	}
	return 5432
}

// --- Operator-injected volumes/env/mounts ---

func buildOperatorInjected(renderedConfig, bootstrapScript, resourceBaseName, forceReload string, configEnvVars []corev1.EnvVar) *resolution.OperatorInjected {
	injected := &resolution.OperatorInjected{}

	if renderedConfig != "" || bootstrapScript != "" {
		// Config volume + mount.
		injected.Volumes = append(injected.Volumes, corev1.Volume{
			Name: configVolumeName,
			ConfigMap: &corev1.ConfigMapVolumeSource{
				Name: naming.ConfigMapName(resourceBaseName),
			},
		})
		injected.VolumeMounts = append(injected.VolumeMounts, corev1.VolumeMount{
			Name:      configVolumeName,
			MountPath: configMountPath,
			ReadOnly:  true,
		})
	}

	// Add config-derived env vars (secret key, metastore fields, etc.).
	injected.Env = append(injected.Env, configEnvVars...)

	// ForceReload propagated via env var (triggers pod restart on change).
	if forceReload != "" {
		injected.Env = append(injected.Env, corev1.EnvVar{
			Name:  naming.EnvForceReload,
			Value: forceReload,
		})
	}

	return injected
}

// --- Resolution output -> CRD type mapping ---

// flatSpecFromResolution converts a FlatSpec into a FlatComponentSpec.
// When imageOverride is non-nil, its Tag and/or Repository override the parent image.
// saName is set on the FlatComponentSpec so it propagates to Deployment pods.
func flatSpecFromResolution(flat *resolution.FlatSpec, parentImage *supersetv1alpha1.ImageSpec, imageOverride *supersetv1alpha1.ImageOverrideSpec, saName string) supersetv1alpha1.FlatComponentSpec {
	replicas := flat.Replicas
	image := *parentImage
	if imageOverride != nil {
		if imageOverride.Tag != nil {
			image.Tag = *imageOverride.Tag
		}
		if imageOverride.Repository != nil {
			image.Repository = *imageOverride.Repository
		}
		if imageOverride.PullPolicy != nil {
			image.PullPolicy = *imageOverride.PullPolicy
		}
	}
	return supersetv1alpha1.FlatComponentSpec{
		Image:               image,
		Replicas:            &replicas,
		DeploymentTemplate:  flat.DeploymentTemplate,
		PodTemplate:         flat.PodTemplate,
		ServiceAccountName:  saName,
		Autoscaling:         flat.Autoscaling,
		PodDisruptionBudget: flat.PodDisruptionBudget,
	}
}
