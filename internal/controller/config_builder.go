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
		SharedInput: resolution.SharedInput{
			PodTemplate: pt,
		},
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
		} else if spec.Metastore.Host != nil {
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
			if ws.CookieName != nil {
				input.WebSocketCookieName = *ws.CookieName
			}
			if ws.JwtExpirationSeconds != nil {
				input.WebSocketJwtExpirationSeconds = *ws.JwtExpirationSeconds
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
	for k, v := range flags {
		merged[k] = v
	}
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
		DistributedCoordination: resolveValkeyCache(v.DistributedCoordination, 7, "coordination_", 300),
		CeleryBroker:            resolveValkeyCelery(v.CeleryBroker, 0),
		CeleryResultBackend:     resolveValkeyCelery(v.CeleryResultBackend, 0),
		ResultsBackend:          resolveValkeyResults(v.ResultsBackend, 6, "superset_results_"),
	}

	if v.SSL != nil {
		vi.SSL = true
		if v.SSL.CertRequired != nil {
			vi.SSLCertRequired = *v.SSL.CertRequired
		}
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
// instance name. The instance name (spec.valkey.keyPrefix, default the CR name) is the Valkey
// namespace: it prefixes cache/results keys and coordination/realtime channels, and is exposed
// so admins can compute instance-scoped values (e.g. Celery queue names) from raw Python in spec.config.
func collectSecretEnvVars(spec *supersetv1alpha1.SupersetSpec, parentName string) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{Name: naming.EnvInstanceName, Value: valkeyKeyPrefix(spec, parentName)},
	}
	isDev := spec.Environment != nil && *spec.Environment == naming.EnvironmentDev

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
		if spec.Metastore.URI != nil {
			envs = append(envs, corev1.EnvVar{
				Name:  naming.EnvDatabaseURI,
				Value: *spec.Metastore.URI,
			})
		} else if spec.Metastore.URIFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvDatabaseURI,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Metastore.URIFrom},
			})
		} else if spec.Metastore.Host != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBHost, Value: *spec.Metastore.Host})
			port := defaultDBPort(spec.Metastore.Type)
			if spec.Metastore.Port != nil {
				port = *spec.Metastore.Port
			}
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPort, Value: fmt.Sprintf("%d", port)})
			if spec.Metastore.Database != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvDBName, Value: *spec.Metastore.Database})
			}
			if spec.Metastore.Username != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvDBUser, Value: *spec.Metastore.Username})
			}
			if spec.Metastore.Password != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPass, Value: *spec.Metastore.Password})
			} else if spec.Metastore.PasswordFrom != nil {
				envs = append(envs, corev1.EnvVar{
					Name:      naming.EnvDBPass,
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Metastore.PasswordFrom},
				})
			}
		}
	}

	// Valkey env vars.
	if spec.Valkey != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyHost, Value: spec.Valkey.Host})
		port := int32(6379)
		if spec.Valkey.Port != nil {
			port = *spec.Valkey.Port
		}
		envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyPort, Value: fmt.Sprintf("%d", port)})
		if spec.Valkey.Username != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyUser, Value: *spec.Valkey.Username})
		}
		if isDev && spec.Valkey.Password != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyPass, Value: *spec.Valkey.Password})
		} else if spec.Valkey.PasswordFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvValkeyPass,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Valkey.PasswordFrom},
			})
		}
	}

	// SUPERSET_OPERATOR__WS_JWT_SECRET — the app mints the cookie the websocket
	// server validates, so it needs the same secret.
	if spec.Realtime != nil && spec.Realtime.WebSocket != nil {
		ws := spec.Realtime.WebSocket
		if isDev && ws.JwtSecret != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvWSJwtSecret, Value: *ws.JwtSecret})
		} else if ws.JwtSecretFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvWSJwtSecret,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ws.JwtSecretFrom},
			})
		}
	}

	return envs
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
	envWSRealtimeChannelPrefix    = "REALTIME_CHANNEL_PREFIX"
	defaultWSCookie               = "superset-ws-token"
	featureFlagGlobalAsyncQueries = "GLOBAL_ASYNC_QUERIES"
)

// collectWebsocketEnvVars builds the env for the websocket server: the shared
// JWT secret plus a Redis connection pointing at the same coordination backend
// (DISTRIBUTED_COORDINATION_CONFIG) the Python app uses.
func collectWebsocketEnvVars(superset *supersetv1alpha1.Superset) []corev1.EnvVar {
	spec := &superset.Spec
	isDev := spec.Environment != nil && *spec.Environment == naming.EnvironmentDev

	envs := []corev1.EnvVar{
		{Name: envWSPort, Value: fmt.Sprintf("%d", resolveWebsocketPort(superset))},
		// Namespace the realtime Pub/Sub channel to match the Python side
		// (REALTIME_CHANNEL_PREFIX) so instances sharing one Valkey/Redis don't
		// cross-deliver. Consumed by superset-websocket once the image supports it.
		{Name: envWSRealtimeChannelPrefix, Value: valkeyKeyPrefix(spec, defaultInstanceName(superset)) + ":"},
	}

	if origins := deriveWebSocketAllowedOrigins(spec); len(origins) > 0 {
		envs = append(envs, corev1.EnvVar{Name: envWSAllowedOrigins, Value: strings.Join(origins, ",")})
	}

	if ws := realtimeWebSocket(spec); ws != nil {
		cookie := defaultWSCookie
		if ws.CookieName != nil {
			cookie = *ws.CookieName
		}
		envs = append(envs, corev1.EnvVar{Name: envWSJWTCookie, Value: cookie})
		if isDev && ws.JwtSecret != nil {
			envs = append(envs, corev1.EnvVar{Name: envWSJWTSecret, Value: *ws.JwtSecret})
		} else if ws.JwtSecretFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      envWSJWTSecret,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ws.JwtSecretFrom},
			})
		}
	}

	if v := spec.Valkey; v != nil {
		envs = append(envs, corev1.EnvVar{Name: envWSRedisHost, Value: v.Host})
		port := int32(6379)
		if v.Port != nil {
			port = *v.Port
		}
		envs = append(envs, corev1.EnvVar{Name: envWSRedisPort, Value: fmt.Sprintf("%d", port)})
		envs = append(envs, corev1.EnvVar{Name: envWSRedisDB, Value: fmt.Sprintf("%d", resolveValkeyCache(v.DistributedCoordination, 7, "coordination_", 300).Database)})
		if v.Username != nil {
			envs = append(envs, corev1.EnvVar{Name: envWSRedisUser, Value: *v.Username})
		}
		if isDev && v.Password != nil {
			envs = append(envs, corev1.EnvVar{Name: envWSRedisPass, Value: *v.Password})
		} else if v.PasswordFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      envWSRedisPass,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: v.PasswordFrom},
			})
		}
		if v.SSL != nil {
			envs = append(envs, corev1.EnvVar{Name: envWSRedisSSL, Value: "true"})
		}
	}

	return envs
}

// realtimeWebSocket returns spec.realtime.webSocket, or nil when unset.
func realtimeWebSocket(spec *supersetv1alpha1.SupersetSpec) *supersetv1alpha1.WebSocketTransportSpec {
	if spec.Realtime == nil {
		return nil
	}
	return spec.Realtime.WebSocket
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
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: naming.ConfigMapName(resourceBaseName),
					},
				},
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
