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
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

func TestSaCreateEnabled(t *testing.T) {
	trueVal := true
	falseVal := false

	tests := []struct {
		name string
		sa   *supersetv1alpha1.ServiceAccountSpec
		want bool
	}{
		{"nil (default true)", nil, true},
		{"create nil (default true)", &supersetv1alpha1.ServiceAccountSpec{}, true},
		{"create true", &supersetv1alpha1.ServiceAccountSpec{Create: &trueVal}, true},
		{"create false", &supersetv1alpha1.ServiceAccountSpec{Create: &falseVal}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := saCreateEnabled(tt.sa)
			if got != tt.want {
				t.Errorf("saCreateEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnyComponentEnabled(t *testing.T) {
	tests := []struct {
		name string
		spec supersetv1alpha1.SupersetSpec
		want bool
	}{
		{"no components", supersetv1alpha1.SupersetSpec{}, false},
		{"only webServer", supersetv1alpha1.SupersetSpec{WebServer: &supersetv1alpha1.WebServerComponentSpec{}}, true},
		{"only celeryWorker", supersetv1alpha1.SupersetSpec{CeleryWorker: &supersetv1alpha1.CeleryWorkerComponentSpec{}}, true},
		{"only websocketServer", supersetv1alpha1.SupersetSpec{WebsocketServer: &supersetv1alpha1.WebsocketServerComponentSpec{}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			superset := &supersetv1alpha1.Superset{Spec: tt.spec}
			if got := anyComponentEnabled(superset); got != tt.want {
				t.Errorf("anyComponentEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsComponentReady(t *testing.T) {
	tests := []struct {
		name   string
		status *supersetv1alpha1.ComponentRefStatus
		want   bool
	}{
		{"ready", &supersetv1alpha1.ComponentRefStatus{Phase: "Ready"}, true},
		{"progressing", &supersetv1alpha1.ComponentRefStatus{Phase: "Progressing"}, false},
		{"unavailable", &supersetv1alpha1.ComponentRefStatus{Phase: "Unavailable"}, false},
		{"pending", &supersetv1alpha1.ComponentRefStatus{Phase: "Pending"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isComponentReady(tt.status); got != tt.want {
				t.Errorf("isComponentReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeChecksum(t *testing.T) {
	obj := struct {
		SecretKey *string
		Metastore *supersetv1alpha1.MetastoreSpec
		Config    *string
	}{common.Ptr("test"), nil, nil}

	c1 := computeChecksum(obj)
	c2 := computeChecksum(obj)

	if c1 != c2 {
		t.Errorf("checksum not deterministic: %s vs %s", c1, c2)
	}
	if c1 == "" {
		t.Error("checksum should not be empty")
	}

	obj2 := struct{ Key *string }{common.Ptr("key2")}
	if c1 == computeChecksum(obj2) {
		t.Error("different inputs should produce different checksums")
	}
}

func TestBuildConfigInput(t *testing.T) {
	tests := []struct {
		name       string
		spec       *supersetv1alpha1.SupersetSpec
		wantMode   supersetconfig.MetastoreMode
		wantType   string
		wantDriver string
		wantConfig string
	}{
		{
			"empty spec",
			&supersetv1alpha1.SupersetSpec{},
			supersetconfig.MetastoreNone, "", "", "",
		},
		{
			"with config",
			&supersetv1alpha1.SupersetSpec{Config: common.Ptr("FEATURE_FLAGS = {}")},
			supersetconfig.MetastoreNone, "", "", "FEATURE_FLAGS = {}",
		},
		{
			"metastore passthrough",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{URI: common.Ptr("postgresql://user:pass@host/db")}},
			supersetconfig.MetastorePassthrough, "", "", "",
		},
		{
			"metastore structured postgresql",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{Host: common.Ptr("db.example.com")}},
			supersetconfig.MetastoreStructured, "PostgreSQL", "", "",
		},
		{
			"metastore structured mysql",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{Type: common.Ptr("MySQL"), Host: common.Ptr("db.example.com")}},
			supersetconfig.MetastoreStructured, "MySQL", "", "",
		},
		{
			"metastore structured custom driver",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{Type: common.Ptr("MySQL"), Host: common.Ptr("db.example.com"), Driver: common.Ptr("pymysql")}},
			supersetconfig.MetastoreStructured, "MySQL", "pymysql", "",
		},
		{
			"URI takes precedence over host",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{URI: common.Ptr("postgresql://..."), Host: common.Ptr("ignored")}},
			supersetconfig.MetastorePassthrough, "", "", "",
		},
		{
			"uriFrom triggers passthrough mode",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{URIFrom: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "db-secret"}, Key: "uri",
			}}},
			supersetconfig.MetastorePassthrough, "", "", "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := buildConfigInput(tt.spec)
			if input.MetastoreMode != tt.wantMode {
				t.Errorf("expected mode %d, got %d", tt.wantMode, input.MetastoreMode)
			}
			if input.DBType != tt.wantType {
				t.Errorf("expected type %q, got %q", tt.wantType, input.DBType)
			}
			if input.DBDriver != tt.wantDriver {
				t.Errorf("expected driver %q, got %q", tt.wantDriver, input.DBDriver)
			}
			if input.Config != tt.wantConfig {
				t.Errorf("expected config %q, got %q", tt.wantConfig, input.Config)
			}
		})
	}
}

func TestCollectSecretEnvVars_InstanceName(t *testing.T) {
	envs := collectSecretEnvVars(&supersetv1alpha1.SupersetSpec{}, "my-superset")
	envMap := envSliceToMap(envs)
	if got := envMap["SUPERSET_OPERATOR__INSTANCE_NAME"]; got != "my-superset" {
		t.Errorf("SUPERSET_OPERATOR__INSTANCE_NAME = %q, want my-superset", got)
	}
}

func TestCollectSecretEnvVars_SecretKey(t *testing.T) {
	tests := []struct {
		name    string
		spec    *supersetv1alpha1.SupersetSpec
		wantKey bool
	}{
		{"empty spec", &supersetv1alpha1.SupersetSpec{}, false},
		{"dev with key", &supersetv1alpha1.SupersetSpec{Environment: common.Ptr("Development"), SecretKey: common.Ptr("mykey")}, true},
		{"prod with key", &supersetv1alpha1.SupersetSpec{Environment: common.Ptr("Production"), SecretKey: common.Ptr("mykey")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envs := collectSecretEnvVars(tt.spec, "test")
			found := false
			for _, env := range envs {
				if env.Name == "SUPERSET_OPERATOR__SECRET_KEY" {
					found = true
				}
			}
			if found != tt.wantKey {
				t.Errorf("SUPERSET_OPERATOR__SECRET_KEY present=%v, want %v", found, tt.wantKey)
			}
		})
	}
}

func TestCollectSecretEnvVars_Metastore(t *testing.T) {
	t.Run("passthrough", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{URI: common.Ptr("postgresql://user:pass@host/db")},
		}
		envs := collectSecretEnvVars(spec, "test")
		envMap := envSliceToMap(envs)
		if _, ok := envMap["SUPERSET_OPERATOR__DB_URI"]; !ok {
			t.Errorf("expected SUPERSET_OPERATOR__DB_URI, got %v", envs)
		}
	})

	t.Run("structured with all fields", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Host: common.Ptr("db.example.com"), Port: common.Ptr(int32(5433)),
				Database: common.Ptr("superset"), Username: common.Ptr("admin"), Password: common.Ptr("secret"),
			},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec, "test"))

		if envMap["SUPERSET_OPERATOR__DB_HOST"] != "db.example.com" {
			t.Errorf("expected host db.example.com, got %s", envMap["SUPERSET_OPERATOR__DB_HOST"])
		}
		if envMap["SUPERSET_OPERATOR__DB_PORT"] != "5433" {
			t.Errorf("expected port 5433, got %s", envMap["SUPERSET_OPERATOR__DB_PORT"])
		}
		if envMap["SUPERSET_OPERATOR__DB_NAME"] != "superset" {
			t.Errorf("expected name superset, got %s", envMap["SUPERSET_OPERATOR__DB_NAME"])
		}
		if envMap["SUPERSET_OPERATOR__DB_USER"] != "admin" {
			t.Errorf("expected user admin, got %s", envMap["SUPERSET_OPERATOR__DB_USER"])
		}
		if envMap["SUPERSET_OPERATOR__DB_PASS"] != "secret" {
			t.Errorf("expected pass secret, got %s", envMap["SUPERSET_OPERATOR__DB_PASS"])
		}
	})

	t.Run("default postgresql port", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{Host: common.Ptr("db.example.com")},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec, "test"))
		if envMap["SUPERSET_OPERATOR__DB_PORT"] != "5432" {
			t.Errorf("expected default port 5432, got %s", envMap["SUPERSET_OPERATOR__DB_PORT"])
		}
	})

	t.Run("default mysql port", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{Type: common.Ptr("MySQL"), Host: common.Ptr("db.example.com")},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec, "test"))
		if envMap["SUPERSET_OPERATOR__DB_PORT"] != "3306" {
			t.Errorf("expected default MySQL port 3306, got %s", envMap["SUPERSET_OPERATOR__DB_PORT"])
		}
	})
}

func TestBuildOperatorInjected(t *testing.T) {
	configEnvVars := []corev1.EnvVar{{Name: "SUPERSET_OPERATOR__SECRET_KEY", Value: "test"}}
	injected := buildOperatorInjected("import os\n", "", "my-web-server", "v2", configEnvVars)

	if len(injected.Volumes) != 1 || injected.Volumes[0].Name != configVolumeName {
		t.Errorf("expected 1 config volume, got %d", len(injected.Volumes))
	}
	if len(injected.VolumeMounts) != 1 {
		t.Errorf("expected 1 volume mount, got %d", len(injected.VolumeMounts))
	}

	envMap := envSliceToMap(injected.Env)
	if envMap["SUPERSET_OPERATOR__SECRET_KEY"] != "test" {
		t.Errorf("expected SUPERSET_OPERATOR__SECRET_KEY=test from configEnvVars, got %s", envMap["SUPERSET_OPERATOR__SECRET_KEY"])
	}
	if envMap["SUPERSET_OPERATOR__FORCE_RELOAD"] != "v2" {
		t.Errorf("expected SUPERSET_OPERATOR__FORCE_RELOAD=v2, got %s", envMap["SUPERSET_OPERATOR__FORCE_RELOAD"])
	}

	// Empty config: no volumes, no mounts.
	empty := buildOperatorInjected("", "", "component", "", nil)
	if len(empty.Volumes) != 0 {
		t.Errorf("expected 0 volumes for empty config, got %d", len(empty.Volumes))
	}
	if len(empty.VolumeMounts) != 0 {
		t.Errorf("expected 0 volume mounts for empty config, got %d", len(empty.VolumeMounts))
	}

	bootstrapOnly := buildOperatorInjected("", "echo bootstrap", "component", "", nil)
	if len(bootstrapOnly.Volumes) != 1 {
		t.Errorf("expected bootstrap-only config volume, got %d", len(bootstrapOnly.Volumes))
	}
	if len(bootstrapOnly.VolumeMounts) != 1 {
		t.Errorf("expected bootstrap-only volume mount, got %d", len(bootstrapOnly.VolumeMounts))
	}
}

func TestFlatSpecFromResolution_Basic(t *testing.T) {
	flat := &resolution.FlatSpec{Replicas: 3}
	image := &supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "4.0.0"}

	result := flatSpecFromResolution(flat, image, nil, "my-sa")

	if result.Image.Repository != "apache/superset" || result.Image.Tag != "4.0.0" {
		t.Errorf("expected image apache/superset:4.0.0, got %s:%s", result.Image.Repository, result.Image.Tag)
	}
	if *result.Replicas != 3 {
		t.Errorf("expected replicas 3, got %d", *result.Replicas)
	}
	if result.ServiceAccountName != "my-sa" {
		t.Errorf("expected SA my-sa, got %s", result.ServiceAccountName)
	}
}

func TestFlatSpecFromResolution_ImageOverride(t *testing.T) {
	flat := &resolution.FlatSpec{Replicas: 1}
	image := &supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "4.0.0"}

	t.Run("full override", func(t *testing.T) {
		override := &supersetv1alpha1.ImageOverrideSpec{Tag: common.Ptr("custom-tag"), Repository: common.Ptr("custom/repo")}
		result := flatSpecFromResolution(flat, image, override, "")
		if result.Image.Tag != "custom-tag" || result.Image.Repository != "custom/repo" {
			t.Errorf("expected custom/repo:custom-tag, got %s:%s", result.Image.Repository, result.Image.Tag)
		}
	})

	t.Run("partial override", func(t *testing.T) {
		override := &supersetv1alpha1.ImageOverrideSpec{Tag: common.Ptr("custom-tag")}
		result := flatSpecFromResolution(flat, image, override, "")
		if result.Image.Tag != "custom-tag" {
			t.Errorf("expected tag override, got %s", result.Image.Tag)
		}
		if result.Image.Repository != "apache/superset" {
			t.Errorf("expected parent repository preserved, got %s", result.Image.Repository)
		}
	})

	t.Run("pull policy override", func(t *testing.T) {
		imageWithPolicy := &supersetv1alpha1.ImageSpec{
			Repository: "apache/superset",
			Tag:        "4.0.0",
			PullPolicy: corev1.PullIfNotPresent,
		}
		always := corev1.PullAlways
		override := &supersetv1alpha1.ImageOverrideSpec{PullPolicy: &always}
		result := flatSpecFromResolution(flat, imageWithPolicy, override, "")
		if result.Image.PullPolicy != corev1.PullAlways {
			t.Errorf("expected component override PullAlways, got %s", result.Image.PullPolicy)
		}
		// Repository and Tag should still inherit from parent.
		if result.Image.Repository != "apache/superset" || result.Image.Tag != "4.0.0" {
			t.Errorf("expected parent repo/tag preserved, got %s:%s", result.Image.Repository, result.Image.Tag)
		}
	})

	t.Run("pull policy inherits when override unset", func(t *testing.T) {
		imageWithPolicy := &supersetv1alpha1.ImageSpec{
			Repository: "apache/superset",
			Tag:        "4.0.0",
			PullPolicy: corev1.PullIfNotPresent,
		}
		override := &supersetv1alpha1.ImageOverrideSpec{Tag: common.Ptr("custom-tag")}
		result := flatSpecFromResolution(flat, imageWithPolicy, override, "")
		if result.Image.PullPolicy != corev1.PullIfNotPresent {
			t.Errorf("expected parent PullIfNotPresent preserved, got %s", result.Image.PullPolicy)
		}
	})
}

func TestCollectSecretEnvVars_FromFields(t *testing.T) {
	secretRef := func(name, key string) *corev1.SecretKeySelector {
		return &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Key:                  key,
		}
	}

	t.Run("prod mode with all From fields", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			SecretKeyFrom: secretRef("app-secret", "secret-key"),
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Host:         common.Ptr("postgres"),
				Database:     common.Ptr("superset"),
				Username:     common.Ptr("admin"),
				PasswordFrom: secretRef("db-secret", "password"),
			},
		}
		envs := collectSecretEnvVars(spec, "test")
		for _, env := range envs {
			switch env.Name {
			case "SUPERSET_OPERATOR__SECRET_KEY":
				if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef.Name != "app-secret" {
					t.Errorf("SUPERSET_OPERATOR__SECRET_KEY: expected secretKeyRef to app-secret, got %+v", env.ValueFrom)
				}
			case "SUPERSET_OPERATOR__DB_PASS":
				if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef.Key != "password" {
					t.Errorf("SUPERSET_OPERATOR__DB_PASS: expected secretKeyRef key=password, got %+v", env.ValueFrom)
				}
			case "SUPERSET_OPERATOR__DB_HOST":
				if env.Value != "postgres" {
					t.Errorf("expected SUPERSET_OPERATOR__DB_HOST=postgres, got %s", env.Value)
				}
			}
		}
	})

	t.Run("uriFrom produces ValueFrom env var", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{
				URIFrom: secretRef("db-secret", "connection-string"),
			},
		}
		envs := collectSecretEnvVars(spec, "test")
		var uri *corev1.EnvVar
		for i := range envs {
			if envs[i].Name == "SUPERSET_OPERATOR__DB_URI" {
				uri = &envs[i]
				break
			}
		}
		if uri == nil {
			t.Fatalf("expected SUPERSET_OPERATOR__DB_URI env, got %v", envs)
		}
		if uri.ValueFrom == nil || uri.ValueFrom.SecretKeyRef.Key != "connection-string" {
			t.Errorf("expected secretKeyRef key=connection-string, got %+v", uri.ValueFrom)
		}
	})
}

func envSliceToMap(envs []corev1.EnvVar) map[string]string {
	m := make(map[string]string)
	for _, env := range envs {
		m[env.Name] = env.Value
	}
	return m
}

func TestResolveValkeyResults(t *testing.T) {
	t.Run("nil spec applies defaults", func(t *testing.T) {
		// Use a non-standard defaultDB and prefix to prove both arguments are
		// honored (not hardcoded) and to keep the function's default parameters
		// genuinely variable across call sites.
		got := resolveValkeyResults(nil, 13, "custom_default_")
		if got.Disabled {
			t.Error("expected not disabled by default")
		}
		if got.Database != 13 {
			t.Errorf("expected default database 13, got %d", got.Database)
		}
		if got.KeyPrefix != "custom_default_" {
			t.Errorf("expected default key prefix, got %q", got.KeyPrefix)
		}
	})

	t.Run("disabled true", func(t *testing.T) {
		got := resolveValkeyResults(&supersetv1alpha1.ValkeyResultsBackendSpec{Disabled: common.Ptr(true)}, 6, "superset_results_")
		if !got.Disabled {
			t.Error("expected disabled=true")
		}
		if got.Database != 6 {
			t.Errorf("expected default database 6, got %d", got.Database)
		}
	})

	t.Run("database override", func(t *testing.T) {
		got := resolveValkeyResults(&supersetv1alpha1.ValkeyResultsBackendSpec{Database: common.Ptr(int32(9))}, 6, "superset_results_")
		if got.Database != 9 {
			t.Errorf("expected database override 9, got %d", got.Database)
		}
		if got.KeyPrefix != "superset_results_" {
			t.Errorf("expected default key prefix preserved, got %q", got.KeyPrefix)
		}
	})

	t.Run("key prefix override", func(t *testing.T) {
		got := resolveValkeyResults(&supersetv1alpha1.ValkeyResultsBackendSpec{KeyPrefix: common.Ptr("custom_results_")}, 6, "superset_results_")
		if got.KeyPrefix != "custom_results_" {
			t.Errorf("expected key prefix override, got %q", got.KeyPrefix)
		}
		if got.Database != 6 {
			t.Errorf("expected default database preserved, got %d", got.Database)
		}
	})
}

func TestResolveValkeyCache(t *testing.T) {
	t.Run("nil spec applies defaults", func(t *testing.T) {
		// Non-standard defaults prove all three arguments are honored, not hardcoded.
		got := resolveValkeyCache(nil, 7, "custom_cache_", 600)
		if got.Disabled {
			t.Error("expected not disabled by default")
		}
		if got.Database != 7 {
			t.Errorf("expected default database 7, got %d", got.Database)
		}
		if got.KeyPrefix != "custom_cache_" {
			t.Errorf("expected default key prefix, got %q", got.KeyPrefix)
		}
		if got.DefaultTimeout != 600 {
			t.Errorf("expected default timeout 600, got %d", got.DefaultTimeout)
		}
	})

	t.Run("disabled true", func(t *testing.T) {
		got := resolveValkeyCache(&supersetv1alpha1.ValkeyCacheSpec{Disabled: common.Ptr(true)}, 1, "superset_cache_", 300)
		if !got.Disabled {
			t.Error("expected disabled=true")
		}
		if got.Database != 1 {
			t.Errorf("expected default database 1 preserved, got %d", got.Database)
		}
	})

	t.Run("disabled false keeps defaults", func(t *testing.T) {
		// An explicit false must not flip the (false) zero value or disturb defaults.
		got := resolveValkeyCache(&supersetv1alpha1.ValkeyCacheSpec{Disabled: common.Ptr(false)}, 1, "superset_cache_", 300)
		if got.Disabled {
			t.Error("expected disabled=false")
		}
	})

	t.Run("database override", func(t *testing.T) {
		got := resolveValkeyCache(&supersetv1alpha1.ValkeyCacheSpec{Database: common.Ptr(int32(4))}, 1, "superset_cache_", 300)
		if got.Database != 4 {
			t.Errorf("expected database override 4, got %d", got.Database)
		}
		if got.KeyPrefix != "superset_cache_" {
			t.Errorf("expected default key prefix preserved, got %q", got.KeyPrefix)
		}
		if got.DefaultTimeout != 300 {
			t.Errorf("expected default timeout preserved, got %d", got.DefaultTimeout)
		}
	})

	t.Run("key prefix override", func(t *testing.T) {
		got := resolveValkeyCache(&supersetv1alpha1.ValkeyCacheSpec{KeyPrefix: common.Ptr("custom_cache_")}, 1, "superset_cache_", 300)
		if got.KeyPrefix != "custom_cache_" {
			t.Errorf("expected key prefix override, got %q", got.KeyPrefix)
		}
	})

	t.Run("default timeout override", func(t *testing.T) {
		got := resolveValkeyCache(&supersetv1alpha1.ValkeyCacheSpec{DefaultTimeout: common.Ptr(int32(900))}, 1, "superset_cache_", 300)
		if got.DefaultTimeout != 900 {
			t.Errorf("expected default timeout override 900, got %d", got.DefaultTimeout)
		}
		if got.Database != 1 {
			t.Errorf("expected default database preserved, got %d", got.Database)
		}
	})
}

func TestResolveValkeyCelery(t *testing.T) {
	t.Run("nil spec applies defaults", func(t *testing.T) {
		got := resolveValkeyCelery(nil, 5)
		if got.Disabled {
			t.Error("expected not disabled by default")
		}
		if got.Database != 5 {
			t.Errorf("expected default database 5, got %d", got.Database)
		}
	})

	t.Run("disabled true", func(t *testing.T) {
		got := resolveValkeyCelery(&supersetv1alpha1.ValkeyCelerySpec{Disabled: common.Ptr(true)}, 0)
		if !got.Disabled {
			t.Error("expected disabled=true")
		}
		if got.Database != 0 {
			t.Errorf("expected default database 0 preserved, got %d", got.Database)
		}
	})

	t.Run("database override", func(t *testing.T) {
		got := resolveValkeyCelery(&supersetv1alpha1.ValkeyCelerySpec{Database: common.Ptr(int32(2))}, 0)
		if got.Database != 2 {
			t.Errorf("expected database override 2, got %d", got.Database)
		}
	})
}

func TestBuildConfigInput_Valkey(t *testing.T) {
	t.Run("nil valkey", func(t *testing.T) {
		input := buildConfigInput(&supersetv1alpha1.SupersetSpec{})
		if input.Valkey != nil {
			t.Error("expected nil ValkeyInput when spec.Valkey is nil")
		}
	})

	t.Run("minimal valkey", func(t *testing.T) {
		input := buildConfigInput(&supersetv1alpha1.SupersetSpec{
			Valkey: &supersetv1alpha1.ValkeySpec{Host: "valkey.default.svc"},
		})
		if input.Valkey == nil {
			t.Fatal("expected non-nil ValkeyInput")
		}
		// Check defaults
		if input.Valkey.Cache.Database != 1 {
			t.Errorf("expected cache db=1, got %d", input.Valkey.Cache.Database)
		}
		if input.Valkey.Cache.KeyPrefix != "superset_" {
			t.Errorf("expected cache prefix 'superset_', got %q", input.Valkey.Cache.KeyPrefix)
		}
		if input.Valkey.Cache.DefaultTimeout != 300 {
			t.Errorf("expected cache timeout=300, got %d", input.Valkey.Cache.DefaultTimeout)
		}
		if input.Valkey.DataCache.Database != 2 {
			t.Errorf("expected data cache db=2, got %d", input.Valkey.DataCache.Database)
		}
		if input.Valkey.DataCache.DefaultTimeout != 86400 {
			t.Errorf("expected data cache timeout=86400, got %d", input.Valkey.DataCache.DefaultTimeout)
		}
		if input.Valkey.CeleryBroker.Database != 0 {
			t.Errorf("expected broker db=0, got %d", input.Valkey.CeleryBroker.Database)
		}
		if input.Valkey.ResultsBackend.Database != 6 {
			t.Errorf("expected results db=6, got %d", input.Valkey.ResultsBackend.Database)
		}
		if input.Valkey.SSL {
			t.Error("expected SSL=false")
		}
	})

	t.Run("custom overrides", func(t *testing.T) {
		input := buildConfigInput(&supersetv1alpha1.SupersetSpec{
			Valkey: &supersetv1alpha1.ValkeySpec{
				Host: "valkey.default.svc",
				Cache: &supersetv1alpha1.ValkeyCacheSpec{
					Database:       common.Ptr(int32(10)),
					KeyPrefix:      common.Ptr("custom_"),
					DefaultTimeout: common.Ptr(int32(600)),
				},
				CeleryBroker: &supersetv1alpha1.ValkeyCelerySpec{
					Database: common.Ptr(int32(14)),
				},
				ResultsBackend: &supersetv1alpha1.ValkeyResultsBackendSpec{
					Disabled: common.Ptr(true),
				},
			},
		})
		if input.Valkey.Cache.Database != 10 {
			t.Errorf("expected cache db=10, got %d", input.Valkey.Cache.Database)
		}
		if input.Valkey.Cache.KeyPrefix != "custom_" {
			t.Errorf("expected prefix 'custom_', got %q", input.Valkey.Cache.KeyPrefix)
		}
		if input.Valkey.Cache.DefaultTimeout != 600 {
			t.Errorf("expected timeout=600, got %d", input.Valkey.Cache.DefaultTimeout)
		}
		if input.Valkey.CeleryBroker.Database != 14 {
			t.Errorf("expected broker db=14, got %d", input.Valkey.CeleryBroker.Database)
		}
		if !input.Valkey.ResultsBackend.Disabled {
			t.Error("expected results backend disabled")
		}
	})

	t.Run("with SSL", func(t *testing.T) {
		input := buildConfigInput(&supersetv1alpha1.SupersetSpec{
			Valkey: &supersetv1alpha1.ValkeySpec{
				Host: "valkey.default.svc",
				SSL: &supersetv1alpha1.ValkeySSLSpec{
					CertRequired: common.Ptr("required"),
					KeyFile:      common.Ptr("/tls/key.pem"),
					CertFile:     common.Ptr("/tls/cert.pem"),
					CACertFile:   common.Ptr("/tls/ca.pem"),
				},
			},
		})
		if !input.Valkey.SSL {
			t.Error("expected SSL=true")
		}
		if input.Valkey.SSLCertRequired != "required" {
			t.Errorf("expected certRequired='required', got %q", input.Valkey.SSLCertRequired)
		}
		if input.Valkey.SSLKeyFile != "/tls/key.pem" {
			t.Errorf("expected keyFile, got %q", input.Valkey.SSLKeyFile)
		}
	})
}

func TestCollectSecretEnvVars_Valkey(t *testing.T) {
	t.Run("host and default port", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Valkey: &supersetv1alpha1.ValkeySpec{Host: "valkey.default.svc"},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec, "test"))
		if envMap["SUPERSET_OPERATOR__VALKEY_HOST"] != "valkey.default.svc" {
			t.Errorf("expected host, got %q", envMap["SUPERSET_OPERATOR__VALKEY_HOST"])
		}
		if envMap["SUPERSET_OPERATOR__VALKEY_PORT"] != "6379" {
			t.Errorf("expected port 6379, got %q", envMap["SUPERSET_OPERATOR__VALKEY_PORT"])
		}
	})

	t.Run("custom port", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Valkey: &supersetv1alpha1.ValkeySpec{Host: "valkey", Port: common.Ptr(int32(6380))},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec, "test"))
		if envMap["SUPERSET_OPERATOR__VALKEY_PORT"] != "6380" {
			t.Errorf("expected port 6380, got %q", envMap["SUPERSET_OPERATOR__VALKEY_PORT"])
		}
	})

	t.Run("username", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Valkey: &supersetv1alpha1.ValkeySpec{Host: "valkey", Username: common.Ptr("acl-user")},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec, "test"))
		if envMap["SUPERSET_OPERATOR__VALKEY_USER"] != "acl-user" {
			t.Errorf("expected username, got %q", envMap["SUPERSET_OPERATOR__VALKEY_USER"])
		}
	})

	t.Run("dev mode password", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Environment: common.Ptr("Development"),
			Valkey:      &supersetv1alpha1.ValkeySpec{Host: "valkey", Password: common.Ptr("secret")},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec, "test"))
		if envMap["SUPERSET_OPERATOR__VALKEY_PASS"] != "secret" {
			t.Errorf("expected password, got %q", envMap["SUPERSET_OPERATOR__VALKEY_PASS"])
		}
	})

	t.Run("prod mode password ignored", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Environment: common.Ptr("Production"),
			Valkey:      &supersetv1alpha1.ValkeySpec{Host: "valkey", Password: common.Ptr("secret")},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec, "test"))
		if _, ok := envMap["SUPERSET_OPERATOR__VALKEY_PASS"]; ok {
			t.Error("prod mode should not emit inline password")
		}
	})

	t.Run("passwordFrom", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Valkey: &supersetv1alpha1.ValkeySpec{
				Host: "valkey",
				PasswordFrom: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "valkey-secret"},
					Key:                  "password",
				},
			},
		}
		envs := collectSecretEnvVars(spec, "test")
		for _, env := range envs {
			if env.Name == "SUPERSET_OPERATOR__VALKEY_PASS" {
				if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef.Name != "valkey-secret" {
					t.Errorf("expected secretKeyRef to valkey-secret, got %+v", env.ValueFrom)
				}
				return
			}
		}
		t.Error("expected SUPERSET_OPERATOR__VALKEY_PASS env var")
	})

	t.Run("no valkey", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{}
		envs := collectSecretEnvVars(spec, "test")
		for _, env := range envs {
			if env.Name == "SUPERSET_OPERATOR__VALKEY_HOST" {
				t.Error("should not emit valkey env vars when spec.Valkey is nil")
			}
		}
	})
}

func TestBuildInitCommand(t *testing.T) {
	loadExamples := true
	noLoadExamples := false

	tests := []struct {
		name       string
		init       *supersetv1alpha1.InitTaskSpec
		wantScript string
	}{
		{
			name:       "nil init runs plain superset init",
			init:       nil,
			wantScript: "superset init",
		},
		{
			name:       "empty init runs plain superset init",
			init:       &supersetv1alpha1.InitTaskSpec{},
			wantScript: "superset init",
		},
		{
			name:       "admin user appends create-admin",
			init:       &supersetv1alpha1.InitTaskSpec{AdminUser: &supersetv1alpha1.AdminUserSpec{}},
			wantScript: `superset init && (superset fab create-admin --username "$SUPERSET_OPERATOR__ADMIN_USERNAME" --password "$SUPERSET_OPERATOR__ADMIN_PASSWORD" --firstname "$SUPERSET_OPERATOR__ADMIN_FIRST_NAME" --lastname "$SUPERSET_OPERATOR__ADMIN_LAST_NAME" --email "$SUPERSET_OPERATOR__ADMIN_EMAIL" || true)`,
		},
		{
			name:       "load examples true appends load-examples",
			init:       &supersetv1alpha1.InitTaskSpec{LoadExamples: &loadExamples},
			wantScript: "superset init && superset load-examples",
		},
		{
			name:       "load examples false is a no-op",
			init:       &supersetv1alpha1.InitTaskSpec{LoadExamples: &noLoadExamples},
			wantScript: "superset init",
		},
		{
			name:       "admin user and load examples both append in order",
			init:       &supersetv1alpha1.InitTaskSpec{AdminUser: &supersetv1alpha1.AdminUserSpec{}, LoadExamples: &loadExamples},
			wantScript: `superset init && (superset fab create-admin --username "$SUPERSET_OPERATOR__ADMIN_USERNAME" --password "$SUPERSET_OPERATOR__ADMIN_PASSWORD" --firstname "$SUPERSET_OPERATOR__ADMIN_FIRST_NAME" --lastname "$SUPERSET_OPERATOR__ADMIN_LAST_NAME" --email "$SUPERSET_OPERATOR__ADMIN_EMAIL" || true) && superset load-examples`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildInitCommand(tt.init)
			want := []string{"/bin/sh", "-c", tt.wantScript}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("buildInitCommand() =\n  %#v\nwant\n  %#v", got, want)
			}
		})
	}
}
