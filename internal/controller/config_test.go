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

func TestIsReadyString(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"", false},
		{"0/0", false},
		{"0/1", false},
		{"1/1", true},
		{"2/2", true},
		{"1/2", false},
		{"invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := isReadyString(tt.input); got != tt.want {
				t.Errorf("isReadyString(%q) = %v, want %v", tt.input, got, tt.want)
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
		wantDriver string
		wantConfig string
	}{
		{
			"empty spec",
			&supersetv1alpha1.SupersetSpec{},
			supersetconfig.MetastoreNone, "", "",
		},
		{
			"with config",
			&supersetv1alpha1.SupersetSpec{Config: common.Ptr("FEATURE_FLAGS = {}")},
			supersetconfig.MetastoreNone, "", "FEATURE_FLAGS = {}",
		},
		{
			"metastore passthrough",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{URI: common.Ptr("postgresql://user:pass@host/db")}},
			supersetconfig.MetastorePassthrough, "", "",
		},
		{
			"metastore structured postgresql",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{Host: common.Ptr("db.example.com")}},
			supersetconfig.MetastoreStructured, "postgresql", "",
		},
		{
			"metastore structured mysql",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{Type: common.Ptr("mysql"), Host: common.Ptr("db.example.com")}},
			supersetconfig.MetastoreStructured, "mysql", "",
		},
		{
			"URI takes precedence over host",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{URI: common.Ptr("postgresql://..."), Host: common.Ptr("ignored")}},
			supersetconfig.MetastorePassthrough, "", "",
		},
		{
			"uriFrom triggers passthrough mode",
			&supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{URIFrom: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "db-secret"}, Key: "uri",
			}}},
			supersetconfig.MetastorePassthrough, "", "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := buildConfigInput(tt.spec)
			if input.MetastoreMode != tt.wantMode {
				t.Errorf("expected mode %d, got %d", tt.wantMode, input.MetastoreMode)
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

func TestCollectSecretEnvVars_SecretKey(t *testing.T) {
	tests := []struct {
		name    string
		spec    *supersetv1alpha1.SupersetSpec
		wantKey bool
	}{
		{"empty spec", &supersetv1alpha1.SupersetSpec{}, false},
		{"dev with key", &supersetv1alpha1.SupersetSpec{Environment: common.Ptr("dev"), SecretKey: common.Ptr("mykey")}, true},
		{"prod with key", &supersetv1alpha1.SupersetSpec{Environment: common.Ptr("prod"), SecretKey: common.Ptr("mykey")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envs := collectSecretEnvVars(tt.spec)
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
		envs := collectSecretEnvVars(spec)
		if len(envs) != 1 || envs[0].Name != "SUPERSET_OPERATOR__DB_URI" {
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
		envMap := envSliceToMap(collectSecretEnvVars(spec))

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
		envMap := envSliceToMap(collectSecretEnvVars(spec))
		if envMap["SUPERSET_OPERATOR__DB_PORT"] != "5432" {
			t.Errorf("expected default port 5432, got %s", envMap["SUPERSET_OPERATOR__DB_PORT"])
		}
	})

	t.Run("default mysql port", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{Type: common.Ptr("mysql"), Host: common.Ptr("db.example.com")},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec))
		if envMap["SUPERSET_OPERATOR__DB_PORT"] != "3306" {
			t.Errorf("expected default MySQL port 3306, got %s", envMap["SUPERSET_OPERATOR__DB_PORT"])
		}
	})
}

func TestBuildOperatorInjected(t *testing.T) {
	configEnvVars := []corev1.EnvVar{{Name: "SUPERSET_OPERATOR__SECRET_KEY", Value: "test"}}
	injected := buildOperatorInjected("import os\n", "my-web-server", "v2", configEnvVars)

	if len(injected.Volumes) != 1 || injected.Volumes[0].Name != configVolumeName {
		t.Errorf("expected 1 config volume, got %d", len(injected.Volumes))
	}
	if len(injected.VolumeMounts) != 1 {
		t.Errorf("expected 1 volume mount, got %d", len(injected.VolumeMounts))
	}

	envMap := envSliceToMap(injected.Env)
	if envMap["PYTHONPATH"] == "" {
		t.Error("expected PYTHONPATH env var")
	}
	if envMap["SUPERSET_OPERATOR__SECRET_KEY"] != "test" {
		t.Errorf("expected SUPERSET_OPERATOR__SECRET_KEY=test from configEnvVars, got %s", envMap["SUPERSET_OPERATOR__SECRET_KEY"])
	}
	if envMap["SUPERSET_OPERATOR__FORCE_RELOAD"] != "v2" {
		t.Errorf("expected SUPERSET_OPERATOR__FORCE_RELOAD=v2, got %s", envMap["SUPERSET_OPERATOR__FORCE_RELOAD"])
	}

	// Empty config: no volumes, no mounts, no PYTHONPATH.
	empty := buildOperatorInjected("", "child", "", nil)
	if len(empty.Volumes) != 0 {
		t.Errorf("expected 0 volumes for empty config, got %d", len(empty.Volumes))
	}
	if len(empty.VolumeMounts) != 0 {
		t.Errorf("expected 0 volume mounts for empty config, got %d", len(empty.VolumeMounts))
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
		envs := collectSecretEnvVars(spec)
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
		envs := collectSecretEnvVars(spec)
		if len(envs) != 1 || envs[0].Name != "SUPERSET_OPERATOR__DB_URI" {
			t.Fatalf("expected single SUPERSET_OPERATOR__DB_URI env, got %v", envs)
		}
		if envs[0].ValueFrom == nil || envs[0].ValueFrom.SecretKeyRef.Key != "connection-string" {
			t.Errorf("expected secretKeyRef key=connection-string, got %+v", envs[0].ValueFrom)
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
		envMap := envSliceToMap(collectSecretEnvVars(spec))
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
		envMap := envSliceToMap(collectSecretEnvVars(spec))
		if envMap["SUPERSET_OPERATOR__VALKEY_PORT"] != "6380" {
			t.Errorf("expected port 6380, got %q", envMap["SUPERSET_OPERATOR__VALKEY_PORT"])
		}
	})

	t.Run("dev mode password", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Environment: common.Ptr("dev"),
			Valkey:      &supersetv1alpha1.ValkeySpec{Host: "valkey", Password: common.Ptr("secret")},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec))
		if envMap["SUPERSET_OPERATOR__VALKEY_PASS"] != "secret" {
			t.Errorf("expected password, got %q", envMap["SUPERSET_OPERATOR__VALKEY_PASS"])
		}
	})

	t.Run("prod mode password ignored", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Environment: common.Ptr("prod"),
			Valkey:      &supersetv1alpha1.ValkeySpec{Host: "valkey", Password: common.Ptr("secret")},
		}
		envMap := envSliceToMap(collectSecretEnvVars(spec))
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
		envs := collectSecretEnvVars(spec)
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
		envs := collectSecretEnvVars(spec)
		for _, env := range envs {
			if env.Name == "SUPERSET_OPERATOR__VALKEY_HOST" {
				t.Error("should not emit valkey env vars when spec.Valkey is nil")
			}
		}
	})
}
