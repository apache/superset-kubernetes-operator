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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// TestInitInputs_ChangesWithRenderedConfig asserts that init's checksum input
// reflects every config-rendering field the lifecycle Job actually consumes.
// The previous hand-picked configChecksum struct silently skipped init re-runs
// when fields like featureFlags changed; hashing the rendered
// superset_config.py forecloses that class of bug.
func TestInitInputs_ChangesWithRenderedConfig(t *testing.T) {
	r := &SupersetReconciler{}

	base := func() *supersetv1alpha1.Superset {
		s := &supersetv1alpha1.Superset{}
		s.Name = "test"
		s.Spec.Image = supersetv1alpha1.ImageSpec{Tag: "4.1.4"}
		s.Spec.SecretKeyFrom = secretKeyRef("secret", "key")
		s.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
			Host:         new("db.svc"),
			Database:     new("superset"),
			Username:     new("superset"),
			PasswordFrom: secretKeyRef("db-secret", "password"),
		}
		// Valkey exercises Celery connectivity rendering in the lifecycle config.
		s.Spec.Valkey = &supersetv1alpha1.ValkeySpec{Host: "valkey.svc"}
		s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
			Init: &supersetv1alpha1.InitTaskSpec{},
		}
		return s
	}

	tests := []struct {
		name   string
		dev    bool
		mutate func(*supersetv1alpha1.Superset)
	}{
		// Config-rendering changes — covered by ConfigHash.
		{
			name: "featureFlags",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.FeatureFlags = map[string]bool{"ALERT_REPORTS": true}
			},
		},
		{
			name: "celery config in raw python",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.Config = new(`CeleryConfig.imports = ("superset.tasks.cache",)`)
			},
		},
		{
			name: "lifecycle.config",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.Lifecycle.Config = new("EXTRA_LIFECYCLE = True")
			},
		},
		{
			name: "lifecycle.sqlaEngineOptions.preset=disabled",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.Lifecycle.SQLAlchemyEngineOptions = &supersetv1alpha1.SQLAlchemyEngineOptionsSpec{
					Preset: new(supersetv1alpha1.PresetDisabled),
				}
			},
		},
		{
			name: "valkey.cache.database",
			mutate: func(s *supersetv1alpha1.Superset) {
				v := int32(10)
				s.Spec.Valkey.Cache = &supersetv1alpha1.ValkeyCacheSpec{Database: &v}
			},
		},

		// Env-only changes — covered by EnvHash. The rendered Python references
		// these via env var names (e.g. os.environ['SUPERSET_OPERATOR__DB_HOST']),
		// so the rendered string is identical when the value changes; only the
		// resolved env var slice differs.
		{
			name: "metastore.host",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.Metastore.Host = new("db-2.svc")
			},
		},
		{
			name: "metastore.database",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.Metastore.Database = new("superset_v2")
			},
		},
		{
			name: "metastore.passwordFrom (different ref)",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.Metastore.PasswordFrom = secretKeyRef("rotated-db-secret", "password")
			},
		},
		{
			name: "secretKeyFrom (different ref)",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.SecretKeyFrom = secretKeyRef("rotated-secret", "key")
			},
		},
		{
			name: "previousSecretKeyFrom",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.PreviousSecretKeyFrom = secretKeyRef("prev-secret", "key")
			},
		},
		{
			name: "valkey.host",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.Valkey.Host = "valkey-2.svc"
			},
		},
		{
			name: "lifecycle.init.adminUser",
			dev:  true,
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.Lifecycle.Init.AdminUser = &supersetv1alpha1.AdminUserSpec{
					Username: new("admin"),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unmodified, modified := base(), base()
			if tt.dev {
				// Development-only fields are compared against a Development baseline.
				unmodified.Spec.Environment = new(supersetv1alpha1.EnvironmentDevelopment)
				modified.Spec.Environment = new(supersetv1alpha1.EnvironmentDevelopment)
			}
			tt.mutate(modified)
			got := r.initInputs(modified)
			if computeChecksum(got) == computeChecksum(r.initInputs(unmodified)) {
				t.Errorf("expected init checksum input to change when %s mutated, but it stayed the same", tt.name)
			}
		})
	}
}

// TestInitInputs_StableForNonConfigFields asserts that mutating fields that do
// not influence the rendered superset_config.py or resolved env vars (e.g.,
// per-Pod resources, replica counts, forceReload) does not change init's
// checksum input — re-running init is expensive (admin user creation, role
// updates, examples loading) and should only happen when config or env
// actually changed.
func TestInitInputs_StableForNonConfigFields(t *testing.T) {
	r := &SupersetReconciler{}

	base := func() *supersetv1alpha1.Superset {
		s := &supersetv1alpha1.Superset{}
		s.Name = "test"
		s.Spec.Image = supersetv1alpha1.ImageSpec{Tag: "4.1.4"}
		s.Spec.SecretKeyFrom = secretKeyRef("secret", "key")
		s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
			Init: &supersetv1alpha1.InitTaskSpec{},
		}
		return s
	}

	baseline := r.initInputs(base())

	tests := []struct {
		name   string
		mutate func(*supersetv1alpha1.Superset)
	}{
		{
			name: "spec.replicas",
			mutate: func(s *supersetv1alpha1.Superset) {
				three := int32(3)
				s.Spec.Replicas = &three
			},
		},
		{
			name: "spec.forceReload",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.ForceReload = "2026-05-22T15:00:00Z"
			},
		},
		{
			name: "spec.logLevel",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.LogLevel = new(supersetv1alpha1.LogLevelDebug)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modified := base()
			tt.mutate(modified)
			if computeChecksum(r.initInputs(modified)) != computeChecksum(baseline) {
				t.Errorf("expected init checksum input to be stable when only %s changed", tt.name)
			}
		})
	}
}

func secretKeyRef(name, key string) *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{
		Name: name,
		Key:  key,
	}
}

// Defense in depth: adminUser and loadExamples are Development-only. Outside
// Development the init task runs plain `superset init` with no admin
// credentials, even if the CR slipped past CEL.
func TestInitTask_DevOnlyFieldsIgnoredOutsideDevelopment(t *testing.T) {
	newSuperset := func(env supersetv1alpha1.Environment) *supersetv1alpha1.Superset {
		s := &supersetv1alpha1.Superset{}
		s.Name = "test"
		s.Spec.Environment = &env
		s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{Init: &supersetv1alpha1.InitTaskSpec{
			AdminUser:    &supersetv1alpha1.AdminUserSpec{Password: new("hunter2")},
			LoadExamples: new(true),
		}}
		return s
	}
	hasAdminEnv := func(env []corev1.EnvVar) bool {
		for _, e := range env {
			if e.Name == "SUPERSET_OPERATOR__ADMIN_PASSWORD" {
				return true
			}
		}
		return false
	}

	for _, env := range []supersetv1alpha1.Environment{supersetv1alpha1.EnvironmentStaging, supersetv1alpha1.EnvironmentProduction} {
		t.Run(string(env), func(t *testing.T) {
			s := newSuperset(env)
			if got, want := defaultInitCommand(s), []string{"/bin/sh", "-c", "superset init"}; !slices.Equal(got, want) {
				t.Errorf("init command = %#v, want %#v", got, want)
			}
			if hasAdminEnv(initTaskEnv(s)) {
				t.Error("expected no admin credential env vars outside Development")
			}
		})
	}

	t.Run("Development", func(t *testing.T) {
		s := newSuperset(supersetv1alpha1.EnvironmentDevelopment)
		cmd := defaultInitCommand(s)
		if len(cmd) != 3 || cmd[2] == "superset init" {
			t.Errorf("expected admin and examples steps in Development, got %#v", cmd)
		}
		if !hasAdminEnv(initTaskEnv(s)) {
			t.Error("expected admin credential env vars in Development")
		}
	})
}
