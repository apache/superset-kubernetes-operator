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
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestTaskMaxRetriesValue(t *testing.T) {
	r := &SupersetReconciler{}

	t.Run("defaults when unset", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{Migrate: &supersetv1alpha1.MigrateTaskSpec{}},
		}}
		assert.Equal(t, defaultMaxRetries, r.taskMaxRetriesValue(superset, taskTypeMigrate))
	})

	t.Run("defaults when lifecycle nil", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{}
		assert.Equal(t, defaultMaxRetries, r.taskMaxRetriesValue(superset, taskTypeMigrate))
	})

	t.Run("uses explicit value", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{Migrate: &supersetv1alpha1.MigrateTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{MaxRetries: int32Ptr(7)},
			}},
		}}
		assert.Equal(t, int32(7), r.taskMaxRetriesValue(superset, taskTypeMigrate))
	})

	t.Run("defaults for unknown task type", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{}
		assert.Equal(t, defaultMaxRetries, r.taskMaxRetriesValue(superset, "Bogus"))
	})
}

func TestTaskTimeoutValue(t *testing.T) {
	r := &SupersetReconciler{}

	t.Run("defaults when unset", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{Migrate: &supersetv1alpha1.MigrateTaskSpec{}},
		}}
		assert.Equal(t, defaultInitTimeout, r.taskTimeoutValue(superset, taskTypeMigrate))
	})

	t.Run("uses explicit value", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{Migrate: &supersetv1alpha1.MigrateTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{
					Timeout: &metav1.Duration{Duration: 90 * time.Second},
				},
			}},
		}}
		assert.Equal(t, 90*time.Second, r.taskTimeoutValue(superset, taskTypeMigrate))
	})

	t.Run("defaults for unknown task type", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{}
		assert.Equal(t, defaultInitTimeout, r.taskTimeoutValue(superset, "Bogus"))
	})
}

func TestTaskRetentionPolicyValue(t *testing.T) {
	r := &SupersetReconciler{}

	t.Run("defaults when unset", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{},
		}}
		assert.Equal(t, defaultRetentionPolicy, r.taskRetentionPolicyValue(superset, taskTypeInit))
	})

	t.Run("uses lifecycle-level policy", func(t *testing.T) {
		policy := retentionDelete
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				PodRetention: &supersetv1alpha1.PodRetentionSpec{Policy: &policy},
			},
		}}
		assert.Equal(t, retentionDelete, r.taskRetentionPolicyValue(superset, taskTypeInit))
	})

	t.Run("clone-specific policy overrides lifecycle-level", func(t *testing.T) {
		lifecyclePolicy := retentionRetain
		clonePolicy := retentionDelete
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				PodRetention: &supersetv1alpha1.PodRetentionSpec{Policy: &lifecyclePolicy},
				Clone: &supersetv1alpha1.CloneTaskSpec{
					PodRetention: &supersetv1alpha1.PodRetentionSpec{Policy: &clonePolicy},
				},
			},
		}}
		assert.Equal(t, retentionDelete, r.taskRetentionPolicyValue(superset, taskTypeClone))
	})
}

func TestGetUpgradeMode(t *testing.T) {
	t.Run("defaults to Automatic when lifecycle nil", func(t *testing.T) {
		assert.Equal(t, upgradeModeAutomatic, getUpgradeMode(&supersetv1alpha1.Superset{}))
	})

	t.Run("defaults to Automatic when unset", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{},
		}}
		assert.Equal(t, upgradeModeAutomatic, getUpgradeMode(superset))
	})

	t.Run("returns Supervised when set", func(t *testing.T) {
		mode := upgradeModeSupervised
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{UpgradeMode: &mode},
		}}
		assert.Equal(t, upgradeModeSupervised, getUpgradeMode(superset))
	})
}

func TestSuffixForTaskType(t *testing.T) {
	tests := []struct {
		taskType string
		want     string
	}{
		{taskTypeClone, suffixClone},
		{taskTypeMigrate, suffixMigrate},
		{taskTypeRotate, suffixRotate},
		{taskTypeInit, suffixInit},
		{"Bogus", "-bogus"},
	}
	for _, tt := range tests {
		t.Run(tt.taskType, func(t *testing.T) {
			assert.Equal(t, tt.want, suffixForTaskType(tt.taskType))
		})
	}
}

func TestDefaultMigrateCommand(t *testing.T) {
	t.Run("default db upgrade", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{}
		got := defaultMigrateCommand(s)
		assert.Equal(t, []string{"/bin/sh", "-c", "superset db upgrade"}, got)
	})

	t.Run("user override wins", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{Migrate: &supersetv1alpha1.MigrateTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Command: []string{"superset", "db", "stamp", "head"}},
			}},
		}}
		got := defaultMigrateCommand(s)
		assert.Equal(t, []string{"superset", "db", "stamp", "head"}, got)
	})

	t.Run("bootstrap script is prepended to the default", func(t *testing.T) {
		script := "pip install foo"
		s := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			BootstrapScript: &script,
		}}
		got := defaultMigrateCommand(s)
		want := []string{"/bin/sh", "-c",
			". '" + common.ConfigMountPath + "/" + bootstrapScriptKey + "'; superset db upgrade"}
		assert.Equal(t, want, got)
	})
}

func TestDefaultInitCommand(t *testing.T) {
	t.Run("default superset init", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{}
		got := defaultInitCommand(s)
		assert.Equal(t, []string{"/bin/sh", "-c", "superset init"}, got)
	})

	t.Run("user override wins", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{Init: &supersetv1alpha1.InitTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Command: []string{"echo", "custom"}},
			}},
		}}
		got := defaultInitCommand(s)
		assert.Equal(t, []string{"echo", "custom"}, got)
	})

	t.Run("bootstrap script is prepended to the default", func(t *testing.T) {
		script := "pip install foo"
		s := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			BootstrapScript: &script,
		}}
		got := defaultInitCommand(s)
		want := []string{"/bin/sh", "-c",
			". '" + common.ConfigMountPath + "/" + bootstrapScriptKey + "'; superset init"}
		assert.Equal(t, want, got)
	})

	t.Run("admin user is appended before bootstrap wrapping", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{Init: &supersetv1alpha1.InitTaskSpec{
				AdminUser: &supersetv1alpha1.AdminUserSpec{},
			}},
		}}
		got := defaultInitCommand(s)
		// No bootstrap, so identical to buildInitCommand output.
		want := buildInitCommand(s.Spec.Lifecycle.Init)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("defaultInitCommand() = %#v, want %#v", got, want)
		}
	})
}
