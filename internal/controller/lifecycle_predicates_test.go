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

	"github.com/stretchr/testify/assert"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

func TestTaskStatusForType(t *testing.T) {
	migrateRef := &supersetv1alpha1.TaskRefStatus{State: taskStateComplete}

	t.Run("nil when lifecycle status absent", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{}
		assert.Nil(t, taskStatusForType(superset, taskTypeMigrate))
	})

	t.Run("nil for unknown task type", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}},
		}
		assert.Nil(t, taskStatusForType(superset, "Bogus"))
	})

	t.Run("returns the addressed task ref", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{
				Migrate: migrateRef,
			}},
		}
		assert.Same(t, migrateRef, taskStatusForType(superset, taskTypeMigrate))
	})
}

func TestTaskTerminalFailedForChecksum(t *testing.T) {
	r := &SupersetReconciler{}
	const checksum = "sha256:abc"

	supersetWithMigrate := func(ref *supersetv1alpha1.TaskRefStatus) *supersetv1alpha1.Superset {
		return &supersetv1alpha1.Superset{
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{Migrate: ref}},
		}
	}

	t.Run("false when no task ref", func(t *testing.T) {
		assert.False(t, r.taskTerminalFailedForChecksum(supersetWithMigrate(nil), taskTypeMigrate, checksum))
	})

	t.Run("false when not failed", func(t *testing.T) {
		ref := &supersetv1alpha1.TaskRefStatus{State: taskStateComplete, CompletedChecksum: checksum}
		assert.False(t, r.taskTerminalFailedForChecksum(supersetWithMigrate(ref), taskTypeMigrate, checksum))
	})

	t.Run("false when failed for a different checksum", func(t *testing.T) {
		ref := &supersetv1alpha1.TaskRefStatus{State: taskStateFailed, CompletedChecksum: "other", Attempts: 5, MaxRetries: 3}
		assert.False(t, r.taskTerminalFailedForChecksum(supersetWithMigrate(ref), taskTypeMigrate, checksum))
	})

	t.Run("false when retries remain", func(t *testing.T) {
		ref := &supersetv1alpha1.TaskRefStatus{State: taskStateFailed, CompletedChecksum: checksum, Attempts: 2, MaxRetries: 3}
		assert.False(t, r.taskTerminalFailedForChecksum(supersetWithMigrate(ref), taskTypeMigrate, checksum))
	})

	t.Run("true when failed at max retries", func(t *testing.T) {
		ref := &supersetv1alpha1.TaskRefStatus{State: taskStateFailed, CompletedChecksum: checksum, Attempts: 3, MaxRetries: 3}
		assert.True(t, r.taskTerminalFailedForChecksum(supersetWithMigrate(ref), taskTypeMigrate, checksum))
	})

	t.Run("falls back to default max retries when ref MaxRetries is zero", func(t *testing.T) {
		// defaultMaxRetries is 3; Attempts at the default ceiling is terminal.
		ref := &supersetv1alpha1.TaskRefStatus{State: taskStateFailed, CompletedChecksum: checksum, Attempts: defaultMaxRetries}
		assert.True(t, r.taskTerminalFailedForChecksum(supersetWithMigrate(ref), taskTypeMigrate, checksum))
	})
}

func TestTaskNeedsRun(t *testing.T) {
	r := &SupersetReconciler{}
	const checksum = "sha256:abc"

	t.Run("true when no status at all", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{}
		assert.True(t, r.taskNeedsRun(superset, taskTypeMigrate, checksum))
	})

	t.Run("false when complete for the same checksum", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{
				Migrate: &supersetv1alpha1.TaskRefStatus{State: taskStateComplete, CompletedChecksum: checksum},
			}},
		}
		assert.False(t, r.taskNeedsRun(superset, taskTypeMigrate, checksum))
	})

	t.Run("true when complete for a different checksum", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{
				Migrate: &supersetv1alpha1.TaskRefStatus{State: taskStateComplete, CompletedChecksum: "stale"},
			}},
		}
		assert.True(t, r.taskNeedsRun(superset, taskTypeMigrate, checksum))
	})

	t.Run("false when terminally failed for the same checksum", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{
				Migrate: &supersetv1alpha1.TaskRefStatus{
					State: taskStateFailed, CompletedChecksum: checksum, Attempts: 3, MaxRetries: 3,
				},
			}},
		}
		assert.False(t, r.taskNeedsRun(superset, taskTypeMigrate, checksum))
	})

	t.Run("uses LastCompletedChecksums when no task ref", func(t *testing.T) {
		matched := &supersetv1alpha1.Superset{
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{
				LastCompletedChecksums: map[string]string{taskTypeMigrate: checksum},
			}},
		}
		assert.False(t, r.taskNeedsRun(matched, taskTypeMigrate, checksum))

		drifted := &supersetv1alpha1.Superset{
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{
				LastCompletedChecksums: map[string]string{taskTypeMigrate: "stale"},
			}},
		}
		assert.True(t, r.taskNeedsRun(drifted, taskTypeMigrate, checksum))
	})
}

func TestResolveLifecycleImage(t *testing.T) {
	parent := &supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "4.0.0"}
	newRepo := "internal/superset"
	newTag := "4.1.0"

	tests := []struct {
		name     string
		override *supersetv1alpha1.ImageOverrideSpec
		want     string
	}{
		{name: "nil override uses parent", override: nil, want: "apache/superset:4.0.0"},
		{name: "empty override uses parent", override: &supersetv1alpha1.ImageOverrideSpec{}, want: "apache/superset:4.0.0"},
		{name: "repository override", override: &supersetv1alpha1.ImageOverrideSpec{Repository: &newRepo}, want: "internal/superset:4.0.0"},
		{name: "tag override", override: &supersetv1alpha1.ImageOverrideSpec{Tag: &newTag}, want: "apache/superset:4.1.0"},
		{name: "both overridden", override: &supersetv1alpha1.ImageOverrideSpec{Repository: &newRepo, Tag: &newTag}, want: "internal/superset:4.1.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveLifecycleImage(parent, tt.override))
		})
	}
}
