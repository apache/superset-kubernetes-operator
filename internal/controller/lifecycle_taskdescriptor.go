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
	"time"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// lifecycleTaskDescriptor centralizes the per-task knobs that the pipeline,
// cascade walker, and prune/cleanup paths all need. Adding a new lifecycle
// task means appending a descriptor here and providing the per-task helpers it
// references — no callers need additional switch arms.
type lifecycleTaskDescriptor struct {
	TaskType        string
	Suffix          string
	Phase           string
	DrainsByDefault bool

	// OutOfCascade excludes the task from the checksum cascade. The backup
	// task uses it: it runs as a guard in front of the first pending
	// GuardedByBackup task instead of as a link in the chain, so changing the
	// backup spec never invalidates downstream task checksums.
	OutOfCascade bool

	// GuardedByBackup marks data-mutating tasks that the backup task (when
	// enabled) snapshots the metastore in front of.
	GuardedByBackup bool

	// DefaultTimeout is the per-attempt timeout used when the task spec does
	// not set one. Zero means defaultInitTimeout.
	DefaultTimeout time.Duration

	// BuildCommand returns the task command (respecting user override).
	BuildCommand func(*SupersetReconciler, *supersetv1alpha1.Superset) []string

	// BuildInputs returns the task-specific semantic inputs hashed into the
	// task checksum. configChecksum is only consumed by tasks that depend on
	// rendered Python config (init).
	BuildInputs func(*SupersetReconciler, *supersetv1alpha1.Superset, string) any

	// IsEnabled determines whether the task participates in the pipeline given
	// the parent spec. Defaults vary per task: seed/rotate require explicit
	// spec; migrate/init are enabled by default.
	IsEnabled func(*supersetv1alpha1.Superset) bool

	// BaseSpec returns the BaseTaskSpec for this task (nil-safe). Callers use
	// it to read the shared fields (Disabled, MaxRetries, Timeout,
	// RequiresDrain, Trigger, Command).
	BaseSpec func(*supersetv1alpha1.Superset) *supersetv1alpha1.BaseTaskSpec

	// TaskRef returns the addressable TaskRefStatus pointer slot in
	// LifecycleStatus so callers can both read and clear it.
	TaskRef func(*supersetv1alpha1.LifecycleStatus) **supersetv1alpha1.TaskRefStatus

	// BuildToolFlatSpec, when set, marks the task as running a database-tool
	// image instead of the Superset image. It replaces the standard flat spec
	// builder, and the task gets no rendered superset_config.py, no task
	// ConfigMap, and no lifecycle bootstrap wrapping. Nil means the task runs
	// the Superset image with the rendered config mounted.
	BuildToolFlatSpec func(*SupersetReconciler, *supersetv1alpha1.Superset, string, *resolution.SharedInput) supersetv1alpha1.FlatComponentSpec

	// PodRetention returns a task-specific retention override (nil-safe).
	// Nil (or a nil return) falls back to spec.lifecycle.podRetention.
	PodRetention func(*supersetv1alpha1.Superset) *supersetv1alpha1.PodRetentionSpec
}

// usesSupersetConfig reports whether the task runs the Superset image with the
// rendered superset_config.py and lifecycle bootstrap script.
func (d *lifecycleTaskDescriptor) usesSupersetConfig() bool {
	return d.BuildToolFlatSpec == nil
}

// lifecycleTaskDescriptors is the source of truth for task ordering and
// per-task wiring. Order is significant: seed → migrate → rotate → init is
// the cascade direction. Backup is listed for shared wiring (status slot,
// prune, retention) but is OutOfCascade; the pipeline runs it in front of the
// first pending GuardedByBackup task.
var lifecycleTaskDescriptors = []*lifecycleTaskDescriptor{
	{
		TaskType:        taskTypeSeed,
		Suffix:          suffixSeed,
		Phase:           lifecyclePhaseSeeding,
		DrainsByDefault: true,
		BuildCommand: func(r *SupersetReconciler, s *supersetv1alpha1.Superset) []string {
			return r.buildSeedCommand(s)
		},
		BuildInputs: func(r *SupersetReconciler, s *supersetv1alpha1.Superset, _ string) any {
			return r.seedInputs(s)
		},
		IsEnabled: func(s *supersetv1alpha1.Superset) bool {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Seed == nil {
				return false
			}
			if isDisabled(s.Spec.Lifecycle.Seed.Disabled) {
				return false
			}
			return seedScheduleIsValid(s.Spec.Lifecycle.Seed.CronSchedule)
		},
		BaseSpec: func(s *supersetv1alpha1.Superset) *supersetv1alpha1.BaseTaskSpec {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Seed == nil {
				return nil
			}
			return &s.Spec.Lifecycle.Seed.BaseTaskSpec
		},
		TaskRef: func(ls *supersetv1alpha1.LifecycleStatus) **supersetv1alpha1.TaskRefStatus {
			return &ls.Seed
		},
		BuildToolFlatSpec: func(r *SupersetReconciler, s *supersetv1alpha1.Superset, saName string, topLevel *resolution.SharedInput) supersetv1alpha1.FlatComponentSpec {
			return r.buildSeedTaskFlatSpec(s, saName, topLevel)
		},
		PodRetention: func(s *supersetv1alpha1.Superset) *supersetv1alpha1.PodRetentionSpec {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Seed == nil {
				return nil
			}
			return s.Spec.Lifecycle.Seed.PodRetention
		},
	},
	{
		TaskType:        taskTypeBackup,
		Suffix:          suffixBackup,
		Phase:           lifecyclePhaseBackingUp,
		DrainsByDefault: true,
		OutOfCascade:    true,
		DefaultTimeout:  defaultBackupTimeout,
		BuildCommand: func(_ *SupersetReconciler, s *supersetv1alpha1.Superset) []string {
			return buildBackupCommand(s)
		},
		BuildInputs: func(_ *SupersetReconciler, _ *supersetv1alpha1.Superset, _ string) any {
			return nil // out of cascade; see backupTaskChecksum
		},
		IsEnabled: func(s *supersetv1alpha1.Superset) bool {
			return s.Spec.Lifecycle != nil && s.Spec.Lifecycle.Backup != nil && !isDisabled(s.Spec.Lifecycle.Backup.Disabled)
		},
		BaseSpec: func(s *supersetv1alpha1.Superset) *supersetv1alpha1.BaseTaskSpec {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Backup == nil {
				return nil
			}
			return &s.Spec.Lifecycle.Backup.BaseTaskSpec
		},
		TaskRef: func(ls *supersetv1alpha1.LifecycleStatus) **supersetv1alpha1.TaskRefStatus {
			return &ls.Backup
		},
		BuildToolFlatSpec: func(r *SupersetReconciler, s *supersetv1alpha1.Superset, saName string, topLevel *resolution.SharedInput) supersetv1alpha1.FlatComponentSpec {
			return r.buildBackupTaskFlatSpec(s, saName, topLevel)
		},
		PodRetention: func(s *supersetv1alpha1.Superset) *supersetv1alpha1.PodRetentionSpec {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Backup == nil {
				return nil
			}
			return s.Spec.Lifecycle.Backup.PodRetention
		},
	},
	{
		TaskType:        taskTypeMigrate,
		Suffix:          suffixMigrate,
		Phase:           lifecyclePhaseMigrating,
		DrainsByDefault: true,
		GuardedByBackup: true,
		BuildCommand: func(_ *SupersetReconciler, s *supersetv1alpha1.Superset) []string {
			return defaultMigrateCommand(s)
		},
		BuildInputs: func(r *SupersetReconciler, s *supersetv1alpha1.Superset, _ string) any {
			return r.migrateInputs(s)
		},
		IsEnabled: func(s *supersetv1alpha1.Superset) bool {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Migrate == nil {
				return true // default-enabled (matches prior isTaskEnabled behavior)
			}
			return !isDisabled(s.Spec.Lifecycle.Migrate.Disabled)
		},
		BaseSpec: func(s *supersetv1alpha1.Superset) *supersetv1alpha1.BaseTaskSpec {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Migrate == nil {
				return nil
			}
			return &s.Spec.Lifecycle.Migrate.BaseTaskSpec
		},
		TaskRef: func(ls *supersetv1alpha1.LifecycleStatus) **supersetv1alpha1.TaskRefStatus {
			return &ls.Migrate
		},
	},
	{
		TaskType:        taskTypeRotate,
		Suffix:          suffixRotate,
		Phase:           lifecyclePhaseRotating,
		DrainsByDefault: true,
		GuardedByBackup: true,
		BuildCommand: func(_ *SupersetReconciler, s *supersetv1alpha1.Superset) []string {
			return defaultRotateCommand(s)
		},
		BuildInputs: func(r *SupersetReconciler, s *supersetv1alpha1.Superset, _ string) any {
			return r.rotateInputs(s)
		},
		IsEnabled: func(s *supersetv1alpha1.Superset) bool {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Rotate == nil {
				return false
			}
			return !isDisabled(s.Spec.Lifecycle.Rotate.Disabled)
		},
		BaseSpec: func(s *supersetv1alpha1.Superset) *supersetv1alpha1.BaseTaskSpec {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Rotate == nil {
				return nil
			}
			return &s.Spec.Lifecycle.Rotate.BaseTaskSpec
		},
		TaskRef: func(ls *supersetv1alpha1.LifecycleStatus) **supersetv1alpha1.TaskRefStatus {
			return &ls.Rotate
		},
	},
	{
		TaskType:        taskTypeInit,
		Suffix:          suffixInit,
		Phase:           lifecyclePhaseInitializing,
		DrainsByDefault: false,
		BuildCommand: func(_ *SupersetReconciler, s *supersetv1alpha1.Superset) []string {
			return defaultInitCommand(s)
		},
		BuildInputs: func(r *SupersetReconciler, s *supersetv1alpha1.Superset, _ string) any {
			return r.initInputs(s)
		},
		IsEnabled: func(s *supersetv1alpha1.Superset) bool {
			if s.Spec.Lifecycle == nil {
				return true // matches prior default-true behavior
			}
			if s.Spec.Lifecycle.Init == nil {
				return true
			}
			return !isDisabled(s.Spec.Lifecycle.Init.Disabled)
		},
		BaseSpec: func(s *supersetv1alpha1.Superset) *supersetv1alpha1.BaseTaskSpec {
			if s.Spec.Lifecycle == nil || s.Spec.Lifecycle.Init == nil {
				return nil
			}
			return &s.Spec.Lifecycle.Init.BaseTaskSpec
		},
		TaskRef: func(ls *supersetv1alpha1.LifecycleStatus) **supersetv1alpha1.TaskRefStatus {
			return &ls.Init
		},
	},
}

// lifecycleTaskDescriptorByType returns the descriptor for a given task type,
// or nil if no descriptor matches (callers should treat nil as "unknown task").
func lifecycleTaskDescriptorByType(taskType string) *lifecycleTaskDescriptor {
	for _, d := range lifecycleTaskDescriptors {
		if d.TaskType == taskType {
			return d
		}
	}
	return nil
}

// taskUsesSupersetConfig reports whether a task type runs the Superset image
// with rendered config. Unknown task types are treated as Superset-image tasks.
func taskUsesSupersetConfig(taskType string) bool {
	desc := lifecycleTaskDescriptorByType(taskType)
	return desc == nil || desc.usesSupersetConfig()
}

// isTaskEnabled is a small convenience wrapper that delegates to the
// descriptor table. Returns false for unknown task types.
func (r *SupersetReconciler) isTaskEnabled(superset *supersetv1alpha1.Superset, taskType string) bool {
	desc := lifecycleTaskDescriptorByType(taskType)
	if desc == nil {
		return false
	}
	return desc.IsEnabled(superset)
}
