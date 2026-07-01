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
	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
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
}

// lifecycleTaskDescriptors is the source of truth for task ordering and
// per-task wiring. Order is significant: seed → migrate → rotate → init is
// the cascade direction.
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
	},
	{
		TaskType:        taskTypeMigrate,
		Suffix:          suffixMigrate,
		Phase:           lifecyclePhaseMigrating,
		DrainsByDefault: true,
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

// isTaskEnabled is a small convenience wrapper that delegates to the
// descriptor table. Returns false for unknown task types.
func (r *SupersetReconciler) isTaskEnabled(superset *supersetv1alpha1.Superset, taskType string) bool {
	desc := lifecycleTaskDescriptorByType(taskType)
	if desc == nil {
		return false
	}
	return desc.IsEnabled(superset)
}
