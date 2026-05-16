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

// lifecycleCascadeStep captures one task's slot in the checksum cascade plus
// the per-task command and computed checksum. Callers (pipeline runner,
// "still-complete" check, "what's pending" check) walk the slice and apply
// their own logic.
type lifecycleCascadeStep struct {
	Desc         *lifecycleTaskDescriptor
	Command      []string
	TaskChecksum string
}

// computeStepChecksum hashes the incoming checksum + task type + command +
// per-task semantic inputs into a stable per-task checksum. The incoming
// checksum carries all upstream state, so re-execution of an upstream task
// automatically invalidates the downstream checksums by construction.
func (r *SupersetReconciler) computeStepChecksum(incomingChecksum, taskType string, command []string, extraInputs any) string {
	return computeChecksum(struct {
		IncomingChecksum string
		TaskType         string
		Command          []string
		ExtraInputs      any
	}{
		IncomingChecksum: incomingChecksum,
		TaskType:         taskType,
		Command:          command,
		ExtraInputs:      extraInputs,
	})
}

// walkLifecycleCascade computes the cascade steps for the enabled tasks.
// Tasks whose IsEnabled returns false are skipped and their absence still
// preserves the cascade chain for the remaining enabled tasks. The returned
// slice is in pipeline order.
func (r *SupersetReconciler) walkLifecycleCascade(superset *supersetv1alpha1.Superset, configChecksum string) []lifecycleCascadeStep {
	steps := make([]lifecycleCascadeStep, 0, len(lifecycleTaskDescriptors))
	incomingChecksum := string(superset.UID)
	for _, desc := range lifecycleTaskDescriptors {
		if !desc.IsEnabled(superset) {
			continue
		}
		command := desc.BuildCommand(r, superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, desc.TaskType, command, desc.BuildInputs(r, superset, configChecksum))
		steps = append(steps, lifecycleCascadeStep{
			Desc:         desc,
			Command:      command,
			TaskChecksum: taskChecksum,
		})
		incomingChecksum = taskChecksum
	}
	return steps
}

// allTasksStillComplete reports whether every enabled task already completed
// for the current cascade. Used as a fast path before drain/maintenance.
func (r *SupersetReconciler) allTasksStillComplete(superset *supersetv1alpha1.Superset, configChecksum string) bool {
	if superset.Status.Lifecycle == nil || superset.Status.Lifecycle.LastCompletedChecksums == nil {
		return false
	}
	checksums := superset.Status.Lifecycle.LastCompletedChecksums
	for _, step := range r.walkLifecycleCascade(superset, configChecksum) {
		if checksums[step.Desc.TaskType] != step.TaskChecksum {
			return false
		}
	}
	return true
}

// pendingLifecycleTasks returns the list of tasks that still need to run for
// the current cascade. Stops at the first task whose terminal failure matches
// the current checksum — downstream tasks would be blocked by it anyway.
func (r *SupersetReconciler) pendingLifecycleTasks(superset *supersetv1alpha1.Superset, configChecksum string) []string {
	steps := r.walkLifecycleCascade(superset, configChecksum)
	var pending []string
	for _, step := range steps {
		if r.taskNeedsRun(superset, step.Desc.TaskType, step.TaskChecksum) {
			pending = append(pending, step.Desc.TaskType)
		}
		if r.taskTerminalFailedForChecksum(superset, step.Desc.TaskType, step.TaskChecksum) {
			return pending
		}
	}
	return pending
}
