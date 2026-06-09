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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/schedule"
)

func (r *SupersetReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *SupersetReconciler) scheduleTick(cronSchedule *string) string {
	if cronSchedule == nil || *cronSchedule == "" {
		return ""
	}
	return schedule.CurrentTick(*cronSchedule, r.now())
}

func (r *SupersetReconciler) nextScheduleRequeue(superset *supersetv1alpha1.Superset) time.Duration {
	if superset.Spec.Lifecycle == nil {
		return 0
	}
	var earliest time.Time
	for _, expr := range r.activeSchedules(superset) {
		next := schedule.NextTick(expr, r.now())
		if next.IsZero() {
			continue
		}
		if earliest.IsZero() || next.Before(earliest) {
			earliest = next
		}
	}
	if earliest.IsZero() {
		return 0
	}
	d := earliest.Sub(r.now()) + time.Second
	if d < time.Second {
		return time.Second
	}
	return d
}

func (r *SupersetReconciler) activeSchedules(superset *supersetv1alpha1.Superset) []string {
	lc := superset.Spec.Lifecycle
	var out []string
	if lc.Seed != nil && lc.Seed.CronSchedule != nil && !isDisabled(lc.Seed.Disabled) {
		out = append(out, *lc.Seed.CronSchedule)
	}
	return out
}

func isDisabled(disabled *bool) bool {
	return disabled != nil && *disabled
}

func (r *SupersetReconciler) projectScheduleStatus(superset *supersetv1alpha1.Superset, taskType string, taskRef *supersetv1alpha1.TaskRefStatus) {
	if superset.Spec.Lifecycle == nil {
		return
	}
	var cronSchedule *string
	switch taskType {
	case taskTypeSeed:
		if superset.Spec.Lifecycle.Seed != nil {
			cronSchedule = superset.Spec.Lifecycle.Seed.CronSchedule
		}
	}
	if cronSchedule == nil || *cronSchedule == "" {
		return
	}
	now := r.now()
	if tick := schedule.CurrentTick(*cronSchedule, now); tick != "" {
		parsed, err := time.Parse(time.RFC3339, tick)
		if err == nil {
			t := metav1.NewTime(parsed)
			taskRef.LastScheduledAt = &t
		}
	}
	if next := schedule.NextTick(*cronSchedule, now); !next.IsZero() {
		t := metav1.NewTime(next)
		taskRef.NextScheduleAt = &t
	}
}

// validateSchedules checks all active cron expressions for validity and sets
// a warning condition + event if any are invalid. When no schedules are
// configured, the ScheduleValid condition is removed entirely so users don't
// see a misleading "valid" signal for absent schedules.
func (r *SupersetReconciler) validateSchedules(superset *supersetv1alpha1.Superset) {
	if superset.Spec.Lifecycle == nil {
		removeCondition(&superset.Status.Conditions, conditionTypeScheduleValid)
		return
	}
	hasSchedule := false
	if superset.Spec.Lifecycle.Seed != nil && superset.Spec.Lifecycle.Seed.CronSchedule != nil &&
		*superset.Spec.Lifecycle.Seed.CronSchedule != "" &&
		!isDisabled(superset.Spec.Lifecycle.Seed.Disabled) {
		hasSchedule = true
		expr := *superset.Spec.Lifecycle.Seed.CronSchedule
		if err := schedule.Validate(expr); err != nil {
			setCondition(&superset.Status.Conditions, conditionTypeScheduleValid,
				metav1.ConditionFalse, "InvalidCronSchedule", err.Error(), superset.Generation)
			r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "InvalidCronSchedule", "Lifecycle",
				"Seed cron schedule is invalid: %v", err)
			return
		}
	}
	if !hasSchedule {
		removeCondition(&superset.Status.Conditions, conditionTypeScheduleValid)
		return
	}
	setCondition(&superset.Status.Conditions, conditionTypeScheduleValid,
		metav1.ConditionTrue, "SchedulesValid", "All cron schedules are valid", superset.Generation)
}

const conditionTypeScheduleValid = "ScheduleValid"

// seedScheduleIsValid returns true if cronSchedule is unset, empty, or a
// valid cron expression. An invalid expression causes the caller to treat
// the seed as disabled until the user corrects it. The matching user-facing
// signal (condition + event) is set by validateSchedules earlier in the
// reconcile.
func seedScheduleIsValid(cronSchedule *string) bool {
	if cronSchedule == nil || *cronSchedule == "" {
		return true
	}
	return schedule.Validate(*cronSchedule) == nil
}
