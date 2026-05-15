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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// TestValidateSchedules_NoConditionWhenNoSchedules covers finding #9: the
// ScheduleValid condition was being set unconditionally on every reconcile,
// even when no schedule was configured. That polluted status with a
// "valid" signal for an empty set of schedules.
func TestValidateSchedules_NoConditionWhenNoSchedules(t *testing.T) {
	r := &SupersetReconciler{Recorder: events.NewFakeRecorder(10)}

	// Case 1: lifecycle is nil entirely.
	superset := &supersetv1alpha1.Superset{}
	r.validateSchedules(superset)
	if hasScheduleValidCondition(superset.Status.Conditions) {
		t.Fatalf("expected no ScheduleValid condition when lifecycle is nil, got %#v", superset.Status.Conditions)
	}

	// Case 2: lifecycle present but no clone (the only scheduled task).
	superset = &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{},
		},
	}
	r.validateSchedules(superset)
	if hasScheduleValidCondition(superset.Status.Conditions) {
		t.Fatalf("expected no ScheduleValid condition when no clone task, got %#v", superset.Status.Conditions)
	}

	// Case 3: clone present but no cron schedule set.
	superset = &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{},
			},
		},
	}
	r.validateSchedules(superset)
	if hasScheduleValidCondition(superset.Status.Conditions) {
		t.Fatalf("expected no ScheduleValid condition when clone has no schedule, got %#v", superset.Status.Conditions)
	}
}

// TestValidateSchedules_RemovesStaleCondition covers finding #9: when a
// previously-set schedule is removed, the leftover ScheduleValid condition
// must be cleared (otherwise users see a stale signal forever).
func TestValidateSchedules_RemovesStaleCondition(t *testing.T) {
	r := &SupersetReconciler{Recorder: events.NewFakeRecorder(10)}
	superset := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Conditions: []metav1.Condition{{
				Type:    conditionTypeScheduleValid,
				Status:  metav1.ConditionTrue,
				Reason:  "SchedulesValid",
				Message: "All cron schedules are valid",
			}},
		},
	}
	r.validateSchedules(superset)
	if hasScheduleValidCondition(superset.Status.Conditions) {
		t.Fatalf("expected stale ScheduleValid condition to be removed when no schedules configured, got %#v", superset.Status.Conditions)
	}
}

// TestValidateSchedules_SetsTrueWhenScheduleValid covers the happy path:
// when at least one valid schedule is configured, the condition appears with
// reason SchedulesValid.
func TestValidateSchedules_SetsTrueWhenScheduleValid(t *testing.T) {
	r := &SupersetReconciler{Recorder: events.NewFakeRecorder(10)}
	schedule := "0 2 * * *"
	superset := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &schedule},
				},
			},
		},
	}
	r.validateSchedules(superset)
	if !hasConditionReason(superset.Status.Conditions, conditionTypeScheduleValid, "SchedulesValid") {
		t.Fatalf("expected ScheduleValid=True with reason SchedulesValid, got %#v", superset.Status.Conditions)
	}
}

func hasScheduleValidCondition(conditions []metav1.Condition) bool {
	for _, c := range conditions {
		if c.Type == conditionTypeScheduleValid {
			return true
		}
	}
	return false
}
