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
	"time"

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

// fixedNow returns a deterministic clock for schedule tests.
func fixedNow(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestNextScheduleRequeue(t *testing.T) {
	// A daily 02:00 cron, evaluated at 03:00, next tick is the following day's
	// 02:00 — i.e. 23h ahead. The requeue adds a one-second guard.
	base := time.Date(2026, 6, 2, 3, 0, 0, 0, time.UTC)

	t.Run("nil lifecycle yields zero", func(t *testing.T) {
		r := &SupersetReconciler{Now: fixedNow(base)}
		superset := &supersetv1alpha1.Superset{}
		if d := r.nextScheduleRequeue(superset); d != 0 {
			t.Fatalf("expected 0 for nil lifecycle, got %s", d)
		}
	})

	t.Run("no active schedule yields zero", func(t *testing.T) {
		r := &SupersetReconciler{Now: fixedNow(base)}
		superset := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{},
			}},
		}
		if d := r.nextScheduleRequeue(superset); d != 0 {
			t.Fatalf("expected 0 with no cron schedule, got %s", d)
		}
	})

	t.Run("clone cron schedule produces a positive requeue", func(t *testing.T) {
		r := &SupersetReconciler{Now: fixedNow(base)}
		sched := "0 2 * * *"
		superset := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &sched},
				},
			}},
		}
		d := r.nextScheduleRequeue(superset)
		// next 02:00 is 23h after 03:00, plus one second guard.
		want := 23*time.Hour + time.Second
		if d != want {
			t.Fatalf("expected requeue %s, got %s", want, d)
		}
	})

	t.Run("disabled clone yields zero", func(t *testing.T) {
		r := &SupersetReconciler{Now: fixedNow(base)}
		sched := "0 2 * * *"
		superset := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
						BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: boolPtr(true)},
						CronSchedule: &sched,
					},
				},
			}},
		}
		if d := r.nextScheduleRequeue(superset); d != 0 {
			t.Fatalf("expected 0 for disabled clone, got %s", d)
		}
	})

	t.Run("sub-second remaining clamps to one second", func(t *testing.T) {
		// Evaluate at 01:59:59.9 so the next 02:00 tick is 0.1s away; the
		// computed duration (next - now + 1s) would still exceed a second, so
		// to exercise the clamp we instead evaluate just past a minute-resolution
		// tick boundary. The clamp guards against a next-tick that is effectively
		// "now": with a per-minute schedule evaluated exactly on the tick, the
		// next tick is ~1 minute out. We assert the floor never drops below 1s.
		now := time.Date(2026, 6, 2, 1, 59, 59, 0, time.UTC)
		r := &SupersetReconciler{Now: fixedNow(now)}
		sched := "* * * * *" // every minute
		superset := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &sched},
				},
			}},
		}
		d := r.nextScheduleRequeue(superset)
		if d < time.Second {
			t.Fatalf("requeue must never be below 1s, got %s", d)
		}
	})
}

func TestProjectScheduleStatus(t *testing.T) {
	base := time.Date(2026, 6, 2, 3, 0, 0, 0, time.UTC)

	t.Run("nil lifecycle is a no-op", func(t *testing.T) {
		r := &SupersetReconciler{Now: fixedNow(base)}
		superset := &supersetv1alpha1.Superset{}
		ref := &supersetv1alpha1.TaskRefStatus{}
		r.projectScheduleStatus(superset, taskTypeClone, ref)
		if ref.LastScheduledAt != nil || ref.NextScheduleAt != nil {
			t.Fatalf("expected no projection for nil lifecycle, got %+v", ref)
		}
	})

	t.Run("clone with no schedule is a no-op", func(t *testing.T) {
		r := &SupersetReconciler{Now: fixedNow(base)}
		superset := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{},
			}},
		}
		ref := &supersetv1alpha1.TaskRefStatus{}
		r.projectScheduleStatus(superset, taskTypeClone, ref)
		if ref.LastScheduledAt != nil || ref.NextScheduleAt != nil {
			t.Fatalf("expected no projection without a cron schedule, got %+v", ref)
		}
	})

	t.Run("non-clone task type is a no-op", func(t *testing.T) {
		r := &SupersetReconciler{Now: fixedNow(base)}
		sched := "0 2 * * *"
		superset := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &sched},
				},
			}},
		}
		ref := &supersetv1alpha1.TaskRefStatus{}
		r.projectScheduleStatus(superset, taskTypeMigrate, ref)
		if ref.LastScheduledAt != nil || ref.NextScheduleAt != nil {
			t.Fatalf("expected no projection for non-clone task, got %+v", ref)
		}
	})

	t.Run("clone with cron projects last and next", func(t *testing.T) {
		r := &SupersetReconciler{Now: fixedNow(base)}
		sched := "0 2 * * *"
		superset := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &sched},
				},
			}},
		}
		ref := &supersetv1alpha1.TaskRefStatus{}
		r.projectScheduleStatus(superset, taskTypeClone, ref)

		if ref.LastScheduledAt == nil {
			t.Fatal("expected LastScheduledAt to be set")
		}
		// Last tick at or before 03:00 is today's 02:00.
		wantLast := time.Date(2026, 6, 2, 2, 0, 0, 0, time.UTC)
		if !ref.LastScheduledAt.Time.Equal(wantLast) {
			t.Errorf("LastScheduledAt = %s, want %s", ref.LastScheduledAt.Time, wantLast)
		}
		if ref.NextScheduleAt == nil {
			t.Fatal("expected NextScheduleAt to be set")
		}
		wantNext := time.Date(2026, 6, 3, 2, 0, 0, 0, time.UTC)
		if !ref.NextScheduleAt.Time.Equal(wantNext) {
			t.Errorf("NextScheduleAt = %s, want %s", ref.NextScheduleAt.Time, wantNext)
		}
	})
}
