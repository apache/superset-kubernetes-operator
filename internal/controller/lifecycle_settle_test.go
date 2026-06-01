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
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// TestReconcileLifecycle_DisabledAdvancesLastLifecycleImage covers finding #5:
// when lifecycle is disabled, LastLifecycleImage must advance so subsequent
// reconciles don't re-trigger the upgrade gate (image change → AwaitingApproval
// in Supervised mode) for work that will never actually run.
func TestReconcileLifecycle_DisabledAdvancesLastLifecycleImage(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.0.1"},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{Disabled: boolPtr(true)},
		},
		Status: supersetv1alpha1.SupersetStatus{LastLifecycleImage: "apache/superset:5.0.0"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	res, err := r.reconcileLifecycle(context.Background(), superset, "checksum", nil, "sa")
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if !res.Complete {
		t.Fatalf("expected lifecycle Complete=true, got %#v", res)
	}
	if superset.Status.LastLifecycleImage != "apache/superset:6.0.1" {
		t.Fatalf("expected LastLifecycleImage to advance to 6.0.1, got %q", superset.Status.LastLifecycleImage)
	}
}

// TestReconcileLifecycle_NoTasksConfigured covers findings #5 and #8:
// when every lifecycle task is absent or disabled, LastLifecycleImage must
// advance and the user-facing message must reflect the actual situation.
func TestReconcileLifecycle_NoTasksConfigured(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.0.1"},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Migrate: &supersetv1alpha1.MigrateTaskSpec{BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: boolPtr(true)}},
				Init:    &supersetv1alpha1.InitTaskSpec{BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: boolPtr(true)}},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{LastLifecycleImage: "apache/superset:5.0.0"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	res, err := r.reconcileLifecycle(context.Background(), superset, "checksum", nil, "sa")
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if !res.Complete {
		t.Fatalf("expected lifecycle Complete=true, got %#v", res)
	}
	if superset.Status.LastLifecycleImage != "apache/superset:6.0.1" {
		t.Fatalf("expected LastLifecycleImage to advance to 6.0.1, got %q", superset.Status.LastLifecycleImage)
	}
	if !hasConditionReason(superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete, "NoLifecycleTasks") {
		t.Fatalf("expected LifecycleComplete reason NoLifecycleTasks, got %#v", superset.Status.Conditions)
	}
	if !hasConditionMessage(superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete, "No lifecycle tasks configured") {
		t.Fatalf("expected user-facing message %q, got %#v", "No lifecycle tasks configured", superset.Status.Conditions)
	}
}

func TestCheckUpgradeGates_SupervisedApprovalRequiresRecordedToken(t *testing.T) {
	mode := upgradeModeSupervised
	lastImage := "apache/superset:1.0.0"
	currentImage := "apache/superset:1.1.0"
	token := upgradeApprovalToken(lastImage, currentImage)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test",
			Namespace:   "default",
			Annotations: map[string]string{annotationApproveUpgrade: token},
		},
		Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{UpgradeMode: &mode},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{},
		},
	}
	r := &SupersetReconciler{}

	_, gated := r.checkUpgradeGates(context.Background(), superset, true, lastImage, currentImage)
	if !gated {
		t.Fatal("expected first reconcile to publish the approval token before accepting it")
	}
	if superset.Status.Phase != phaseAwaitingApproval {
		t.Fatalf("expected parent phase AwaitingApproval, got %q", superset.Status.Phase)
	}
	if got := superset.Status.Lifecycle.Upgrade.ApprovalToken; got != token {
		t.Fatalf("expected approval token %q, got %q", token, got)
	}

	_, gated = r.checkUpgradeGates(context.Background(), superset, true, lastImage, currentImage)
	if gated {
		t.Fatal("expected matching recorded approval token to allow the upgrade")
	}
}

func TestCheckUpgradeGates_StaleApprovalDoesNotApproveChangedTarget(t *testing.T) {
	mode := upgradeModeSupervised
	lastImage := "apache/superset:1.0.0"
	oldTarget := "apache/superset:1.1.0"
	newTarget := "apache/superset:1.2.0"
	oldToken := upgradeApprovalToken(lastImage, oldTarget)
	newToken := upgradeApprovalToken(lastImage, newTarget)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test",
			Namespace:   "default",
			Annotations: map[string]string{annotationApproveUpgrade: oldToken},
		},
		Spec: supersetv1alpha1.SupersetSpec{
			Lifecycle: &supersetv1alpha1.LifecycleSpec{UpgradeMode: &mode},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{
				Upgrade: &supersetv1alpha1.UpgradeContext{
					FromVersion:   "1.0.0",
					ToVersion:     "1.1.0",
					Direction:     string(DirectionUpgrade),
					ApprovalToken: oldToken,
				},
			},
		},
	}
	r := &SupersetReconciler{}

	_, gated := r.checkUpgradeGates(context.Background(), superset, true, lastImage, newTarget)
	if !gated {
		t.Fatal("expected stale approval token to keep the changed target gated")
	}
	if got := superset.Status.Lifecycle.Upgrade.ToVersion; got != "1.2.0" {
		t.Fatalf("expected upgrade context to move to 1.2.0, got %q", got)
	}
	if got := superset.Status.Lifecycle.Upgrade.ApprovalToken; got != newToken {
		t.Fatalf("expected new approval token %q, got %q", newToken, got)
	}
}

// TestClearUpgradeApprovalAnnotation_RemovesAnnotation covers finding #6:
// the annotation-clearing helper must actually remove the annotation when
// invoked. This guards the "happy path" piece of the bug fix.
func TestClearUpgradeApprovalAnnotation_RemovesAnnotation(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test",
			Namespace:   "default",
			UID:         "uid-1",
			Annotations: map[string]string{annotationApproveUpgrade: "true", "other": "preserved"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	if err := r.clearUpgradeApprovalAnnotation(context.Background(), superset); err != nil {
		t.Fatalf("clearUpgradeApprovalAnnotation: %v", err)
	}
	if _, ok := superset.GetAnnotations()[annotationApproveUpgrade]; ok {
		t.Fatalf("expected annotation to be removed in-memory, still present: %#v", superset.GetAnnotations())
	}

	persisted := &supersetv1alpha1.Superset{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, persisted); err != nil {
		t.Fatalf("get persisted Superset: %v", err)
	}
	if _, ok := persisted.GetAnnotations()[annotationApproveUpgrade]; ok {
		t.Fatalf("expected annotation to be removed in API, still present: %#v", persisted.GetAnnotations())
	}
	if persisted.GetAnnotations()["other"] != "preserved" {
		t.Fatalf("expected other annotations to be preserved, got %#v", persisted.GetAnnotations())
	}
}

// TestClearUpgradeApprovalAnnotation_NoOpWhenAbsent covers finding #6:
// the helper must be safe to call when the annotation is absent (the common
// case once lifecycle has settled). It must not error or mutate annotations.
func TestClearUpgradeApprovalAnnotation_NoOpWhenAbsent(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	if err := r.clearUpgradeApprovalAnnotation(context.Background(), superset); err != nil {
		t.Fatalf("clearUpgradeApprovalAnnotation on absent annotation should be no-op, got %v", err)
	}
}

// TestFinalizeLifecycleDoesNotPatchAnnotation covers finding #6: the bug was
// that finalizeLifecycle issued a separate annotation Patch *before* status
// was persisted, so a status patch failure would strand the annotation
// (re-gating Supervised upgrades while LastLifecycleImage was stale). Annotation
// clearing has moved to the parent reconciler post-status-persist. This test
// pins finalizeLifecycle to settling status only.
func TestFinalizeLifecycleDoesNotPatchAnnotation(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test",
			Namespace:   "default",
			UID:         "uid-1",
			Annotations: map[string]string{annotationApproveUpgrade: "true"},
		},
		Spec: supersetv1alpha1.SupersetSpec{
			Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.0.1"},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{Phase: lifecyclePhaseInitializing},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	r.finalizeLifecycle(superset, "apache/superset:6.0.1")

	if superset.Status.LastLifecycleImage != "apache/superset:6.0.1" {
		t.Fatalf("expected LastLifecycleImage to advance, got %q", superset.Status.LastLifecycleImage)
	}
	persisted := &supersetv1alpha1.Superset{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, persisted); err != nil {
		t.Fatalf("get persisted: %v", err)
	}
	if _, ok := persisted.GetAnnotations()[annotationApproveUpgrade]; !ok {
		t.Fatalf("finalizeLifecycle must not touch the annotation; the parent reconciler clears it after status persist. Annotation was removed.")
	}
}

func hasConditionMessage(conditions []metav1.Condition, conditionType, message string) bool {
	for _, c := range conditions {
		if c.Type == conditionType && c.Message == message {
			return true
		}
	}
	return false
}

// TestReconcileLifecycle_BlocksOnInvalidCloneSchedule pins the gate that
// prevents downstream tasks from running when clone is configured with a cron
// schedule that passes structural CRD validation but fails runtime parsing
// (e.g. out-of-range values). Without the gate, IsEnabled would treat clone
// as disabled and migrate/init would silently run against the wrong dataset.
func TestReconcileLifecycle_BlocksOnInvalidCloneSchedule(t *testing.T) {
	scheme := testScheme(t)
	devMode := "Development"
	invalidSchedule := "99 99 99 99 99"
	metastoreHost := "postgres.default.svc"
	metastoreDB := "superset"
	metastoreUser := "superset"

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Environment: &devMode,
			Image:       supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.0.1"},
			SecretKeyFrom: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "app-secret"},
				Key:                  "secret-key",
			},
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Host:     &metastoreHost,
				Database: &metastoreDB,
				Username: &metastoreUser,
				PasswordFrom: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "db-secret"},
					Key:                  "password",
				},
			},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
						CronSchedule: &invalidSchedule,
					},
					Source: supersetv1alpha1.CloneSourceSpec{
						Host:     "pg-prod.svc",
						Database: "superset_prod",
						Username: "reader",
						PasswordFrom: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "src-secret"},
							Key:                  "password",
						},
					},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	res, err := r.reconcileLifecycle(context.Background(), superset, "checksum", nil, "sa")
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if !res.TerminalFailure {
		t.Fatalf("expected terminal failure for invalid clone schedule, got %#v", res)
	}
	if res.Complete {
		t.Fatal("expected lifecycle blocked, not complete")
	}
	if !hasConditionReason(superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete, "InvalidCronSchedule") {
		t.Fatalf("expected LifecycleComplete=False reason InvalidCronSchedule, got %#v", superset.Status.Conditions)
	}
	if superset.Status.Lifecycle.Phase != lifecyclePhaseBlocked {
		t.Fatalf("expected lifecycle phase Blocked, got %q", superset.Status.Lifecycle.Phase)
	}
	if superset.Status.Phase != phaseBlocked {
		t.Fatalf("expected parent phase Blocked, got %q", superset.Status.Phase)
	}
	jobs := &batchv1.JobList{}
	if err := c.List(context.Background(), jobs); err != nil {
		t.Fatalf("listing jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		names := make([]string, 0, len(jobs.Items))
		for _, j := range jobs.Items {
			names = append(names, j.Name)
		}
		t.Fatalf("expected no task Jobs to be created when clone schedule is invalid, got %v", names)
	}
}
