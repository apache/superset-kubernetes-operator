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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// TestLifecyclePipeline_FullSuccess walks the full seed → migrate → rotate →
// init pipeline through fake Job status transitions and asserts the lifecycle
// reaches Phase=Complete with no terminal failure. This locks in the cascade
// behavior end-to-end so a refactor that breaks task sequencing or status
// propagation will fail this test.
func TestLifecyclePipeline_FullSuccess(t *testing.T) {
	scheme := testScheme(t)
	devMode := "Development"
	previousSecretKey := "old-key"
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
			PreviousSecretKey: &previousSecretKey,
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
				Seed: &supersetv1alpha1.SeedTaskSpec{
					Source: supersetv1alpha1.SeedSourceSpec{
						Host:     "pg-prod.svc",
						Database: "superset_prod",
						Username: "reader",
						PasswordFrom: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "src-secret"},
							Key:                  "password",
						},
					},
				},
				Migrate: &supersetv1alpha1.MigrateTaskSpec{},
				Rotate:  &supersetv1alpha1.RotateTaskSpec{},
				Init:    &supersetv1alpha1.InitTaskSpec{},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset).
		WithStatusSubresource(&supersetv1alpha1.Superset{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(50)}

	expected := []string{taskTypeSeed, taskTypeMigrate, taskTypeRotate, taskTypeInit}
	for _, taskType := range expected {
		// Drive the pipeline until the next task's Job exists, then mark it
		// succeeded. Bound the loop to defend against an accidental infinite
		// reconcile in case of a regression.
		var advanced bool
		for range 8 {
			res, err := r.reconcileLifecycle(context.Background(), superset, "config-checksum", nil, "sa")
			if err != nil {
				t.Fatalf("reconcileLifecycle (%s): %v", taskType, err)
			}
			if res.TerminalFailure {
				t.Fatalf("unexpected terminal failure during %s: %#v", taskType, superset.Status)
			}

			job, err := getTaskJob(t, c, superset.Namespace, taskJobName(superset.Name, taskType))
			if err != nil {
				t.Fatalf("get %s job: %v", taskType, err)
			}
			if job == nil {
				continue
			}
			if jobComplete(job) {
				advanced = true
				break
			}
			markJobSucceeded(t, c, job)
		}
		if !advanced {
			t.Fatalf("pipeline did not advance past %s task within iteration budget; status=%#v", taskType, superset.Status)
		}
	}

	res, err := r.reconcileLifecycle(context.Background(), superset, "config-checksum", nil, "sa")
	if err != nil {
		t.Fatalf("final reconcileLifecycle: %v", err)
	}
	if !res.Complete {
		t.Fatalf("expected lifecycle Complete=true after all tasks, got %#v (status=%#v)", res, superset.Status)
	}
	if got := superset.Status.Lifecycle.Phase; got != lifecyclePhaseComplete && got != lifecyclePhaseRestoring {
		t.Fatalf("expected final phase Complete or Restoring, got %q", got)
	}
	if !hasConditionReason(superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete, "LifecycleComplete") {
		t.Fatalf("expected LifecycleComplete condition reason, got %#v", superset.Status.Conditions)
	}
	if superset.Status.LastLifecycleImage != "apache/superset:6.0.1" {
		t.Fatalf("expected LastLifecycleImage to advance to 6.0.1, got %q", superset.Status.LastLifecycleImage)
	}
}

func taskJobName(parent, taskType string) string {
	switch taskType {
	case taskTypeSeed:
		return parent + suffixSeed
	case taskTypeMigrate:
		return parent + suffixMigrate
	case taskTypeRotate:
		return parent + suffixRotate
	case taskTypeInit:
		return parent + suffixInit
	}
	return ""
}

func getTaskJob(t *testing.T, c client.Client, namespace, name string) (*batchv1.Job, error) {
	t.Helper()
	job := &batchv1.Job{}
	err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, job)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}

func markJobSucceeded(t *testing.T, c client.Client, job *batchv1.Job) {
	t.Helper()
	now := metav1.Now()
	job.Status.Succeeded = 1
	job.Status.StartTime = &now
	job.Status.Conditions = []batchv1.JobCondition{{
		Type:               batchv1.JobComplete,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: now,
	}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatalf("marking %s job succeeded: %v", job.Name, err)
	}
}

func TestClearUpgradeApprovalAnnotation(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	t.Run("no annotations is a no-op", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
		assert.NoError(t, r.clearUpgradeApprovalAnnotation(ctx, superset))
	})

	t.Run("annotation present is removed via patch", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "default",
				Annotations: map[string]string{
					annotationApproveUpgrade: "token-123",
					"keep":                   "me",
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

		require.NoError(t, r.clearUpgradeApprovalAnnotation(ctx, superset))

		got := &supersetv1alpha1.Superset{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, got))
		_, present := got.Annotations[annotationApproveUpgrade]
		assert.False(t, present, "approval annotation should be removed")
		assert.Equal(t, "me", got.Annotations["keep"], "other annotations are preserved")
	})

	t.Run("different annotations present, no approval key is a no-op", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test", Namespace: "default",
				Annotations: map[string]string{"other": "v"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
		assert.NoError(t, r.clearUpgradeApprovalAnnotation(ctx, superset))
	})
}

func TestDeleteLifecycleTaskResources(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{
				Migrate:                &supersetv1alpha1.TaskRefStatus{State: taskStateComplete},
				LastCompletedChecksums: map[string]string{taskTypeMigrate: "sum"},
			},
		},
	}
	taskName := "test" + suffixMigrate
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      taskName,
		Namespace: "default",
		Labels:    map[string]string{labelInitTask: taskName, labelInitInstance: "test"},
	}}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "test-migrate-config", Namespace: "default"}}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, job, cm).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	require.NoError(t, r.deleteLifecycleTaskResources(ctx, superset, taskTypeMigrate, suffixMigrate))

	// Status slot cleared.
	assert.Nil(t, superset.Status.Lifecycle.Migrate)
	_, present := superset.Status.Lifecycle.LastCompletedChecksums[taskTypeMigrate]
	assert.False(t, present, "completed checksum entry should be deleted")
}

func TestFinalizeLifecycle(t *testing.T) {
	t.Run("with components enters Restoring", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{WebServer: &supersetv1alpha1.WebServerComponentSpec{}},
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{
				Upgrade: &supersetv1alpha1.UpgradeContext{FromVersion: "1", ToVersion: "2"},
			}},
		}
		r := &SupersetReconciler{Recorder: events.NewFakeRecorder(10)}
		r.finalizeLifecycle(superset, "apache/superset:2")
		assert.Equal(t, lifecyclePhaseRestoring, superset.Status.Lifecycle.Phase)
		assert.Equal(t, "apache/superset:2", superset.Status.LastLifecycleImage)
		assert.Nil(t, superset.Status.Lifecycle.Upgrade, "settle clears the upgrade context")
		assert.True(t, hasConditionReason(superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete, "LifecycleComplete"))
	})

	t.Run("without components enters Complete", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}},
		}
		r := &SupersetReconciler{Recorder: events.NewFakeRecorder(10)}
		r.finalizeLifecycle(superset, "apache/superset:2")
		assert.Equal(t, lifecyclePhaseComplete, superset.Status.Lifecycle.Phase)
	})
}

func TestCheckUpgradeGates(t *testing.T) {
	ctx := context.Background()
	r := &SupersetReconciler{Recorder: events.NewFakeRecorder(10)}

	t.Run("no image change is not gated", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}}}
		_, gated := r.checkUpgradeGates(ctx, s, false, "apache/superset:1", "apache/superset:1")
		assert.False(t, gated)
	})

	t.Run("first install (empty lastImage) is not gated", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}}}
		_, gated := r.checkUpgradeGates(ctx, s, true, "", "apache/superset:1")
		assert.False(t, gated)
	})

	t.Run("downgrade blocks with terminal result", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{Name: "test"},
			Status:     supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}},
		}
		res, gated := r.checkUpgradeGates(ctx, s, true, "apache/superset:3.0.0", "apache/superset:2.0.0")
		assert.True(t, gated)
		assert.True(t, res.TerminalFailure)
		assert.Equal(t, lifecyclePhaseBlocked, s.Status.Lifecycle.Phase)
		assert.True(t, hasConditionReason(s.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete, "DowngradeBlocked"))
	})

	t.Run("supervised upgrade awaits approval", func(t *testing.T) {
		mode := upgradeModeSupervised
		s := &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{Name: "test"},
			Spec: supersetv1alpha1.SupersetSpec{
				Lifecycle: &supersetv1alpha1.LifecycleSpec{UpgradeMode: &mode},
			},
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}},
		}
		res, gated := r.checkUpgradeGates(ctx, s, true, "apache/superset:2.0.0", "apache/superset:3.0.0")
		assert.True(t, gated)
		assert.False(t, res.TerminalFailure)
		assert.Equal(t, lifecyclePhaseAwaitingApproval, s.Status.Lifecycle.Phase)
		assert.Equal(t, phaseAwaitingApproval, s.Status.Phase)
	})

	t.Run("automatic upgrade proceeds past the gate", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{Name: "test"},
			Status:     supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}},
		}
		_, gated := r.checkUpgradeGates(ctx, s, true, "apache/superset:2.0.0", "apache/superset:3.0.0")
		assert.False(t, gated)
		// Upgrade context recorded for the in-flight upgrade.
		require.NotNil(t, s.Status.Lifecycle.Upgrade)
		assert.Equal(t, "2.0.0", s.Status.Lifecycle.Upgrade.FromVersion)
		assert.Equal(t, "3.0.0", s.Status.Lifecycle.Upgrade.ToVersion)
	})

	t.Run("non-semver tags proceed but emit a warning", func(t *testing.T) {
		rec := events.NewFakeRecorder(10)
		nr := &SupersetReconciler{Recorder: rec}
		s := &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{Name: "test"},
			Status:     supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}},
		}
		// "latest" → "main" cannot be ordered: not a downgrade, so not gated.
		_, gated := nr.checkUpgradeGates(ctx, s, true, "apache/superset:latest", "apache/superset:main")
		assert.False(t, gated)
		assert.NotEqual(t, lifecyclePhaseBlocked, s.Status.Lifecycle.Phase)
		require.NotNil(t, s.Status.Lifecycle.Upgrade)
		assert.Equal(t, string(DirectionUnknown), s.Status.Lifecycle.Upgrade.Direction)
		select {
		case ev := <-rec.Events:
			assert.Contains(t, ev, "VersionComparisonSkipped")
		default:
			t.Fatal("expected a VersionComparisonSkipped warning event")
		}
	})
}
