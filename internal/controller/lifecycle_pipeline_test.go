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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// TestLifecyclePipeline_FullSuccess walks the full clone → migrate → rotate →
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
				Clone: &supersetv1alpha1.CloneTaskSpec{
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

	expected := []string{taskTypeClone, taskTypeMigrate, taskTypeRotate, taskTypeInit}
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
	case taskTypeClone:
		return parent + suffixClone
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
