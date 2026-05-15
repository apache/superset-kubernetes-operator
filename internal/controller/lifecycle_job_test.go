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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestReconcileLifecycleTaskJob_CheckpointsCompletionBeforeRetention(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	taskChecksum := "sha256:test"

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
		},
	}
	job := lifecycleTaskJobForRetention("test-migrate", taskChecksum, batchv1.JobComplete)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, job).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	taskRef := &supersetv1alpha1.TaskRefStatus{MaxRetries: 3, Image: "apache/superset:latest"}
	flatSpec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
	}

	result, err := r.reconcileLifecycleTaskJob(ctx, superset, "test-migrate", taskTypeMigrate, flatSpec, taskChecksum, taskRef)
	if err != nil {
		t.Fatalf("reconcileLifecycleTaskJob: %v", err)
	}
	if result.Complete {
		t.Fatalf("expected checkpoint result before advancing pipeline, got %#v", result)
	}
	if taskRef.State != taskStateComplete || taskRef.CompletedChecksum != taskChecksum {
		t.Fatalf("expected completed status with checksum, got state=%q checksum=%q", taskRef.State, taskRef.CompletedChecksum)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &batchv1.Job{}); err != nil {
		t.Fatalf("completed job should remain until status persistence cleanup: %v", err)
	}

	if err := r.cleanupTaskJobsByRetention(ctx, superset, "test-migrate", taskTypeMigrate); err != nil {
		t.Fatalf("cleanupTaskJobsByRetention: %v", err)
	}
	err = c.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &batchv1.Job{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected completed job to be deleted after retention cleanup, got %v", err)
	}
}

func TestCleanupTaskJobsByRetention_DefaultKeepsFailedOnly(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	succeeded := lifecycleTaskJobForRetention("test-migrate-succeeded", "sha256:ok", batchv1.JobComplete)
	failed := lifecycleTaskJobForRetention("test-migrate-failed", "sha256:bad", batchv1.JobFailed)
	running := lifecycleTaskJobForRetention("test-migrate-running", "sha256:run", "")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, succeeded, failed, running).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	if err := r.cleanupTaskJobsByRetention(ctx, superset, "test-migrate", taskTypeMigrate); err != nil {
		t.Fatalf("cleanupTaskJobsByRetention: %v", err)
	}

	if err := c.Get(ctx, types.NamespacedName{Name: succeeded.Name, Namespace: succeeded.Namespace}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected succeeded job to be deleted, got %v", err)
	}
	for _, job := range []*batchv1.Job{failed, running} {
		if err := c.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &batchv1.Job{}); err != nil {
			t.Fatalf("expected job %s to remain: %v", job.Name, err)
		}
	}
}

func TestReconcileLifecycleTaskJob_DeterministicNameAvoidsDuplicateCreate(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	taskChecksum := "sha256:test"

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	flatSpec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	taskRef := &supersetv1alpha1.TaskRefStatus{MaxRetries: 3}
	if _, err := r.reconcileLifecycleTaskJob(ctx, superset, "test-migrate", taskTypeMigrate, flatSpec, taskChecksum, taskRef); err != nil {
		t.Fatalf("first reconcileLifecycleTaskJob: %v", err)
	}
	taskRef = &supersetv1alpha1.TaskRefStatus{MaxRetries: 3}
	if _, err := r.reconcileLifecycleTaskJob(ctx, superset, "test-migrate", taskTypeMigrate, flatSpec, taskChecksum, taskRef); err != nil {
		t.Fatalf("second reconcileLifecycleTaskJob: %v", err)
	}

	jobs := &batchv1.JobList{}
	if err := c.List(ctx, jobs); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected one deterministic job, got %d", len(jobs.Items))
	}
	if jobs.Items[0].Name != "test-migrate" {
		t.Fatalf("expected deterministic job name, got %q", jobs.Items[0].Name)
	}
	if jobs.Items[0].Spec.Completions == nil || *jobs.Items[0].Spec.Completions != 1 {
		t.Fatalf("expected explicit completions=1, got %#v", jobs.Items[0].Spec.Completions)
	}
	if jobs.Items[0].Spec.Parallelism == nil || *jobs.Items[0].Spec.Parallelism != 1 {
		t.Fatalf("expected explicit parallelism=1, got %#v", jobs.Items[0].Spec.Parallelism)
	}
}

func TestReconcileLifecycleTaskJob_StaleStatusImageDoesNotDeleteMatchingJob(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	taskChecksum := "sha256:test"
	oldImage := "apache/superset:old"
	newImage := "apache/superset:new"

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	flatSpec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "new"},
	}
	r := &SupersetReconciler{Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	job := r.buildLifecycleTaskJob(superset, "test-migrate", taskTypeMigrate, flatSpec, taskChecksum, defaultInitTimeout)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, job).
		Build()
	r.Client = c

	taskRef := &supersetv1alpha1.TaskRefStatus{
		State:      taskStateRunning,
		MaxRetries: 3,
		Image:      oldImage,
	}
	result, err := r.reconcileLifecycleTaskJob(ctx, superset, "test-migrate", taskTypeMigrate, flatSpec, taskChecksum, taskRef)
	if err != nil {
		t.Fatalf("reconcileLifecycleTaskJob: %v", err)
	}
	if result.Complete || result.RequeueAfter == 0 {
		t.Fatalf("expected running job wait result, got %#v", result)
	}
	if taskRef.Image != newImage {
		t.Fatalf("expected status image to be refreshed to %q, got %q", newImage, taskRef.Image)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &batchv1.Job{}); err != nil {
		t.Fatalf("matching job should not be deleted because status image was stale: %v", err)
	}
	assertNoEvents(t, r.Recorder.(*events.FakeRecorder))
}

func TestReconcileLifecycleTaskJob_DeletesJobWhenActualImageDiffers(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	taskChecksum := "sha256:test"

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	oldSpec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "old"},
	}
	newSpec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "new"},
	}
	recorder := events.NewFakeRecorder(10)
	r := &SupersetReconciler{Scheme: scheme, Recorder: recorder}
	job := r.buildLifecycleTaskJob(superset, "test-migrate", taskTypeMigrate, oldSpec, taskChecksum, defaultInitTimeout)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, job).
		Build()
	r.Client = c

	taskRef := &supersetv1alpha1.TaskRefStatus{
		State:      taskStateRunning,
		MaxRetries: 3,
		Image:      "apache/superset:old",
	}
	result, err := r.reconcileLifecycleTaskJob(ctx, superset, "test-migrate", taskTypeMigrate, newSpec, taskChecksum, taskRef)
	if err != nil {
		t.Fatalf("reconcileLifecycleTaskJob: %v", err)
	}
	if result.Complete || result.RequeueAfter == 0 {
		t.Fatalf("expected checkpoint/wait result after deleting stale job, got %#v", result)
	}
	err = c.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &batchv1.Job{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected stale job to be deleted, got %v", err)
	}
	assertNextEventContains(t, recorder, "Normal TaskImageChanged Migrate image changed from apache/superset:old to apache/superset:new, re-running task")
}

func lifecycleTaskJobForRetention(name, checksum string, conditionType batchv1.JobConditionType) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				labelInitInstance:        "test-migrate",
				labelInitTask:            "migrate",
				common.LabelKeyParent:    "test",
				common.LabelKeyComponent: string(common.ComponentInit),
			},
			Annotations: map[string]string{
				common.AnnotationConfigChecksum: checksum,
			},
		},
	}
	switch conditionType {
	case batchv1.JobComplete:
		job.Status.Succeeded = 1
		job.Status.Conditions = []batchv1.JobCondition{{
			Type:               batchv1.JobComplete,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
		}}
	case batchv1.JobFailed:
		job.Status.Conditions = []batchv1.JobCondition{{
			Type:               batchv1.JobFailed,
			Status:             corev1.ConditionTrue,
			Reason:             "BackoffLimitExceeded",
			Message:            "Job failed",
			LastTransitionTime: metav1.Now(),
		}}
	}
	return job
}
