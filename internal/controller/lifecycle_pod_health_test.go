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
	"strings"
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

func TestPodStartupError(t *testing.T) {
	waiting := func(reason string) corev1.ContainerStatus {
		return corev1.ContainerStatus{Name: "c", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: "boom"}}}
	}
	cases := map[string]struct {
		pod      corev1.Pod
		wantErr  bool
		contains string
	}{
		"create container config error (init)": {
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase:                 corev1.PodPending,
				InitContainerStatuses: []corev1.ContainerStatus{waiting("CreateContainerConfigError")},
			}},
			wantErr:  true,
			contains: "CreateContainerConfigError",
		},
		"image pull backoff (main)": {
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase:             corev1.PodPending,
				ContainerStatuses: []corev1.ContainerStatus{waiting("ImagePullBackOff")},
			}},
			wantErr:  true,
			contains: "ImagePullBackOff",
		},
		"unschedulable": {
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{{
					Type:    corev1.PodScheduled,
					Status:  corev1.ConditionFalse,
					Reason:  corev1.PodReasonUnschedulable,
					Message: "0/3 nodes available",
				}},
			}},
			wantErr:  true,
			contains: "unschedulable",
		},
		"container creating is transient": {
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase:                 corev1.PodPending,
				InitContainerStatuses: []corev1.ContainerStatus{waiting("ContainerCreating")},
			}},
			wantErr: false,
		},
		"pod initializing is transient": {
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase:             corev1.PodPending,
				ContainerStatuses: []corev1.ContainerStatus{waiting("PodInitializing")},
			}},
			wantErr: false,
		},
		"running pod is fine": {
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{Name: "c", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			}},
			wantErr: false,
		},
		"terminating pod ignored": {
			pod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time}},
				Status: corev1.PodStatus{
					Phase:                 corev1.PodPending,
					InitContainerStatuses: []corev1.ContainerStatus{waiting("CreateContainerConfigError")},
				},
			},
			wantErr: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			msg, got := podStartupError(&tc.pod)
			if got != tc.wantErr {
				t.Fatalf("podStartupError stuck=%v, want %v (msg=%q)", got, tc.wantErr, msg)
			}
			if tc.wantErr && tc.contains != "" && !strings.Contains(msg, tc.contains) {
				t.Errorf("message %q does not contain %q", msg, tc.contains)
			}
		})
	}
}

func TestPodSpecHash_SensitiveToSecurityContext(t *testing.T) {
	base := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
	}
	hardened := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
		PodTemplate: &supersetv1alpha1.PodTemplate{
			PodSecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: common.Ptr(true)},
		},
	}
	if podSpecHash(buildInitPod(base)) == podSpecHash(buildInitPod(hardened)) {
		t.Error("expected pod-spec hash to change when the pod security context changes")
	}
	// Stable for identical input.
	first := podSpecHash(buildInitPod(base))
	second := podSpecHash(buildInitPod(base))
	if first != second {
		t.Error("expected pod-spec hash to be stable for identical input")
	}
}

func TestHandleStuckTaskPod_SelfHealsWhenSpecChanged(t *testing.T) {
	// A wedged task Pod plus a Job whose stamped pod-spec hash no longer matches
	// the desired spec means the user fixed the spec. The operator must delete
	// the Job (so the next reconcile recreates it) instead of waiting forever —
	// the user should never have to delete the Job by hand.
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-migrate",
			Namespace:   "default",
			Annotations: map[string]string{common.AnnotationTaskPodSpecHash: "stale-hash"},
		},
	}
	pod := wedgedTaskPod("test-migrate", "CreateContainerConfigError")

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, job, pod).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	flatSpec := &supersetv1alpha1.FlatComponentSpec{Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"}}
	taskRef := &supersetv1alpha1.TaskRefStatus{State: taskStateRunning, MaxRetries: 3}

	res, handled, err := r.handleStuckTaskPod(ctx, superset, job, taskTypeMigrate, "test-migrate", flatSpec, taskRef)
	if err != nil {
		t.Fatalf("handleStuckTaskPod: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true for a wedged pod")
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a requeue, got %#v", res)
	}
	if taskRef.State != taskStatePending {
		t.Errorf("expected taskRef reset to Pending for recreation, got %q", taskRef.State)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected wedged Job to be deleted for recreation, got %v", err)
	}
}

func TestHandleStuckTaskPod_SurfacesWhenSpecUnchanged(t *testing.T) {
	// When the spec has not changed (stamped hash matches desired), the operator
	// cannot fix the wedge itself. It must surface a clear condition/event and
	// keep the Job (no churn), rather than silently looping.
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	flatSpec := &supersetv1alpha1.FlatComponentSpec{Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"}}
	matchingHash := podSpecHash(buildInitPod(flatSpec))

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-migrate",
			Namespace:   "default",
			Annotations: map[string]string{common.AnnotationTaskPodSpecHash: matchingHash},
		},
	}
	pod := wedgedTaskPod("test-migrate", "CreateContainerConfigError")

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, job, pod).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	taskRef := &supersetv1alpha1.TaskRefStatus{State: taskStateRunning, MaxRetries: 3}

	_, handled, err := r.handleStuckTaskPod(ctx, superset, job, taskTypeMigrate, "test-migrate", flatSpec, taskRef)
	if err != nil {
		t.Fatalf("handleStuckTaskPod: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true for a wedged pod")
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, &batchv1.Job{}); err != nil {
		t.Errorf("expected Job to be kept when spec is unchanged, got %v", err)
	}
	if !conditionHasReason(superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete, reasonTaskCannotStart) {
		t.Error("expected parent LifecycleComplete condition with reason TaskCannotStart")
	}
	if taskRef.Message == "" {
		t.Error("expected taskRef.Message to describe the startup failure")
	}
}

func TestHandleStuckTaskPod_NotStuckPassesThrough(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "test-migrate", Namespace: "default"}}
	// A healthy (running) pod must not be treated as stuck.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-migrate-x", Namespace: "default", Labels: map[string]string{labelInitInstance: "test-migrate"}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, job, pod).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	flatSpec := &supersetv1alpha1.FlatComponentSpec{Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"}}
	_, handled, err := r.handleStuckTaskPod(ctx, superset, job, taskTypeMigrate, "test-migrate", flatSpec, &supersetv1alpha1.TaskRefStatus{})
	if err != nil {
		t.Fatalf("handleStuckTaskPod: %v", err)
	}
	if handled {
		t.Error("expected handled=false for a healthy pod so normal handling proceeds")
	}
}

func TestTaskPodSpecChanged(t *testing.T) {
	// Drives the terminal-failed retry gate: a spec change since the failed Job
	// was created must be detectable so the controller can rerun it.
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
	flatSpec := &supersetv1alpha1.FlatComponentSpec{Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"}}
	matchingHash := podSpecHash(buildInitPod(flatSpec))

	t.Run("no job -> not changed", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
		changed, err := r.taskPodSpecChanged(ctx, superset, "test-migrate", flatSpec)
		if err != nil || changed {
			t.Fatalf("expected (false,nil) when no Job exists, got (%v,%v)", changed, err)
		}
	})

	t.Run("matching hash -> not changed", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "test-migrate", Namespace: "default", Annotations: map[string]string{common.AnnotationTaskPodSpecHash: matchingHash}}}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, job).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
		changed, err := r.taskPodSpecChanged(ctx, superset, "test-migrate", flatSpec)
		if err != nil || changed {
			t.Fatalf("expected (false,nil) for matching hash, got (%v,%v)", changed, err)
		}
	})

	t.Run("stale hash -> changed", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "test-migrate", Namespace: "default", Annotations: map[string]string{common.AnnotationTaskPodSpecHash: "stale"}}}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, job).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
		changed, err := r.taskPodSpecChanged(ctx, superset, "test-migrate", flatSpec)
		if err != nil || !changed {
			t.Fatalf("expected (true,nil) for stale hash, got (%v,%v)", changed, err)
		}
	})
}

func wedgedTaskPod(taskName, reason string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName + "-abcde",
			Namespace: "default",
			Labels:    map[string]string{labelInitInstance: taskName},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{
				{Name: "create-database", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: "container has runAsNonRoot and image will run as root"}}},
			},
		},
	}
}

func conditionHasReason(conditions []metav1.Condition, condType, reason string) bool {
	for _, c := range conditions {
		if c.Type == condType && c.Reason == reason {
			return true
		}
	}
	return false
}
