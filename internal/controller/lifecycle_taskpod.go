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
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

func ensureTaskStatus(superset *supersetv1alpha1.Superset, taskType string) *supersetv1alpha1.TaskRefStatus {
	if superset.Status.Lifecycle == nil {
		superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{}
	}
	switch taskType {
	case taskTypeClone:
		if superset.Status.Lifecycle.Clone == nil {
			superset.Status.Lifecycle.Clone = &supersetv1alpha1.TaskRefStatus{}
		}
		return superset.Status.Lifecycle.Clone
	case taskTypeMigrate:
		if superset.Status.Lifecycle.Migrate == nil {
			superset.Status.Lifecycle.Migrate = &supersetv1alpha1.TaskRefStatus{}
		}
		return superset.Status.Lifecycle.Migrate
	case taskTypeRotate:
		if superset.Status.Lifecycle.Rotate == nil {
			superset.Status.Lifecycle.Rotate = &supersetv1alpha1.TaskRefStatus{}
		}
		return superset.Status.Lifecycle.Rotate
	default:
		if superset.Status.Lifecycle.Init == nil {
			superset.Status.Lifecycle.Init = &supersetv1alpha1.TaskRefStatus{}
		}
		return superset.Status.Lifecycle.Init
	}
}

func resetTaskStatusForRun(taskRef *supersetv1alpha1.TaskRefStatus, desiredChecksum string, maxRetries int32) {
	taskRef.State = taskStatePending
	taskRef.StartedAt = nil
	taskRef.CompletedAt = nil
	taskRef.Duration = ""
	taskRef.Attempts = 0
	taskRef.MaxRetries = maxRetries
	taskRef.Ref = ""
	taskRef.PodName = ""
	taskRef.Image = ""
	taskRef.Message = ""
	taskRef.NextAttemptAt = nil
	taskRef.DesiredChecksum = desiredChecksum
	taskRef.CompletedChecksum = ""
	taskRef.Conditions = nil
}

func rememberCompletedTaskChecksum(superset *supersetv1alpha1.Superset, taskType, checksum string) {
	if superset.Status.Lifecycle == nil {
		superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{}
	}
	if superset.Status.Lifecycle.LastCompletedChecksums == nil {
		superset.Status.Lifecycle.LastCompletedChecksums = make(map[string]string)
	}
	superset.Status.Lifecycle.LastCompletedChecksums[taskType] = checksum
}

func (r *SupersetReconciler) reconcileLifecycleTaskPod(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	taskName, taskType string,
	flatSpec *supersetv1alpha1.FlatComponentSpec,
	taskChecksum string,
	taskRef *supersetv1alpha1.TaskRefStatus,
) (lifecycleResult, error) {
	log := logf.FromContext(ctx)
	maxRetries := taskRef.MaxRetries
	timeout := r.taskTimeoutValue(superset, taskType)
	image := fmt.Sprintf("%s:%s", flatSpec.Image.Repository, flatSpec.Image.Tag)

	if taskRef.State == "" {
		taskRef.State = taskStatePending
		taskRef.Image = image
	}

	if taskRef.NextAttemptAt != nil {
		if remaining := taskRef.NextAttemptAt.Sub(r.now()); remaining > 0 {
			return lifecycleResult{RequeueAfter: remaining}, nil
		}
		taskRef.NextAttemptAt = nil
	}

	existingPod, err := r.findLifecycleTaskPod(ctx, superset, taskName, taskType)
	if err != nil {
		return lifecycleResult{}, err
	}

	if existingPod != nil {
		taskRef.PodName = existingPod.Name
		taskRef.Ref = "Pod/" + existingPod.Name

		if taskRef.Image != "" && taskRef.Image != image {
			oldImage := taskRef.Image
			log.Info("Lifecycle task image changed, deleting stale pod", "task", taskType, "old", oldImage, "new", image)
			if err := r.Delete(ctx, existingPod); client.IgnoreNotFound(err) != nil {
				return lifecycleResult{}, err
			}
			taskRef.State = taskStatePending
			taskRef.Image = image
			taskRef.Ref = ""
			taskRef.PodName = ""
			taskRef.Message = "Image changed, re-running task"
			r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "TaskImageChanged", "Lifecycle",
				"%s image changed from %s to %s, re-running task", taskType, oldImage, image)
			return lifecycleResult{RequeueAfter: time.Second}, nil
		}

		switch existingPod.Status.Phase {
		case corev1.PodSucceeded:
			now := metav1.Now()
			taskRef.State = taskStateComplete
			taskRef.CompletedAt = &now
			if taskRef.StartedAt != nil {
				taskRef.Duration = now.Sub(taskRef.StartedAt.Time).Round(time.Second).String()
			}
			taskRef.Message = "Completed successfully"
			taskRef.CompletedChecksum = taskChecksum
			setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
				metav1.ConditionTrue, "TaskComplete", "Task completed successfully", superset.Generation)
			r.applyTaskPodRetention(ctx, superset, taskType, existingPod)
			return lifecycleComplete(), nil

		case corev1.PodFailed:
			taskRef.Attempts++
			taskRef.Message = podFailureMessage(existingPod)

			if taskRef.Attempts >= maxRetries {
				taskRef.State = taskStateFailed
				taskRef.CompletedChecksum = taskChecksum
				r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "TaskFailed", "Lifecycle",
					"%s task failed after %d attempts: %s", taskType, taskRef.Attempts, taskRef.Message)
				setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
					metav1.ConditionFalse, "TaskFailed", taskRef.Message, superset.Generation)
				r.applyTaskPodRetention(ctx, superset, taskType, existingPod)
				return lifecycleTerminal(), nil
			}

			backoff := calculateBackoff(taskRef.Attempts)
			next := metav1.NewTime(r.now().Add(backoff))
			taskRef.NextAttemptAt = &next
			taskRef.State = taskStatePending

			if err := r.Delete(ctx, existingPod); client.IgnoreNotFound(err) != nil {
				return lifecycleResult{}, err
			}

			r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "TaskRetry", "Lifecycle",
				"%s task failed (attempt %d/%d), retrying in %s", taskType, taskRef.Attempts, maxRetries, backoff)
			setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
				metav1.ConditionFalse, "TaskRetrying", fmt.Sprintf("Retrying after attempt %d", taskRef.Attempts), superset.Generation)
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
				metav1.ConditionFalse, "TaskRetrying", fmt.Sprintf("%s task is retrying", taskType), superset.Generation)
			return lifecycleResult{RequeueAfter: backoff}, nil

		case corev1.PodRunning, corev1.PodPending:
			taskRef.State = taskStateRunning
			if taskRef.StartedAt == nil {
				started := existingPod.CreationTimestamp
				taskRef.StartedAt = &started
			}
			if taskRef.StartedAt != nil && r.now().Sub(taskRef.StartedAt.Time) > timeout {
				taskRef.Message = fmt.Sprintf("Timed out after %s", timeout)
				taskRef.Attempts++
				if taskRef.Attempts >= maxRetries {
					taskRef.State = taskStateFailed
					taskRef.CompletedChecksum = taskChecksum
					r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "TaskFailed", "Lifecycle",
						"%s task timed out after %d attempts", taskType, taskRef.Attempts)
					setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
						metav1.ConditionFalse, "TaskTimedOut", taskRef.Message, superset.Generation)
					r.applyTaskPodRetention(ctx, superset, taskType, existingPod)
					return lifecycleTerminal(), nil
				}
				backoff := calculateBackoff(taskRef.Attempts)
				next := metav1.NewTime(r.now().Add(backoff))
				taskRef.NextAttemptAt = &next
				taskRef.State = taskStatePending
				if err := r.Delete(ctx, existingPod); client.IgnoreNotFound(err) != nil {
					return lifecycleResult{}, err
				}
				return lifecycleResult{RequeueAfter: backoff}, nil
			}
			setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
				metav1.ConditionFalse, "TaskInProgress", "Task is in progress", superset.Generation)
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
				metav1.ConditionFalse, "TaskInProgress", fmt.Sprintf("%s task is in progress", taskType), superset.Generation)
			return lifecycleWait(), nil
		}

		return lifecycleWait(), nil
	}

	log.Info("Creating lifecycle task pod", "task", taskType, "attempt", taskRef.Attempts+1)
	podSpec := buildInitPod(flatSpec)
	pt := safePodTemplatePtr(flatSpec.PodTemplate)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: taskName + "-",
			Namespace:    superset.Namespace,
			Labels:       mergeLabels(pt.Labels, r.lifecycleTaskPodLabels(superset, taskName, taskType)),
			Annotations:  mergeAnnotations(nil, pt.Annotations),
		},
		Spec: podSpec,
	}
	if err := controllerutil.SetControllerReference(superset, pod, r.Scheme); err != nil {
		return lifecycleResult{}, fmt.Errorf("setting controller reference on lifecycle task pod: %w", err)
	}
	if err := r.Create(ctx, pod); err != nil {
		return lifecycleResult{}, fmt.Errorf("creating lifecycle task pod: %w", err)
	}

	now := metav1.Now()
	taskRef.State = taskStateRunning
	taskRef.PodName = pod.Name
	taskRef.Ref = "Pod/" + pod.Name
	taskRef.StartedAt = &now
	taskRef.CompletedAt = nil
	taskRef.Duration = ""
	taskRef.Image = image
	taskRef.Message = ""
	setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
		metav1.ConditionFalse, "TaskInProgress", "Task is in progress", superset.Generation)
	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
		metav1.ConditionFalse, "TaskInProgress", fmt.Sprintf("%s task is in progress", taskType), superset.Generation)
	r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "TaskStarted", "Lifecycle",
		"Started %s task pod: %s", taskType, pod.Name)
	return lifecycleWait(), nil
}

func (r *SupersetReconciler) lifecycleTaskPodLabels(superset *supersetv1alpha1.Superset, taskName, taskType string) map[string]string {
	return map[string]string{
		naming.LabelKeyName:      naming.LabelValueApp,
		naming.LabelKeyParent:    superset.Name,
		naming.LabelKeyComponent: string(naming.ComponentInit),
		labelInitInstance:        taskName,
		labelInitTask:            strings.ToLower(taskType),
	}
}

func (r *SupersetReconciler) findLifecycleTaskPod(ctx context.Context, superset *supersetv1alpha1.Superset, taskName, taskType string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(superset.Namespace),
		client.MatchingLabels{
			labelInitInstance: taskName,
			labelInitTask:     strings.ToLower(taskType),
		},
	); err != nil {
		return nil, fmt.Errorf("listing lifecycle task pods: %w", err)
	}

	if len(podList.Items) == 0 {
		return nil, nil
	}

	var latest *corev1.Pod
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		if latest == nil || p.CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = p
		}
	}
	return latest, nil
}

func (r *SupersetReconciler) deleteTaskPods(ctx context.Context, superset *supersetv1alpha1.Superset, taskName string) error {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(superset.Namespace),
		client.MatchingLabels{labelInitInstance: taskName},
	); err != nil {
		return fmt.Errorf("listing lifecycle task pods: %w", err)
	}
	for i := range podList.Items {
		if err := client.IgnoreNotFound(r.Delete(ctx, &podList.Items[i])); err != nil {
			return err
		}
	}
	return nil
}

func (r *SupersetReconciler) applyTaskPodRetention(ctx context.Context, superset *supersetv1alpha1.Superset, taskType string, pod *corev1.Pod) {
	policy := r.taskRetentionPolicyValue(superset, taskType)
	if ShouldDeletePod(policy, pod.Status.Phase) {
		if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
			logf.FromContext(ctx).Error(err, "Failed to delete completed lifecycle task pod", "pod", pod.Name)
		}
	}
}
