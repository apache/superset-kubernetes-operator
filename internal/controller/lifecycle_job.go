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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

const jobBackoffLimit int32 = 0

func ensureTaskStatus(superset *supersetv1alpha1.Superset, taskType string) *supersetv1alpha1.TaskRefStatus {
	if superset.Status.Lifecycle == nil {
		superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{}
	}
	desc := lifecycleTaskDescriptorByType(taskType)
	if desc == nil {
		// Defensive default: unknown task types fall back to the init slot to
		// preserve the prior behavior of ensureTaskStatus, which never returned nil.
		desc = lifecycleTaskDescriptorByType(taskTypeInit)
	}
	ref := desc.TaskRef(superset.Status.Lifecycle)
	if *ref == nil {
		*ref = &supersetv1alpha1.TaskRefStatus{}
	}
	return *ref
}

func resetTaskStatusForRun(taskRef *supersetv1alpha1.TaskRefStatus, desiredChecksum string, maxRetries int32) {
	taskRef.State = taskStatePending
	taskRef.StartedAt = nil
	taskRef.CompletedAt = nil
	taskRef.Attempts = 0
	taskRef.MaxRetries = maxRetries
	taskRef.NextAttemptAt = nil
	taskRef.DesiredChecksum = desiredChecksum
	taskRef.CompletedChecksum = ""
	taskRef.Message = ""
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

func (r *SupersetReconciler) reconcileLifecycleTaskJob(
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
	}
	if taskRef.Image == "" {
		taskRef.Image = image
	}

	if taskRef.NextAttemptAt != nil {
		if remaining := taskRef.NextAttemptAt.Sub(r.now()); remaining > 0 {
			return lifecycleResult{RequeueAfter: remaining}, nil
		}
		taskRef.NextAttemptAt = nil
	}

	existingJob, err := r.getLifecycleTaskJob(ctx, superset, taskName)
	if err != nil {
		return lifecycleResult{}, err
	}

	if existingJob != nil {
		if existingJob.DeletionTimestamp != nil {
			return lifecycleWait(), nil
		}

		if !r.taskJobMatchesChecksum(existingJob, taskChecksum) {
			log.Info("Deleting stale lifecycle task job", "task", taskType, "job", existingJob.Name)
			if err := r.Delete(ctx, existingJob); client.IgnoreNotFound(err) != nil {
				return lifecycleResult{}, err
			}
			return lifecycleWait(), nil
		}

		if result, handled, err := r.reconcileTaskJobImage(ctx, superset, existingJob, taskType, image, taskRef); handled || err != nil {
			return result, err
		}

		if jobComplete(existingJob) {
			now := metav1.Now()
			if completed := jobConditionTransitionTime(existingJob, batchv1.JobComplete); completed != nil {
				now = *completed
			}
			taskRef.State = taskStateComplete
			taskRef.CompletedAt = &now
			if taskRef.StartedAt == nil && existingJob.Status.StartTime != nil {
				taskRef.StartedAt = existingJob.Status.StartTime
			}
			taskRef.Message = "Completed successfully"
			taskRef.CompletedChecksum = taskChecksum
			setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
				metav1.ConditionTrue, "TaskComplete", "Task completed successfully", superset.Generation)
			return lifecycleCheckpoint(), nil
		}

		if jobFailed(existingJob) {
			if taskRef.State == taskStatePending && taskRef.Attempts > 0 && taskRef.NextAttemptAt == nil {
				if err := r.Delete(ctx, existingJob); client.IgnoreNotFound(err) != nil {
					return lifecycleResult{}, err
				}
				return lifecycleWait(), nil
			}

			taskRef.Attempts++
			taskRef.Message = jobFailureMessage(existingJob)
			if taskRef.Attempts >= maxRetries {
				taskRef.State = taskStateFailed
				taskRef.CompletedChecksum = taskChecksum
				if completed := jobConditionTransitionTime(existingJob, batchv1.JobFailed); completed != nil {
					taskRef.CompletedAt = completed
				}
				r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "TaskFailed", "Lifecycle",
					"%s task failed after %d attempts: %s", taskType, taskRef.Attempts, taskRef.Message)
				setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
					metav1.ConditionFalse, "TaskFailed", taskRef.Message, superset.Generation)
				return lifecycleTerminal(), nil
			}

			backoff := calculateBackoff(taskRef.Attempts)
			next := metav1.NewTime(r.now().Add(backoff))
			taskRef.NextAttemptAt = &next
			taskRef.State = taskStatePending

			r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "TaskRetry", "Lifecycle",
				"%s task failed (attempt %d/%d), retrying in %s", taskType, taskRef.Attempts, maxRetries, backoff)
			setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
				metav1.ConditionFalse, "TaskRetrying", fmt.Sprintf("Retrying after attempt %d", taskRef.Attempts), superset.Generation)
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
				metav1.ConditionFalse, "TaskRetrying", fmt.Sprintf("%s task is retrying", taskType), superset.Generation)
			return lifecycleCheckpoint(), nil
		}

		if result, handled, err := r.handleStuckTaskPod(ctx, superset, existingJob, taskType, taskName, flatSpec, taskRef); handled || err != nil {
			return result, err
		}

		taskRef.State = taskStateRunning
		if taskRef.StartedAt == nil {
			if existingJob.Status.StartTime != nil {
				taskRef.StartedAt = existingJob.Status.StartTime
			} else {
				started := existingJob.CreationTimestamp
				taskRef.StartedAt = &started
			}
		}
		setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
			metav1.ConditionFalse, "TaskInProgress", "Task is in progress", superset.Generation)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionFalse, "TaskInProgress", fmt.Sprintf("%s task is in progress", taskType), superset.Generation)
		return lifecycleWait(), nil
	}

	log.Info("Creating lifecycle task job", "task", taskType, "attempt", taskRef.Attempts+1)
	job := r.buildLifecycleTaskJob(superset, taskName, taskType, flatSpec, taskChecksum, timeout)
	if err := controllerutil.SetControllerReference(superset, job, r.Scheme); err != nil {
		return lifecycleResult{}, fmt.Errorf("setting controller reference on lifecycle task job: %w", err)
	}
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return lifecycleWait(), nil
		}
		return lifecycleResult{}, fmt.Errorf("creating lifecycle task job: %w", err)
	}

	now := metav1.Now()
	taskRef.State = taskStateRunning
	taskRef.StartedAt = &now
	taskRef.CompletedAt = nil
	taskRef.Image = image
	taskRef.Message = ""
	setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
		metav1.ConditionFalse, "TaskInProgress", "Task is in progress", superset.Generation)
	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
		metav1.ConditionFalse, "TaskInProgress", fmt.Sprintf("%s task is in progress", taskType), superset.Generation)
	r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "TaskStarted", "Lifecycle",
		"Started %s task job: %s", taskType, job.Name)
	return lifecycleWait(), nil
}

// reasonTaskCannotStart is the condition reason and event reason used when a
// task Pod is wedged (un-startable) and the controller cannot self-heal it.
const reasonTaskCannotStart = "TaskCannotStart"

// handleStuckTaskPod detects a wedged task Pod (one that cannot start its
// containers without intervention) and either self-heals or surfaces it.
//
// Self-heal: a Job's pod template is immutable, so a spec fix can only take
// effect by replacing the Job. When the desired pod-spec hash differs from the
// hash stamped on the existing Job, the spec that would fix the wedge has
// changed, so the controller deletes the Job and lets the next reconcile
// recreate it from the current spec — without anyone having to delete it by
// hand. When the hash matches (spec unchanged), it instead records a clear,
// actionable status/condition/event and waits, rather than silently looping
// until the Job's activeDeadlineSeconds.
//
// Returns handled=true when it took ownership of this reconcile step.
func (r *SupersetReconciler) handleStuckTaskPod(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	existingJob *batchv1.Job,
	taskType, taskName string,
	flatSpec *supersetv1alpha1.FlatComponentSpec,
	taskRef *supersetv1alpha1.TaskRefStatus,
) (lifecycleResult, bool, error) {
	log := logf.FromContext(ctx)

	msg, stuck, err := r.taskPodStartupError(ctx, superset, taskName)
	if err != nil {
		return lifecycleResult{}, false, err
	}
	if !stuck {
		return lifecycleResult{}, false, nil
	}

	desiredHash := podSpecHash(buildInitPod(flatSpec))
	if existingJob.Annotations[naming.AnnotationTaskPodSpecHash] != desiredHash {
		log.Info("Task pod cannot start and pod spec changed; recreating task job",
			"task", taskType, "reason", msg)
		if err := r.Delete(ctx, existingJob); client.IgnoreNotFound(err) != nil {
			return lifecycleResult{}, false, fmt.Errorf("deleting wedged task job %s: %w", existingJob.Name, err)
		}
		taskRef.State = taskStatePending
		taskRef.Message = "Recreating task after spec change: " + msg
		r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "TaskRecovering", "Lifecycle",
			"%s task pod could not start (%s); recreating with updated spec", taskType, msg)
		return lifecycleWait(), true, nil
	}

	// Spec unchanged: cannot self-heal automatically. Surface the blocker so it
	// is visible on the Superset status and as an event, and keep waiting.
	taskRef.Message = msg
	if !hasLifecycleConditionReason(superset, reasonTaskCannotStart) {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, reasonTaskCannotStart, "Lifecycle",
			"%s task pod cannot start: %s", taskType, msg)
	}
	setCondition(&taskRef.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
		metav1.ConditionFalse, reasonTaskCannotStart, msg, superset.Generation)
	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
		metav1.ConditionFalse, reasonTaskCannotStart,
		fmt.Sprintf("%s task pod cannot start: %s", taskType, msg), superset.Generation)
	return lifecycleWait(), true, nil
}

// taskPodStartupError lists the Pods of a lifecycle task Job and returns the
// first un-startable one's reason, if any.
func (r *SupersetReconciler) taskPodStartupError(ctx context.Context, superset *supersetv1alpha1.Superset, taskName string) (string, bool, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(superset.Namespace),
		client.MatchingLabels{labelInitInstance: taskName},
	); err != nil {
		return "", false, fmt.Errorf("listing task pods for %s: %w", taskName, err)
	}
	for i := range pods.Items {
		if msg, stuck := podStartupError(&pods.Items[i]); stuck {
			return msg, true, nil
		}
	}
	return "", false, nil
}

// taskPodSpecChanged reports whether the desired pod spec for a task differs
// from the spec stamped on its existing Job (AnnotationTaskPodSpecHash). It lets
// the controller retry a terminally failed task once the user changes something
// about how the pod runs (securityContext, resources, etc.) — that change may be
// exactly what fixes the failure. Returns false when no Job exists, so the
// normal create path applies.
func (r *SupersetReconciler) taskPodSpecChanged(ctx context.Context, superset *supersetv1alpha1.Superset, taskName string, flatSpec *supersetv1alpha1.FlatComponentSpec) (bool, error) {
	job, err := r.getLifecycleTaskJob(ctx, superset, taskName)
	if err != nil {
		return false, err
	}
	if job == nil {
		return false, nil
	}
	return job.Annotations[naming.AnnotationTaskPodSpecHash] != podSpecHash(buildInitPod(flatSpec)), nil
}

func (r *SupersetReconciler) getLifecycleTaskJob(ctx context.Context, superset *supersetv1alpha1.Superset, taskName string) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: taskName, Namespace: superset.Namespace}, job); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting lifecycle task job: %w", err)
	}
	return job, nil
}

func (r *SupersetReconciler) buildLifecycleTaskJob(
	superset *supersetv1alpha1.Superset,
	taskName, taskType string,
	flatSpec *supersetv1alpha1.FlatComponentSpec,
	taskChecksum string,
	timeout time.Duration,
) *batchv1.Job {
	podSpec := buildInitPod(flatSpec)
	pt := safePodTemplatePtr(flatSpec.PodTemplate)
	labels := mergeLabels(pt.Labels, r.lifecycleTaskLabels(superset, taskName, taskType))
	// Pod-template annotations carry the semantic task checksum (drives rolling
	// restarts). The pod-spec hash is stamped on the Job only — never on the pod
	// template — so it cannot feed back into its own input.
	podAnnotations := mergeAnnotations(pt.Annotations, map[string]string{
		naming.AnnotationConfigChecksum: taskChecksum,
	})
	jobAnnotations := mergeAnnotations(podAnnotations, map[string]string{
		naming.AnnotationTaskPodSpecHash: podSpecHash(podSpec),
	})
	var activeDeadlineSeconds *int64
	if timeout > 0 {
		seconds := int64(timeout.Seconds())
		if seconds < 1 {
			seconds = 1
		}
		activeDeadlineSeconds = &seconds
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        taskName,
			Namespace:   superset.Namespace,
			Labels:      labels,
			Annotations: jobAnnotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          ptrInt32(jobBackoffLimit),
			Completions:           ptrInt32(1),
			Parallelism:           ptrInt32(1),
			ActiveDeadlineSeconds: activeDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: podAnnotations,
				},
				Spec: podSpec,
			},
		},
	}
}

func ptrInt32(v int32) *int32 {
	return &v
}

// podSpecHash returns a stable hash of a task Job's rendered pod spec. It is
// stamped on the Job (AnnotationTaskPodSpecHash) so the controller can tell
// whether the desired pod spec has changed since a wedged Job was created.
func podSpecHash(podSpec corev1.PodSpec) string {
	return computeChecksum(podSpec)
}

func (r *SupersetReconciler) taskJobMatchesChecksum(job *batchv1.Job, taskChecksum string) bool {
	if job.Annotations == nil || job.Annotations[naming.AnnotationConfigChecksum] == "" {
		return !jobComplete(job) && !jobFailed(job)
	}
	return job.Annotations[naming.AnnotationConfigChecksum] == taskChecksum
}

func (r *SupersetReconciler) reconcileTaskJobImage(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	existingJob *batchv1.Job,
	taskType string,
	image string,
	taskRef *supersetv1alpha1.TaskRefStatus,
) (lifecycleResult, bool, error) {
	existingImage := lifecycleJobMainImage(existingJob)
	if existingImage != "" && existingImage != image {
		log := logf.FromContext(ctx)
		log.Info("Lifecycle task job image changed, deleting stale job", "task", taskType, "old", existingImage, "new", image)
		if err := r.Delete(ctx, existingJob); client.IgnoreNotFound(err) != nil {
			return lifecycleResult{}, false, err
		}
		taskRef.State = taskStatePending
		taskRef.Image = image
		taskRef.Message = "Image changed, re-running task"
		r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "TaskImageChanged", "Lifecycle",
			"%s image changed from %s to %s, re-running task", taskType, existingImage, image)
		return lifecycleCheckpoint(), true, nil
	}
	if taskRef.Image != image {
		taskRef.Image = image
	}
	return lifecycleResult{}, false, nil
}

func lifecycleJobMainImage(job *batchv1.Job) string {
	if job == nil {
		return ""
	}
	for _, container := range job.Spec.Template.Spec.Containers {
		if container.Name == naming.Container {
			return container.Image
		}
	}
	if len(job.Spec.Template.Spec.Containers) == 0 {
		return ""
	}
	return job.Spec.Template.Spec.Containers[0].Image
}

func (r *SupersetReconciler) lifecycleTaskLabels(superset *supersetv1alpha1.Superset, taskName, taskType string) map[string]string {
	return map[string]string{
		naming.LabelKeyName:      naming.LabelValueApp,
		naming.LabelKeyParent:    superset.Name,
		naming.LabelKeyComponent: string(naming.ComponentInit),
		labelInitInstance:        taskName,
		labelInitTask:            strings.ToLower(taskType),
	}
}

func (r *SupersetReconciler) deleteTaskJobs(ctx context.Context, superset *supersetv1alpha1.Superset, taskName string) error {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: taskName, Namespace: superset.Namespace},
	}
	if err := r.deleteLifecycleJob(ctx, job); err != nil {
		return fmt.Errorf("deleting lifecycle task job: %w", err)
	}
	return nil
}

func (r *SupersetReconciler) deleteLifecycleJob(ctx context.Context, job *batchv1.Job) error {
	err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationForeground))
	return client.IgnoreNotFound(err)
}

func (r *SupersetReconciler) cleanupLifecycleTaskJobsByRetention(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	for _, desc := range lifecycleTaskDescriptors {
		if err := r.cleanupTaskJobsByRetention(ctx, superset, superset.Name+desc.Suffix, desc.TaskType); err != nil {
			return err
		}
	}
	return nil
}

func (r *SupersetReconciler) cleanupTaskJobsByRetention(ctx context.Context, superset *supersetv1alpha1.Superset, taskName, taskType string) error {
	jobList := &batchv1.JobList{}
	if err := r.List(ctx, jobList,
		client.InNamespace(superset.Namespace),
		client.MatchingLabels{
			labelInitInstance: taskName,
			labelInitTask:     strings.ToLower(taskType),
		},
	); err != nil {
		return fmt.Errorf("listing lifecycle task jobs for retention: %w", err)
	}

	policy := r.taskRetentionPolicyValue(superset, taskType)
	for i := range jobList.Items {
		job := &jobList.Items[i]
		if job.DeletionTimestamp != nil {
			continue
		}
		phase, terminal := lifecycleJobPhase(job)
		if !terminal {
			continue
		}
		if ShouldDeletePod(policy, phase) {
			if err := r.deleteLifecycleJob(ctx, job); err != nil {
				return fmt.Errorf("deleting retained lifecycle task job %s: %w", job.Name, err)
			}
		}
	}
	return nil
}

func lifecycleJobPhase(job *batchv1.Job) (corev1.PodPhase, bool) {
	if jobComplete(job) {
		return corev1.PodSucceeded, true
	}
	if jobFailed(job) {
		return corev1.PodFailed, true
	}
	return "", false
}

func jobComplete(job *batchv1.Job) bool {
	if job.Status.Succeeded > 0 {
		return true
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailed(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobConditionTransitionTime(job *batchv1.Job, conditionType batchv1.JobConditionType) *metav1.Time {
	for _, condition := range job.Status.Conditions {
		if condition.Type == conditionType && condition.Status == corev1.ConditionTrue {
			return &condition.LastTransitionTime
		}
	}
	return nil
}

func jobFailureMessage(job *batchv1.Job) string {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			if condition.Message != "" {
				return truncateFailureMessage(condition.Message)
			}
			if condition.Reason != "" {
				return truncateFailureMessage(condition.Reason)
			}
		}
	}
	return "Job failed"
}

// truncationMarker is appended to a failure message when it is truncated. It is
// counted against maxTerminationMessageLen so the returned string never exceeds
// that budget.
const truncationMarker = "..."

func truncateFailureMessage(msg string) string {
	if len(msg) > maxTerminationMessageLen {
		return msg[:maxTerminationMessageLen-len(truncationMarker)] + truncationMarker
	}
	return msg
}
