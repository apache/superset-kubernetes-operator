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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// SupersetLifecycleTaskReconciler reconciles a SupersetLifecycleTask object.
// It manages the initialization lifecycle (database migrations, init commands)
// by running bare Pods instead of Deployments.
type SupersetLifecycleTaskReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
	// Now lets tests inject a deterministic clock for backoff timing.
	Now func() time.Time
}

func (r *SupersetLifecycleTaskReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetlifecycletasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetlifecycletasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update

func (r *SupersetLifecycleTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	taskCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := r.Get(ctx, req.NamespacedName, taskCR); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Capture the server-side status for diffing; only patch if something changed.
	origTaskCR := taskCR.DeepCopy()

	log.Info("Reconciling SupersetLifecycleTask", "name", taskCR.Name)

	// Run the init pod lifecycle.
	result, err := r.reconcileInitPod(ctx, taskCR)
	if err != nil {
		r.Recorder.Eventf(taskCR, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile init pod: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling init pod: %w", err)
	}

	// Update status only if something actually changed. ObservedGeneration
	// is a frequent reason for a diff but is itself a meaningful update.
	taskCR.Status.ObservedGeneration = taskCR.Generation
	if err := patchStatusIfChanged(ctx, r.Client, taskCR, origTaskCR, origTaskCR.Status, taskCR.Status); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return result, nil
}

// reconcileInitPod handles the init pod lifecycle state machine.
func (r *SupersetLifecycleTaskReconciler) reconcileInitPod(ctx context.Context, taskCR *supersetv1alpha1.SupersetLifecycleTask) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	resourceBaseName := taskCR.Name
	maxRetries := getTaskMaxRetries(taskCR)
	timeout := getTaskTimeout(taskCR)
	image := fmt.Sprintf("%s:%s", taskCR.Spec.Image.Repository, taskCR.Spec.Image.Tag)

	// Terminal states: the parent controller is responsible for deleting and
	// recreating the CR if a re-run is needed. The task controller never resets.
	// Apply retention policy here (state already persisted from a previous reconcile).
	if taskCR.Status.State == initStateComplete ||
		(taskCR.Status.State == initStateFailed && taskCR.Status.Attempts >= maxRetries) {
		existingPod, err := r.findInitPod(ctx, taskCR, resourceBaseName)
		if err != nil {
			return ctrl.Result{}, err
		}
		if existingPod != nil {
			r.applyRetentionPolicy(ctx, taskCR, existingPod)
		}
		return ctrl.Result{}, nil
	}

	// Initialize status if empty.
	if taskCR.Status.State == "" {
		taskCR.Status.State = initStatePending
		taskCR.Status.Image = image
	}

	// Honor backoff: if NextAttemptAt has been set from a previous failure
	// and hasn't passed yet, wait without creating a pod. This prevents the
	// Pod-delete owner watch from enqueuing a reconcile that bypasses the
	// exponential backoff.
	if taskCR.Status.NextAttemptAt != nil {
		if remaining := taskCR.Status.NextAttemptAt.Sub(r.now()); remaining > 0 {
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
		taskCR.Status.NextAttemptAt = nil
	}

	// Look for an existing pod for this init task.
	existingPod, err := r.findInitPod(ctx, taskCR, resourceBaseName)
	if err != nil {
		return ctrl.Result{}, err
	}

	if existingPod != nil {
		taskCR.Status.PodName = existingPod.Name

		// If the desired image changed (e.g., tag was corrected), delete the
		// stale pod so it gets recreated with the updated image.
		if taskCR.Status.Image != "" && taskCR.Status.Image != image {
			log.Info("Image changed, deleting stale pod", "old", taskCR.Status.Image, "new", image)
			if err := r.Delete(ctx, existingPod); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			taskCR.Status.State = initStatePending
			taskCR.Status.Image = image
			taskCR.Status.PodName = ""
			taskCR.Status.Message = "Image changed, re-running task"
			r.Recorder.Eventf(taskCR, nil, corev1.EventTypeNormal, "ImageChanged", "Reconcile",
				"Image changed from %s to %s, re-running task", taskCR.Status.Image, image)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}

		switch existingPod.Status.Phase {
		case corev1.PodSucceeded:
			log.Info("Init pod succeeded", "pod", existingPod.Name)
			now := metav1.Now()
			taskCR.Status.State = initStateComplete
			taskCR.Status.CompletedAt = &now
			if taskCR.Status.StartedAt != nil {
				taskCR.Status.Duration = now.Sub(taskCR.Status.StartedAt.Time).Round(time.Second).String()
			}
			taskCR.Status.Message = "Completed successfully"
			taskCR.Status.ConfigChecksum = taskCR.Spec.ConfigChecksum

			setCondition(&taskCR.Status.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
				metav1.ConditionTrue, "TaskComplete", "Task completed successfully", taskCR.Generation)

			return ctrl.Result{}, nil

		case corev1.PodFailed:
			log.Info("Init pod failed", "pod", existingPod.Name, "attempt", taskCR.Status.Attempts)
			taskCR.Status.Attempts++
			taskCR.Status.Message = podFailureMessage(existingPod)

			if taskCR.Status.Attempts >= maxRetries {
				taskCR.Status.State = initStateFailed
				taskCR.Status.ConfigChecksum = taskCR.Spec.ConfigChecksum
				r.Recorder.Eventf(taskCR, nil, corev1.EventTypeWarning, "TaskFailed", "Reconcile",
					"Task failed after %d attempts: %s", taskCR.Status.Attempts, taskCR.Status.Message)
				setCondition(&taskCR.Status.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
					metav1.ConditionFalse, "TaskFailed", taskCR.Status.Message, taskCR.Generation)
				return ctrl.Result{}, nil
			}

			// Not exhausted -- delete the failed pod before retry.
			backoff := calculateBackoff(taskCR.Status.Attempts)
			next := metav1.NewTime(r.now().Add(backoff))
			taskCR.Status.NextAttemptAt = &next

			if err := r.Delete(ctx, existingPod); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}

			taskCR.Status.State = initStatePending
			r.Recorder.Eventf(taskCR, nil, corev1.EventTypeWarning, "TaskRetry", "Reconcile",
				"Task failed (attempt %d/%d), retrying in %s", taskCR.Status.Attempts, maxRetries, backoff)
			setCondition(&taskCR.Status.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
				metav1.ConditionFalse, "TaskRetrying", fmt.Sprintf("Retrying after attempt %d", taskCR.Status.Attempts), taskCR.Generation)
			return ctrl.Result{RequeueAfter: backoff}, nil

		case corev1.PodRunning, corev1.PodPending:
			taskCR.Status.State = initStateRunning
			// Check timeout.
			if taskCR.Status.StartedAt != nil {
				if r.now().Sub(taskCR.Status.StartedAt.Time) > timeout {
					log.Info("Init pod timed out", "timeout", timeout)
					taskCR.Status.Message = fmt.Sprintf("Timed out after %s", timeout)
					taskCR.Status.Attempts++
					if taskCR.Status.Attempts >= maxRetries {
						taskCR.Status.State = initStateFailed
						taskCR.Status.ConfigChecksum = taskCR.Spec.ConfigChecksum
						r.Recorder.Eventf(taskCR, nil, corev1.EventTypeWarning, "TaskFailed", "Reconcile",
							"Task timed out after %d attempts", taskCR.Status.Attempts)
						setCondition(&taskCR.Status.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
							metav1.ConditionFalse, "TaskTimedOut", taskCR.Status.Message, taskCR.Generation)
						return ctrl.Result{}, nil
					}
					backoff := calculateBackoff(taskCR.Status.Attempts)
					next := metav1.NewTime(r.now().Add(backoff))
					taskCR.Status.NextAttemptAt = &next
					if err := r.Delete(ctx, existingPod); client.IgnoreNotFound(err) != nil {
						return ctrl.Result{}, err
					}
					taskCR.Status.State = initStatePending
					return ctrl.Result{RequeueAfter: backoff}, nil
				}
			}
			setCondition(&taskCR.Status.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
				metav1.ConditionFalse, "TaskInProgress", "Task is in progress", taskCR.Generation)
			return ctrl.Result{RequeueAfter: initRequeueInterval}, nil
		}

		return ctrl.Result{RequeueAfter: initRequeueInterval}, nil
	}

	// No existing pod found. Create one.
	log.Info("Creating init pod", "attempt", taskCR.Status.Attempts+1)

	podSpec := buildInitPod(&taskCR.Spec.FlatComponentSpec)
	pt := safePodTemplatePtr(taskCR.Spec.PodTemplate)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: resourceBaseName + "-",
			Namespace:    taskCR.Namespace,
			Labels: mergeLabels(pt.Labels, map[string]string{
				labelInitInstance: resourceBaseName,
				labelInitTask:     initTaskName,
			}),
			Annotations: mergeAnnotations(nil, pt.Annotations),
		},
		Spec: podSpec,
	}

	if err := controllerutil.SetControllerReference(taskCR, pod, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting controller reference on init pod: %w", err)
	}

	if err := r.Create(ctx, pod); err != nil {
		return ctrl.Result{}, fmt.Errorf("creating init pod: %w", err)
	}

	now := metav1.Now()
	taskCR.Status.State = initStateRunning
	taskCR.Status.PodName = pod.Name
	taskCR.Status.StartedAt = &now
	taskCR.Status.Image = image
	taskCR.Status.Message = ""

	r.Recorder.Eventf(taskCR, nil, corev1.EventTypeNormal, "TaskStarted", "Reconcile",
		"Started task pod: %s", pod.Name)

	setCondition(&taskCR.Status.Conditions, supersetv1alpha1.ConditionTypeTaskComplete,
		metav1.ConditionFalse, "TaskInProgress", "Task is in progress", taskCR.Generation)

	return ctrl.Result{RequeueAfter: initRequeueInterval}, nil
}

// findInitPod finds the most recent existing init pod for this SupersetLifecycleTask CR.
func (r *SupersetLifecycleTaskReconciler) findInitPod(ctx context.Context, taskCR *supersetv1alpha1.SupersetLifecycleTask, resourceBaseName string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(taskCR.Namespace),
		client.MatchingLabels{
			labelInitInstance: resourceBaseName,
			labelInitTask:     initTaskName,
		},
	); err != nil {
		return nil, fmt.Errorf("listing init pods: %w", err)
	}

	if len(podList.Items) == 0 {
		return nil, nil
	}

	// Return the most recent pod, ignoring pods that are being deleted.
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

// applyRetentionPolicy handles pod cleanup after task completion.
func (r *SupersetLifecycleTaskReconciler) applyRetentionPolicy(ctx context.Context, taskCR *supersetv1alpha1.SupersetLifecycleTask, pod *corev1.Pod) {
	log := logf.FromContext(ctx)
	policy := getTaskRetentionPolicy(taskCR)

	if ShouldDeletePod(policy, pod.Status.Phase) {
		if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
			log.Error(err, "Failed to delete completed init pod", "pod", pod.Name)
		}
	}
}

// buildInitPod builds a PodSpec from the flat component spec for an init pod.
func buildInitPod(spec *supersetv1alpha1.FlatComponentSpec) corev1.PodSpec {
	pt := safePodTemplatePtr(spec.PodTemplate)
	ct := safeContainerTemplatePtr(pt.Container)

	image := fmt.Sprintf("%s:%s", spec.Image.Repository, spec.Image.Tag)
	container := corev1.Container{
		Name:            common.Container,
		Image:           image,
		ImagePullPolicy: spec.Image.PullPolicy,
		Command:         ct.Command,
		Args:            ct.Args,
		Env:             ct.Env,
		EnvFrom:         ct.EnvFrom,
		VolumeMounts:    ct.VolumeMounts,
		SecurityContext: ct.SecurityContext,
	}
	if ct.Resources != nil {
		container.Resources = *ct.Resources
	}

	podSpec := corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyNever,
		Containers:                    []corev1.Container{container},
		Volumes:                       pt.Volumes,
		ImagePullSecrets:              spec.Image.PullSecrets,
		NodeSelector:                  pt.NodeSelector,
		Tolerations:                   pt.Tolerations,
		Affinity:                      pt.Affinity,
		TopologySpreadConstraints:     pt.TopologySpreadConstraints,
		HostAliases:                   pt.HostAliases,
		SecurityContext:               pt.PodSecurityContext,
		TerminationGracePeriodSeconds: pt.TerminationGracePeriodSeconds,
		RuntimeClassName:              pt.RuntimeClassName,
		ShareProcessNamespace:         pt.ShareProcessNamespace,
		EnableServiceLinks:            pt.EnableServiceLinks,
		DNSConfig:                     pt.DNSConfig,
		Resources:                     pt.Resources,
	}
	if pt.PriorityClassName != nil {
		podSpec.PriorityClassName = *pt.PriorityClassName
	}
	if spec.ServiceAccountName != "" {
		podSpec.ServiceAccountName = spec.ServiceAccountName
	}
	if pt.DNSPolicy != nil {
		podSpec.DNSPolicy = *pt.DNSPolicy
	}
	podSpec.Containers = append(podSpec.Containers, pt.Sidecars...)
	podSpec.InitContainers = pt.InitContainers

	return podSpec
}

// --- Helper functions for reading spec values from the init CR ---

func getTaskMaxRetries(taskCR *supersetv1alpha1.SupersetLifecycleTask) int32 {
	if taskCR.Spec.MaxRetries != nil {
		return *taskCR.Spec.MaxRetries
	}
	return defaultMaxRetries
}

func getTaskTimeout(taskCR *supersetv1alpha1.SupersetLifecycleTask) time.Duration {
	if taskCR.Spec.Timeout != nil {
		return taskCR.Spec.Timeout.Duration
	}
	return defaultInitTimeout
}

func getTaskRetentionPolicy(taskCR *supersetv1alpha1.SupersetLifecycleTask) string {
	if taskCR.Spec.PodRetention != nil && taskCR.Spec.PodRetention.Policy != nil {
		return *taskCR.Spec.PodRetention.Policy
	}
	return defaultRetentionPolicy
}

func (r *SupersetLifecycleTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&supersetv1alpha1.SupersetLifecycleTask{}).
		Owns(&corev1.Pod{}).
		Named("supersetlifecycletask").
		Complete(r)
}
