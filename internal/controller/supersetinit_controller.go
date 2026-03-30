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

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// SupersetInitReconciler reconciles a SupersetInit object.
// It manages the initialization lifecycle (database migrations, init commands)
// by running bare Pods instead of Deployments.
type SupersetInitReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetinits,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetinits/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *SupersetInitReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	initCR := &supersetv1alpha1.SupersetInit{}
	if err := r.Get(ctx, req.NamespacedName, initCR); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Reconciling SupersetInit", "name", initCR.Name)

	resourceBaseName := common.ResourceBaseName(initCR.Name, common.ComponentInit)

	// Reconcile the ConfigMap for superset_config.py.
	if err := reconcileChildConfigMap(ctx, r.Client, r.Scheme, initCR, initCR.Spec.Config, resourceBaseName); err != nil {
		r.Recorder.Eventf(initCR, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile ConfigMap: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling ConfigMap: %w", err)
	}

	// Run the init pod lifecycle.
	result, err := r.reconcileInitPod(ctx, initCR)
	if err != nil {
		r.Recorder.Eventf(initCR, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile init pod: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling init pod: %w", err)
	}

	// Update status.
	initCR.Status.ObservedGeneration = initCR.Generation
	if err := r.Status().Update(ctx, initCR); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return result, nil
}

// reconcileInitPod handles the init pod lifecycle state machine.
func (r *SupersetInitReconciler) reconcileInitPod(ctx context.Context, initCR *supersetv1alpha1.SupersetInit) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	resourceBaseName := common.ResourceBaseName(initCR.Name, common.ComponentInit)
	maxRetries := getInitMaxRetries(initCR)
	timeout := getInitTimeout(initCR)
	image := fmt.Sprintf("%s:%s", initCR.Spec.Image.Repository, initCR.Spec.Image.Tag)

	// If already complete or permanently failed, check for config changes.
	if initCR.Status.State == initStateComplete ||
		(initCR.Status.State == initStateFailed && initCR.Status.Attempts >= maxRetries) {
		if initCR.Spec.ConfigChecksum != "" && initCR.Status.ConfigChecksum != initCR.Spec.ConfigChecksum {
			if err := r.resetForConfigChange(ctx, log, initCR, resourceBaseName); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			return ctrl.Result{}, nil
		}
	}

	// Initialize status if empty.
	if initCR.Status.State == "" {
		initCR.Status.State = initStatePending
		initCR.Status.Image = image
	}

	// Look for an existing pod for this init task.
	existingPod, err := r.findInitPod(ctx, initCR, resourceBaseName)
	if err != nil {
		return ctrl.Result{}, err
	}

	if existingPod != nil {
		initCR.Status.PodName = existingPod.Name

		switch existingPod.Status.Phase {
		case corev1.PodSucceeded:
			log.Info("Init pod succeeded", "pod", existingPod.Name)
			now := metav1.Now()
			initCR.Status.State = initStateComplete
			initCR.Status.CompletedAt = &now
			if initCR.Status.StartedAt != nil {
				initCR.Status.Duration = now.Sub(initCR.Status.StartedAt.Time).Round(time.Second).String()
			}
			initCR.Status.Message = "Completed successfully"
			initCR.Status.ConfigChecksum = initCR.Spec.ConfigChecksum

			r.applyRetentionPolicy(ctx, initCR, existingPod)

			setCondition(&initCR.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
				metav1.ConditionTrue, "InitComplete", "Initialization completed successfully", initCR.Generation)

			return ctrl.Result{}, nil

		case corev1.PodFailed:
			log.Info("Init pod failed", "pod", existingPod.Name, "attempt", initCR.Status.Attempts)
			initCR.Status.Attempts++
			initCR.Status.Message = podFailureMessage(existingPod)

			if initCR.Status.Attempts >= maxRetries {
				initCR.Status.State = initStateFailed
				initCR.Status.ConfigChecksum = initCR.Spec.ConfigChecksum
				r.applyRetentionPolicy(ctx, initCR, existingPod)
				r.Recorder.Eventf(initCR, corev1.EventTypeWarning, "InitFailed",
					"Init failed after %d attempts: %s", initCR.Status.Attempts, initCR.Status.Message)
				setCondition(&initCR.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
					metav1.ConditionFalse, "InitFailed", initCR.Status.Message, initCR.Generation)
				return ctrl.Result{}, nil
			}

			// Not exhausted -- delete the failed pod before retry.
			if err := r.Delete(ctx, existingPod); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}

			backoff := calculateBackoff(initCR.Status.Attempts)
			initCR.Status.State = initStatePending
			r.Recorder.Eventf(initCR, corev1.EventTypeWarning, "InitRetry",
				"Init failed (attempt %d/%d), retrying in %s", initCR.Status.Attempts, maxRetries, backoff)
			setCondition(&initCR.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
				metav1.ConditionFalse, "InitRetrying", fmt.Sprintf("Retrying after attempt %d", initCR.Status.Attempts), initCR.Generation)
			return ctrl.Result{RequeueAfter: backoff}, nil

		case corev1.PodRunning, corev1.PodPending:
			initCR.Status.State = initStateRunning
			// Check timeout.
			if initCR.Status.StartedAt != nil {
				if time.Since(initCR.Status.StartedAt.Time) > timeout {
					log.Info("Init pod timed out", "timeout", timeout)
					initCR.Status.Message = fmt.Sprintf("Timed out after %s", timeout)
					initCR.Status.Attempts++
					if initCR.Status.Attempts >= maxRetries {
						initCR.Status.State = initStateFailed
						initCR.Status.ConfigChecksum = initCR.Spec.ConfigChecksum
						r.applyRetentionPolicy(ctx, initCR, existingPod)
						r.Recorder.Eventf(initCR, corev1.EventTypeWarning, "InitFailed",
							"Init timed out after %d attempts", initCR.Status.Attempts)
						setCondition(&initCR.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
							metav1.ConditionFalse, "InitTimedOut", initCR.Status.Message, initCR.Generation)
						return ctrl.Result{}, nil
					}
					if err := r.Delete(ctx, existingPod); client.IgnoreNotFound(err) != nil {
						return ctrl.Result{}, err
					}
					backoff := calculateBackoff(initCR.Status.Attempts)
					initCR.Status.State = initStatePending
					return ctrl.Result{RequeueAfter: backoff}, nil
				}
			}
			setCondition(&initCR.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
				metav1.ConditionFalse, "InitInProgress", "Initialization is in progress", initCR.Generation)
			return ctrl.Result{RequeueAfter: initRequeueInterval}, nil
		}

		return ctrl.Result{RequeueAfter: initRequeueInterval}, nil
	}

	// No existing pod found. Create one.
	log.Info("Creating init pod", "attempt", initCR.Status.Attempts+1)

	podSpec := buildInitPod(&initCR.Spec.FlatComponentSpec)
	pt := safePodTemplatePtr(initCR.Spec.PodTemplate)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: resourceBaseName + "-",
			Namespace:    initCR.Namespace,
			Labels: mergeLabels(pt.Labels, map[string]string{
				labelInitInstance: resourceBaseName,
				labelInitTask:     initTaskName,
			}),
			Annotations: mergeAnnotations(nil, pt.Annotations),
		},
		Spec: podSpec,
	}

	if err := controllerutil.SetControllerReference(initCR, pod, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting controller reference on init pod: %w", err)
	}

	if err := r.Create(ctx, pod); err != nil {
		return ctrl.Result{}, fmt.Errorf("creating init pod: %w", err)
	}

	now := metav1.Now()
	initCR.Status.State = initStateRunning
	initCR.Status.PodName = pod.Name
	initCR.Status.StartedAt = &now
	initCR.Status.Image = image
	initCR.Status.Message = ""

	r.Recorder.Eventf(initCR, corev1.EventTypeNormal, "InitStarted",
		"Started init pod: %s", pod.Name)

	setCondition(&initCR.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
		metav1.ConditionFalse, "InitInProgress", "Initialization is in progress", initCR.Generation)

	return ctrl.Result{RequeueAfter: initRequeueInterval}, nil
}

// resetForConfigChange deletes existing init pods and resets status to
// Pending so init re-runs with the new configuration.
func (r *SupersetInitReconciler) resetForConfigChange(ctx context.Context, log logr.Logger, initCR *supersetv1alpha1.SupersetInit, resourceBaseName string) error {
	log.Info("Config changed, resetting init to re-run", "oldChecksum", initCR.Status.ConfigChecksum, "newChecksum", initCR.Spec.ConfigChecksum)
	if err := r.deleteInitPods(ctx, initCR, resourceBaseName); err != nil {
		return err
	}
	initCR.Status.State = initStatePending
	initCR.Status.Attempts = 0
	initCR.Status.Message = "Config changed, re-running initialization"
	initCR.Status.CompletedAt = nil
	initCR.Status.StartedAt = nil
	initCR.Status.PodName = ""
	initCR.Status.Duration = ""
	initCR.Status.ConfigChecksum = ""
	r.Recorder.Event(initCR, corev1.EventTypeNormal, "ConfigChanged", "Config changed, re-running initialization")
	return nil
}

// findInitPod finds the most recent existing init pod for this SupersetInit CR.
func (r *SupersetInitReconciler) findInitPod(ctx context.Context, initCR *supersetv1alpha1.SupersetInit, resourceBaseName string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(initCR.Namespace),
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
func (r *SupersetInitReconciler) applyRetentionPolicy(ctx context.Context, initCR *supersetv1alpha1.SupersetInit, pod *corev1.Pod) {
	log := logf.FromContext(ctx)
	policy := getInitRetentionPolicy(initCR)

	if ShouldDeletePod(policy, pod.Status.Phase) {
		if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
			log.Error(err, "Failed to delete completed init pod", "pod", pod.Name)
		}
	}
}

// deleteInitPods deletes all init pods for the given SupersetInit CR.
// Used when resetting init state after a config change to ensure retained
// pods from a previous run don't get mistaken for the new run.
func (r *SupersetInitReconciler) deleteInitPods(ctx context.Context, initCR *supersetv1alpha1.SupersetInit, resourceBaseName string) error {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(initCR.Namespace),
		client.MatchingLabels{
			labelInitInstance: resourceBaseName,
			labelInitTask:     initTaskName,
		},
	); err != nil {
		return fmt.Errorf("listing init pods for cleanup: %w", err)
	}
	for i := range podList.Items {
		if err := r.Delete(ctx, &podList.Items[i]); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting init pod %s: %w", podList.Items[i].Name, err)
		}
	}
	return nil
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

func getInitMaxRetries(initCR *supersetv1alpha1.SupersetInit) int32 {
	if initCR.Spec.MaxRetries != nil {
		return *initCR.Spec.MaxRetries
	}
	return defaultMaxRetries
}

func getInitTimeout(initCR *supersetv1alpha1.SupersetInit) time.Duration {
	if initCR.Spec.Timeout != nil {
		return initCR.Spec.Timeout.Duration
	}
	return defaultInitTimeout
}

func getInitRetentionPolicy(initCR *supersetv1alpha1.SupersetInit) string {
	if initCR.Spec.PodRetention != nil && initCR.Spec.PodRetention.Policy != nil {
		return *initCR.Spec.PodRetention.Policy
	}
	return defaultRetentionPolicy
}

func (r *SupersetInitReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&supersetv1alpha1.SupersetInit{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.ConfigMap{}).
		Named("supersetinit").
		Complete(r)
}
