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

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

func isLifecycleDisabled(superset *supersetv1alpha1.Superset) bool {
	return superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Disabled != nil && *superset.Spec.Lifecycle.Disabled
}

// drainIfNeeded checks whether any pending task requires drain and executes it.
// Complete=true means drain isn't needed or completed successfully; otherwise
// RequeueAfter indicates how long to wait before re-checking.
func (r *SupersetReconciler) drainIfNeeded(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	configChecksum string,
	parentPhase string,
) (lifecycleResult, error) {
	if !r.lifecycleNeedsDrain(superset, configChecksum) {
		return lifecycleComplete(), nil
	}

	wasDraining := hasLifecycleConditionReason(superset, "Draining")
	drained, err := r.drainComponents(ctx, superset)
	if err != nil {
		return lifecycleResult{}, fmt.Errorf("draining components: %w", err)
	}
	if !drained {
		if !wasDraining {
			r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "DrainingStarted", "Lifecycle",
				"Draining component workloads before lifecycle tasks")
		}
		superset.Status.Lifecycle.Phase = lifecyclePhaseDraining
		superset.Status.Phase = parentPhase
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionFalse, "Draining", "Scaling components to zero before lifecycle tasks", superset.Generation)
		return lifecycleWait(), nil
	}
	if wasDraining {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "DrainingCompleted", "Lifecycle",
			"All component workloads drained; lifecycle tasks can proceed")
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionFalse, "ComponentsDrained", "Component workloads drained; lifecycle tasks can proceed", superset.Generation)
	}
	return lifecycleComplete(), nil
}

func (r *SupersetReconciler) lifecycleNeedsDrain(
	superset *supersetv1alpha1.Superset,
	configChecksum string,
) bool {
	if !hasDrainableComponents(superset) {
		return false
	}
	for _, task := range r.pendingLifecycleTasks(superset, configChecksum) {
		if r.taskRequiresDrain(superset, task) {
			return true
		}
	}
	return false
}

func (r *SupersetReconciler) taskNeedsRun(
	superset *supersetv1alpha1.Superset,
	taskType string,
	taskChecksum string,
) bool {
	taskRef := taskStatusForType(superset, taskType)
	if taskRef != nil {
		if taskRef.State == taskStateComplete && taskRef.CompletedChecksum == taskChecksum {
			return false
		}
		if r.taskTerminalFailedForChecksum(superset, taskType, taskChecksum) {
			return false
		}
	}
	if superset.Status.Lifecycle != nil && superset.Status.Lifecycle.LastCompletedChecksums != nil {
		return superset.Status.Lifecycle.LastCompletedChecksums[taskType] != taskChecksum
	}
	return true
}

func (r *SupersetReconciler) taskTerminalFailedForChecksum(
	superset *supersetv1alpha1.Superset,
	taskType string,
	taskChecksum string,
) bool {
	taskRef := taskStatusForType(superset, taskType)
	if taskRef == nil || taskRef.State != taskStateFailed || taskRef.CompletedChecksum != taskChecksum {
		return false
	}
	maxRetries := taskRef.MaxRetries
	if maxRetries == 0 {
		maxRetries = r.taskMaxRetriesValue(superset, taskType)
	}
	return taskRef.Attempts >= maxRetries
}

func taskStatusForType(superset *supersetv1alpha1.Superset, taskType string) *supersetv1alpha1.TaskRefStatus {
	if superset.Status.Lifecycle == nil {
		return nil
	}
	desc := lifecycleTaskDescriptorByType(taskType)
	if desc == nil {
		return nil
	}
	return *desc.TaskRef(superset.Status.Lifecycle)
}

func hasDrainableComponents(superset *supersetv1alpha1.Superset) bool {
	for _, desc := range componentDescriptors {
		accessor := desc.extract(&superset.Spec)
		if accessor == nil {
			continue
		}
		if desiredReplicasForStatus(superset, desc, accessor) > 0 {
			return true
		}
	}
	return false
}

// drainComponents deletes component workloads and traffic resources directly.
// Returns (drained, error) where drained=true means no component Deployments remain.
func (r *SupersetReconciler) drainComponents(ctx context.Context, superset *supersetv1alpha1.Superset) (bool, error) {
	log := logf.FromContext(ctx)

	for _, desc := range componentDescriptors {
		if desc.extract(&superset.Spec) == nil {
			continue
		}
		resourceBaseName := naming.ResourceBaseName(superset.Name, desc.componentType)
		deleteNamed := func(obj client.Object) error {
			obj.SetName(resourceBaseName)
			obj.SetNamespace(superset.Namespace)
			return client.IgnoreNotFound(r.Delete(ctx, obj))
		}
		if err := deleteNamed(&appsv1.Deployment{}); err != nil {
			return false, fmt.Errorf("deleting Deployment for drain %s: %w", desc.componentType, err)
		}
		if err := deleteNamed(&autoscalingv2.HorizontalPodAutoscaler{}); err != nil {
			return false, fmt.Errorf("deleting HPA for drain %s: %w", desc.componentType, err)
		}
		if err := deleteNamed(&policyv1.PodDisruptionBudget{}); err != nil {
			return false, fmt.Errorf("deleting PDB for drain %s: %w", desc.componentType, err)
		}
		if desc.componentType != naming.ComponentWebServer {
			if err := deleteNamed(&corev1.Service{}); err != nil {
				return false, fmt.Errorf("deleting Service for drain %s: %w", desc.componentType, err)
			}
		}
		log.Info("Deleted component resources for drain", "component", desc.componentType)
	}

	// Verify all component pods are terminated. Pods are the last resource in
	// the Deployment cascade, so their absence confirms the workload is gone.
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(superset.Namespace),
		client.MatchingLabels{naming.LabelKeyParent: superset.Name},
	); err != nil {
		return false, fmt.Errorf("listing pods: %w", err)
	}

	componentPods := 0
	for i := range podList.Items {
		component := podList.Items[i].Labels[naming.LabelKeyComponent]
		if component != string(naming.ComponentInit) && component != string(naming.ComponentMaintenancePage) {
			componentPods++
		}
	}
	if componentPods > 0 {
		log.V(1).Info("Waiting for component pods to terminate", "remaining", componentPods)
		return false, nil
	}

	log.Info("All components drained")
	return true, nil
}

func hasLifecycleConditionReason(superset *supersetv1alpha1.Superset, reason string) bool {
	for _, condition := range superset.Status.Conditions {
		if condition.Type == supersetv1alpha1.ConditionTypeLifecycleComplete && condition.Reason == reason {
			return true
		}
	}
	return false
}

func resolveLifecycleImage(parentImage *supersetv1alpha1.ImageSpec, override *supersetv1alpha1.ImageOverrideSpec) string {
	repo := parentImage.Repository
	tag := parentImage.Tag
	if override != nil {
		if override.Repository != nil {
			repo = *override.Repository
		}
		if override.Tag != nil {
			tag = *override.Tag
		}
	}
	return ImageRef(repo, tag)
}

func tagFromImageRef(ref string) string {
	if idx := strings.LastIndex(ref, ":"); idx != -1 {
		return ref[idx+1:]
	}
	return ref
}

func nowPtr() *metav1.Time {
	now := metav1.Now()
	return &now
}
