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

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

const (
	componentPhaseReady       = "Ready"
	componentPhaseProgressing = "Progressing"
	componentPhaseUnavailable = "Unavailable"
	componentPhaseDrained     = "Drained"

	componentResourceStatusPresent = "Present"
	componentResourceStatusMissing = "Missing"
)

// patchStatusIfChanged issues a status MergeFrom patch iff the two status
// values are not semantically equal. origObj is the deep copy of obj
// captured before mutation; origStatus and currentStatus are the compared
// status values (typically obj.Status before/after mutation).
//
// This avoids bumping resourceVersion on reconciles where no observable
// status field changed, cutting down on self-enqueued reconciles.
func patchStatusIfChanged(ctx context.Context, c client.Client, obj client.Object, origObj client.Object, origStatus, currentStatus any) error {
	if equality.Semantic.DeepEqual(origStatus, currentStatus) {
		return nil
	}
	return c.Status().Patch(ctx, obj, client.MergeFrom(origObj))
}

// setCondition sets a condition on a conditions slice, replacing any existing
// condition of the same type.
func setCondition(conditions *[]metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string, observedGeneration int64) {
	now := metav1.Now()
	for i, c := range *conditions {
		if c.Type == conditionType {
			if c.Status != status || c.Reason != reason || c.Message != message || c.ObservedGeneration != observedGeneration {
				transitionTime := c.LastTransitionTime
				if c.Status != status {
					transitionTime = now
				}
				(*conditions)[i] = metav1.Condition{
					Type:               conditionType,
					Status:             status,
					LastTransitionTime: transitionTime,
					Reason:             reason,
					Message:            message,
					ObservedGeneration: observedGeneration,
				}
			}
			return
		}
	}
	*conditions = append(*conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observedGeneration,
	})
}

// removeCondition removes a condition of the given type from the slice, if present.
func removeCondition(conditions *[]metav1.Condition, conditionType string) {
	for i, c := range *conditions {
		if c.Type == conditionType {
			*conditions = append((*conditions)[:i], (*conditions)[i+1:]...)
			return
		}
	}
}

func (r *SupersetReconciler) updateStatus(ctx context.Context, superset *supersetv1alpha1.Superset, origSuperset *supersetv1alpha1.Superset) error {
	superset.Status.ObservedGeneration = superset.Generation
	superset.Status.Version = superset.Spec.Image.Tag

	if superset.Status.Components == nil {
		superset.Status.Components = &supersetv1alpha1.ComponentStatusMap{}
	}

	summary := r.refreshComponentStatuses(ctx, superset, componentStatusOptions{})
	superset.Status.Ready = fmt.Sprintf("%d/%d", summary.ready, summary.desired)

	if !anyComponentEnabled(superset) {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionTrue, "NoComponentsEnabled", "No components are enabled", superset.Generation)
		superset.Status.Phase = phaseRunning
	} else if summary.allReady {
		completeRestoringLifecycle(superset)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionTrue, "AllComponentsReady", "All components are ready", superset.Generation)
		superset.Status.Phase = phaseRunning
	} else if isRestoringLifecycle(superset) {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionFalse, "ComponentsRestoring", "Components are being restored after lifecycle tasks", superset.Generation)
	} else {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionFalse, "ComponentsNotReady", "One or more components are not ready", superset.Generation)
		superset.Status.Phase = phaseDegraded
	}

	return patchStatusIfChanged(ctx, r.Client, superset, origSuperset, origSuperset.Status, superset.Status)
}

func (r *SupersetReconciler) updateLifecycleComponentStatus(ctx context.Context, superset *supersetv1alpha1.Superset, configChecksum string) {
	_ = ctx
	superset.Status.ObservedGeneration = superset.Generation
	superset.Status.Version = superset.Spec.Image.Tag
	superset.Status.ConfigChecksum = configChecksum

	if superset.Status.Components == nil {
		superset.Status.Components = &supersetv1alpha1.ComponentStatusMap{}
	}

	maintenanceActive := superset.Status.Lifecycle != nil && superset.Status.Lifecycle.MaintenanceActive
	summary := r.refreshComponentStatuses(ctx, superset, componentStatusOptions{
		lifecycleView:     true,
		maintenanceActive: maintenanceActive,
	})
	superset.Status.Ready = fmt.Sprintf("%d/%d", summary.ready, summary.desired)

	if anyComponentEnabled(superset) {
		reason := "LifecycleInProgress"
		message := "Lifecycle tasks are running; component status reflects observed workloads"
		if maintenanceActive {
			reason = "MaintenanceActive"
			message = "Web-server Service is routing to the maintenance page"
		}
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionFalse, reason, message, superset.Generation)
	}
}

func isRestoringLifecycle(superset *supersetv1alpha1.Superset) bool {
	return superset.Status.Lifecycle != nil &&
		superset.Status.Lifecycle.Phase == lifecyclePhaseRestoring
}

func completeRestoringLifecycle(superset *supersetv1alpha1.Superset) {
	if isRestoringLifecycle(superset) {
		superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
	}
}

type componentStatusOptions struct {
	lifecycleView     bool
	maintenanceActive bool
}

type componentStatusSummary struct {
	ready    int32
	desired  int32
	allReady bool
}

func (r *SupersetReconciler) refreshComponentStatuses(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	opts componentStatusOptions,
) componentStatusSummary {
	summary := componentStatusSummary{allReady: true}

	for _, desc := range componentDescriptors {
		isEnabled := desc.extract(&superset.Spec) != nil
		statusSlot := desc.statusAccessor(superset.Status.Components)
		if !isEnabled {
			*statusSlot = nil
			continue
		}

		status := r.getComponentStatus(ctx, superset, desc)
		if opts.lifecycleView && componentDeploymentMissing(status) && lifecycleStatusIndicatesDrain(superset) {
			status = drainedComponentStatus(superset, desc)
		}
		if opts.maintenanceActive && desc.componentType == naming.ComponentWebServer {
			status = webServerMaintenanceStatus(status)
		}

		*statusSlot = status
		if status != nil {
			summary.ready += status.ReadyReplicas
			summary.desired += status.Replicas
			if !isComponentReady(status) {
				summary.allReady = false
			}
		}
	}

	return summary
}

func componentDeploymentMissing(status *supersetv1alpha1.ComponentRefStatus) bool {
	if status == nil {
		return false
	}
	for _, resource := range status.Resources {
		if resource.Kind == "Deployment" && resource.Status == componentResourceStatusMissing {
			return true
		}
	}
	return false
}

func lifecycleStatusIndicatesDrain(superset *supersetv1alpha1.Superset) bool {
	return hasLifecycleConditionReason(superset, "Draining") ||
		hasLifecycleConditionReason(superset, "ComponentsDrained") ||
		superset.Status.Lifecycle != nil && superset.Status.Lifecycle.MaintenanceActive
}

func webServerMaintenanceStatus(status *supersetv1alpha1.ComponentRefStatus) *supersetv1alpha1.ComponentRefStatus {
	if status == nil || status.Replicas == 0 {
		return status
	}
	copied := status.DeepCopy()
	copied.Phase = componentPhaseProgressing
	copied.ReadyReplicas = 0
	copied.AvailableReplicas = 0
	copied.Message = "Web-server Service is routing to the maintenance page"
	return copied
}

func drainedComponentStatus(superset *supersetv1alpha1.Superset, desc *componentDescriptor) *supersetv1alpha1.ComponentRefStatus {
	resourceBaseName := desc.resourceBaseName(&superset.Spec, superset.Name)
	accessor := desc.extract(&superset.Spec)
	desired := desiredReplicasForStatus(superset, desc, accessor)
	resources := []supersetv1alpha1.ComponentResourceStatus{
		{
			Kind:   "Deployment",
			Name:   resourceBaseName,
			Status: componentResourceStatusMissing,
		},
	}
	return &supersetv1alpha1.ComponentRefStatus{
		Phase:     componentPhaseDrained,
		Resources: resources,
		Replicas:  desired,
		Message:   "Component reconciliation is paused while lifecycle tasks run",
	}
}

func (r *SupersetReconciler) getComponentStatus(ctx context.Context, superset *supersetv1alpha1.Superset, desc *componentDescriptor) *supersetv1alpha1.ComponentRefStatus {
	resourceBaseName := desc.resourceBaseName(&superset.Spec, superset.Name)
	accessor := desc.extract(&superset.Spec)
	cfg, _ := componentResourceConfig(desc.componentType)
	desired := desiredReplicasForStatus(superset, desc, accessor)
	resources := []supersetv1alpha1.ComponentResourceStatus{}

	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: superset.Namespace, Name: resourceBaseName}, deploy); err != nil {
		if !apierrors.IsNotFound(err) {
			log := logf.FromContext(ctx)
			log.Info("failed to read component Deployment for status", "component", desc.componentType, "name", resourceBaseName, "error", err)
		}
		resources = append(resources, componentResourceStatus("Deployment", resourceBaseName, false))
		resources = append(resources, r.expectedComponentResources(ctx, superset, desc, accessor, cfg, resourceBaseName)...)
		return &supersetv1alpha1.ComponentRefStatus{
			Phase:     "Pending",
			Resources: resources,
			Replicas:  desired,
			Message:   fmt.Sprintf("Deployment %s not found", resourceBaseName),
		}
	}
	resources = append(resources, componentResourceStatus("Deployment", resourceBaseName, true))
	resources = append(resources, r.expectedComponentResources(ctx, superset, desc, accessor, cfg, resourceBaseName)...)

	if deploy.Spec.Replicas != nil {
		desired = *deploy.Spec.Replicas
	} else if deploy.Status.Replicas > 0 {
		desired = deploy.Status.Replicas
	}
	configChecksum := deploy.Spec.Template.Annotations[naming.AnnotationConfigChecksum]
	phase, message := componentPhaseAndMessage(deploy, desired)

	return &supersetv1alpha1.ComponentRefStatus{
		Phase:             phase,
		Resources:         resources,
		Image:             deploymentMainImage(deploy),
		Replicas:          desired,
		ReadyReplicas:     deploy.Status.ReadyReplicas,
		UpdatedReplicas:   deploy.Status.UpdatedReplicas,
		AvailableReplicas: deploy.Status.AvailableReplicas,
		ConfigChecksum:    configChecksum,
		Message:           message,
	}
}

func (r *SupersetReconciler) expectedComponentResources(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	desc *componentDescriptor,
	accessor *componentAccessor,
	cfg componentReconcilerConfig,
	resourceBaseName string,
) []supersetv1alpha1.ComponentResourceStatus {
	resources := []supersetv1alpha1.ComponentResourceStatus{}
	if desc.hasPythonConfig {
		resources = append(resources, r.observedResourceStatus(ctx, superset.Namespace, "ConfigMap", naming.ConfigMapName(resourceBaseName), &corev1.ConfigMap{}))
	}
	if desc.componentType == naming.ComponentWebsocketServer && accessor != nil && accessor.websocketConfig != nil {
		resources = append(resources, r.observedResourceStatus(ctx, superset.Namespace, "ConfigMap", naming.ConfigMapName(resourceBaseName), &corev1.ConfigMap{}))
	}
	if componentHasService(desc, cfg) {
		resources = append(resources, r.observedResourceStatus(ctx, superset.Namespace, "Service", resourceBaseName, &corev1.Service{}))
	}
	if cfg.hasScaling {
		if effectiveAutoscalingForStatus(superset, accessor) != nil {
			resources = append(resources, r.observedResourceStatus(ctx, superset.Namespace, "HorizontalPodAutoscaler", resourceBaseName, &autoscalingv2.HorizontalPodAutoscaler{}))
		}
		if effectivePDBForStatus(superset, accessor) != nil {
			resources = append(resources, r.observedResourceStatus(ctx, superset.Namespace, "PodDisruptionBudget", resourceBaseName, &policyv1.PodDisruptionBudget{}))
		}
	}
	return resources
}

func (r *SupersetReconciler) observedResourceStatus(ctx context.Context, namespace, kind, name string, obj client.Object) supersetv1alpha1.ComponentResourceStatus {
	obj.SetName(name)
	obj.SetNamespace(namespace)
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, obj)
	return componentResourceStatus(kind, name, err == nil)
}

func componentResourceStatus(kind, name string, present bool) supersetv1alpha1.ComponentResourceStatus {
	status := componentResourceStatusMissing
	if present {
		status = componentResourceStatusPresent
	}
	return supersetv1alpha1.ComponentResourceStatus{
		Kind:   kind,
		Name:   name,
		Status: status,
	}
}

func componentHasService(desc *componentDescriptor, cfg componentReconcilerConfig) bool {
	return cfg.defaultPort > 0 || desc.componentType == naming.ComponentWebServer
}

func effectiveAutoscalingForStatus(superset *supersetv1alpha1.Superset, accessor *componentAccessor) *supersetv1alpha1.AutoscalingSpec {
	if accessor != nil && accessor.autoscaling != nil {
		return accessor.autoscaling
	}
	return superset.Spec.Autoscaling
}

func effectivePDBForStatus(superset *supersetv1alpha1.Superset, accessor *componentAccessor) *supersetv1alpha1.PDBSpec {
	if accessor != nil && accessor.pdb != nil {
		return accessor.pdb
	}
	return superset.Spec.PodDisruptionBudget
}

func desiredReplicasForStatus(superset *supersetv1alpha1.Superset, desc *componentDescriptor, accessor *componentAccessor) int32 {
	if desc.componentType == naming.ComponentCeleryBeat {
		return celeryBeatSingletonReplica
	}
	if autoscaling := effectiveAutoscalingForStatus(superset, accessor); autoscaling != nil {
		if autoscaling.MinReplicas != nil {
			return *autoscaling.MinReplicas
		}
		return 1
	}
	if accessor != nil && accessor.replicas != nil {
		return *accessor.replicas
	}
	if superset.Spec.Replicas != nil {
		return *superset.Spec.Replicas
	}
	return 1
}

func deploymentMainImage(deploy *appsv1.Deployment) string {
	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		return ""
	}
	for _, c := range deploy.Spec.Template.Spec.Containers {
		if c.Name == naming.Container {
			return c.Image
		}
	}
	return deploy.Spec.Template.Spec.Containers[0].Image
}

func componentPhaseAndMessage(deploy *appsv1.Deployment, desired int32) (string, string) {
	if desired == 0 {
		return componentPhaseReady, "Scaled to zero"
	}
	if deploy.Status.ReadyReplicas == desired &&
		deploy.Status.UpdatedReplicas >= desired &&
		deploy.Status.AvailableReplicas >= desired {
		return componentPhaseReady, ""
	}
	if deploy.Generation > deploy.Status.ObservedGeneration {
		return componentPhaseProgressing, "Deployment has not observed the latest generation"
	}
	for _, condition := range deploy.Status.Conditions {
		if condition.Type == appsv1.DeploymentProgressing &&
			condition.Status == corev1.ConditionFalse &&
			condition.Reason == "ProgressDeadlineExceeded" {
			return componentPhaseUnavailable, condition.Message
		}
	}
	if deploy.Status.ReadyReplicas > 0 || deploy.Status.UpdatedReplicas > 0 {
		return componentPhaseProgressing, fmt.Sprintf("%d of %d replicas are ready", deploy.Status.ReadyReplicas, desired)
	}
	return componentPhaseUnavailable, fmt.Sprintf("%d of %d replicas are ready", deploy.Status.ReadyReplicas, desired)
}

func anyComponentEnabled(superset *supersetv1alpha1.Superset) bool {
	return superset.Spec.WebServer != nil ||
		superset.Spec.CeleryWorker != nil ||
		superset.Spec.CeleryBeat != nil ||
		superset.Spec.CeleryFlower != nil ||
		superset.Spec.WebsocketServer != nil ||
		superset.Spec.McpServer != nil
}

func isComponentReady(status *supersetv1alpha1.ComponentRefStatus) bool {
	return status.Phase == componentPhaseReady
}
