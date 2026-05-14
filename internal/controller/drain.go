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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

func isLifecycleDisabled(superset *supersetv1alpha1.Superset) bool {
	return superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Disabled != nil && *superset.Spec.Lifecycle.Disabled
}

func (r *SupersetReconciler) deleteTaskCR(ctx context.Context, name, namespace string) error {
	task := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, task); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return r.Delete(ctx, task)
}

// drainIfNeeded checks whether any enabled task requires drain and executes it.
// Complete=true means drain isn't needed or completed successfully; otherwise
// RequeueAfter indicates how long to wait before re-checking.
func (r *SupersetReconciler) drainIfNeeded(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	cloneEnabled, migrateEnabled, rotateEnabled, initEnabled bool,
) (lifecycleResult, error) {
	needsDrain := (cloneEnabled && r.taskRequiresDrain(superset, taskTypeClone)) ||
		(migrateEnabled && r.taskRequiresDrain(superset, taskTypeMigrate)) ||
		(rotateEnabled && r.taskRequiresDrain(superset, taskTypeRotate)) ||
		(initEnabled && r.taskRequiresDrain(superset, taskTypeInit))
	if !needsDrain {
		return lifecycleComplete(), nil
	}

	superset.Status.Lifecycle.Phase = lifecyclePhaseDraining
	superset.Status.Phase = phaseDraining
	drained, err := r.drainComponents(ctx, superset)
	if err != nil {
		return lifecycleResult{}, fmt.Errorf("draining components: %w", err)
	}
	if !drained {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionFalse, "Draining", "Scaling components to zero before lifecycle tasks", superset.Generation)
		return lifecycleWait(), nil
	}
	return lifecycleComplete(), nil
}

// drainComponents deletes all component child CRs, which cascades to their
// Deployments, Services, and HPAs via ownerReference garbage collection.
// Returns (drained, error) where drained=true means no component Deployments remain.
func (r *SupersetReconciler) drainComponents(ctx context.Context, superset *supersetv1alpha1.Superset) (bool, error) {
	log := logf.FromContext(ctx)

	// Delete child CRs for each component type (not task CRs).
	for _, desc := range componentDescriptors {
		if desc.extract(&superset.Spec) == nil {
			continue
		}
		childName := superset.Name
		childObj := desc.newChild()
		childObj.SetName(childName)
		childObj.SetNamespace(superset.Namespace)
		if err := r.Delete(ctx, childObj); err != nil {
			if !errors.IsNotFound(err) {
				return false, fmt.Errorf("deleting child CR %s/%s: %w", desc.componentType, childName, err)
			}
		} else {
			log.Info("Deleted child CR for drain", "component", desc.componentType)
		}
	}

	// Verify all component pods are terminated. Pods are the last resource in
	// the GC cascade (CR → Deployment → ReplicaSet → Pod), so their absence
	// confirms the full cascade is complete.
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(superset.Namespace),
		client.MatchingLabels{naming.LabelKeyParent: superset.Name},
	); err != nil {
		return false, fmt.Errorf("listing pods: %w", err)
	}

	componentPods := 0
	for i := range podList.Items {
		if podList.Items[i].Labels[naming.LabelKeyComponent] != string(naming.ComponentInit) {
			componentPods++
		}
	}
	if componentPods > 0 {
		log.Info("Waiting for component pods to terminate", "remaining", componentPods)
		return false, nil
	}

	log.Info("All components drained")
	return true, nil
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
