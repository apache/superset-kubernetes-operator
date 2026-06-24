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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// componentReconcilerConfig captures the component-specific parameters needed
// by reconcileComponentResources to orchestrate resource reconciliation.
type componentReconcilerConfig struct {
	componentName string
	deployConfig  DeploymentConfig
	defaultPort   int32 // 0 = no service
	hasScaling    bool
}

// reconcileComponentResources orchestrates the standard resource lifecycle for
// a Deployment-based component: ConfigMap -> Deployment -> Service -> Scaling.
func reconcileComponentResources(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	recorder events.EventRecorder,
	owner client.Object,
	spec *supersetv1alpha1.FlatComponentSpec,
	cfg componentReconcilerConfig,
	configChecksum string,
	service *supersetv1alpha1.ComponentServiceSpec,
	autoscaling *supersetv1alpha1.AutoscalingSpec,
	pdb *supersetv1alpha1.PDBSpec,
) error {
	resourceBaseName := common.ResourceBaseName(owner.GetName(), common.ComponentType(cfg.componentName))

	// Deployment.
	var checksums map[string]string
	if configChecksum != "" {
		checksums = buildChecksumAnnotations(configChecksum)
	}
	if err := reconcileComponentDeployment(ctx, c, scheme, owner, spec, cfg.deployConfig, checksums, cfg.componentName, resourceBaseName); err != nil {
		recorder.Eventf(owner, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile Deployment: %v", err)
		return fmt.Errorf("reconciling Deployment: %w", err)
	}

	// Service (if defaultPort > 0).
	if cfg.defaultPort > 0 {
		containerPort := resolveContainerPort(spec, cfg.defaultPort)
		if err := reconcileComponentService(ctx, c, scheme, owner, service, cfg.componentName, containerPort, cfg.defaultPort, resourceBaseName); err != nil {
			recorder.Eventf(owner, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile Service: %v", err)
			return fmt.Errorf("reconciling Service: %w", err)
		}
	}

	// Scaling (if hasScaling).
	if cfg.hasScaling {
		if err := reconcileScaling(ctx, c, scheme, owner, autoscaling, pdb, cfg.componentName, resourceBaseName); err != nil {
			recorder.Eventf(owner, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile Scaling: %v", err)
			return fmt.Errorf("reconciling Scaling: %w", err)
		}
	}

	return nil
}

// reconcileComponentDeployment creates or updates a Deployment from the flat component spec.
func reconcileComponentDeployment(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	spec *supersetv1alpha1.FlatComponentSpec,
	cfg DeploymentConfig,
	checksumAnnotations map[string]string,
	componentName string,
	resourceBaseName string,
) error {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceBaseName,
			Namespace: owner.GetNamespace(),
		},
	}

	labels := componentLabels(componentName, owner.GetName())

	op, err := createOrUpdateWithRetry(ctx, c, deploy, func() error {
		if err := controllerutil.SetControllerReference(owner, deploy, scheme); err != nil {
			return err
		}
		dt := safeDeploymentTemplatePtr(spec.DeploymentTemplate)
		// Operator-managed labels are passed last so they win on conflict and
		// cannot be overridden by user-supplied deployment labels.
		deploy.Labels = mergeLabels(dt.Labels, labels)
		deploy.Annotations = mergeAnnotations(deploy.Annotations, dt.Annotations)
		desiredSpec := buildDeploymentSpec(spec, cfg, checksumAnnotations, labels)
		preserveDeploymentReplicasWhenUnmanaged(&desiredSpec, deploy.Spec)
		deploy.Spec = desiredSpec
		return nil
	})
	if err != nil {
		return err
	}
	if op != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled component",
			"component", componentName, "name", resourceBaseName, "operation", op)
	} else {
		logf.FromContext(ctx).V(2).Info("Component unchanged",
			"component", componentName, "name", resourceBaseName, "operation", op)
	}
	return nil
}

// preserveDeploymentReplicasWhenUnmanaged keeps Deployment.spec.replicas out of
// the operator's control when buildDeploymentSpec decides replicas are
// unmanaged. The API server defaults an omitted Deployment replicas field to 1
// on update, so a full-spec update with Replicas=nil would otherwise fight the
// HPA and continuously scale down.
func preserveDeploymentReplicasWhenUnmanaged(
	desired *appsv1.DeploymentSpec,
	existing appsv1.DeploymentSpec,
) {
	if desired.Replicas != nil || existing.Replicas == nil {
		return
	}
	replicas := *existing.Replicas
	desired.Replicas = &replicas
}

// reconcileComponentService creates or updates a Service for the component.
func reconcileComponentService(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	svcSpec *supersetv1alpha1.ComponentServiceSpec,
	componentName string,
	containerPort int32,
	defaultPort int32,
	resourceBaseName string,
) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceBaseName,
			Namespace: owner.GetNamespace(),
		},
	}

	labels := componentLabels(componentName, owner.GetName())

	_, err := createOrUpdateWithRetry(ctx, c, svc, func() error {
		if err := controllerutil.SetControllerReference(owner, svc, scheme); err != nil {
			return err
		}
		desiredSpec := buildServiceSpec(svcSpec, labels, containerPort, defaultPort)
		preserveServiceAllocatedFields(&desiredSpec, svc.Spec)
		svc.Spec = desiredSpec
		var userLabels map[string]string
		var userAnnotations map[string]string
		if svcSpec != nil {
			userLabels = svcSpec.Labels
			userAnnotations = svcSpec.Annotations
		}
		svc.Labels = mergeLabels(userLabels, labels)
		svc.Annotations = mergeAnnotations(nil, userAnnotations)
		return nil
	})
	return err
}

func preserveServiceAllocatedFields(desired *corev1.ServiceSpec, existing corev1.ServiceSpec) {
	desired.ClusterIP = existing.ClusterIP
	desired.ClusterIPs = existing.ClusterIPs
	desired.IPFamilies = existing.IPFamilies
	desired.IPFamilyPolicy = existing.IPFamilyPolicy
	desired.HealthCheckNodePort = existing.HealthCheckNodePort

	if desired.Type == corev1.ServiceTypeExternalName {
		desired.ClusterIP = ""
		desired.ClusterIPs = nil
		desired.IPFamilies = nil
		desired.IPFamilyPolicy = nil
	}
}

// buildChecksumAnnotations builds pod annotations from checksum fields.
// Empty checksum values are omitted.
func buildChecksumAnnotations(configChecksum string) map[string]string {
	annotations := make(map[string]string)
	if configChecksum != "" {
		annotations[common.AnnotationConfigChecksum] = configChecksum
	}
	return annotations
}

// reconcileScaling reconciles both HPA and PDB for a component.
func reconcileScaling(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	autoscaling *supersetv1alpha1.AutoscalingSpec,
	pdb *supersetv1alpha1.PDBSpec,
	componentName string,
	resourceBaseName string,
) error {
	labels := componentLabels(componentName, owner.GetName())
	if err := reconcileHPA(ctx, c, scheme, owner, autoscaling, labels, resourceBaseName, owner.GetNamespace()); err != nil {
		return fmt.Errorf("reconciling HPA: %w", err)
	}
	if err := reconcilePDB(ctx, c, scheme, owner, pdb, labels, resourceBaseName, owner.GetNamespace()); err != nil {
		return fmt.Errorf("reconciling PDB: %w", err)
	}
	return nil
}

// deleteByLabels lists all resources matching the given labels and deletes any
// whose name does not match keepName. Pass empty keepName to delete all matches.
func deleteByLabels(
	ctx context.Context,
	c client.Client,
	ns string,
	labels map[string]string,
	newList func() client.ObjectList,
	keepName string,
) error {
	list := newList()
	if err := c.List(ctx, list,
		client.InNamespace(ns),
		client.MatchingLabels(labels),
	); err != nil {
		return err
	}
	return deleteMatches(ctx, c, list, keepName)
}

// deleteMatches deletes all items in the list whose name does not match keepName.
func deleteMatches(ctx context.Context, c client.Client, list client.ObjectList, keepName string) error {
	items, err := meta.ExtractList(list)
	if err != nil {
		return fmt.Errorf("extracting list items: %w", err)
	}
	for _, item := range items {
		obj := item.(client.Object)
		if obj.GetName() != keepName {
			if err := client.IgnoreNotFound(c.Delete(ctx, obj)); err != nil {
				return fmt.Errorf("deleting %s: %w", obj.GetName(), err)
			}
		}
	}
	return nil
}
