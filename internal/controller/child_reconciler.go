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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// ChildCR is implemented by all child CRD types for generic reconciliation.
type ChildCR interface {
	client.Object
	GetFlatSpec() *supersetv1alpha1.FlatComponentSpec
	GetConfig() string
	GetConfigChecksum() string
	GetService() *supersetv1alpha1.ComponentServiceSpec
	GetAutoscaling() *supersetv1alpha1.AutoscalingSpec
	GetPDB() *supersetv1alpha1.PDBSpec
	GetComponentStatus() *supersetv1alpha1.ChildComponentStatus
}

// ChildReconciler is a generic reconciler for all child CRDs.
type ChildReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Config   childReconcilerConfig
	NewObj   func() ChildCR
}

// Reconcile handles the reconciliation loop for a child CRD.
func (r *ChildReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	obj := r.NewObj()
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Reconciling "+r.Config.componentName, "name", obj.GetName())

	if err := reconcileChildResources(ctx, r.Client, r.Scheme, r.Recorder, obj,
		obj.GetFlatSpec(), r.Config,
		obj.GetConfig(), obj.GetConfigChecksum(),
		obj.GetService(), obj.GetAutoscaling(), obj.GetPDB(),
	); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, updateChildStatus(ctx, r.Client, r.Client, obj, obj.GetComponentStatus(), obj.GetGeneration(),
		common.ResourceBaseName(obj.GetName(), common.ComponentType(r.Config.componentName)))
}

// SetupWithManager registers the controller with the manager.
func (r *ChildReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(r.NewObj()).
		Owns(&appsv1.Deployment{})

	if r.Config.hasConfig {
		b = b.Owns(&corev1.ConfigMap{})
	}
	if r.Config.defaultPort > 0 {
		b = b.Owns(&corev1.Service{})
	}
	if r.Config.hasScaling {
		b = b.Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
			Owns(&policyv1.PodDisruptionBudget{})
	}

	return b.Named(r.Config.componentName).Complete(r)
}

// childReconcilerConfig captures the component-specific parameters needed
// by reconcileChildResources to orchestrate sub-resource reconciliation.
type childReconcilerConfig struct {
	componentName string
	deployConfig  DeploymentConfig
	defaultPort   int32 // 0 = no service
	hasConfig     bool
	hasScaling    bool
}

// reconcileChildResources orchestrates the standard sub-resource lifecycle for
// a Deployment-based child controller: ConfigMap -> Deployment -> Service -> Scaling.
func reconcileChildResources(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	owner client.Object,
	spec *supersetv1alpha1.FlatComponentSpec,
	cfg childReconcilerConfig,
	config string,
	configChecksum string,
	service *supersetv1alpha1.ComponentServiceSpec,
	autoscaling *supersetv1alpha1.AutoscalingSpec,
	pdb *supersetv1alpha1.PDBSpec,
) error {
	resourceBaseName := common.ResourceBaseName(owner.GetName(), common.ComponentType(cfg.componentName))

	// ConfigMap (if hasConfig).
	if cfg.hasConfig {
		if err := reconcileChildConfigMap(ctx, c, scheme, owner, config, resourceBaseName); err != nil {
			recorder.Eventf(owner, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile ConfigMap: %v", err)
			return fmt.Errorf("reconciling ConfigMap: %w", err)
		}
	}

	// Deployment.
	var checksums map[string]string
	if cfg.hasConfig {
		checksums = buildChecksumAnnotations(configChecksum)
	}
	if err := reconcileChildDeployment(ctx, c, scheme, owner, spec, cfg.deployConfig, checksums, cfg.componentName, resourceBaseName); err != nil {
		recorder.Eventf(owner, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile Deployment: %v", err)
		return fmt.Errorf("reconciling Deployment: %w", err)
	}

	// Service (if defaultPort > 0).
	if cfg.defaultPort > 0 {
		containerPort := resolveContainerPort(spec, cfg.defaultPort)
		if err := reconcileChildService(ctx, c, scheme, owner, service, cfg.componentName, containerPort, cfg.defaultPort, resourceBaseName); err != nil {
			recorder.Eventf(owner, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile Service: %v", err)
			return fmt.Errorf("reconciling Service: %w", err)
		}
	}

	// Scaling (if hasScaling).
	if cfg.hasScaling {
		if err := reconcileScaling(ctx, c, scheme, owner, autoscaling, pdb, cfg.componentName, resourceBaseName); err != nil {
			recorder.Eventf(owner, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile Scaling: %v", err)
			return fmt.Errorf("reconciling Scaling: %w", err)
		}
	}

	return nil
}

// reconcileChildConfigMap creates, updates, or deletes a ConfigMap containing superset_config.py.
// When config is empty, any existing ConfigMap is deleted.
func reconcileChildConfigMap(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	config string,
	resourceBaseName string,
) error {
	cmName := common.ConfigMapName(resourceBaseName)
	ns := owner.GetNamespace()

	if config == "" {
		cm := &corev1.ConfigMap{}
		cm.Name = cmName
		cm.Namespace = ns
		return client.IgnoreNotFound(c.Delete(ctx, cm))
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: ns,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, c, cm, func() error {
		if err := controllerutil.SetControllerReference(owner, cm, scheme); err != nil {
			return err
		}
		cm.Data = map[string]string{
			"superset_config.py": config,
		}
		return nil
	})
	return err
}

// reconcileChildDeployment creates or updates a Deployment from the flat component spec.
func reconcileChildDeployment(
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

	_, err := controllerutil.CreateOrUpdate(ctx, c, deploy, func() error {
		if err := controllerutil.SetControllerReference(owner, deploy, scheme); err != nil {
			return err
		}
		deploy.Spec = buildDeploymentSpec(spec, cfg, checksumAnnotations, labels)
		return nil
	})
	return err
}

// reconcileChildService creates or updates a Service for the component.
func reconcileChildService(
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

	_, err := controllerutil.CreateOrUpdate(ctx, c, svc, func() error {
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

// updateChildStatus reads the Deployment and updates the ChildComponentStatus on the owner.
func updateChildStatus(
	ctx context.Context,
	c client.Client,
	statusClient client.StatusClient,
	owner client.Object,
	status *supersetv1alpha1.ChildComponentStatus,
	generation int64,
	resourceBaseName string,
) error {
	deploy := &appsv1.Deployment{}
	if err := c.Get(ctx, types.NamespacedName{Name: resourceBaseName, Namespace: owner.GetNamespace()}, deploy); err != nil {
		return err
	}

	updateComponentStatusFromDeployment(status, deploy, generation)
	return statusClient.Status().Update(ctx, owner)
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
