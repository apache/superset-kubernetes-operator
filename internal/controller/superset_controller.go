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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// SupersetReconciler reconciles a Superset object.
type SupersetReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
	Now      func() time.Time
}

// +kubebuilder:rbac:groups=superset.apache.org,resources=supersets,verbs=get;list;watch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebservers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryworkers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryworkers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetcelerybeats,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetcelerybeats/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryflowers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryflowers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebsocketservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebsocketservers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetmcpservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetmcpservers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetlifecycletasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetlifecycletasks/status,verbs=get
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=list;watch

func (r *SupersetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	superset := &supersetv1alpha1.Superset{}
	if err := r.Get(ctx, req.NamespacedName, superset); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle suspend.
	if superset.Spec.Suspend != nil && *superset.Spec.Suspend {
		log.Info("Reconciliation suspended", "name", superset.Name)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeSuspended,
			metav1.ConditionTrue, "Suspended", "Reconciliation is suspended", superset.Generation)
		superset.Status.Phase = phaseSuspended
		superset.Status.ObservedGeneration = superset.Generation
		return ctrl.Result{}, r.Status().Update(ctx, superset)
	}

	// Clear Suspended condition when not suspended.
	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeSuspended,
		metav1.ConditionFalse, "NotSuspended", "Reconciliation is not suspended", superset.Generation)

	log.Info("Reconciling Superset", "name", superset.Name)

	// Phase 1: Compute shared config checksum (per-component checksums are
	// derived from this combined with each component's rendered config).
	configChecksum := computeChecksum(struct {
		SecretKey           *string
		SecretKeyFrom       *corev1.SecretKeySelector
		Metastore           *supersetv1alpha1.MetastoreSpec
		Valkey              *supersetv1alpha1.ValkeySpec
		Config              *string
		SQLAEngineOptions   *supersetv1alpha1.SQLAlchemyEngineOptionsSpec
		WebServerGunicorn   *supersetv1alpha1.GunicornSpec
		CeleryWorkerProcess *supersetv1alpha1.CeleryWorkerProcessSpec
	}{
		superset.Spec.SecretKey, superset.Spec.SecretKeyFrom, superset.Spec.Metastore, superset.Spec.Valkey, superset.Spec.Config,
		superset.Spec.SQLAlchemyEngineOptions,
		gunicornSpecFrom(superset.Spec.WebServer),
		celerySpecFrom(superset.Spec.CeleryWorker),
	})

	// Phase 2: Reconcile shared resources.
	if err := r.reconcileServiceAccount(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile ServiceAccount: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling ServiceAccount: %w", err)
	}

	// Phase 2.5: Lifecycle tasks (migrate + init) via SupersetLifecycleTask child CRs.
	// Gates component deployment on lifecycle completion.
	topLevel := convertTopLevelSpec(&superset.Spec)
	saName := resolveServiceAccountName(superset)

	requeueAfter, lifecycleComplete, err := r.reconcileLifecycle(ctx, superset, configChecksum, topLevel, saName)
	if err != nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile Init: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling Init: %w", err)
	}
	if !lifecycleComplete {
		// Update status before returning.
		if statusErr := r.Status().Update(ctx, superset); statusErr != nil {
			return ctrl.Result{}, fmt.Errorf("updating status during init: %w", statusErr)
		}
		if requeueAfter < 0 {
			// Terminal failure — only a spec change (watch event) can recover.
			return ctrl.Result{}, nil
		}
		if requeueAfter > 0 {
			return ctrl.Result{RequeueAfter: requeueAfter}, nil
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Persist lifecycle completion status (lastCompletedChecksums) before
	// creating components. Component creation triggers watch events that cause
	// concurrent reconciles — if we defer this to Phase 5, a conflict there
	// loses the checksums and the next reconcile re-enters lifecycle.
	if statusErr := r.Status().Update(ctx, superset); statusErr != nil {
		return ctrl.Result{}, fmt.Errorf("updating status after lifecycle: %w", statusErr)
	}

	// Phase 3: Resolve and reconcile each component (table-driven).
	for _, desc := range componentDescriptors {
		if err := r.reconcileComponent(ctx, superset, desc, topLevel, configChecksum, saName); err != nil {
			r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile %s: %v", desc.componentType, err)
			return ctrl.Result{}, fmt.Errorf("reconciling %s: %w", desc.componentType, err)
		}
	}

	// Phase 3.5: Reconcile parent-owned web-server Service and maintenance return.
	maintenanceCleared, err := r.reconcileMaintenanceReturn(ctx, superset)
	if err != nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile maintenance return: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling maintenance return: %w", err)
	}
	if err := r.reconcileWebServerService(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile web-server Service: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling web-server Service: %w", err)
	}
	if !maintenanceCleared {
		if statusErr := r.Status().Update(ctx, superset); statusErr != nil {
			return ctrl.Result{}, fmt.Errorf("updating status during maintenance return: %w", statusErr)
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Phase 4: Reconcile networking, monitoring, network policies.
	if err := r.reconcileNetworking(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile Networking: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling Networking: %w", err)
	}

	if err := r.reconcileMonitoring(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile Monitoring: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling Monitoring: %w", err)
	}

	if err := r.reconcileNetworkPolicies(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile NetworkPolicies: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling NetworkPolicies: %w", err)
	}

	// Phase 5: Update aggregate status.
	if err := r.updateStatus(ctx, superset); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	// Phase 6: Schedule-based requeue for periodic lifecycle tasks.
	if requeue := r.nextScheduleRequeue(superset); requeue > 0 {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	return ctrl.Result{}, nil
}

// applyChildCR creates or updates a child CR with the resolved flat spec.
func (r *SupersetReconciler) applyChildCR(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	childName string,
	componentType naming.ComponentType,
	flat *resolution.FlatSpec,
	configChecksum, saName string,
	imageOverride *supersetv1alpha1.ImageOverrideSpec,
	newObj func() client.Object,
	applySpec func(client.Object, supersetv1alpha1.FlatComponentSpec, string),
) error {
	obj := newObj()
	obj.SetName(childName)
	obj.SetNamespace(superset.Namespace)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		if err := controllerutil.SetControllerReference(superset, obj, r.Scheme); err != nil {
			return err
		}
		obj.SetLabels(mergeLabels(obj.GetLabels(), map[string]string{
			naming.LabelKeyName:      naming.LabelValueApp,
			naming.LabelKeyComponent: string(componentType),
			naming.LabelKeyParent:    superset.Name,
		}))
		flatSpec := flatSpecFromResolution(flat, &superset.Spec.Image, imageOverride, saName)
		applySpec(obj, flatSpec, configChecksum)
		return nil
	})
	return err
}

// --- Conversion helpers: CRD types -> resolution engine types ---

// --- Shared resource reconciliation ---

func (r *SupersetReconciler) reconcileServiceAccount(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	saName := superset.Name
	if superset.Spec.ServiceAccount != nil && superset.Spec.ServiceAccount.Name != "" {
		saName = superset.Spec.ServiceAccount.Name
	}

	keepName := saName
	if !saCreateEnabled(superset.Spec.ServiceAccount) {
		keepName = ""
	}
	if err := r.deleteByLabels(ctx, superset.Namespace, parentLabels(superset.Name),
		func() client.ObjectList { return &corev1.ServiceAccountList{} }, keepName); err != nil {
		return err
	}

	if !saCreateEnabled(superset.Spec.ServiceAccount) {
		return nil
	}

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: superset.Namespace},
	}

	// Guard against adopting a pre-existing ServiceAccount not owned by this CR.
	existing := &corev1.ServiceAccount{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sa), existing); err == nil {
		if !isOwnedBy(existing, superset) {
			return fmt.Errorf("ServiceAccount %q already exists and is not owned by Superset %q; set serviceAccount.create=false to use a pre-existing ServiceAccount",
				saName, superset.Name)
		}
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		if err := controllerutil.SetControllerReference(superset, sa, r.Scheme); err != nil {
			return err
		}
		sa.Labels = mergeLabels(sa.Labels, parentLabels(superset.Name))
		sa.Annotations = nil
		if superset.Spec.ServiceAccount != nil {
			sa.Annotations = mergeAnnotations(nil, superset.Spec.ServiceAccount.Annotations)
		}
		return nil
	})
	return err
}

// isOwnedBy returns true if obj has a controller ownerReference pointing to owner.
func isOwnedBy(obj, owner client.Object) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == owner.GetUID() {
			return true
		}
	}
	return false
}

// --- Status aggregation ---

func (r *SupersetReconciler) updateStatus(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	superset.Status.ObservedGeneration = superset.Generation
	superset.Status.Version = superset.Spec.Image.Tag

	if superset.Status.Components == nil {
		superset.Status.Components = &supersetv1alpha1.ComponentStatusMap{}
	}

	allReady := true

	// Table-driven status aggregation.
	for _, desc := range componentDescriptors {
		isEnabled := desc.extract(&superset.Spec) != nil
		if isEnabled {
			childName := desc.childName(&superset.Spec, superset.Name)
			status := r.getChildStatus(ctx, superset.Namespace, childName, desc.kind)
			desc.setStatus(superset.Status.Components, status)
			if status != nil && !isReadyString(status.Ready) {
				allReady = false
			}
		} else {
			desc.setStatus(superset.Status.Components, nil)
		}
	}

	if !anyComponentEnabled(superset) {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionTrue, "NoComponentsEnabled", "No components are enabled", superset.Generation)
		superset.Status.Phase = phaseRunning
	} else if allReady {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionTrue, "AllComponentsReady", "All components are ready", superset.Generation)
		superset.Status.Phase = phaseRunning
	} else {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionFalse, "ComponentsNotReady", "One or more components are not ready", superset.Generation)
		superset.Status.Phase = phaseDegraded
	}

	return r.Status().Update(ctx, superset)
}

// getChildStatus reads a child CR's status using unstructured API.
func (r *SupersetReconciler) getChildStatus(ctx context.Context, namespace, childName, kind string) *supersetv1alpha1.ComponentRefStatus {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "superset.apache.org",
		Version: "v1alpha1",
		Kind:    kind,
	})

	err := r.Get(ctx, types.NamespacedName{Name: childName, Namespace: namespace}, obj)
	if err != nil {
		log := logf.FromContext(ctx)
		log.Info("child CR not found for status", "kind", kind, "name", childName, "error", err)
		return &supersetv1alpha1.ComponentRefStatus{
			Ready: "0/0",
			Ref:   kind + "/" + childName,
		}
	}

	// Read the status.ready field from the unstructured object.
	ready, _, _ := unstructured.NestedString(obj.Object, "status", "ready")

	return &supersetv1alpha1.ComponentRefStatus{
		Ready: ready,
		Ref:   kind + "/" + childName,
	}
}

// --- Utility functions ---

func resolveServiceAccountName(superset *supersetv1alpha1.Superset) string {
	if superset.Spec.ServiceAccount == nil {
		return superset.Name
	}
	if superset.Spec.ServiceAccount.Create != nil && !*superset.Spec.ServiceAccount.Create {
		if superset.Spec.ServiceAccount.Name != "" {
			return superset.Spec.ServiceAccount.Name
		}
		return ""
	}
	if superset.Spec.ServiceAccount.Name != "" {
		return superset.Spec.ServiceAccount.Name
	}
	return superset.Name
}

func anyComponentEnabled(superset *supersetv1alpha1.Superset) bool {
	return superset.Spec.WebServer != nil ||
		superset.Spec.CeleryWorker != nil ||
		superset.Spec.CeleryBeat != nil ||
		superset.Spec.CeleryFlower != nil ||
		superset.Spec.WebsocketServer != nil ||
		superset.Spec.McpServer != nil
}

func computeChecksum(obj any) string {
	data, err := json.Marshal(obj)
	if err != nil {
		data = fmt.Appendf(nil, "%v", obj)
	}
	h := sha256.New()
	h.Write(data)
	return fmt.Sprintf("sha256:%x", h.Sum(nil))
}

func gunicornSpecFrom(ws *supersetv1alpha1.WebServerComponentSpec) *supersetv1alpha1.GunicornSpec {
	if ws == nil {
		return nil
	}
	return ws.Gunicorn
}

func celerySpecFrom(cw *supersetv1alpha1.CeleryWorkerComponentSpec) *supersetv1alpha1.CeleryWorkerProcessSpec {
	if cw == nil {
		return nil
	}
	return cw.Celery
}

func isReadyString(ready string) bool {
	if ready == "" || ready == "0/0" {
		return false
	}
	// Parse "X/Y" and check X == Y and X > 0.
	var readyCount, desiredCount int
	if _, err := fmt.Sscanf(ready, "%d/%d", &readyCount, &desiredCount); err != nil {
		return false
	}
	return readyCount > 0 && readyCount == desiredCount
}

// pruneOrphans lists all resources matching the parent+component labels and deletes
// any whose name does not match keepName. If keepName is empty, all matches are deleted.
func (r *SupersetReconciler) pruneOrphans(
	ctx context.Context,
	ns, parentName string,
	componentType naming.ComponentType,
	newList func() client.ObjectList,
	keepName string,
) error {
	return r.deleteByLabels(ctx, ns, map[string]string{
		naming.LabelKeyParent:    parentName,
		naming.LabelKeyComponent: string(componentType),
	}, newList, keepName)
}

// deleteByLabels lists all resources matching the given labels and deletes any
// whose name does not match keepName. Pass empty keepName to delete all matches.
// Gracefully handles missing CRDs (returns nil for NoMatchError).
func (r *SupersetReconciler) deleteByLabels(
	ctx context.Context,
	ns string,
	labels map[string]string,
	newList func() client.ObjectList,
	keepName string,
) error {
	list := newList()
	if err := r.List(ctx, list,
		client.InNamespace(ns),
		client.MatchingLabels(labels),
	); err != nil {
		if meta.IsNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("listing resources by labels %v: %w", labels, err)
	}
	return deleteMatches(ctx, r.Client, list, keepName)
}

// saCreateEnabled returns true if the ServiceAccount spec says to create one.
func saCreateEnabled(sa *supersetv1alpha1.ServiceAccountSpec) bool {
	if sa == nil {
		return true
	}
	return sa.Create == nil || *sa.Create
}

// SetupWithManager sets up the controller with the Manager.
func (r *SupersetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&supersetv1alpha1.Superset{}).
		Owns(&supersetv1alpha1.SupersetLifecycleTask{}).
		Owns(&supersetv1alpha1.SupersetWebServer{}).
		Owns(&supersetv1alpha1.SupersetCeleryWorker{}).
		Owns(&supersetv1alpha1.SupersetCeleryBeat{}).
		Owns(&supersetv1alpha1.SupersetCeleryFlower{}).
		Owns(&supersetv1alpha1.SupersetWebsocketServer{}).
		Owns(&supersetv1alpha1.SupersetMcpServer{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Named("superset")

	// Only watch HTTPRoute if the Gateway API CRDs are installed.
	_, err := mgr.GetRESTMapper().RESTMapping(
		schema.GroupKind{Group: "gateway.networking.k8s.io", Kind: "HTTPRoute"},
	)
	if err == nil {
		b = b.Owns(&gatewayv1.HTTPRoute{})
	}

	return b.Complete(r)
}

// reconcileParentOwnedConfigMap creates or updates a ConfigMap owned by the
// parent Superset CR. The ConfigMap contains superset_config.py and is mounted
// by child component pods via a conventional name.
func reconcileParentOwnedConfigMap(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	parent *supersetv1alpha1.Superset,
	config string,
	resourceBaseName string,
) error {
	cmName := naming.ConfigMapName(resourceBaseName)

	if config == "" {
		cm := &corev1.ConfigMap{}
		cm.Name = cmName
		cm.Namespace = parent.Namespace
		return client.IgnoreNotFound(c.Delete(ctx, cm))
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: parent.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, c, cm, func() error {
		if err := controllerutil.SetControllerReference(parent, cm, scheme); err != nil {
			return err
		}
		cm.Data = map[string]string{
			"superset_config.py": config,
		}
		return nil
	})
	return err
}
