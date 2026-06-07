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
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

// SupersetReconciler reconciles a Superset object.
type SupersetReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
	Now      func() time.Time
}

// +kubebuilder:rbac:groups=superset.apache.org,resources=supersets,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

func (r *SupersetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	superset := &supersetv1alpha1.Superset{}
	if err := r.Get(ctx, req.NamespacedName, superset); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Capture the server-side status for diffing; all subsequent status writes
	// are guarded by equality.Semantic.DeepEqual to avoid reconcile churn.
	origSuperset := superset.DeepCopy()

	// Handle suspend.
	if superset.Spec.Suspend != nil && *superset.Spec.Suspend {
		log.V(1).Info("Reconciliation suspended", "name", superset.Name)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeSuspended,
			metav1.ConditionTrue, "Suspended", "Reconciliation is suspended", superset.Generation)
		superset.Status.Phase = phaseSuspended
		superset.Status.ObservedGeneration = superset.Generation
		return ctrl.Result{}, patchStatusIfChanged(ctx, r.Client, superset, origSuperset, origSuperset.Status, superset.Status)
	}

	// Clear Suspended condition when not suspended.
	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeSuspended,
		metav1.ConditionFalse, "NotSuspended", "Reconciliation is not suspended", superset.Generation)

	log.V(1).Info("Reconciling Superset", "name", superset.Name)

	// Phase 1: Compute shared config checksum (per-component checksums are
	// derived from this combined with each component's rendered config).
	configChecksum := computeChecksum(struct {
		SecretKey           *string
		SecretKeyFrom       *corev1.SecretKeySelector
		Metastore           *supersetv1alpha1.MetastoreSpec
		Valkey              *supersetv1alpha1.ValkeySpec
		Config              *string
		BootstrapScript     *string
		SQLAEngineOptions   *supersetv1alpha1.SQLAlchemyEngineOptionsSpec
		WebServerGunicorn   *supersetv1alpha1.GunicornSpec
		CeleryWorkerProcess *supersetv1alpha1.CeleryWorkerProcessSpec
	}{
		superset.Spec.SecretKey, superset.Spec.SecretKeyFrom, superset.Spec.Metastore, superset.Spec.Valkey, superset.Spec.Config, superset.Spec.BootstrapScript,
		superset.Spec.SQLAlchemyEngineOptions,
		gunicornSpecFrom(superset.Spec.WebServer),
		celerySpecFrom(superset.Spec.CeleryWorker),
	})
	log.V(2).Info("Computed config checksum", "name", superset.Name, "checksum", configChecksum)

	// Phase 2: Reconcile shared resources.
	if err := r.reconcileServiceAccount(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile ServiceAccount: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling ServiceAccount: %w", err)
	}

	// Phase 2.5: Lifecycle tasks (clone + migrate + rotate + init) via
	// parent-owned Jobs. Gates component deployment on lifecycle completion.
	topLevel := convertTopLevelSpec(&superset.Spec)
	saName := resolveServiceAccountName(superset)

	lifecycleRes, err := r.reconcileLifecycle(ctx, superset, configChecksum, topLevel, saName)
	if err != nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile lifecycle: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling lifecycle: %w", err)
	}
	if !lifecycleRes.Complete {
		// Update status before returning.
		log.V(1).Info("Lifecycle incomplete, requeueing", "name", superset.Name, "terminalFailure", lifecycleRes.TerminalFailure)
		r.updateLifecycleComponentStatus(ctx, superset, configChecksum)
		if statusErr := patchStatusIfChanged(ctx, r.Client, superset, origSuperset, origSuperset.Status, superset.Status); statusErr != nil {
			return ctrl.Result{}, fmt.Errorf("updating status during lifecycle: %w", statusErr)
		}
		if lifecycleRes.TerminalFailure {
			// Even on terminal failure, wake up for the next scheduled run
			// (e.g., cron-driven clone) if one is configured.
			if next := r.nextScheduleRequeue(superset); next > 0 {
				return ctrl.Result{RequeueAfter: next}, nil
			}
			// No schedule; only a spec change (watch event) can recover.
			return ctrl.Result{}, nil
		}
		if lifecycleRes.RequeueAfter > 0 {
			return ctrl.Result{RequeueAfter: lifecycleRes.RequeueAfter}, nil
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Persist lifecycle completion status (lastCompletedChecksums) before
	// creating components. Component creation triggers watch events that cause
	// concurrent reconciles — if we defer this to Phase 5, a conflict there
	// loses the checksums and the next reconcile re-enters lifecycle.
	if statusErr := patchStatusIfChanged(ctx, r.Client, superset, origSuperset, origSuperset.Status, superset.Status); statusErr != nil {
		return ctrl.Result{}, fmt.Errorf("updating status after lifecycle: %w", statusErr)
	}
	// Clear the supervised upgrade approval annotation only after status has
	// been persisted. Doing this earlier risks stranding the annotation if the
	// status patch fails — the next reconcile would see imageChanged=true and
	// re-gate AwaitingApproval despite the user already approving.
	if err := r.clearUpgradeApprovalAnnotation(ctx, superset); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.cleanupLifecycleTaskJobsByRetention(ctx, superset); err != nil {
		return ctrl.Result{}, fmt.Errorf("cleaning up lifecycle task jobs after lifecycle: %w", err)
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
	if maintenanceCleared {
		// Service selector has been switched to web-server; safe to clean up
		// maintenance resources now. Errors are non-fatal — GC will handle
		// them since they are parent-owned.
		_ = r.deleteMaintenanceResources(ctx, superset)
	}
	if !maintenanceCleared {
		log.V(1).Info("Maintenance active, requeueing before component reconcile", "name", superset.Name)
		r.updateLifecycleComponentStatus(ctx, superset, configChecksum)
		if statusErr := patchStatusIfChanged(ctx, r.Client, superset, origSuperset, origSuperset.Status, superset.Status); statusErr != nil {
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
	superset.Status.ConfigChecksum = configChecksum
	if err := r.updateStatus(ctx, superset, origSuperset); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	// Phase 6: Schedule-based requeue for periodic lifecycle tasks.
	if requeue := r.nextScheduleRequeue(superset); requeue > 0 {
		log.V(1).Info("Scheduling next reconcile", "name", superset.Name, "after", requeue.String())
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	return ctrl.Result{}, nil
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

	_, err := createOrUpdateWithRetry(ctx, r.Client, sa, func() error {
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
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Pod{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&policyv1.PodDisruptionBudget{}).
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

	// Only watch ServiceMonitor if the monitoring CRDs are installed. The
	// controller reconciles this resource unstructured to avoid a hard
	// Prometheus Operator API dependency.
	_, err = mgr.GetRESTMapper().RESTMapping(
		schema.GroupKind{Group: serviceMonitorGVK.Group, Kind: serviceMonitorGVK.Kind},
	)
	if err == nil {
		sm := &unstructured.Unstructured{}
		sm.SetGroupVersionKind(serviceMonitorGVK)
		b = b.Watches(sm, handler.EnqueueRequestForOwner(
			mgr.GetScheme(), mgr.GetRESTMapper(), &supersetv1alpha1.Superset{}, handler.OnlyControllerOwner(),
		))
	}

	return b.Complete(r)
}

// reconcileParentOwnedConfigMap creates or updates a ConfigMap owned by the
// parent Superset CR. The ConfigMap contains superset_config.py and is mounted
// by component pods via a conventional name. Pass labels matching the workload
// pods so the ConfigMap is discoverable via the same label selector.
func reconcileParentOwnedConfigMap(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	parent *supersetv1alpha1.Superset,
	config string,
	bootstrapScript string,
	resourceBaseName string,
	labels map[string]string,
) error {
	cmName := naming.ConfigMapName(resourceBaseName)

	if config == "" && bootstrapScript == "" {
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

	_, err := createOrUpdateWithRetry(ctx, c, cm, func() error {
		if err := controllerutil.SetControllerReference(parent, cm, scheme); err != nil {
			return err
		}
		cm.Labels = mergeLabels(cm.Labels, labels)
		data := map[string]string{}
		if config != "" {
			data["superset_config.py"] = config
		}
		if bootstrapScript != "" {
			data[bootstrapScriptKey] = bootstrapScript
		}
		cm.Data = data
		return nil
	})
	return err
}
