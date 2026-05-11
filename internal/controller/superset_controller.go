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
	"strings"
	"time"

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
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
	"github.com/apache/superset-kubernetes-operator/internal/schedule"
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

	// Phase 3: Resolve and reconcile each component (table-driven).
	for _, desc := range componentDescriptors {
		if err := r.reconcileComponent(ctx, superset, desc, topLevel, configChecksum, saName); err != nil {
			r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", "Failed to reconcile %s: %v", desc.componentType, err)
			return ctrl.Result{}, fmt.Errorf("reconciling %s: %w", desc.componentType, err)
		}
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

func convertTopLevelSpec(spec *supersetv1alpha1.SupersetSpec) *resolution.SharedInput {
	return &resolution.SharedInput{
		Replicas:            spec.Replicas,
		DeploymentTemplate:  spec.DeploymentTemplate,
		PodTemplate:         spec.PodTemplate,
		Autoscaling:         spec.Autoscaling,
		PodDisruptionBudget: spec.PodDisruptionBudget,
	}
}

// buildInitCommand constructs the init shell command from InitTaskSpec fields.
func buildInitCommand(init *supersetv1alpha1.InitTaskSpec) []string {
	script := "superset init"

	if init != nil && init.AdminUser != nil {
		script += ` && (superset fab create-admin` +
			` --username "$SUPERSET_OPERATOR__ADMIN_USERNAME"` +
			` --password "$SUPERSET_OPERATOR__ADMIN_PASSWORD"` +
			` --firstname "$SUPERSET_OPERATOR__ADMIN_FIRST_NAME"` +
			` --lastname "$SUPERSET_OPERATOR__ADMIN_LAST_NAME"` +
			` --email "$SUPERSET_OPERATOR__ADMIN_EMAIL"` +
			` || true)`
	}

	if init != nil && init.LoadExamples != nil && *init.LoadExamples {
		script += " && superset load-examples"
	}

	return []string{"/bin/sh", "-c", script}
}

func convertTaskComponent(lifecycle *supersetv1alpha1.LifecycleSpec, command []string) *resolution.ComponentInput {
	var pt *supersetv1alpha1.PodTemplate
	if lifecycle != nil {
		pt = lifecycle.PodTemplate
	}

	var ct *supersetv1alpha1.ContainerTemplate
	if pt != nil && pt.Container != nil {
		copied := *pt.Container
		ct = &copied
	} else {
		ct = &supersetv1alpha1.ContainerTemplate{}
	}
	ct.Command = command

	if pt != nil {
		copied := *pt
		copied.Container = ct
		pt = &copied
	} else {
		pt = &supersetv1alpha1.PodTemplate{Container: ct}
	}

	return &resolution.ComponentInput{
		SharedInput: resolution.SharedInput{
			PodTemplate: pt,
		},
	}
}

const (
	taskTypeMigrate = "Migrate"
	taskTypeInit    = "Init"
	taskTypeClone   = "Clone"

	suffixMigrate = "-migrate"
	suffixInit    = "-init"
	suffixClone   = "-clone"

	upgradeModeAutomatic  = "Automatic"
	upgradeModeSupervsied = "Supervised"

	lifecyclePhaseIdle             = "Idle"
	lifecyclePhaseCloning          = "Cloning"
	lifecyclePhaseDraining         = "Draining"
	lifecyclePhaseMigrating        = "Migrating"
	lifecyclePhaseInitializing     = "Initializing"
	lifecyclePhaseComplete         = "Complete"
	lifecyclePhaseBlocked          = "Blocked"
	lifecyclePhaseAwaitingApproval = "AwaitingApproval"

	annotationApproveUpgrade = "superset.apache.org/approve-upgrade"

	dbTypePostgresql = "PostgreSQL"
	dbTypeMySQL      = "MySQL"

	defaultImageTag = "latest"

	phaseUpgrading        = "Upgrading"
	phaseDraining         = "Draining"
	phaseBlocked          = "Blocked"
	phaseAwaitingApproval = "AwaitingApproval"
)

// reconcileLifecycle orchestrates the lifecycle tasks (clone + migrate + init) and gates
// component deployment. Returns (requeueAfter, lifecycleComplete, error).
func (r *SupersetReconciler) reconcileLifecycle(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	configChecksum string,
	topLevel *resolution.SharedInput,
	saName string,
) (time.Duration, bool, error) {

	// If lifecycle is disabled, prune orphans and mark complete.
	if isLifecycleDisabled(superset) {
		if err := r.pruneOrphans(ctx, superset.Namespace, superset.Name,
			naming.ComponentInit,
			func() client.ObjectList { return &supersetv1alpha1.SupersetLifecycleTaskList{} },
			"",
		); err != nil {
			return 0, false, fmt.Errorf("pruning orphaned task CRs: %w", err)
		}
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionTrue, "LifecycleDisabled", "Lifecycle tasks are disabled", superset.Generation)
		superset.Status.Lifecycle = nil
		return 0, true, nil
	}

	// Ensure lifecycle status exists.
	if superset.Status.Lifecycle == nil {
		superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{}
	}

	// Resolve the current lifecycle image.
	var imageOverride *supersetv1alpha1.ImageOverrideSpec
	if superset.Spec.Lifecycle != nil {
		imageOverride = superset.Spec.Lifecycle.Image
	}
	currentImage := resolveLifecycleImage(&superset.Spec.Image, imageOverride)
	lastImage := superset.Status.LastLifecycleImage
	imageChanged := lastImage == "" || currentImage != lastImage

	// Check upgrade gates (version comparison, downgrade blocking, supervised approval).
	if gateResult, gated := r.checkUpgradeGates(ctx, superset, imageChanged, lastImage, currentImage); gated {
		return gateResult, false, nil
	}

	// Determine which tasks are enabled and prune orphans for disabled ones.
	cloneEnabled := r.isTaskEnabled(superset, taskTypeClone)
	migrateEnabled := r.isTaskEnabled(superset, taskTypeMigrate)
	initEnabled := r.isTaskEnabled(superset, taskTypeInit)

	if err := r.pruneDisabledTasks(ctx, superset, cloneEnabled, migrateEnabled, initEnabled); err != nil {
		return 0, false, err
	}

	// If no tasks are enabled, lifecycle is complete.
	if !cloneEnabled && !migrateEnabled && !initEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionTrue, "LifecycleComplete", "Lifecycle tasks completed successfully", superset.Generation)
		return 0, true, nil
	}

	// Drain components if any enabled task requires it.
	if requeueAfter, drained, err := r.drainIfNeeded(ctx, superset, cloneEnabled, migrateEnabled, initEnabled); err != nil {
		return 0, false, err
	} else if !drained {
		return requeueAfter, false, nil
	}

	// Orchestrate lifecycle pipeline: clone → migrate → init.
	// Each task receives an incoming checksum, adds its own inputs, and produces
	// a task checksum stored on completion. The next task's incoming checksum is
	// the previous task's completed checksum — so upstream execution automatically
	// invalidates downstream tasks (the chain propagates changes forward).
	//
	// The pipeline anchor is the parentUID (stable, scoped to this CR).
	// Each task adds only its own relevant trigger inputs:
	//   Clone: trigger, source config, excludes
	//   Migrate: image (version)
	//   Init: configChecksum (rendered Python config)
	incomingChecksum := string(superset.UID)

	if cloneEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseCloning

		cloneCmd := r.buildCloneCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeClone, cloneCmd, r.cloneInputs(superset))
		requeueAfter, complete, err := r.reconcileLifecycleTask(ctx, superset, taskTypeClone, suffixClone, cloneCmd, taskChecksum, configChecksum, topLevel, saName)
		if err != nil {
			return 0, false, fmt.Errorf("reconciling clone task: %w", err)
		}
		if !complete {
			return requeueAfter, false, nil
		}
		incomingChecksum = r.getTaskStatusChecksum(ctx, superset, suffixClone)
	}

	if migrateEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseMigrating
		if imageChanged {
			superset.Status.Phase = phaseUpgrading
		} else {
			superset.Status.Phase = phaseInitializing
		}

		migrateCmd := defaultMigrateCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeMigrate, migrateCmd, r.migrateInputs(superset))
		requeueAfter, complete, err := r.reconcileLifecycleTask(ctx, superset, taskTypeMigrate, suffixMigrate, migrateCmd, taskChecksum, configChecksum, topLevel, saName)
		if err != nil {
			return 0, false, fmt.Errorf("reconciling migrate task: %w", err)
		}
		if !complete {
			return requeueAfter, false, nil
		}
		incomingChecksum = r.getTaskStatusChecksum(ctx, superset, suffixMigrate)
	}

	if initEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseInitializing
		if superset.Status.Phase != phaseUpgrading {
			superset.Status.Phase = phaseInitializing
		}

		initCmd := defaultInitCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeInit, initCmd, r.initInputs(superset, configChecksum))
		requeueAfter, complete, err := r.reconcileLifecycleTask(ctx, superset, taskTypeInit, suffixInit, initCmd, taskChecksum, configChecksum, topLevel, saName)
		if err != nil {
			return 0, false, fmt.Errorf("reconciling init task: %w", err)
		}
		if !complete {
			return requeueAfter, false, nil
		}
	}

	// All tasks complete. Update lastLifecycleImage and clear upgrade context.
	superset.Status.LastLifecycleImage = currentImage
	superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
	superset.Status.Lifecycle.Upgrade = nil

	// Clear approval annotation if it was set.
	if annotations := superset.GetAnnotations(); annotations != nil {
		if _, ok := annotations[annotationApproveUpgrade]; ok {
			patch := client.MergeFrom(superset.DeepCopy())
			delete(annotations, annotationApproveUpgrade)
			superset.SetAnnotations(annotations)
			if err := r.Patch(ctx, superset, patch); err != nil {
				return 0, false, fmt.Errorf("clearing approval annotation: %w", err)
			}
		}
	}

	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
		metav1.ConditionTrue, "LifecycleComplete", "Lifecycle tasks completed successfully", superset.Generation)
	return 0, true, nil
}

// checkUpgradeGates handles version comparison, downgrade blocking, and supervised approval.
// Returns (requeueAfter, gated) — if gated is true, the caller should return early.
func (r *SupersetReconciler) checkUpgradeGates(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	imageChanged bool,
	lastImage, currentImage string,
) (time.Duration, bool) {
	log := logf.FromContext(ctx)

	if !imageChanged || lastImage == "" {
		return 0, false
	}

	oldTag := tagFromImageRef(lastImage)
	newTag := tagFromImageRef(currentImage)
	direction := CompareVersions(oldTag, newTag)

	if direction == DirectionDowngrade {
		log.Info("Downgrade detected, blocking lifecycle", "from", oldTag, "to", newTag)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionFalse, "DowngradeBlocked",
			fmt.Sprintf("Downgrade from %s to %s is not supported. Alembic migrations are forward-only.", oldTag, newTag),
			superset.Generation)
		superset.Status.Phase = phaseBlocked
		superset.Status.Lifecycle.Phase = lifecyclePhaseBlocked
		superset.Status.Lifecycle.Upgrade = &supersetv1alpha1.UpgradeContext{
			FromVersion: oldTag,
			ToVersion:   newTag,
			Direction:   string(DirectionDowngrade),
		}
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "DowngradeBlocked", "Lifecycle",
			"Downgrade from %s to %s is not supported", oldTag, newTag)
		return -1, true
	}

	// Set upgrade context only once (preserve StartedAt across reconciles).
	if superset.Status.Lifecycle.Upgrade == nil {
		superset.Status.Lifecycle.Upgrade = &supersetv1alpha1.UpgradeContext{
			FromVersion: oldTag,
			ToVersion:   newTag,
			Direction:   string(direction),
			StartedAt:   nowPtr(),
		}
	}

	// Supervised mode: check for approval annotation.
	if getUpgradeMode(superset) == upgradeModeSupervsied {
		annotations := superset.GetAnnotations()
		if annotations == nil || annotations[annotationApproveUpgrade] != "true" {
			log.Info("Upgrade awaiting approval")
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
				metav1.ConditionFalse, "AwaitingApproval",
				fmt.Sprintf("Upgrade from %s to %s detected. Approve with: kubectl annotate superset %s %s=true",
					superset.Status.Lifecycle.Upgrade.FromVersion,
					superset.Status.Lifecycle.Upgrade.ToVersion,
					superset.Name, annotationApproveUpgrade),
				superset.Generation)
			superset.Status.Phase = phaseAwaitingApproval
			superset.Status.Lifecycle.Phase = lifecyclePhaseAwaitingApproval
			return 0, true
		}
	}

	return 0, false
}

// reconcileTask creates or updates a single SupersetLifecycleTask child CR and polls its status.
// Returns (requeueAfter, taskComplete, error).
// reconcileLifecycleTask creates or manages a single lifecycle task CR and polls its status.
// This is the unified task reconciler for all task types (clone, migrate, init).
// The checksum is pre-computed by the caller (strategy-aware + upstream propagation).
func (r *SupersetReconciler) reconcileLifecycleTask(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	taskType string,
	suffix string,
	command []string,
	taskChecksum string,
	configChecksum string,
	topLevel *resolution.SharedInput,
	saName string,
) (time.Duration, bool, error) {
	log := logf.FromContext(ctx)
	childName := superset.Name + suffix

	// Build the task's flat spec and pod configuration.
	flatSpec, renderedConfig := r.buildTaskFlatSpec(superset, taskType, command, configChecksum, topLevel, saName)

	// Get the task CR. Use Get+Create/Delete pattern (never CreateOrUpdate)
	// to avoid races with the task controller's status writes.
	child := &supersetv1alpha1.SupersetLifecycleTask{}
	err := r.Get(ctx, types.NamespacedName{Name: childName, Namespace: superset.Namespace}, child)

	if errors.IsNotFound(err) {
		// If the lifecycle previously completed (LastLifecycleImage is set) but
		// no task CR exists (GC or manual deletion), the task was already done.
		// Skip creation — the task will be re-created on the next actual change
		// (when the computed checksum would differ from the stored one anyway).
		if superset.Status.LastLifecycleImage != "" &&
			superset.Status.LastLifecycleImage == resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset)) {
			log.Info("Task already completed in previous lifecycle run (no CR, inputs unchanged)", "task", taskType)
			return 0, true, nil
		}

		child = &supersetv1alpha1.SupersetLifecycleTask{
			ObjectMeta: metav1.ObjectMeta{
				Name:      childName,
				Namespace: superset.Namespace,
				Labels: map[string]string{
					naming.LabelKeyName:      naming.LabelValueApp,
					naming.LabelKeyComponent: string(naming.ComponentInit),
					naming.LabelKeyParent:    superset.Name,
				},
			},
		}
		if err := controllerutil.SetControllerReference(superset, child, r.Scheme); err != nil {
			return 0, false, fmt.Errorf("setting controller reference on %s: %w", childName, err)
		}
		child.Spec.FlatComponentSpec = flatSpec
		child.Spec.Type = taskType
		child.Spec.Command = command
		child.Spec.ConfigChecksum = taskChecksum
		child.Spec.PodRetention = r.taskPodRetention(superset, taskType)
		child.Spec.MaxRetries = r.taskMaxRetries(superset, taskType)
		child.Spec.Timeout = r.taskTimeout(superset, taskType)

		// Create the ConfigMap before the task CR (only for tasks that need Python config).
		if renderedConfig != "" {
			resourceBaseName := childName
			if err := reconcileParentOwnedConfigMap(ctx, r.Client, r.Scheme, superset, renderedConfig, resourceBaseName); err != nil {
				return 0, false, fmt.Errorf("reconciling ConfigMap for lifecycle task %s: %w", childName, err)
			}
		}

		if err := r.Create(ctx, child); err != nil {
			return 0, false, fmt.Errorf("creating SupersetLifecycleTask %s: %w", childName, err)
		}
		log.Info("Created lifecycle task CR", "task", taskType)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionFalse, "TaskInProgress", fmt.Sprintf("%s task is in progress", taskType), superset.Generation)
		return taskRequeueInterval, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("fetching SupersetLifecycleTask %s: %w", childName, err)
	}

	// Task CR is being deleted — wait for GC to finish.
	if child.DeletionTimestamp != nil {
		log.Info("Task CR is being deleted, waiting for GC", "task", taskType)
		return taskRequeueInterval, false, nil
	}

	// Project status to parent.
	taskRef := &supersetv1alpha1.TaskRefStatus{
		State:       child.Status.State,
		StartedAt:   child.Status.StartedAt,
		CompletedAt: child.Status.CompletedAt,
		Duration:    child.Status.Duration,
		Attempts:    child.Status.Attempts,
		PodName:     child.Status.PodName,
		Image:       child.Status.Image,
		Message:     child.Status.Message,
	}
	r.projectScheduleStatus(superset, taskType, taskRef)
	switch taskType {
	case taskTypeClone:
		superset.Status.Lifecycle.Clone = taskRef
	case taskTypeMigrate:
		superset.Status.Lifecycle.Migrate = taskRef
	case taskTypeInit:
		superset.Status.Lifecycle.Init = taskRef
	}

	maxRetries := r.taskMaxRetriesValue(superset, taskType)

	switch child.Status.State {
	case taskStateComplete:
		if child.Status.ConfigChecksum == taskChecksum {
			log.Info("Task complete (checksum match, skipping)", "task", taskType)
			return 0, true, nil
		}
		log.Info("Task completed for previous inputs, deleting to re-run", "task", taskType,
			"statusChecksum", child.Status.ConfigChecksum, "expectedChecksum", taskChecksum)
		if err := r.Delete(ctx, child); err != nil {
			return 0, false, fmt.Errorf("deleting stale task CR %s: %w", childName, err)
		}
		return taskRequeueInterval, false, nil

	case taskStateFailed:
		if child.Status.Attempts >= maxRetries {
			if child.Status.ConfigChecksum == taskChecksum {
				log.Info("Task permanently failed", "task", taskType)
				setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
					metav1.ConditionFalse, "TaskFailed", fmt.Sprintf("%s: %s", taskType, child.Status.Message), superset.Generation)
				superset.Status.Phase = phaseInitializing
				return -1, false, nil
			}
			log.Info("Task failed for previous inputs, deleting to re-run", "task", taskType)
			if err := r.Delete(ctx, child); err != nil {
				return 0, false, fmt.Errorf("deleting stale task CR %s: %w", childName, err)
			}
			return taskRequeueInterval, false, nil
		}
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionFalse, "TaskRetrying", fmt.Sprintf("%s task is retrying", taskType), superset.Generation)
		return taskRequeueInterval, false, nil

	default:
		log.Info("Task not yet complete", "task", taskType, "state", child.Status.State)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionFalse, "TaskInProgress", fmt.Sprintf("%s task is in progress", taskType), superset.Generation)
		return taskRequeueInterval, false, nil
	}
}

// buildTaskFlatSpec constructs the fully-resolved FlatComponentSpec for a task pod.
// Clone tasks use a database-tool image; migrate/init use the Superset image.
// Returns (flatSpec, renderedConfig) — renderedConfig is empty for clone.
func (r *SupersetReconciler) buildTaskFlatSpec(
	superset *supersetv1alpha1.Superset,
	taskType string,
	command []string,
	_ string,
	topLevel *resolution.SharedInput,
	saName string,
) (supersetv1alpha1.FlatComponentSpec, string) {
	if taskType == taskTypeClone {
		return r.buildCloneTaskFlatSpec(superset, saName, topLevel), ""
	}
	return r.buildStandardTaskFlatSpec(superset, taskType, command, topLevel, saName)
}

// buildCloneTaskFlatSpec builds the flat spec for clone tasks (database-tool image, no Python config).
func (r *SupersetReconciler) buildCloneTaskFlatSpec(
	superset *supersetv1alpha1.Superset,
	saName string,
	topLevel *resolution.SharedInput,
) supersetv1alpha1.FlatComponentSpec {
	clone := superset.Spec.Lifecycle.Clone
	childName := superset.Name + suffixClone

	cloneEnvVars := collectCloneEnvVars(superset)
	comp := convertCloneComponent(clone)
	operatorInjected := &resolution.OperatorInjected{Env: cloneEnvVars}

	flat := resolution.ResolveChildSpec(
		resolution.ComponentInit, topLevel, comp,
		podOperatorLabels(string(naming.ComponentInit), childName, superset.Name), operatorInjected,
	)

	cloneImage := resolveCloneImage(clone)
	one := int32(1)
	flatSpec := supersetv1alpha1.FlatComponentSpec{
		Image:              cloneImage,
		Replicas:           &one,
		PodTemplate:        flatPodTemplate(flat),
		ServiceAccountName: saName,
	}
	flatSpec.Autoscaling = nil
	flatSpec.PodDisruptionBudget = nil
	return flatSpec
}

// buildStandardTaskFlatSpec builds the flat spec for migrate/init tasks (Superset image + Python config).
func (r *SupersetReconciler) buildStandardTaskFlatSpec(
	superset *supersetv1alpha1.Superset,
	taskType string,
	command []string,
	topLevel *resolution.SharedInput,
	saName string,
) (supersetv1alpha1.FlatComponentSpec, string) {
	childName := superset.Name + suffixForTaskType(taskType)
	resourceBaseName := childName

	compConfigInput := buildConfigInput(&superset.Spec)
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Config != nil {
		compConfigInput.ComponentConfig = *superset.Spec.Lifecycle.Config
	}

	var lifecycleSQLASpec *supersetv1alpha1.SQLAlchemyEngineOptionsSpec
	if superset.Spec.Lifecycle != nil {
		lifecycleSQLASpec = superset.Spec.Lifecycle.SQLAlchemyEngineOptions
	}
	compConfigInput.EngineOptions = supersetconfig.ComputeEngineOptions(
		naming.ComponentInit, superset.Spec.SQLAlchemyEngineOptions, lifecycleSQLASpec, 0, 0,
	)

	comp := convertTaskComponent(superset.Spec.Lifecycle, command)
	renderedConfig := supersetconfig.RenderConfig(supersetconfig.ComponentInit, compConfigInput)

	secretEnvVars := collectSecretEnvVars(&superset.Spec)
	var initEnvVars []corev1.EnvVar
	if taskType == taskTypeInit {
		initEnvVars = collectLifecycleInitEnvVars(superset.Spec.Lifecycle)
	}
	operatorInjected := buildOperatorInjected(renderedConfig, resourceBaseName, superset.Spec.ForceReload, append(secretEnvVars, initEnvVars...))

	flat := resolution.ResolveChildSpec(
		resolution.ComponentInit, topLevel, comp,
		podOperatorLabels(string(naming.ComponentInit), childName, superset.Name), operatorInjected,
	)

	var imageOverride *supersetv1alpha1.ImageOverrideSpec
	if superset.Spec.Lifecycle != nil {
		imageOverride = superset.Spec.Lifecycle.Image
	}
	flatSpec := flatSpecFromResolution(flat, &superset.Spec.Image, imageOverride, saName)
	flatSpec.Autoscaling = nil
	flatSpec.PodDisruptionBudget = nil
	return flatSpec, renderedConfig
}

func suffixForTaskType(taskType string) string {
	switch taskType {
	case taskTypeClone:
		return suffixClone
	case taskTypeMigrate:
		return suffixMigrate
	case taskTypeInit:
		return suffixInit
	default:
		return "-" + strings.ToLower(taskType)
	}
}

func lifecycleImageOverride(superset *supersetv1alpha1.Superset) *supersetv1alpha1.ImageOverrideSpec {
	if superset.Spec.Lifecycle != nil {
		return superset.Spec.Lifecycle.Image
	}
	return nil
}

// taskPodRetention returns the pod retention spec for a task type.
func (r *SupersetReconciler) taskPodRetention(superset *supersetv1alpha1.Superset, taskType string) *supersetv1alpha1.PodRetentionSpec {
	if superset.Spec.Lifecycle == nil {
		return nil
	}
	if taskType == taskTypeClone && superset.Spec.Lifecycle.Clone != nil && superset.Spec.Lifecycle.Clone.PodRetention != nil {
		return superset.Spec.Lifecycle.Clone.PodRetention
	}
	return superset.Spec.Lifecycle.PodRetention
}

// isTaskEnabled returns true if the task is part of the lifecycle pipeline (strategy != Never).
// isTaskEnabled returns true if the task is part of the lifecycle pipeline.
// A task is enabled when its spec exists (presence = enabled) and Disabled != true.
// Clone requires spec.lifecycle.clone to be set; migrate/init are enabled by default.
func (r *SupersetReconciler) isTaskEnabled(superset *supersetv1alpha1.Superset, taskType string) bool {
	if superset.Spec.Lifecycle == nil {
		return taskType != taskTypeClone
	}
	switch taskType {
	case taskTypeClone:
		if superset.Spec.Lifecycle.Clone == nil {
			return false
		}
		return superset.Spec.Lifecycle.Clone.Disabled == nil || !*superset.Spec.Lifecycle.Clone.Disabled
	case taskTypeMigrate:
		if superset.Spec.Lifecycle.Migrate != nil {
			return superset.Spec.Lifecycle.Migrate.Disabled == nil || !*superset.Spec.Lifecycle.Migrate.Disabled
		}
		return true
	case taskTypeInit:
		if superset.Spec.Lifecycle.Init != nil {
			return superset.Spec.Lifecycle.Init.Disabled == nil || !*superset.Spec.Lifecycle.Init.Disabled
		}
		return true
	}
	return false
}

// pruneDisabledTasks deletes task CRs for disabled tasks.
func (r *SupersetReconciler) pruneDisabledTasks(ctx context.Context, superset *supersetv1alpha1.Superset, cloneEnabled, migrateEnabled, initEnabled bool) error {
	if !cloneEnabled {
		if err := r.deleteTaskCR(ctx, superset.Name+suffixClone, superset.Namespace); err != nil {
			return fmt.Errorf("deleting clone task CR: %w", err)
		}
	}
	if !migrateEnabled {
		if err := r.deleteTaskCR(ctx, superset.Name+suffixMigrate, superset.Namespace); err != nil {
			return fmt.Errorf("deleting migrate task CR: %w", err)
		}
	}
	if !initEnabled {
		if err := r.deleteTaskCR(ctx, superset.Name+suffixInit, superset.Namespace); err != nil {
			return fmt.Errorf("deleting init task CR: %w", err)
		}
	}
	return nil
}

// getTaskStatusChecksum retrieves the status checksum from a completed task CR.
// Returns empty string if the task CR doesn't exist or isn't complete.
func (r *SupersetReconciler) getTaskStatusChecksum(ctx context.Context, superset *supersetv1alpha1.Superset, suffix string) string {
	child := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := r.Get(ctx, types.NamespacedName{Name: superset.Name + suffix, Namespace: superset.Namespace}, child); err != nil {
		return ""
	}
	if child.Status.State == taskStateComplete {
		return child.Status.ConfigChecksum
	}
	return ""
}

// computePipelineSeed computes a seed checksum for the lifecycle pipeline.
// Reserved for future use (custom task hooks may need a shared seed).
// Currently unused — the pipeline anchor is parentUID directly.
//
//nolint:unused
func (r *SupersetReconciler) computePipelineSeed(superset *supersetv1alpha1.Superset, configChecksum string) string {
	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	return computeChecksum(struct {
		ParentUID      string
		Image          string
		ConfigChecksum string
	}{
		ParentUID:      string(superset.UID),
		Image:          currentImage,
		ConfigChecksum: configChecksum,
	})
}

// computeStepChecksum computes a task's checksum from the incoming checksum
// (seed or previous task's completed checksum) plus the task's own inputs.
// The incoming checksum carries all upstream state — each task only adds its
// own unique contribution.
func (r *SupersetReconciler) computeStepChecksum(incomingChecksum, taskType string, command []string, extraInputs any) string {
	return computeChecksum(struct {
		IncomingChecksum string
		TaskType         string
		Command          []string
		ExtraInputs      any
	}{
		IncomingChecksum: incomingChecksum,
		TaskType:         taskType,
		Command:          command,
		ExtraInputs:      extraInputs,
	})
}

// cloneInputs returns the clone-specific inputs that contribute to its step checksum.
func (r *SupersetReconciler) cloneInputs(superset *supersetv1alpha1.Superset) any {
	clone := superset.Spec.Lifecycle.Clone
	return struct {
		Trigger          string
		ScheduleTick     string
		Source           supersetv1alpha1.CloneSourceSpec
		ExcludeTables    []string
		ExcludeTableData []string
	}{
		Trigger:          derefOrDefault(clone.Trigger, ""),
		ScheduleTick:     r.scheduleTick(clone.CronSchedule),
		Source:           clone.Source,
		ExcludeTables:    clone.ExcludeTables,
		ExcludeTableData: clone.ExcludeTableData,
	}
}

// migrateInputs returns the migrate-specific inputs: image (version changes).
func (r *SupersetReconciler) migrateInputs(superset *supersetv1alpha1.Superset) any {
	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	trigger := ""
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Migrate != nil {
		trigger = derefOrDefault(superset.Spec.Lifecycle.Migrate.Trigger, "")
	}
	return struct {
		Image   string
		Trigger string
	}{
		Image:   currentImage,
		Trigger: trigger,
	}
}

// initInputs returns the init-specific inputs: config checksum (config changes).
func (r *SupersetReconciler) initInputs(superset *supersetv1alpha1.Superset, configChecksum string) any {
	trigger := ""
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Init != nil {
		trigger = derefOrDefault(superset.Spec.Lifecycle.Init.Trigger, "")
	}
	return struct {
		ConfigChecksum string
		Trigger        string
	}{
		ConfigChecksum: configChecksum,
		Trigger:        trigger,
	}
}

func (r *SupersetReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *SupersetReconciler) scheduleTick(cronSchedule *string) string {
	if cronSchedule == nil || *cronSchedule == "" {
		return ""
	}
	return schedule.CurrentTick(*cronSchedule, r.now())
}

func (r *SupersetReconciler) nextScheduleRequeue(superset *supersetv1alpha1.Superset) time.Duration {
	if superset.Spec.Lifecycle == nil {
		return 0
	}
	var earliest time.Time
	for _, expr := range r.activeSchedules(superset) {
		next := schedule.NextTick(expr, r.now())
		if next.IsZero() {
			continue
		}
		if earliest.IsZero() || next.Before(earliest) {
			earliest = next
		}
	}
	if earliest.IsZero() {
		return 0
	}
	d := earliest.Sub(r.now()) + time.Second
	if d < time.Second {
		return time.Second
	}
	return d
}

func (r *SupersetReconciler) activeSchedules(superset *supersetv1alpha1.Superset) []string {
	lc := superset.Spec.Lifecycle
	var out []string
	if lc.Clone != nil && lc.Clone.CronSchedule != nil && !isDisabled(lc.Clone.Disabled) {
		out = append(out, *lc.Clone.CronSchedule)
	}
	return out
}

func isDisabled(disabled *bool) bool {
	return disabled != nil && *disabled
}

func (r *SupersetReconciler) projectScheduleStatus(superset *supersetv1alpha1.Superset, taskType string, taskRef *supersetv1alpha1.TaskRefStatus) {
	if superset.Spec.Lifecycle == nil {
		return
	}
	var cronSchedule *string
	switch taskType {
	case taskTypeClone:
		if superset.Spec.Lifecycle.Clone != nil {
			cronSchedule = superset.Spec.Lifecycle.Clone.CronSchedule
		}
	}
	if cronSchedule == nil || *cronSchedule == "" {
		return
	}
	now := r.now()
	if tick := schedule.CurrentTick(*cronSchedule, now); tick != "" {
		parsed, err := time.Parse(time.RFC3339, tick)
		if err == nil {
			t := metav1.NewTime(parsed)
			taskRef.LastScheduledAt = &t
		}
	}
	if next := schedule.NextTick(*cronSchedule, now); !next.IsZero() {
		t := metav1.NewTime(next)
		taskRef.NextScheduleAt = &t
	}
}

func (r *SupersetReconciler) taskMaxRetries(superset *supersetv1alpha1.Superset, taskType string) *int32 {
	if superset.Spec.Lifecycle == nil {
		return nil
	}
	switch taskType {
	case taskTypeClone:
		if superset.Spec.Lifecycle.Clone != nil {
			return superset.Spec.Lifecycle.Clone.MaxRetries
		}
	case taskTypeMigrate:
		if superset.Spec.Lifecycle.Migrate != nil {
			return superset.Spec.Lifecycle.Migrate.MaxRetries
		}
	case taskTypeInit:
		if superset.Spec.Lifecycle.Init != nil {
			return superset.Spec.Lifecycle.Init.MaxRetries
		}
	}
	return nil
}

func (r *SupersetReconciler) taskMaxRetriesValue(superset *supersetv1alpha1.Superset, taskType string) int32 {
	if ptr := r.taskMaxRetries(superset, taskType); ptr != nil {
		return *ptr
	}
	return defaultMaxRetries
}

func (r *SupersetReconciler) taskTimeout(superset *supersetv1alpha1.Superset, taskType string) *metav1.Duration {
	if superset.Spec.Lifecycle == nil {
		return nil
	}
	switch taskType {
	case taskTypeClone:
		if superset.Spec.Lifecycle.Clone != nil {
			return superset.Spec.Lifecycle.Clone.Timeout
		}
	case taskTypeMigrate:
		if superset.Spec.Lifecycle.Migrate != nil {
			return superset.Spec.Lifecycle.Migrate.Timeout
		}
	case taskTypeInit:
		if superset.Spec.Lifecycle.Init != nil {
			return superset.Spec.Lifecycle.Init.Timeout
		}
	}
	return nil
}

func defaultMigrateCommand(superset *supersetv1alpha1.Superset) []string {
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Migrate != nil && len(superset.Spec.Lifecycle.Migrate.Command) > 0 {
		return superset.Spec.Lifecycle.Migrate.Command
	}
	return []string{"/bin/sh", "-c", "superset db upgrade"}
}

func defaultInitCommand(superset *supersetv1alpha1.Superset) []string {
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Init != nil && len(superset.Spec.Lifecycle.Init.Command) > 0 {
		return superset.Spec.Lifecycle.Init.Command
	}
	var initSpec *supersetv1alpha1.InitTaskSpec
	if superset.Spec.Lifecycle != nil {
		initSpec = superset.Spec.Lifecycle.Init
	}
	return buildInitCommand(initSpec)
}

// buildCloneCommand constructs the pg_dump|psql or mysqldump|mysql streaming command
// from the clone spec. Returns the user's custom command if specified.
func (r *SupersetReconciler) buildCloneCommand(superset *supersetv1alpha1.Superset) []string {
	clone := superset.Spec.Lifecycle.Clone
	if len(clone.Command) > 0 {
		return clone.Command
	}

	srcType := dbTypePostgresql
	if clone.Source.Type != nil {
		srcType = *clone.Source.Type
	}

	if srcType == dbTypeMySQL {
		return []string{"/bin/sh", "-c", buildMySQLCloneScript(clone)}
	}
	return []string{"/bin/sh", "-c", buildPostgresCloneScript(clone)}
}

func buildPostgresCloneScript(clone *supersetv1alpha1.CloneTaskSpec) string {
	var b strings.Builder
	b.WriteString(`set -e
PGPASSWORD="$SUPERSET_OPERATOR__DB_PASS" dropdb --if-exists -h "$SUPERSET_OPERATOR__DB_HOST" -p "$SUPERSET_OPERATOR__DB_PORT" -U "$SUPERSET_OPERATOR__DB_USER" "$SUPERSET_OPERATOR__DB_NAME"
PGPASSWORD="$SUPERSET_OPERATOR__DB_PASS" createdb -h "$SUPERSET_OPERATOR__DB_HOST" -p "$SUPERSET_OPERATOR__DB_PORT" -U "$SUPERSET_OPERATOR__DB_USER" "$SUPERSET_OPERATOR__DB_NAME"
PGPASSWORD="$SUPERSET_OPERATOR__CLONE_SRC_PASS" pg_dump -h "$SUPERSET_OPERATOR__CLONE_SRC_HOST" -p "$SUPERSET_OPERATOR__CLONE_SRC_PORT" -U "$SUPERSET_OPERATOR__CLONE_SRC_USER" --no-owner --no-privileges`)

	for _, t := range clone.ExcludeTables {
		fmt.Fprintf(&b, ` --exclude-table=%q`, t)
	}
	for _, t := range clone.ExcludeTableData {
		fmt.Fprintf(&b, ` --exclude-table-data=%q`, t)
	}

	b.WriteString(` "$SUPERSET_OPERATOR__CLONE_SRC_DB" | PGPASSWORD="$SUPERSET_OPERATOR__DB_PASS" psql -h "$SUPERSET_OPERATOR__DB_HOST" -p "$SUPERSET_OPERATOR__DB_PORT" -U "$SUPERSET_OPERATOR__DB_USER" "$SUPERSET_OPERATOR__DB_NAME"`)
	return b.String()
}

func buildMySQLCloneScript(clone *supersetv1alpha1.CloneTaskSpec) string {
	var b strings.Builder
	b.WriteString(`set -e
mysql -h "$SUPERSET_OPERATOR__DB_HOST" -P "$SUPERSET_OPERATOR__DB_PORT" -u "$SUPERSET_OPERATOR__DB_USER" -p"$SUPERSET_OPERATOR__DB_PASS" -e "DROP DATABASE IF EXISTS ` + "`$SUPERSET_OPERATOR__DB_NAME`" + `; CREATE DATABASE ` + "`$SUPERSET_OPERATOR__DB_NAME`" + `;"
mysqldump -h "$SUPERSET_OPERATOR__CLONE_SRC_HOST" -P "$SUPERSET_OPERATOR__CLONE_SRC_PORT" -u "$SUPERSET_OPERATOR__CLONE_SRC_USER" -p"$SUPERSET_OPERATOR__CLONE_SRC_PASS" --single-transaction --routines --triggers`)

	for _, t := range clone.ExcludeTables {
		fmt.Fprintf(&b, ` --ignore-table="$SUPERSET_OPERATOR__CLONE_SRC_DB".%q`, t)
	}

	b.WriteString(` "$SUPERSET_OPERATOR__CLONE_SRC_DB" | mysql -h "$SUPERSET_OPERATOR__DB_HOST" -P "$SUPERSET_OPERATOR__DB_PORT" -u "$SUPERSET_OPERATOR__DB_USER" -p"$SUPERSET_OPERATOR__DB_PASS" "$SUPERSET_OPERATOR__DB_NAME"`)
	return b.String()
}

// collectCloneEnvVars builds env vars for the clone task pod.
// Includes both source (CLONE_SRC_*) and target (DB_*) connection details.
func collectCloneEnvVars(superset *supersetv1alpha1.Superset) []corev1.EnvVar {
	var envs []corev1.EnvVar
	clone := superset.Spec.Lifecycle.Clone
	spec := &superset.Spec

	// Source env vars.
	envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcHost, Value: clone.Source.Host})

	port := defaultDBPort(clone.Source.Type)
	if clone.Source.Port != nil {
		port = *clone.Source.Port
	}
	envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcPort, Value: fmt.Sprintf("%d", port)})
	envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcDB, Value: clone.Source.Database})
	envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcUser, Value: clone.Source.Username})

	if clone.Source.Password != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcPass, Value: *clone.Source.Password})
	} else if clone.Source.PasswordFrom != nil {
		envs = append(envs, corev1.EnvVar{
			Name:      naming.EnvCloneSrcPass,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: clone.Source.PasswordFrom},
		})
	}

	// Target env vars (from spec.metastore — clone requires structured metastore).
	if spec.Metastore != nil && spec.Metastore.Host != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvDBHost, Value: *spec.Metastore.Host})
		targetPort := defaultDBPort(spec.Metastore.Type)
		if spec.Metastore.Port != nil {
			targetPort = *spec.Metastore.Port
		}
		envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPort, Value: fmt.Sprintf("%d", targetPort)})
		if spec.Metastore.Database != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBName, Value: *spec.Metastore.Database})
		}
		if spec.Metastore.Username != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBUser, Value: *spec.Metastore.Username})
		}
		if spec.Metastore.Password != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPass, Value: *spec.Metastore.Password})
		} else if spec.Metastore.PasswordFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvDBPass,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Metastore.PasswordFrom},
			})
		}
	}

	return envs
}

// resolveCloneImage determines the image for the clone pod.
func resolveCloneImage(clone *supersetv1alpha1.CloneTaskSpec) supersetv1alpha1.ImageSpec {
	if clone.Image != nil {
		return *clone.Image
	}
	srcType := dbTypePostgresql
	if clone.Source.Type != nil {
		srcType = *clone.Source.Type
	}
	if srcType == dbTypeMySQL {
		repo, tag := splitImageRef(naming.CloneImageMySQL)
		return supersetv1alpha1.ImageSpec{Repository: repo, Tag: tag}
	}
	repo, tag := splitImageRef(naming.CloneImagePostgres)
	return supersetv1alpha1.ImageSpec{Repository: repo, Tag: tag}
}

func splitImageRef(ref string) (string, string) {
	if idx := strings.LastIndex(ref, ":"); idx != -1 {
		return ref[:idx], ref[idx+1:]
	}
	return ref, defaultImageTag
}

// convertCloneComponent builds a minimal ComponentInput for the clone task pod.
func convertCloneComponent(clone *supersetv1alpha1.CloneTaskSpec) *resolution.ComponentInput {
	if clone.PodTemplate == nil {
		return &resolution.ComponentInput{}
	}
	return &resolution.ComponentInput{
		SharedInput: resolution.SharedInput{
			PodTemplate: clone.PodTemplate,
		},
	}
}

// flatPodTemplate extracts the PodTemplate from a resolved FlatSpec.
func flatPodTemplate(flat *resolution.FlatSpec) *supersetv1alpha1.PodTemplate {
	return flat.PodTemplate
}

func getUpgradeMode(superset *supersetv1alpha1.Superset) string {
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.UpgradeMode != nil {
		return *superset.Spec.Lifecycle.UpgradeMode
	}
	return upgradeModeAutomatic
}

// taskRequiresDrain returns whether a task requires components to be drained.
// Defaults: clone=true (DROP DATABASE needs no connections), migrate=true
// (schema changes risk deadlocks), init=false (roles/permissions are safe).
func (r *SupersetReconciler) taskRequiresDrain(superset *supersetv1alpha1.Superset, taskType string) bool {
	var spec *supersetv1alpha1.BaseTaskSpec
	if superset.Spec.Lifecycle != nil {
		switch taskType {
		case taskTypeClone:
			if superset.Spec.Lifecycle.Clone != nil {
				spec = &superset.Spec.Lifecycle.Clone.BaseTaskSpec
			}
		case taskTypeMigrate:
			if superset.Spec.Lifecycle.Migrate != nil {
				spec = &superset.Spec.Lifecycle.Migrate.BaseTaskSpec
			}
		case taskTypeInit:
			if superset.Spec.Lifecycle.Init != nil {
				spec = &superset.Spec.Lifecycle.Init.BaseTaskSpec
			}
		}
	}
	if spec != nil && spec.RequiresDrain != nil {
		return *spec.RequiresDrain
	}
	// Defaults per task type.
	switch taskType {
	case taskTypeClone, taskTypeMigrate:
		return true
	default:
		return false
	}
}

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
// Returns (requeueAfter, drained, error). drained=true means either drain is not
// needed or drain completed successfully.
func (r *SupersetReconciler) drainIfNeeded(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	cloneEnabled, migrateEnabled, initEnabled bool,
) (time.Duration, bool, error) {
	needsDrain := (cloneEnabled && r.taskRequiresDrain(superset, taskTypeClone)) ||
		(migrateEnabled && r.taskRequiresDrain(superset, taskTypeMigrate)) ||
		(initEnabled && r.taskRequiresDrain(superset, taskTypeInit))
	if !needsDrain {
		return 0, true, nil
	}

	superset.Status.Lifecycle.Phase = lifecyclePhaseDraining
	superset.Status.Phase = phaseDraining
	drained, err := r.drainComponents(ctx, superset)
	if err != nil {
		return 0, false, fmt.Errorf("draining components: %w", err)
	}
	if !drained {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionFalse, "Draining", "Scaling components to zero before lifecycle tasks", superset.Generation)
		return taskRequeueInterval, false, nil
	}
	return 0, true, nil
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
	parts := strings.SplitN(ref, ":", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ref
}

func nowPtr() *metav1.Time {
	now := metav1.Now()
	return &now
}

// collectLifecycleInitEnvVars returns env vars for the init task (admin user credentials).
func collectLifecycleInitEnvVars(lifecycle *supersetv1alpha1.LifecycleSpec) []corev1.EnvVar {
	if lifecycle == nil || lifecycle.Init == nil || lifecycle.Init.AdminUser == nil {
		return nil
	}
	admin := lifecycle.Init.AdminUser
	return []corev1.EnvVar{
		{Name: naming.EnvAdminUsername, Value: derefOrDefault(admin.Username, "admin")},
		{Name: naming.EnvAdminPassword, Value: derefOrDefault(admin.Password, "admin")},
		{Name: naming.EnvAdminFirstName, Value: derefOrDefault(admin.FirstName, "Superset")},
		{Name: naming.EnvAdminLastName, Value: derefOrDefault(admin.LastName, "Admin")},
		{Name: naming.EnvAdminEmail, Value: derefOrDefault(admin.Email, "admin@example.com")},
	}
}

// --- Config input building ---

func buildConfigInput(spec *supersetv1alpha1.SupersetSpec) *supersetconfig.ConfigInput {
	input := &supersetconfig.ConfigInput{}

	if spec.Metastore != nil {
		if spec.Metastore.URI != nil || spec.Metastore.URIFrom != nil {
			input.MetastoreMode = supersetconfig.MetastorePassthrough
		} else if spec.Metastore.Host != nil {
			input.MetastoreMode = supersetconfig.MetastoreStructured
			dbType := dbTypePostgresql
			if spec.Metastore.Type != nil {
				dbType = *spec.Metastore.Type
			}
			input.DBDriver = dbType
		}
	}

	if spec.Valkey != nil {
		input.Valkey = buildValkeyInput(spec.Valkey)
	}

	if spec.Config != nil {
		input.Config = *spec.Config
	}

	return input
}

// buildValkeyInput converts the CRD ValkeySpec into a resolved ValkeyInput with defaults applied.
func buildValkeyInput(v *supersetv1alpha1.ValkeySpec) *supersetconfig.ValkeyInput {
	vi := &supersetconfig.ValkeyInput{
		Cache:                resolveValkeyCache(v.Cache, 1, "superset_", 300),
		DataCache:            resolveValkeyCache(v.DataCache, 2, "superset_data_", 86400),
		FilterStateCache:     resolveValkeyCache(v.FilterStateCache, 3, "superset_filter_", 3600),
		ExploreFormDataCache: resolveValkeyCache(v.ExploreFormDataCache, 4, "superset_explore_", 3600),
		ThumbnailCache:       resolveValkeyCache(v.ThumbnailCache, 5, "superset_thumbnail_", 3600),
		CeleryBroker:         resolveValkeyCelery(v.CeleryBroker, 0),
		CeleryResultBackend:  resolveValkeyCelery(v.CeleryResultBackend, 0),
		ResultsBackend:       resolveValkeyResults(v.ResultsBackend, 6, "superset_results_"),
	}

	if v.SSL != nil {
		vi.SSL = true
		if v.SSL.CertRequired != nil {
			vi.SSLCertRequired = *v.SSL.CertRequired
		}
		if v.SSL.KeyFile != nil {
			vi.SSLKeyFile = *v.SSL.KeyFile
		}
		if v.SSL.CertFile != nil {
			vi.SSLCertFile = *v.SSL.CertFile
		}
		if v.SSL.CACertFile != nil {
			vi.SSLCACertFile = *v.SSL.CACertFile
		}
	}

	return vi
}

func resolveValkeyCache(spec *supersetv1alpha1.ValkeyCacheSpec, defaultDB int32, defaultPrefix string, defaultTimeout int32) supersetconfig.ValkeyCacheInput {
	input := supersetconfig.ValkeyCacheInput{
		Database:       defaultDB,
		KeyPrefix:      defaultPrefix,
		DefaultTimeout: defaultTimeout,
	}
	if spec == nil {
		return input
	}
	if spec.Disabled != nil && *spec.Disabled {
		input.Disabled = true
	}
	if spec.Database != nil {
		input.Database = *spec.Database
	}
	if spec.KeyPrefix != nil {
		input.KeyPrefix = *spec.KeyPrefix
	}
	if spec.DefaultTimeout != nil {
		input.DefaultTimeout = *spec.DefaultTimeout
	}
	return input
}

func resolveValkeyCelery(spec *supersetv1alpha1.ValkeyCelerySpec, defaultDB int32) supersetconfig.ValkeyCeleryInput {
	input := supersetconfig.ValkeyCeleryInput{Database: defaultDB}
	if spec == nil {
		return input
	}
	if spec.Disabled != nil && *spec.Disabled {
		input.Disabled = true
	}
	if spec.Database != nil {
		input.Database = *spec.Database
	}
	return input
}

func resolveValkeyResults(spec *supersetv1alpha1.ValkeyResultsBackendSpec, defaultDB int32, defaultPrefix string) supersetconfig.ValkeyResultsInput {
	input := supersetconfig.ValkeyResultsInput{
		Database:  defaultDB,
		KeyPrefix: defaultPrefix,
	}
	if spec == nil {
		return input
	}
	if spec.Disabled != nil && *spec.Disabled {
		input.Disabled = true
	}
	if spec.Database != nil {
		input.Database = *spec.Database
	}
	if spec.KeyPrefix != nil {
		input.KeyPrefix = *spec.KeyPrefix
	}
	return input
}

// --- Secret env var collection ---

// collectSecretEnvVars gathers env vars for SECRET_KEY and metastore fields.
func collectSecretEnvVars(spec *supersetv1alpha1.SupersetSpec) []corev1.EnvVar {
	var envs []corev1.EnvVar
	isDev := spec.Environment != nil && *spec.Environment == naming.EnvironmentDev

	// SUPERSET_OPERATOR__SECRET_KEY — rendered into superset_config.py as SECRET_KEY.
	if isDev && spec.SecretKey != nil {
		envs = append(envs, corev1.EnvVar{
			Name:  naming.EnvSecretKey,
			Value: *spec.SecretKey,
		})
	} else if spec.SecretKeyFrom != nil {
		envs = append(envs, corev1.EnvVar{
			Name:      naming.EnvSecretKey,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.SecretKeyFrom},
		})
	}

	// Metastore env vars.
	if spec.Metastore != nil {
		if spec.Metastore.URI != nil {
			envs = append(envs, corev1.EnvVar{
				Name:  naming.EnvDatabaseURI,
				Value: *spec.Metastore.URI,
			})
		} else if spec.Metastore.URIFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvDatabaseURI,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Metastore.URIFrom},
			})
		} else if spec.Metastore.Host != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBHost, Value: *spec.Metastore.Host})
			port := defaultDBPort(spec.Metastore.Type)
			if spec.Metastore.Port != nil {
				port = *spec.Metastore.Port
			}
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPort, Value: fmt.Sprintf("%d", port)})
			if spec.Metastore.Database != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvDBName, Value: *spec.Metastore.Database})
			}
			if spec.Metastore.Username != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvDBUser, Value: *spec.Metastore.Username})
			}
			if spec.Metastore.Password != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPass, Value: *spec.Metastore.Password})
			} else if spec.Metastore.PasswordFrom != nil {
				envs = append(envs, corev1.EnvVar{
					Name:      naming.EnvDBPass,
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Metastore.PasswordFrom},
				})
			}
		}
	}

	// Valkey env vars.
	if spec.Valkey != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyHost, Value: spec.Valkey.Host})
		port := int32(6379)
		if spec.Valkey.Port != nil {
			port = *spec.Valkey.Port
		}
		envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyPort, Value: fmt.Sprintf("%d", port)})
		if isDev && spec.Valkey.Password != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyPass, Value: *spec.Valkey.Password})
		} else if spec.Valkey.PasswordFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvValkeyPass,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Valkey.PasswordFrom},
			})
		}
	}

	return envs
}

func derefOrDefault(ptr *string, def string) string {
	if ptr != nil {
		return *ptr
	}
	return def
}

func defaultDBPort(driver *string) int32 {
	if driver != nil && *driver == dbTypeMySQL {
		return 3306
	}
	return 5432
}

// --- Operator-injected volumes/env/mounts ---

func buildOperatorInjected(renderedConfig, childName, forceReload string, configEnvVars []corev1.EnvVar) *resolution.OperatorInjected {
	injected := &resolution.OperatorInjected{}

	if renderedConfig != "" {
		// Config volume + mount.
		injected.Volumes = append(injected.Volumes, corev1.Volume{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: naming.ConfigMapName(childName),
					},
				},
			},
		})
		injected.VolumeMounts = append(injected.VolumeMounts, corev1.VolumeMount{
			Name:      configVolumeName,
			MountPath: configMountPath,
			ReadOnly:  true,
		})
	}

	// Add config-derived env vars (secret key, metastore fields, etc.).
	injected.Env = append(injected.Env, configEnvVars...)

	// ForceReload propagated via env var (triggers pod restart on change).
	if forceReload != "" {
		injected.Env = append(injected.Env, corev1.EnvVar{
			Name:  naming.EnvForceReload,
			Value: forceReload,
		})
	}

	return injected
}

// --- Resolution output -> CRD type mapping ---

// flatSpecFromResolution converts a FlatSpec into a FlatComponentSpec.
// When imageOverride is non-nil, its Tag and/or Repository override the parent image.
// saName is set on the FlatComponentSpec so it propagates to Deployment pods.
func flatSpecFromResolution(flat *resolution.FlatSpec, parentImage *supersetv1alpha1.ImageSpec, imageOverride *supersetv1alpha1.ImageOverrideSpec, saName string) supersetv1alpha1.FlatComponentSpec {
	replicas := flat.Replicas
	image := *parentImage
	if imageOverride != nil {
		if imageOverride.Tag != nil {
			image.Tag = *imageOverride.Tag
		}
		if imageOverride.Repository != nil {
			image.Repository = *imageOverride.Repository
		}
	}
	return supersetv1alpha1.FlatComponentSpec{
		Image:               image,
		Replicas:            &replicas,
		DeploymentTemplate:  flat.DeploymentTemplate,
		PodTemplate:         flat.PodTemplate,
		ServiceAccountName:  saName,
		Autoscaling:         flat.Autoscaling,
		PodDisruptionBudget: flat.PodDisruptionBudget,
	}
}

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
