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
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// SupersetReconciler reconciles a Superset object.
type SupersetReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
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

	return ctrl.Result{}, nil
}

// applyChildCR creates or updates a child CR with the resolved flat spec.
func (r *SupersetReconciler) applyChildCR(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	childName string,
	componentType naming.ComponentType,
	flat *resolution.FlatSpec,
	renderedConfig, configChecksum, saName string,
	imageOverride *supersetv1alpha1.ImageOverrideSpec,
	newObj func() client.Object,
	applySpec func(client.Object, supersetv1alpha1.FlatComponentSpec, string, string),
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
		applySpec(obj, flatSpec, renderedConfig, configChecksum)
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

	suffixMigrate = "-migrate"
	suffixInit    = "-init"

	strategyVersionChange = "VersionChange"
	strategyAlways        = "Always"
	strategyNever         = "Never"

	upgradeModeAutomatic  = "Automatic"
	upgradeModeSupervsied = "Supervised"

	lifecyclePhaseIdle             = "Idle"
	lifecyclePhaseDraining         = "Draining"
	lifecyclePhaseMigrating        = "Migrating"
	lifecyclePhaseInitializing     = "Initializing"
	lifecyclePhaseComplete         = "Complete"
	lifecyclePhaseBlocked          = "Blocked"
	lifecyclePhaseAwaitingApproval = "AwaitingApproval"

	annotationApproveUpgrade = "superset.apache.org/approve-upgrade"

	upgradeStrategyRolling = "Rolling"
	upgradeStrategyDrain   = "Drain"

	phaseUpgrading        = "Upgrading"
	phaseDraining         = "Draining"
	phaseBlocked          = "Blocked"
	phaseAwaitingApproval = "AwaitingApproval"
)

// reconcileLifecycle orchestrates the lifecycle tasks (migrate + init) and gates
// component deployment. Returns (requeueAfter, lifecycleComplete, error).
func (r *SupersetReconciler) reconcileLifecycle(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	configChecksum string,
	topLevel *resolution.SharedInput,
	saName string,
) (time.Duration, bool, error) {

	// Prune orphaned task CRs only when lifecycle is disabled.
	if isLifecycleDisabled(superset) {
		if err := r.pruneOrphans(ctx, superset.Namespace, superset.Name,
			naming.ComponentInit,
			func() client.ObjectList { return &supersetv1alpha1.SupersetLifecycleTaskList{} },
			"",
		); err != nil {
			return 0, false, fmt.Errorf("pruning orphaned task CRs: %w", err)
		}
	}

	// If lifecycle is disabled, mark complete.
	if isLifecycleDisabled(superset) {
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

	// Determine which tasks need to run and prune disabled task CRs.
	migrateNeeded, initNeeded, err := r.resolveTaskNeeds(ctx, superset, imageChanged)
	if err != nil {
		return 0, false, err
	}

	// If neither task is needed, lifecycle is complete.
	if !migrateNeeded && !initNeeded {
		superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionTrue, "LifecycleComplete", "Lifecycle tasks completed successfully", superset.Generation)
		return 0, true, nil
	}

	// Drain strategy: delete all component child CRs before running tasks.
	// Only drain when migrate is needed — init alone doesn't alter the schema.
	if migrateNeeded && getUpgradeStrategy(superset) == upgradeStrategyDrain {
		superset.Status.Lifecycle.Phase = lifecyclePhaseDraining
		superset.Status.Phase = phaseDraining
		drained, err := r.drainComponents(ctx, superset)
		if err != nil {
			return 0, false, fmt.Errorf("draining components: %w", err)
		}
		if !drained {
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
				metav1.ConditionFalse, "Draining", "Scaling components to zero before migration", superset.Generation)
			return taskRequeueInterval, false, nil
		}
	}

	// Orchestrate: migrate first, then init.
	if migrateNeeded {
		superset.Status.Lifecycle.Phase = lifecyclePhaseMigrating
		if imageChanged {
			superset.Status.Phase = phaseUpgrading
		} else {
			superset.Status.Phase = phaseInitializing
		}

		migrateCmd := defaultMigrateCommand(superset)
		requeueAfter, complete, err := r.reconcileTask(ctx, superset, configChecksum, topLevel, saName, taskTypeMigrate, suffixMigrate, migrateCmd)
		if err != nil {
			return 0, false, fmt.Errorf("reconciling migrate task: %w", err)
		}
		if !complete {
			return requeueAfter, false, nil
		}
	}

	if initNeeded {
		superset.Status.Lifecycle.Phase = lifecyclePhaseInitializing
		if superset.Status.Phase != phaseUpgrading {
			superset.Status.Phase = phaseInitializing
		}

		initCmd := defaultInitCommand(superset)
		requeueAfter, complete, err := r.reconcileTask(ctx, superset, configChecksum, topLevel, saName, taskTypeInit, suffixInit, initCmd)
		if err != nil {
			return 0, false, fmt.Errorf("reconciling init task: %w", err)
		}
		if !complete {
			return requeueAfter, false, nil
		}
	}

	// Both tasks complete. Update lastLifecycleImage and clear upgrade context.
	superset.Status.LastLifecycleImage = currentImage
	superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
	superset.Status.Lifecycle.Upgrade = nil

	// Clear approval annotation if it was set. Use a metadata-only patch
	// to avoid coupling annotation cleanup to the full object state.
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
func (r *SupersetReconciler) reconcileTask(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	configChecksum string,
	topLevel *resolution.SharedInput,
	saName string,
	taskType string,
	suffix string,
	command []string,
) (time.Duration, bool, error) {
	log := logf.FromContext(ctx)
	childName := superset.Name + suffix
	resourceBaseName := childName

	// Build task config.
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

	// CreateOrUpdate the SupersetLifecycleTask child CR.
	child := &supersetv1alpha1.SupersetLifecycleTask{
		ObjectMeta: metav1.ObjectMeta{Name: childName, Namespace: superset.Namespace},
	}

	taskChecksum := computeChecksum(struct {
		SharedConfigChecksum string
		Config               string
		FlatSpec             supersetv1alpha1.FlatComponentSpec
		TaskType             string
		Command              []string
	}{
		SharedConfigChecksum: configChecksum,
		Config:               renderedConfig,
		FlatSpec:             flatSpec,
		TaskType:             taskType,
		Command:              command,
	})

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, child, func() error {
		if err := controllerutil.SetControllerReference(superset, child, r.Scheme); err != nil {
			return err
		}
		child.SetLabels(mergeLabels(child.GetLabels(), map[string]string{
			naming.LabelKeyName:      naming.LabelValueApp,
			naming.LabelKeyComponent: string(naming.ComponentInit),
			naming.LabelKeyParent:    superset.Name,
		}))
		child.Spec.FlatComponentSpec = flatSpec
		child.Spec.Type = taskType
		child.Spec.Command = command
		child.Spec.Config = renderedConfig
		child.Spec.ConfigChecksum = taskChecksum

		// Pass lifecycle-level settings.
		if superset.Spec.Lifecycle != nil {
			child.Spec.PodRetention = superset.Spec.Lifecycle.PodRetention
		} else {
			child.Spec.PodRetention = nil
		}
		child.Spec.MaxRetries = r.taskMaxRetries(superset, taskType)
		child.Spec.Timeout = r.taskTimeout(superset, taskType)

		return nil
	})
	if err != nil {
		return 0, false, fmt.Errorf("creating/updating SupersetLifecycleTask %s: %w", childName, err)
	}

	// Re-fetch to get latest status.
	if err := r.Get(ctx, client.ObjectKeyFromObject(child), child); err != nil {
		return 0, false, fmt.Errorf("fetching SupersetLifecycleTask %s status: %w", childName, err)
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
	switch taskType {
	case taskTypeMigrate:
		superset.Status.Lifecycle.Migrate = taskRef
	case taskTypeInit:
		superset.Status.Lifecycle.Init = taskRef
	}

	switch child.Status.State {
	case taskStateComplete:
		if child.Spec.ConfigChecksum != "" && child.Status.ConfigChecksum != child.Spec.ConfigChecksum {
			log.Info("Task complete for previous config, waiting for re-execution", "task", taskType)
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
				metav1.ConditionFalse, "TaskConfigChanged", fmt.Sprintf("%s task config changed, awaiting re-execution", taskType), superset.Generation)
			return taskRequeueInterval, false, nil
		}
		log.Info("Task complete", "task", taskType)
		return 0, true, nil

	case taskStateFailed:
		maxRetries := r.taskMaxRetriesValue(superset, taskType)
		if child.Status.Attempts >= maxRetries {
			log.Info("Task permanently failed", "task", taskType)
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
				metav1.ConditionFalse, "TaskFailed", fmt.Sprintf("%s: %s", taskType, child.Status.Message), superset.Generation)
			superset.Status.Phase = phaseInitializing
			return -1, false, nil
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

// taskNeeded determines if a task should run based on its strategy and the current state.
func (r *SupersetReconciler) taskNeeded(superset *supersetv1alpha1.Superset, taskType string, imageChanged bool) bool {
	strategy := r.taskStrategy(superset, taskType)
	switch strategy {
	case strategyNever:
		return false
	case strategyAlways:
		return true
	case strategyVersionChange:
		return imageChanged
	default:
		return imageChanged
	}
}

// resolveTaskNeeds determines which tasks need to run and prunes CRs for disabled tasks.
func (r *SupersetReconciler) resolveTaskNeeds(ctx context.Context, superset *supersetv1alpha1.Superset, imageChanged bool) (bool, bool, error) {
	migrateNeeded := r.taskNeeded(superset, taskTypeMigrate, imageChanged)
	initNeeded := r.taskNeeded(superset, taskTypeInit, imageChanged)

	if !migrateNeeded && r.taskStrategy(superset, taskTypeMigrate) == strategyNever {
		if err := r.deleteTaskCR(ctx, superset.Name+suffixMigrate, superset.Namespace); err != nil {
			return false, false, fmt.Errorf("deleting migrate task CR: %w", err)
		}
	}
	if !initNeeded && r.taskStrategy(superset, taskTypeInit) == strategyNever {
		if err := r.deleteTaskCR(ctx, superset.Name+suffixInit, superset.Namespace); err != nil {
			return false, false, fmt.Errorf("deleting init task CR: %w", err)
		}
	}

	return migrateNeeded, initNeeded, nil
}

func (r *SupersetReconciler) taskStrategy(superset *supersetv1alpha1.Superset, taskType string) string {
	if superset.Spec.Lifecycle == nil {
		return strategyVersionChange
	}
	switch taskType {
	case taskTypeMigrate:
		if superset.Spec.Lifecycle.Migrate != nil && superset.Spec.Lifecycle.Migrate.Strategy != nil {
			return *superset.Spec.Lifecycle.Migrate.Strategy
		}
	case taskTypeInit:
		if superset.Spec.Lifecycle.Init != nil && superset.Spec.Lifecycle.Init.Strategy != nil {
			return *superset.Spec.Lifecycle.Init.Strategy
		}
	}
	return strategyVersionChange
}

func (r *SupersetReconciler) taskMaxRetries(superset *supersetv1alpha1.Superset, taskType string) *int32 {
	if superset.Spec.Lifecycle == nil {
		return nil
	}
	switch taskType {
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

func getUpgradeMode(superset *supersetv1alpha1.Superset) string {
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.UpgradeMode != nil {
		return *superset.Spec.Lifecycle.UpgradeMode
	}
	return upgradeModeAutomatic
}

func getUpgradeStrategy(superset *supersetv1alpha1.Superset) string {
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.UpgradeStrategy != nil {
		return *superset.Spec.Lifecycle.UpgradeStrategy
	}
	return upgradeStrategyRolling
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

	// Check if any component Deployments still exist (waiting for GC).
	deployList := &appsv1.DeploymentList{}
	if err := r.List(ctx, deployList,
		client.InNamespace(superset.Namespace),
		client.MatchingLabels{naming.LabelKeyParent: superset.Name},
	); err != nil {
		return false, fmt.Errorf("listing deployments: %w", err)
	}

	if len(deployList.Items) > 0 {
		log.Info("Waiting for component Deployments to be garbage collected", "remaining", len(deployList.Items))
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
			dbType := "postgresql"
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
	if driver != nil && *driver == "mysql" {
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

		injected.Env = append(injected.Env, corev1.EnvVar{
			Name:  naming.EnvPythonPath,
			Value: configMountPath,
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
