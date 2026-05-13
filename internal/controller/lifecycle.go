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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

const (
	taskTypeMigrate = "Migrate"
	taskTypeInit    = "Init"
	taskTypeClone   = "Clone"
	taskTypeRotate  = "Rotate"

	suffixMigrate = "-migrate"
	suffixInit    = "-init"
	suffixClone   = "-clone"
	suffixRotate  = "-rotate"

	upgradeModeAutomatic  = "Automatic"
	upgradeModeSupervsied = "Supervised"

	lifecyclePhaseIdle             = "Idle"
	lifecyclePhaseCloning          = "Cloning"
	lifecyclePhaseDraining         = "Draining"
	lifecyclePhaseMigrating        = "Migrating"
	lifecyclePhaseRotating         = "Rotating"
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
		if err := r.cleanupMaintenanceResources(ctx, superset); err != nil {
			return 0, false, fmt.Errorf("cleaning up maintenance resources: %w", err)
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

	// Validate cron schedules early so invalid expressions are surfaced immediately.
	r.validateSchedules(superset)

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
	rotateEnabled := r.isTaskEnabled(superset, taskTypeRotate)
	initEnabled := r.isTaskEnabled(superset, taskTypeInit)

	if err := r.pruneDisabledTasks(ctx, superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled); err != nil {
		return 0, false, err
	}

	// If no tasks are enabled, lifecycle is complete.
	if !cloneEnabled && !migrateEnabled && !rotateEnabled && !initEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionTrue, "LifecycleComplete", "Lifecycle tasks completed successfully", superset.Generation)
		return 0, true, nil
	}

	// Fast path: if all enabled tasks already completed with matching checksums,
	// skip drain and pipeline entirely. This prevents unnecessary component
	// deletion on reconciles triggered by child CR creation.
	if r.allTasksStillComplete(superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled, configChecksum) {
		superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionTrue, "LifecycleComplete", "Lifecycle tasks completed successfully", superset.Generation)
		return 0, true, nil
	}

	// Spin up the maintenance page before drain (if configured).
	needsDrain := (cloneEnabled && r.taskRequiresDrain(superset, taskTypeClone)) ||
		(migrateEnabled && r.taskRequiresDrain(superset, taskTypeMigrate)) ||
		(rotateEnabled && r.taskRequiresDrain(superset, taskTypeRotate)) ||
		(initEnabled && r.taskRequiresDrain(superset, taskTypeInit))
	if isMaintenancePageEnabled(superset) && needsDrain {
		ready, err := r.reconcileMaintenancePageUp(ctx, superset)
		if err != nil {
			return 0, false, fmt.Errorf("reconciling maintenance page: %w", err)
		}
		if !ready {
			superset.Status.Lifecycle.Phase = lifecyclePhaseDraining
			return taskRequeueInterval, false, nil
		}
		// Switch Service selector to maintenance pods before drain begins.
		if err := r.reconcileWebServerService(ctx, superset); err != nil {
			return 0, false, fmt.Errorf("switching web-server Service to maintenance: %w", err)
		}
	}

	// Drain components if any enabled task requires it.
	if requeueAfter, drained, err := r.drainIfNeeded(ctx, superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled); err != nil {
		return 0, false, err
	} else if !drained {
		return requeueAfter, false, nil
	}

	// Orchestrate lifecycle pipeline: clone → migrate → rotate → init.
	if requeueAfter, complete, err := r.runLifecyclePipeline(ctx, superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled, imageChanged, configChecksum, topLevel, saName); err != nil {
		return 0, false, err
	} else if !complete {
		return requeueAfter, false, nil
	}

	// All tasks complete.
	return 0, true, r.finalizeLifecycle(ctx, superset, currentImage)
}

// finalizeLifecycle updates status and clears approval annotations after all
// lifecycle tasks complete. Maintenance teardown is handled separately in
// reconcileMaintenanceReturn(), gated on web-server readiness.
func (r *SupersetReconciler) finalizeLifecycle(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	currentImage string,
) error {
	superset.Status.LastLifecycleImage = currentImage
	superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
	superset.Status.Lifecycle.Upgrade = nil

	if annotations := superset.GetAnnotations(); annotations != nil {
		if _, ok := annotations[annotationApproveUpgrade]; ok {
			patch := client.MergeFrom(superset.DeepCopy())
			delete(annotations, annotationApproveUpgrade)
			superset.SetAnnotations(annotations)
			if err := r.Patch(ctx, superset, patch); err != nil {
				return fmt.Errorf("clearing approval annotation: %w", err)
			}
		}
	}

	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
		metav1.ConditionTrue, "LifecycleComplete", "Lifecycle tasks completed successfully", superset.Generation)
	return nil
}

// runLifecyclePipeline executes the sequential task pipeline (clone → migrate → init).
// Each task receives an incoming checksum from the previous task, creating a chain
// that automatically invalidates downstream tasks when upstream re-executes.
func (r *SupersetReconciler) runLifecyclePipeline(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	cloneEnabled, migrateEnabled, rotateEnabled, initEnabled, imageChanged bool,
	configChecksum string,
	topLevel *resolution.SharedInput,
	saName string,
) (time.Duration, bool, error) {
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

	if rotateEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseRotating

		rotateCmd := defaultRotateCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeRotate, rotateCmd, r.rotateInputs(superset))
		requeueAfter, complete, err := r.reconcileLifecycleTask(ctx, superset, taskTypeRotate, suffixRotate, rotateCmd, taskChecksum, configChecksum, topLevel, saName)
		if err != nil {
			return 0, false, fmt.Errorf("reconciling rotate task: %w", err)
		}
		if !complete {
			return requeueAfter, false, nil
		}
		incomingChecksum = r.getTaskStatusChecksum(ctx, superset, suffixRotate)
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
		// no task CR exists (GC or manual deletion), skip only when both image
		// and task checksum are unchanged. This prevents silent skips when
		// non-image inputs (trigger, cronSchedule, config) change.
		if superset.Status.LastLifecycleImage != "" &&
			superset.Status.LastLifecycleImage == resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset)) {
			storedChecksum := ""
			if superset.Status.Lifecycle != nil && superset.Status.Lifecycle.LastCompletedChecksums != nil {
				storedChecksum = superset.Status.Lifecycle.LastCompletedChecksums[taskType]
			}
			if storedChecksum != "" && storedChecksum == taskChecksum {
				log.Info("Task already completed in previous lifecycle run (no CR, inputs unchanged)", "task", taskType)
				return 0, true, nil
			}
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
	case taskTypeRotate:
		superset.Status.Lifecycle.Rotate = taskRef
	case taskTypeInit:
		superset.Status.Lifecycle.Init = taskRef
	}

	maxRetries := r.taskMaxRetriesValue(superset, taskType)

	switch child.Status.State {
	case taskStateComplete:
		if child.Status.ConfigChecksum == taskChecksum {
			if superset.Status.Lifecycle == nil {
				superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{}
			}
			if superset.Status.Lifecycle.LastCompletedChecksums == nil {
				superset.Status.Lifecycle.LastCompletedChecksums = make(map[string]string)
			}
			superset.Status.Lifecycle.LastCompletedChecksums[taskType] = taskChecksum
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
	cloneCmd := r.buildCloneCommand(superset)
	comp := convertCloneComponent(clone, cloneCmd)
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
	case taskTypeRotate:
		return suffixRotate
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

// isTaskEnabled returns true if the task is part of the lifecycle pipeline.
// A task is enabled when its spec exists (presence = enabled) and Disabled != true.
// Clone and rotate require their spec to be set; migrate/init are enabled by default.
func (r *SupersetReconciler) isTaskEnabled(superset *supersetv1alpha1.Superset, taskType string) bool {
	if superset.Spec.Lifecycle == nil {
		return taskType != taskTypeClone && taskType != taskTypeRotate
	}
	switch taskType {
	case taskTypeClone:
		if superset.Spec.Lifecycle.Clone == nil {
			return false
		}
		return superset.Spec.Lifecycle.Clone.Disabled == nil || !*superset.Spec.Lifecycle.Clone.Disabled
	case taskTypeRotate:
		if superset.Spec.Lifecycle.Rotate == nil {
			return false
		}
		return superset.Spec.Lifecycle.Rotate.Disabled == nil || !*superset.Spec.Lifecycle.Rotate.Disabled
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
func (r *SupersetReconciler) pruneDisabledTasks(ctx context.Context, superset *supersetv1alpha1.Superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled bool) error {
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
	if !rotateEnabled {
		if err := r.deleteTaskCR(ctx, superset.Name+suffixRotate, superset.Namespace); err != nil {
			return fmt.Errorf("deleting rotate task CR: %w", err)
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
		PostCloneSQL     []string
	}{
		Trigger:          derefOrDefault(clone.Trigger, ""),
		ScheduleTick:     r.scheduleTick(clone.CronSchedule),
		Source:           clone.Source,
		ExcludeTables:    clone.ExcludeTables,
		ExcludeTableData: clone.ExcludeTableData,
		PostCloneSQL:     clone.PostCloneSQL,
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

// rotateInputs returns the rotate-specific inputs: secret key references and trigger.
func (r *SupersetReconciler) rotateInputs(superset *supersetv1alpha1.Superset) any {
	trigger := ""
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Rotate != nil {
		trigger = derefOrDefault(superset.Spec.Lifecycle.Rotate.Trigger, "")
	}
	return struct {
		Trigger               string
		SecretKey             string
		SecretKeyFrom         *corev1.SecretKeySelector
		PreviousSecretKey     string
		PreviousSecretKeyFrom *corev1.SecretKeySelector
	}{
		Trigger:               trigger,
		SecretKey:             derefOrDefault(superset.Spec.SecretKey, ""),
		SecretKeyFrom:         superset.Spec.SecretKeyFrom,
		PreviousSecretKey:     derefOrDefault(superset.Spec.PreviousSecretKey, ""),
		PreviousSecretKeyFrom: superset.Spec.PreviousSecretKeyFrom,
	}
}

func defaultRotateCommand(superset *supersetv1alpha1.Superset) []string {
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Rotate != nil && len(superset.Spec.Lifecycle.Rotate.Command) > 0 {
		return superset.Spec.Lifecycle.Rotate.Command
	}
	return []string{"/bin/sh", "-c", "superset re-encrypt-secrets"}
}

// allTasksStillComplete checks whether all enabled tasks have already completed
// with checksums matching the current inputs. Used as a fast path to avoid
// unnecessary draining on reconciles where nothing has changed.
func (r *SupersetReconciler) allTasksStillComplete(
	superset *supersetv1alpha1.Superset,
	cloneEnabled, migrateEnabled, rotateEnabled, initEnabled bool,
	configChecksum string,
) bool {
	if superset.Status.Lifecycle == nil || superset.Status.Lifecycle.LastCompletedChecksums == nil {
		return false
	}
	checksums := superset.Status.Lifecycle.LastCompletedChecksums
	incomingChecksum := string(superset.UID)

	if cloneEnabled {
		cloneCmd := r.buildCloneCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeClone, cloneCmd, r.cloneInputs(superset))
		if checksums[taskTypeClone] != taskChecksum {
			return false
		}
		incomingChecksum = checksums[taskTypeClone]
	}

	if migrateEnabled {
		migrateCmd := defaultMigrateCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeMigrate, migrateCmd, r.migrateInputs(superset))
		if checksums[taskTypeMigrate] != taskChecksum {
			return false
		}
		incomingChecksum = checksums[taskTypeMigrate]
	}

	if rotateEnabled {
		rotateCmd := defaultRotateCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeRotate, rotateCmd, r.rotateInputs(superset))
		if checksums[taskTypeRotate] != taskChecksum {
			return false
		}
		incomingChecksum = checksums[taskTypeRotate]
	}

	if initEnabled {
		initCmd := defaultInitCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeInit, initCmd, r.initInputs(superset, configChecksum))
		if checksums[taskTypeInit] != taskChecksum {
			return false
		}
	}

	return true
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
	case taskTypeRotate:
		if superset.Spec.Lifecycle.Rotate != nil {
			return superset.Spec.Lifecycle.Rotate.MaxRetries
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
	case taskTypeRotate:
		if superset.Spec.Lifecycle.Rotate != nil {
			return superset.Spec.Lifecycle.Rotate.Timeout
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

	for _, sql := range clone.PostCloneSQL {
		fmt.Fprintf(&b, "\nPGPASSWORD=\"$SUPERSET_OPERATOR__DB_PASS\" psql -h \"$SUPERSET_OPERATOR__DB_HOST\" -p \"$SUPERSET_OPERATOR__DB_PORT\" -U \"$SUPERSET_OPERATOR__DB_USER\" \"$SUPERSET_OPERATOR__DB_NAME\" -c %q", sql)
	}

	return b.String()
}

func buildMySQLCloneScript(clone *supersetv1alpha1.CloneTaskSpec) string {
	var b strings.Builder
	b.WriteString(`set -e
mysql -h "$SUPERSET_OPERATOR__DB_HOST" -P "$SUPERSET_OPERATOR__DB_PORT" -u "$SUPERSET_OPERATOR__DB_USER" -p"$SUPERSET_OPERATOR__DB_PASS" -e "DROP DATABASE IF EXISTS $SUPERSET_OPERATOR__DB_NAME; CREATE DATABASE $SUPERSET_OPERATOR__DB_NAME;"
mysqldump -h "$SUPERSET_OPERATOR__CLONE_SRC_HOST" -P "$SUPERSET_OPERATOR__CLONE_SRC_PORT" -u "$SUPERSET_OPERATOR__CLONE_SRC_USER" -p"$SUPERSET_OPERATOR__CLONE_SRC_PASS" --single-transaction --routines --triggers`)

	for _, t := range clone.ExcludeTables {
		fmt.Fprintf(&b, ` --ignore-table="$SUPERSET_OPERATOR__CLONE_SRC_DB".%q`, t)
	}

	b.WriteString(` "$SUPERSET_OPERATOR__CLONE_SRC_DB" | mysql -h "$SUPERSET_OPERATOR__DB_HOST" -P "$SUPERSET_OPERATOR__DB_PORT" -u "$SUPERSET_OPERATOR__DB_USER" -p"$SUPERSET_OPERATOR__DB_PASS" "$SUPERSET_OPERATOR__DB_NAME"`)

	for _, sql := range clone.PostCloneSQL {
		fmt.Fprintf(&b, "\nmysql -h \"$SUPERSET_OPERATOR__DB_HOST\" -P \"$SUPERSET_OPERATOR__DB_PORT\" -u \"$SUPERSET_OPERATOR__DB_USER\" -p\"$SUPERSET_OPERATOR__DB_PASS\" \"$SUPERSET_OPERATOR__DB_NAME\" -e %q", sql)
	}

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
func convertCloneComponent(clone *supersetv1alpha1.CloneTaskSpec, command []string) *resolution.ComponentInput {
	var pt *supersetv1alpha1.PodTemplate
	if clone.PodTemplate != nil {
		pt = clone.PodTemplate
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
		case taskTypeRotate:
			if superset.Spec.Lifecycle.Rotate != nil {
				spec = &superset.Spec.Lifecycle.Rotate.BaseTaskSpec
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
	case taskTypeClone, taskTypeMigrate, taskTypeRotate:
		return true
	default:
		return false
	}
}
