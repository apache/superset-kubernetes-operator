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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
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
	upgradeModeSupervised = "Supervised"

	lifecyclePhaseCloning          = "Cloning"
	lifecyclePhaseDraining         = "Draining"
	lifecyclePhaseMigrating        = "Migrating"
	lifecyclePhaseRotating         = "Rotating"
	lifecyclePhaseInitializing     = "Initializing"
	lifecyclePhaseComplete         = "Complete"
	lifecyclePhaseRestoring        = "Restoring"
	lifecyclePhaseBlocked          = "Blocked"
	lifecyclePhaseAwaitingApproval = "AwaitingApproval"

	annotationApproveUpgrade = "superset.apache.org/approve-upgrade"

	dbTypePostgresql = "PostgreSQL"
	dbTypeMySQL      = "MySQL"

	defaultImageTag = "latest"

	phaseUpgrading        = "Upgrading"
	phaseBlocked          = "Blocked"
	phaseAwaitingApproval = "AwaitingApproval"
)

// lifecycleResult carries the outcome of a lifecycle reconcile step.
// Complete=true means the caller may proceed past lifecycle (to components).
// TerminalFailure=true means a permanent failure was reached: the parent will
// not requeue on its own, but callers should still consider scheduled re-runs
// (cron schedules) before giving up.
// RequeueAfter>0 means wait that long before reconciling again.
// Complete/TerminalFailure/RequeueAfter are mutually exclusive outcomes
// except that RequeueAfter may coexist with !Complete.
type lifecycleResult struct {
	RequeueAfter    time.Duration
	Complete        bool
	TerminalFailure bool
}

// lifecycleComplete is the "pipeline done, move on" result.
func lifecycleComplete() lifecycleResult { return lifecycleResult{Complete: true} }

// lifecycleWait returns a "not done, requeue after taskRequeueInterval" result.
// All lifecycle steps that poll task Jobs use the same interval.
func lifecycleWait() lifecycleResult { return lifecycleResult{RequeueAfter: taskRequeueInterval} }

// lifecycleCheckpoint returns a "status changed, persist before more side
// effects" result. The next reconcile may continue from the durable state.
func lifecycleCheckpoint() lifecycleResult { return lifecycleResult{RequeueAfter: time.Second} }

// lifecycleTerminal is a "permanent failure, don't requeue on own" result.
func lifecycleTerminal() lifecycleResult { return lifecycleResult{TerminalFailure: true} }

// reconcileLifecycle orchestrates lifecycle tasks (clone + migrate + rotate + init)
// as parent-owned Jobs and gates component deployment.
func (r *SupersetReconciler) reconcileLifecycle(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	configChecksum string,
	topLevel *resolution.SharedInput,
	saName string,
) (lifecycleResult, error) {

	// If lifecycle is disabled, prune orphans and mark settled. Settling
	// (advancing LastLifecycleImage) is what stops Supervised mode from
	// re-gating image changes when no task would actually run.
	if isLifecycleDisabled(superset) {
		if err := r.deleteLifecycleTaskResources(ctx, superset, taskTypeClone, suffixClone); err != nil {
			return lifecycleResult{}, fmt.Errorf("pruning clone task resources: %w", err)
		}
		if err := r.deleteLifecycleTaskResources(ctx, superset, taskTypeMigrate, suffixMigrate); err != nil {
			return lifecycleResult{}, fmt.Errorf("pruning migrate task resources: %w", err)
		}
		if err := r.deleteLifecycleTaskResources(ctx, superset, taskTypeRotate, suffixRotate); err != nil {
			return lifecycleResult{}, fmt.Errorf("pruning rotate task resources: %w", err)
		}
		if err := r.deleteLifecycleTaskResources(ctx, superset, taskTypeInit, suffixInit); err != nil {
			return lifecycleResult{}, fmt.Errorf("pruning init task resources: %w", err)
		}
		if err := r.cleanupMaintenanceResources(ctx, superset); err != nil {
			return lifecycleResult{}, fmt.Errorf("cleaning up maintenance resources: %w", err)
		}
		currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
		r.settleLifecycle(superset, currentImage, "LifecycleDisabled", "Lifecycle tasks are disabled")
		superset.Status.Lifecycle = nil
		return lifecycleComplete(), nil
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
	upgradeInProgress := lastImage != "" && currentImage != lastImage
	parentLifecyclePhase := lifecycleParentPhase(upgradeInProgress)

	// Check upgrade gates (version comparison, downgrade blocking, supervised approval).
	if gateResult, gated := r.checkUpgradeGates(ctx, superset, imageChanged, lastImage, currentImage); gated {
		return gateResult, nil
	}

	// Determine which tasks are enabled and prune orphans for disabled ones.
	cloneEnabled := r.isTaskEnabled(superset, taskTypeClone)
	migrateEnabled := r.isTaskEnabled(superset, taskTypeMigrate)
	rotateEnabled := r.isTaskEnabled(superset, taskTypeRotate)
	initEnabled := r.isTaskEnabled(superset, taskTypeInit)

	if err := r.pruneDisabledTasks(ctx, superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled); err != nil {
		return lifecycleResult{}, err
	}

	// If no tasks are enabled, the pipeline is already settled. Advancing
	// LastLifecycleImage here avoids re-gating Supervised image changes when
	// the user has disabled every task.
	if !cloneEnabled && !migrateEnabled && !rotateEnabled && !initEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
		r.settleLifecycle(superset, currentImage, "NoLifecycleTasks", "No lifecycle tasks configured")
		return lifecycleComplete(), nil
	}

	// Fast path: if all enabled tasks already completed with matching checksums,
	// skip drain and pipeline entirely. This prevents unnecessary component
	// disruption on steady-state reconciles.
	if r.allTasksStillComplete(superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled, configChecksum) {
		if superset.Status.Lifecycle.Phase != lifecyclePhaseRestoring {
			superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
		}
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionTrue, "LifecycleComplete", "Lifecycle tasks completed successfully", superset.Generation)
		return lifecycleComplete(), nil
	}

	// Spin up the maintenance page before drain (if configured).
	if result, err := r.prepareMaintenancePage(ctx, superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled, configChecksum, parentLifecyclePhase); err != nil {
		return lifecycleResult{}, err
	} else if !result.Complete {
		return result, nil
	}

	// Drain components if any task that will run requires it.
	drainResult, err := r.drainIfNeeded(ctx, superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled, configChecksum, parentLifecyclePhase)
	if err != nil {
		return lifecycleResult{}, err
	}
	if !drainResult.Complete {
		return drainResult, nil
	}

	// Orchestrate lifecycle pipeline: clone → migrate → rotate → init.
	pipelineResult, err := r.runLifecyclePipeline(ctx, superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled, upgradeInProgress, configChecksum, topLevel, saName)
	if err != nil {
		return lifecycleResult{}, err
	}
	if !pipelineResult.Complete {
		return pipelineResult, nil
	}

	// All tasks complete.
	r.finalizeLifecycle(superset, currentImage)
	return lifecycleComplete(), nil
}

// prepareMaintenancePage brings up the maintenance Deployment and switches
// the web-server Service selector before the drain step, if configured.
// Returns Complete=true if maintenance isn't needed or the page is serving.
func (r *SupersetReconciler) prepareMaintenancePage(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	cloneEnabled, migrateEnabled, rotateEnabled, initEnabled bool,
	configChecksum string,
	parentPhase string,
) (lifecycleResult, error) {
	if !isMaintenancePageEnabled(superset) ||
		webServerDesiredReplicas(superset) == 0 ||
		!r.lifecycleNeedsDrain(superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled, configChecksum) {
		return lifecycleComplete(), nil
	}
	hasWebWorkload, err := r.hasExistingWebServerWorkload(ctx, superset)
	if err != nil {
		return lifecycleResult{}, err
	}
	if !hasWebWorkload {
		return lifecycleComplete(), nil
	}
	ready, err := r.reconcileMaintenancePageUp(ctx, superset)
	if err != nil {
		return lifecycleResult{}, fmt.Errorf("reconciling maintenance page: %w", err)
	}
	if !ready {
		superset.Status.Lifecycle.Phase = lifecyclePhaseDraining
		superset.Status.Phase = parentPhase
		return lifecycleWait(), nil
	}
	if err := r.reconcileWebServerService(ctx, superset); err != nil {
		return lifecycleResult{}, fmt.Errorf("switching web-server Service to maintenance: %w", err)
	}
	return lifecycleComplete(), nil
}

func lifecycleParentPhase(upgradeInProgress bool) string {
	if upgradeInProgress {
		return phaseUpgrading
	}
	return phaseInitializing
}

// finalizeLifecycle updates status after all lifecycle tasks complete.
// Maintenance teardown is handled separately in reconcileMaintenanceReturn(),
// gated on web-server readiness. The upgrade approval annotation is cleared by
// the parent reconciler after status is persisted, so a status patch failure
// does not leave the annotation cleared (which would re-gate Supervised
// upgrades on the next reconcile while LastLifecycleImage was stale).
func (r *SupersetReconciler) finalizeLifecycle(superset *supersetv1alpha1.Superset, currentImage string) {
	if anyComponentEnabled(superset) {
		superset.Status.Lifecycle.Phase = lifecyclePhaseRestoring
	} else {
		superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
	}
	r.settleLifecycle(superset, currentImage, "LifecycleComplete", "Lifecycle tasks completed successfully")
}

// settleLifecycle records that the lifecycle pipeline has nothing more to do
// for the current image. Used by all completion paths (lifecycle disabled, no
// enabled tasks, finalize after a successful run). Advancing
// LastLifecycleImage is what prevents the upgrade gate from re-triggering on
// the next reconcile when no task would actually run.
func (r *SupersetReconciler) settleLifecycle(superset *supersetv1alpha1.Superset, currentImage, reason, message string) {
	superset.Status.LastLifecycleImage = currentImage
	if superset.Status.Lifecycle != nil {
		superset.Status.Lifecycle.Upgrade = nil
	}
	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
		metav1.ConditionTrue, reason, message, superset.Generation)
}

// clearUpgradeApprovalAnnotation removes the supervised upgrade approval
// annotation. Called by the parent reconciler after the post-lifecycle status
// patch succeeds, so a failed status patch never leaves the annotation
// cleared while LastLifecycleImage is still stale.
func (r *SupersetReconciler) clearUpgradeApprovalAnnotation(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	annotations := superset.GetAnnotations()
	if annotations == nil {
		return nil
	}
	if _, ok := annotations[annotationApproveUpgrade]; !ok {
		return nil
	}
	patch := client.MergeFrom(superset.DeepCopy())
	delete(annotations, annotationApproveUpgrade)
	superset.SetAnnotations(annotations)
	if err := r.Patch(ctx, superset, patch); err != nil {
		return fmt.Errorf("clearing upgrade approval annotation: %w", err)
	}
	return nil
}

// runLifecyclePipeline executes the sequential task pipeline (clone → migrate → rotate → init).
// Each task receives an incoming checksum from the previous task, creating a chain
// that automatically invalidates downstream tasks when upstream re-executes.
func (r *SupersetReconciler) runLifecyclePipeline(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	cloneEnabled, migrateEnabled, rotateEnabled, initEnabled, upgradeInProgress bool,
	configChecksum string,
	topLevel *resolution.SharedInput,
	saName string,
) (lifecycleResult, error) {
	incomingChecksum := string(superset.UID)

	if cloneEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseCloning
		if upgradeInProgress {
			superset.Status.Phase = phaseUpgrading
		} else {
			superset.Status.Phase = phaseInitializing
		}

		cloneCmd := r.buildCloneCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeClone, cloneCmd, r.cloneInputs(superset))
		result, err := r.reconcileLifecycleTask(ctx, superset, taskTypeClone, suffixClone, cloneCmd, taskChecksum, configChecksum, topLevel, saName)
		if err != nil {
			return lifecycleResult{}, fmt.Errorf("reconciling clone task: %w", err)
		}
		if !result.Complete {
			return result, nil
		}
		incomingChecksum = r.getTaskStatusChecksum(ctx, superset, suffixClone)
	}

	if migrateEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseMigrating
		if upgradeInProgress {
			superset.Status.Phase = phaseUpgrading
		} else {
			superset.Status.Phase = phaseInitializing
		}

		migrateCmd := defaultMigrateCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeMigrate, migrateCmd, r.migrateInputs(superset))
		result, err := r.reconcileLifecycleTask(ctx, superset, taskTypeMigrate, suffixMigrate, migrateCmd, taskChecksum, configChecksum, topLevel, saName)
		if err != nil {
			return lifecycleResult{}, fmt.Errorf("reconciling migrate task: %w", err)
		}
		if !result.Complete {
			return result, nil
		}
		incomingChecksum = r.getTaskStatusChecksum(ctx, superset, suffixMigrate)
	}

	if rotateEnabled {
		superset.Status.Lifecycle.Phase = lifecyclePhaseRotating
		if upgradeInProgress {
			superset.Status.Phase = phaseUpgrading
		} else {
			superset.Status.Phase = phaseInitializing
		}

		rotateCmd := defaultRotateCommand(superset)
		taskChecksum := r.computeStepChecksum(incomingChecksum, taskTypeRotate, rotateCmd, r.rotateInputs(superset))
		result, err := r.reconcileLifecycleTask(ctx, superset, taskTypeRotate, suffixRotate, rotateCmd, taskChecksum, configChecksum, topLevel, saName)
		if err != nil {
			return lifecycleResult{}, fmt.Errorf("reconciling rotate task: %w", err)
		}
		if !result.Complete {
			return result, nil
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
		result, err := r.reconcileLifecycleTask(ctx, superset, taskTypeInit, suffixInit, initCmd, taskChecksum, configChecksum, topLevel, saName)
		if err != nil {
			return lifecycleResult{}, fmt.Errorf("reconciling init task: %w", err)
		}
		if !result.Complete {
			return result, nil
		}
	}

	return lifecycleComplete(), nil
}

// checkUpgradeGates handles version comparison, downgrade blocking, and supervised approval.
// Returns (result, gated) — if gated is true, the caller should return early with result.
func (r *SupersetReconciler) checkUpgradeGates(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	imageChanged bool,
	lastImage, currentImage string,
) (lifecycleResult, bool) {
	log := logf.FromContext(ctx)

	if !imageChanged || lastImage == "" {
		return lifecycleResult{}, false
	}

	oldTag := tagFromImageRef(lastImage)
	newTag := tagFromImageRef(currentImage)
	direction := CompareVersions(oldTag, newTag)

	if direction == DirectionDowngrade {
		log.Info("Downgrade detected, blocking lifecycle", "from", oldTag, "to", newTag)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
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
		return lifecycleTerminal(), true
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
	if getUpgradeMode(superset) == upgradeModeSupervised {
		annotations := superset.GetAnnotations()
		if annotations == nil || annotations[annotationApproveUpgrade] != "true" {
			log.Info("Upgrade awaiting approval")
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
				metav1.ConditionFalse, "AwaitingApproval",
				fmt.Sprintf("Upgrade from %s to %s detected. Approve with: kubectl annotate superset %s %s=true",
					superset.Status.Lifecycle.Upgrade.FromVersion,
					superset.Status.Lifecycle.Upgrade.ToVersion,
					superset.Name, annotationApproveUpgrade),
				superset.Generation)
			superset.Status.Phase = phaseAwaitingApproval
			superset.Status.Lifecycle.Phase = lifecyclePhaseAwaitingApproval
			return lifecycleResult{}, true
		}
	}

	return lifecycleResult{}, false
}

// reconcileLifecycleTask creates or manages a single parent-owned lifecycle task
// Job and stores durable execution state on the parent Superset status.
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
) (lifecycleResult, error) {
	log := logf.FromContext(ctx)
	taskName := superset.Name + suffix

	// Build the task's flat spec and pod configuration.
	flatSpec, renderedConfig := r.buildTaskFlatSpec(superset, taskType, command, configChecksum, topLevel, saName)

	// Create the ConfigMap before the task Pod (only for tasks that need Python config).
	if renderedConfig != "" {
		if err := reconcileParentOwnedConfigMap(ctx, r.Client, r.Scheme, superset, renderedConfig, taskName); err != nil {
			return lifecycleResult{}, fmt.Errorf("reconciling ConfigMap for lifecycle task %s: %w", taskName, err)
		}
	}

	taskRef := ensureTaskStatus(superset, taskType)
	taskRef.DesiredChecksum = taskChecksum
	taskRef.MaxRetries = r.taskMaxRetriesValue(superset, taskType)
	r.projectScheduleStatus(superset, taskType, taskRef)

	if taskRef.State == taskStateComplete && taskRef.CompletedChecksum == taskChecksum {
		rememberCompletedTaskChecksum(superset, taskType, taskChecksum)
		log.Info("Task complete (checksum match, skipping)", "task", taskType)
		return lifecycleComplete(), nil
	}

	if taskRef.State == taskStateFailed && taskRef.CompletedChecksum == taskChecksum && taskRef.Attempts >= taskRef.MaxRetries {
		log.Info("Task permanently failed", "task", taskType)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionFalse, "TaskFailed", fmt.Sprintf("%s: %s", taskType, taskRef.Message), superset.Generation)
		return lifecycleTerminal(), nil
	}

	if taskRef.State == taskStateComplete || taskRef.State == taskStateFailed || (taskRef.CompletedChecksum != "" && taskRef.CompletedChecksum != taskChecksum) {
		log.Info("Task status is stale, resetting to re-run", "task", taskType,
			"completedChecksum", taskRef.CompletedChecksum, "expectedChecksum", taskChecksum)
		if err := r.deleteTaskJobs(ctx, superset, taskName); err != nil {
			return lifecycleResult{}, fmt.Errorf("deleting stale task jobs for %s: %w", taskName, err)
		}
		resetTaskStatusForRun(taskRef, taskChecksum, taskRef.MaxRetries)
		return lifecycleWait(), nil
	}

	result, err := r.reconcileLifecycleTaskJob(ctx, superset, taskName, taskType, &flatSpec, taskChecksum, taskRef)
	if err != nil {
		return lifecycleResult{}, err
	}
	if result.Complete {
		rememberCompletedTaskChecksum(superset, taskType, taskChecksum)
		return lifecycleComplete(), nil
	}
	return result, nil
}

// buildTaskFlatSpec constructs the fully-resolved FlatComponentSpec for a task Job.
// Clone tasks use a database-tool image; migrate/init use the Superset image.
// Returns (flatSpec, renderedConfig) — renderedConfig is empty for clone.
// taskPodRetention returns the retention spec for a task type.
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
// Clone is additionally gated on a valid CronSchedule: an invalid expression
// surfaces as a ScheduleValid=False condition + event, and the task is treated
// as disabled (the stale CR is pruned) until the expression is corrected.
func (r *SupersetReconciler) isTaskEnabled(superset *supersetv1alpha1.Superset, taskType string) bool {
	if superset.Spec.Lifecycle == nil {
		return taskType != taskTypeClone && taskType != taskTypeRotate
	}
	switch taskType {
	case taskTypeClone:
		if superset.Spec.Lifecycle.Clone == nil {
			return false
		}
		if superset.Spec.Lifecycle.Clone.Disabled != nil && *superset.Spec.Lifecycle.Clone.Disabled {
			return false
		}
		return cloneScheduleIsValid(superset.Spec.Lifecycle.Clone.CronSchedule)
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

// pruneDisabledTasks deletes task Jobs/config for disabled tasks and clears
// their projected status.
func (r *SupersetReconciler) pruneDisabledTasks(ctx context.Context, superset *supersetv1alpha1.Superset, cloneEnabled, migrateEnabled, rotateEnabled, initEnabled bool) error {
	if !cloneEnabled {
		if err := r.deleteLifecycleTaskResources(ctx, superset, taskTypeClone, suffixClone); err != nil {
			return fmt.Errorf("deleting clone task resources: %w", err)
		}
	}
	if !migrateEnabled {
		if err := r.deleteLifecycleTaskResources(ctx, superset, taskTypeMigrate, suffixMigrate); err != nil {
			return fmt.Errorf("deleting migrate task resources: %w", err)
		}
	}
	if !rotateEnabled {
		if err := r.deleteLifecycleTaskResources(ctx, superset, taskTypeRotate, suffixRotate); err != nil {
			return fmt.Errorf("deleting rotate task resources: %w", err)
		}
	}
	if !initEnabled {
		if err := r.deleteLifecycleTaskResources(ctx, superset, taskTypeInit, suffixInit); err != nil {
			return fmt.Errorf("deleting init task resources: %w", err)
		}
	}
	return nil
}

func (r *SupersetReconciler) deleteLifecycleTaskResources(ctx context.Context, superset *supersetv1alpha1.Superset, taskType, suffix string) error {
	taskName := superset.Name + suffix
	if err := r.deleteTaskJobs(ctx, superset, taskName); err != nil {
		return err
	}
	if taskType != taskTypeClone {
		if err := reconcileParentOwnedConfigMap(ctx, r.Client, r.Scheme, superset, "", taskName); err != nil {
			return err
		}
	}
	if superset.Status.Lifecycle != nil {
		switch taskType {
		case taskTypeClone:
			superset.Status.Lifecycle.Clone = nil
		case taskTypeMigrate:
			superset.Status.Lifecycle.Migrate = nil
		case taskTypeRotate:
			superset.Status.Lifecycle.Rotate = nil
		case taskTypeInit:
			superset.Status.Lifecycle.Init = nil
		}
		if superset.Status.Lifecycle.LastCompletedChecksums != nil {
			delete(superset.Status.Lifecycle.LastCompletedChecksums, taskType)
		}
	}
	return nil
}

// getTaskStatusChecksum retrieves the completed checksum from parent lifecycle
// status. Returns empty string if the task isn't complete.
func (r *SupersetReconciler) getTaskStatusChecksum(ctx context.Context, superset *supersetv1alpha1.Superset, suffix string) string {
	_ = ctx
	if superset.Status.Lifecycle == nil {
		return ""
	}
	var taskRef *supersetv1alpha1.TaskRefStatus
	switch suffix {
	case suffixClone:
		taskRef = superset.Status.Lifecycle.Clone
	case suffixMigrate:
		taskRef = superset.Status.Lifecycle.Migrate
	case suffixRotate:
		taskRef = superset.Status.Lifecycle.Rotate
	case suffixInit:
		taskRef = superset.Status.Lifecycle.Init
	}
	if taskRef != nil && taskRef.State == taskStateComplete {
		return taskRef.CompletedChecksum
	}
	if superset.Status.Lifecycle.LastCompletedChecksums != nil {
		switch suffix {
		case suffixClone:
			return superset.Status.Lifecycle.LastCompletedChecksums[taskTypeClone]
		case suffixMigrate:
			return superset.Status.Lifecycle.LastCompletedChecksums[taskTypeMigrate]
		case suffixRotate:
			return superset.Status.Lifecycle.LastCompletedChecksums[taskTypeRotate]
		case suffixInit:
			return superset.Status.Lifecycle.LastCompletedChecksums[taskTypeInit]
		}
	}
	return ""
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
	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	trigger := ""
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Init != nil {
		trigger = derefOrDefault(superset.Spec.Lifecycle.Init.Trigger, "")
	}
	return struct {
		Image          string
		ConfigChecksum string
		Trigger        string
	}{
		Image:          currentImage,
		ConfigChecksum: configChecksum,
		Trigger:        trigger,
	}
}

// rotateInputs returns the rotate-specific inputs: secret key references and trigger.
func (r *SupersetReconciler) rotateInputs(superset *supersetv1alpha1.Superset) any {
	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	trigger := ""
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Rotate != nil {
		trigger = derefOrDefault(superset.Spec.Lifecycle.Rotate.Trigger, "")
	}
	return struct {
		Image                 string
		Trigger               string
		SecretKey             string
		SecretKeyFrom         *corev1.SecretKeySelector
		PreviousSecretKey     string
		PreviousSecretKeyFrom *corev1.SecretKeySelector
	}{
		Image:                 currentImage,
		Trigger:               trigger,
		SecretKey:             derefOrDefault(superset.Spec.SecretKey, ""),
		SecretKeyFrom:         superset.Spec.SecretKeyFrom,
		PreviousSecretKey:     derefOrDefault(superset.Spec.PreviousSecretKey, ""),
		PreviousSecretKeyFrom: superset.Spec.PreviousSecretKeyFrom,
	}
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

func (r *SupersetReconciler) taskTimeoutValue(superset *supersetv1alpha1.Superset, taskType string) time.Duration {
	if timeout := r.taskTimeout(superset, taskType); timeout != nil {
		return timeout.Duration
	}
	return defaultInitTimeout
}

func (r *SupersetReconciler) taskRetentionPolicyValue(superset *supersetv1alpha1.Superset, taskType string) string {
	if retention := r.taskPodRetention(superset, taskType); retention != nil && retention.Policy != nil {
		return *retention.Policy
	}
	return defaultRetentionPolicy
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
