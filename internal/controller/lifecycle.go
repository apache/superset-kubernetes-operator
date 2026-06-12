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

// Package controller implements the parent Superset controller and its
// lifecycle pipeline.
//
// # Lifecycle pipeline
//
// The lifecycle pipeline is a small state machine over four sequential tasks
// (seed → migrate → rotate → init) executed as parent-owned Jobs with
// backoffLimit: 0. Per-task wiring (suffix, phase, command builder, inputs
// builder, IsEnabled, BaseSpec accessor, status slot) lives in
// lifecycleTaskDescriptors (lifecycle_taskdescriptor.go); per-task spec
// construction lives in lifecycle_<task>.go; cascade math (per-task checksum
// computation, "all complete?", "what's pending?") lives in
// lifecycle_cascade.go; Job creation/state mechanics live in lifecycle_job.go.
// Adding a new task means appending a descriptor and providing a per-task
// file — no other code paths require changes.
//
// # Two phase enums
//
// Two phase enums coexist intentionally:
//
//   - lifecyclePhase* (Seeding, Draining, Migrating, Rotating, Initializing,
//     Restoring, Complete, Blocked, AwaitingApproval) is the lifecycle
//     sub-state, surfaced on Status.Lifecycle.Phase. It tells operators what
//     the lifecycle pipeline is currently doing.
//   - parent phase* (Upgrading, Initializing, Blocked, AwaitingApproval,
//     Running, Degraded, Suspended) is the high-level parent phase, surfaced
//     on Status.Phase. It tells users whether the Superset instance is up,
//     coming up, or upgrading.
//
// lifecycleParentPhase() bridges the two during pipeline execution.
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
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

const (
	taskTypeMigrate = "Migrate"
	taskTypeInit    = "Init"
	taskTypeSeed    = "Seed"
	taskTypeRotate  = "Rotate"
	taskTypeBackup  = "Backup"
	taskTypeRestore = "Restore"

	suffixMigrate = "-migrate"
	suffixInit    = "-init"
	suffixSeed    = "-seed"
	suffixRotate  = "-rotate"
	suffixBackup  = "-backup"
	suffixRestore = "-restore"

	upgradeModeAutomatic  = "Automatic"
	upgradeModeSupervised = "Supervised"

	lifecyclePhaseSeeding          = "Seeding"
	lifecyclePhaseBackingUp        = "BackingUp"
	lifecyclePhaseDraining         = "Draining"
	lifecyclePhaseMigrating        = "Migrating"
	lifecyclePhaseRotating         = "Rotating"
	lifecyclePhaseInitializing     = "Initializing"
	lifecyclePhaseComplete         = "Complete"
	lifecyclePhaseRestoring        = "Restoring"
	lifecyclePhaseBlocked          = "Blocked"
	lifecyclePhaseAwaitingApproval = "AwaitingApproval"

	annotationApproveUpgrade = "superset.apache.org/approve-upgrade"
	annotationApproveRestore = "superset.apache.org/approve-restore"

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

// reconcileLifecycle orchestrates lifecycle tasks (seed + migrate + rotate + init)
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
		for _, desc := range lifecycleTaskDescriptors {
			if err := r.deleteLifecycleTaskResources(ctx, superset, desc.TaskType, desc.Suffix); err != nil {
				return lifecycleResult{}, fmt.Errorf("pruning %s task resources: %w", desc.TaskType, err)
			}
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

	// Block the pipeline when the user configured seed with a malformed cron
	// schedule. Without this gate, IsEnabled would treat seed as disabled and
	// downstream tasks would silently run without the seedd database, which
	// is almost never what the user wants — typo in cron should not yield a
	// migrate/init against the wrong data set.
	if blockResult, blocked := r.gateOnInvalidSeedSchedule(superset); blocked {
		return blockResult, nil
	}

	// Approval-gated restore runs before the upgrade gate and the forward
	// pipeline. An approved restore performs the in-place restore and, by
	// advancing LastLifecycleImage on completion, dissolves the
	// downgrade-blocked state for the current transition. When restore is
	// absent or already complete for the selected artifact, the pipeline
	// proceeds normally.
	if restoreResult, handled, err := r.reconcileRestore(ctx, superset, topLevel, saName); err != nil {
		return lifecycleResult{}, err
	} else if handled {
		return restoreResult, nil
	}

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
	enabledTasks := r.enabledTaskTypes(superset)

	if err := r.pruneDisabledTasks(ctx, superset, enabledTasks); err != nil {
		return lifecycleResult{}, err
	}

	// If no tasks are enabled, the pipeline is already settled. Advancing
	// LastLifecycleImage here avoids re-gating Supervised image changes when
	// the user has disabled every task.
	if len(enabledTasks) == 0 {
		superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
		r.settleLifecycle(superset, currentImage, "NoLifecycleTasks", "No lifecycle tasks configured")
		return lifecycleComplete(), nil
	}

	// Fast path: if all enabled tasks already completed with matching checksums,
	// skip drain and pipeline entirely. This prevents unnecessary component
	// disruption on steady-state reconciles.
	if r.allTasksStillComplete(superset, configChecksum) {
		if superset.Status.Lifecycle.Phase != lifecyclePhaseRestoring {
			superset.Status.Lifecycle.Phase = lifecyclePhaseComplete
		}
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionTrue, "LifecycleComplete", "Lifecycle tasks completed successfully", superset.Generation)
		return lifecycleComplete(), nil
	}

	// Spin up the maintenance page before drain (if configured).
	if result, err := r.prepareMaintenancePage(ctx, superset, configChecksum, parentLifecyclePhase); err != nil {
		return lifecycleResult{}, err
	} else if !result.Complete {
		return result, nil
	}

	// Drain components if any task that will run requires it.
	drainResult, err := r.drainIfNeeded(ctx, superset, configChecksum, parentLifecyclePhase)
	if err != nil {
		return lifecycleResult{}, err
	}
	if !drainResult.Complete {
		return drainResult, nil
	}

	// Orchestrate lifecycle pipeline: seed → migrate → rotate → init.
	pipelineResult, err := r.runLifecyclePipeline(ctx, superset, upgradeInProgress, configChecksum, topLevel, saName)
	if err != nil {
		return lifecycleResult{}, err
	}
	if !pipelineResult.Complete {
		return pipelineResult, nil
	}

	// All tasks complete.
	logf.FromContext(ctx).Info("Lifecycle tasks complete, reconciling components", "image", currentImage)
	r.finalizeLifecycle(superset, currentImage)
	return lifecycleComplete(), nil
}

// enabledTaskTypes returns the set of lifecycle task types that are enabled
// for the current spec. The slice is in pipeline order.
func (r *SupersetReconciler) enabledTaskTypes(superset *supersetv1alpha1.Superset) []string {
	out := make([]string, 0, len(lifecycleTaskDescriptors))
	for _, desc := range lifecycleTaskDescriptors {
		if desc.OutOfBand {
			continue
		}
		if desc.IsEnabled(superset) {
			out = append(out, desc.TaskType)
		}
	}
	return out
}

// prepareMaintenancePage brings up the maintenance Deployment and switches
// the web-server Service selector before the drain step, if configured.
// Returns Complete=true if maintenance isn't needed or the page is serving.
func (r *SupersetReconciler) prepareMaintenancePage(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	configChecksum string,
	parentPhase string,
) (lifecycleResult, error) {
	if !isMaintenancePageEnabled(superset) ||
		webServerDesiredReplicas(superset) == 0 ||
		!r.lifecycleNeedsDrain(superset, configChecksum) {
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

// gateOnInvalidSeedSchedule returns a terminal result when the user
// configured seed with a malformed cron schedule. CRD pattern validation only
// covers the structural shape (5 whitespace-separated fields of allowed
// characters); out-of-range values like "99 99 99 99 99" still pass admission
// and only fail at runtime when robfig/cron parses them. Without this gate,
// IsEnabled would treat seed as disabled and downstream tasks would run
// against the wrong data set.
func (r *SupersetReconciler) gateOnInvalidSeedSchedule(superset *supersetv1alpha1.Superset) (lifecycleResult, bool) {
	if superset.Spec.Lifecycle == nil || superset.Spec.Lifecycle.Seed == nil {
		return lifecycleResult{}, false
	}
	seed := superset.Spec.Lifecycle.Seed
	if isDisabled(seed.Disabled) {
		return lifecycleResult{}, false
	}
	if seedScheduleIsValid(seed.CronSchedule) {
		return lifecycleResult{}, false
	}
	expr := ""
	if seed.CronSchedule != nil {
		expr = *seed.CronSchedule
	}
	message := fmt.Sprintf("seed cronSchedule %q is invalid; downstream lifecycle tasks blocked until corrected", expr)
	superset.Status.Lifecycle.Phase = lifecyclePhaseBlocked
	superset.Status.Phase = phaseBlocked
	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
		metav1.ConditionFalse, "InvalidCronSchedule", message, superset.Generation)
	r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "InvalidCronSchedule", "Lifecycle",
		"Lifecycle blocked: seed cronSchedule %q is invalid", expr)
	return lifecycleTerminal(), true
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

// runLifecyclePipeline executes the sequential task pipeline (seed → migrate → rotate → init).
// Each task receives an incoming checksum from the previous task, creating a chain
// that automatically invalidates downstream tasks when upstream re-executes.
func (r *SupersetReconciler) runLifecyclePipeline(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	upgradeInProgress bool,
	configChecksum string,
	topLevel *resolution.SharedInput,
	saName string,
) (lifecycleResult, error) {
	parentPhase := lifecycleParentPhase(upgradeInProgress)
	for _, step := range r.walkLifecycleCascade(superset, configChecksum) {
		superset.Status.Lifecycle.Phase = step.Desc.Phase
		superset.Status.Phase = parentPhase

		result, err := r.reconcileLifecycleTask(ctx, superset, step.Desc.TaskType, step.Desc.Suffix, step.Command, step.TaskChecksum, configChecksum, topLevel, saName)
		if err != nil {
			return lifecycleResult{}, fmt.Errorf("reconciling %s task: %w", step.Desc.TaskType, err)
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
	approvalToken := upgradeApprovalToken(lastImage, currentImage)
	log.V(2).Info("Compared image versions", "from", oldTag, "to", newTag, "direction", direction)

	if direction == DirectionDowngrade {
		log.Info("Downgrade detected, blocking lifecycle", "from", oldTag, "to", newTag)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionFalse, "DowngradeBlocked",
			fmt.Sprintf("Downgrade from %s to %s is not supported. Alembic migrations are forward-only.", oldTag, newTag),
			superset.Generation)
		superset.Status.Phase = phaseBlocked
		superset.Status.Lifecycle.Phase = lifecyclePhaseBlocked
		superset.Status.Lifecycle.Upgrade = &supersetv1alpha1.UpgradeContext{
			FromVersion:   oldTag,
			ToVersion:     newTag,
			Direction:     string(DirectionDowngrade),
			ApprovalToken: approvalToken,
		}
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "DowngradeBlocked", "Lifecycle",
			"Downgrade from %s to %s is not supported", oldTag, newTag)
		return lifecycleTerminal(), true
	}

	contextMatches := upgradeContextMatches(superset.Status.Lifecycle.Upgrade, oldTag, newTag, direction, approvalToken)

	// Non-semver tags (e.g. "latest", date stamps, digest pins) cannot be
	// ordered, so downgrade protection does not apply — a downgrade expressed
	// with such tags would run forward-only migrations against an older image
	// undetected. Surface this once per distinct image transition so operators
	// can intervene; the !contextMatches guard prevents per-reconcile spam.
	if direction == DirectionUnknown && !contextMatches {
		log.Info("Image version change could not be compared (non-semver tags); downgrade protection skipped",
			"from", oldTag, "to", newTag)
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "VersionComparisonSkipped", "Lifecycle",
			"Cannot compare image tags %q and %q (non-semver); downgrade protection does not apply", oldTag, newTag)
	}

	if !contextMatches {
		superset.Status.Lifecycle.Upgrade = &supersetv1alpha1.UpgradeContext{
			FromVersion:   oldTag,
			ToVersion:     newTag,
			Direction:     string(direction),
			ApprovalToken: approvalToken,
			StartedAt:     nowPtr(),
		}
	}

	// Supervised mode: check for approval annotation.
	if getUpgradeMode(superset) == upgradeModeSupervised {
		annotations := superset.GetAnnotations()
		if !contextMatches || annotations == nil || annotations[annotationApproveUpgrade] != approvalToken {
			log.Info("Upgrade awaiting approval")
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
				metav1.ConditionFalse, "AwaitingApproval",
				fmt.Sprintf("Upgrade from %s to %s detected. Approve with: kubectl annotate superset %s %s=%s",
					superset.Status.Lifecycle.Upgrade.FromVersion,
					superset.Status.Lifecycle.Upgrade.ToVersion,
					superset.Name, annotationApproveUpgrade, approvalToken),
				superset.Generation)
			superset.Status.Phase = phaseAwaitingApproval
			superset.Status.Lifecycle.Phase = lifecyclePhaseAwaitingApproval
			return lifecycleResult{}, true
		}
	}

	return lifecycleResult{}, false
}

func upgradeContextMatches(
	upgrade *supersetv1alpha1.UpgradeContext,
	fromVersion string,
	toVersion string,
	direction VersionDirection,
	approvalToken string,
) bool {
	return upgrade != nil &&
		upgrade.FromVersion == fromVersion &&
		upgrade.ToVersion == toVersion &&
		upgrade.Direction == string(direction) &&
		upgrade.ApprovalToken == approvalToken
}

func upgradeApprovalToken(fromImage, toImage string) string {
	return computeChecksum(struct {
		FromImage string `json:"fromImage"`
		ToImage   string `json:"toImage"`
	}{
		FromImage: fromImage,
		ToImage:   toImage,
	})
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
	bootstrapScript := ""
	if !taskUsesDatabaseToolImage(taskType) {
		bootstrapScript = effectiveLifecycleBootstrapScript(&superset.Spec)
	}

	// Create the ConfigMap before the task Pod (only for tasks that need Python config).
	if renderedConfig != "" || bootstrapScript != "" {
		if err := reconcileParentOwnedConfigMap(ctx, r.Client, r.Scheme, superset, renderedConfig, bootstrapScript, taskName, componentLabels(string(naming.ComponentInit), taskName)); err != nil {
			return lifecycleResult{}, fmt.Errorf("reconciling ConfigMap for lifecycle task %s: %w", taskName, err)
		}
	}

	taskRef := ensureTaskStatus(superset, taskType)
	taskRef.DesiredChecksum = taskChecksum
	taskRef.MaxRetries = r.taskMaxRetriesValue(superset, taskType)
	r.projectScheduleStatus(superset, taskType, taskRef)

	if taskRef.State == taskStateComplete && taskRef.CompletedChecksum == taskChecksum {
		rememberCompletedTaskChecksum(superset, taskType, taskChecksum)
		if taskType == taskTypeBackup {
			recordBackupArtifact(superset)
		}
		log.V(1).Info("Task complete (checksum match, skipping)", "task", taskType)
		return lifecycleComplete(), nil
	}

	if taskRef.State == taskStateFailed && taskRef.CompletedChecksum == taskChecksum && taskRef.Attempts >= taskRef.MaxRetries {
		// A terminally failed task is normally not retried. But if the user has
		// since changed the task's pod spec (e.g. fixed a securityContext or
		// bumped resources), that change may be exactly what fixes the failure —
		// so fall through to the stale-reset path below to give it another run.
		// Absent a pod-spec change we stay terminal, so genuine failures (bad
		// migration SQL, etc.) don't loop.
		podSpecChanged, err := r.taskPodSpecChanged(ctx, superset, taskName, &flatSpec)
		if err != nil {
			return lifecycleResult{}, fmt.Errorf("checking pod spec for failed task %s: %w", taskName, err)
		}
		if !podSpecChanged {
			log.V(1).Info("Task permanently failed", "task", taskType)
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
				metav1.ConditionFalse, "TaskFailed", fmt.Sprintf("%s: %s", taskType, taskRef.Message), superset.Generation)
			return lifecycleTerminal(), nil
		}
		log.Info("Task previously failed but pod spec changed; retrying", "task", taskType)
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
		if taskType == taskTypeBackup {
			recordBackupArtifact(superset)
		}
		return lifecycleComplete(), nil
	}
	return result, nil
}

// buildTaskFlatSpec constructs the fully-resolved FlatComponentSpec for a task Job.
// Seed tasks use a database-tool image; migrate/init use the Superset image.
// Returns (flatSpec, renderedConfig) — renderedConfig is empty for seed.
// taskPodRetention returns the retention spec for a task type.
func (r *SupersetReconciler) taskPodRetention(superset *supersetv1alpha1.Superset, taskType string) *supersetv1alpha1.PodRetentionSpec {
	if superset.Spec.Lifecycle == nil {
		return nil
	}
	if taskType == taskTypeSeed && superset.Spec.Lifecycle.Seed != nil && superset.Spec.Lifecycle.Seed.PodRetention != nil {
		return superset.Spec.Lifecycle.Seed.PodRetention
	}
	if taskType == taskTypeBackup && superset.Spec.Lifecycle.Backup != nil && superset.Spec.Lifecycle.Backup.PodRetention != nil {
		return superset.Spec.Lifecycle.Backup.PodRetention
	}
	return superset.Spec.Lifecycle.PodRetention
}

// pruneDisabledTasks deletes task Jobs/config for disabled tasks and clears
// their projected status. enabledTaskTypes is the result of enabledTaskTypes()
// — descriptors not represented in the slice are pruned.
func (r *SupersetReconciler) pruneDisabledTasks(ctx context.Context, superset *supersetv1alpha1.Superset, enabledTaskTypes []string) error {
	enabled := make(map[string]struct{}, len(enabledTaskTypes))
	for _, t := range enabledTaskTypes {
		enabled[t] = struct{}{}
	}
	for _, desc := range lifecycleTaskDescriptors {
		if desc.OutOfBand {
			// Out-of-band tasks (restore) manage their own resource lifecycle.
			continue
		}
		if _, ok := enabled[desc.TaskType]; ok {
			continue
		}
		if err := r.deleteLifecycleTaskResources(ctx, superset, desc.TaskType, desc.Suffix); err != nil {
			return fmt.Errorf("deleting %s task resources: %w", desc.TaskType, err)
		}
	}
	return nil
}

func (r *SupersetReconciler) deleteLifecycleTaskResources(ctx context.Context, superset *supersetv1alpha1.Superset, taskType, suffix string) error {
	taskName := superset.Name + suffix
	if err := r.deleteTaskJobs(ctx, superset, taskName); err != nil {
		return err
	}
	if !taskUsesDatabaseToolImage(taskType) {
		if err := reconcileParentOwnedConfigMap(ctx, r.Client, r.Scheme, superset, "", "", taskName, nil); err != nil {
			return err
		}
	}
	if superset.Status.Lifecycle != nil {
		if desc := lifecycleTaskDescriptorByType(taskType); desc != nil {
			*desc.TaskRef(superset.Status.Lifecycle) = nil
		}
		if superset.Status.Lifecycle.LastCompletedChecksums != nil {
			delete(superset.Status.Lifecycle.LastCompletedChecksums, taskType)
		}
	}
	return nil
}

func (r *SupersetReconciler) taskMaxRetries(superset *supersetv1alpha1.Superset, taskType string) *int32 {
	desc := lifecycleTaskDescriptorByType(taskType)
	if desc == nil {
		return nil
	}
	if base := desc.BaseSpec(superset); base != nil {
		return base.MaxRetries
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
	desc := lifecycleTaskDescriptorByType(taskType)
	if desc == nil {
		return nil
	}
	if base := desc.BaseSpec(superset); base != nil {
		return base.Timeout
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
// Defaults: seed=true (DROP DATABASE needs no connections), migrate=true
// (schema changes risk deadlocks), init=false (roles/permissions are safe).
func (r *SupersetReconciler) taskRequiresDrain(superset *supersetv1alpha1.Superset, taskType string) bool {
	desc := lifecycleTaskDescriptorByType(taskType)
	if desc == nil {
		return false
	}
	if base := desc.BaseSpec(superset); base != nil && base.RequiresDrain != nil {
		return *base.RequiresDrain
	}
	return desc.DrainsByDefault
}
