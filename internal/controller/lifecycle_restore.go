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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// restoreSourceByID mirrors RestoreSourceSpec.Type's explicit-id variant; the
// default ("Latest") is handled as the fallthrough.
const restoreSourceByID = "BackupID"

// resolveRestoreArtifact resolves the backup artifact the restore should load
// from the operator-recorded catalog (status.lifecycle.backups). Returns nil
// when the requested artifact is not (yet) recorded.
func resolveRestoreArtifact(superset *supersetv1alpha1.Superset) *supersetv1alpha1.BackupArtifact {
	if superset.Status.Lifecycle == nil || len(superset.Status.Lifecycle.Backups) == 0 {
		return nil
	}
	src := superset.Spec.Lifecycle.Restore.Source
	catalog := superset.Status.Lifecycle.Backups
	if src.Type == restoreSourceByID && src.BackupID != nil {
		for i := range catalog {
			if catalog[i].ID == *src.BackupID {
				return &catalog[i]
			}
		}
		return nil
	}
	// Latest: catalog is maintained most-recent-first.
	return &catalog[0]
}

// restoreTargetIdentity returns a stable identifier for the restore target
// (metastore host/port/db/user). It is folded into the approval token so an
// approval is bound to a specific backup AND a specific target database.
func restoreTargetIdentity(superset *supersetv1alpha1.Superset) string {
	m := superset.Spec.Metastore
	if m == nil {
		return ""
	}
	host, db, user := "", "", ""
	if m.Host != nil {
		host = *m.Host
	}
	if m.Database != nil {
		db = *m.Database
	}
	if m.Username != nil {
		user = *m.Username
	}
	return fmt.Sprintf("%s:%d/%s@%s", host, defaultDBPort(m.Type), db, user)
}

// restoreApprovalToken is the deterministic annotation value required to approve
// a restore of a specific backup into a specific target. Mirrors
// upgradeApprovalToken — re-pointing the restore (different backup or target)
// changes the token and voids any prior approval.
func restoreApprovalToken(backupID, targetIdentity string) string {
	return computeChecksum(struct {
		BackupID       string `json:"backupID"`
		TargetIdentity string `json:"targetIdentity"`
	}{
		BackupID:       backupID,
		TargetIdentity: targetIdentity,
	})
}

// restoreContextMatches reports whether the recorded restore context still
// describes the backup/token the operator is about to act on. A mismatch means
// the selection changed and any prior approval must be re-collected.
func restoreContextMatches(rc *supersetv1alpha1.RestoreContext, backupID, approvalToken string) bool {
	return rc != nil && rc.BackupID == backupID && rc.ApprovalToken == approvalToken
}

// preRestoreSnapshotID derives the id for the safety snapshot taken of the
// current database before an approved restore overwrites it. Deterministic for
// a given (restore target artifact, current image) so re-running the restore
// Job reuses the same snapshot artifact.
func preRestoreSnapshotID(superset *supersetv1alpha1.Superset, restoreID, currentImage string) string {
	hash := strings.TrimPrefix(computeChecksum(struct {
		UID       string
		RestoreID string
		Image     string
	}{UID: string(superset.UID), RestoreID: restoreID, Image: currentImage}), "sha256:")
	if len(hash) > 12 {
		hash = hash[:12]
	}
	return "pre-restore-" + hash
}

// restoreInputs returns the restore-specific inputs that contribute to its task
// checksum. The selected artifact id is folded in so changing the source
// re-runs the restore (and forces fresh approval via the changed token).
func (r *SupersetReconciler) restoreInputs(superset *supersetv1alpha1.Superset, artifact *supersetv1alpha1.BackupArtifact) any {
	restore := superset.Spec.Lifecycle.Restore
	return struct {
		Image        supersetv1alpha1.ImageSpec
		BackupID     string
		Destination  supersetv1alpha1.BackupDestinationSpec
		SkipSnapshot bool
		Trigger      string
	}{
		Image:        resolveRestoreImage(superset),
		BackupID:     artifact.ID,
		Destination:  superset.Spec.Lifecycle.Backup.Destination,
		SkipSnapshot: restore.SkipPreRestoreSnapshot != nil && *restore.SkipPreRestoreSnapshot,
		Trigger:      derefOrDefault(restore.Trigger, ""),
	}
}

// buildRestoreCommand constructs the restore script: an optional pre-restore
// safety snapshot of the current database, followed by loading and applying the
// selected backup artifact. Returns the user's custom command if specified.
func (r *SupersetReconciler) buildRestoreCommand(superset *supersetv1alpha1.Superset) []string {
	restore := superset.Spec.Lifecycle.Restore
	if len(restore.Command) > 0 {
		return restore.Command
	}
	dbType := metastoreType(superset.Spec.Metastore)
	dest := superset.Spec.Lifecycle.Backup.Destination
	return []string{"/bin/sh", "-c", buildRestoreScript(dest, dbType)}
}

func buildRestoreScript(dest supersetv1alpha1.BackupDestinationSpec, dbType string) string {
	ext := backupArtifactExt(dbType)
	var b strings.Builder
	b.WriteString("set -e\n")
	b.WriteString(destinationToolingPreamble(dest.Type, dbType))

	// Pre-restore safety snapshot of the current database (unless skipped). The
	// snapshot is written under EnvBackupID (the operator sets this to the
	// pre-restore id); the artifact to restore is read from EnvRestoreID.
	fmt.Fprintf(&b, "if [ -z \"${%s:-}\" ]; then\n", naming.EnvSkipPreRestoreSnapshot)
	fmt.Fprintf(&b, "  %s | %s\n", backupDumpPipeline(dbType), destinationStoreCmd(dest.Type, ext))
	b.WriteString("fi\n")

	// Restore: stream the selected artifact into the metastore.
	b.WriteString(restoreLoadPipeline(dest, dbType, ext))
	fmt.Fprintf(&b, "\necho \"restore complete: ${%s}\"", naming.EnvRestoreID)
	return b.String()
}

// restoreLoadPipeline returns the command that loads the selected artifact into
// the metastore. PostgreSQL uses pg_restore --clean --if-exists (drops and
// recreates objects in place). MySQL drops and recreates the database, then
// applies the gunzipped dump.
func restoreLoadPipeline(dest supersetv1alpha1.BackupDestinationSpec, dbType, ext string) string {
	load := destinationLoadCmd(dest.Type, ext)
	if dbType == dbTypeMySQL {
		var b strings.Builder
		b.WriteString(`if [ -n "${SUPERSET_OPERATOR__DB_PASS:-}" ]; then export MYSQL_PWD="$SUPERSET_OPERATOR__DB_PASS"; fi
`)
		b.WriteString(`ESC_NAME=$(printf '%s' "$SUPERSET_OPERATOR__DB_NAME" | sed 's/` + "`" + `/` + "``" + `/g')
`)
		b.WriteString(`mysql -h "$SUPERSET_OPERATOR__DB_HOST" -P "$SUPERSET_OPERATOR__DB_PORT" -u "$SUPERSET_OPERATOR__DB_USER" -e "DROP DATABASE IF EXISTS \` + "`" + `${ESC_NAME}\` + "`" + `; CREATE DATABASE \` + "`" + `${ESC_NAME}\` + "`" + `;"
`)
		fmt.Fprintf(&b, `%s | gunzip | mysql -h "$SUPERSET_OPERATOR__DB_HOST" -P "$SUPERSET_OPERATOR__DB_PORT" -u "$SUPERSET_OPERATOR__DB_USER" "$SUPERSET_OPERATOR__DB_NAME"`, load)
		return b.String()
	}
	return fmt.Sprintf(`%s | PGPASSWORD="${SUPERSET_OPERATOR__DB_PASS:-}" pg_restore --clean --if-exists --no-owner --no-privileges `+
		`-h "$SUPERSET_OPERATOR__DB_HOST" -p "$SUPERSET_OPERATOR__DB_PORT" -U "$SUPERSET_OPERATOR__DB_USER" -d "$SUPERSET_OPERATOR__DB_NAME"`, load)
}

// collectRestoreEnvVars builds env vars for the restore task Job: the metastore
// target connection, the destination configuration, the selected artifact id,
// the pre-restore snapshot id (written as EnvBackupID), and the skip-snapshot
// flag.
func (r *SupersetReconciler) collectRestoreEnvVars(superset *supersetv1alpha1.Superset, artifact *supersetv1alpha1.BackupArtifact, currentImage string) []corev1.EnvVar {
	restore := superset.Spec.Lifecycle.Restore

	envs := createDatabaseEnvVars(superset.Spec.Metastore)
	envs = append(envs, backupDestinationEnvVars(superset.Spec.Lifecycle.Backup)...)
	envs = append(envs, corev1.EnvVar{Name: naming.EnvRestoreID, Value: artifact.ID})
	envs = append(envs, corev1.EnvVar{Name: naming.EnvBackupID, Value: preRestoreSnapshotID(superset, artifact.ID, currentImage)})
	if restore.SkipPreRestoreSnapshot != nil && *restore.SkipPreRestoreSnapshot {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvSkipPreRestoreSnapshot, Value: "1"})
	}
	return envs
}

// resolveRestoreImage selects the restore image, falling back to the backup
// image defaults (postgres:17-alpine / mysql:8-alpine) when not overridden.
func resolveRestoreImage(superset *supersetv1alpha1.Superset) supersetv1alpha1.ImageSpec {
	dbType := metastoreType(superset.Spec.Metastore)
	defaultRef := naming.DatabaseToolImagePostgres
	if dbType == dbTypeMySQL {
		defaultRef = naming.DatabaseToolImageMySQL
	}
	defRepo, defTag := splitImageRef(defaultRef)
	return resolveContainerImage(superset.Spec.Lifecycle.Restore.Image, defRepo, defTag)
}

// buildRestoreTaskFlatSpec builds the flat spec for the restore task (database-
// tool image, destination volumes, no Python config).
func (r *SupersetReconciler) buildRestoreTaskFlatSpec(
	superset *supersetv1alpha1.Superset,
	saName string,
	topLevel *resolution.SharedInput,
) supersetv1alpha1.FlatComponentSpec {
	restore := superset.Spec.Lifecycle.Restore
	dest := superset.Spec.Lifecycle.Backup.Destination
	instanceName := superset.Name + suffixRestore
	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))

	artifact := resolveRestoreArtifact(superset)
	if artifact == nil {
		// Should not happen: reconcileRestore gates on a resolvable artifact
		// before building the spec. Use an empty placeholder to stay nil-safe.
		artifact = &supersetv1alpha1.BackupArtifact{}
	}

	command := r.buildRestoreCommand(superset)
	comp := convertBackupComponent(restore.PodTemplate, command, dest)
	operatorInjected := &resolution.OperatorInjected{Env: r.collectRestoreEnvVars(superset, artifact, currentImage)}

	flat := resolution.ResolveComponentSpec(
		resolution.ComponentInit, topLevel, comp,
		podOperatorLabels(string(naming.ComponentInit), instanceName, superset.Name), operatorInjected,
	)

	one := int32(1)
	flatSpec := supersetv1alpha1.FlatComponentSpec{
		Image:              resolveRestoreImage(superset),
		Replicas:           &one,
		PodTemplate:        flatPodTemplate(flat),
		ServiceAccountName: saName,
	}
	flatSpec.Autoscaling = nil
	flatSpec.PodDisruptionBudget = nil
	return flatSpec
}

// recordBackupArtifact upserts the current backup into the status catalog
// (most-recent-first). Deterministic ids make this idempotent: re-running the
// same upgrade's backup updates the existing entry rather than duplicating it.
func recordBackupArtifact(superset *supersetv1alpha1.Superset) {
	if superset.Status.Lifecycle == nil {
		return
	}
	dbType := metastoreType(superset.Spec.Metastore)
	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	id := backupArtifactID(superset, currentImage)
	artifact := supersetv1alpha1.BackupArtifact{
		ID:        id,
		CreatedAt: nowPtr(),
		// The artifact captures the pre-upgrade state, so its contents match the
		// last successfully deployed image, not the in-flight target.
		Image:    superset.Status.LastLifecycleImage,
		Location: destinationLocation(superset.Spec.Lifecycle.Backup.Destination, id, backupArtifactExt(dbType)),
	}
	upsertBackupArtifact(superset.Status.Lifecycle, artifact)
}

// recordPreRestoreSnapshot records the safety snapshot taken before an approved
// restore into the catalog, so the overwritten state remains discoverable and
// itself restorable.
func recordPreRestoreSnapshot(superset *supersetv1alpha1.Superset, restoreID, currentImage string) {
	if superset.Status.Lifecycle == nil {
		return
	}
	restore := superset.Spec.Lifecycle.Restore
	if restore.SkipPreRestoreSnapshot != nil && *restore.SkipPreRestoreSnapshot {
		return
	}
	dbType := metastoreType(superset.Spec.Metastore)
	id := preRestoreSnapshotID(superset, restoreID, currentImage)
	upsertBackupArtifact(superset.Status.Lifecycle, supersetv1alpha1.BackupArtifact{
		ID:        id,
		CreatedAt: nowPtr(),
		Image:     currentImage,
		Location:  destinationLocation(superset.Spec.Lifecycle.Backup.Destination, id, backupArtifactExt(dbType)),
	})
}

// upsertBackupArtifact inserts the artifact at the front of the catalog, or
// updates it in place if its id is already present. The catalog is bounded to
// avoid unbounded status growth.
func upsertBackupArtifact(ls *supersetv1alpha1.LifecycleStatus, artifact supersetv1alpha1.BackupArtifact) {
	const maxCatalog = 20
	for i := range ls.Backups {
		if ls.Backups[i].ID == artifact.ID {
			ls.Backups[i] = artifact
			return
		}
	}
	ls.Backups = append([]supersetv1alpha1.BackupArtifact{artifact}, ls.Backups...)
	if len(ls.Backups) > maxCatalog {
		ls.Backups = ls.Backups[:maxCatalog]
	}
}

// reconcileRestore drives the approval-gated restore flow. It runs before the
// upgrade gate and the forward pipeline, so an approved restore both performs
// the in-place restore and — by advancing LastLifecycleImage to the current
// image on completion — dissolves the downgrade-blocked state for that
// transition. Returns (result, handled): when handled is true the caller must
// return result early; when false the forward pipeline proceeds (restore is
// absent or already complete for the selected artifact).
func (r *SupersetReconciler) reconcileRestore(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	topLevel *resolution.SharedInput,
	saName string,
) (lifecycleResult, bool, error) {
	log := logf.FromContext(ctx)

	if superset.Spec.Lifecycle == nil || superset.Spec.Lifecycle.Restore == nil {
		if err := r.cleanupRestore(ctx, superset); err != nil {
			return lifecycleResult{}, true, err
		}
		return lifecycleResult{}, false, nil
	}

	if superset.Status.Lifecycle == nil {
		superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{}
	}

	artifact := resolveRestoreArtifact(superset)
	if artifact == nil {
		// Nothing to restore: the requested artifact is not in the catalog.
		// Block rather than silently proceed, so the user notices the misref.
		msg := "restore requested but no matching backup artifact is recorded in status.lifecycle.backups"
		superset.Status.Phase = phaseBlocked
		superset.Status.Lifecycle.Phase = lifecyclePhaseBlocked
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionFalse, "NoBackupToRestore", msg, superset.Generation)
		return lifecycleTerminal(), true, nil
	}

	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	token := restoreApprovalToken(artifact.ID, restoreTargetIdentity(superset))
	command := r.buildRestoreCommand(superset)
	restoreChecksum := r.computeStepChecksum(string(superset.UID), taskTypeRestore, command, r.restoreInputs(superset, artifact))

	// Already complete for this artifact: finalize idempotently (the completion
	// may have been observed at the task layer on a prior reconcile) and let the
	// forward pipeline proceed. LastLifecycleImage was advanced on completion,
	// so the downgrade gate stays dissolved.
	if ref := taskStatusForType(superset, taskTypeRestore); ref != nil &&
		ref.State == taskStateComplete && ref.CompletedChecksum == restoreChecksum {
		r.finalizeRestore(superset, artifact, currentImage)
		return lifecycleResult{}, false, nil
	}

	// Record/refresh the restore context so a stale approval for a different
	// artifact or target is voided.
	if !restoreContextMatches(superset.Status.Lifecycle.RestoreApproval, artifact.ID, token) {
		superset.Status.Lifecycle.RestoreApproval = &supersetv1alpha1.RestoreContext{
			BackupID:      artifact.ID,
			FromImage:     artifact.Image,
			ApprovalToken: token,
			RequestedAt:   nowPtr(),
		}
	}

	// Approval gate: require the annotation to match the token for this exact
	// backup→target restore.
	annotations := superset.GetAnnotations()
	if annotations == nil || annotations[annotationApproveRestore] != token {
		log.Info("Restore awaiting approval", "backupID", artifact.ID)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeLifecycleComplete,
			metav1.ConditionFalse, "AwaitingRestoreApproval",
			fmt.Sprintf("Restore of backup %s detected. This OVERWRITES the metastore. Approve with: kubectl annotate superset %s %s=%s",
				artifact.ID, superset.Name, annotationApproveRestore, token),
			superset.Generation)
		superset.Status.Phase = phaseAwaitingApproval
		superset.Status.Lifecycle.Phase = lifecyclePhaseAwaitingApproval
		return lifecycleResult{}, true, nil
	}

	// Approved: drain component workloads so nothing writes to the metastore
	// while it is overwritten.
	if hasDrainableComponents(superset) {
		drained, err := r.drainComponents(ctx, superset)
		if err != nil {
			return lifecycleResult{}, true, fmt.Errorf("draining components for restore: %w", err)
		}
		if !drained {
			superset.Status.Lifecycle.Phase = lifecyclePhaseDraining
			superset.Status.Phase = phaseUpgrading
			return lifecycleWait(), true, nil
		}
	}

	superset.Status.Lifecycle.Phase = lifecyclePhaseRestoring
	superset.Status.Phase = phaseUpgrading

	result, err := r.reconcileLifecycleTask(ctx, superset, taskTypeRestore, suffixRestore, command, restoreChecksum, "", topLevel, saName)
	if err != nil {
		return lifecycleResult{}, true, fmt.Errorf("reconciling restore task: %w", err)
	}
	if !result.Complete {
		return result, true, nil
	}

	// Restore just completed: finalize and emit the completion event once.
	r.finalizeRestore(superset, artifact, currentImage)
	r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "RestoreComplete", "Lifecycle",
		"Restored backup %s into the metastore", artifact.ID)
	log.Info("Restore complete", "backupID", artifact.ID, "image", currentImage)
	return lifecycleResult{}, false, nil
}

// finalizeRestore performs the idempotent bookkeeping after a restore Job
// reaches Complete: record the pre-restore snapshot in the catalog, advance
// LastLifecycleImage to the current image (dissolving the downgrade-blocked
// state for this transition), and clear the restore approval context. Safe to
// call on every reconcile once the restore is complete.
func (r *SupersetReconciler) finalizeRestore(superset *supersetv1alpha1.Superset, artifact *supersetv1alpha1.BackupArtifact, currentImage string) {
	recordPreRestoreSnapshot(superset, artifact.ID, currentImage)
	superset.Status.LastLifecycleImage = currentImage
	if superset.Status.Lifecycle != nil {
		superset.Status.Lifecycle.RestoreApproval = nil
	}
}

// cleanupRestore removes restore task resources and clears restore status when
// spec.lifecycle.restore is absent. The leftover approval annotation is cleared
// best-effort so it cannot linger to approve a future restore.
func (r *SupersetReconciler) cleanupRestore(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	if err := r.deleteLifecycleTaskResources(ctx, superset, taskTypeRestore, suffixRestore); err != nil {
		return fmt.Errorf("deleting restore task resources: %w", err)
	}
	if superset.Status.Lifecycle != nil {
		superset.Status.Lifecycle.RestoreApproval = nil
	}
	return r.clearRestoreApprovalAnnotation(ctx, superset)
}

// clearRestoreApprovalAnnotation removes the restore approval annotation.
// Mirrors clearUpgradeApprovalAnnotation.
func (r *SupersetReconciler) clearRestoreApprovalAnnotation(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	annotations := superset.GetAnnotations()
	if annotations == nil {
		return nil
	}
	if _, ok := annotations[annotationApproveRestore]; !ok {
		return nil
	}
	patch := client.MergeFrom(superset.DeepCopy())
	delete(annotations, annotationApproveRestore)
	superset.SetAnnotations(annotations)
	if err := r.Patch(ctx, superset, patch); err != nil {
		return fmt.Errorf("clearing restore approval annotation: %w", err)
	}
	return nil
}
