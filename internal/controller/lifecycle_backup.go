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
	"strings"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

const (
	backupVolumeName = "superset-operator-backup"
	backupMountPath  = "/backup"

	// backupPrefixMaxLen bounds the sanitized image-tag component of backup
	// file names so the full name stays well under common 255-byte limits.
	backupPrefixMaxLen = 128
)

// Postgres: dump the metastore in custom format (compressed, selectively
// restorable with pg_restore) to a .partial file, verify the archive, then
// rename it into place so an interrupted or out-of-space run never leaves a
// file that looks like a complete backup. An existing file is never
// overwritten. Stale .partial files from earlier
// failed attempts of this instance are removed first. On the first lifecycle
// run (nothing has settled yet) the database may not exist yet — the
// create-database init container runs later, inside the migrate Job — so a
// confirmed-absent database is skipped. If the existence probe itself cannot
// run (e.g. no access to the postgres database), the dump is attempted and
// fails loudly if the database really is missing. All CR-derived values
// arrive via env vars, never interpolated into the script. -X skips psqlrc.
// umask 077 makes dumps owner-only (0600): they hold the whole metastore, and
// the volume may be mounted by other pods.
const backupPostgresScript = `set -eu
umask 077
H=$(printf '%s' "$SUPERSET_OPERATOR__DB_HOST" | tr -d '[:space:]')
P=$(printf '%s' "$SUPERSET_OPERATOR__DB_PORT" | tr -d '[:space:]')
DIR="$SUPERSET_OPERATOR__BACKUP_DIR"
rm -f "$DIR/$SUPERSET_OPERATOR__INSTANCE_NAME"_*.partial
if [ "${SUPERSET_OPERATOR__BACKUP_FIRST_RUN:-false}" = "true" ]; then
  ESC=$(printf '%s' "$SUPERSET_OPERATOR__DB_NAME" | sed "s/'/''/g")
  if EXISTS=$(PGPASSWORD="${SUPERSET_OPERATOR__DB_PASS:-}" psql -X -h "$H" -p "$P" \
      -U "$SUPERSET_OPERATOR__DB_USER" -d postgres -v ON_ERROR_STOP=1 -tA \
      -c "SELECT 1 FROM pg_database WHERE datname = '$ESC'") && [ "$EXISTS" != "1" ]; then
    echo "Metastore database $SUPERSET_OPERATOR__DB_NAME does not exist yet; nothing to back up"
    exit 0
  fi
fi
OUT="$DIR/${SUPERSET_OPERATOR__INSTANCE_NAME}_$(date -u +%Y%m%dT%H%M%SZ)_${SUPERSET_OPERATOR__BACKUP_FROM_TAG}.dump"
if [ -e "$OUT" ]; then
  echo "Refusing to overwrite existing backup $OUT" >&2
  exit 1
fi
PGPASSWORD="${SUPERSET_OPERATOR__DB_PASS:-}" pg_dump -h "$H" -p "$P" \
  -U "$SUPERSET_OPERATOR__DB_USER" -d "$SUPERSET_OPERATOR__DB_NAME" \
  --no-password -Fc -f "$OUT.partial"
pg_restore --list "$OUT.partial" >/dev/null
mv "$OUT.partial" "$OUT"
echo "Backup written: $OUT"`

// MySQL: --single-transaction gives a consistent InnoDB snapshot without
// locking; --no-tablespaces avoids requiring the PROCESS privilege, which
// managed MySQL users commonly lack. The dump is plain SQL (no gzip pipe, so
// no reliance on pipefail). Completeness is verified via the trailing
// "-- Dump completed" marker. The first-run existence probe passes the
// database name as a hex literal so no quoting of the name is needed.
const backupMySQLScript = `set -eu
umask 077
H=$(printf '%s' "$SUPERSET_OPERATOR__DB_HOST" | tr -d '[:space:]')
P=$(printf '%s' "$SUPERSET_OPERATOR__DB_PORT" | tr -d '[:space:]')
DIR="$SUPERSET_OPERATOR__BACKUP_DIR"
rm -f "$DIR/$SUPERSET_OPERATOR__INSTANCE_NAME"_*.partial
if [ -n "${SUPERSET_OPERATOR__DB_PASS:-}" ]; then
  export MYSQL_PWD="$SUPERSET_OPERATOR__DB_PASS"
fi
if [ "${SUPERSET_OPERATOR__BACKUP_FIRST_RUN:-false}" = "true" ]; then
  HEX=$(printf '%s' "$SUPERSET_OPERATOR__DB_NAME" | od -An -tx1 | tr -d ' \n')
  if EXISTS=$(mysql -h "$H" -P "$P" -u "$SUPERSET_OPERATOR__DB_USER" -N -B \
      -e "SELECT 1 FROM information_schema.schemata WHERE schema_name = X'$HEX'") && [ "$EXISTS" != "1" ]; then
    echo "Metastore database $SUPERSET_OPERATOR__DB_NAME does not exist yet; nothing to back up"
    exit 0
  fi
fi
OUT="$DIR/${SUPERSET_OPERATOR__INSTANCE_NAME}_$(date -u +%Y%m%dT%H%M%SZ)_${SUPERSET_OPERATOR__BACKUP_FROM_TAG}.sql"
if [ -e "$OUT" ]; then
  echo "Refusing to overwrite existing backup $OUT" >&2
  exit 1
fi
mysqldump -h "$H" -P "$P" -u "$SUPERSET_OPERATOR__DB_USER" \
  --single-transaction --routines --triggers --no-tablespaces \
  --result-file="$OUT.partial" "$SUPERSET_OPERATOR__DB_NAME"
tail -n 1 "$OUT.partial" | grep -q '^-- Dump completed'
mv "$OUT.partial" "$OUT"
echo "Backup written: $OUT"`

func backupSpec(superset *supersetv1alpha1.Superset) *supersetv1alpha1.BackupTaskSpec {
	if superset.Spec.Lifecycle == nil {
		return nil
	}
	return superset.Spec.Lifecycle.Backup
}

func backupPVC(superset *supersetv1alpha1.Superset) *supersetv1alpha1.BackupPVCSource {
	b := backupSpec(superset)
	if b == nil || b.Destination == nil {
		return nil
	}
	return b.Destination.PersistentVolumeClaim
}

// buildBackupCommand returns the user override or the default dump script
// for the metastore type.
func buildBackupCommand(superset *supersetv1alpha1.Superset) []string {
	if b := backupSpec(superset); b != nil && len(b.Command) > 0 {
		return b.Command
	}
	script := backupPostgresScript
	if metastoreType(superset.Spec.Metastore) == dbTypeMySQL {
		script = backupMySQLScript
	}
	return []string{bootstrapShell, "-c", script}
}

// settledChecksumFor hashes the completed checksums of the cascade tasks. It
// deliberately excludes out-of-cascade tasks (backup) so that the backup
// baseline depends only on what has been applied to the database.
func settledChecksumFor(ls *supersetv1alpha1.LifecycleStatus) string {
	if ls == nil || len(ls.LastCompletedChecksums) == 0 {
		return ""
	}
	cascade := make(map[string]string, len(ls.LastCompletedChecksums))
	for _, desc := range lifecycleTaskDescriptors {
		if desc.OutOfCascade {
			continue
		}
		if v, ok := ls.LastCompletedChecksums[desc.TaskType]; ok {
			cascade[desc.TaskType] = v
		}
	}
	if len(cascade) == 0 {
		return ""
	}
	return computeChecksum(cascade)
}

// recordSettledChecksum advances status.lifecycle.settledChecksum once the
// pipeline has fully completed. It is only called from settle paths, so the
// value stays fixed while a lifecycle run is in progress.
func recordSettledChecksum(superset *supersetv1alpha1.Superset) {
	ls := superset.Status.Lifecycle
	if ls == nil {
		return
	}
	if v := settledChecksumFor(ls); v != "" {
		ls.SettledChecksum = v
	}
}

// backupFirstRun reports whether no lifecycle run has settled yet, in which
// case the metastore database may not exist yet.
func backupFirstRun(superset *supersetv1alpha1.Superset) bool {
	return superset.Status.Lifecycle == nil || superset.Status.Lifecycle.SettledChecksum == ""
}

// backupTaskChecksum identifies one backup per settled baseline. It changes
// only when a lifecycle run settles (new baseline) or the user bumps
// backup.trigger. Backup spec changes are intentionally excluded: a completed
// backup is not redone because its command or image changed, while a
// terminally failed backup still retries when its pod spec changes (the
// generic taskPodSpecChanged path).
func backupTaskChecksum(superset *supersetv1alpha1.Superset) string {
	trigger := ""
	if b := backupSpec(superset); b != nil {
		trigger = derefOrDefault(b.Trigger, "")
	}
	settled := ""
	if superset.Status.Lifecycle != nil {
		settled = superset.Status.Lifecycle.SettledChecksum
	}
	return computeChecksum(struct {
		UID      string
		TaskType string
		Settled  string
		Trigger  string
	}{
		UID:      string(superset.UID),
		TaskType: taskTypeBackup,
		Settled:  settled,
		Trigger:  trigger,
	})
}

// guardedTaskStarted reports whether a guarded task has already started (or
// attempted) a run for the given checksum. Backup must never start while a
// guarded task Job for the same run exists: the snapshot would race the
// mutation it is meant to precede.
func guardedTaskStarted(superset *supersetv1alpha1.Superset, taskType, taskChecksum string) bool {
	ref := taskStatusForType(superset, taskType)
	if ref == nil || ref.DesiredChecksum != taskChecksum {
		return false
	}
	return ref.StartedAt != nil || ref.Attempts > 0 || ref.State == taskStateRunning
}

// backupGatesStep reports whether the backup task must run (or confirm
// completion) before the given cascade step.
func (r *SupersetReconciler) backupGatesStep(superset *supersetv1alpha1.Superset, step lifecycleCascadeStep) bool {
	if !step.Desc.GuardedByBackup || !r.isTaskEnabled(superset, taskTypeBackup) {
		return false
	}
	if !r.taskNeedsRun(superset, step.Desc.TaskType, step.TaskChecksum) {
		return false
	}
	return !guardedTaskStarted(superset, step.Desc.TaskType, step.TaskChecksum)
}

// sanitizeBackupLabel maps an image tag to a safe file-name component: only
// [A-Za-z0-9._-] survive, other characters become '_', leading '.' and '-'
// are stripped (no hidden files, no option-like names), and the length is
// bounded. Image tags are not format-validated by the CRD, so this is what
// keeps a tag like "../x" from escaping the backup directory.
func sanitizeBackupLabel(s string) string {
	var b strings.Builder
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
			b.WriteRune(c)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.TrimLeft(b.String(), ".-")
	if len(out) > backupPrefixMaxLen {
		out = out[:backupPrefixMaxLen]
	}
	return out
}

// backupFromTag returns the file-name label for the image tag the database
// was on before this run ("initial" before the first run). Files are named
// "{parent}_{UTC timestamp}_{fromTag}" so that name order is chronological.
// The value is fixed for the duration of a run, which keeps the Job pod spec
// deterministic across reconciles.
func backupFromTag(superset *supersetv1alpha1.Superset) string {
	label := ""
	if last := superset.Status.LastLifecycleImage; last != "" {
		label = sanitizeBackupLabel(tagFromImageRef(last))
	}
	if label == "" {
		label = "initial"
	}
	return label
}

// collectBackupEnvVars builds the backup Job env: metastore connection (the
// structured DB_* vars, or DB_URI for custom commands against a uri/uriFrom
// metastore) plus the backup transport vars. Secrets are passed through as
// valueFrom.secretKeyRef; the operator never reads them.
func collectBackupEnvVars(superset *supersetv1alpha1.Superset) []corev1.EnvVar {
	spec := &superset.Spec
	isDev := isDevEnvironment(spec)
	envs := []corev1.EnvVar{{Name: naming.EnvInstanceName, Value: superset.Name}}

	if m := spec.Metastore; m != nil {
		switch {
		case isStructuredMetastore(m):
			envs = append(envs, structuredMetastoreEnvVars(m, isDev)...)
		case isDev && m.URI != nil:
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDatabaseURI, Value: *m.URI})
		case m.URIFrom != nil:
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvDatabaseURI,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: m.URIFrom},
			})
		}
	}

	if backupPVC(superset) != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvBackupDir, Value: backupMountPath})
	}
	firstRun := "false"
	if backupFirstRun(superset) {
		firstRun = "true"
	}
	envs = append(envs,
		corev1.EnvVar{Name: naming.EnvBackupFromTag, Value: backupFromTag(superset)},
		corev1.EnvVar{Name: naming.EnvBackupFirstRun, Value: firstRun},
	)
	return envs
}

// resolveBackupImage selects the DB-tool image for the backup Job, defaulting
// by metastore type like the seed and create-database helpers.
func resolveBackupImage(superset *supersetv1alpha1.Superset) supersetv1alpha1.ImageSpec {
	defaultRef := naming.SeedImagePostgres
	if metastoreType(superset.Spec.Metastore) == dbTypeMySQL {
		defaultRef = naming.SeedImageMySQL
	}
	defRepo, defTag := splitImageRef(defaultRef)
	var img *supersetv1alpha1.ContainerImageSpec
	if b := backupSpec(superset); b != nil {
		img = b.Image
	}
	return resolveContainerImage(img, defRepo, defTag)
}

// buildBackupTaskFlatSpec builds the flat spec for the backup task Job
// (database-tool image, no Python config). Like seed, it resolves the
// task's own podTemplate over the top-level podTemplate.
//
// The DB-tool images run as root by default; the dump runs as the image's
// built-in non-root user unless a UID is pinned, mirroring the
// create-database helper. When a destination PVC is mounted and no fsGroup
// is set, fsGroup defaults to that UID so the non-root process can write to
// the volume; OnRootMismatch avoids a recursive chown of a large backup
// volume on every run.
func (r *SupersetReconciler) buildBackupTaskFlatSpec(
	superset *supersetv1alpha1.Superset,
	saName string,
	topLevel *resolution.SharedInput,
) supersetv1alpha1.FlatComponentSpec {
	var podTemplate *supersetv1alpha1.PodTemplate
	if b := backupSpec(superset); b != nil {
		podTemplate = b.PodTemplate
	}
	comp := convertToolTaskComponent(podTemplate, buildBackupCommand(superset))

	operatorInjected := &resolution.OperatorInjected{Env: collectBackupEnvVars(superset)}
	if pvc := backupPVC(superset); pvc != nil {
		operatorInjected.Volumes = []corev1.Volume{{
			Name:                  backupVolumeName,
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.ClaimName},
		}}
		operatorInjected.VolumeMounts = []corev1.VolumeMount{{
			Name:      backupVolumeName,
			MountPath: backupMountPath,
			SubPath:   derefOrDefault(pvc.SubPath, ""),
		}}
	}

	instanceName := superset.Name + suffixBackup
	flat := resolution.ResolveComponentSpec(
		resolution.ComponentInit, topLevel, comp,
		podOperatorLabels(string(naming.ComponentInit), instanceName, superset.Name), operatorInjected,
	)

	pt := flatPodTemplate(flat)
	if pt.Container == nil {
		pt.Container = &supersetv1alpha1.ContainerTemplate{}
	}
	uid := helperNonRootUID(metastoreType(superset.Spec.Metastore))
	pt.Container.SecurityContext = helperNonRootSecurityContext(pt.Container.SecurityContext, pt.PodSecurityContext, uid)
	if backupPVC(superset) != nil {
		podSC := pt.PodSecurityContext.DeepCopy()
		if podSC == nil {
			podSC = &corev1.PodSecurityContext{}
		}
		if podSC.FSGroup == nil {
			podSC.FSGroup = &uid
			if podSC.FSGroupChangePolicy == nil {
				policy := corev1.FSGroupChangeOnRootMismatch
				podSC.FSGroupChangePolicy = &policy
			}
		}
		pt.PodSecurityContext = podSC
	}

	one := int32(1)
	return supersetv1alpha1.FlatComponentSpec{
		Image:              resolveBackupImage(superset),
		Replicas:           &one,
		PodTemplate:        pt,
		ServiceAccountName: saName,
	}
}
