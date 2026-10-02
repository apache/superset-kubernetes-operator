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
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

// The default backup scripts share a prelude and a commit/retention tail and
// differ only in how they probe, dump, and verify the metastore.
//
// Guarantees, in order:
//   - Failures are reported: fail() writes the reason (tool stderr tail
//     included) to the termination message, which the operator surfaces in
//     status.lifecycle.backup.message instead of a bare "Job failed".
//   - The destination is proven writable before connecting to the database.
//   - Stale .partial files from earlier failed attempts of this instance are
//     removed; completed backups are never overwritten.
//   - On the first lifecycle run (nothing has settled yet) the database may
//     not exist yet (the create-database init container runs later, inside
//     the migrate Job), so a confirmed-absent database is skipped. If the
//     existence probe itself cannot run, the dump is attempted and fails
//     loudly if the database really is missing.
//   - The dump is written to a .partial file and read back in full before it
//     is renamed into place, so an interrupted, out-of-space, or corrupt run
//     never leaves a file that looks like a complete backup.
//   - After the rename the committed file is hashed again and compared with
//     the checksum of the verified file (post-commit integrity check).
//   - A JSON manifest (image tags, Alembic revision, size, SHA-256) is then
//     written atomically next to the dump; it is the commit marker that
//     retention and restore rely on.
//   - Retention (when configured) only deletes manifests of this Superset
//     (matched by UID) beyond keepLast, and only after the new backup has
//     been committed. Files without a manifest are never deleted.
//   - The result is reported through the termination message as JSON.
//
// All CR-derived values arrive via env vars, never interpolated into the
// script. umask 077 makes dumps owner-only (0600): they hold the whole
// metastore, and the volume may be mounted by other pods.
const backupScriptPrelude = `set -eu
umask 077
fail() {
  printf '%s\n' "$1" >&2
  printf '%s' "$1" | head -c 1024 2>/dev/null >/dev/termination-log || true
  exit 1
}
run() {
  step=$1
  shift
  if ! err=$("$@" 2>&1 >/dev/null); then
    fail "backup failed at $step: $(printf '%s' "$err" | tail -n 3 | tr '\n' ' ')"
  fi
}
skip() {
  echo "$1; nothing to back up"
  printf '{"skipped":"%s"}' "$1" 2>/dev/null >/dev/termination-log || true
  exit 0
}
H=$(printf '%s' "$SUPERSET_OPERATOR__DB_HOST" | tr -d '[:space:]')
P=$(printf '%s' "$SUPERSET_OPERATOR__DB_PORT" | tr -d '[:space:]')
U="$SUPERSET_OPERATOR__DB_USER"
DB="$SUPERSET_OPERATOR__DB_NAME"
DIR="${SUPERSET_OPERATOR__BACKUP_DIR:-}"
NAME="$SUPERSET_OPERATOR__INSTANCE_NAME"
[ -n "$DIR" ] || fail "backup failed: no backup destination is configured"
rm -f "$DIR/$NAME"_*.partial
PROBE="$DIR/${NAME}_write-test.partial"
( : >"$PROBE" ) 2>/dev/null || fail "backup failed: cannot write to $DIR; check the backup PVC access mode and the pod fsGroup/runAsUser"
rm -f "$PROBE"
`

const backupScriptCommit = `SUM=$(sha256sum "$OUT.partial" | cut -d' ' -f1)
mv "$OUT.partial" "$OUT"
SUM_COMMITTED=$(sha256sum "$OUT" | cut -d' ' -f1)
if [ "$SUM" != "$SUM_COMMITTED" ]; then
  mv "$OUT" "$OUT.corrupt"
  fail "backup failed at post-commit verification: checksum of $OUT changed after commit; kept as $OUT.corrupt"
fi
SIZE=$(wc -c <"$OUT" | tr -d '[:space:]')
CREATED=$(date -u +%Y-%m-%dT%H:%M:%SZ)
FILE=${OUT##*/}
BASE=${OUT%.*}
printf '{"version":1,"file":"%s","format":"%s","sizeBytes":%s,"sha256":"%s","alembicRevision":"%s","serverVersion":"%s","clientVersion":"%s","verification":"%s","createdAt":"%s","superset":%s}\n' \
  "$FILE" "$FORMAT" "$SIZE" "$SUM" "$REV" "$SERVER_VERSION" "$CLIENT_VERSION" "$VERIFY" "$CREATED" \
  "$SUPERSET_OPERATOR__BACKUP_METADATA" >"$BASE.json.partial"
mv "$BASE.json.partial" "$BASE.json"
if [ -n "${SUPERSET_OPERATOR__BACKUP_KEEP_LAST:-}" ]; then
  KEPT=0
  for M in $(ls -1 "$DIR/$NAME"_*.json 2>/dev/null | sort -r); do
    grep -q "\"uid\":\"$SUPERSET_OPERATOR__BACKUP_UID\"" "$M" || continue
    KEPT=$((KEPT + 1))
    [ "$KEPT" -gt "$SUPERSET_OPERATOR__BACKUP_KEEP_LAST" ] || continue
    B=${M%.json}
    rm -f -- "$M" "$B.dump" "$B.sql"
    echo "Pruned old backup $B"
  done
fi
printf '{"file":"%s","sizeBytes":%s,"sha256":"%s","alembicRevision":"%s","createdAt":"%s"}' \
  "$FILE" "$SIZE" "$SUM" "$REV" "$CREATED" 2>/dev/null >/dev/termination-log || true
echo "Backup written: $OUT ($SIZE bytes, sha256 $SUM, verified by $VERIFY)"`

// Postgres: custom format (compressed, selectively restorable with
// pg_restore). pg_dump refuses to dump a server newer than its own major
// version, so the preflight fails fast with the image tag to use instead.
// --lock-wait-timeout makes the dump fail instead of queueing behind a
// long-held lock (the dump runs before drain by default). Verification reads
// the whole archive back with pg_restore, which decompresses and checks every
// data block, not just the table of contents. -X skips psqlrc.
const backupPostgresScript = backupScriptPrelude + `export PGPASSWORD="${SUPERSET_OPERATOR__DB_PASS:-}"
if [ "${SUPERSET_OPERATOR__BACKUP_FIRST_RUN:-false}" = "true" ]; then
  ESC=$(printf '%s' "$DB" | sed "s/'/''/g")
  if EXISTS=$(psql -X -h "$H" -p "$P" -U "$U" -d postgres -v ON_ERROR_STOP=1 -tA \
      -c "SELECT 1 FROM pg_database WHERE datname = '$ESC'" 2>/dev/null) && [ "$EXISTS" != "1" ]; then
    skip "metastore database $DB does not exist yet"
  fi
fi
if ! SERVER_NUM=$(psql -X -h "$H" -p "$P" -U "$U" -d "$DB" -v ON_ERROR_STOP=1 -tA -c "SHOW server_version_num" 2>&1); then
  fail "backup failed at connect: $(printf '%s' "$SERVER_NUM" | tail -n 3 | tr '\n' ' ')"
fi
case "$SERVER_NUM" in
  '' | *[!0-9]*) fail "backup failed at connect: unexpected server_version_num '$SERVER_NUM'" ;;
esac
SERVER_VERSION=$((SERVER_NUM / 10000))
CLIENT_VERSION=$(pg_dump --version | sed -E 's/^[^0-9]*([0-9]+).*/\1/')
if [ "$SERVER_VERSION" -gt "$CLIENT_VERSION" ]; then
  fail "backup failed at preflight: pg_dump $CLIENT_VERSION cannot dump PostgreSQL $SERVER_VERSION; set spec.lifecycle.backup.image.tag to $SERVER_VERSION-alpine"
fi
REV=$(psql -X -h "$H" -p "$P" -U "$U" -d "$DB" -tA -c "SELECT string_agg(version_num, ',') FROM alembic_version" 2>/dev/null | tr -cd 'A-Za-z0-9_,' | cut -c1-128) || REV=""
OUT="$DIR/${NAME}_$(date -u +%Y%m%dT%H%M%SZ)_${SUPERSET_OPERATOR__BACKUP_FROM_TAG}.dump"
[ ! -e "$OUT" ] || fail "backup failed: refusing to overwrite existing backup $OUT"
run pg_dump pg_dump -h "$H" -p "$P" -U "$U" -d "$DB" --no-password --lock-wait-timeout=120000 -Fc -f "$OUT.partial"
run verification pg_restore -f /dev/null "$OUT.partial"
FORMAT=pg_dump-custom
VERIFY=pg_restore-full-read
` + backupScriptCommit

// MySQL: --single-transaction gives a consistent InnoDB snapshot without
// locking; --no-tablespaces avoids requiring the PROCESS privilege, which
// managed MySQL users commonly lack. The dump is plain SQL (no gzip pipe, so
// no reliance on pipefail). Completeness is verified via the trailing
// "-- Dump completed" marker; the SHA-256 pass reads every byte back. The
// first-run existence probe passes the database name as a hex literal so no
// quoting of the name is needed.
const backupMySQLScript = backupScriptPrelude + `if [ -n "${SUPERSET_OPERATOR__DB_PASS:-}" ]; then
  export MYSQL_PWD="$SUPERSET_OPERATOR__DB_PASS"
fi
if [ "${SUPERSET_OPERATOR__BACKUP_FIRST_RUN:-false}" = "true" ]; then
  HEX=$(printf '%s' "$DB" | od -An -tx1 | tr -d ' \n')
  if EXISTS=$(mysql -h "$H" -P "$P" -u "$U" -N -B \
      -e "SELECT 1 FROM information_schema.schemata WHERE schema_name = X'$HEX'" 2>/dev/null) && [ "$EXISTS" != "1" ]; then
    skip "metastore database $DB does not exist yet"
  fi
fi
if ! SERVER_VERSION=$(mysql -h "$H" -P "$P" -u "$U" -N -B -e "SELECT VERSION()" "$DB" 2>&1); then
  fail "backup failed at connect: $(printf '%s' "$SERVER_VERSION" | tail -n 3 | tr '\n' ' ')"
fi
SERVER_VERSION=$(printf '%s' "$SERVER_VERSION" | tr -cd 'A-Za-z0-9._-' | cut -c1-64)
CLIENT_VERSION=$(mysqldump --version | sed -E 's/.*Ver ([0-9][0-9.]*).*/\1/' | tr -cd '0-9.' | cut -c1-32)
REV=$(mysql -h "$H" -P "$P" -u "$U" -N -B -e "SELECT GROUP_CONCAT(version_num) FROM alembic_version" "$DB" 2>/dev/null | tr -cd 'A-Za-z0-9_,' | cut -c1-128) || REV=""
[ "$REV" != "NULL" ] || REV=""
OUT="$DIR/${NAME}_$(date -u +%Y%m%dT%H%M%SZ)_${SUPERSET_OPERATOR__BACKUP_FROM_TAG}.sql"
[ ! -e "$OUT" ] || fail "backup failed: refusing to overwrite existing backup $OUT"
run mysqldump mysqldump -h "$H" -P "$P" -u "$U" \
  --single-transaction --routines --triggers --no-tablespaces \
  --result-file="$OUT.partial" "$DB"
tail -n 1 "$OUT.partial" | grep -q '^-- Dump completed' || fail "backup failed at verification: $OUT.partial has no '-- Dump completed' trailer (truncated dump)"
FORMAT=mysqldump-sql
VERIFY=dump-completed-trailer
` + backupScriptCommit

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
		corev1.EnvVar{Name: naming.EnvBackupUID, Value: string(superset.UID)},
		corev1.EnvVar{Name: naming.EnvBackupMetadata, Value: backupManifestMetadata(superset)},
	)
	if b := backupSpec(superset); b != nil && b.Retention != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvBackupKeepLast, Value: strconv.Itoa(int(b.Retention.KeepLast))})
	}
	return envs
}

// backupManifestMetadata is the operator-known part of the backup manifest,
// embedded verbatim by the backup script. It is JSON-encoded here so values
// such as image references never need escaping in shell. Every field is
// fixed for the duration of a lifecycle run, which keeps the Job pod spec
// deterministic across reconciles.
func backupManifestMetadata(superset *supersetv1alpha1.Superset) string {
	encoded, err := json.Marshal(struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
		UID       string `json:"uid"`
		DBType    string `json:"dbType"`
		FromImage string `json:"fromImage"`
		ToImage   string `json:"toImage"`
	}{
		Name:      superset.Name,
		Namespace: superset.Namespace,
		UID:       string(superset.UID),
		DBType:    metastoreType(superset.Spec.Metastore),
		FromImage: superset.Status.LastLifecycleImage,
		ToImage:   resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset)),
	})
	if err != nil {
		// Marshaling a struct of strings cannot fail.
		return "{}"
	}
	return string(encoded)
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

// maxBackupRecords bounds status.lifecycle.backups.
const maxBackupRecords = 5

var (
	backupFilePattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$`)
	backupSHA256Pattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	backupRevisionPattern = regexp.MustCompile(`^[A-Za-z0-9_,]{0,128}$`)
)

// backupResult is the JSON the default backup script writes to its
// termination message: either a committed dump or the reason it skipped.
type backupResult struct {
	File            string `json:"file,omitempty"`
	SizeBytes       *int64 `json:"sizeBytes,omitempty"`
	SHA256          string `json:"sha256,omitempty"`
	AlembicRevision string `json:"alembicRevision,omitempty"`
	CreatedAt       string `json:"createdAt,omitempty"`
	Skipped         string `json:"skipped,omitempty"`
}

// parseBackupResult strictly decodes a backup termination message. The
// message is written by the backup container, which may run a user-supplied
// command, so it is untrusted input: unknown fields, malformed values, and
// oversized messages are rejected rather than partially recorded.
func parseBackupResult(message string) (backupResult, bool) {
	var res backupResult
	if message == "" || len(message) > 4096 {
		return res, false
	}
	dec := json.NewDecoder(strings.NewReader(message))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&res); err != nil || dec.More() {
		return res, false
	}
	if res.Skipped != "" {
		return res, res.File == "" && len(res.Skipped) <= 256
	}
	if !backupFilePattern.MatchString(res.File) || res.SizeBytes == nil || *res.SizeBytes < 0 ||
		!backupSHA256Pattern.MatchString(res.SHA256) || !backupRevisionPattern.MatchString(res.AlembicRevision) {
		return res, false
	}
	if _, err := time.Parse(time.RFC3339, res.CreatedAt); err != nil {
		return res, false
	}
	return res, true
}

// backupLocation renders where a dump lives, e.g.
// pvc://superset-backups/prod/superset_20260926T010203Z_6.0.0.dump.
func backupLocation(superset *supersetv1alpha1.Superset, file string) string {
	pvc := backupPVC(superset)
	if pvc == nil {
		return file
	}
	parts := []string{pvc.ClaimName}
	if sub := strings.Trim(derefOrDefault(pvc.SubPath, ""), "/"); sub != "" {
		parts = append(parts, sub)
	}
	parts = append(parts, file)
	return "pvc://" + strings.Join(parts, "/")
}

// recordBackupResult turns the backup Job's reported result into status: the
// task message, a status.lifecycle.backups entry for a committed dump, and an
// Event. Custom commands that report nothing keep the generic completion
// message. It is idempotent (entries are keyed by location), so replaying it
// after a failed status patch does not duplicate records.
func (r *SupersetReconciler) recordBackupResult(ctx context.Context, superset *supersetv1alpha1.Superset, job *batchv1.Job, taskRef *supersetv1alpha1.TaskRefStatus) error {
	message, err := r.taskTerminationMessage(ctx, superset, job)
	if err != nil {
		return err
	}
	if message == "" {
		return nil
	}
	res, ok := parseBackupResult(message)
	if !ok {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "BackupResultUnrecognized", "Lifecycle",
			"backup Job %s completed but reported an unrecognized result; no backup was recorded in status", job.Name)
		return nil
	}
	if res.Skipped != "" {
		taskRef.Message = "Skipped: " + res.Skipped
		r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "BackupSkipped", "Lifecycle", "Backup skipped: %s", res.Skipped)
		return nil
	}

	createdAt, _ := time.Parse(time.RFC3339, res.CreatedAt)
	record := supersetv1alpha1.BackupRecord{
		Location:        backupLocation(superset, res.File),
		SizeBytes:       *res.SizeBytes,
		SHA256:          res.SHA256,
		AlembicRevision: res.AlembicRevision,
		FromImage:       superset.Status.LastLifecycleImage,
		ToImage:         resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset)),
		CreatedAt:       metav1.NewTime(createdAt),
	}
	ls := superset.Status.Lifecycle
	records := make([]supersetv1alpha1.BackupRecord, 0, maxBackupRecords)
	records = append(records, record)
	for _, existing := range ls.Backups {
		if existing.Location != record.Location && len(records) < maxBackupRecords {
			records = append(records, existing)
		}
	}
	ls.Backups = records

	taskRef.Message = fmt.Sprintf("Backup written: %s (%d bytes, sha256 %s)", record.Location, record.SizeBytes, record.SHA256[:12])
	r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "BackupCompleted", "Lifecycle",
		"Verified backup written to %s (%d bytes, sha256 %s, alembic revision %q)",
		record.Location, record.SizeBytes, record.SHA256, record.AlembicRevision)
	return nil
}
