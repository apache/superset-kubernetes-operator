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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// backupArtifactID derives a deterministic, stable backup id for the current
// upgrade. It must NOT vary across reconciles for the same upgrade (otherwise
// the task env would change and the Job would be recreated forever), so it is a
// pure function of the parent UID and the target image: a sanitized image tag
// plus a short hash. A new upgrade (new image) yields a new id; re-running the
// same upgrade reuses (overwrites) the same artifact, which is idempotent.
func backupArtifactID(superset *supersetv1alpha1.Superset, targetImage string) string {
	tag := sanitizeArtifactSegment(tagFromImageRef(targetImage))
	hash := strings.TrimPrefix(computeChecksum(struct {
		UID   string
		Image string
	}{UID: string(superset.UID), Image: targetImage}), "sha256:")
	if len(hash) > 12 {
		hash = hash[:12]
	}
	if tag == "" {
		return hash
	}
	return tag + "-" + hash
}

// sanitizeArtifactSegment keeps only characters safe for a filename/object key
// segment (lowercase alphanumerics, dot, dash, underscore), replacing the rest
// with a dash.
func sanitizeArtifactSegment(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// backupInputs returns the backup-specific inputs that contribute to its step
// checksum. The target image is folded in so a new upgrade re-runs the backup
// (capturing the pre-upgrade state), which then cascades to migrate.
func (r *SupersetReconciler) backupInputs(superset *supersetv1alpha1.Superset) any {
	backup := superset.Spec.Lifecycle.Backup
	targetImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	return struct {
		Image       supersetv1alpha1.ImageSpec
		TargetImage string
		BackupID    string
		Trigger     string
		Destination supersetv1alpha1.BackupDestinationSpec
		Retention   *supersetv1alpha1.BackupRetentionSpec
	}{
		Image:       resolveBackupImage(backup, metastoreType(superset.Spec.Metastore)),
		TargetImage: targetImage,
		BackupID:    backupArtifactID(superset, targetImage),
		Trigger:     derefOrDefault(backup.Trigger, ""),
		Destination: backup.Destination,
		Retention:   backup.Retention,
	}
}

// buildBackupCommand constructs the pg_dump/mysqldump streaming command that
// writes a logical dump of the metastore to the configured destination, then
// prunes old artifacts. Returns the user's custom command if specified.
func (r *SupersetReconciler) buildBackupCommand(superset *supersetv1alpha1.Superset) []string {
	backup := superset.Spec.Lifecycle.Backup
	if len(backup.Command) > 0 {
		return backup.Command
	}
	dbType := metastoreType(superset.Spec.Metastore)
	return []string{"/bin/sh", "-c", buildBackupScript(backup.Destination, dbType)}
}

func buildBackupScript(dest supersetv1alpha1.BackupDestinationSpec, dbType string) string {
	ext := backupArtifactExt(dbType)
	var b strings.Builder
	b.WriteString("set -e\n")
	b.WriteString(destinationToolingPreamble(dest.Type, dbType))
	b.WriteString(backupDumpPipeline(dbType))
	fmt.Fprintf(&b, " | %s\n", destinationStoreCmd(dest.Type, ext))
	if prune := destinationPruneCmd(dest.Type, ext); prune != "" {
		b.WriteString(prune)
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, `echo "backup complete: ${%s}"`, naming.EnvBackupID)
	return b.String()
}

// backupDumpPipeline returns the dump command that writes the metastore to
// stdout. PostgreSQL uses pg_dump's compressed custom format; MySQL uses a
// single-transaction logical dump piped through gzip. Passwords are passed via
// PGPASSWORD / MYSQL_PWD so they never appear in argv, matching the seed and
// create-database helpers.
func backupDumpPipeline(dbType string) string {
	if dbType == dbTypeMySQL {
		return `( if [ -n "${SUPERSET_OPERATOR__DB_PASS:-}" ]; then export MYSQL_PWD="$SUPERSET_OPERATOR__DB_PASS"; fi ; ` +
			`mysqldump --single-transaction --routines --triggers ` +
			`-h "$SUPERSET_OPERATOR__DB_HOST" -P "$SUPERSET_OPERATOR__DB_PORT" -u "$SUPERSET_OPERATOR__DB_USER" ` +
			`"$SUPERSET_OPERATOR__DB_NAME" ) | gzip`
	}
	return `PGPASSWORD="${SUPERSET_OPERATOR__DB_PASS:-}" pg_dump -Fc ` +
		`-h "$SUPERSET_OPERATOR__DB_HOST" -p "$SUPERSET_OPERATOR__DB_PORT" -U "$SUPERSET_OPERATOR__DB_USER" ` +
		`"$SUPERSET_OPERATOR__DB_NAME"`
}

// collectBackupEnvVars builds env vars for the backup task Job: the metastore
// target connection (SUPERSET_OPERATOR__DB_*), the destination configuration,
// and the resolved backup id.
func collectBackupEnvVars(superset *supersetv1alpha1.Superset) []corev1.EnvVar {
	backup := superset.Spec.Lifecycle.Backup
	targetImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))

	envs := createDatabaseEnvVars(superset.Spec.Metastore)
	envs = append(envs, backupDestinationEnvVars(backup)...)
	envs = append(envs, corev1.EnvVar{Name: naming.EnvBackupID, Value: backupArtifactID(superset, targetImage)})
	return envs
}

// resolveBackupImage determines the image for the backup/restore pod. Defaults
// are type-aware (postgres:17-alpine / mysql:8-alpine), mirroring seed; partial
// user specs inherit the type-appropriate default for omitted fields.
func resolveBackupImage(backup *supersetv1alpha1.BackupTaskSpec, dbType string) supersetv1alpha1.ImageSpec {
	defaultRef := naming.DatabaseToolImagePostgres
	if dbType == dbTypeMySQL {
		defaultRef = naming.DatabaseToolImageMySQL
	}
	defRepo, defTag := splitImageRef(defaultRef)
	return resolveContainerImage(backup.Image, defRepo, defTag)
}

// convertBackupComponent builds a ComponentInput for a backup/restore Job:
// it sets the command, the destination volume mounts on the main container, and
// the destination volumes on the pod.
func convertBackupComponent(pt *supersetv1alpha1.PodTemplate, command []string, dest supersetv1alpha1.BackupDestinationSpec) *resolution.ComponentInput {
	volumes, mounts := backupDestinationVolumes(dest)

	var ct *supersetv1alpha1.ContainerTemplate
	if pt != nil && pt.Container != nil {
		copied := *pt.Container
		ct = &copied
	} else {
		ct = &supersetv1alpha1.ContainerTemplate{}
	}
	ct.Command = command
	ct.VolumeMounts = append(ct.VolumeMounts, mounts...)

	var out *supersetv1alpha1.PodTemplate
	if pt != nil {
		copied := *pt
		out = &copied
	} else {
		out = &supersetv1alpha1.PodTemplate{}
	}
	out.Container = ct
	out.Volumes = append(out.Volumes, volumes...)

	return &resolution.ComponentInput{
		SharedInput: resolution.SharedInput{PodTemplate: out},
	}
}

// buildBackupTaskFlatSpec builds the flat spec for the backup task (database-tool
// image, destination volumes, no Python config).
func (r *SupersetReconciler) buildBackupTaskFlatSpec(
	superset *supersetv1alpha1.Superset,
	saName string,
	topLevel *resolution.SharedInput,
) supersetv1alpha1.FlatComponentSpec {
	backup := superset.Spec.Lifecycle.Backup
	dbType := metastoreType(superset.Spec.Metastore)
	instanceName := superset.Name + suffixBackup

	command := r.buildBackupCommand(superset)
	comp := convertBackupComponent(backup.PodTemplate, command, backup.Destination)
	operatorInjected := &resolution.OperatorInjected{Env: collectBackupEnvVars(superset)}

	flat := resolution.ResolveComponentSpec(
		resolution.ComponentInit, topLevel, comp,
		podOperatorLabels(string(naming.ComponentInit), instanceName, superset.Name), operatorInjected,
	)

	one := int32(1)
	flatSpec := supersetv1alpha1.FlatComponentSpec{
		Image:              resolveBackupImage(backup, dbType),
		Replicas:           &one,
		PodTemplate:        flatPodTemplate(flat),
		ServiceAccountName: saName,
	}
	flatSpec.Autoscaling = nil
	flatSpec.PodDisruptionBudget = nil
	return flatSpec
}
