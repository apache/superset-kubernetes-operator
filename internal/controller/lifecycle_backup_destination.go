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

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

// This file is the seam between the backup/restore tasks and the storage
// backend. The backup command (lifecycle_backup.go) and restore command
// (lifecycle_restore.go) are storage-agnostic: they build a dump/load stream
// and delegate the "where does the artifact live" decisions — env vars,
// volumes, and the shell fragments that store/load/prune an artifact — to the
// helpers here. Adding a future backend (seed-to-database, CSI volume
// snapshot) means extending these switches and BackupDestinationSpec; no task
// logic changes.

const (
	destinationTypeVolume      = "Volume"
	destinationTypeObjectStore = "ObjectStore"

	backupVolumeName        = "superset-backup"
	defaultBackupVolumePath = "/backups"
)

// backupArtifactExt returns the dump file extension for the metastore engine.
// PostgreSQL uses pg_dump's compressed custom format (.dump); MySQL uses a
// gzip-compressed SQL stream (.sql.gz).
func backupArtifactExt(dbType string) string {
	if dbType == dbTypeMySQL {
		return "sql.gz"
	}
	return "dump"
}

// backupVolumePath returns the in-pod mount path for the Volume backend.
func backupVolumePath(dest supersetv1alpha1.BackupDestinationSpec) string {
	if dest.Volume != nil && dest.Volume.Path != nil && *dest.Volume.Path != "" {
		return *dest.Volume.Path
	}
	return defaultBackupVolumePath
}

// backupDestinationEnvVars returns destination-specific env vars (volume path,
// S3 URL/region/endpoint/credentials, retention) injected into backup and
// restore Jobs. The metastore connection (SUPERSET_OPERATOR__DB_*) is added by
// the caller via createDatabaseEnvVars.
func backupDestinationEnvVars(backup *supersetv1alpha1.BackupTaskSpec) []corev1.EnvVar {
	var envs []corev1.EnvVar
	dest := backup.Destination
	switch dest.Type {
	case destinationTypeVolume:
		envs = append(envs, corev1.EnvVar{Name: naming.EnvBackupVolumePath, Value: backupVolumePath(dest)})
	case destinationTypeObjectStore:
		if os := dest.ObjectStore; os != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvBackupS3URL, Value: os.URL})
			if os.Region != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvBackupS3Region, Value: *os.Region})
			}
			if os.Endpoint != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvBackupS3Endpoint, Value: *os.Endpoint})
			}
			if os.AccessKeyFrom != nil {
				envs = append(envs, corev1.EnvVar{
					Name:      naming.EnvBackupS3AccessKey,
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: os.AccessKeyFrom},
				})
			}
			if os.SecretKeyFrom != nil {
				envs = append(envs, corev1.EnvVar{
					Name:      naming.EnvBackupS3SecretKey,
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: os.SecretKeyFrom},
				})
			}
		}
	}
	if backup.Retention != nil && backup.Retention.KeepLast != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvBackupKeepLast, Value: fmt.Sprintf("%d", *backup.Retention.KeepLast)})
	}
	return envs
}

// backupDestinationVolumes returns the pod volumes and main-container volume
// mounts a backup/restore Job needs for the destination: a PVC mount for the
// Volume backend, nothing for ObjectStore (the AWS CLI streams over the
// network).
func backupDestinationVolumes(dest supersetv1alpha1.BackupDestinationSpec) ([]corev1.Volume, []corev1.VolumeMount) {
	if dest.Type != destinationTypeVolume || dest.Volume == nil {
		return nil, nil
	}
	return []corev1.Volume{{
			Name: backupVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: dest.Volume.ClaimName},
			},
		}}, []corev1.VolumeMount{{
			Name:      backupVolumeName,
			MountPath: backupVolumePath(dest),
		}}
}

// destinationToolingPreamble returns a shell fragment run before the dump that
// ensures backend tooling is available. The ObjectStore backend needs the AWS
// CLI, which is not in the default postgres:17-alpine / mysql:8-alpine images;
// the postgres alpine image can install it from the community repo. Users on
// MySQL or other base images should supply an image with `aws` preinstalled —
// the preamble is a no-op when `aws` is already on PATH.
func destinationToolingPreamble(destType, dbType string) string {
	if destType != destinationTypeObjectStore {
		return ""
	}
	if dbType == dbTypeMySQL {
		// The Oracle MySQL image is Oracle Linux based; we cannot reliably
		// install the AWS CLI there, so require a preinstalled aws.
		return `command -v aws >/dev/null 2>&1 || { echo "object-store backup requires the AWS CLI; provide a backup image with 'aws' installed" >&2; exit 1; }
`
	}
	return `command -v aws >/dev/null 2>&1 || apk add --no-cache aws-cli >/dev/null
`
}

// destinationArtifactRef returns the shell expression for the artifact path/URL
// of a given backup-id variable. destType selects volume file vs S3 key.
func destinationArtifactRef(destType, idVar, ext string) string {
	switch destType {
	case destinationTypeVolume:
		return fmt.Sprintf(`"$%s/${%s}.%s"`, naming.EnvBackupVolumePath, idVar, ext)
	case destinationTypeObjectStore:
		return fmt.Sprintf(`"$%s/${%s}.%s"`, naming.EnvBackupS3URL, idVar, ext)
	}
	return ""
}

// destinationStoreCmd returns a shell fragment that consumes the dump stream on
// stdin and writes it to the artifact named by $SUPERSET_OPERATOR__BACKUP_ID.
func destinationStoreCmd(destType, ext string) string {
	ref := destinationArtifactRef(destType, naming.EnvBackupID, ext)
	switch destType {
	case destinationTypeVolume:
		return fmt.Sprintf("cat > %s", ref)
	case destinationTypeObjectStore:
		return fmt.Sprintf("aws s3 cp - %s", ref)
	}
	return ""
}

// destinationLoadCmd returns a shell fragment that writes the artifact named by
// $SUPERSET_OPERATOR__RESTORE_ID to stdout. The operator always resolves a
// concrete id (never "latest") before launching the restore Job, so no listing
// is needed here.
func destinationLoadCmd(destType, ext string) string {
	ref := destinationArtifactRef(destType, naming.EnvRestoreID, ext)
	switch destType {
	case destinationTypeVolume:
		return fmt.Sprintf("cat %s", ref)
	case destinationTypeObjectStore:
		return fmt.Sprintf("aws s3 cp %s -", ref)
	}
	return ""
}

// destinationPruneCmd returns a shell fragment that prunes artifacts beyond
// $SUPERSET_OPERATOR__BACKUP_KEEP_LAST after a successful backup. The Volume
// backend prunes by modification time (chronologically correct). The
// ObjectStore backend prunes by lexical key order — backup ids share a stable
// prefix, so this approximates recency; precise object-store retention is left
// to bucket lifecycle policies.
func destinationPruneCmd(destType, ext string) string {
	switch destType {
	case destinationTypeVolume:
		return fmt.Sprintf(`if [ -n "${%[1]s:-}" ]; then
  ls -1t "$%[2]s"/*.%[3]s 2>/dev/null | tail -n +$(( %[1]s + 1 )) | xargs -r rm -f
fi`, naming.EnvBackupKeepLast, naming.EnvBackupVolumePath, ext)
	case destinationTypeObjectStore:
		return fmt.Sprintf(`if [ -n "${%[1]s:-}" ]; then
  aws s3 ls "$%[2]s/" | awk '{print $4}' | grep '\.%[3]s$' | sort | head -n -"$%[1]s" | while read -r obj; do
    [ -n "$obj" ] && aws s3 rm "$%[2]s/$obj"
  done
fi`, naming.EnvBackupKeepLast, naming.EnvBackupS3URL, ext)
	}
	return ""
}

// destinationLocation returns a human-readable location string recorded in the
// status backup catalog for a given artifact id.
func destinationLocation(dest supersetv1alpha1.BackupDestinationSpec, id, ext string) string {
	switch dest.Type {
	case destinationTypeVolume:
		return fmt.Sprintf("%s/%s.%s", backupVolumePath(dest), id, ext)
	case destinationTypeObjectStore:
		if dest.ObjectStore != nil {
			return fmt.Sprintf("%s/%s.%s", dest.ObjectStore.URL, id, ext)
		}
	}
	return id
}
