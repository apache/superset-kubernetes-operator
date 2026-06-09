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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

func volumeDest() supersetv1alpha1.BackupDestinationSpec {
	return supersetv1alpha1.BackupDestinationSpec{
		Type:   destinationTypeVolume,
		Volume: &supersetv1alpha1.VolumeBackupSpec{ClaimName: "backups-pvc"},
	}
}

func objectStoreDest() supersetv1alpha1.BackupDestinationSpec {
	return supersetv1alpha1.BackupDestinationSpec{
		Type:        destinationTypeObjectStore,
		ObjectStore: &supersetv1alpha1.ObjectStoreBackupSpec{URL: "s3://bucket/superset"},
	}
}

// TestBuildBackupScript covers the dump pipeline, store fragment, and retention
// prune across engines and destinations.
func TestBuildBackupScript(t *testing.T) {
	t.Run("postgres volume", func(t *testing.T) {
		script := buildBackupScript(volumeDest(), dbTypePostgresql)
		for _, want := range []string{"set -e", "pg_dump -Fc", "cat > \"$" + naming.EnvBackupVolumePath, "backup complete"} {
			if !strings.Contains(script, want) {
				t.Errorf("expected %q in:\n%s", want, script)
			}
		}
		if strings.Contains(script, "apk add") {
			t.Error("volume backend must not install the AWS CLI")
		}
	})

	t.Run("postgres object store installs aws and streams", func(t *testing.T) {
		script := buildBackupScript(objectStoreDest(), dbTypePostgresql)
		if !strings.Contains(script, "apk add --no-cache aws-cli") {
			t.Errorf("expected aws-cli install preamble in:\n%s", script)
		}
		if !strings.Contains(script, "aws s3 cp - \"$"+naming.EnvBackupS3URL) {
			t.Errorf("expected aws s3 cp store in:\n%s", script)
		}
	})

	t.Run("mysql volume dumps via gzip", func(t *testing.T) {
		script := buildBackupScript(volumeDest(), dbTypeMySQL)
		if !strings.Contains(script, "mysqldump --single-transaction") {
			t.Errorf("expected mysqldump in:\n%s", script)
		}
		if !strings.Contains(script, "| gzip") {
			t.Errorf("expected gzip in:\n%s", script)
		}
		if !strings.Contains(script, `export MYSQL_PWD`) {
			t.Errorf("expected MYSQL_PWD export (password not in argv) in:\n%s", script)
		}
	})

	t.Run("retention prunes only when keepLast set", func(t *testing.T) {
		// The prune fragment is wired from destination env, not the script
		// builder; assert the destination prune command shape directly.
		prune := destinationPruneCmd(destinationTypeVolume, "dump")
		if !strings.Contains(prune, "ls -1t") || !strings.Contains(prune, "rm -f") {
			t.Errorf("expected mtime-based volume prune, got: %s", prune)
		}
		if !strings.Contains(prune, naming.EnvBackupKeepLast) {
			t.Errorf("expected keepLast guard, got: %s", prune)
		}
	})
}

// TestBuildRestoreScript covers the pre-restore snapshot toggle and the
// engine-specific load pipeline.
func TestBuildRestoreScript(t *testing.T) {
	t.Run("postgres uses pg_restore --clean and guards snapshot", func(t *testing.T) {
		script := buildRestoreScript(volumeDest(), dbTypePostgresql)
		if !strings.Contains(script, "pg_restore --clean --if-exists") {
			t.Errorf("expected pg_restore --clean --if-exists in:\n%s", script)
		}
		if !strings.Contains(script, "if [ -z \"${"+naming.EnvSkipPreRestoreSnapshot) {
			t.Errorf("expected pre-restore snapshot guard in:\n%s", script)
		}
		// The snapshot stores under BACKUP_ID; the restore loads RESTORE_ID.
		if !strings.Contains(script, naming.EnvRestoreID) {
			t.Errorf("expected restore-id load in:\n%s", script)
		}
	})

	t.Run("mysql drops and recreates before load", func(t *testing.T) {
		script := buildRestoreScript(objectStoreDest(), dbTypeMySQL)
		if !strings.Contains(script, "DROP DATABASE IF EXISTS") || !strings.Contains(script, "CREATE DATABASE") {
			t.Errorf("expected drop+create in:\n%s", script)
		}
		if !strings.Contains(script, "gunzip | mysql") {
			t.Errorf("expected gunzip|mysql load in:\n%s", script)
		}
		if !strings.Contains(script, "aws s3 cp \"$"+naming.EnvBackupS3URL) {
			t.Errorf("expected aws s3 load in:\n%s", script)
		}
	})
}

func TestBackupArtifactID(t *testing.T) {
	s := &supersetv1alpha1.Superset{ObjectMeta: metav1.ObjectMeta{UID: types.UID("abc-123")}}

	id1 := backupArtifactID(s, "repo/superset:4.0.0")
	id2 := backupArtifactID(s, "repo/superset:4.0.0")
	if id1 != id2 {
		t.Errorf("backup id must be deterministic: %q != %q", id1, id2)
	}
	if !strings.HasPrefix(id1, "4.0.0-") {
		t.Errorf("expected sanitized tag prefix, got %q", id1)
	}
	if id3 := backupArtifactID(s, "repo/superset:4.1.0"); id3 == id1 {
		t.Error("a new target image must yield a new backup id")
	}
}

func TestSanitizeArtifactSegment(t *testing.T) {
	got := sanitizeArtifactSegment("Foo/Bar:1.2@sha")
	if strings.ContainsAny(got, "/:@") {
		t.Errorf("unsafe characters survived: %q", got)
	}
	if got != "foo-bar-1.2-sha" {
		t.Errorf("unexpected sanitization: %q", got)
	}
}

func TestRestoreApprovalTokenAndContextMatch(t *testing.T) {
	tok := restoreApprovalToken("backup-1", "host:5432/db@user")
	if tok != restoreApprovalToken("backup-1", "host:5432/db@user") {
		t.Error("token must be deterministic")
	}
	if tok == restoreApprovalToken("backup-2", "host:5432/db@user") {
		t.Error("different backup must yield a different token")
	}
	if tok == restoreApprovalToken("backup-1", "other:5432/db@user") {
		t.Error("different target must yield a different token")
	}

	rc := &supersetv1alpha1.RestoreContext{BackupID: "backup-1", ApprovalToken: tok}
	if !restoreContextMatches(rc, "backup-1", tok) {
		t.Error("matching context should match")
	}
	if restoreContextMatches(rc, "backup-2", tok) {
		t.Error("changed backup id must void the context (stale approval)")
	}
	if restoreContextMatches(nil, "backup-1", tok) {
		t.Error("nil context must not match")
	}
}

func TestResolveRestoreArtifact(t *testing.T) {
	mk := func(src supersetv1alpha1.RestoreSourceSpec, backups []supersetv1alpha1.BackupArtifact) *supersetv1alpha1.Superset {
		return &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Restore: &supersetv1alpha1.RestoreTaskSpec{Source: src},
			}},
			Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{Backups: backups}},
		}
	}
	catalog := []supersetv1alpha1.BackupArtifact{{ID: "newest"}, {ID: "older"}}

	if a := resolveRestoreArtifact(mk(supersetv1alpha1.RestoreSourceSpec{Type: "Latest"}, catalog)); a == nil || a.ID != "newest" {
		t.Errorf("Latest should resolve newest, got %v", a)
	}
	id := "older"
	if a := resolveRestoreArtifact(mk(supersetv1alpha1.RestoreSourceSpec{Type: "BackupID", BackupID: &id}, catalog)); a == nil || a.ID != "older" {
		t.Errorf("BackupID should resolve the named artifact, got %v", a)
	}
	missing := "nope"
	if a := resolveRestoreArtifact(mk(supersetv1alpha1.RestoreSourceSpec{Type: "BackupID", BackupID: &missing}, catalog)); a != nil {
		t.Errorf("missing artifact should resolve nil, got %v", a)
	}
	if a := resolveRestoreArtifact(mk(supersetv1alpha1.RestoreSourceSpec{Type: "Latest"}, nil)); a != nil {
		t.Errorf("empty catalog should resolve nil, got %v", a)
	}
}

func TestUpsertBackupArtifact(t *testing.T) {
	ls := &supersetv1alpha1.LifecycleStatus{}
	upsertBackupArtifact(ls, supersetv1alpha1.BackupArtifact{ID: "a", Location: "loc-a"})
	upsertBackupArtifact(ls, supersetv1alpha1.BackupArtifact{ID: "b"})
	if len(ls.Backups) != 2 || ls.Backups[0].ID != "b" {
		t.Fatalf("expected most-recent-first [b a], got %v", ls.Backups)
	}
	// Re-upsert updates in place without duplicating or reordering.
	upsertBackupArtifact(ls, supersetv1alpha1.BackupArtifact{ID: "a", Location: "loc-a2"})
	if len(ls.Backups) != 2 {
		t.Fatalf("re-upsert must not duplicate, got %v", ls.Backups)
	}
	for _, a := range ls.Backups {
		if a.ID == "a" && a.Location != "loc-a2" {
			t.Errorf("expected in-place update of location, got %q", a.Location)
		}
	}
}

func TestBackupDestinationVolumes(t *testing.T) {
	vols, mounts := backupDestinationVolumes(volumeDest())
	if len(vols) != 1 || vols[0].PersistentVolumeClaim == nil || vols[0].PersistentVolumeClaim.ClaimName != "backups-pvc" {
		t.Errorf("expected PVC volume, got %v", vols)
	}
	if len(mounts) != 1 || mounts[0].MountPath != defaultBackupVolumePath {
		t.Errorf("expected mount at default path, got %v", mounts)
	}
	if v, m := backupDestinationVolumes(objectStoreDest()); v != nil || m != nil {
		t.Error("object store needs no volumes/mounts")
	}
}

func TestBackupDescriptorPreUpgradeOnly(t *testing.T) {
	desc := lifecycleTaskDescriptorByType(taskTypeBackup)
	if desc == nil {
		t.Fatal("backup descriptor not registered")
	}
	s := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{Lifecycle: &supersetv1alpha1.LifecycleSpec{
			Backup: &supersetv1alpha1.BackupTaskSpec{Destination: volumeDest()},
		}},
	}
	// First install: no prior image → backup disabled (nothing to back up).
	if desc.IsEnabled(s) {
		t.Error("backup must be disabled on first install (no LastLifecycleImage)")
	}
	// Established instance: backup enabled.
	s.Status.LastLifecycleImage = "repo/superset:4.0.0"
	if !desc.IsEnabled(s) {
		t.Error("backup must be enabled once a prior image exists")
	}
}

// structuredMetastore is a minimal structured metastore for pipeline tests.
func structuredMetastore() *supersetv1alpha1.MetastoreSpec {
	host, db, user := "postgres.default.svc", "superset", "superset"
	return &supersetv1alpha1.MetastoreSpec{Host: &host, Database: &db, Username: &user}
}

// TestLifecyclePipeline_BackupBeforeMigrate asserts the backup task is
// sequenced before migrate during an upgrade and that a completed backup is
// recorded in the status catalog.
func TestLifecyclePipeline_BackupBeforeMigrate(t *testing.T) {
	scheme := testScheme(t)
	dev := "Development"
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-bk"},
		Spec: supersetv1alpha1.SupersetSpec{
			Environment: &dev,
			Image:       supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.0.1"},
			SecretKey:   func() *string { v := "k"; return &v }(),
			Metastore:   structuredMetastore(),
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Backup:  &supersetv1alpha1.BackupTaskSpec{Destination: volumeDest()},
				Migrate: &supersetv1alpha1.MigrateTaskSpec{},
			},
		},
		// Pre-existing image: backup is a pre-upgrade task, so it only runs when
		// upgrading FROM an established image.
		Status: supersetv1alpha1.SupersetStatus{
			LastLifecycleImage: "apache/superset:6.0.0",
			Lifecycle:          &supersetv1alpha1.LifecycleStatus{},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).
		WithStatusSubresource(&supersetv1alpha1.Superset{}).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(50)}

	// Backup must reach a Job before migrate ever does.
	for _, taskType := range []string{taskTypeBackup, taskTypeMigrate} {
		var advanced bool
		for range 8 {
			res, err := r.reconcileLifecycle(context.Background(), superset, "cfg", nil, "sa")
			require.NoError(t, err)
			require.False(t, res.TerminalFailure, "unexpected terminal failure on %s: %#v", taskType, superset.Status)

			// Migrate's Job must never exist before backup has completed.
			if taskType == taskTypeBackup {
				mj, err := getTaskJob(t, c, superset.Namespace, taskJobName(superset.Name, taskTypeMigrate))
				require.NoError(t, err)
				assert.Nil(t, mj, "migrate Job must not be created before backup completes")
			}

			job, err := getTaskJob(t, c, superset.Namespace, taskJobName(superset.Name, taskType))
			require.NoError(t, err)
			if job == nil {
				continue
			}
			if jobComplete(job) {
				advanced = true
				break
			}
			markJobSucceeded(t, c, job)
		}
		require.True(t, advanced, "pipeline did not advance past %s: %#v", taskType, superset.Status)
	}

	// The completed backup is recorded in the catalog, tagged with the
	// pre-upgrade (contents) image.
	require.NotEmpty(t, superset.Status.Lifecycle.Backups, "expected a recorded backup artifact")
	got := superset.Status.Lifecycle.Backups[0]
	assert.Equal(t, "apache/superset:6.0.0", got.Image, "artifact should record the pre-upgrade image")
	assert.NotEmpty(t, got.ID)
}

// TestReconcileRestore_GateAndComplete drives the restore flow: it blocks
// awaiting approval, then on a matching annotation runs the restore Job,
// advances LastLifecycleImage, records the pre-restore snapshot, and clears the
// approval context.
func TestReconcileRestore_GateAndComplete(t *testing.T) {
	scheme := testScheme(t)
	dev := "Development"
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-rs"},
		Spec: supersetv1alpha1.SupersetSpec{
			Environment: &dev,
			Image:       supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.0.0"},
			SecretKey:   func() *string { v := "k"; return &v }(),
			Metastore:   structuredMetastore(),
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Backup:  &supersetv1alpha1.BackupTaskSpec{Destination: volumeDest()},
				Restore: &supersetv1alpha1.RestoreTaskSpec{Source: supersetv1alpha1.RestoreSourceSpec{Type: "Latest"}},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{
			LastLifecycleImage: "apache/superset:6.0.0",
			Lifecycle: &supersetv1alpha1.LifecycleStatus{
				Backups: []supersetv1alpha1.BackupArtifact{{ID: "bkp-1", Image: "apache/superset:5.0.0"}},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).
		WithStatusSubresource(&supersetv1alpha1.Superset{}).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(50)}

	// Without approval: blocks, records the restore context + token.
	_, handled, err := r.reconcileRestore(context.Background(), superset, nil, "sa")
	require.NoError(t, err)
	require.True(t, handled, "restore must gate (handled) without approval")
	assert.Equal(t, lifecyclePhaseAwaitingApproval, superset.Status.Lifecycle.Phase)
	require.NotNil(t, superset.Status.Lifecycle.RestoreApproval)
	token := superset.Status.Lifecycle.RestoreApproval.ApprovalToken
	assert.Equal(t, "bkp-1", superset.Status.Lifecycle.RestoreApproval.BackupID)
	absent, err := getTaskJob(t, c, superset.Namespace, superset.Name+suffixRestore)
	require.NoError(t, err)
	assert.Nil(t, absent, "restore Job must not be created before approval")

	// Approve with the exact token; drive the restore Job to completion.
	superset.SetAnnotations(map[string]string{annotationApproveRestore: token})
	var done bool
	for range 8 {
		_, handled, err := r.reconcileRestore(context.Background(), superset, nil, "sa")
		require.NoError(t, err)
		if !handled {
			done = true
			break
		}
		job, err := getTaskJob(t, c, superset.Namespace, superset.Name+suffixRestore)
		require.NoError(t, err)
		if job != nil && !jobComplete(job) {
			markJobSucceeded(t, c, job)
		}
	}
	require.True(t, done, "restore did not complete: %#v", superset.Status.Lifecycle)

	// Completion advances LastLifecycleImage (dissolving any downgrade gate),
	// clears the approval context, and records the pre-restore snapshot.
	assert.Equal(t, "apache/superset:6.0.0", superset.Status.LastLifecycleImage)
	assert.Nil(t, superset.Status.Lifecycle.RestoreApproval)
	var sawSnapshot bool
	for _, a := range superset.Status.Lifecycle.Backups {
		if strings.HasPrefix(a.ID, "pre-restore-") {
			sawSnapshot = true
		}
	}
	assert.True(t, sawSnapshot, "expected a pre-restore snapshot in the catalog: %#v", superset.Status.Lifecycle.Backups)
}
