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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

// --- helpers ---

func newBackupSuperset() *supersetv1alpha1.Superset {
	return &supersetv1alpha1.Superset{
		Name: "test", Namespace: "default", UID: "uid-1",
		Spec: supersetv1alpha1.SupersetSpec{
			Image:         supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.0.1"},
			SecretKeyFrom: &corev1.SecretKeySelector{Name: "app-secret", Key: "secret-key"},
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Host:         new("postgres.default.svc"),
				Database:     new("superset"),
				Username:     new("superset"),
				PasswordFrom: &corev1.SecretKeySelector{Name: "db-secret", Key: "password"},
			},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Backup: &supersetv1alpha1.BackupTaskSpec{
					MaxRetries: new(int32(1)),
					Destination: &supersetv1alpha1.BackupDestinationSpec{
						PersistentVolumeClaim: &supersetv1alpha1.BackupPVCSource{ClaimName: "superset-backups"},
					},
				},
				Migrate: &supersetv1alpha1.MigrateTaskSpec{MaxRetries: new(int32(1))},
			},
		},
	}
}

// backupHarness drives reconcileLifecycle against a fake client, settling
// each task Job it finds and recording the order in which Jobs ran.
type backupHarness struct {
	t    *testing.T
	c    client.Client
	r    *SupersetReconciler
	s    *supersetv1alpha1.Superset
	fail map[string]bool
	// ran is the ordered list of task Job names settled in the last drive.
	ran []string
	// jobs keeps the last settled Job per name for spec assertions.
	jobs map[string]*batchv1.Job
	// podMessages, when set for a Job name, creates a Job-owned Pod whose
	// main container terminated with that message before the Job settles.
	podMessages map[string]string
}

func newBackupHarness(t *testing.T, s *supersetv1alpha1.Superset) *backupHarness {
	t.Helper()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(s).
		WithStatusSubresource(&supersetv1alpha1.Superset{}).
		Build()
	return &backupHarness{
		t:           t,
		c:           c,
		r:           &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(200)},
		s:           s,
		fail:        map[string]bool{},
		jobs:        map[string]*batchv1.Job{},
		podMessages: map[string]string{},
	}
}

// drive reconciles until the lifecycle completes, fails terminally, or the
// iteration budget runs out. Any active task Job is marked succeeded (or
// failed, if listed in h.fail) once observed.
func (h *backupHarness) drive(configChecksum string) lifecycleResult {
	h.t.Helper()
	h.ran = nil
	var res lifecycleResult
	for range 40 {
		var err error
		res, err = h.r.reconcileLifecycle(context.Background(), h.s, configChecksum, nil, "sa")
		require.NoError(h.t, err)
		if res.Complete || res.TerminalFailure {
			return res
		}
		jobs := &batchv1.JobList{}
		require.NoError(h.t, h.c.List(context.Background(), jobs, client.InNamespace(h.s.Namespace)))
		for i := range jobs.Items {
			job := &jobs.Items[i]
			if jobComplete(job) || jobFailed(job) {
				continue
			}
			h.ran = append(h.ran, job.Name)
			h.jobs[job.Name] = job.DeepCopy()
			if msg, ok := h.podMessages[job.Name]; ok {
				createTerminatedTaskPod(h.t, h.c, job, msg)
			}
			if h.fail[job.Name] {
				markJobFailedForTest(h.t, h.c, job)
			} else {
				markJobSucceeded(h.t, h.c, job)
			}
		}
	}
	h.t.Fatalf("lifecycle did not settle within budget; ran=%v status=%#v", h.ran, h.s.Status.Lifecycle)
	return res
}

func markJobFailedForTest(t *testing.T, c client.Client, job *batchv1.Job) {
	t.Helper()
	now := metav1.Now()
	job.Status.Failed = 1
	job.Status.StartTime = &now
	job.Status.Conditions = []batchv1.JobCondition{{
		Type:               batchv1.JobFailed,
		Status:             corev1.ConditionTrue,
		Reason:             "BackoffLimitExceeded",
		Message:            "dump failed",
		LastTransitionTime: now,
	}}
	require.NoError(t, c.Status().Update(context.Background(), job))
}

// createTerminatedTaskPod creates a Pod controlled by job whose main
// container terminated with message, replacing any earlier one.
func createTerminatedTaskPod(t *testing.T, c client.Client, job *batchv1.Job, message string) {
	t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{
		Name:      job.Name + "-pod",
		Namespace: job.Namespace,
		Labels:    map[string]string{labelInitInstance: job.Name},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: new(true),
		}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: naming.Container, Image: "x"}}},
	}
	_ = c.Delete(ctx, pod)
	require.NoError(t, c.Create(ctx, pod))
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  naming.Container,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: message}},
	}}
	require.NoError(t, c.Status().Update(ctx, pod))
}

func backupResultMessage(file, sha string) string {
	return `{"file":"` + file + `","sizeBytes":4096,"sha256":"` + sha + `","alembicRevision":"a1b2c3","createdAt":"2026-09-26T01:02:03Z"}`
}

func jobEnv(job *batchv1.Job, name string) string {
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func jobExists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	job, err := getTaskJob(t, c, "default", name)
	require.NoError(t, err)
	return job != nil
}

// --- pipeline behavior ---

func TestBackupPipeline_RunsOncePerRunBeforeMutatingTasks(t *testing.T) {
	h := newBackupHarness(t, newBackupSuperset())

	// First install: backup runs (flagged as first run), then migrate, init.
	res := h.drive("config-1")
	require.True(t, res.Complete)
	assert.Equal(t, []string{"test-backup", "test-migrate", "test-init"}, h.ran)
	firstRun := jobEnv(h.jobs["test-backup"], naming.EnvBackupFirstRun)
	assert.Equal(t, "true", firstRun)
	fromTag := jobEnv(h.jobs["test-backup"], naming.EnvBackupFromTag)
	assert.Equal(t, "initial", fromTag)
	assert.NotEmpty(t, h.s.Status.Lifecycle.SettledChecksum, "settled baseline recorded on completion")

	// Steady state: nothing runs.
	res = h.drive("config-1")
	require.True(t, res.Complete)
	assert.Empty(t, h.ran)

	// Config-only change re-runs init only; no backup, no migrate.
	h.s.Spec.Config = new("FEATURE_X = True")
	res = h.drive("config-1")
	require.True(t, res.Complete)
	assert.Equal(t, []string{"test-init"}, h.ran)

	// Image change: a fresh backup precedes migrate.
	h.s.Spec.Image.Tag = "6.1.0"
	res = h.drive("config-1")
	require.True(t, res.Complete)
	assert.Equal(t, []string{"test-backup", "test-migrate", "test-init"}, h.ran)
	firstRun = jobEnv(h.jobs["test-backup"], naming.EnvBackupFirstRun)
	assert.Equal(t, "false", firstRun)
	fromTag = jobEnv(h.jobs["test-backup"], naming.EnvBackupFromTag)
	assert.Equal(t, "6.0.1", fromTag, "files are named after the pre-upgrade tag")

	// Backup spec and trigger changes alone never start a run.
	h.s.Spec.Lifecycle.Backup.Image = &supersetv1alpha1.ContainerImageSpec{Tag: "18-alpine"}
	h.s.Spec.Lifecycle.Backup.Trigger = new("manual-1")
	res = h.drive("config-1")
	require.True(t, res.Complete)
	assert.Empty(t, h.ran)
}

func TestBackupPipeline_FailedMigrateRetryKeepsSnapshot(t *testing.T) {
	h := newBackupHarness(t, newBackupSuperset())
	require.True(t, h.drive("c").Complete)

	h.s.Spec.Image.Tag = "6.1.0"
	h.fail["test-migrate"] = true
	res := h.drive("c")
	require.True(t, res.TerminalFailure)
	assert.Equal(t, []string{"test-backup", "test-migrate"}, h.ran)
	backupChecksum := h.s.Status.Lifecycle.Backup.CompletedChecksum

	// Retrying migrate must not re-snapshot the partially migrated database.
	delete(h.fail, "test-migrate")
	h.s.Spec.Lifecycle.Migrate.Trigger = new("retry-1")
	res = h.drive("c")
	require.True(t, res.Complete)
	assert.Equal(t, []string{"test-migrate", "test-init"}, h.ran)
	assert.Equal(t, backupChecksum, h.s.Status.Lifecycle.Backup.CompletedChecksum)
}

func TestBackupPipeline_FailureBlocksMutatingTasks(t *testing.T) {
	h := newBackupHarness(t, newBackupSuperset())
	require.True(t, h.drive("c").Complete)
	installedMigrate := h.s.Status.Lifecycle.Migrate.CompletedChecksum

	h.s.Spec.Image.Tag = "6.1.0"
	h.fail["test-backup"] = true
	res := h.drive("c")
	require.True(t, res.TerminalFailure)
	assert.Equal(t, []string{"test-backup"}, h.ran)
	assert.Equal(t, taskStateComplete, h.s.Status.Lifecycle.Migrate.State, "migrate must not start after a failed backup")
	assert.Equal(t, installedMigrate, h.s.Status.Lifecycle.Migrate.DesiredChecksum)

	t.Run("reverting the image restores without a backup", func(t *testing.T) {
		h.s.Spec.Image.Tag = "6.0.1"
		res := h.drive("c")
		require.True(t, res.Complete)
		assert.Empty(t, h.ran)
		h.s.Spec.Image.Tag = "6.1.0"
	})

	t.Run("disabling backup proceeds without it", func(t *testing.T) {
		h.s.Spec.Lifecycle.Backup.Disabled = new(true)
		res := h.drive("c")
		require.True(t, res.Complete)
		assert.Equal(t, []string{"test-migrate", "test-init"}, h.ran)
		assert.Nil(t, h.s.Status.Lifecycle.Backup, "disabled backup status is pruned")
	})
}

func TestBackupPipeline_TriggerRetriesFailedBackup(t *testing.T) {
	h := newBackupHarness(t, newBackupSuperset())
	require.True(t, h.drive("c").Complete)

	h.s.Spec.Image.Tag = "6.1.0"
	h.fail["test-backup"] = true
	require.True(t, h.drive("c").TerminalFailure)

	// Without a spec change the failure stays terminal.
	delete(h.fail, "test-backup")
	res := h.drive("c")
	require.True(t, res.TerminalFailure)
	assert.Empty(t, h.ran)

	// An external fix plus a trigger bump retries the backup, then migrates.
	h.s.Spec.Lifecycle.Backup.Trigger = new("retry-1")
	res = h.drive("c")
	require.True(t, res.Complete)
	assert.Equal(t, []string{"test-backup", "test-migrate", "test-init"}, h.ran)
}

func TestBackupPipeline_RotateOnlyChangeTakesBackup(t *testing.T) {
	s := newBackupSuperset()
	s.Spec.PreviousSecretKeyFrom = &corev1.SecretKeySelector{Name: "old", Key: "k"}
	s.Spec.Lifecycle.Rotate = &supersetv1alpha1.RotateTaskSpec{}
	h := newBackupHarness(t, s)
	require.True(t, h.drive("c").Complete)

	h.s.Spec.PreviousSecretKeyFrom = &corev1.SecretKeySelector{Name: "old-2", Key: "k"}
	res := h.drive("c")
	require.True(t, res.Complete)
	assert.Equal(t, []string{"test-backup", "test-rotate", "test-init"}, h.ran)
}

func TestBackupPipeline_NotStartedWhileGuardedTaskRuns(t *testing.T) {
	s := newBackupSuperset()
	s.Spec.Lifecycle.Backup = nil
	h := newBackupHarness(t, s)
	require.True(t, h.drive("c").Complete)

	// Start an upgrade and let the new migrate Job get created, but not finish.
	installedMigrate := h.s.Status.Lifecycle.Migrate.CompletedChecksum
	migrateRunning := func() bool {
		m := h.s.Status.Lifecycle.Migrate
		return m.DesiredChecksum != installedMigrate && m.State == taskStateRunning
	}
	h.s.Spec.Image.Tag = "6.1.0"
	for range 5 {
		_, err := h.r.reconcileLifecycle(context.Background(), h.s, "c", nil, "sa")
		require.NoError(t, err)
		if migrateRunning() {
			break
		}
	}
	require.True(t, migrateRunning())

	// Enabling backup mid-run must not snapshot concurrently with migrate.
	h.s.Spec.Lifecycle.Backup = newBackupSuperset().Spec.Lifecycle.Backup
	_, err := h.r.reconcileLifecycle(context.Background(), h.s, "c", nil, "sa")
	require.NoError(t, err)
	assert.False(t, jobExists(t, h.c, "test-backup"))

	res := h.drive("c")
	require.True(t, res.Complete)
	assert.NotContains(t, h.ran, "test-backup")
}

func TestBackupPipeline_RunsBeforeDrainByDefault(t *testing.T) {
	ctx := context.Background()
	webServer := func() *appsv1.Deployment {
		return &appsv1.Deployment{Name: "test-web-server", Namespace: "default"}
	}
	webServerExists := func(h *backupHarness) bool {
		err := h.c.Get(ctx, client.ObjectKeyFromObject(webServer()), &appsv1.Deployment{})
		if apierrors.IsNotFound(err) {
			return false
		}
		require.NoError(t, err)
		return true
	}
	// upgradeUntilBackupJob installs, starts a web server, bumps the image, and
	// reconciles until the backup Job exists (without letting it finish).
	upgradeUntilBackupJob := func(t *testing.T, requiresDrain *bool) *backupHarness {
		s := newBackupSuperset()
		s.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
		s.Spec.Lifecycle.Backup.RequiresDrain = requiresDrain
		h := newBackupHarness(t, s)
		require.True(t, h.drive("c").Complete)
		require.NoError(t, h.c.Create(ctx, webServer()))

		h.s.Spec.Image.Tag = "6.1.0"
		for range 10 {
			_, err := h.r.reconcileLifecycle(ctx, h.s, "c", nil, "sa")
			require.NoError(t, err)
			if jobExists(t, h.c, "test-backup") {
				return h
			}
		}
		t.Fatal("backup Job was never created")
		return nil
	}

	t.Run("default: backup runs while the current version still serves", func(t *testing.T) {
		h := upgradeUntilBackupJob(t, nil)
		assert.True(t, webServerExists(h), "components must not be drained before the backup completes")
		assert.Equal(t, lifecyclePhaseBackingUp, h.s.Status.Lifecycle.Phase)
		assert.False(t, h.s.Status.Lifecycle.MaintenanceActive)

		res := h.drive("c")
		require.True(t, res.Complete)
		assert.Equal(t, []string{"test-backup", "test-migrate", "test-init"}, h.ran)
		assert.False(t, webServerExists(h), "migrate still drains after the backup")
	})

	t.Run("default: a failed backup blocks the upgrade without draining", func(t *testing.T) {
		h := upgradeUntilBackupJob(t, nil)
		installedMigrate := h.s.Status.Lifecycle.Migrate.CompletedChecksum
		h.fail["test-backup"] = true
		res := h.drive("c")
		require.True(t, res.TerminalFailure)
		assert.True(t, webServerExists(h), "the current version keeps serving when the backup fails")
		assert.Equal(t, installedMigrate, h.s.Status.Lifecycle.Migrate.DesiredChecksum, "migrate must not start")
		assert.Equal(t, lifecyclePhaseBackingUp, h.s.Status.Lifecycle.Phase)
	})

	t.Run("requiresDrain: true drains before the backup", func(t *testing.T) {
		h := upgradeUntilBackupJob(t, new(true))
		assert.False(t, webServerExists(h), "components are drained before the backup starts")
	})
}

func TestBackupPipeline_RecordsVerifiedBackups(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	h := newBackupHarness(t, newBackupSuperset())
	h.s.Spec.Lifecycle.Backup.Destination.PersistentVolumeClaim.SubPath = new("prod/")
	h.podMessages["test-backup"] = `{"skipped":"metastore database superset does not exist yet"}`
	require.True(t, h.drive("c").Complete)
	assert.Equal(t, "Skipped: metastore database superset does not exist yet", h.s.Status.Lifecycle.Backup.Message)
	assert.Empty(t, h.s.Status.Lifecycle.Backups, "a skipped run records no backup")
	installedImage := h.s.Status.LastLifecycleImage

	h.podMessages["test-backup"] = backupResultMessage("test_20260926T010203Z_6.0.1.dump", sha)
	h.s.Spec.Image.Tag = "6.1.0"
	require.True(t, h.drive("c").Complete)
	require.Len(t, h.s.Status.Lifecycle.Backups, 1)
	got := h.s.Status.Lifecycle.Backups[0]
	assert.Equal(t, "pvc://superset-backups/prod/test_20260926T010203Z_6.0.1.dump", got.Location)
	assert.Equal(t, int64(4096), got.SizeBytes)
	assert.Equal(t, sha, got.SHA256)
	assert.Equal(t, "a1b2c3", got.AlembicRevision)
	assert.Equal(t, installedImage, got.FromImage, "restore the dump with the image it was taken on")
	assert.Equal(t, "apache/superset:6.1.0", got.ToImage)
	assert.Equal(t, time.Date(2026, 9, 26, 1, 2, 3, 0, time.UTC), got.CreatedAt.UTC())
	assert.Contains(t, h.s.Status.Lifecycle.Backup.Message, "Backup written: pvc://superset-backups/prod/test_20260926T010203Z_6.0.1.dump")

	t.Run("newest first, bounded", func(t *testing.T) {
		for i := range maxBackupRecords + 2 {
			h.podMessages["test-backup"] = backupResultMessage(fmt.Sprintf("test_2026092%dT000000Z_x.dump", i), sha)
			h.s.Spec.Image.Tag = fmt.Sprintf("7.%d.0", i)
			require.True(t, h.drive("c").Complete)
		}
		require.Len(t, h.s.Status.Lifecycle.Backups, maxBackupRecords)
		assert.True(t, strings.HasSuffix(h.s.Status.Lifecycle.Backups[0].Location, fmt.Sprintf("test_2026092%dT000000Z_x.dump", maxBackupRecords+1)))
	})

	t.Run("replaying a completion does not duplicate the record", func(t *testing.T) {
		before := len(h.s.Status.Lifecycle.Backups)
		job := h.jobs["test-backup"]
		require.NoError(t, h.r.recordBackupResult(context.Background(), h.s, job, h.s.Status.Lifecycle.Backup))
		assert.Len(t, h.s.Status.Lifecycle.Backups, before)
	})

	t.Run("records survive disabling backup", func(t *testing.T) {
		h.s.Spec.Lifecycle.Backup.Disabled = new(true)
		h.s.Spec.Image.Tag = "8.0.0"
		require.True(t, h.drive("c").Complete)
		assert.Len(t, h.s.Status.Lifecycle.Backups, maxBackupRecords)
	})
}

func TestBackupPipeline_UntrustedResultIsNotRecorded(t *testing.T) {
	h := newBackupHarness(t, newBackupSuperset())
	require.True(t, h.drive("c").Complete)

	h.podMessages["test-backup"] = `{"file":"../../etc/passwd","sizeBytes":1,"sha256":"x","createdAt":"now"}`
	h.s.Spec.Image.Tag = "6.1.0"
	require.True(t, h.drive("c").Complete, "an unrecognized result does not fail an otherwise successful Job")
	assert.Empty(t, h.s.Status.Lifecycle.Backups)
	assert.Equal(t, "Completed successfully", h.s.Status.Lifecycle.Backup.Message)
}

func TestBackupPipeline_FailureReasonSurfacesInStatus(t *testing.T) {
	h := newBackupHarness(t, newBackupSuperset())
	require.True(t, h.drive("c").Complete)

	h.s.Spec.Image.Tag = "6.1.0"
	h.fail["test-backup"] = true
	h.podMessages["test-backup"] = "backup failed at preflight: pg_dump 17 cannot dump PostgreSQL 18; set spec.lifecycle.backup.image.tag to 18-alpine"
	require.True(t, h.drive("c").TerminalFailure)
	assert.Equal(t, h.podMessages["test-backup"], h.s.Status.Lifecycle.Backup.Message,
		"the script's reason replaces the generic Job condition")

	t.Run("credentials in tool output are redacted", func(t *testing.T) {
		h.s.Spec.Lifecycle.Backup.Trigger = new("retry-1")
		h.podMessages["test-backup"] = "backup failed at pg_dump: connection to postgresql://superset:hunter2@db:5432/superset failed"
		require.True(t, h.drive("c").TerminalFailure)
		assert.NotContains(t, h.s.Status.Lifecycle.Backup.Message, "hunter2")
	})
}

func TestParseBackupResult(t *testing.T) {
	sha := strings.Repeat("0f", 32)
	valid := backupResultMessage("demo_20260926T010203Z_6.0.0.dump", sha)
	cases := map[string]struct {
		message string
		ok      bool
	}{
		"committed dump":        {valid, true},
		"skipped":               {`{"skipped":"metastore database x does not exist yet"}`, true},
		"empty":                 {"", false},
		"plain text":            {"Backup written", false},
		"unknown field":         {strings.TrimSuffix(valid, "}") + `,"extra":1}`, false},
		"trailing data":         {valid + valid, false},
		"path traversal":        {backupResultMessage("../x.dump", sha), false},
		"hidden file":           {backupResultMessage(".x.dump", sha), false},
		"bad checksum":          {backupResultMessage("x.dump", "abc"), false},
		"uppercase checksum":    {backupResultMessage("x.dump", strings.ToUpper(sha)), false},
		"missing size":          {`{"file":"x.dump","sha256":"` + sha + `","createdAt":"2026-09-26T01:02:03Z"}`, false},
		"negative size":         {strings.Replace(valid, "4096", "-1", 1), false},
		"bad time":              {strings.Replace(valid, "2026-09-26T01:02:03Z", "yesterday", 1), false},
		"revision injection":    {strings.Replace(valid, "a1b2c3", `a"b`, 1), false},
		"skipped with file":     {`{"skipped":"x","file":"x.dump"}`, false},
		"oversized skip reason": {`{"skipped":"` + strings.Repeat("x", 300) + `"}`, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, ok := parseBackupResult(tc.message)
			assert.Equal(t, tc.ok, ok)
		})
	}
}

func TestBackupPipeline_BlockedWithSeed(t *testing.T) {
	s := newBackupSuperset()
	s.Spec.Environment = new("Staging")
	s.Spec.Lifecycle.Seed = &supersetv1alpha1.SeedTaskSpec{
		Source: supersetv1alpha1.SeedSourceSpec{Host: "src", Database: "superset", Username: "ro"},
	}
	h := newBackupHarness(t, s)
	res, err := h.r.reconcileLifecycle(context.Background(), h.s, "c", nil, "sa")
	require.NoError(t, err)
	assert.True(t, res.TerminalFailure)
	assert.True(t, hasLifecycleConditionReason(h.s, "BackupWithSeedNotAllowed"))
	assert.False(t, jobExists(t, h.c, "test-seed"))
}

// --- cascade isolation ---

func TestBackupSpecDoesNotAffectCascadeChecksums(t *testing.T) {
	r := &SupersetReconciler{}
	checksums := func(s *supersetv1alpha1.Superset) map[string]string {
		out := map[string]string{}
		for _, step := range r.walkLifecycleCascade(s, "config") {
			out[step.Desc.TaskType] = step.TaskChecksum
		}
		return out
	}

	without := newBackupSuperset()
	without.Spec.Lifecycle.Backup = nil
	base := checksums(without)
	require.Contains(t, base, taskTypeMigrate)
	assert.NotContains(t, base, taskTypeBackup)

	with := newBackupSuperset()
	assert.Equal(t, base, checksums(with))

	with.Spec.Lifecycle.Backup.Command = []string{"sh", "-c", "custom"}
	with.Spec.Lifecycle.Backup.Trigger = new("t")
	with.Spec.Lifecycle.Backup.Image = &supersetv1alpha1.ContainerImageSpec{Tag: "18-alpine"}
	assert.Equal(t, base, checksums(with))
}

func TestSettledChecksumExcludesBackup(t *testing.T) {
	ls := &supersetv1alpha1.LifecycleStatus{LastCompletedChecksums: map[string]string{
		taskTypeMigrate: "m", taskTypeInit: "i",
	}}
	base := settledChecksumFor(ls)
	require.NotEmpty(t, base)
	ls.LastCompletedChecksums[taskTypeBackup] = "b"
	assert.Equal(t, base, settledChecksumFor(ls))
	assert.Empty(t, settledChecksumFor(&supersetv1alpha1.LifecycleStatus{}))
	assert.Empty(t, settledChecksumFor(nil))
}

func TestBackupTaskChecksum(t *testing.T) {
	s := newBackupSuperset()
	s.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{SettledChecksum: "a"}
	base := backupTaskChecksum(s)

	s.Spec.Lifecycle.Backup.Command = []string{"x"}
	s.Spec.Lifecycle.Backup.Image = &supersetv1alpha1.ContainerImageSpec{Tag: "18-alpine"}
	assert.Equal(t, base, backupTaskChecksum(s), "spec changes do not redo a completed backup")

	s.Spec.Lifecycle.Backup.Trigger = new("t")
	withTrigger := backupTaskChecksum(s)
	assert.NotEqual(t, base, withTrigger)

	s.Status.Lifecycle.SettledChecksum = "b"
	assert.NotEqual(t, withTrigger, backupTaskChecksum(s), "a new baseline needs a new backup")
}

func TestLifecycleNeedsDrainAccountsForBackup(t *testing.T) {
	r := &SupersetReconciler{}
	s := newBackupSuperset()
	s.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
	s.Spec.Lifecycle.Migrate.RequiresDrain = new(false)
	s.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{}

	assert.Equal(t, []string{taskTypeBackup, taskTypeMigrate, taskTypeInit}, r.pendingLifecycleTasks(s, "c"))
	assert.False(t, r.lifecycleNeedsDrain(s, "c"), "backup runs before drain by default")

	s.Spec.Lifecycle.Backup.RequiresDrain = new(true)
	assert.True(t, r.lifecycleNeedsDrain(s, "c"), "requiresDrain: true drains for the backup even when migrate does not")
}

// --- task wiring ---

func TestBackupTaskWiring(t *testing.T) {
	r := &SupersetReconciler{}
	s := newBackupSuperset()

	assert.False(t, taskUsesSupersetConfig(taskTypeBackup))
	assert.Equal(t, time.Hour, r.taskTimeoutValue(s, taskTypeBackup))
	assert.Equal(t, defaultInitTimeout, r.taskTimeoutValue(s, taskTypeMigrate))
	s.Spec.Lifecycle.Backup.Timeout = &metav1.Duration{Duration: 2 * time.Hour}
	assert.Equal(t, 2*time.Hour, r.taskTimeoutValue(s, taskTypeBackup))

	s.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{}
	ref := ensureTaskStatus(s, taskTypeBackup)
	assert.Same(t, s.Status.Lifecycle.Backup, ref, "backup has its own status slot")
	assert.Nil(t, s.Status.Lifecycle.Init)

	lifecyclePolicy := retentionRetain
	backupPolicy := retentionDelete
	s.Spec.Lifecycle.PodRetention = &supersetv1alpha1.PodRetentionSpec{Policy: &lifecyclePolicy}
	assert.Equal(t, retentionRetain, r.taskRetentionPolicyValue(s, taskTypeBackup))
	s.Spec.Lifecycle.Backup.PodRetention = &supersetv1alpha1.PodRetentionSpec{Policy: &backupPolicy}
	assert.Equal(t, retentionDelete, r.taskRetentionPolicyValue(s, taskTypeBackup))
}

func TestBuildBackupTaskFlatSpec(t *testing.T) {
	r := &SupersetReconciler{}

	t.Run("postgres defaults", func(t *testing.T) {
		s := newBackupSuperset()
		s.Spec.Lifecycle.Backup.Destination.PersistentVolumeClaim.SubPath = new("superset")
		flat, rendered := r.buildTaskFlatSpec(s, taskTypeBackup, nil, "", nil, "sa")
		assert.Empty(t, rendered)
		assert.Equal(t, "postgres", flat.Image.Repository)
		assert.Equal(t, "17-alpine", flat.Image.Tag)

		pod := buildInitPod(&flat)
		require.Len(t, pod.Volumes, 1)
		assert.Equal(t, backupVolumeName, pod.Volumes[0].Name)
		assert.Equal(t, "superset-backups", pod.Volumes[0].PersistentVolumeClaim.ClaimName)
		ctr := pod.Containers[0]
		require.Len(t, ctr.VolumeMounts, 1)
		assert.Equal(t, backupMountPath, ctr.VolumeMounts[0].MountPath)
		assert.Equal(t, "superset", ctr.VolumeMounts[0].SubPath)
		assert.Equal(t, buildBackupCommand(s), ctr.Command)

		env := map[string]corev1.EnvVar{}
		for _, e := range ctr.Env {
			env[e.Name] = e
		}
		assert.Equal(t, backupMountPath, env[naming.EnvBackupDir].Value)
		assert.Equal(t, "postgres.default.svc", env[naming.EnvDBHost].Value)
		require.NotNil(t, env[naming.EnvDBPass].ValueFrom, "password stays a secretKeyRef")
		assert.Equal(t, "db-secret", env[naming.EnvDBPass].ValueFrom.SecretKeyRef.Name)
		assert.NotContains(t, env, naming.EnvSecretKey, "backup Job never receives the Superset secret key")
		assert.Equal(t, "uid-1", env[naming.EnvBackupUID].Value)
		assert.NotContains(t, env, naming.EnvBackupKeepLast, "no retention: completed backups are never deleted")
		var meta map[string]string
		require.NoError(t, json.Unmarshal([]byte(env[naming.EnvBackupMetadata].Value), &meta))
		assert.Equal(t, map[string]string{
			"name": "test", "namespace": "default", "uid": "uid-1", "dbType": dbTypePostgresql,
			"fromImage": "", "toImage": "apache/superset:6.0.1",
		}, meta)

		s.Spec.Lifecycle.Backup.Retention = &supersetv1alpha1.BackupRetentionSpec{KeepLast: 7}
		s.Status.LastLifecycleImage = "apache/superset:6.0.0"
		flat, _ = r.buildTaskFlatSpec(s, taskTypeBackup, nil, "", nil, "sa")
		env = map[string]corev1.EnvVar{}
		for _, e := range buildInitPod(&flat).Containers[0].Env {
			env[e.Name] = e
		}
		assert.Equal(t, "7", env[naming.EnvBackupKeepLast].Value)
		assert.Contains(t, env[naming.EnvBackupMetadata].Value, `"fromImage":"apache/superset:6.0.0"`)

		require.NotNil(t, ctr.SecurityContext.RunAsUser)
		assert.Equal(t, int64(70), *ctr.SecurityContext.RunAsUser)
		require.NotNil(t, pod.SecurityContext)
		assert.Equal(t, int64(70), *pod.SecurityContext.FSGroup)
		assert.Equal(t, corev1.FSGroupChangeOnRootMismatch, *pod.SecurityContext.FSGroupChangePolicy)
	})

	t.Run("user securityContext is respected", func(t *testing.T) {
		s := newBackupSuperset()
		s.Spec.Lifecycle.Backup.PodTemplate = &supersetv1alpha1.PodTemplate{
			PodSecurityContext: &corev1.PodSecurityContext{RunAsUser: new(int64(1000)), FSGroup: new(int64(2000))},
		}
		flat, _ := r.buildTaskFlatSpec(s, taskTypeBackup, nil, "", nil, "sa")
		pod := buildInitPod(&flat)
		assert.Nil(t, pod.Containers[0].SecurityContext.RunAsUser, "pod-level UID wins")
		assert.Equal(t, int64(2000), *pod.SecurityContext.FSGroup)
		assert.Nil(t, pod.SecurityContext.FSGroupChangePolicy)
		assert.Equal(t, int64(2000), *s.Spec.Lifecycle.Backup.PodTemplate.PodSecurityContext.FSGroup, "spec not mutated")
	})

	t.Run("mysql defaults", func(t *testing.T) {
		s := newBackupSuperset()
		s.Spec.Metastore.Type = new(dbTypeMySQL)
		flat, _ := r.buildTaskFlatSpec(s, taskTypeBackup, nil, "", nil, "sa")
		assert.Equal(t, "mysql", flat.Image.Repository)
		pod := buildInitPod(&flat)
		assert.Equal(t, int64(999), *pod.Containers[0].SecurityContext.RunAsUser)
		assert.Contains(t, strings.Join(pod.Containers[0].Command, " "), "mysqldump")
	})

	t.Run("custom command against uriFrom metastore", func(t *testing.T) {
		s := newBackupSuperset()
		s.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{URIFrom: &corev1.SecretKeySelector{Name: "uri", Key: "k"}}
		s.Spec.Lifecycle.Backup.Destination = nil
		s.Spec.Lifecycle.Backup.Command = []string{"/bin/sh", "-c", "ship-to-s3"}
		flat, _ := r.buildTaskFlatSpec(s, taskTypeBackup, nil, "", nil, "sa")
		pod := buildInitPod(&flat)
		assert.Empty(t, pod.Volumes)
		assert.Equal(t, []string{"/bin/sh", "-c", "ship-to-s3"}, pod.Containers[0].Command)
		env := map[string]corev1.EnvVar{}
		for _, e := range pod.Containers[0].Env {
			env[e.Name] = e
		}
		assert.NotContains(t, env, naming.EnvBackupDir)
		require.NotNil(t, env[naming.EnvDatabaseURI].ValueFrom)
		assert.Equal(t, "uri", env[naming.EnvDatabaseURI].ValueFrom.SecretKeyRef.Name)
		assert.Nil(t, pod.SecurityContext, "no fsGroup without a destination volume")
	})
}

func TestBuildBackupCommand(t *testing.T) {
	s := newBackupSuperset()
	cmd := buildBackupCommand(s)
	require.Len(t, cmd, 3)
	script := cmd[2]
	for _, want := range []string{
		"-Fc", `"$OUT.partial"`, "--lock-wait-timeout=",
		`run verification pg_restore -f /dev/null "$OUT.partial"`,      // full read, not just the TOC (--list)
		`mv "$OUT.partial" "$OUT"`, `SUM_COMMITTED=$(sha256sum "$OUT"`, // post-commit integrity check
		`mv "$BASE.json.partial" "$BASE.json"`, // manifest committed last, atomically
		"refusing to overwrite", "cannot dump PostgreSQL",
	} {
		assert.Contains(t, script, want)
	}
	assert.NotContains(t, script, "pg_restore --list", "a TOC listing misses truncated and corrupt data blocks")
	assert.True(t, strings.HasPrefix(script, "set -eu\numask 077\n"), "dumps are created owner-only before anything is written")
	assert.NotContains(t, script, "postgres.default.svc", "CR values are passed via env, not interpolated")
	assert.Less(t, strings.Index(script, "run verification"), strings.Index(script, `mv "$OUT.partial" "$OUT"`), "verify before commit")
	assert.Less(t, strings.Index(script, `mv "$OUT.partial" "$OUT"`), strings.Index(script, `mv "$BASE.json.partial"`), "manifest after dump")
	assert.Less(t, strings.Index(script, `mv "$BASE.json.partial"`), strings.Index(script, "KEEP_LAST"), "prune only after commit")
	for line := range strings.SplitSeq(script, "\n") {
		if strings.HasPrefix(line, "run ") {
			assert.NotContainsf(t, line, "|", "dump/verify must not run in a pipeline whose failure sh would mask: %q", line)
		}
	}

	s.Spec.Metastore.Type = new(dbTypeMySQL)
	mysql := buildBackupCommand(s)[2]
	assert.True(t, strings.HasPrefix(mysql, "set -eu\numask 077\n"))
	for _, want := range []string{"run mysqldump mysqldump", "--single-transaction", "--no-tablespaces", "-- Dump completed", `SUM_COMMITTED=$(sha256sum "$OUT"`, `mv "$BASE.json.partial" "$BASE.json"`} {
		assert.Contains(t, mysql, want)
	}
}

func TestSanitizeBackupLabel(t *testing.T) {
	cases := map[string]string{
		"6.1.0":           "6.1.0",
		"6.1.0-dev_rc1":   "6.1.0-dev_rc1",
		"../../etc":       "_.._etc",
		"-rf":             "rf",
		".hidden":         "hidden",
		"a/b c":           "a_b_c",
		"":                "",
		"...":             "",
		"tag\x00with-nul": "tag_with-nul",
	}
	for in, want := range cases {
		assert.Equalf(t, want, sanitizeBackupLabel(in), "input %q", in)
	}
	assert.Len(t, sanitizeBackupLabel(strings.Repeat("a", 500)), backupPrefixMaxLen)
}

func TestBackupFromTag(t *testing.T) {
	s := newBackupSuperset()
	assert.Equal(t, "initial", backupFromTag(s))
	s.Status.LastLifecycleImage = "apache/superset:6.0.1"
	assert.Equal(t, "6.0.1", backupFromTag(s))
	s.Status.LastLifecycleImage = "registry:5000/superset:../x"
	assert.Equal(t, "_x", backupFromTag(s))
	s.Status.LastLifecycleImage = "apache/superset:..."
	assert.Equal(t, "initial", backupFromTag(s))
}
