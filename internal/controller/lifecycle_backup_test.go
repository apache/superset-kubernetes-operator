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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
		t:    t,
		c:    c,
		r:    &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(200)},
		s:    s,
		fail: map[string]bool{},
		jobs: map[string]*batchv1.Job{},
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
	assert.True(t, r.lifecycleNeedsDrain(s, "c"), "backup drains by default even when migrate does not")

	s.Spec.Lifecycle.Backup.RequiresDrain = new(false)
	assert.False(t, r.lifecycleNeedsDrain(s, "c"))
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
	for _, want := range []string{"pg_dump", "-Fc", `"$OUT.partial"`, "pg_restore --list", `mv "$OUT.partial" "$OUT"`, "set -eu", `if [ -e "$OUT" ]`} {
		assert.Contains(t, script, want)
	}
	assert.True(t, strings.HasPrefix(script, "set -eu\numask 077\n"), "dumps are created owner-only before anything is written")
	assert.NotContains(t, script, "postgres.default.svc", "CR values are passed via env, not interpolated")
	for line := range strings.SplitSeq(script, "\n") {
		if strings.Contains(line, "pg_dump") || strings.Contains(line, "pg_restore") {
			assert.NotContainsf(t, line, "|", "dump/verify must not run in a pipeline whose failure sh would mask: %q", line)
		}
	}

	s.Spec.Metastore.Type = new(dbTypeMySQL)
	mysql := buildBackupCommand(s)[2]
	assert.True(t, strings.HasPrefix(mysql, "set -eu\numask 077\n"))
	for _, want := range []string{"mysqldump", "--single-transaction", "--no-tablespaces", "-- Dump completed"} {
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
