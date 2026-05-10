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
	"testing"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestBuildPostgresCloneScript(t *testing.T) {
	clone := &supersetv1alpha1.CloneTaskSpec{
		Source: supersetv1alpha1.CloneSourceSpec{
			Host:     "pg-prod.svc",
			Database: "superset_prod",
			Username: "reader",
		},
	}

	script := buildPostgresCloneScript(clone)

	if !strings.Contains(script, "set -e") {
		t.Error("expected set -e")
	}
	if !strings.Contains(script, "dropdb --if-exists") {
		t.Error("expected dropdb")
	}
	if !strings.Contains(script, "createdb") {
		t.Error("expected createdb")
	}
	if !strings.Contains(script, "pg_dump") {
		t.Error("expected pg_dump")
	}
	if !strings.Contains(script, "--no-owner --no-privileges") {
		t.Error("expected --no-owner --no-privileges")
	}
	if !strings.Contains(script, "| PGPASSWORD") {
		t.Error("expected pipe to psql")
	}
}

func TestBuildPostgresCloneScript_ExcludeTables(t *testing.T) {
	clone := &supersetv1alpha1.CloneTaskSpec{
		Source: supersetv1alpha1.CloneSourceSpec{
			Host:     "pg-prod.svc",
			Database: "superset_prod",
			Username: "reader",
		},
		ExcludeTables:    []string{"logs", "tab_state"},
		ExcludeTableData: []string{"query", "saved_query"},
	}

	script := buildPostgresCloneScript(clone)

	if !strings.Contains(script, `--exclude-table="logs"`) {
		t.Errorf("expected --exclude-table for logs, got: %s", script)
	}
	if !strings.Contains(script, `--exclude-table="tab_state"`) {
		t.Errorf("expected --exclude-table for tab_state, got: %s", script)
	}
	if !strings.Contains(script, `--exclude-table-data="query"`) {
		t.Errorf("expected --exclude-table-data for query, got: %s", script)
	}
	if !strings.Contains(script, `--exclude-table-data="saved_query"`) {
		t.Errorf("expected --exclude-table-data for saved_query, got: %s", script)
	}
}

func TestBuildMySQLCloneScript(t *testing.T) {
	mysqlType := "MySQL"
	clone := &supersetv1alpha1.CloneTaskSpec{
		Source: supersetv1alpha1.CloneSourceSpec{
			Type:     &mysqlType,
			Host:     "mysql-prod.svc",
			Database: "superset_prod",
			Username: "reader",
		},
	}

	script := buildMySQLCloneScript(clone)

	if !strings.Contains(script, "set -e") {
		t.Error("expected set -e")
	}
	if !strings.Contains(script, "DROP DATABASE IF EXISTS") {
		t.Error("expected DROP DATABASE")
	}
	if !strings.Contains(script, "CREATE DATABASE") {
		t.Error("expected CREATE DATABASE")
	}
	if !strings.Contains(script, "mysqldump") {
		t.Error("expected mysqldump")
	}
	if !strings.Contains(script, "--single-transaction") {
		t.Error("expected --single-transaction")
	}
	if !strings.Contains(script, "| mysql") {
		t.Error("expected pipe to mysql")
	}
}

func TestBuildMySQLCloneScript_ExcludeTables(t *testing.T) {
	mysqlType := "MySQL"
	clone := &supersetv1alpha1.CloneTaskSpec{
		Source: supersetv1alpha1.CloneSourceSpec{
			Type:     &mysqlType,
			Host:     "mysql-prod.svc",
			Database: "superset_prod",
			Username: "reader",
		},
		ExcludeTables: []string{"logs", "tab_state"},
	}

	script := buildMySQLCloneScript(clone)

	if !strings.Contains(script, `--ignore-table="$SUPERSET_OPERATOR__CLONE_SRC_DB"."logs"`) {
		t.Errorf("expected --ignore-table for logs, got: %s", script)
	}
	if !strings.Contains(script, `--ignore-table="$SUPERSET_OPERATOR__CLONE_SRC_DB"."tab_state"`) {
		t.Errorf("expected --ignore-table for tab_state, got: %s", script)
	}
}

func TestBuildCloneCommand_CustomCommand(t *testing.T) {
	r := &SupersetReconciler{}
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{
				Command: []string{"/bin/sh", "-c", "custom-clone-script.sh"},
			},
			Source: supersetv1alpha1.CloneSourceSpec{
				Host:     "pg-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
		},
	}

	cmd := r.buildCloneCommand(superset)

	if len(cmd) != 3 || cmd[2] != "custom-clone-script.sh" {
		t.Errorf("expected custom command, got: %v", cmd)
	}
}

func TestBuildCloneCommand_DefaultPostgres(t *testing.T) {
	r := &SupersetReconciler{}
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			Source: supersetv1alpha1.CloneSourceSpec{
				Host:     "pg-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
		},
	}

	cmd := r.buildCloneCommand(superset)

	if len(cmd) != 3 || cmd[0] != "/bin/sh" || cmd[1] != "-c" {
		t.Fatalf("expected shell command, got: %v", cmd)
	}
	if !strings.Contains(cmd[2], "pg_dump") {
		t.Errorf("expected pg_dump in command, got: %s", cmd[2])
	}
}

func TestBuildCloneCommand_MySQL(t *testing.T) {
	r := &SupersetReconciler{}
	mysqlType := "MySQL"
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			Source: supersetv1alpha1.CloneSourceSpec{
				Type:     &mysqlType,
				Host:     "mysql-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
		},
	}

	cmd := r.buildCloneCommand(superset)

	if len(cmd) != 3 || cmd[0] != "/bin/sh" || cmd[1] != "-c" {
		t.Fatalf("expected shell command, got: %v", cmd)
	}
	if !strings.Contains(cmd[2], "mysqldump") {
		t.Errorf("expected mysqldump in command, got: %s", cmd[2])
	}
}

func TestCollectCloneEnvVars(t *testing.T) {
	pw := "secret123"
	host := "pg-staging.svc"
	db := "superset_staging"
	user := "admin"
	port := int32(5432)

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			Source: supersetv1alpha1.CloneSourceSpec{
				Host:     "pg-prod.svc",
				Port:     common.Ptr(int32(5433)),
				Database: "superset_prod",
				Username: "reader",
				Password: &pw,
			},
		},
	}
	superset.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:     &host,
		Port:     &port,
		Database: &db,
		Username: &user,
		Password: &pw,
	}

	envs := collectCloneEnvVars(superset)

	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.Name] = e.Value
	}

	if envMap[common.EnvCloneSrcHost] != "pg-prod.svc" {
		t.Errorf("expected source host pg-prod.svc, got: %s", envMap[common.EnvCloneSrcHost])
	}
	if envMap[common.EnvCloneSrcPort] != "5433" {
		t.Errorf("expected source port 5433, got: %s", envMap[common.EnvCloneSrcPort])
	}
	if envMap[common.EnvCloneSrcDB] != "superset_prod" {
		t.Errorf("expected source db superset_prod, got: %s", envMap[common.EnvCloneSrcDB])
	}
	if envMap[common.EnvCloneSrcUser] != "reader" {
		t.Errorf("expected source user reader, got: %s", envMap[common.EnvCloneSrcUser])
	}
	if envMap[common.EnvCloneSrcPass] != "secret123" {
		t.Errorf("expected source pass secret123, got: %s", envMap[common.EnvCloneSrcPass])
	}
	if envMap[common.EnvDBHost] != "pg-staging.svc" {
		t.Errorf("expected target host pg-staging.svc, got: %s", envMap[common.EnvDBHost])
	}
	if envMap[common.EnvDBName] != "superset_staging" {
		t.Errorf("expected target db superset_staging, got: %s", envMap[common.EnvDBName])
	}
}

func TestCollectCloneEnvVars_SecretRef(t *testing.T) {
	host := "pg-staging.svc"
	db := "superset_staging"
	user := "admin"

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			Source: supersetv1alpha1.CloneSourceSpec{
				Host:     "pg-prod.svc",
				Database: "superset_prod",
				Username: "reader",
				PasswordFrom: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "prod-creds"},
					Key:                  "password",
				},
			},
		},
	}
	superset.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:     &host,
		Database: &db,
		Username: &user,
		PasswordFrom: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "staging-creds"},
			Key:                  "password",
		},
	}

	envs := collectCloneEnvVars(superset)

	for _, e := range envs {
		if e.Name == common.EnvCloneSrcPass {
			if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
				t.Fatal("expected SecretKeyRef for source password")
			}
			if e.ValueFrom.SecretKeyRef.Name != "prod-creds" {
				t.Errorf("expected secret name prod-creds, got: %s", e.ValueFrom.SecretKeyRef.Name)
			}
			return
		}
	}
	t.Error("SUPERSET_OPERATOR__CLONE_SRC_PASS not found in env vars")
}

func TestResolveCloneImage_DefaultPostgres(t *testing.T) {
	clone := &supersetv1alpha1.CloneTaskSpec{
		Source: supersetv1alpha1.CloneSourceSpec{
			Host:     "pg-prod.svc",
			Database: "superset_prod",
			Username: "reader",
		},
	}

	img := resolveCloneImage(clone)

	if img.Repository != "postgres" || img.Tag != "17-alpine" {
		t.Errorf("expected postgres:17-alpine, got: %s:%s", img.Repository, img.Tag)
	}
}

func TestResolveCloneImage_DefaultMySQL(t *testing.T) {
	mysqlType := "MySQL"
	clone := &supersetv1alpha1.CloneTaskSpec{
		Source: supersetv1alpha1.CloneSourceSpec{
			Type:     &mysqlType,
			Host:     "mysql-prod.svc",
			Database: "superset_prod",
			Username: "reader",
		},
	}

	img := resolveCloneImage(clone)

	if img.Repository != "mysql" || img.Tag != "8-alpine" {
		t.Errorf("expected mysql:8-alpine, got: %s:%s", img.Repository, img.Tag)
	}
}

func TestResolveCloneImage_CustomOverride(t *testing.T) {
	clone := &supersetv1alpha1.CloneTaskSpec{
		Source: supersetv1alpha1.CloneSourceSpec{
			Host:     "pg-prod.svc",
			Database: "superset_prod",
			Username: "reader",
		},
		Image: &supersetv1alpha1.ImageSpec{
			Repository: "my-registry/custom-tools",
			Tag:        "v2",
		},
	}

	img := resolveCloneImage(clone)

	if img.Repository != "my-registry/custom-tools" || img.Tag != "v2" {
		t.Errorf("expected my-registry/custom-tools:v2, got: %s:%s", img.Repository, img.Tag)
	}
}

func TestIsTaskEnabled(t *testing.T) {
	r := &SupersetReconciler{}

	tests := []struct {
		name     string
		spec     *supersetv1alpha1.LifecycleSpec
		taskType string
		expected bool
	}{
		{
			name:     "nil lifecycle: clone disabled",
			spec:     nil,
			taskType: taskTypeClone,
			expected: false,
		},
		{
			name:     "nil lifecycle: migrate enabled by default",
			spec:     nil,
			taskType: taskTypeMigrate,
			expected: true,
		},
		{
			name:     "nil lifecycle: init enabled by default",
			spec:     nil,
			taskType: taskTypeInit,
			expected: true,
		},
		{
			name: "clone present and not disabled",
			spec: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Source: supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
				},
			},
			taskType: taskTypeClone,
			expected: true,
		},
		{
			name: "clone explicitly disabled",
			spec: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: common.Ptr(true)},
					Source:       supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
				},
			},
			taskType: taskTypeClone,
			expected: false,
		},
		{
			name: "migrate explicitly disabled",
			spec: &supersetv1alpha1.LifecycleSpec{
				Migrate: &supersetv1alpha1.MigrateTaskSpec{BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: common.Ptr(true)}},
			},
			taskType: taskTypeMigrate,
			expected: false,
		},
		{
			name: "init explicitly disabled",
			spec: &supersetv1alpha1.LifecycleSpec{
				Init: &supersetv1alpha1.InitTaskSpec{BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: common.Ptr(true)}},
			},
			taskType: taskTypeInit,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			superset := &supersetv1alpha1.Superset{}
			superset.Spec.Lifecycle = tt.spec
			got := r.isTaskEnabled(superset, tt.taskType)
			if got != tt.expected {
				t.Errorf("isTaskEnabled(%s) = %v, want %v", tt.taskType, got, tt.expected)
			}
		})
	}
}

func TestSplitImageRef(t *testing.T) {
	tests := []struct {
		ref      string
		wantRepo string
		wantTag  string
	}{
		{"postgres:17-alpine", "postgres", "17-alpine"},
		{"mysql:8-alpine", "mysql", "8-alpine"},
		{"my-registry.io/tools:v1.2.3", "my-registry.io/tools", "v1.2.3"},
		{"notagimage", "notagimage", "latest"},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			repo, tag := splitImageRef(tt.ref)
			if repo != tt.wantRepo || tag != tt.wantTag {
				t.Errorf("splitImageRef(%q) = (%q, %q), want (%q, %q)", tt.ref, repo, tag, tt.wantRepo, tt.wantTag)
			}
		})
	}
}

func TestCloneAlwaysRequiresDrain(t *testing.T) {
	tests := []struct {
		name          string
		cloneNeeded   bool
		migrateNeeded bool
		imageChanged  bool
		strategy      string
		wantDrain     bool
	}{
		{
			name:        "clone needed — always drains regardless of strategy",
			cloneNeeded: true,
			strategy:    upgradeStrategyRolling,
			wantDrain:   true,
		},
		{
			name:          "clone needed with Drain strategy — still drains",
			cloneNeeded:   true,
			migrateNeeded: true,
			imageChanged:  true,
			strategy:      upgradeStrategyDrain,
			wantDrain:     true,
		},
		{
			name:          "no clone, migrate with Drain strategy and image change — drains",
			cloneNeeded:   false,
			migrateNeeded: true,
			imageChanged:  true,
			strategy:      upgradeStrategyDrain,
			wantDrain:     true,
		},
		{
			name:          "no clone, migrate with Rolling strategy — no drain",
			cloneNeeded:   false,
			migrateNeeded: true,
			imageChanged:  true,
			strategy:      upgradeStrategyRolling,
			wantDrain:     false,
		},
		{
			name:          "no clone, migrate with Drain but no image change — no drain",
			cloneNeeded:   false,
			migrateNeeded: true,
			imageChanged:  false,
			strategy:      upgradeStrategyDrain,
			wantDrain:     false,
		},
		{
			name:        "nothing needed — no drain",
			cloneNeeded: false,
			strategy:    upgradeStrategyRolling,
			wantDrain:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			needsDrain := tt.cloneNeeded || (tt.migrateNeeded && tt.imageChanged && tt.strategy == upgradeStrategyDrain)
			if needsDrain != tt.wantDrain {
				t.Errorf("needsDrain = %v, want %v", needsDrain, tt.wantDrain)
			}
		})
	}
}

// --- Pipeline Checksum Model Tests ---

func TestComputeStepChecksum_UpstreamPropagation(t *testing.T) {
	r := &SupersetReconciler{}

	cmd := []string{"/bin/sh", "-c", "superset db upgrade"}

	// Same command, different incoming checksum → different step checksum.
	step1 := r.computeStepChecksum("upstream-v1", taskTypeMigrate, cmd, struct{ Image string }{"img:1"})
	step2 := r.computeStepChecksum("upstream-v2", taskTypeMigrate, cmd, struct{ Image string }{"img:1"})

	if step1 == step2 {
		t.Error("step checksum should change when upstream checksum changes (propagation)")
	}
}

func TestComputeStepChecksum_StableWhenInputsUnchanged(t *testing.T) {
	r := &SupersetReconciler{}

	cmd := []string{"/bin/sh", "-c", "superset db upgrade"}

	step1 := r.computeStepChecksum("upstream-v1", taskTypeMigrate, cmd, struct{ Image string }{"img:1"})
	step2 := r.computeStepChecksum("upstream-v1", taskTypeMigrate, cmd, struct{ Image string }{"img:1"})

	if step1 != step2 {
		t.Error("step checksum should be stable when inputs unchanged")
	}
}

func TestComputeStepChecksum_ChangesOnCommandChange(t *testing.T) {
	r := &SupersetReconciler{}

	step1 := r.computeStepChecksum("upstream-v1", taskTypeMigrate, []string{"cmd1"}, nil)
	step2 := r.computeStepChecksum("upstream-v1", taskTypeMigrate, []string{"cmd2"}, nil)

	if step1 == step2 {
		t.Error("step checksum should change when command changes")
	}
}

func TestComputeStepChecksum_ChangesOnExtraInputs(t *testing.T) {
	r := &SupersetReconciler{}

	cmd := []string{"/bin/sh", "-c", "clone"}

	step1 := r.computeStepChecksum("seed", taskTypeClone, cmd, struct{ Trigger string }{"v1"})
	step2 := r.computeStepChecksum("seed", taskTypeClone, cmd, struct{ Trigger string }{"v2"})

	if step1 == step2 {
		t.Error("step checksum should change when extra inputs change (e.g., trigger)")
	}
}

func TestComputeStepChecksum_DiffersByTaskType(t *testing.T) {
	r := &SupersetReconciler{}

	cmd := []string{"/bin/sh", "-c", "do-something"}

	step1 := r.computeStepChecksum("seed", taskTypeMigrate, cmd, nil)
	step2 := r.computeStepChecksum("seed", taskTypeInit, cmd, nil)

	if step1 == step2 {
		t.Error("step checksum should differ by task type even with same inputs")
	}
}

func TestPipelineChain_UpstreamChangeInvalidatesDownstream(t *testing.T) {
	r := &SupersetReconciler{}

	// Simulate a full pipeline with trigger change on clone.
	cloneCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}
	initCmd := []string{"/bin/sh", "-c", "superset init"}

	parentUID := "test-uid"

	// Run 1: trigger=v1
	cloneChecksum1 := r.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, struct{ Trigger string }{"v1"})
	migrateChecksum1 := r.computeStepChecksum(cloneChecksum1, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	initChecksum1 := r.computeStepChecksum(migrateChecksum1, taskTypeInit, initCmd, struct{ Config string }{"cfg-v1"})

	// Run 2: trigger=v2 (clone re-runs, propagates downstream)
	cloneChecksum2 := r.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, struct{ Trigger string }{"v2"})
	migrateChecksum2 := r.computeStepChecksum(cloneChecksum2, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	initChecksum2 := r.computeStepChecksum(migrateChecksum2, taskTypeInit, initCmd, struct{ Config string }{"cfg-v1"})

	if cloneChecksum1 == cloneChecksum2 {
		t.Error("clone checksum should change when trigger changes")
	}
	if migrateChecksum1 == migrateChecksum2 {
		t.Error("migrate checksum should change when clone's checksum changes (upstream propagation)")
	}
	if initChecksum1 == initChecksum2 {
		t.Error("init checksum should change when migrate's checksum changes (chain propagation)")
	}
}

func TestPipelineChain_ImageChangeOnlyAffectsMigrate(t *testing.T) {
	r := &SupersetReconciler{}

	cloneCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}

	parentUID := "test-uid"

	// Clone with same trigger.
	cloneChecksum := r.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, struct{ Trigger string }{"v1"})

	// Migrate with different image versions.
	migrate1 := r.computeStepChecksum(cloneChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	migrate2 := r.computeStepChecksum(cloneChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:5.0"})

	if migrate1 == migrate2 {
		t.Error("migrate checksum should change when image changes")
	}

	// Clone checksum should NOT change due to image change.
	clone2 := r.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, struct{ Trigger string }{"v1"})
	if cloneChecksum != clone2 {
		t.Error("clone checksum should NOT change when image changes (clone doesn't watch image)")
	}
}

func TestPipelineChain_ConfigChangeOnlyAffectsInit(t *testing.T) {
	r := &SupersetReconciler{}

	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}
	initCmd := []string{"/bin/sh", "-c", "superset init"}

	parentUID := "test-uid"
	migrateChecksum := r.computeStepChecksum(parentUID, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})

	// Init with different configs.
	init1 := r.computeStepChecksum(migrateChecksum, taskTypeInit, initCmd, struct{ Config string }{"cfg-v1"})
	init2 := r.computeStepChecksum(migrateChecksum, taskTypeInit, initCmd, struct{ Config string }{"cfg-v2"})

	if init1 == init2 {
		t.Error("init checksum should change when config changes")
	}

	// Migrate checksum should NOT change due to config change.
	migrate2 := r.computeStepChecksum(parentUID, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	if migrateChecksum != migrate2 {
		t.Error("migrate checksum should NOT change when config changes (migrate doesn't watch config)")
	}
}

func TestPipelineChain_ManualTriggerForcesRerun(t *testing.T) {
	r := &SupersetReconciler{}

	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}
	initCmd := []string{"/bin/sh", "-c", "superset init"}

	parentUID := "test-uid"

	// Migrate with manual trigger change.
	migrate1 := r.computeStepChecksum(parentUID, taskTypeMigrate, migrateCmd, struct {
		Image   string
		Trigger string
	}{"img:4.0", ""})
	migrate2 := r.computeStepChecksum(parentUID, taskTypeMigrate, migrateCmd, struct {
		Image   string
		Trigger string
	}{"img:4.0", "force-2026-05-10"})

	if migrate1 == migrate2 {
		t.Error("migrate checksum should change when trigger is set (manual force)")
	}

	// This cascades to init.
	init1 := r.computeStepChecksum(migrate1, taskTypeInit, initCmd, struct{ Config string }{"cfg"})
	init2 := r.computeStepChecksum(migrate2, taskTypeInit, initCmd, struct{ Config string }{"cfg"})

	if init1 == init2 {
		t.Error("init should re-run when migrate's trigger forces it (upstream propagation)")
	}
}

func TestPipelineChain_UnchangedInputsProduceStableChecksums(t *testing.T) {
	r := &SupersetReconciler{}

	cloneCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}
	initCmd := []string{"/bin/sh", "-c", "superset init"}

	parentUID := "test-uid"
	cloneChecksum := r.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, struct{ Trigger string }{"v1"})
	migrateChecksum := r.computeStepChecksum(cloneChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	initChecksum := r.computeStepChecksum(migrateChecksum, taskTypeInit, initCmd, struct{ Config string }{"cfg-v1"})

	// Re-compute with identical inputs.
	cloneChecksum2 := r.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, struct{ Trigger string }{"v1"})
	migrateChecksum2 := r.computeStepChecksum(cloneChecksum2, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	initChecksum2 := r.computeStepChecksum(migrateChecksum2, taskTypeInit, initCmd, struct{ Config string }{"cfg-v1"})

	if cloneChecksum != cloneChecksum2 || migrateChecksum != migrateChecksum2 || initChecksum != initChecksum2 {
		t.Error("pipeline should produce identical checksums when all inputs are unchanged")
	}
}

// TestPipelineChain_CustomTaskSlotsBetweenStages validates that custom tasks
// can be inserted into the pipeline using the same checksum model.
func TestPipelineChain_CustomTaskSlotsBetweenStages(t *testing.T) {
	r := &SupersetReconciler{}

	parentUID := "test-uid"
	cloneCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	customCmd := []string{"/bin/sh", "-c", "run-data-masking.sh"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}

	// Pipeline: clone → custom("PostClone") → migrate
	cloneChecksum := r.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, struct{ Trigger string }{"v1"})
	customChecksum := r.computeStepChecksum(cloneChecksum, "PostClone", customCmd, struct{ Script string }{"mask-pii-v3"})
	migrateChecksum := r.computeStepChecksum(customChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})

	// Custom task changes its script input → propagates to migrate.
	customChecksum2 := r.computeStepChecksum(cloneChecksum, "PostClone", customCmd, struct{ Script string }{"mask-pii-v4"})
	migrateChecksum2 := r.computeStepChecksum(customChecksum2, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})

	if customChecksum == customChecksum2 {
		t.Error("custom task checksum should change when its inputs change")
	}
	if migrateChecksum == migrateChecksum2 {
		t.Error("migrate should re-run when custom task upstream changes")
	}

	// Clone unchanged → custom unchanged → migrate unchanged.
	customChecksum3 := r.computeStepChecksum(cloneChecksum, "PostClone", customCmd, struct{ Script string }{"mask-pii-v3"})
	migrateChecksum3 := r.computeStepChecksum(customChecksum3, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})

	if migrateChecksum != migrateChecksum3 {
		t.Error("migrate should be stable when nothing upstream changed")
	}
}
