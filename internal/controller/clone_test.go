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
	"time"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
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
	if strings.Contains(script, "`") {
		t.Error("script must not contain backticks (shell command substitution)")
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

func TestBuildCloneScript_PostCloneSQL(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		clone := &supersetv1alpha1.CloneTaskSpec{
			Source: supersetv1alpha1.CloneSourceSpec{
				Host: "pg-prod.svc", Database: "superset_prod", Username: "reader",
			},
			PostCloneSQL: []string{
				"UPDATE report_schedule SET active = false",
				"DELETE FROM oauth2_token",
			},
		}

		script := buildPostgresCloneScript(clone)

		if !strings.Contains(script, `psql`) {
			t.Fatal("expected psql in script")
		}
		if !strings.Contains(script, `-c "UPDATE report_schedule SET active = false"`) {
			t.Errorf("expected first postCloneSQL statement, got: %s", script)
		}
		if !strings.Contains(script, `-c "DELETE FROM oauth2_token"`) {
			t.Errorf("expected second postCloneSQL statement, got: %s", script)
		}
	})

	t.Run("mysql", func(t *testing.T) {
		mysqlType := "MySQL"
		clone := &supersetv1alpha1.CloneTaskSpec{
			Source: supersetv1alpha1.CloneSourceSpec{
				Type: &mysqlType, Host: "mysql-prod.svc", Database: "superset_prod", Username: "reader",
			},
			PostCloneSQL: []string{"UPDATE report_schedule SET active = 0"},
		}

		script := buildMySQLCloneScript(clone)

		if !strings.Contains(script, `-e "UPDATE report_schedule SET active = 0"`) {
			t.Errorf("expected postCloneSQL statement in mysql script, got: %s", script)
		}
	})
}

func TestBuildCloneCommand_CustomCommand(t *testing.T) {
	r := &SupersetReconciler{}
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{
					Command: []string{"/bin/sh", "-c", "custom-clone-script.sh"},
				},
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

func TestBuildCloneTaskFlatSpec_CommandOnContainer(t *testing.T) {
	r := &SupersetReconciler{}
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			Source: supersetv1alpha1.CloneSourceSpec{
				Host:     "pg-prod.svc",
				Database: "superset_prod",
				Username: "reader",
				Password: common.Ptr("secret"),
			},
		},
	}
	superset.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:     common.Ptr("postgres"),
		Database: common.Ptr("superset_staging"),
		Username: common.Ptr("superset"),
		Password: common.Ptr("pass"),
	}

	flatSpec := r.buildCloneTaskFlatSpec(superset, "default", &resolution.SharedInput{})
	podSpec := buildInitPod(&flatSpec)

	if len(podSpec.Containers) == 0 {
		t.Fatal("expected at least one container")
	}
	cmd := podSpec.Containers[0].Command
	if len(cmd) == 0 {
		t.Fatal("expected command on clone pod container, got nil")
	}
	if cmd[0] != "/bin/sh" || !strings.Contains(cmd[2], "pg_dump") {
		t.Errorf("expected pg_dump shell command, got: %v", cmd)
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
			name:     "nil lifecycle: rotate disabled",
			spec:     nil,
			taskType: taskTypeRotate,
			expected: false,
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
					SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: common.Ptr(true)}},
					Source:                  supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
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
		{
			name: "rotate present and not disabled",
			spec: &supersetv1alpha1.LifecycleSpec{
				Rotate: &supersetv1alpha1.RotateTaskSpec{},
			},
			taskType: taskTypeRotate,
			expected: true,
		},
		{
			name: "rotate explicitly disabled",
			spec: &supersetv1alpha1.LifecycleSpec{
				Rotate: &supersetv1alpha1.RotateTaskSpec{BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: common.Ptr(true)}},
			},
			taskType: taskTypeRotate,
			expected: false,
		},
		{
			name:     "rotate nil with lifecycle set",
			spec:     &supersetv1alpha1.LifecycleSpec{},
			taskType: taskTypeRotate,
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

func TestTagFromImageRef(t *testing.T) {
	tests := []struct {
		ref  string
		want string
	}{
		{"apache/superset:4.1.0", "4.1.0"},
		{"registry:5000/apache/superset:4.1.0", "4.1.0"},
		{"localhost:5000/img:latest", "latest"},
		{"registry.io/image", "registry.io/image"},
		{"myimage:v2.0.0-rc1", "v2.0.0-rc1"},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := tagFromImageRef(tt.ref)
			if got != tt.want {
				t.Errorf("tagFromImageRef(%q) = %q, want %q", tt.ref, got, tt.want)
			}
		})
	}
}

func TestTaskRequiresDrain_Defaults(t *testing.T) {
	r := &SupersetReconciler{}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			Source: supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
		},
	}

	tests := []struct {
		taskType string
		want     bool
	}{
		{taskTypeClone, true},
		{taskTypeMigrate, true},
		{taskTypeRotate, true},
		{taskTypeInit, false},
	}

	for _, tt := range tests {
		t.Run(tt.taskType, func(t *testing.T) {
			got := r.taskRequiresDrain(superset, tt.taskType)
			if got != tt.want {
				t.Errorf("taskRequiresDrain(%s) = %v, want %v", tt.taskType, got, tt.want)
			}
		})
	}
}

func TestTaskRequiresDrain_Override(t *testing.T) {
	r := &SupersetReconciler{}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{
			BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{RequiresDrain: common.Ptr(false)},
		},
		Init: &supersetv1alpha1.InitTaskSpec{
			BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{RequiresDrain: common.Ptr(true)},
		},
	}

	if r.taskRequiresDrain(superset, taskTypeMigrate) {
		t.Error("migrate should NOT drain when requiresDrain=false override is set")
	}
	if !r.taskRequiresDrain(superset, taskTypeInit) {
		t.Error("init SHOULD drain when requiresDrain=true override is set")
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

// TestPipelineChain_DownstreamInputChangesDoNotRippleUpstream verifies that
// the cascade only flows in the pipeline direction: changing a migrate-only
// hashed input does not affect clone's checksum. Production clone inputs
// include the target Superset image (see cloneInputs); this test exercises
// the cascade math with synthetic inputs that intentionally omit it.
func TestPipelineChain_DownstreamInputChangesDoNotRippleUpstream(t *testing.T) {
	r := &SupersetReconciler{}

	cloneCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}

	parentUID := "test-uid"

	// Synthetic clone inputs (Trigger only) — in production cloneInputs also
	// includes target image, but here we want to isolate cascade direction.
	cloneChecksum := r.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, struct{ Trigger string }{"v1"})

	// Migrate with different image versions.
	migrate1 := r.computeStepChecksum(cloneChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	migrate2 := r.computeStepChecksum(cloneChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:5.0"})

	if migrate1 == migrate2 {
		t.Error("migrate checksum should change when its hashed Image input changes")
	}

	// Re-computing clone with identical synthetic inputs yields the same
	// checksum: cascade does not ripple upstream from migrate.
	clone2 := r.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, struct{ Trigger string }{"v1"})
	if cloneChecksum != clone2 {
		t.Error("clone checksum should not change when only migrate's hashed input changed (cascade is downstream-only)")
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

func TestCloneInputs_ScheduleTickChangesChecksum(t *testing.T) {
	// When the clock crosses a cron boundary, the clone checksum should change.
	before := time.Date(2026, 5, 11, 1, 59, 0, 0, time.UTC)
	after := time.Date(2026, 5, 11, 2, 1, 0, 0, time.UTC)

	cronExpr := "0 2 * * *"
	source := supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"}

	r1 := &SupersetReconciler{Now: func() time.Time { return before }}
	r2 := &SupersetReconciler{Now: func() time.Time { return after }}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &cronExpr},
			Source:                  source,
		},
	}

	inputs1 := r1.cloneInputs(superset)
	inputs2 := r2.cloneInputs(superset)

	checksum1 := r1.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, inputs1)
	checksum2 := r2.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, inputs2)

	if checksum1 == checksum2 {
		t.Error("clone checksum should change when crossing a cron boundary")
	}
}

func TestCloneInputs_ScheduleAndTrigger_BothContribute(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 0, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	cronExpr := "0 * * * *"
	source := supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"}
	trigger1 := "v1"
	trigger2 := "v2"

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Trigger: &trigger1},
				CronSchedule: &cronExpr,
			},
			Source: source,
		},
	}

	inputs1 := r.cloneInputs(superset)
	checksum1 := r.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, inputs1)

	// Change trigger only.
	superset.Spec.Lifecycle.Clone.Trigger = &trigger2
	inputs2 := r.cloneInputs(superset)
	checksum2 := r.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, inputs2)

	if checksum1 == checksum2 {
		t.Error("changing trigger should change checksum even with same schedule tick")
	}
}

func TestCloneInputs_TargetImageChangesChecksum(t *testing.T) {
	r := &SupersetReconciler{}
	source := supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Image = supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.1.0rc3-dev"}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			Source: source,
		},
	}

	checksum1 := r.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, r.cloneInputs(superset))

	superset.Spec.Image.Tag = "6.1.0-dev"
	checksum2 := r.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, r.cloneInputs(superset))

	if checksum1 == checksum2 {
		t.Error("clone checksum should change when the target Superset image changes")
	}
}

func TestCloneInputs_NoSchedule_StableChecksum(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 0, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	source := supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"}
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			Source: source,
		},
	}

	inputs1 := r.cloneInputs(superset)
	inputs2 := r.cloneInputs(superset)

	checksum1 := r.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, inputs1)
	checksum2 := r.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, inputs2)

	if checksum1 != checksum2 {
		t.Error("checksum should be stable when no schedule is set")
	}
}

func TestCloneInputs_ScheduleStableWithinWindow(t *testing.T) {
	// Two reconciles within the same cron window produce the same checksum.
	t1 := time.Date(2026, 5, 11, 14, 10, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 11, 14, 50, 0, 0, time.UTC)

	cronExpr := "0 * * * *"
	source := supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &cronExpr},
			Source:                  source,
		},
	}

	r1 := &SupersetReconciler{Now: func() time.Time { return t1 }}
	r2 := &SupersetReconciler{Now: func() time.Time { return t2 }}

	checksum1 := r1.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, r1.cloneInputs(superset))
	checksum2 := r2.computeStepChecksum("uid", taskTypeClone, []string{"cmd"}, r2.cloneInputs(superset))

	if checksum1 != checksum2 {
		t.Error("checksum should be stable within the same cron window")
	}
}

func TestPipelineChain_ScheduleTickPropagatesDownstream(t *testing.T) {
	before := time.Date(2026, 5, 11, 1, 59, 0, 0, time.UTC)
	after := time.Date(2026, 5, 11, 2, 1, 0, 0, time.UTC)

	cronExpr := "0 2 * * *"
	source := supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &cronExpr},
			Source:                  source,
		},
	}

	cloneCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}
	initCmd := []string{"/bin/sh", "-c", "superset init"}
	parentUID := "test-uid"

	// Before boundary.
	r1 := &SupersetReconciler{Now: func() time.Time { return before }}
	cloneChecksum1 := r1.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, r1.cloneInputs(superset))
	migrateChecksum1 := r1.computeStepChecksum(cloneChecksum1, taskTypeMigrate, migrateCmd, struct {
		Image   string
		Trigger string
	}{"img:4.0", ""})
	initChecksum1 := r1.computeStepChecksum(migrateChecksum1, taskTypeInit, initCmd, struct {
		ConfigChecksum string
		Trigger        string
	}{"cfg", ""})

	// After boundary.
	r2 := &SupersetReconciler{Now: func() time.Time { return after }}
	cloneChecksum2 := r2.computeStepChecksum(parentUID, taskTypeClone, cloneCmd, r2.cloneInputs(superset))
	migrateChecksum2 := r2.computeStepChecksum(cloneChecksum2, taskTypeMigrate, migrateCmd, struct {
		Image   string
		Trigger string
	}{"img:4.0", ""})
	initChecksum2 := r2.computeStepChecksum(migrateChecksum2, taskTypeInit, initCmd, struct {
		ConfigChecksum string
		Trigger        string
	}{"cfg", ""})

	if cloneChecksum1 == cloneChecksum2 {
		t.Error("clone checksum should change after boundary")
	}
	if migrateChecksum1 == migrateChecksum2 {
		t.Error("migrate should cascade from clone schedule tick change")
	}
	if initChecksum1 == initChecksum2 {
		t.Error("init should cascade from clone schedule tick change")
	}
}

func TestScheduleRequeue_ComputesCorrectDuration(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	cronExpr := "0 * * * *" // hourly at :00
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &cronExpr},
			Source:                  supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
		},
	}

	requeue := r.nextScheduleRequeue(superset)
	// Next tick is 15:00, so 30 minutes + 1s buffer.
	expected := 30*time.Minute + time.Second
	if requeue != expected {
		t.Errorf("expected requeue %v, got %v", expected, requeue)
	}
}

func TestScheduleRequeue_NoSchedule(t *testing.T) {
	r := &SupersetReconciler{Now: func() time.Time { return time.Now() }}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			Source: supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
		},
	}

	requeue := r.nextScheduleRequeue(superset)
	if requeue != 0 {
		t.Errorf("expected 0 requeue with no schedule, got %v", requeue)
	}
}

func TestScheduleRequeue_DisabledClone(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	cronExpr := "0 * * * *"
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: common.Ptr(true)},
				CronSchedule: &cronExpr,
			},
			Source: supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
		},
	}

	requeue := r.nextScheduleRequeue(superset)
	if requeue != 0 {
		t.Errorf("expected 0 requeue with disabled clone, got %v", requeue)
	}
}

func TestAllTasksStillComplete_SkipsDrainWhenNothingChanged(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	superset := &supersetv1alpha1.Superset{}
	superset.UID = "test-uid"
	superset.Spec.Image = supersetv1alpha1.ImageSpec{Tag: "4.1.4"}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{},
		Init:    &supersetv1alpha1.InitTaskSpec{},
	}

	configChecksum := "config-abc"

	// Simulate a completed lifecycle: compute checksums and store them.
	incomingChecksum := string(superset.UID)
	migrateCmd := defaultMigrateCommand(superset)
	migrateChecksum := r.computeStepChecksum(incomingChecksum, taskTypeMigrate, migrateCmd, r.migrateInputs(superset))
	initCmd := defaultInitCommand(superset)
	initChecksum := r.computeStepChecksum(migrateChecksum, taskTypeInit, initCmd, r.initInputs(superset, configChecksum))

	superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{
		LastCompletedChecksums: map[string]string{
			taskTypeMigrate: migrateChecksum,
			taskTypeInit:    initChecksum,
		},
	}

	t.Run("returns true when nothing changed", func(t *testing.T) {
		if !r.allTasksStillComplete(superset, configChecksum) {
			t.Error("expected allTasksStillComplete=true when checksums match")
		}
	})

	t.Run("returns false when config changes", func(t *testing.T) {
		if r.allTasksStillComplete(superset, "config-changed") {
			t.Error("expected allTasksStillComplete=false when config checksum changed")
		}
	})

	t.Run("returns false when image changes", func(t *testing.T) {
		modified := superset.DeepCopy()
		modified.Spec.Image.Tag = "5.0.0"
		if r.allTasksStillComplete(modified, configChecksum) {
			t.Error("expected allTasksStillComplete=false when image changed")
		}
	})

	t.Run("returns false with no stored checksums", func(t *testing.T) {
		modified := superset.DeepCopy()
		modified.Status.Lifecycle.LastCompletedChecksums = nil
		if r.allTasksStillComplete(modified, configChecksum) {
			t.Error("expected allTasksStillComplete=false with nil checksums")
		}
	})

	t.Run("returns false when trigger changes", func(t *testing.T) {
		modified := superset.DeepCopy()
		modified.Spec.Lifecycle.Migrate = &supersetv1alpha1.MigrateTaskSpec{
			BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Trigger: common.Ptr("force-v1")},
		}
		if r.allTasksStillComplete(modified, configChecksum) {
			t.Error("expected allTasksStillComplete=false when trigger changed")
		}
	})
}

func TestAllTasksStillComplete_WithCloneSchedule(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	cronExpr := "0 * * * *"
	superset := &supersetv1alpha1.Superset{}
	superset.UID = "test-uid"
	superset.Spec.Image = supersetv1alpha1.ImageSpec{Tag: "4.1.4"}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
				CronSchedule: &cronExpr,
			},
			Source: supersetv1alpha1.CloneSourceSpec{Host: "prod-db", Database: "superset", Username: "reader"},
		},
		Migrate: &supersetv1alpha1.MigrateTaskSpec{},
		Init:    &supersetv1alpha1.InitTaskSpec{},
	}

	configChecksum := "config-abc"

	// Compute and store checksums as if lifecycle already completed at :30.
	incomingChecksum := string(superset.UID)
	cloneCmd := r.buildCloneCommand(superset)
	cloneChecksum := r.computeStepChecksum(incomingChecksum, taskTypeClone, cloneCmd, r.cloneInputs(superset))
	migrateCmd := defaultMigrateCommand(superset)
	migrateChecksum := r.computeStepChecksum(cloneChecksum, taskTypeMigrate, migrateCmd, r.migrateInputs(superset))
	initCmd := defaultInitCommand(superset)
	initChecksum := r.computeStepChecksum(migrateChecksum, taskTypeInit, initCmd, r.initInputs(superset, configChecksum))

	superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{
		LastCompletedChecksums: map[string]string{
			taskTypeClone:   cloneChecksum,
			taskTypeMigrate: migrateChecksum,
			taskTypeInit:    initChecksum,
		},
	}

	t.Run("stable within cron window", func(t *testing.T) {
		if !r.allTasksStillComplete(superset, configChecksum) {
			t.Error("expected allTasksStillComplete=true within same cron window")
		}
	})

	t.Run("returns false when cron tick crosses boundary", func(t *testing.T) {
		nextHour := time.Date(2026, 5, 11, 15, 1, 0, 0, time.UTC)
		r2 := &SupersetReconciler{Now: func() time.Time { return nextHour }}
		if r2.allTasksStillComplete(superset, configChecksum) {
			t.Error("expected allTasksStillComplete=false after cron boundary crossing")
		}
	})
}

func TestCollectSecretEnvVars_PreviousSecretKey(t *testing.T) {
	t.Run("dev mode plaintext", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			Environment:       common.Ptr("Development"),
			SecretKey:         common.Ptr("new-key"),
			PreviousSecretKey: common.Ptr("old-key"),
		}
		envs := collectSecretEnvVars(spec)
		found := false
		for _, e := range envs {
			if e.Name == common.EnvPreviousSecretKey {
				found = true
				if e.Value != "old-key" {
					t.Errorf("expected plaintext value 'old-key', got %q", e.Value)
				}
			}
		}
		if !found {
			t.Error("expected SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY env var")
		}
	})

	t.Run("prod mode secretKeyRef", func(t *testing.T) {
		ref := &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "prev-secret"},
			Key:                  "key",
		}
		spec := &supersetv1alpha1.SupersetSpec{
			SecretKeyFrom:         &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"},
			PreviousSecretKeyFrom: ref,
		}
		envs := collectSecretEnvVars(spec)
		found := false
		for _, e := range envs {
			if e.Name == common.EnvPreviousSecretKey {
				found = true
				if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
					t.Fatal("expected secretKeyRef")
				}
				if e.ValueFrom.SecretKeyRef.Name != "prev-secret" {
					t.Errorf("expected secret name 'prev-secret', got %q", e.ValueFrom.SecretKeyRef.Name)
				}
			}
		}
		if !found {
			t.Error("expected SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY env var")
		}
	})

	t.Run("not present when not configured", func(t *testing.T) {
		spec := &supersetv1alpha1.SupersetSpec{
			SecretKeyFrom: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"},
		}
		envs := collectSecretEnvVars(spec)
		for _, e := range envs {
			if e.Name == common.EnvPreviousSecretKey {
				t.Error("should not have SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY when not configured")
			}
		}
	})
}

func TestDefaultRotateCommand(t *testing.T) {
	t.Run("default command", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{}
		cmd := defaultRotateCommand(superset)
		if len(cmd) != 3 || cmd[2] != "superset re-encrypt-secrets" {
			t.Errorf("unexpected default command: %v", cmd)
		}
	})

	t.Run("custom command", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{}
		superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
			Rotate: &supersetv1alpha1.RotateTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{
					Command: []string{"/bin/sh", "-c", "custom-rotate"},
				},
			},
		}
		cmd := defaultRotateCommand(superset)
		if len(cmd) != 3 || cmd[2] != "custom-rotate" {
			t.Errorf("expected custom command, got: %v", cmd)
		}
	})
}

func TestRotateInputs(t *testing.T) {
	r := &SupersetReconciler{}

	secretRef := &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "secret-v1"},
		Key:                  "key",
	}
	prevRef := &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "secret-v0"},
		Key:                  "key",
	}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.SecretKeyFrom = secretRef
	superset.Spec.PreviousSecretKeyFrom = prevRef
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Rotate: &supersetv1alpha1.RotateTaskSpec{},
	}

	cmd := defaultRotateCommand(superset)
	base := r.computeStepChecksum("seed", taskTypeRotate, cmd, r.rotateInputs(superset))

	t.Run("changes when previousSecretKeyFrom changes", func(t *testing.T) {
		modified := superset.DeepCopy()
		modified.Spec.PreviousSecretKeyFrom = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "secret-v0-changed"},
			Key:                  "key",
		}
		check := r.computeStepChecksum("seed", taskTypeRotate, cmd, r.rotateInputs(modified))
		if check == base {
			t.Error("expected checksum to change when previousSecretKeyFrom changes")
		}
	})

	t.Run("changes when secretKeyFrom changes", func(t *testing.T) {
		modified := superset.DeepCopy()
		modified.Spec.SecretKeyFrom = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "secret-v2"},
			Key:                  "key",
		}
		check := r.computeStepChecksum("seed", taskTypeRotate, cmd, r.rotateInputs(modified))
		if check == base {
			t.Error("expected checksum to change when secretKeyFrom changes")
		}
	})

	t.Run("changes when trigger changes", func(t *testing.T) {
		modified := superset.DeepCopy()
		modified.Spec.Lifecycle.Rotate.Trigger = common.Ptr("force-v1")
		check := r.computeStepChecksum("seed", taskTypeRotate, cmd, r.rotateInputs(modified))
		if check == base {
			t.Error("expected checksum to change when trigger changes")
		}
	})

	t.Run("stable when nothing changes", func(t *testing.T) {
		check := r.computeStepChecksum("seed", taskTypeRotate, cmd, r.rotateInputs(superset))
		if check != base {
			t.Error("expected checksum to be stable when nothing changes")
		}
	})
}

func TestAllTasksStillComplete_WithRotate(t *testing.T) {
	r := &SupersetReconciler{}

	secretRef := &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "secret-v1"},
		Key:                  "key",
	}
	prevRef := &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "secret-v0"},
		Key:                  "key",
	}

	superset := &supersetv1alpha1.Superset{}
	superset.UID = "test-uid"
	superset.Spec.Image = supersetv1alpha1.ImageSpec{Tag: "4.1.4"}
	superset.Spec.SecretKeyFrom = secretRef
	superset.Spec.PreviousSecretKeyFrom = prevRef
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Rotate: &supersetv1alpha1.RotateTaskSpec{},
	}

	configChecksum := "config-abc"

	incomingChecksum := string(superset.UID)
	migrateCmd := defaultMigrateCommand(superset)
	migrateChecksum := r.computeStepChecksum(incomingChecksum, taskTypeMigrate, migrateCmd, r.migrateInputs(superset))
	rotateCmd := defaultRotateCommand(superset)
	rotateChecksum := r.computeStepChecksum(migrateChecksum, taskTypeRotate, rotateCmd, r.rotateInputs(superset))
	initCmd := defaultInitCommand(superset)
	initChecksum := r.computeStepChecksum(rotateChecksum, taskTypeInit, initCmd, r.initInputs(superset, configChecksum))

	superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{
		LastCompletedChecksums: map[string]string{
			taskTypeMigrate: migrateChecksum,
			taskTypeRotate:  rotateChecksum,
			taskTypeInit:    initChecksum,
		},
	}

	t.Run("returns true when nothing changed", func(t *testing.T) {
		if !r.allTasksStillComplete(superset, configChecksum) {
			t.Error("expected allTasksStillComplete=true when checksums match")
		}
	})

	t.Run("returns false when previousSecretKeyFrom changes", func(t *testing.T) {
		modified := superset.DeepCopy()
		modified.Spec.PreviousSecretKeyFrom = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "rotated"},
			Key:                  "key",
		}
		if r.allTasksStillComplete(modified, configChecksum) {
			t.Error("expected allTasksStillComplete=false when previousSecretKeyFrom changes")
		}
	})

	t.Run("rotate cascades to init", func(t *testing.T) {
		modified := superset.DeepCopy()
		modified.Spec.Lifecycle.Rotate.Trigger = common.Ptr("force")
		if r.allTasksStillComplete(modified, configChecksum) {
			t.Error("expected allTasksStillComplete=false when rotate trigger changes (cascades to init)")
		}
	})
}

// TestIsTaskEnabled_InvalidCronScheduleGatesClone verifies that an invalid
// cron expression causes clone to be treated as disabled — the user's
// malformed schedule surfaces as a ScheduleValid=False condition (set by
// validateSchedules) and does not trigger an opportunistic one-shot clone.
func TestIsTaskEnabled_InvalidCronScheduleGatesClone(t *testing.T) {
	r := &SupersetReconciler{}

	badExpr := "not a cron expression"
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Clone: &supersetv1alpha1.CloneTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
				CronSchedule: &badExpr,
			},
			Source: supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
		},
	}

	if r.isTaskEnabled(superset, taskTypeClone) {
		t.Fatal("expected clone to be disabled when CronSchedule is invalid")
	}

	// A valid schedule re-enables clone.
	validExpr := "0 * * * *"
	superset.Spec.Lifecycle.Clone.CronSchedule = &validExpr
	if !r.isTaskEnabled(superset, taskTypeClone) {
		t.Fatal("expected clone to be enabled for a valid CronSchedule")
	}

	// No schedule at all — clone remains enabled (legacy on-demand path).
	superset.Spec.Lifecycle.Clone.CronSchedule = nil
	if !r.isTaskEnabled(superset, taskTypeClone) {
		t.Fatal("expected clone to be enabled when no CronSchedule is set")
	}
}
