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

func TestTaskStrategy_Clone(t *testing.T) {
	r := &SupersetReconciler{}

	tests := []struct {
		name     string
		spec     *supersetv1alpha1.LifecycleSpec
		expected string
	}{
		{
			name:     "nil lifecycle returns Never for clone",
			spec:     nil,
			expected: strategyNever,
		},
		{
			name:     "nil clone returns Never",
			spec:     &supersetv1alpha1.LifecycleSpec{},
			expected: strategyNever,
		},
		{
			name: "clone with no strategy defaults to OnTrigger",
			spec: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Source: supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
				},
			},
			expected: strategyOnTrigger,
		},
		{
			name: "clone with explicit Always",
			spec: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Strategy: common.Ptr("Always"),
					Source:   supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
				},
			},
			expected: strategyAlways,
		},
		{
			name: "clone with explicit Never",
			spec: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Strategy: common.Ptr("Never"),
					Source:   supersetv1alpha1.CloneSourceSpec{Host: "h", Database: "d", Username: "u"},
				},
			},
			expected: strategyNever,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			superset := &supersetv1alpha1.Superset{}
			superset.Spec.Lifecycle = tt.spec
			got := r.taskStrategy(superset, taskTypeClone)
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
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
