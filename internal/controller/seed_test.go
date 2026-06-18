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
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// TestBuildSeedScript covers the seed-script builders (buildPostgresSeedScript /
// buildMySQLSeedScript): base dump+restore pipelines, table and table-data
// exclusions, single- vs two-pass mysqldump, and postSeedSQL injection.
func TestBuildSeedScript(t *testing.T) {
	t.Run("postgres base", func(t *testing.T) {
		seed := &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Host:     "pg-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
		}

		script := buildPostgresSeedScript(seed)

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
	})

	t.Run("postgres exclude tables", func(t *testing.T) {
		seed := &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Host:     "pg-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
			ExcludeTables:    []string{"logs", "tab_state"},
			ExcludeTableData: []string{"query", "saved_query"},
		}

		script := buildPostgresSeedScript(seed)

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
	})

	t.Run("mysql base", func(t *testing.T) {
		mysqlType := "MySQL"
		seed := &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Type:     &mysqlType,
				Host:     "mysql-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
		}

		script := buildMySQLSeedScript(seed)

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
		// Passwords must travel via MYSQL_PWD, never on the command line, so
		// they don't leak into process argv (ps / /proc/<pid>/cmdline).
		if strings.Contains(script, `-p"$`) {
			t.Errorf("password must not be passed via -p (leaks into argv), got: %s", script)
		}
		if !strings.Contains(script, `export MYSQL_PWD="$SUPERSET_OPERATOR__DB_PASS"`) {
			t.Errorf("expected target password via MYSQL_PWD, got: %s", script)
		}
		if !strings.Contains(script, `export MYSQL_PWD="$SUPERSET_OPERATOR__SEED_SRC_PASS"`) {
			t.Errorf("expected source password via MYSQL_PWD, got: %s", script)
		}
		// The destructive DROP/CREATE must use a backtick-quoted, escaped
		// identifier rather than interpolating the raw name into SQL. The
		// backticks are backslash-escaped so the shell passes them literally.
		if !strings.Contains(script, "ESC_NAME=$(printf '%s' \"$SUPERSET_OPERATOR__DB_NAME\" | sed 's/`/``/g')") {
			t.Errorf("expected DB identifier to be backtick-escaped, got: %s", script)
		}
		if !strings.Contains(script, "DROP DATABASE IF EXISTS \\`${ESC_NAME}\\`") {
			t.Errorf("expected backtick-quoted identifier in DROP DATABASE, got: %s", script)
		}
		if !strings.Contains(script, "CREATE DATABASE \\`${ESC_NAME}\\`") {
			t.Errorf("expected backtick-quoted identifier in CREATE DATABASE, got: %s", script)
		}
		// The raw, unquoted identifier must no longer reach the SQL statement.
		if strings.Contains(script, "DROP DATABASE IF EXISTS $SUPERSET_OPERATOR__DB_NAME") {
			t.Errorf("DB name must be backtick-quoted, not interpolated raw, got: %s", script)
		}
	})

	t.Run("mysql exclude tables", func(t *testing.T) {
		mysqlType := "MySQL"
		seed := &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Type:     &mysqlType,
				Host:     "mysql-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
			ExcludeTables: []string{"logs", "tab_state"},
		}

		script := buildMySQLSeedScript(seed)

		if !strings.Contains(script, `--ignore-table="$SUPERSET_OPERATOR__SEED_SRC_DB"."logs"`) {
			t.Errorf("expected --ignore-table for logs, got: %s", script)
		}
		if !strings.Contains(script, `--ignore-table="$SUPERSET_OPERATOR__SEED_SRC_DB"."tab_state"`) {
			t.Errorf("expected --ignore-table for tab_state, got: %s", script)
		}
	})

	t.Run("mysql exclude table data", func(t *testing.T) {
		mysqlType := "MySQL"
		seed := &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Type:     &mysqlType,
				Host:     "mysql-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
			ExcludeTables:    []string{"tab_state"},
			ExcludeTableData: []string{"logs", "query"},
		}

		script := buildMySQLSeedScript(seed)

		// Schema-only pass for ExcludeTableData tables.
		if !strings.Contains(script, `--no-data`) {
			t.Errorf("expected --no-data pass for ExcludeTableData, got: %s", script)
		}
		if strings.Contains(script, "--skip-triggers") {
			t.Errorf("schema-only pass must preserve triggers (mirrors Postgres --exclude-table-data which keeps schema objects), got: %s", script)
		}
		if !strings.Contains(script, `"$SUPERSET_OPERATOR__SEED_SRC_DB" "logs" "query"`) {
			t.Errorf("expected schema-only dump to list logs and query tables, got: %s", script)
		}

		// Data pass should --ignore-table both ExcludeTables and ExcludeTableData.
		for _, table := range []string{"tab_state", "logs", "query"} {
			needle := `--ignore-table="$SUPERSET_OPERATOR__SEED_SRC_DB".` + fmt.Sprintf("%q", table)
			if !strings.Contains(script, needle) {
				t.Errorf("expected --ignore-table for %q in data pass, got: %s", table, script)
			}
		}

		// Combined output piped to mysql.
		if !strings.Contains(script, `) | mysql `) {
			t.Errorf("expected grouped passes piped into mysql, got: %s", script)
		}
	})

	t.Run("mysql no exclude table data keeps single pass", func(t *testing.T) {
		mysqlType := "MySQL"
		seed := &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Type:     &mysqlType,
				Host:     "mysql-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
		}

		script := buildMySQLSeedScript(seed)

		if strings.Contains(script, "--no-data") {
			t.Errorf("did not expect --no-data pass when ExcludeTableData is empty, got: %s", script)
		}
	})

	t.Run("postSeedSQL", func(t *testing.T) {
		t.Run("postgres", func(t *testing.T) {
			seed := &supersetv1alpha1.SeedTaskSpec{
				Source: supersetv1alpha1.SeedSourceSpec{
					Host: "pg-prod.svc", Database: "superset_prod", Username: "reader",
				},
				PostSeedSQL: []string{
					"UPDATE report_schedule SET active = false",
					"DELETE FROM oauth2_token",
				},
			}

			script := buildPostgresSeedScript(seed)

			if !strings.Contains(script, `psql`) {
				t.Fatal("expected psql in script")
			}
			if !strings.Contains(script, `-c "UPDATE report_schedule SET active = false"`) {
				t.Errorf("expected first postSeedSQL statement, got: %s", script)
			}
			if !strings.Contains(script, `-c "DELETE FROM oauth2_token"`) {
				t.Errorf("expected second postSeedSQL statement, got: %s", script)
			}
		})

		t.Run("mysql", func(t *testing.T) {
			mysqlType := "MySQL"
			seed := &supersetv1alpha1.SeedTaskSpec{
				Source: supersetv1alpha1.SeedSourceSpec{
					Type: &mysqlType, Host: "mysql-prod.svc", Database: "superset_prod", Username: "reader",
				},
				PostSeedSQL: []string{"UPDATE report_schedule SET active = 0"},
			}

			script := buildMySQLSeedScript(seed)

			if !strings.Contains(script, `-e "UPDATE report_schedule SET active = 0"`) {
				t.Errorf("expected postSeedSQL statement in mysql script, got: %s", script)
			}
		})
	})
}

// TestBuildSeedCommand covers buildSeedCommand: honoring a user command override
// and constructing the default /bin/sh dump pipeline for PostgreSQL and MySQL sources.
func TestBuildSeedCommand(t *testing.T) {
	t.Run("custom command", func(t *testing.T) {
		r := &SupersetReconciler{}
		superset := &supersetv1alpha1.Superset{}
		superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
			Seed: &supersetv1alpha1.SeedTaskSpec{
				SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
					BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{
						Command: []string{"/bin/sh", "-c", "custom-seed-script.sh"},
					},
				},
				Source: supersetv1alpha1.SeedSourceSpec{
					Host:     "pg-prod.svc",
					Database: "superset_prod",
					Username: "reader",
				},
			},
		}

		cmd := r.buildSeedCommand(superset)

		if len(cmd) != 3 || cmd[2] != "custom-seed-script.sh" {
			t.Errorf("expected custom command, got: %v", cmd)
		}
	})

	t.Run("default postgres", func(t *testing.T) {
		r := &SupersetReconciler{}
		superset := &supersetv1alpha1.Superset{}
		superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
			Seed: &supersetv1alpha1.SeedTaskSpec{
				Source: supersetv1alpha1.SeedSourceSpec{
					Host:     "pg-prod.svc",
					Database: "superset_prod",
					Username: "reader",
				},
			},
		}

		cmd := r.buildSeedCommand(superset)

		if len(cmd) != 3 || cmd[0] != "/bin/sh" || cmd[1] != "-c" {
			t.Fatalf("expected shell command, got: %v", cmd)
		}
		if !strings.Contains(cmd[2], "pg_dump") {
			t.Errorf("expected pg_dump in command, got: %s", cmd[2])
		}
	})

	t.Run("mysql", func(t *testing.T) {
		r := &SupersetReconciler{}
		mysqlType := "MySQL"
		superset := &supersetv1alpha1.Superset{}
		superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
			Seed: &supersetv1alpha1.SeedTaskSpec{
				Source: supersetv1alpha1.SeedSourceSpec{
					Type:     &mysqlType,
					Host:     "mysql-prod.svc",
					Database: "superset_prod",
					Username: "reader",
				},
			},
		}

		cmd := r.buildSeedCommand(superset)

		if len(cmd) != 3 || cmd[0] != "/bin/sh" || cmd[1] != "-c" {
			t.Fatalf("expected shell command, got: %v", cmd)
		}
		if !strings.Contains(cmd[2], "mysqldump") {
			t.Errorf("expected mysqldump in command, got: %s", cmd[2])
		}
	})
}

func TestBuildSeedTaskFlatSpec_CommandOnContainer(t *testing.T) {
	r := &SupersetReconciler{}
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
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

	flatSpec := r.buildSeedTaskFlatSpec(superset, "default", &resolution.SharedInput{})
	podSpec := buildInitPod(&flatSpec)

	if len(podSpec.Containers) == 0 {
		t.Fatal("expected at least one container")
	}
	cmd := podSpec.Containers[0].Command
	if len(cmd) == 0 {
		t.Fatal("expected command on seed pod container, got nil")
	}
	if cmd[0] != "/bin/sh" || !strings.Contains(cmd[2], "pg_dump") {
		t.Errorf("expected pg_dump shell command, got: %v", cmd)
	}
}

func TestReconcileLifecycleTask_SeedIgnoresBootstrapScript(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	bootstrapScript := "echo bootstrap"
	password := "secret"
	host := "postgres.default.svc"
	database := "superset"
	username := "superset"

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			BootstrapScript: &bootstrapScript,
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Seed: &supersetv1alpha1.SeedTaskSpec{
					Source: supersetv1alpha1.SeedSourceSpec{
						Host:     "pg-prod.svc",
						Database: "superset_prod",
						Username: "reader",
						Password: &password,
					},
				},
			},
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Host:     &host,
				Database: &database,
				Username: &username,
				Password: &password,
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	if _, err := r.reconcileLifecycleTask(
		ctx,
		superset,
		taskTypeSeed,
		suffixSeed,
		nil,
		"sha256:seed",
		"sha256:config",
		&resolution.SharedInput{},
		"default",
	); err != nil {
		t.Fatalf("reconcileLifecycleTask: %v", err)
	}

	cm := &corev1.ConfigMap{}
	err := c.Get(ctx, types.NamespacedName{Name: common.ConfigMapName("test-seed"), Namespace: "default"}, cm)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected seed task to skip bootstrap ConfigMap, got %v", err)
	}
}

func TestCollectSeedEnvVars(t *testing.T) {
	pw := "secret123"
	host := "pg-staging.svc"
	db := "superset_staging"
	user := "admin"
	port := int32(5432)

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
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

	envs := collectSeedEnvVars(superset)

	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.Name] = e.Value
	}

	if envMap[common.EnvSeedSrcHost] != "pg-prod.svc" {
		t.Errorf("expected source host pg-prod.svc, got: %s", envMap[common.EnvSeedSrcHost])
	}
	if envMap[common.EnvSeedSrcPort] != "5433" {
		t.Errorf("expected source port 5433, got: %s", envMap[common.EnvSeedSrcPort])
	}
	if envMap[common.EnvSeedSrcDB] != "superset_prod" {
		t.Errorf("expected source db superset_prod, got: %s", envMap[common.EnvSeedSrcDB])
	}
	if envMap[common.EnvSeedSrcUser] != "reader" {
		t.Errorf("expected source user reader, got: %s", envMap[common.EnvSeedSrcUser])
	}
	if envMap[common.EnvSeedSrcPass] != "secret123" {
		t.Errorf("expected source pass secret123, got: %s", envMap[common.EnvSeedSrcPass])
	}
	if envMap[common.EnvDBHost] != "pg-staging.svc" {
		t.Errorf("expected target host pg-staging.svc, got: %s", envMap[common.EnvDBHost])
	}
	if envMap[common.EnvDBName] != "superset_staging" {
		t.Errorf("expected target db superset_staging, got: %s", envMap[common.EnvDBName])
	}
}

func TestCollectSeedEnvVars_SecretRef(t *testing.T) {
	host := "pg-staging.svc"
	db := "superset_staging"
	user := "admin"

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
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

	envs := collectSeedEnvVars(superset)

	for _, e := range envs {
		if e.Name == common.EnvSeedSrcPass {
			if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
				t.Fatal("expected SecretKeyRef for source password")
			}
			if e.ValueFrom.SecretKeyRef.Name != "prod-creds" {
				t.Errorf("expected secret name prod-creds, got: %s", e.ValueFrom.SecretKeyRef.Name)
			}
			return
		}
	}
	t.Error("SUPERSET_OPERATOR__SEED_SRC_PASS not found in env vars")
}

// TestResolveSeedImage covers resolveSeedImage: type-appropriate defaults
// (postgres/mysql), full custom overrides, and partial overrides that inherit the
// database tooling repository or tag rather than the Superset image.
func TestResolveSeedImage(t *testing.T) {
	t.Run("default postgres", func(t *testing.T) {
		seed := &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Host:     "pg-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
		}

		img := resolveSeedImage(seed)

		if img.Repository != "postgres" || img.Tag != "17-alpine" {
			t.Errorf("expected postgres:17-alpine, got: %s:%s", img.Repository, img.Tag)
		}
	})

	t.Run("default mysql", func(t *testing.T) {
		mysqlType := "MySQL"
		seed := &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Type:     &mysqlType,
				Host:     "mysql-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
		}

		img := resolveSeedImage(seed)

		if img.Repository != "mysql" || img.Tag != "8-alpine" {
			t.Errorf("expected mysql:8-alpine, got: %s:%s", img.Repository, img.Tag)
		}
	})

	t.Run("custom override", func(t *testing.T) {
		seed := &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Host:     "pg-prod.svc",
				Database: "superset_prod",
				Username: "reader",
			},
			Image: &supersetv1alpha1.ContainerImageSpec{
				Repository: "my-registry/custom-tools",
				Tag:        "v2",
			},
		}

		img := resolveSeedImage(seed)

		if img.Repository != "my-registry/custom-tools" || img.Tag != "v2" {
			t.Errorf("expected my-registry/custom-tools:v2, got: %s:%s", img.Repository, img.Tag)
		}
	})

	// partial override: an image spec with only a tag (or only a repository) set
	// inherits the type-appropriate database tooling repository/tag, not the
	// Superset image repository — the bug ContainerImageSpec was introduced to prevent.
	t.Run("partial override", func(t *testing.T) {
		tests := []struct {
			name         string
			srcType      string
			image        *supersetv1alpha1.ContainerImageSpec
			expectedRepo string
			expectedTag  string
		}{
			{
				name:         "tag-only override on PostgreSQL inherits postgres repo",
				srcType:      dbTypePostgresql,
				image:        &supersetv1alpha1.ContainerImageSpec{Tag: "16-alpine"},
				expectedRepo: "postgres",
				expectedTag:  "16-alpine",
			},
			{
				name:         "tag-only override on MySQL inherits mysql repo",
				srcType:      dbTypeMySQL,
				image:        &supersetv1alpha1.ContainerImageSpec{Tag: "9-alpine"},
				expectedRepo: "mysql",
				expectedTag:  "9-alpine",
			},
			{
				name:         "repository-only override on PostgreSQL inherits default tag",
				srcType:      dbTypePostgresql,
				image:        &supersetv1alpha1.ContainerImageSpec{Repository: "my-registry/postgres"},
				expectedRepo: "my-registry/postgres",
				expectedTag:  "17-alpine",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				srcType := tt.srcType
				seed := &supersetv1alpha1.SeedTaskSpec{
					Source: supersetv1alpha1.SeedSourceSpec{
						Host:     "src.svc",
						Database: "src",
						Username: "reader",
						Type:     &srcType,
					},
					Image: tt.image,
				}
				img := resolveSeedImage(seed)
				if img.Repository != tt.expectedRepo || img.Tag != tt.expectedTag {
					t.Errorf("expected %s:%s, got %s:%s", tt.expectedRepo, tt.expectedTag, img.Repository, img.Tag)
				}
			})
		}
	})
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
			name:     "nil lifecycle: seed disabled",
			spec:     nil,
			taskType: taskTypeSeed,
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
			name: "seed present and not disabled",
			spec: &supersetv1alpha1.LifecycleSpec{
				Seed: &supersetv1alpha1.SeedTaskSpec{
					Source: supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"},
				},
			},
			taskType: taskTypeSeed,
			expected: true,
		},
		{
			name: "seed explicitly disabled",
			spec: &supersetv1alpha1.LifecycleSpec{
				Seed: &supersetv1alpha1.SeedTaskSpec{
					SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: common.Ptr(true)}},
					Source:                  supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"},
				},
			},
			taskType: taskTypeSeed,
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
		Seed: &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"},
		},
	}

	tests := []struct {
		taskType string
		want     bool
	}{
		{taskTypeSeed, true},
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

	cmd := []string{"/bin/sh", "-c", "seed"}

	step1 := r.computeStepChecksum("seed", taskTypeSeed, cmd, struct{ Trigger string }{"v1"})
	step2 := r.computeStepChecksum("seed", taskTypeSeed, cmd, struct{ Trigger string }{"v2"})

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

	// Simulate a full pipeline with trigger change on seed.
	seedCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}
	initCmd := []string{"/bin/sh", "-c", "superset init"}

	parentUID := "test-uid"

	// Run 1: trigger=v1
	seedChecksum1 := r.computeStepChecksum(parentUID, taskTypeSeed, seedCmd, struct{ Trigger string }{"v1"})
	migrateChecksum1 := r.computeStepChecksum(seedChecksum1, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	initChecksum1 := r.computeStepChecksum(migrateChecksum1, taskTypeInit, initCmd, struct{ Config string }{"cfg-v1"})

	// Run 2: trigger=v2 (seed re-runs, propagates downstream)
	seedChecksum2 := r.computeStepChecksum(parentUID, taskTypeSeed, seedCmd, struct{ Trigger string }{"v2"})
	migrateChecksum2 := r.computeStepChecksum(seedChecksum2, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	initChecksum2 := r.computeStepChecksum(migrateChecksum2, taskTypeInit, initCmd, struct{ Config string }{"cfg-v1"})

	if seedChecksum1 == seedChecksum2 {
		t.Error("seed checksum should change when trigger changes")
	}
	if migrateChecksum1 == migrateChecksum2 {
		t.Error("migrate checksum should change when seed's checksum changes (upstream propagation)")
	}
	if initChecksum1 == initChecksum2 {
		t.Error("init checksum should change when migrate's checksum changes (chain propagation)")
	}
}

// TestPipelineChain_DownstreamInputChangesDoNotRippleUpstream verifies that
// the cascade only flows in the pipeline direction: changing a migrate-only
// hashed input does not affect seed's checksum. Production seed inputs
// include the target Superset image (see seedInputs); this test exercises
// the cascade math with synthetic inputs that intentionally omit it.
func TestPipelineChain_DownstreamInputChangesDoNotRippleUpstream(t *testing.T) {
	r := &SupersetReconciler{}

	seedCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}

	parentUID := "test-uid"

	// Synthetic seed inputs (Trigger only) — in production seedInputs also
	// includes target image, but here we want to isolate cascade direction.
	seedChecksum := r.computeStepChecksum(parentUID, taskTypeSeed, seedCmd, struct{ Trigger string }{"v1"})

	// Migrate with different image versions.
	migrate1 := r.computeStepChecksum(seedChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	migrate2 := r.computeStepChecksum(seedChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:5.0"})

	if migrate1 == migrate2 {
		t.Error("migrate checksum should change when its hashed Image input changes")
	}

	// Re-computing seed with identical synthetic inputs yields the same
	// checksum: cascade does not ripple upstream from migrate.
	seed2 := r.computeStepChecksum(parentUID, taskTypeSeed, seedCmd, struct{ Trigger string }{"v1"})
	if seedChecksum != seed2 {
		t.Error("seed checksum should not change when only migrate's hashed input changed (cascade is downstream-only)")
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

	seedCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}
	initCmd := []string{"/bin/sh", "-c", "superset init"}

	parentUID := "test-uid"
	seedChecksum := r.computeStepChecksum(parentUID, taskTypeSeed, seedCmd, struct{ Trigger string }{"v1"})
	migrateChecksum := r.computeStepChecksum(seedChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	initChecksum := r.computeStepChecksum(migrateChecksum, taskTypeInit, initCmd, struct{ Config string }{"cfg-v1"})

	// Re-compute with identical inputs.
	seedChecksum2 := r.computeStepChecksum(parentUID, taskTypeSeed, seedCmd, struct{ Trigger string }{"v1"})
	migrateChecksum2 := r.computeStepChecksum(seedChecksum2, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})
	initChecksum2 := r.computeStepChecksum(migrateChecksum2, taskTypeInit, initCmd, struct{ Config string }{"cfg-v1"})

	if seedChecksum != seedChecksum2 || migrateChecksum != migrateChecksum2 || initChecksum != initChecksum2 {
		t.Error("pipeline should produce identical checksums when all inputs are unchanged")
	}
}

// TestPipelineChain_CustomTaskSlotsBetweenStages validates that custom tasks
// can be inserted into the pipeline using the same checksum model.
func TestPipelineChain_CustomTaskSlotsBetweenStages(t *testing.T) {
	r := &SupersetReconciler{}

	parentUID := "test-uid"
	seedCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	customCmd := []string{"/bin/sh", "-c", "run-data-masking.sh"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}

	// Pipeline: seed → custom("PostSeed") → migrate
	seedChecksum := r.computeStepChecksum(parentUID, taskTypeSeed, seedCmd, struct{ Trigger string }{"v1"})
	customChecksum := r.computeStepChecksum(seedChecksum, "PostSeed", customCmd, struct{ Script string }{"mask-pii-v3"})
	migrateChecksum := r.computeStepChecksum(customChecksum, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})

	// Custom task changes its script input → propagates to migrate.
	customChecksum2 := r.computeStepChecksum(seedChecksum, "PostSeed", customCmd, struct{ Script string }{"mask-pii-v4"})
	migrateChecksum2 := r.computeStepChecksum(customChecksum2, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})

	if customChecksum == customChecksum2 {
		t.Error("custom task checksum should change when its inputs change")
	}
	if migrateChecksum == migrateChecksum2 {
		t.Error("migrate should re-run when custom task upstream changes")
	}

	// Seed unchanged → custom unchanged → migrate unchanged.
	customChecksum3 := r.computeStepChecksum(seedChecksum, "PostSeed", customCmd, struct{ Script string }{"mask-pii-v3"})
	migrateChecksum3 := r.computeStepChecksum(customChecksum3, taskTypeMigrate, migrateCmd, struct{ Image string }{"img:4.0"})

	if migrateChecksum != migrateChecksum3 {
		t.Error("migrate should be stable when nothing upstream changed")
	}
}

func TestSeedInputs_ScheduleTickChangesChecksum(t *testing.T) {
	// When the clock crosses a cron boundary, the seed checksum should change.
	before := time.Date(2026, 5, 11, 1, 59, 0, 0, time.UTC)
	after := time.Date(2026, 5, 11, 2, 1, 0, 0, time.UTC)

	cronExpr := "0 2 * * *"
	source := supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"}

	r1 := &SupersetReconciler{Now: func() time.Time { return before }}
	r2 := &SupersetReconciler{Now: func() time.Time { return after }}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &cronExpr},
			Source:                  source,
		},
	}

	inputs1 := r1.seedInputs(superset)
	inputs2 := r2.seedInputs(superset)

	checksum1 := r1.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, inputs1)
	checksum2 := r2.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, inputs2)

	if checksum1 == checksum2 {
		t.Error("seed checksum should change when crossing a cron boundary")
	}
}

func TestSeedInputs_ScheduleAndTrigger_BothContribute(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 0, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	cronExpr := "0 * * * *"
	source := supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"}
	trigger1 := "v1"
	trigger2 := "v2"

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Trigger: &trigger1},
				CronSchedule: &cronExpr,
			},
			Source: source,
		},
	}

	inputs1 := r.seedInputs(superset)
	checksum1 := r.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, inputs1)

	// Change trigger only.
	superset.Spec.Lifecycle.Seed.Trigger = &trigger2
	inputs2 := r.seedInputs(superset)
	checksum2 := r.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, inputs2)

	if checksum1 == checksum2 {
		t.Error("changing trigger should change checksum even with same schedule tick")
	}
}

func TestSeedInputs_TargetImageChangesChecksum(t *testing.T) {
	r := &SupersetReconciler{}
	source := supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Image = supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.1.0rc3-dev"}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			Source: source,
		},
	}

	checksum1 := r.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, r.seedInputs(superset))

	superset.Spec.Image.Tag = "6.1.0-dev"
	checksum2 := r.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, r.seedInputs(superset))

	if checksum1 == checksum2 {
		t.Error("seed checksum should change when the target Superset image changes")
	}
}

func TestSeedInputs_NoSchedule_StableChecksum(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 0, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	source := supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"}
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			Source: source,
		},
	}

	inputs1 := r.seedInputs(superset)
	inputs2 := r.seedInputs(superset)

	checksum1 := r.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, inputs1)
	checksum2 := r.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, inputs2)

	if checksum1 != checksum2 {
		t.Error("checksum should be stable when no schedule is set")
	}
}

func TestSeedInputs_ScheduleStableWithinWindow(t *testing.T) {
	// Two reconciles within the same cron window produce the same checksum.
	t1 := time.Date(2026, 5, 11, 14, 10, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 11, 14, 50, 0, 0, time.UTC)

	cronExpr := "0 * * * *"
	source := supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &cronExpr},
			Source:                  source,
		},
	}

	r1 := &SupersetReconciler{Now: func() time.Time { return t1 }}
	r2 := &SupersetReconciler{Now: func() time.Time { return t2 }}

	checksum1 := r1.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, r1.seedInputs(superset))
	checksum2 := r2.computeStepChecksum("uid", taskTypeSeed, []string{"cmd"}, r2.seedInputs(superset))

	if checksum1 != checksum2 {
		t.Error("checksum should be stable within the same cron window")
	}
}

func TestPipelineChain_ScheduleTickPropagatesDownstream(t *testing.T) {
	before := time.Date(2026, 5, 11, 1, 59, 0, 0, time.UTC)
	after := time.Date(2026, 5, 11, 2, 1, 0, 0, time.UTC)

	cronExpr := "0 2 * * *"
	source := supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"}

	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &cronExpr},
			Source:                  source,
		},
	}

	seedCmd := []string{"/bin/sh", "-c", "pg_dump | psql"}
	migrateCmd := []string{"/bin/sh", "-c", "superset db upgrade"}
	initCmd := []string{"/bin/sh", "-c", "superset init"}
	parentUID := "test-uid"

	// Before boundary.
	r1 := &SupersetReconciler{Now: func() time.Time { return before }}
	seedChecksum1 := r1.computeStepChecksum(parentUID, taskTypeSeed, seedCmd, r1.seedInputs(superset))
	migrateChecksum1 := r1.computeStepChecksum(seedChecksum1, taskTypeMigrate, migrateCmd, struct {
		Image   string
		Trigger string
	}{"img:4.0", ""})
	initChecksum1 := r1.computeStepChecksum(migrateChecksum1, taskTypeInit, initCmd, struct {
		ConfigChecksum string
		Trigger        string
	}{"cfg", ""})

	// After boundary.
	r2 := &SupersetReconciler{Now: func() time.Time { return after }}
	seedChecksum2 := r2.computeStepChecksum(parentUID, taskTypeSeed, seedCmd, r2.seedInputs(superset))
	migrateChecksum2 := r2.computeStepChecksum(seedChecksum2, taskTypeMigrate, migrateCmd, struct {
		Image   string
		Trigger string
	}{"img:4.0", ""})
	initChecksum2 := r2.computeStepChecksum(migrateChecksum2, taskTypeInit, initCmd, struct {
		ConfigChecksum string
		Trigger        string
	}{"cfg", ""})

	if seedChecksum1 == seedChecksum2 {
		t.Error("seed checksum should change after boundary")
	}
	if migrateChecksum1 == migrateChecksum2 {
		t.Error("migrate should cascade from seed schedule tick change")
	}
	if initChecksum1 == initChecksum2 {
		t.Error("init should cascade from seed schedule tick change")
	}
}

func TestScheduleRequeue_ComputesCorrectDuration(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	cronExpr := "0 * * * *" // hourly at :00
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{CronSchedule: &cronExpr},
			Source:                  supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"},
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
		Seed: &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"},
		},
	}

	requeue := r.nextScheduleRequeue(superset)
	if requeue != 0 {
		t.Errorf("expected 0 requeue with no schedule, got %v", requeue)
	}
}

func TestScheduleRequeue_DisabledSeed(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	cronExpr := "0 * * * *"
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
				BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Disabled: common.Ptr(true)},
				CronSchedule: &cronExpr,
			},
			Source: supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"},
		},
	}

	requeue := r.nextScheduleRequeue(superset)
	if requeue != 0 {
		t.Errorf("expected 0 requeue with disabled seed, got %v", requeue)
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
	initChecksum := r.computeStepChecksum(migrateChecksum, taskTypeInit, initCmd, r.initInputs(superset))

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
		modified := superset.DeepCopy()
		modified.Spec.FeatureFlags = map[string]bool{"ALERT_REPORTS": true}
		if r.allTasksStillComplete(modified, configChecksum) {
			t.Error("expected allTasksStillComplete=false when featureFlags change the rendered config")
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

func TestAllTasksStillComplete_WithSeedSchedule(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	r := &SupersetReconciler{Now: func() time.Time { return now }}

	cronExpr := "0 * * * *"
	superset := &supersetv1alpha1.Superset{}
	superset.UID = "test-uid"
	superset.Spec.Image = supersetv1alpha1.ImageSpec{Tag: "4.1.4"}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
				CronSchedule: &cronExpr,
			},
			Source: supersetv1alpha1.SeedSourceSpec{Host: "prod-db", Database: "superset", Username: "reader"},
		},
		Migrate: &supersetv1alpha1.MigrateTaskSpec{},
		Init:    &supersetv1alpha1.InitTaskSpec{},
	}

	configChecksum := "config-abc"

	// Compute and store checksums as if lifecycle already completed at :30.
	incomingChecksum := string(superset.UID)
	seedCmd := r.buildSeedCommand(superset)
	seedChecksum := r.computeStepChecksum(incomingChecksum, taskTypeSeed, seedCmd, r.seedInputs(superset))
	migrateCmd := defaultMigrateCommand(superset)
	migrateChecksum := r.computeStepChecksum(seedChecksum, taskTypeMigrate, migrateCmd, r.migrateInputs(superset))
	initCmd := defaultInitCommand(superset)
	initChecksum := r.computeStepChecksum(migrateChecksum, taskTypeInit, initCmd, r.initInputs(superset))

	superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{
		LastCompletedChecksums: map[string]string{
			taskTypeSeed:    seedChecksum,
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
		envs := collectSecretEnvVars(spec, "test")
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
		envs := collectSecretEnvVars(spec, "test")
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
		envs := collectSecretEnvVars(spec, "test")
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

func TestDefaultLifecycleCommandsSourceBootstrap(t *testing.T) {
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.BootstrapScript = common.Ptr("echo bootstrap")

	for name, cmd := range map[string][]string{
		"migrate": defaultMigrateCommand(superset),
		"rotate":  defaultRotateCommand(superset),
		"init":    defaultInitCommand(superset),
	} {
		if len(cmd) < 3 || cmd[0] != "/bin/sh" || cmd[1] != "-c" || !strings.Contains(cmd[2], bootstrapScriptKey) {
			t.Errorf("%s command should source bootstrap script, got %v", name, cmd)
		}
	}
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
	initChecksum := r.computeStepChecksum(rotateChecksum, taskTypeInit, initCmd, r.initInputs(superset))

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

// TestIsTaskEnabled_InvalidCronScheduleGatesSeed verifies that an invalid
// cron expression causes seed to be treated as disabled — the user's
// malformed schedule surfaces as a ScheduleValid=False condition (set by
// validateSchedules) and does not trigger an opportunistic one-shot seed.
func TestIsTaskEnabled_InvalidCronScheduleGatesSeed(t *testing.T) {
	r := &SupersetReconciler{}

	badExpr := "not a cron expression"
	superset := &supersetv1alpha1.Superset{}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			SchedulableBaseTaskSpec: supersetv1alpha1.SchedulableBaseTaskSpec{
				CronSchedule: &badExpr,
			},
			Source: supersetv1alpha1.SeedSourceSpec{Host: "h", Database: "d", Username: "u"},
		},
	}

	if r.isTaskEnabled(superset, taskTypeSeed) {
		t.Fatal("expected seed to be disabled when CronSchedule is invalid")
	}

	// A valid schedule re-enables seed.
	validExpr := "0 * * * *"
	superset.Spec.Lifecycle.Seed.CronSchedule = &validExpr
	if !r.isTaskEnabled(superset, taskTypeSeed) {
		t.Fatal("expected seed to be enabled for a valid CronSchedule")
	}

	// No schedule at all — seed remains enabled (legacy on-demand path).
	superset.Spec.Lifecycle.Seed.CronSchedule = nil
	if !r.isTaskEnabled(superset, taskTypeSeed) {
		t.Fatal("expected seed to be enabled when no CronSchedule is set")
	}
}
