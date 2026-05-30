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
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

func TestBuildCreateDatabaseInitContainer_DisabledByDefault(t *testing.T) {
	cases := map[string]*supersetv1alpha1.Superset{
		"nil metastore":       {},
		"flag unset":          {Spec: supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{Host: common.Ptr("pg")}}},
		"flag explicit false": {Spec: supersetv1alpha1.SupersetSpec{Metastore: &supersetv1alpha1.MetastoreSpec{Host: common.Ptr("pg"), CreateDatabase: common.Ptr(false)}}},
	}
	for name, ss := range cases {
		t.Run(name, func(t *testing.T) {
			if got := buildCreateDatabaseInitContainer(ss, nil); got != nil {
				t.Errorf("expected nil init container, got %+v", got)
			}
		})
	}
}

func TestBuildCreateDatabaseInitContainer_Postgres(t *testing.T) {
	pw := "p@$$"
	superset := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Host:           common.Ptr("pg.svc"),
				Database:       common.Ptr("superset"),
				Username:       common.Ptr("superset"),
				Password:       &pw,
				CreateDatabase: common.Ptr(true),
			},
		},
	}

	ctr := buildCreateDatabaseInitContainer(superset, nil)
	if ctr == nil {
		t.Fatal("expected init container, got nil")
	}
	if ctr.Name != createDatabaseContainerName {
		t.Errorf("expected name %q, got %q", createDatabaseContainerName, ctr.Name)
	}
	if want := common.CloneImagePostgres; ctr.Image != want {
		t.Errorf("expected image %q, got %q", want, ctr.Image)
	}
	if len(ctr.Command) != 3 || ctr.Command[0] != "/bin/sh" || ctr.Command[1] != "-c" {
		t.Fatalf("expected /bin/sh -c <script>, got %v", ctr.Command)
	}
	script := ctr.Command[2]
	for _, want := range []string{
		"createdb",
		"pg_database",
		`sed "s/'/''/g"`,
		`datname = '$ESC_NAME'`,
		"-tA -c",
		`-- "$SUPERSET_OPERATOR__DB_NAME"`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q\n--- script ---\n%s", want, script)
		}
	}
	if strings.Contains(script, ":'name'") {
		t.Errorf("script must not use psql :'name' interpolation; psql does not process client-side features when invoked with -c\n--- script ---\n%s", script)
	}
	if strings.Index(script, "-v ON_ERROR_STOP=1") > strings.Index(script, "-tA -c") {
		t.Errorf("psql -v options must appear before -c so they are parsed as options\n--- script ---\n%s", script)
	}
	envMap := envSliceToMap(ctr.Env)
	if envMap[common.EnvDBHost] != "pg.svc" {
		t.Errorf("expected DB_HOST=pg.svc, got %q", envMap[common.EnvDBHost])
	}
	if envMap[common.EnvDBPort] != "5432" {
		t.Errorf("expected default DB_PORT=5432, got %q", envMap[common.EnvDBPort])
	}
	if envMap[common.EnvDBName] != "superset" {
		t.Errorf("expected DB_NAME=superset, got %q", envMap[common.EnvDBName])
	}
	if envMap[common.EnvDBUser] != "superset" {
		t.Errorf("expected DB_USER=superset, got %q", envMap[common.EnvDBUser])
	}
	if envMap[common.EnvDBPass] != pw {
		t.Errorf("expected DB_PASS=%q, got %q", pw, envMap[common.EnvDBPass])
	}
}

func TestBuildCreateDatabaseInitContainer_MySQL(t *testing.T) {
	mysqlType := "MySQL"
	superset := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Type:     &mysqlType,
				Host:     common.Ptr("mysql.svc"),
				Database: common.Ptr("superset"),
				Username: common.Ptr("superset"),
				PasswordFrom: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "metastore-secret"},
					Key:                  "password",
				},
				CreateDatabase: common.Ptr(true),
			},
		},
	}

	ctr := buildCreateDatabaseInitContainer(superset, nil)
	if ctr == nil {
		t.Fatal("expected init container, got nil")
	}
	if want := common.CloneImageMySQL; ctr.Image != want {
		t.Errorf("expected image %q, got %q", want, ctr.Image)
	}
	script := ctr.Command[2]
	for _, want := range []string{
		"CREATE DATABASE IF NOT EXISTS",
		"sed 's/`/``/g'",
		`mysql -h "$SUPERSET_OPERATOR__DB_HOST"`,
		`export MYSQL_PWD="$SUPERSET_OPERATOR__DB_PASS"`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q\n--- script ---\n%s", want, script)
		}
	}
	envMap := envSliceToMap(ctr.Env)
	if envMap[common.EnvDBPort] != "3306" {
		t.Errorf("expected default mysql DB_PORT=3306, got %q", envMap[common.EnvDBPort])
	}
	// PasswordFrom must produce a ValueFrom-backed env var, not a Value.
	for _, e := range ctr.Env {
		if e.Name != common.EnvDBPass {
			continue
		}
		if e.Value != "" {
			t.Errorf("expected PasswordFrom to use ValueFrom, got plaintext Value=%q", e.Value)
		}
		if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil || e.ValueFrom.SecretKeyRef.Name != "metastore-secret" {
			t.Errorf("expected secretKeyRef from metastore-secret, got %+v", e.ValueFrom)
		}
	}
}

func TestBuildCreateDatabaseInitContainer_CustomPort(t *testing.T) {
	port := int32(15432)
	superset := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Host:           common.Ptr("pg.svc"),
				Port:           &port,
				Database:       common.Ptr("superset"),
				Username:       common.Ptr("superset"),
				CreateDatabase: common.Ptr(true),
			},
		},
	}
	ctr := buildCreateDatabaseInitContainer(superset, nil)
	if ctr == nil {
		t.Fatal("expected init container, got nil")
	}
	if got := envSliceToMap(ctr.Env)[common.EnvDBPort]; got != "15432" {
		t.Errorf("expected custom DB_PORT=15432, got %q", got)
	}
}

func TestBuildCreateDatabaseInitContainer_FunkyCredentials(t *testing.T) {
	// Bash variable expansion is single-pass — the contents of an expanded
	// $VAR are not re-parsed. So as long as the script wraps $VAR in "...",
	// arbitrary password/username/db-name characters survive verbatim. This
	// test pins that property at the env-var boundary: the operator must
	// pass the literal value through to the container env without any
	// transformation that would corrupt it.
	funky := `p@$$"w'or` + "`d`"
	weirdName := `it's"weird` + "`db`"
	superset := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Host:           common.Ptr("pg.svc"),
				Database:       &weirdName,
				Username:       common.Ptr(funky),
				Password:       &funky,
				CreateDatabase: common.Ptr(true),
			},
		},
	}

	ctr := buildCreateDatabaseInitContainer(superset, nil)
	if ctr == nil {
		t.Fatal("expected init container, got nil")
	}
	envMap := envSliceToMap(ctr.Env)
	if envMap[common.EnvDBPass] != funky {
		t.Errorf("password mangled: got %q, want %q", envMap[common.EnvDBPass], funky)
	}
	if envMap[common.EnvDBUser] != funky {
		t.Errorf("username mangled: got %q, want %q", envMap[common.EnvDBUser], funky)
	}
	if envMap[common.EnvDBName] != weirdName {
		t.Errorf("database name mangled: got %q, want %q", envMap[common.EnvDBName], weirdName)
	}
}

func TestBuildCreateDatabaseInitContainer_Passwordless(t *testing.T) {
	// Trust/peer auth and IAM-issued credentials are valid metastore configs;
	// the renderer already supports passwordless via os.environ.get(...). The
	// init container scripts must do the same — they reference DB_PASS via
	// ${VAR:-} so set -u doesn't trip when neither password nor passwordFrom
	// is configured.
	cases := map[string]string{
		"postgres": "PGPASSWORD=\"${SUPERSET_OPERATOR__DB_PASS:-}\"",
		"mysql":    `export MYSQL_PWD="$SUPERSET_OPERATOR__DB_PASS"`,
	}
	for kind, wantSnippet := range cases {
		t.Run(kind, func(t *testing.T) {
			meta := &supersetv1alpha1.MetastoreSpec{
				Host:           common.Ptr("db.svc"),
				Database:       common.Ptr("superset"),
				Username:       common.Ptr("superset"),
				CreateDatabase: common.Ptr(true),
			}
			if kind == "mysql" {
				meta.Type = common.Ptr("MySQL")
			}
			ctr := buildCreateDatabaseInitContainer(&supersetv1alpha1.Superset{
				Spec: supersetv1alpha1.SupersetSpec{Metastore: meta},
			}, nil)
			if ctr == nil {
				t.Fatal("expected init container, got nil")
			}
			for _, e := range ctr.Env {
				if e.Name == common.EnvDBPass {
					t.Errorf("expected no DB_PASS env var when password is unset, got %+v", e)
				}
			}
			if !strings.Contains(ctr.Command[2], wantSnippet) {
				t.Errorf("script missing %q\n--- script ---\n%s", wantSnippet, ctr.Command[2])
			}
		})
	}
}

func TestBuildCreateDatabaseInitContainer_DefensiveOnMissingFields(t *testing.T) {
	// CEL should make this unreachable, but if a malformed CR slips through
	// (older CRD versions, direct etcd writes, missing CEL feature gate on
	// the apiserver), the controller must not panic dereferencing nil host/
	// database/username. It returns nil instead, so migrate runs without the
	// init container — the migrate command itself will then fail with a
	// clear connection/identifier error rather than crashing the operator.
	cases := map[string]*supersetv1alpha1.MetastoreSpec{
		"missing host":     {Database: common.Ptr("d"), Username: common.Ptr("u"), CreateDatabase: common.Ptr(true)},
		"missing database": {Host: common.Ptr("h"), Username: common.Ptr("u"), CreateDatabase: common.Ptr(true)},
		"missing username": {Host: common.Ptr("h"), Database: common.Ptr("d"), CreateDatabase: common.Ptr(true)},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			got := buildCreateDatabaseInitContainer(&supersetv1alpha1.Superset{
				Spec: supersetv1alpha1.SupersetSpec{Metastore: m},
			}, nil)
			if got != nil {
				t.Errorf("expected nil (defensive bailout), got container %+v", got)
			}
		})
	}
}

func TestBuildCreateDatabaseInitContainer_InheritsFromMigrateContainerTemplate(t *testing.T) {
	// Strict admission policies (PSS restricted, Kyverno, OPA) require every
	// container — including init containers — to declare resources and
	// securityContext. Rather than adding dedicated knobs, the operator
	// inherits these from the resolved lifecycle container template, which
	// the user already configures via spec.lifecycle.podTemplate.container.
	wantResources := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
	wantSecCtx := &corev1.SecurityContext{
		RunAsNonRoot:             common.Ptr(true),
		AllowPrivilegeEscalation: common.Ptr(false),
		ReadOnlyRootFilesystem:   common.Ptr(true),
		// The user set runAsNonRoot but no UID; the operator defaults the helper
		// to the image's non-root UID so kubelet does not reject the root-default
		// postgres image with CreateContainerConfigError.
		RunAsUser: common.Ptr(int64(70)),
	}
	migratePod := &supersetv1alpha1.PodTemplate{
		Container: &supersetv1alpha1.ContainerTemplate{
			Resources:       &wantResources,
			SecurityContext: wantSecCtx,
		},
	}
	superset := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			Metastore: &supersetv1alpha1.MetastoreSpec{
				Host:           common.Ptr("pg.svc"),
				Database:       common.Ptr("superset"),
				Username:       common.Ptr("superset"),
				CreateDatabase: common.Ptr(true),
			},
		},
	}

	ctr := buildCreateDatabaseInitContainer(superset, migratePod)
	if ctr == nil {
		t.Fatal("expected init container, got nil")
	}
	if !reflect.DeepEqual(ctr.Resources, wantResources) {
		t.Errorf("resources not inherited:\n got: %+v\nwant: %+v", ctr.Resources, wantResources)
	}
	if !reflect.DeepEqual(ctr.SecurityContext, wantSecCtx) {
		t.Errorf("securityContext not inherited:\n got: %+v\nwant: %+v", ctr.SecurityContext, wantSecCtx)
	}
}

func TestBuildStandardTaskFlatSpec_InheritsContainerHardeningOnMigrate(t *testing.T) {
	// End-to-end check: when the user sets spec.lifecycle.podTemplate.container
	// hardening, it propagates through resolution into the create-database
	// init container on the migrate Job.
	superset := &supersetv1alpha1.Superset{}
	superset.Name = "demo"
	superset.Spec.SecretKeyFrom = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "secret"},
		Key:                  "key",
	}
	superset.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:           common.Ptr("pg.svc"),
		Database:       common.Ptr("superset"),
		Username:       common.Ptr("superset"),
		PasswordFrom:   &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "metastore-secret"}, Key: "password"},
		CreateDatabase: common.Ptr(true),
	}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		PodTemplate: &supersetv1alpha1.PodTemplate{
			Container: &supersetv1alpha1.ContainerTemplate{
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot:             common.Ptr(true),
					AllowPrivilegeEscalation: common.Ptr(false),
				},
			},
		},
	}

	r := &SupersetReconciler{}
	flatSpec, _ := r.buildStandardTaskFlatSpec(superset, taskTypeMigrate, []string{"/bin/sh", "-c", "true"}, &resolution.SharedInput{}, "default")
	pod := buildInitPod(&flatSpec)

	var initCtr *corev1.Container
	for i := range pod.InitContainers {
		if pod.InitContainers[i].Name == createDatabaseContainerName {
			initCtr = &pod.InitContainers[i]
			break
		}
	}
	if initCtr == nil {
		t.Fatal("expected create-database init container on migrate Job")
	}
	if initCtr.SecurityContext == nil || initCtr.SecurityContext.RunAsNonRoot == nil || !*initCtr.SecurityContext.RunAsNonRoot {
		t.Errorf("expected RunAsNonRoot=true to propagate to init container, got %+v", initCtr.SecurityContext)
	}
	if initCtr.SecurityContext == nil || initCtr.SecurityContext.AllowPrivilegeEscalation == nil || *initCtr.SecurityContext.AllowPrivilegeEscalation {
		t.Errorf("expected AllowPrivilegeEscalation=false to propagate to init container, got %+v", initCtr.SecurityContext)
	}
}

func TestBuildStandardTaskFlatSpec_DropsUserInitContainerWithReservedName(t *testing.T) {
	// `create-database` is a reserved init container name. If the user
	// happens to define their own init container with that name in
	// spec.lifecycle.podTemplate.initContainers, K8s would otherwise reject
	// the resulting Pod for duplicate container names. The operator drops
	// the user's container so its own version wins deterministically; other
	// user-supplied init containers are preserved.
	superset := &supersetv1alpha1.Superset{}
	superset.Name = "demo"
	superset.Spec.SecretKeyFrom = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "secret"},
		Key:                  "key",
	}
	superset.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:           common.Ptr("pg.svc"),
		Database:       common.Ptr("superset"),
		Username:       common.Ptr("superset"),
		PasswordFrom:   &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "metastore-secret"}, Key: "password"},
		CreateDatabase: common.Ptr(true),
	}
	superset.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		PodTemplate: &supersetv1alpha1.PodTemplate{
			InitContainers: []corev1.Container{
				{Name: createDatabaseContainerName, Image: "user-supplied:latest"},
				{Name: "user-keeper", Image: "keeper:1"},
			},
		},
	}

	r := &SupersetReconciler{}
	flatSpec, _ := r.buildStandardTaskFlatSpec(superset, taskTypeMigrate, []string{"/bin/sh", "-c", "true"}, &resolution.SharedInput{}, "default")
	pod := buildInitPod(&flatSpec)

	count := 0
	var createDB *corev1.Container
	for i := range pod.InitContainers {
		if pod.InitContainers[i].Name == createDatabaseContainerName {
			count++
			createDB = &pod.InitContainers[i]
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 init container named %q, got %d", createDatabaseContainerName, count)
	}
	if createDB.Image != common.CloneImagePostgres && createDB.Image != common.CloneImageMySQL {
		t.Errorf("operator's create-database container should win; got image %q", createDB.Image)
	}
	if !podHasContainer(pod.InitContainers, "user-keeper") {
		t.Error("expected unrelated user-keeper init container to be preserved")
	}
}

func TestBuildStandardTaskFlatSpec_AttachesCreateDBOnlyToMigrate(t *testing.T) {
	superset := &supersetv1alpha1.Superset{}
	superset.Name = "demo"
	superset.Spec.SecretKeyFrom = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "secret"},
		Key:                  "key",
	}
	superset.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:           common.Ptr("pg.svc"),
		Database:       common.Ptr("superset"),
		Username:       common.Ptr("superset"),
		PasswordFrom:   &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "metastore-secret"}, Key: "password"},
		CreateDatabase: common.Ptr(true),
	}

	r := &SupersetReconciler{}
	for _, taskType := range []string{taskTypeMigrate, taskTypeRotate, taskTypeInit} {
		t.Run(taskType, func(t *testing.T) {
			flatSpec, _ := r.buildStandardTaskFlatSpec(superset, taskType, []string{"/bin/sh", "-c", "true"}, &resolution.SharedInput{}, "default")
			pod := buildInitPod(&flatSpec)
			has := podHasContainer(pod.InitContainers, createDatabaseContainerName)
			wantHas := taskType == taskTypeMigrate
			if has != wantHas {
				t.Errorf("taskType %s: hasCreateDBInit=%v, want %v", taskType, has, wantHas)
			}
		})
	}
}

func TestBuildStandardTaskFlatSpec_NoCreateDBInitWhenDisabled(t *testing.T) {
	superset := &supersetv1alpha1.Superset{}
	superset.Name = "demo"
	superset.Spec.SecretKeyFrom = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "secret"},
		Key:                  "key",
	}
	superset.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:         common.Ptr("pg.svc"),
		Database:     common.Ptr("superset"),
		Username:     common.Ptr("superset"),
		PasswordFrom: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "metastore-secret"}, Key: "password"},
	}

	r := &SupersetReconciler{}
	flatSpec, _ := r.buildStandardTaskFlatSpec(superset, taskTypeMigrate, []string{"/bin/sh", "-c", "true"}, &resolution.SharedInput{}, "default")
	pod := buildInitPod(&flatSpec)
	if podHasContainer(pod.InitContainers, createDatabaseContainerName) {
		t.Error("did not expect create-database init container when createDatabase is unset")
	}
}

func TestMigrateInputs_CreateDatabaseAffectsChecksum(t *testing.T) {
	r := &SupersetReconciler{}
	base := &supersetv1alpha1.Superset{}
	base.Spec.Image = supersetv1alpha1.ImageSpec{Repository: "superset", Tag: "1.0"}

	off := *base
	off.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:           common.Ptr("pg"),
		Database:       common.Ptr("superset"),
		Username:       common.Ptr("superset"),
		CreateDatabase: common.Ptr(false),
	}
	on := *base
	on.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:           common.Ptr("pg"),
		Database:       common.Ptr("superset"),
		Username:       common.Ptr("superset"),
		CreateDatabase: common.Ptr(true),
	}

	if r.migrateInputs(&off) == r.migrateInputs(&on) {
		t.Error("expected migrateInputs to differ when createDatabase toggles, but they were equal")
	}
}

func TestMigrateInputs_StructuredTargetAffectsChecksumWhenCreateDatabaseTrue(t *testing.T) {
	// When createDatabase is true, the migrate Job carries an init container
	// that reads host/port/database/username/type. Changing any of those must
	// invalidate the migrate checksum so the init container actually runs
	// against the new target — otherwise the init container would point at
	// the previous server forever.
	r := &SupersetReconciler{}
	mkSuperset := func(mutate func(*supersetv1alpha1.MetastoreSpec)) *supersetv1alpha1.Superset {
		s := &supersetv1alpha1.Superset{}
		s.Spec.Image = supersetv1alpha1.ImageSpec{Repository: "superset", Tag: "1.0"}
		s.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
			Host:           common.Ptr("pg-old"),
			Port:           common.Ptr(int32(5432)),
			Database:       common.Ptr("superset"),
			Username:       common.Ptr("superset"),
			CreateDatabase: common.Ptr(true),
		}
		mutate(s.Spec.Metastore)
		return s
	}

	baseline := r.migrateInputs(mkSuperset(func(m *supersetv1alpha1.MetastoreSpec) {}))

	cases := map[string]func(*supersetv1alpha1.MetastoreSpec){
		"host":     func(m *supersetv1alpha1.MetastoreSpec) { m.Host = common.Ptr("pg-new") },
		"port":     func(m *supersetv1alpha1.MetastoreSpec) { m.Port = common.Ptr(int32(15432)) },
		"database": func(m *supersetv1alpha1.MetastoreSpec) { m.Database = common.Ptr("superset_new") },
		"username": func(m *supersetv1alpha1.MetastoreSpec) { m.Username = common.Ptr("superset_new") },
		"type":     func(m *supersetv1alpha1.MetastoreSpec) { m.Type = common.Ptr("MySQL") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if r.migrateInputs(mkSuperset(mutate)) == baseline {
				t.Errorf("expected migrateInputs to differ when %s changes, but they were equal", name)
			}
		})
	}
}

func TestMigrateInputs_StructuredTargetIgnoredWhenCreateDatabaseFalse(t *testing.T) {
	// Symmetric guarantee: when createDatabase is false, structured-target
	// changes must NOT churn the migrate checksum — re-running migrate on a
	// host change is the user's call (they'd bump trigger). This pins that
	// the new target plumbing is gated on the flag.
	r := &SupersetReconciler{}
	mkSuperset := func(host string) *supersetv1alpha1.Superset {
		s := &supersetv1alpha1.Superset{}
		s.Spec.Image = supersetv1alpha1.ImageSpec{Repository: "superset", Tag: "1.0"}
		s.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
			Host:     common.Ptr(host),
			Database: common.Ptr("superset"),
			Username: common.Ptr("superset"),
		}
		return s
	}
	if r.migrateInputs(mkSuperset("pg-old")) != r.migrateInputs(mkSuperset("pg-new")) {
		t.Error("expected migrateInputs to ignore host changes when createDatabase is unset")
	}
}

func TestMigrateInputs_InitContainerScriptParticipatesInChecksum(t *testing.T) {
	// The create-database init container's script body is rendered by the
	// operator binary, not the user's spec. Including its content in the
	// migrate inputs is what lets a previously failed migrate retry after the
	// operator is upgraded with a fix to the script — otherwise the migrate
	// checksum is stable across operator upgrades and Block A in
	// reconcileLifecycleTask would keep returning terminal even though the
	// rendered Job would now succeed.
	r := &SupersetReconciler{}
	mkSuperset := func(dbType *string) *supersetv1alpha1.Superset {
		s := &supersetv1alpha1.Superset{}
		s.Spec.Image = supersetv1alpha1.ImageSpec{Repository: "superset", Tag: "1.0"}
		s.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
			Host:           common.Ptr("pg.svc"),
			Database:       common.Ptr("superset"),
			Username:       common.Ptr("superset"),
			CreateDatabase: common.Ptr(true),
			Type:           dbType,
		}
		return s
	}
	pgInputs := r.migrateInputs(mkSuperset(nil))
	mysqlInputs := r.migrateInputs(mkSuperset(common.Ptr("MySQL")))

	pgStruct, ok := pgInputs.(struct {
		Image               string
		Trigger             string
		BootstrapScript     string
		CreateDatabase      bool
		Target              any
		InitContainerScript string
	})
	if !ok {
		t.Fatalf("migrateInputs returned unexpected type: %T", pgInputs)
	}
	if pgStruct.InitContainerScript != createDatabasePostgresScript {
		t.Errorf("postgres init script not embedded in migrate inputs:\n--- got ---\n%s", pgStruct.InitContainerScript)
	}

	if pgInputs == mysqlInputs {
		t.Error("expected migrateInputs to differ between Postgres and MySQL init scripts")
	}
}

func podHasContainer(containers []corev1.Container, name string) bool {
	for _, c := range containers {
		if c.Name == name {
			return true
		}
	}
	return false
}

func TestHelperNonRootSecurityContext(t *testing.T) {
	// The DB-tool images (postgres/mysql) run as root by default, so an
	// inherited pod-level runAsNonRoot would make kubelet reject the
	// create-database init container with CreateContainerConfigError. These
	// cases pin that the operator defaults a non-root UID when (and only when)
	// neither the container nor the pod already pins one.
	cases := map[string]struct {
		containerSC *corev1.SecurityContext
		podSC       *corev1.PodSecurityContext
		dbType      string
		wantUID     *int64
		wantNonRoot *bool
	}{
		"nothing pinned -> default postgres uid": {
			dbType:      dbTypePostgresql,
			wantUID:     common.Ptr(int64(70)),
			wantNonRoot: common.Ptr(true),
		},
		"nothing pinned -> default mysql uid": {
			dbType:      dbTypeMySQL,
			wantUID:     common.Ptr(int64(999)),
			wantNonRoot: common.Ptr(true),
		},
		"pod runAsNonRoot but no uid -> default uid, do not override nonRoot": {
			podSC:       &corev1.PodSecurityContext{RunAsNonRoot: common.Ptr(true)},
			dbType:      dbTypePostgresql,
			wantUID:     common.Ptr(int64(70)),
			wantNonRoot: nil, // pod-level runAsNonRoot already applies; container stays unset
		},
		"explicit container uid respected": {
			containerSC: &corev1.SecurityContext{RunAsUser: common.Ptr(int64(1234))},
			dbType:      dbTypePostgresql,
			wantUID:     common.Ptr(int64(1234)),
			wantNonRoot: nil,
		},
		"pod pins uid -> container not defaulted": {
			podSC:       &corev1.PodSecurityContext{RunAsUser: common.Ptr(int64(2000))},
			dbType:      dbTypePostgresql,
			wantUID:     nil,
			wantNonRoot: nil,
		},
		"explicit runAsNonRoot false honored (no uid forced)": {
			containerSC: &corev1.SecurityContext{RunAsNonRoot: common.Ptr(false)},
			dbType:      dbTypePostgresql,
			// no uid pinned anywhere, so the helper still defaults a uid, but
			// must not flip the user's explicit runAsNonRoot:false.
			wantUID:     common.Ptr(int64(70)),
			wantNonRoot: common.Ptr(false),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := helperNonRootSecurityContext(tc.containerSC, tc.podSC, helperNonRootUID(tc.dbType))
			if !int64PtrEqual(got.RunAsUser, tc.wantUID) {
				t.Errorf("RunAsUser = %v, want %v", derefInt64(got.RunAsUser), derefInt64(tc.wantUID))
			}
			if !boolPtrEqual(got.RunAsNonRoot, tc.wantNonRoot) {
				t.Errorf("RunAsNonRoot = %v, want %v", got.RunAsNonRoot, tc.wantNonRoot)
			}
		})
	}
}

func int64PtrEqual(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func boolPtrEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func derefInt64(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}
