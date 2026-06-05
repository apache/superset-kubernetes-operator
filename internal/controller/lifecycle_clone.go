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

// cloneInputs returns the clone-specific inputs that contribute to its step checksum.
func (r *SupersetReconciler) cloneInputs(superset *supersetv1alpha1.Superset) any {
	clone := superset.Spec.Lifecycle.Clone
	targetImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	return struct {
		Image            supersetv1alpha1.ImageSpec
		TargetImage      string
		Trigger          string
		ScheduleTick     string
		Source           supersetv1alpha1.CloneSourceSpec
		ExcludeTables    []string
		ExcludeTableData []string
		PostCloneSQL     []string
	}{
		Image:            resolveCloneImage(clone),
		TargetImage:      targetImage,
		Trigger:          derefOrDefault(clone.Trigger, ""),
		ScheduleTick:     r.scheduleTick(clone.CronSchedule),
		Source:           clone.Source,
		ExcludeTables:    clone.ExcludeTables,
		ExcludeTableData: clone.ExcludeTableData,
		PostCloneSQL:     clone.PostCloneSQL,
	}
}

// buildCloneCommand constructs the pg_dump|psql or mysqldump|mysql streaming command
// from the clone spec. Returns the user's custom command if specified.
func (r *SupersetReconciler) buildCloneCommand(superset *supersetv1alpha1.Superset) []string {
	clone := superset.Spec.Lifecycle.Clone
	if len(clone.Command) > 0 {
		return clone.Command
	}

	srcType := dbTypePostgresql
	if clone.Source.Type != nil {
		srcType = *clone.Source.Type
	}

	if srcType == dbTypeMySQL {
		return []string{"/bin/sh", "-c", buildMySQLCloneScript(clone)}
	}
	return []string{"/bin/sh", "-c", buildPostgresCloneScript(clone)}
}

func buildPostgresCloneScript(clone *supersetv1alpha1.CloneTaskSpec) string {
	var b strings.Builder
	b.WriteString(`set -e
PGPASSWORD="$SUPERSET_OPERATOR__DB_PASS" dropdb --if-exists -h "$SUPERSET_OPERATOR__DB_HOST" -p "$SUPERSET_OPERATOR__DB_PORT" -U "$SUPERSET_OPERATOR__DB_USER" "$SUPERSET_OPERATOR__DB_NAME"
PGPASSWORD="$SUPERSET_OPERATOR__DB_PASS" createdb -h "$SUPERSET_OPERATOR__DB_HOST" -p "$SUPERSET_OPERATOR__DB_PORT" -U "$SUPERSET_OPERATOR__DB_USER" "$SUPERSET_OPERATOR__DB_NAME"
PGPASSWORD="$SUPERSET_OPERATOR__CLONE_SRC_PASS" pg_dump -h "$SUPERSET_OPERATOR__CLONE_SRC_HOST" -p "$SUPERSET_OPERATOR__CLONE_SRC_PORT" -U "$SUPERSET_OPERATOR__CLONE_SRC_USER" --no-owner --no-privileges`)

	for _, t := range clone.ExcludeTables {
		fmt.Fprintf(&b, ` --exclude-table=%q`, t)
	}
	for _, t := range clone.ExcludeTableData {
		fmt.Fprintf(&b, ` --exclude-table-data=%q`, t)
	}

	b.WriteString(` "$SUPERSET_OPERATOR__CLONE_SRC_DB" | PGPASSWORD="$SUPERSET_OPERATOR__DB_PASS" psql -h "$SUPERSET_OPERATOR__DB_HOST" -p "$SUPERSET_OPERATOR__DB_PORT" -U "$SUPERSET_OPERATOR__DB_USER" "$SUPERSET_OPERATOR__DB_NAME"`)

	for _, sql := range clone.PostCloneSQL {
		fmt.Fprintf(&b, "\nPGPASSWORD=\"$SUPERSET_OPERATOR__DB_PASS\" psql -h \"$SUPERSET_OPERATOR__DB_HOST\" -p \"$SUPERSET_OPERATOR__DB_PORT\" -U \"$SUPERSET_OPERATOR__DB_USER\" \"$SUPERSET_OPERATOR__DB_NAME\" -c %q", sql)
	}

	return b.String()
}

func buildMySQLCloneScript(clone *supersetv1alpha1.CloneTaskSpec) string {
	var b strings.Builder
	// Passwords are passed via the MYSQL_PWD environment variable rather than
	// -p"$PASS" so they never appear in the process argv (visible via ps or
	// /proc/<pid>/cmdline), mirroring lifecycle_create_db.go. The target
	// password is exported once at the top; every target mysql invocation
	// inherits it. The source mysqldump runs inside the pipeline subshell where
	// MYSQL_PWD is re-exported to the source password, so the two passwords
	// never collide. Empty passwords are left unset to support passwordless
	// auth (trust/IAM), matching the create-database helper.
	//
	// The target database identifier is backtick-quoted with internal backticks
	// doubled (MySQL's escape rule), and the backticks are backslash-escaped so
	// they reach mysql literally instead of triggering shell command
	// substitution inside the double-quoted -e argument.
	b.WriteString(`set -e
if [ -n "${SUPERSET_OPERATOR__DB_PASS:-}" ]; then export MYSQL_PWD="$SUPERSET_OPERATOR__DB_PASS"; fi
ESC_NAME=$(printf '%s' "$SUPERSET_OPERATOR__DB_NAME" | sed 's/` + "`" + `/` + "``" + `/g')
mysql -h "$SUPERSET_OPERATOR__DB_HOST" -P "$SUPERSET_OPERATOR__DB_PORT" -u "$SUPERSET_OPERATOR__DB_USER" -e "DROP DATABASE IF EXISTS \` + "`" + `${ESC_NAME}\` + "`" + `; CREATE DATABASE \` + "`" + `${ESC_NAME}\` + "`" + `;"
`)

	// mysqldump has no per-table --no-data flag, so emit two passes joined into
	// one stream: a schema-only pass for ExcludeTableData tables (preserves
	// CREATE TABLE plus per-table triggers, mirroring Postgres
	// --exclude-table-data which keeps everything except row data), then a
	// data pass that ignores both ExcludeTables and ExcludeTableData. The
	// combined stdout is piped to the target mysql client. The schema pass is
	// skipped when ExcludeTableData is empty so the existing single-pass
	// behaviour is preserved.
	mysqldumpHead := `mysqldump -h "$SUPERSET_OPERATOR__CLONE_SRC_HOST" -P "$SUPERSET_OPERATOR__CLONE_SRC_PORT" -u "$SUPERSET_OPERATOR__CLONE_SRC_USER"`

	// Open the dump subshell and scope the source password to it only, so the
	// downstream target mysql keeps the target password exported above.
	b.WriteString(`( if [ -n "${SUPERSET_OPERATOR__CLONE_SRC_PASS:-}" ]; then export MYSQL_PWD="$SUPERSET_OPERATOR__CLONE_SRC_PASS"; fi ; `)
	if len(clone.ExcludeTableData) > 0 {
		fmt.Fprintf(&b, "%s --single-transaction --no-data", mysqldumpHead)
		b.WriteString(` "$SUPERSET_OPERATOR__CLONE_SRC_DB"`)
		for _, t := range clone.ExcludeTableData {
			fmt.Fprintf(&b, ` %q`, t)
		}
		b.WriteString(" ; ")
	}

	fmt.Fprintf(&b, "%s --single-transaction --routines --triggers", mysqldumpHead)
	for _, t := range clone.ExcludeTables {
		fmt.Fprintf(&b, ` --ignore-table="$SUPERSET_OPERATOR__CLONE_SRC_DB".%q`, t)
	}
	for _, t := range clone.ExcludeTableData {
		fmt.Fprintf(&b, ` --ignore-table="$SUPERSET_OPERATOR__CLONE_SRC_DB".%q`, t)
	}
	b.WriteString(` "$SUPERSET_OPERATOR__CLONE_SRC_DB" ) | mysql -h "$SUPERSET_OPERATOR__DB_HOST" -P "$SUPERSET_OPERATOR__DB_PORT" -u "$SUPERSET_OPERATOR__DB_USER" "$SUPERSET_OPERATOR__DB_NAME"`)

	for _, sql := range clone.PostCloneSQL {
		fmt.Fprintf(&b, "\nmysql -h \"$SUPERSET_OPERATOR__DB_HOST\" -P \"$SUPERSET_OPERATOR__DB_PORT\" -u \"$SUPERSET_OPERATOR__DB_USER\" \"$SUPERSET_OPERATOR__DB_NAME\" -e %q", sql)
	}

	return b.String()
}

// collectCloneEnvVars builds env vars for the clone task Job.
// Includes both source (CLONE_SRC_*) and target (DB_*) connection details.
func collectCloneEnvVars(superset *supersetv1alpha1.Superset) []corev1.EnvVar {
	var envs []corev1.EnvVar
	clone := superset.Spec.Lifecycle.Clone
	spec := &superset.Spec

	// Source env vars.
	envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcHost, Value: clone.Source.Host})

	port := defaultDBPort(clone.Source.Type)
	if clone.Source.Port != nil {
		port = *clone.Source.Port
	}
	envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcPort, Value: fmt.Sprintf("%d", port)})
	envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcDB, Value: clone.Source.Database})
	envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcUser, Value: clone.Source.Username})

	if clone.Source.Password != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvCloneSrcPass, Value: *clone.Source.Password})
	} else if clone.Source.PasswordFrom != nil {
		envs = append(envs, corev1.EnvVar{
			Name:      naming.EnvCloneSrcPass,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: clone.Source.PasswordFrom},
		})
	}

	// Target env vars (from spec.metastore; clone requires structured metastore).
	if spec.Metastore != nil && spec.Metastore.Host != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvDBHost, Value: *spec.Metastore.Host})
		targetPort := defaultDBPort(spec.Metastore.Type)
		if spec.Metastore.Port != nil {
			targetPort = *spec.Metastore.Port
		}
		envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPort, Value: fmt.Sprintf("%d", targetPort)})
		if spec.Metastore.Database != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBName, Value: *spec.Metastore.Database})
		}
		if spec.Metastore.Username != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBUser, Value: *spec.Metastore.Username})
		}
		if spec.Metastore.Password != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPass, Value: *spec.Metastore.Password})
		} else if spec.Metastore.PasswordFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvDBPass,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Metastore.PasswordFrom},
			})
		}
	}

	return envs
}

// resolveCloneImage determines the image for the clone pod. Defaults are
// type-aware: postgres:17-alpine for PostgreSQL sources, mysql:8-alpine for
// MySQL. Partial user specs inherit the default repository or tag for omitted
// fields rather than the Superset image (which would be incorrect for a
// database tooling container).
func resolveCloneImage(clone *supersetv1alpha1.CloneTaskSpec) supersetv1alpha1.ImageSpec {
	srcType := dbTypePostgresql
	if clone.Source.Type != nil {
		srcType = *clone.Source.Type
	}
	defaultRef := naming.CloneImagePostgres
	if srcType == dbTypeMySQL {
		defaultRef = naming.CloneImageMySQL
	}
	defRepo, defTag := splitImageRef(defaultRef)
	return resolveContainerImage(clone.Image, defRepo, defTag)
}

// resolveContainerImage merges a user-provided ContainerImageSpec with
// context-specific defaults. Fields the user set win; omitted fields fall back
// to the supplied defaults.
func resolveContainerImage(spec *supersetv1alpha1.ContainerImageSpec, defaultRepo, defaultTag string) supersetv1alpha1.ImageSpec {
	if spec == nil {
		return supersetv1alpha1.ImageSpec{Repository: defaultRepo, Tag: defaultTag}
	}
	img := supersetv1alpha1.ImageSpec{
		Repository:  spec.Repository,
		Tag:         spec.Tag,
		PullPolicy:  spec.PullPolicy,
		PullSecrets: spec.PullSecrets,
	}
	if img.Repository == "" {
		img.Repository = defaultRepo
	}
	if img.Tag == "" {
		img.Tag = defaultTag
	}
	return img
}

func splitImageRef(ref string) (string, string) {
	if idx := strings.LastIndex(ref, ":"); idx != -1 {
		return ref[:idx], ref[idx+1:]
	}
	return ref, defaultImageTag
}

// convertCloneComponent builds a minimal ComponentInput for the clone task Job.
func convertCloneComponent(clone *supersetv1alpha1.CloneTaskSpec, command []string) *resolution.ComponentInput {
	var pt *supersetv1alpha1.PodTemplate
	if clone.PodTemplate != nil {
		pt = clone.PodTemplate
	}

	var ct *supersetv1alpha1.ContainerTemplate
	if pt != nil && pt.Container != nil {
		copied := *pt.Container
		ct = &copied
	} else {
		ct = &supersetv1alpha1.ContainerTemplate{}
	}
	ct.Command = command

	if pt != nil {
		copied := *pt
		copied.Container = ct
		pt = &copied
	} else {
		pt = &supersetv1alpha1.PodTemplate{Container: ct}
	}

	return &resolution.ComponentInput{
		SharedInput: resolution.SharedInput{
			PodTemplate: pt,
		},
	}
}
