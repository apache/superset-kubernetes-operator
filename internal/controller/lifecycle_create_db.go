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

const createDatabaseContainerName = "create-database"

// Postgres: query pg_database to check existence, then `createdb` if absent.
// The DB name is SQL-escaped client-side by doubling single quotes (the only
// metacharacter inside a SQL string literal). We avoid psql's :'name'
// interpolation because psql does not process client-side features like
// variable substitution when the command is passed via -c — the literal
// :'name' would reach the server and fail with a syntax error. `createdb --
// "$NAME"` then passes the name as a CLI arg (handled by libpq), and `--`
// prevents it being interpreted as a flag if it starts with `-`.
// Password uses ${VAR:-} to support passwordless connections (trust/peer auth,
// IAM-issued credentials), matching the rendered config's os.environ.get fallback.
const createDatabasePostgresScript = `set -eu
ESC_NAME=$(printf '%s' "$SUPERSET_OPERATOR__DB_NAME" | sed "s/'/''/g")
EXISTS=$(PGPASSWORD="${SUPERSET_OPERATOR__DB_PASS:-}" psql \
  -h "$SUPERSET_OPERATOR__DB_HOST" \
  -p "$SUPERSET_OPERATOR__DB_PORT" \
  -U "$SUPERSET_OPERATOR__DB_USER" \
  -d postgres \
  -v ON_ERROR_STOP=1 \
  -tA -c \
  "SELECT 1 FROM pg_database WHERE datname = '$ESC_NAME'")
if [ "$EXISTS" = "1" ]; then
  echo "Database $SUPERSET_OPERATOR__DB_NAME already exists"
else
  PGPASSWORD="${SUPERSET_OPERATOR__DB_PASS:-}" createdb \
    -h "$SUPERSET_OPERATOR__DB_HOST" \
    -p "$SUPERSET_OPERATOR__DB_PORT" \
    -U "$SUPERSET_OPERATOR__DB_USER" \
    -- "$SUPERSET_OPERATOR__DB_NAME"
  echo "Database $SUPERSET_OPERATOR__DB_NAME created"
fi`

// MySQL: native CREATE DATABASE IF NOT EXISTS is idempotent. The DB name is
// emitted as a backtick-quoted identifier with internal backticks doubled —
// MySQL's escape rule for backtick-quoted identifiers. The shell `\` before
// each ` suppresses command substitution inside the double-quoted argument
// so the literal backticks reach mysql. Password is passed via MYSQL_PWD env
// var (rather than -p"$PASS") so passwords containing whitespace or shell
// metacharacters work without word-splitting, and so passwordless setups
// (trust auth, IAM) skip MYSQL_PWD entirely instead of passing -p which would
// trigger an interactive prompt.
const createDatabaseMySQLScript = `set -eu
ESC_NAME=$(printf '%s' "$SUPERSET_OPERATOR__DB_NAME" | sed 's/` + "`" + `/` + "``" + `/g')
if [ -n "${SUPERSET_OPERATOR__DB_PASS:-}" ]; then
  export MYSQL_PWD="$SUPERSET_OPERATOR__DB_PASS"
fi
mysql -h "$SUPERSET_OPERATOR__DB_HOST" \
      -P "$SUPERSET_OPERATOR__DB_PORT" \
      -u "$SUPERSET_OPERATOR__DB_USER" \
      -e "CREATE DATABASE IF NOT EXISTS \` + "`" + `${ESC_NAME}\` + "`" + `"
echo "Ensured MySQL database $SUPERSET_OPERATOR__DB_NAME exists"`

// buildCreateDatabaseInitContainer returns a one-shot init container that
// issues `CREATE DATABASE` against the metastore server, or nil when the
// feature is not enabled. Resources and SecurityContext are inherited from
// the resolved lifecycle container template (spec.lifecycle.podTemplate.container,
// merged with the top-level spec.podTemplate.container default by the
// resolver). This lets users satisfy strict admission policies (Pod Security
// Standards, Kyverno, OPA) by configuring lifecycle hardening once. The
// image stays operator-default — the migrate task uses the Superset image,
// but this init container needs psql/mysql clients (postgres:17-alpine /
// mysql:8-alpine).
func buildCreateDatabaseInitContainer(superset *supersetv1alpha1.Superset, lifecyclePod *supersetv1alpha1.PodTemplate) *corev1.Container {
	if !createDatabaseEnabled(superset) {
		return nil
	}
	dbType := metastoreType(superset.Spec.Metastore)
	image := resolveCreateDatabaseImage(dbType)
	script := createDatabasePostgresScript
	if dbType == dbTypeMySQL {
		script = createDatabaseMySQLScript
	}
	ctr := &corev1.Container{
		Name:            createDatabaseContainerName,
		Image:           fmt.Sprintf("%s:%s", image.Repository, image.Tag),
		ImagePullPolicy: image.PullPolicy,
		Command:         []string{"/bin/sh", "-c", script},
		Env:             createDatabaseEnvVars(superset.Spec.Metastore),
	}
	if lifecyclePod != nil && lifecyclePod.Container != nil {
		if lifecyclePod.Container.Resources != nil {
			ctr.Resources = *lifecyclePod.Container.Resources
		}
		ctr.SecurityContext = lifecyclePod.Container.SecurityContext
	}
	return ctr
}

// createDatabaseEnabled reports whether spec.metastore.createDatabase is true
// AND the structured fields the init container relies on are present. CEL
// already enforces this, but checking here keeps the controller defensive
// against malformed CRs that might slip past validation (e.g., upgrades from
// older CRD versions, direct etcd writes, missing CEL on the apiserver).
func createDatabaseEnabled(superset *supersetv1alpha1.Superset) bool {
	if superset == nil || superset.Spec.Metastore == nil {
		return false
	}
	m := superset.Spec.Metastore
	if m.CreateDatabase == nil || !*m.CreateDatabase {
		return false
	}
	return m.Host != nil && m.Database != nil && m.Username != nil
}

// metastoreType returns the DB type, defaulting to PostgreSQL.
func metastoreType(metastore *supersetv1alpha1.MetastoreSpec) string {
	if metastore != nil && metastore.Type != nil {
		return *metastore.Type
	}
	return dbTypePostgresql
}

// resolveCreateDatabaseImage selects the DB-tool image. The metastore spec
// has no image override field, so callers always get the operator default.
func resolveCreateDatabaseImage(dbType string) supersetv1alpha1.ImageSpec {
	defaultRef := naming.CloneImagePostgres
	if dbType == dbTypeMySQL {
		defaultRef = naming.CloneImageMySQL
	}
	repo, tag := splitImageRef(defaultRef)
	return supersetv1alpha1.ImageSpec{Repository: repo, Tag: tag}
}

// createDatabaseEnvVars returns the SUPERSET_OPERATOR__DB_* vars consumed by
// the init container's psql/mysql invocation. Mirrors the structured-mode
// branch of collectSecretEnvVars; the init container does not need URI/Valkey
// vars, and CEL prevents URI mode + createDatabase.
func createDatabaseEnvVars(metastore *supersetv1alpha1.MetastoreSpec) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{Name: naming.EnvDBHost, Value: *metastore.Host},
	}
	port := defaultDBPort(metastore.Type)
	if metastore.Port != nil {
		port = *metastore.Port
	}
	envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPort, Value: fmt.Sprintf("%d", port)})
	if metastore.Database != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvDBName, Value: *metastore.Database})
	}
	if metastore.Username != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvDBUser, Value: *metastore.Username})
	}
	if metastore.Password != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPass, Value: *metastore.Password})
	} else if metastore.PasswordFrom != nil {
		envs = append(envs, corev1.EnvVar{
			Name:      naming.EnvDBPass,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: metastore.PasswordFrom},
		})
	}
	return envs
}
