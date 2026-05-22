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
	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// migrateInputs returns the migrate-specific inputs that contribute to its step
// checksum. Migrate is image/schema-version driven and intentionally ignores
// most config changes — a config tweak alone must not re-run migrations. When
// createDatabase is true, the migrate Job carries a create-database init
// container that reads the structured metastore target; changes to that
// target (host/port/database/username/type) must re-run migrate so the init
// container actually executes against the new server. The flag itself is also
// included so toggling it re-runs migrate.
func (r *SupersetReconciler) migrateInputs(superset *supersetv1alpha1.Superset) any {
	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	trigger := ""
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Migrate != nil {
		trigger = derefOrDefault(superset.Spec.Lifecycle.Migrate.Trigger, "")
	}
	createDatabase := false
	var target struct {
		Type     string
		Host     string
		Port     int32
		Database string
		Username string
	}
	if superset.Spec.Metastore != nil && superset.Spec.Metastore.CreateDatabase != nil && *superset.Spec.Metastore.CreateDatabase {
		createDatabase = true
		m := superset.Spec.Metastore
		target.Type = derefOrDefault(m.Type, dbTypePostgresql)
		target.Host = derefOrDefault(m.Host, "")
		target.Port = defaultDBPort(m.Type)
		if m.Port != nil {
			target.Port = *m.Port
		}
		target.Database = derefOrDefault(m.Database, "")
		target.Username = derefOrDefault(m.Username, "")
	}
	return struct {
		Image          string
		Trigger        string
		CreateDatabase bool
		Target         any
	}{
		Image:          currentImage,
		Trigger:        trigger,
		CreateDatabase: createDatabase,
		Target:         target,
	}
}

// defaultMigrateCommand returns the user override or the standard
// `superset db upgrade` command.
func defaultMigrateCommand(superset *supersetv1alpha1.Superset) []string {
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Migrate != nil && len(superset.Spec.Lifecycle.Migrate.Command) > 0 {
		return superset.Spec.Lifecycle.Migrate.Command
	}
	return []string{"/bin/sh", "-c", "superset db upgrade"}
}
