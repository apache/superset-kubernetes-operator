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

package v1alpha1

// CelerySpec configures the operator-rendered CeleryConfig class. Settings here
// flow into the class the operator generates when spec.valkey is set. Admins
// can extend the class further from raw spec.config (mutating attributes,
// subclassing, or replacing CELERY_CONFIG outright).
type CelerySpec struct {
	// Imports lists Python modules that Celery workers import on startup to
	// discover task definitions. When unset, defaults to the modules shipped
	// by upstream Superset:
	// superset.sql_lab, superset.tasks.scheduler, superset.tasks.thumbnails,
	// superset.tasks.cache, superset.tasks.slack.
	// Setting this field replaces the default list wholesale; admins who want
	// to extend rather than replace can mutate CeleryConfig.imports from raw
	// spec.config.
	// +optional
	Imports []string `json:"imports,omitempty"`
}
