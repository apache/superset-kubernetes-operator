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

package config

import (
	"fmt"
	"strings"
)

// CeleryInput is the resolved spec.celery passed to the renderer. The controller
// fills in upstream defaults so the renderer can emit unconditionally.
type CeleryInput struct {
	// Imports lists Python modules registered on the Celery worker via
	// CeleryConfig.imports. Pre-defaulted by the controller; nil here means
	// "do not emit imports" (admins explicitly set imports: [] in YAML).
	Imports []string
}

// DefaultCeleryImports lists the Python modules upstream Superset registers
// on the Celery worker by default (see superset/config.py CeleryConfig.imports).
// Used by the controller when spec.celery.imports is unset.
var DefaultCeleryImports = []string{
	"superset.sql_lab",
	"superset.tasks.scheduler",
	"superset.tasks.thumbnails",
	"superset.tasks.cache",
	"superset.tasks.slack",
}

// renderCeleryClass writes the CeleryConfig class derived from Valkey settings
// and typed spec.celery, plus upstream Superset defaults that would otherwise
// be lost by replacing the upstream CeleryConfig class. Admins extend the class
// further via spec.config (mutating attributes, subclassing, or replacing
// CELERY_CONFIG) using SUPERSET_OPERATOR__INSTANCE_NAME to compute
// instance-scoped queue names.
func renderCeleryClass(b *strings.Builder, v *ValkeyInput, c *CeleryInput, hasSSLOpts bool) {
	brokerEnabled := !v.CeleryBroker.Disabled
	resultEnabled := !v.CeleryResultBackend.Disabled
	if !brokerEnabled && !resultEnabled {
		return
	}

	b.WriteString("\nclass CeleryConfig:\n")
	if brokerEnabled {
		fmt.Fprintf(b, "    broker_url = f\"{_vk_base}/%d\"\n", v.CeleryBroker.Database)
	}
	if resultEnabled {
		fmt.Fprintf(b, "    result_backend = f\"{_vk_base}/%d\"\n", v.CeleryResultBackend.Database)
	}
	if hasSSLOpts {
		if brokerEnabled {
			b.WriteString("    broker_use_ssl = _vk_ssl_opts\n")
		}
		if resultEnabled {
			b.WriteString("    redis_backend_use_ssl = _vk_ssl_opts\n")
		}
	}

	// Typed imports (defaults to upstream tuple, applied by the controller).
	if c != nil {
		renderCeleryImports(b, c.Imports)
	}

	// Hardcoded upstream defaults, preserved so that replacing upstream's
	// CeleryConfig class doesn't silently drop them. Admins override via
	// raw spec.config (e.g. CeleryConfig.beat_schedule = {...}).
	b.WriteString("    worker_prefetch_multiplier = 1\n")
	b.WriteString("    task_acks_late = False\n")
	b.WriteString("    task_annotations = {\n")
	b.WriteString("        \"sql_lab.get_sql_results\": {\n")
	b.WriteString("            \"rate_limit\": \"100/s\",\n")
	b.WriteString("        },\n")
	b.WriteString("    }\n")
	b.WriteString("    beat_schedule = {\n")
	b.WriteString("        \"reports.scheduler\": {\n")
	b.WriteString("            \"task\": \"reports.scheduler\",\n")
	b.WriteString("            \"schedule\": crontab(minute=\"*\", hour=\"*\"),\n")
	b.WriteString("        },\n")
	b.WriteString("        \"reports.prune_log\": {\n")
	b.WriteString("            \"task\": \"reports.prune_log\",\n")
	b.WriteString("            \"schedule\": crontab(minute=0, hour=0),\n")
	b.WriteString("        },\n")
	b.WriteString("    }\n")

	b.WriteString("CELERY_CONFIG = CeleryConfig\n")
}

// renderCeleryImports writes the imports tuple. An empty (non-nil) slice
// renders as `imports = ()` to honor admins who explicitly suppress imports.
// Nil means "skip" (controller is responsible for applying defaults).
func renderCeleryImports(b *strings.Builder, imports []string) {
	if imports == nil {
		return
	}
	if len(imports) == 0 {
		b.WriteString("    imports = ()\n")
		return
	}
	b.WriteString("    imports = (\n")
	for _, m := range imports {
		fmt.Fprintf(b, "        \"%s\",\n", pyQuote(m))
	}
	b.WriteString("    )\n")
}

// celeryClassWillRender reports whether renderCeleryClass would emit a class
// for the given inputs. Used by the renderer to decide whether to add the
// `from celery.schedules import crontab` import.
func celeryClassWillRender(v *ValkeyInput) bool {
	if v == nil {
		return false
	}
	return !v.CeleryBroker.Disabled || !v.CeleryResultBackend.Disabled
}
