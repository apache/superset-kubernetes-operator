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

// renderCeleryClass writes a CeleryConfig that subclasses Superset's default
// CeleryConfig and overrides only the connection fields derived from structured
// Valkey settings. Superset's default task imports, worker settings, and beat
// schedule (reports, log pruning, version-history and soft-delete retention)
// therefore stay in effect; spec.config can extend or replace them. Global Async
// Queries extends the inherited imports and beat schedule. Importing
// superset.config here is safe: superset_config is loaded from the end of that
// module, after its CeleryConfig is defined.
func renderCeleryClass(b *strings.Builder, v *ValkeyInput, asyncQueries bool) {
	brokerEnabled := !v.CeleryBroker.Disabled
	resultEnabled := !v.CeleryResultBackend.Disabled
	if !brokerEnabled && !resultEnabled {
		return
	}

	b.WriteString("\n")
	b.WriteString("from superset.config import CeleryConfig as _SupersetCeleryConfig\n")
	b.WriteString("\n")
	b.WriteString("\n")
	b.WriteString("class CeleryConfig(_SupersetCeleryConfig):\n")
	if v.Sentinel != nil {
		if brokerEnabled {
			fmt.Fprintf(b, "    broker_url = _vk_sentinel_url(%d)\n", v.CeleryBroker.Database)
			b.WriteString("    broker_transport_options = _vk_sentinel_transport\n")
		}
		if resultEnabled {
			fmt.Fprintf(b, "    result_backend = _vk_sentinel_url(%d)\n", v.CeleryResultBackend.Database)
			b.WriteString("    result_backend_transport_options = _vk_sentinel_transport\n")
		}
	} else {
		if brokerEnabled {
			fmt.Fprintf(b, "    broker_url = f\"{_vk_base}/%d\"\n", v.CeleryBroker.Database)
		}
		if resultEnabled {
			fmt.Fprintf(b, "    result_backend = f\"{_vk_base}/%d\"\n", v.CeleryResultBackend.Database)
		}
	}
	if v.SSL {
		if brokerEnabled {
			b.WriteString("    broker_use_ssl = _vk_ssl_opts\n")
		}
		if resultEnabled {
			b.WriteString("    redis_backend_use_ssl = _vk_ssl_opts\n")
		}
	}

	// The Celery app auto-imports superset.tasks.scheduler (reap_orphaned_tasks,
	// tasks.execute) but not async_queries, where the chart-data task lives; the
	// reaper only runs when it has a short beat schedule.
	if asyncQueries {
		b.WriteString("    imports = (*_SupersetCeleryConfig.imports, \"superset.tasks.async_queries\")\n")
		b.WriteString("    beat_schedule = {\n")
		b.WriteString("        **_SupersetCeleryConfig.beat_schedule,\n")
		b.WriteString("        \"reap_orphaned_tasks\": {\n")
		b.WriteString("            \"task\": \"reap_orphaned_tasks\",\n")
		b.WriteString("            \"schedule\": 60.0,\n")
		b.WriteString("        },\n")
		b.WriteString("    }\n")
	}

	b.WriteString("CELERY_CONFIG = CeleryConfig\n")
}
