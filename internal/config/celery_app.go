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

// renderCeleryClass writes only the CeleryConfig fields derived from structured
// Valkey settings. Application-level Celery behavior such as imports, task
// annotations, and beat schedules belongs in spec.config.
func renderCeleryClass(b *strings.Builder, v *ValkeyInput, hasSSLOpts bool) {
	brokerEnabled := !v.CeleryBroker.Disabled
	resultEnabled := !v.CeleryResultBackend.Disabled
	if !brokerEnabled && !resultEnabled {
		return
	}

	b.WriteString("\n")
	b.WriteString("class CeleryConfig:\n")
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

	b.WriteString("CELERY_CONFIG = CeleryConfig\n")
}
