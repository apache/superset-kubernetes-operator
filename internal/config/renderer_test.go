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
	"strings"
	"testing"
)

func TestRenderConfig_WebServer(t *testing.T) {
	input := &ConfigInput{
		MetastoreMode:   MetastoreNone,
		Config:          "FEATURE_FLAGS = {\"ALERT_REPORTS\": True}",
		ComponentConfig: "WEBSERVER_THREADS = 16",
	}

	result := RenderConfig(ComponentWebServer, input)

	assertContains(t, result, "import os")
	assertContains(t, result, "SECRET_KEY = os.environ['SUPERSET_OPERATOR__SECRET_KEY']")
	assertContains(t, result, "SUPERSET_WEBSERVER_PORT = 8088")
	assertContains(t, result, "# Base config (spec.config)")
	assertContains(t, result, "FEATURE_FLAGS")
	assertContains(t, result, "# Component config")
	assertContains(t, result, "WEBSERVER_THREADS = 16")
}

func TestRenderConfig_PassthroughMetastore(t *testing.T) {
	input := &ConfigInput{
		MetastoreMode: MetastorePassthrough,
	}
	result := RenderConfig(ComponentWebServer, input)
	assertContains(t, result, "SQLALCHEMY_DATABASE_URI = os.environ['SUPERSET_OPERATOR__DB_URI']")
	assertContains(t, result, "SQLALCHEMY_EXAMPLES_URI = SQLALCHEMY_DATABASE_URI")
	assertNotContains(t, result, "from urllib.parse import quote")
}

func TestRenderConfig_StructuredMetastore(t *testing.T) {
	t.Run("postgresql", func(t *testing.T) {
		input := &ConfigInput{
			MetastoreMode: MetastoreStructured,
			DBDriver:      "postgresql",
		}
		result := RenderConfig(ComponentWebServer, input)
		assertContains(t, result, "from urllib.parse import quote")
		assertContains(t, result, "postgresql+psycopg2://")
		assertContains(t, result, "quote(os.environ['SUPERSET_OPERATOR__DB_USER'], safe='')")
		assertContains(t, result, "quote(_db_pass, safe='')")
		assertContains(t, result, "os.environ.get(\"SUPERSET_OPERATOR__DB_PASS\"")
		assertContains(t, result, "os.environ['SUPERSET_OPERATOR__DB_HOST']")
		assertContains(t, result, "os.environ['SUPERSET_OPERATOR__DB_PORT']")
		assertContains(t, result, "quote(os.environ['SUPERSET_OPERATOR__DB_NAME'], safe='')")
	})

	t.Run("mysql", func(t *testing.T) {
		input := &ConfigInput{
			MetastoreMode: MetastoreStructured,
			DBDriver:      "mysql",
		}
		result := RenderConfig(ComponentWebServer, input)
		assertContains(t, result, "mysql+mysqlconnector://")
	})
}

func TestRenderConfig_ComponentVariants(t *testing.T) {
	tests := []struct {
		name      string
		component ComponentType
		input     *ConfigInput
		contains  []string
	}{
		{"init", ComponentInit, &ConfigInput{MetastoreMode: MetastoreNone},
			[]string{"import os", "# Operator-generated configs"}},
		{"celery-worker", ComponentCeleryWorker, &ConfigInput{MetastoreMode: MetastoreNone},
			[]string{"import os", "# Operator-generated configs"}},
		{"celery-beat", ComponentCeleryBeat, &ConfigInput{MetastoreMode: MetastoreNone},
			[]string{"import os", "# Operator-generated configs"}},
		{"celery-flower", ComponentCeleryFlower, &ConfigInput{MetastoreMode: MetastoreNone},
			[]string{"import os", "# Operator-generated configs"}},
		{"mcp-server", ComponentMcpServer, &ConfigInput{MetastoreMode: MetastoreNone},
			[]string{"import os", "# Operator-generated configs"}},
		{"minimal (no metastore)", ComponentCeleryWorker, &ConfigInput{},
			[]string{"import os", "# Operator-generated configs"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RenderConfig(tt.component, tt.input)
			for _, want := range tt.contains {
				assertContains(t, result, want)
			}
		})
	}
}

func TestRenderConfig_SectionOrder(t *testing.T) {
	input := &ConfigInput{
		MetastoreMode:   MetastoreStructured,
		DBDriver:        "postgresql",
		Config:          "BASE_SETTING = True\n",
		ComponentConfig: "COMP_SETTING = 42\n",
	}

	result := RenderConfig(ComponentWebServer, input)

	opIdx := strings.Index(result, "# Operator-generated configs")
	baseIdx := strings.Index(result, "# Base config (spec.config)")
	compIdx := strings.Index(result, "# Component config")

	if opIdx < 0 || baseIdx < 0 || compIdx < 0 {
		t.Fatalf("missing section headers in output:\n%s", result)
	}
	if opIdx >= baseIdx {
		t.Errorf("operator-generated header (pos %d) should appear before base config header (pos %d)", opIdx, baseIdx)
	}
	if baseIdx >= compIdx {
		t.Errorf("base config header (pos %d) should appear before component config header (pos %d)", baseIdx, compIdx)
	}
}

func TestRenderConfig_WebsocketServerReturnsEmpty(t *testing.T) {
	input := &ConfigInput{MetastoreMode: MetastoreNone}
	result := RenderConfig(ComponentWebsocketServer, input)
	if result != "" {
		t.Errorf("expected empty string for WebsocketServer, got:\n%s", result)
	}
}

func TestRenderConfig_ValkeyMinimal(t *testing.T) {
	input := &ConfigInput{
		Valkey: &ValkeyInput{
			Cache:                ValkeyCacheInput{Database: 1, KeyPrefix: "superset_", DefaultTimeout: 300},
			DataCache:            ValkeyCacheInput{Database: 2, KeyPrefix: "superset_data_", DefaultTimeout: 86400},
			FilterStateCache:     ValkeyCacheInput{Database: 3, KeyPrefix: "superset_filter_", DefaultTimeout: 3600},
			ExploreFormDataCache: ValkeyCacheInput{Database: 4, KeyPrefix: "superset_explore_", DefaultTimeout: 3600},
			ThumbnailCache:       ValkeyCacheInput{Database: 5, KeyPrefix: "superset_thumbnail_", DefaultTimeout: 3600},
			CeleryBroker:         ValkeyCeleryInput{Database: 0},
			CeleryResultBackend:  ValkeyCeleryInput{Database: 0},
			ResultsBackend:       ValkeyResultsInput{Database: 6, KeyPrefix: "superset_results_"},
		},
	}
	result := RenderConfig(ComponentWebServer, input)

	// Imports
	assertContains(t, result, "import os")
	assertContains(t, result, "from cachelib.redis import RedisCache as _CachelibRedis")

	// Connection helpers
	assertContains(t, result, "_vk_pass = os.environ.get(\"SUPERSET_OPERATOR__VALKEY_PASS\"")
	assertContains(t, result, "_vk_scheme = \"redis\"")
	assertContains(t, result, "from urllib.parse import quote")
	assertContains(t, result, "quote(_vk_pass, safe='')")
	assertContains(t, result, "_vk_base = f\"{_vk_scheme}://")
	assertContains(t, result, "SUPERSET_OPERATOR__VALKEY_HOST")
	assertContains(t, result, "SUPERSET_OPERATOR__VALKEY_PORT")

	// Flask-Caching sections
	assertContains(t, result, "CACHE_CONFIG = {")
	assertContains(t, result, "\"CACHE_DEFAULT_TIMEOUT\": 300")
	assertContains(t, result, "\"CACHE_KEY_PREFIX\": \"superset_\"")
	assertContains(t, result, "\"CACHE_REDIS_URL\": f\"{_vk_base}/1\"")

	assertContains(t, result, "DATA_CACHE_CONFIG = {")
	assertContains(t, result, "\"CACHE_DEFAULT_TIMEOUT\": 86400")
	assertContains(t, result, "\"CACHE_REDIS_URL\": f\"{_vk_base}/2\"")

	assertContains(t, result, "FILTER_STATE_CACHE_CONFIG = {")
	assertContains(t, result, "EXPLORE_FORM_DATA_CACHE_CONFIG = {")
	assertContains(t, result, "THUMBNAIL_CACHE_CONFIG = {")

	// Celery
	assertContains(t, result, "class CeleryConfig:")
	assertContains(t, result, "broker_url = f\"{_vk_base}/0\"")
	assertContains(t, result, "result_backend = f\"{_vk_base}/0\"")
	assertContains(t, result, "CELERY_CONFIG = CeleryConfig")

	// Results backend
	assertContains(t, result, "RESULTS_BACKEND = _CachelibRedis(")
	assertContains(t, result, "db=6")
	assertContains(t, result, "key_prefix=\"superset_results_\"")

	// No SSL
	assertNotContains(t, result, "rediss")
	assertNotContains(t, result, "_vk_ssl_opts")
	assertNotContains(t, result, "ssl=True")
}

func TestRenderConfig_ValkeyWithSSL(t *testing.T) {
	input := &ConfigInput{
		Valkey: &ValkeyInput{
			SSL:                  true,
			Cache:                ValkeyCacheInput{Database: 1, KeyPrefix: "s_", DefaultTimeout: 300},
			CeleryBroker:         ValkeyCeleryInput{Disabled: true},
			CeleryResultBackend:  ValkeyCeleryInput{Disabled: true},
			ResultsBackend:       ValkeyResultsInput{Database: 6, KeyPrefix: "r_"},
			DataCache:            ValkeyCacheInput{Disabled: true},
			FilterStateCache:     ValkeyCacheInput{Disabled: true},
			ExploreFormDataCache: ValkeyCacheInput{Disabled: true},
			ThumbnailCache:       ValkeyCacheInput{Disabled: true},
		},
	}
	result := RenderConfig(ComponentCeleryWorker, input)

	assertContains(t, result, "_vk_scheme = \"rediss\"")
	assertContains(t, result, "CACHE_CONFIG = {")
	assertContains(t, result, "ssl=True")
	// No ssl_opts when no cert paths set
	assertNotContains(t, result, "_vk_ssl_opts")
}

func TestRenderConfig_ValkeyWithMTLS(t *testing.T) {
	input := &ConfigInput{
		Valkey: &ValkeyInput{
			SSL:                  true,
			SSLCertRequired:      "required",
			SSLKeyFile:           "/mnt/tls/client.key.pem",
			SSLCertFile:          "/mnt/tls/client.crt.pem",
			SSLCACertFile:        "/mnt/tls/ca.pem",
			Cache:                ValkeyCacheInput{Database: 1, KeyPrefix: "s_", DefaultTimeout: 300},
			DataCache:            ValkeyCacheInput{Disabled: true},
			FilterStateCache:     ValkeyCacheInput{Disabled: true},
			ExploreFormDataCache: ValkeyCacheInput{Disabled: true},
			ThumbnailCache:       ValkeyCacheInput{Disabled: true},
			CeleryBroker:         ValkeyCeleryInput{Database: 0},
			CeleryResultBackend:  ValkeyCeleryInput{Database: 0},
			ResultsBackend:       ValkeyResultsInput{Database: 6, KeyPrefix: "r_"},
		},
	}
	result := RenderConfig(ComponentWebServer, input)

	assertContains(t, result, "_vk_ssl_opts = {")
	assertContains(t, result, "\"ssl_cert_reqs\": \"required\"")
	assertContains(t, result, "\"ssl_keyfile\": \"/mnt/tls/client.key.pem\"")
	assertContains(t, result, "\"ssl_certfile\": \"/mnt/tls/client.crt.pem\"")
	assertContains(t, result, "\"ssl_ca_certs\": \"/mnt/tls/ca.pem\"")

	// Flask-Caching gets CACHE_OPTIONS
	assertContains(t, result, "\"CACHE_OPTIONS\": _vk_ssl_opts")

	// Celery gets SSL opts
	assertContains(t, result, "broker_use_ssl = _vk_ssl_opts")
	assertContains(t, result, "redis_backend_use_ssl = _vk_ssl_opts")

	// Results backend gets SSL kwargs
	assertContains(t, result, "ssl=True")
	assertContains(t, result, "**_vk_ssl_opts")
}

func TestRenderConfig_ValkeyDisabledSections(t *testing.T) {
	input := &ConfigInput{
		Valkey: &ValkeyInput{
			Cache:                ValkeyCacheInput{Database: 1, KeyPrefix: "s_", DefaultTimeout: 300},
			DataCache:            ValkeyCacheInput{Disabled: true},
			FilterStateCache:     ValkeyCacheInput{Disabled: true},
			ExploreFormDataCache: ValkeyCacheInput{Disabled: true},
			ThumbnailCache:       ValkeyCacheInput{Disabled: true},
			CeleryBroker:         ValkeyCeleryInput{Disabled: true},
			CeleryResultBackend:  ValkeyCeleryInput{Disabled: true},
			ResultsBackend:       ValkeyResultsInput{Disabled: true},
		},
	}
	result := RenderConfig(ComponentWebServer, input)

	assertContains(t, result, "CACHE_CONFIG = {")
	assertNotContains(t, result, "DATA_CACHE_CONFIG")
	assertNotContains(t, result, "FILTER_STATE_CACHE_CONFIG")
	assertNotContains(t, result, "EXPLORE_FORM_DATA_CACHE_CONFIG")
	assertNotContains(t, result, "THUMBNAIL_CACHE_CONFIG")
	assertNotContains(t, result, "class CeleryConfig:")
	assertNotContains(t, result, "RESULTS_BACKEND")
	// No cachelib import when results backend disabled
	assertNotContains(t, result, "_CachelibRedis")
}

func TestRenderConfig_ValkeyCustomDatabases(t *testing.T) {
	input := &ConfigInput{
		Valkey: &ValkeyInput{
			Cache:                ValkeyCacheInput{Database: 10, KeyPrefix: "my_", DefaultTimeout: 600},
			DataCache:            ValkeyCacheInput{Database: 11, KeyPrefix: "my_data_", DefaultTimeout: 43200},
			FilterStateCache:     ValkeyCacheInput{Disabled: true},
			ExploreFormDataCache: ValkeyCacheInput{Disabled: true},
			ThumbnailCache:       ValkeyCacheInput{Disabled: true},
			CeleryBroker:         ValkeyCeleryInput{Database: 14},
			CeleryResultBackend:  ValkeyCeleryInput{Database: 15},
			ResultsBackend:       ValkeyResultsInput{Database: 13, KeyPrefix: "my_results_"},
		},
	}
	result := RenderConfig(ComponentCeleryWorker, input)

	assertContains(t, result, "\"CACHE_REDIS_URL\": f\"{_vk_base}/10\"")
	assertContains(t, result, "\"CACHE_DEFAULT_TIMEOUT\": 600")
	assertContains(t, result, "\"CACHE_KEY_PREFIX\": \"my_\"")
	assertContains(t, result, "\"CACHE_REDIS_URL\": f\"{_vk_base}/11\"")
	assertContains(t, result, "broker_url = f\"{_vk_base}/14\"")
	assertContains(t, result, "result_backend = f\"{_vk_base}/15\"")
	assertContains(t, result, "db=13")
	assertContains(t, result, "key_prefix=\"my_results_\"")
}

func TestRenderConfig_ValkeyWebsocketServerSkipped(t *testing.T) {
	input := &ConfigInput{
		Valkey: &ValkeyInput{
			Cache:                ValkeyCacheInput{Database: 1, KeyPrefix: "s_", DefaultTimeout: 300},
			DataCache:            ValkeyCacheInput{Disabled: true},
			FilterStateCache:     ValkeyCacheInput{Disabled: true},
			ExploreFormDataCache: ValkeyCacheInput{Disabled: true},
			ThumbnailCache:       ValkeyCacheInput{Disabled: true},
			CeleryBroker:         ValkeyCeleryInput{Disabled: true},
			CeleryResultBackend:  ValkeyCeleryInput{Disabled: true},
			ResultsBackend:       ValkeyResultsInput{Disabled: true},
		},
	}
	result := RenderConfig(ComponentWebsocketServer, input)
	if result != "" {
		t.Errorf("expected empty for WebsocketServer, got:\n%s", result)
	}
}

func TestRenderConfig_ValkeyNoValkey(t *testing.T) {
	input := &ConfigInput{MetastoreMode: MetastoreNone}
	result := RenderConfig(ComponentWebServer, input)
	assertNotContains(t, result, "_vk_")
	assertNotContains(t, result, "CACHE_CONFIG")
	assertNotContains(t, result, "CeleryConfig")
}

func TestRenderConfig_ValkeySectionOrder(t *testing.T) {
	input := &ConfigInput{
		MetastoreMode: MetastoreStructured,
		DBDriver:      "postgresql",
		Config:        "BASE = True\n",
		Valkey: &ValkeyInput{
			Cache:                ValkeyCacheInput{Database: 1, KeyPrefix: "s_", DefaultTimeout: 300},
			DataCache:            ValkeyCacheInput{Disabled: true},
			FilterStateCache:     ValkeyCacheInput{Disabled: true},
			ExploreFormDataCache: ValkeyCacheInput{Disabled: true},
			ThumbnailCache:       ValkeyCacheInput{Disabled: true},
			CeleryBroker:         ValkeyCeleryInput{Disabled: true},
			CeleryResultBackend:  ValkeyCeleryInput{Disabled: true},
			ResultsBackend:       ValkeyResultsInput{Disabled: true},
		},
	}
	result := RenderConfig(ComponentWebServer, input)

	opIdx := strings.Index(result, "# Operator-generated configs")
	vkIdx := strings.Index(result, "# Valkey cache config")
	baseIdx := strings.Index(result, "# Base config (spec.config)")

	if opIdx < 0 || vkIdx < 0 || baseIdx < 0 {
		t.Fatalf("missing section headers in output:\n%s", result)
	}
	if opIdx >= vkIdx {
		t.Errorf("operator-generated (pos %d) should appear before valkey (pos %d)", opIdx, vkIdx)
	}
	if vkIdx >= baseIdx {
		t.Errorf("valkey (pos %d) should appear before base config (pos %d)", vkIdx, baseIdx)
	}
}

func TestRenderConfig_StructuredMetastore_URLReservedChars(t *testing.T) {
	input := &ConfigInput{
		MetastoreMode: MetastoreStructured,
		DBDriver:      "postgresql",
	}
	result := RenderConfig(ComponentWebServer, input)

	// Verify quote() uses safe="" so characters like / are percent-encoded.
	// Python's default quote(safe="/") leaves / unescaped, which breaks
	// credentials like base64-encoded passwords containing /.
	assertContains(t, result, "quote(os.environ['SUPERSET_OPERATOR__DB_USER'], safe='')")
	assertContains(t, result, "quote(_db_pass, safe='')")
	assertContains(t, result, "quote(os.environ['SUPERSET_OPERATOR__DB_NAME'], safe='')")
}

func TestRenderConfig_Valkey_URLReservedChars(t *testing.T) {
	input := &ConfigInput{
		Valkey: &ValkeyInput{
			Cache:                ValkeyCacheInput{Database: 1, KeyPrefix: "s_", DefaultTimeout: 300},
			DataCache:            ValkeyCacheInput{Disabled: true},
			FilterStateCache:     ValkeyCacheInput{Disabled: true},
			ExploreFormDataCache: ValkeyCacheInput{Disabled: true},
			ThumbnailCache:       ValkeyCacheInput{Disabled: true},
			CeleryBroker:         ValkeyCeleryInput{Disabled: true},
			CeleryResultBackend:  ValkeyCeleryInput{Disabled: true},
			ResultsBackend:       ValkeyResultsInput{Disabled: true},
		},
	}
	result := RenderConfig(ComponentWebServer, input)

	// Verify Valkey password quoting uses safe="" for the same reason.
	assertContains(t, result, "quote(_vk_pass, safe='')")
}

func TestRenderConfig_ValkeyKeyPrefixWithQuotes(t *testing.T) {
	input := &ConfigInput{
		Valkey: &ValkeyInput{
			Cache:                ValkeyCacheInput{Database: 1, KeyPrefix: `my"prefix_`, DefaultTimeout: 300},
			DataCache:            ValkeyCacheInput{Disabled: true},
			FilterStateCache:     ValkeyCacheInput{Disabled: true},
			ExploreFormDataCache: ValkeyCacheInput{Disabled: true},
			ThumbnailCache:       ValkeyCacheInput{Disabled: true},
			CeleryBroker:         ValkeyCeleryInput{Disabled: true},
			CeleryResultBackend:  ValkeyCeleryInput{Disabled: true},
			ResultsBackend:       ValkeyResultsInput{Database: 6, KeyPrefix: `res"ults_`},
		},
	}
	result := RenderConfig(ComponentWebServer, input)

	assertContains(t, result, `"CACHE_KEY_PREFIX": "my\"prefix_"`)
	assertContains(t, result, `key_prefix="res\"ults_"`)
}

func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("expected output to contain %q, but it doesn't.\nFull output:\n%s", substr, s)
	}
}

func assertNotContains(t *testing.T, s, substr string) {
	t.Helper()
	if strings.Contains(s, substr) {
		t.Errorf("expected output NOT to contain %q, but it does.\nFull output:\n%s", substr, s)
	}
}
