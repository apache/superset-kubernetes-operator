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
	"strconv"
	"strings"

	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// ComponentType is an alias for common.ComponentType.
type ComponentType = common.ComponentType

// Component type constants re-exported for convenience.
const (
	ComponentWebServer       = common.ComponentWebServer
	ComponentCeleryWorker    = common.ComponentCeleryWorker
	ComponentCeleryBeat      = common.ComponentCeleryBeat
	ComponentCeleryFlower    = common.ComponentCeleryFlower
	ComponentWebsocketServer = common.ComponentWebsocketServer
	ComponentMcpServer       = common.ComponentMcpServer
	ComponentInit            = common.ComponentInit
)

// MetastoreMode controls how the SQLALCHEMY_DATABASE_URI is rendered.
type MetastoreMode int

const (
	// MetastoreNone means no metastore config rendered — user handles DB via
	// raw Python config.
	MetastoreNone MetastoreMode = iota
	// MetastorePassthrough means a full URI was provided — renders a simple
	// assignment from an operator-internal env var.
	MetastorePassthrough
	// MetastoreStructured means structured fields were set — renders f-string
	// URI from operator-internal env vars.
	MetastoreStructured
)

// ValkeyCacheInput holds resolved settings for a Flask-Caching section.
type ValkeyCacheInput struct {
	Disabled       bool
	Database       int32
	KeyPrefix      string
	DefaultTimeout int32
}

// ValkeyCeleryInput holds resolved settings for a Celery Valkey backend.
type ValkeyCeleryInput struct {
	Disabled bool
	Database int32
}

// ValkeyResultsInput holds resolved settings for the SQL Lab results backend.
type ValkeyResultsInput struct {
	Disabled  bool
	Database  int32
	KeyPrefix string
}

// ValkeyInput holds all resolved Valkey configuration for config rendering.
type ValkeyInput struct {
	SSL             bool
	SSLCertRequired string
	SSLKeyFile      string
	SSLCertFile     string
	SSLCACertFile   string

	Cache                ValkeyCacheInput
	DataCache            ValkeyCacheInput
	FilterStateCache     ValkeyCacheInput
	ExploreFormDataCache ValkeyCacheInput
	ThumbnailCache       ValkeyCacheInput
	CeleryBroker         ValkeyCeleryInput
	CeleryResultBackend  ValkeyCeleryInput
	ResultsBackend       ValkeyResultsInput
}

// ConfigInput holds the simplified config fields needed to render superset_config.py.
type ConfigInput struct {
	// Metastore mode and driver.
	MetastoreMode MetastoreMode
	// DBDriver is the database driver for structured mode (e.g. "postgresql", "mysql").
	DBDriver string

	// Valkey cache configuration. Nil when spec.valkey is not set.
	Valkey *ValkeyInput

	// Engine options for SQLALCHEMY_ENGINE_OPTIONS. Nil = do not render.
	EngineOptions *EngineOptionsInput

	// Top-level raw Python from spec.config.
	Config string

	// Per-component raw Python from component.config.
	ComponentConfig string
}

// RenderConfig generates the superset_config.py content for a given component type.
// Returns empty string for ComponentWebsocketServer (Node.js, no Python config).
func RenderConfig(componentType ComponentType, input *ConfigInput) string {
	if componentType == ComponentWebsocketServer {
		return ""
	}

	var b strings.Builder

	// [1] Imports
	b.WriteString("import os\n")
	if input.MetastoreMode == MetastoreStructured || input.Valkey != nil {
		b.WriteString("from urllib.parse import quote\n")
	}
	if input.Valkey != nil && !input.Valkey.ResultsBackend.Disabled {
		b.WriteString("from cachelib.redis import RedisCache as _CachelibRedis\n")
	}
	if input.EngineOptions != nil && input.EngineOptions.UseNullPool {
		b.WriteString("from sqlalchemy.pool import NullPool\n")
	}
	b.WriteString("\n")

	// [2] Operator-generated configs
	b.WriteString("# Operator-generated configs\n")

	// SECRET_KEY from operator-internal env var.
	fmt.Fprintf(&b, "SECRET_KEY = os.environ['%s']\n", common.EnvSecretKey)

	// Passthrough metastore: read full URI from operator-internal env var.
	if input.MetastoreMode == MetastorePassthrough {
		fmt.Fprintf(&b, "SQLALCHEMY_DATABASE_URI = os.environ['%s']\n", common.EnvDatabaseURI)
	}

	// Structured metastore: assemble URI from operator-internal env vars at Python runtime.
	// Password uses os.environ.get() to support password-less connections (trust auth, IAM).
	if input.MetastoreMode == MetastoreStructured {
		driver := driverScheme(input.DBDriver)
		fmt.Fprintf(&b, "_db_pass = os.environ.get(\"%s\", \"\")\n", common.EnvDBPass)
		fmt.Fprintf(&b, "_db_cred = f\"{quote(os.environ['%s'], safe='')}:{quote(_db_pass, safe='')}\" if _db_pass else quote(os.environ['%s'], safe='')\n",
			common.EnvDBUser, common.EnvDBUser,
		)
		fmt.Fprintf(&b,
			"SQLALCHEMY_DATABASE_URI = f\"%s://{_db_cred}@{os.environ['%s']}:{os.environ['%s']}/{quote(os.environ['%s'], safe='')}\"\n",
			driver,
			common.EnvDBHost, common.EnvDBPort, common.EnvDBName,
		)
	}

	if input.MetastoreMode != MetastoreNone {
		b.WriteString("SQLALCHEMY_EXAMPLES_URI = SQLALCHEMY_DATABASE_URI\n")
	}

	// Web server port (web server only).
	if componentType == ComponentWebServer {
		fmt.Fprintf(&b, "SUPERSET_WEBSERVER_PORT = %d\n", common.PortWebServer)
	}

	b.WriteString("\n")

	// [2.5] SQLALCHEMY_ENGINE_OPTIONS
	if input.EngineOptions != nil {
		renderEngineOptions(&b, input.EngineOptions)
	}

	// [3] Valkey cache config
	if input.Valkey != nil {
		renderValkey(&b, input.Valkey)
	}

	// [4] Base config (spec.config)
	if input.Config != "" {
		b.WriteString("# Base config (spec.config)\n")
		b.WriteString(input.Config)
		if !strings.HasSuffix(input.Config, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	// [5] Component config
	if input.ComponentConfig != "" {
		b.WriteString("# Component config\n")
		b.WriteString(input.ComponentConfig)
		if !strings.HasSuffix(input.ComponentConfig, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	return b.String()
}

// renderValkey writes the Valkey cache/broker/results Python configuration.
func renderValkey(b *strings.Builder, v *ValkeyInput) {
	b.WriteString("# Valkey cache config\n")

	// Connection helpers using operator-injected env vars.
	fmt.Fprintf(b, "_vk_pass = os.environ.get(\"%s\", \"\")\n", common.EnvValkeyPass)

	scheme := "redis"
	if v.SSL {
		scheme = "rediss"
	}
	fmt.Fprintf(b, "_vk_scheme = \"%s\"\n", scheme)
	b.WriteString("_vk_auth = f\":{quote(_vk_pass, safe='')}@\" if _vk_pass else \"\"\n")
	fmt.Fprintf(b, "_vk_base = f\"{_vk_scheme}://{_vk_auth}{os.environ['%s']}:{os.environ['%s']}\"\n",
		common.EnvValkeyHost, common.EnvValkeyPort,
	)

	// SSL options dict (used by Flask-Caching CACHE_OPTIONS, CacheLib kwargs, and Celery).
	hasSSLOpts := v.SSL && (v.SSLKeyFile != "" || v.SSLCertFile != "" || v.SSLCACertFile != "")
	if hasSSLOpts {
		certReq := v.SSLCertRequired
		if certReq == "" {
			certReq = "required"
		}
		b.WriteString("_vk_ssl_opts = {\n")
		fmt.Fprintf(b, "    \"ssl_cert_reqs\": \"%s\",\n", pyQuote(certReq))
		if v.SSLKeyFile != "" {
			fmt.Fprintf(b, "    \"ssl_keyfile\": \"%s\",\n", pyQuote(v.SSLKeyFile))
		}
		if v.SSLCertFile != "" {
			fmt.Fprintf(b, "    \"ssl_certfile\": \"%s\",\n", pyQuote(v.SSLCertFile))
		}
		if v.SSLCACertFile != "" {
			fmt.Fprintf(b, "    \"ssl_ca_certs\": \"%s\",\n", pyQuote(v.SSLCACertFile))
		}
		b.WriteString("}\n")
	}

	b.WriteString("\n")

	// Flask-Caching sections.
	type cacheSection struct {
		pythonVar string
		input     ValkeyCacheInput
	}
	sections := []cacheSection{
		{"CACHE_CONFIG", v.Cache},
		{"DATA_CACHE_CONFIG", v.DataCache},
		{"FILTER_STATE_CACHE_CONFIG", v.FilterStateCache},
		{"EXPLORE_FORM_DATA_CACHE_CONFIG", v.ExploreFormDataCache},
		{"THUMBNAIL_CACHE_CONFIG", v.ThumbnailCache},
	}
	for _, s := range sections {
		if s.input.Disabled {
			continue
		}
		fmt.Fprintf(b, "%s = {\n", s.pythonVar)
		b.WriteString("    \"CACHE_TYPE\": \"RedisCache\",\n")
		fmt.Fprintf(b, "    \"CACHE_DEFAULT_TIMEOUT\": %d,\n", s.input.DefaultTimeout)
		fmt.Fprintf(b, "    \"CACHE_KEY_PREFIX\": \"%s\",\n", pyQuote(s.input.KeyPrefix))
		fmt.Fprintf(b, "    \"CACHE_REDIS_URL\": f\"{_vk_base}/%d\",\n", s.input.Database)
		if hasSSLOpts {
			b.WriteString("    \"CACHE_OPTIONS\": _vk_ssl_opts,\n")
		}
		b.WriteString("}\n")
	}

	// Celery config.
	brokerEnabled := !v.CeleryBroker.Disabled
	resultEnabled := !v.CeleryResultBackend.Disabled
	if brokerEnabled || resultEnabled {
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
		b.WriteString("CELERY_CONFIG = CeleryConfig\n")
	}

	// Results backend (CacheLib RedisCache).
	if !v.ResultsBackend.Disabled {
		b.WriteString("\nRESULTS_BACKEND = _CachelibRedis(\n")
		fmt.Fprintf(b, "    host=os.environ[\"%s\"],\n", common.EnvValkeyHost)
		fmt.Fprintf(b, "    port=int(os.environ[\"%s\"]),\n", common.EnvValkeyPort)
		fmt.Fprintf(b, "    password=os.environ.get(\"%s\", \"\"),\n", common.EnvValkeyPass)
		fmt.Fprintf(b, "    db=%d,\n", v.ResultsBackend.Database)
		fmt.Fprintf(b, "    key_prefix=\"%s\",\n", pyQuote(v.ResultsBackend.KeyPrefix))
		if v.SSL {
			b.WriteString("    ssl=True,\n")
		}
		if hasSSLOpts {
			b.WriteString("    **_vk_ssl_opts,\n")
		}
		b.WriteString(")\n")
	}

	b.WriteString("\n")
}

// renderEngineOptions writes the SQLALCHEMY_ENGINE_OPTIONS Python configuration.
func renderEngineOptions(b *strings.Builder, opts *EngineOptionsInput) {
	b.WriteString("SQLALCHEMY_ENGINE_OPTIONS = {\n")
	if opts.UseNullPool {
		b.WriteString("    \"poolclass\": NullPool,\n")
	} else {
		fmt.Fprintf(b, "    \"pool_size\": %d,\n", opts.PoolSize)
		fmt.Fprintf(b, "    \"max_overflow\": %d,\n", opts.MaxOverflow)
		if opts.PoolRecycle > 0 {
			fmt.Fprintf(b, "    \"pool_recycle\": %d,\n", opts.PoolRecycle)
		}
		if opts.PoolPrePing {
			b.WriteString("    \"pool_pre_ping\": True,\n")
		} else {
			b.WriteString("    \"pool_pre_ping\": False,\n")
		}
		if opts.PoolTimeout > 0 {
			fmt.Fprintf(b, "    \"pool_timeout\": %d,\n", opts.PoolTimeout)
		}
	}
	b.WriteString("}\n\n")
}

// pyQuote returns s escaped for use inside a Python double-quoted string literal.
// It strips the surrounding quotes from strconv.Quote so callers embed the result
// in their own quoting context: fmt.Fprintf(b, `"CACHE_KEY_PREFIX": "%s"`, pyQuote(s)).
func pyQuote(s string) string {
	q := strconv.Quote(s)
	return q[1 : len(q)-1]
}

// driverScheme returns the SQLAlchemy connection scheme for a given driver type.
func driverScheme(dbType string) string {
	switch dbType {
	case "mysql":
		return "mysql+mysqlconnector"
	default:
		return "postgresql+psycopg2"
	}
}
