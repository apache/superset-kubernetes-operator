/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package config

import (
	"strconv"
	"strings"
	"testing"
)

// FuzzPyQuote checks that pyQuote produces a safe Python double-quoted string
// body for any input. Config values such as cache key prefixes flow through it
// into generated superset_config.py, so the escaped result must never break out
// of its quoting context. See docs/contributing/development-guidelines.md (Fuzzing).
func FuzzPyQuote(f *testing.F) {
	for _, s := range []string{
		"",
		"simple",
		`has "double" quotes`,
		`has \ backslash`,
		"has\nnewline\ttab",
		"null\x00byte",
		"unicode: café 日本語 🚀",
		"'single'",
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		got := pyQuote(s)

		// The body must stay on a single line: a raw newline or carriage return
		// would split the generated assignment and produce invalid Python.
		if strings.ContainsAny(got, "\n\r") {
			t.Fatalf("pyQuote(%q) = %q contains a raw newline/CR", s, got)
		}

		// Wrapping the body back in double quotes must round-trip to the original
		// string, proving it is a well-formed escaped string body (no unescaped
		// delimiter, valid escape sequences).
		unquoted, err := strconv.Unquote(`"` + got + `"`)
		if err != nil {
			t.Fatalf("pyQuote(%q) = %q is not a valid quoted body: %v", s, got, err)
		}
		if unquoted != s {
			t.Fatalf("pyQuote round-trip mismatch: got %q, want %q", unquoted, s)
		}
	})
}

// renderConfigComponents are the component types RenderConfig knows how to
// render config for. WebsocketServer is included to cover its empty-string path.
var renderConfigComponents = []ComponentType{
	ComponentWebServer,
	ComponentCeleryWorker,
	ComponentCeleryBeat,
	ComponentCeleryFlower,
	ComponentWebsocketServer,
	ComponentMcpServer,
	ComponentInit,
}

// FuzzRenderConfig exercises RenderConfig across every component type with
// arbitrary user-controlled string fields. It asserts that rendering never
// panics and is deterministic (identical input yields byte-identical output).
func FuzzRenderConfig(f *testing.F) {
	f.Add("FEATURE_FLAGS = {}", "WEBSERVER_THREADS = 16", "PostgreSQL", "psycopg2", "ALERT_REPORTS", true, "superset_", uint8(2), true, int32(8088))
	f.Add("", "", "MySQL", "", "", false, "", uint8(0), false, int32(0))
	f.Add(`x = "quoted \n value"`, "", "MySQL", "mysqldb", "DASHBOARD_RBAC", false, `pre"fix`, uint8(1), true, int32(-1))

	f.Fuzz(func(t *testing.T,
		config, componentConfig, dbType, dbDriver, flagKey string,
		flagVal bool, keyPrefix string, modeSel uint8, withValkey bool, port int32,
	) {
		input := &ConfigInput{
			MetastoreMode:   MetastoreMode(int(modeSel) % 3),
			DBType:          dbType,
			DBDriver:        dbDriver,
			Config:          config,
			ComponentConfig: componentConfig,
			WebServerPort:   port,
		}
		if flagKey != "" {
			input.FeatureFlags = map[string]bool{flagKey: flagVal}
		}
		if withValkey {
			input.Valkey = &ValkeyInput{
				Cache:          ValkeyCacheInput{KeyPrefix: keyPrefix},
				DataCache:      ValkeyCacheInput{KeyPrefix: keyPrefix},
				ResultsBackend: ValkeyResultsInput{KeyPrefix: keyPrefix},
			}
		}

		for _, ct := range renderConfigComponents {
			first := RenderConfig(ct, input)
			if second := RenderConfig(ct, input); first != second {
				t.Fatalf("RenderConfig(%q) is non-deterministic:\nfirst:\n%s\nsecond:\n%s", ct, first, second)
			}
		}
	})
}
