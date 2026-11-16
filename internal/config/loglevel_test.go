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
	"testing"

	"github.com/stretchr/testify/assert"

	v1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

func TestLogLevelResolutionAndMappings(t *testing.T) {
	t.Run("resolution precedence", func(t *testing.T) {
		component := v1alpha1.LogLevelDebug
		topLevel := v1alpha1.LogLevelWarning
		tests := []struct {
			name      string
			component *v1alpha1.LogLevel
			topLevel  *v1alpha1.LogLevel
			want      v1alpha1.LogLevel
			explicit  bool
		}{
			{"default", nil, nil, v1alpha1.LogLevelInfo, false},
			{"top-level", nil, &topLevel, v1alpha1.LogLevelWarning, true},
			{"component overrides top-level", &component, &topLevel, v1alpha1.LogLevelDebug, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, tt.want, ResolveLogLevel(tt.component, tt.topLevel))
				assert.Equal(t, tt.explicit, IsLogLevelExplicit(tt.component, tt.topLevel))
			})
		}
	})

	tests := map[v1alpha1.LogLevel]struct {
		python       string
		celery       string
		winston      string
		celeryFlower string
	}{
		v1alpha1.LogLevelDebug:    {"DEBUG", "--loglevel=debug", "debug", "DEBUG"},
		v1alpha1.LogLevelInfo:     {"INFO", "--loglevel=info", "info", "INFO"},
		v1alpha1.LogLevelWarning:  {"WARNING", "--loglevel=warning", "warn", "WARNING"},
		v1alpha1.LogLevelError:    {"ERROR", "--loglevel=error", "error", "ERROR"},
		v1alpha1.LogLevelCritical: {"CRITICAL", "--loglevel=critical", "error", "ERROR"},
	}
	for in, want := range tests {
		t.Run(string(in), func(t *testing.T) {
			assert.Equal(t, want.python, PythonLogLevel(in))
			assert.Equal(t, want.celery, CeleryLogLevelFlag(in))
			assert.Equal(t, want.winston, WinstonLogLevel(in))
			assert.Equal(t, want.celeryFlower, CeleryFlowerLogLevel(in))
		})
	}
}
