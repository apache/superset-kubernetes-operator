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

func ptr[T any](v T) *T { return &v }

func TestResolveGunicorn_NilSpec(t *testing.T) {
	g := ResolveGunicorn(nil)
	assert.False(t, g.Disabled)
	assert.Equal(t, int32(2), g.Workers)
	assert.Equal(t, int32(8), g.Threads)
	assert.Equal(t, "gthread", g.WorkerClass)
	assert.Equal(t, int32(60), g.Timeout)
	assert.Equal(t, int32(2), g.KeepAlive)
	assert.Equal(t, int32(0), g.MaxRequests)
	assert.Equal(t, "info", g.LogLevel)
}

func TestResolveGunicorn_Presets(t *testing.T) {
	tests := []struct {
		preset  string
		workers int32
		threads int32
	}{
		{PresetConservative, 1, 4},
		{PresetBalanced, 2, 8},
		{PresetPerformance, 4, 8},
		{PresetAggressive, 8, 16},
	}
	for _, tt := range tests {
		t.Run(tt.preset, func(t *testing.T) {
			g := ResolveGunicorn(&v1alpha1.GunicornSpec{Preset: &tt.preset})
			assert.Equal(t, tt.workers, g.Workers)
			assert.Equal(t, tt.threads, g.Threads)
			assert.Equal(t, "gthread", g.WorkerClass)
		})
	}
}

func TestResolveGunicorn_Disabled(t *testing.T) {
	g := ResolveGunicorn(&v1alpha1.GunicornSpec{Preset: ptr(PresetDisabled)})
	assert.True(t, g.Disabled)
}

func TestResolveGunicorn_FieldOverrides(t *testing.T) {
	g := ResolveGunicorn(&v1alpha1.GunicornSpec{
		Preset:  ptr(PresetConservative),
		Workers: ptr(int32(6)),
		Timeout: ptr(int32(120)),
	})
	assert.Equal(t, int32(6), g.Workers)
	assert.Equal(t, int32(4), g.Threads) // preset default preserved
	assert.Equal(t, int32(120), g.Timeout)
}

func TestResolveGunicorn_EnvVars(t *testing.T) {
	g := ResolveGunicorn(nil)
	envs := g.EnvVars()
	assert.Len(t, envs, 10)

	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.Name] = e.Value
	}
	assert.Equal(t, "2", envMap[EnvServerWorkerAmount])
	assert.Equal(t, "8", envMap[EnvServerThreadsAmount])
	assert.Equal(t, "gthread", envMap[EnvServerWorkerClass])
	assert.Equal(t, "60", envMap[EnvGunicornTimeout])
	assert.Equal(t, "info", envMap[EnvGunicornLogLevel])
}
