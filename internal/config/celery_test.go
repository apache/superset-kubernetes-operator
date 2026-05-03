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

func TestResolveCelery_NilSpec(t *testing.T) {
	c := ResolveCelery(nil)
	assert.False(t, c.Disabled)
	assert.Equal(t, int32(4), c.Concurrency)
	assert.Equal(t, "prefork", c.Pool)
	assert.Equal(t, "fair", c.Optimization)
	assert.Equal(t, int32(0), c.MaxTasksPerChild)
	assert.Equal(t, int32(0), c.MaxMemoryPerChild)
	assert.Equal(t, int32(4), c.PrefetchMultiplier)
	assert.Equal(t, int32(0), c.SoftTimeLimit)
	assert.Equal(t, int32(0), c.TimeLimit)
}

func TestResolveCelery_Presets(t *testing.T) {
	tests := []struct {
		preset      string
		concurrency int32
	}{
		{PresetConservative, 2},
		{PresetBalanced, 4},
		{PresetPerformance, 8},
		{PresetAggressive, 16},
	}
	for _, tt := range tests {
		t.Run(tt.preset, func(t *testing.T) {
			c := ResolveCelery(&v1alpha1.CeleryWorkerProcessSpec{Preset: &tt.preset})
			assert.Equal(t, tt.concurrency, c.Concurrency)
			assert.Equal(t, "prefork", c.Pool)
		})
	}
}

func TestResolveCelery_Disabled(t *testing.T) {
	c := ResolveCelery(&v1alpha1.CeleryWorkerProcessSpec{Preset: ptr(PresetDisabled)})
	assert.True(t, c.Disabled)
}

func TestResolveCelery_FieldOverrides(t *testing.T) {
	c := ResolveCelery(&v1alpha1.CeleryWorkerProcessSpec{
		Preset:      ptr(PresetConservative),
		Concurrency: ptr(int32(12)),
		Pool:        ptr("gevent"),
	})
	assert.Equal(t, int32(12), c.Concurrency)
	assert.Equal(t, "gevent", c.Pool)
	assert.Equal(t, "fair", c.Optimization)
}

func TestResolveCelery_Command(t *testing.T) {
	c := ResolveCelery(nil)
	cmd := c.Command()
	assert.Equal(t, []string{
		"celery", "--app=superset.tasks.celery_app:app", "worker",
		"--pool=prefork", "-O", "fair", "-c", "4",
		"--prefetch-multiplier=4",
	}, cmd)
}

func TestResolveCelery_CommandWithOptionalFlags(t *testing.T) {
	c := ResolveCelery(&v1alpha1.CeleryWorkerProcessSpec{
		MaxTasksPerChild:  ptr(int32(100)),
		MaxMemoryPerChild: ptr(int32(200000)),
		SoftTimeLimit:     ptr(int32(300)),
		TimeLimit:         ptr(int32(600)),
	})
	cmd := c.Command()
	assert.Contains(t, cmd, "--max-tasks-per-child=100")
	assert.Contains(t, cmd, "--max-memory-per-child=200000")
	assert.Contains(t, cmd, "--soft-time-limit=300")
	assert.Contains(t, cmd, "--time-limit=600")
}
