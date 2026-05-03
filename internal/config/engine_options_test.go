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
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestComputeEngineOptions_DisabledPreset(t *testing.T) {
	spec := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetDisabled)}
	result := ComputeEngineOptions(common.ComponentWebServer, spec, nil, 2, 8)
	assert.Nil(t, result)
}

func TestComputeEngineOptions_DisabledPerComponent(t *testing.T) {
	topLevel := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetBalanced)}
	perComp := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetDisabled)}
	result := ComputeEngineOptions(common.ComponentWebServer, topLevel, perComp, 2, 8)
	assert.Nil(t, result)
}

func TestComputeEngineOptions_NilSpecsBalancedDefault(t *testing.T) {
	result := ComputeEngineOptions(common.ComponentWebServer, nil, nil, 2, 8)
	assert.NotNil(t, result)
	assert.False(t, result.UseNullPool)
	assert.Equal(t, int32(1), result.PoolSize)
	assert.Equal(t, int32(-1), result.MaxOverflow)
	assert.Equal(t, int32(3600), result.PoolRecycle)
	assert.False(t, result.PoolPrePing)
}

func TestComputeEngineOptions_ConservativeNullPool(t *testing.T) {
	spec := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetConservative)}
	result := ComputeEngineOptions(common.ComponentWebServer, spec, nil, 2, 8)
	assert.True(t, result.UseNullPool)
}

func TestComputeEngineOptions_PerformanceWebServer(t *testing.T) {
	spec := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetPerformance)}
	result := ComputeEngineOptions(common.ComponentWebServer, spec, nil, 4, 8)
	assert.Equal(t, int32(4), result.PoolSize) // workers
	assert.Equal(t, int32(-1), result.MaxOverflow)
}

func TestComputeEngineOptions_AggressiveWebServer(t *testing.T) {
	spec := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetAggressive)}
	result := ComputeEngineOptions(common.ComponentWebServer, spec, nil, 8, 16)
	assert.Equal(t, int32(128), result.PoolSize) // workers × threads
}

func TestComputeEngineOptions_PerformanceCeleryWorker(t *testing.T) {
	spec := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetPerformance)}
	result := ComputeEngineOptions(common.ComponentCeleryWorker, spec, nil, 8, 0)
	assert.Equal(t, int32(8), result.PoolSize) // concurrency
}

func TestComputeEngineOptions_CeleryBeatAlwaysNullPool(t *testing.T) {
	tests := []string{PresetBalanced, PresetPerformance, PresetAggressive}
	for _, preset := range tests {
		t.Run(preset, func(t *testing.T) {
			spec := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(preset)}
			result := ComputeEngineOptions(common.ComponentCeleryBeat, spec, nil, 0, 0)
			assert.True(t, result.UseNullPool)
		})
	}
}

func TestComputeEngineOptions_InitAlwaysNullPool(t *testing.T) {
	result := ComputeEngineOptions(common.ComponentInit, nil, nil, 0, 0)
	assert.True(t, result.UseNullPool)
}

func TestComputeEngineOptions_McpServerPoolSizes(t *testing.T) {
	tests := []struct {
		preset   string
		poolSize int32
	}{
		{PresetBalanced, 5},
		{PresetPerformance, 10},
		{PresetAggressive, 20},
	}
	for _, tt := range tests {
		t.Run(tt.preset, func(t *testing.T) {
			spec := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(tt.preset)}
			result := ComputeEngineOptions(common.ComponentMcpServer, spec, nil, 0, 0)
			assert.Equal(t, tt.poolSize, result.PoolSize)
		})
	}
}

func TestComputeEngineOptions_ExplicitOverrides(t *testing.T) {
	spec := &v1alpha1.SQLAlchemyEngineOptionsSpec{
		Preset:      ptr(PresetBalanced),
		PoolSize:    ptr(int32(10)),
		PoolRecycle: ptr(int32(1800)),
		PoolPrePing: ptr(true),
	}
	result := ComputeEngineOptions(common.ComponentWebServer, spec, nil, 2, 8)
	assert.Equal(t, int32(10), result.PoolSize)
	assert.Equal(t, int32(1800), result.PoolRecycle)
	assert.True(t, result.PoolPrePing)
	assert.Equal(t, int32(-1), result.MaxOverflow) // default
}

func TestComputeEngineOptions_PerComponentOverridesTopLevel(t *testing.T) {
	topLevel := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetConservative)}
	perComp := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetPerformance)}
	result := ComputeEngineOptions(common.ComponentWebServer, topLevel, perComp, 4, 8)
	assert.False(t, result.UseNullPool)
	assert.Equal(t, int32(4), result.PoolSize) // performance: workers
}

func TestFullPipeline_WebServerPerformance(t *testing.T) {
	g := ResolveGunicorn(&v1alpha1.GunicornSpec{Preset: ptr(PresetPerformance)})
	sqla := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetAggressive)}
	opts := ComputeEngineOptions(common.ComponentWebServer, sqla, nil, g.Workers, g.Threads)

	assert.Equal(t, int32(4), g.Workers)
	assert.Equal(t, int32(8), g.Threads)
	assert.Equal(t, int32(32), opts.PoolSize) // aggressive: 4 × 8
	assert.Equal(t, int32(-1), opts.MaxOverflow)

	input := &ConfigInput{
		MetastoreMode: MetastorePassthrough,
		EngineOptions: opts,
	}
	rendered := RenderConfig(ComponentWebServer, input)
	assert.Contains(t, rendered, `"pool_size": 32,`)
	assert.Contains(t, rendered, `"max_overflow": -1,`)
	assert.Contains(t, rendered, `"pool_recycle": 3600,`)
}

func TestFullPipeline_CeleryWorkerBalanced(t *testing.T) {
	c := ResolveCelery(nil)
	opts := ComputeEngineOptions(common.ComponentCeleryWorker, nil, nil, c.Concurrency, 0)

	assert.Equal(t, int32(4), c.Concurrency)
	assert.Equal(t, int32(1), opts.PoolSize) // balanced: 1

	cmd := c.Command()
	assert.Contains(t, cmd, "-c")
	assert.Contains(t, cmd, "4")

	input := &ConfigInput{
		MetastoreMode: MetastorePassthrough,
		EngineOptions: opts,
	}
	rendered := RenderConfig(ComponentCeleryWorker, input)
	assert.Contains(t, rendered, `"pool_size": 1,`)
}

func TestFullPipeline_CeleryBeatAlwaysNullPool(t *testing.T) {
	sqla := &v1alpha1.SQLAlchemyEngineOptionsSpec{Preset: ptr(PresetAggressive)}
	opts := ComputeEngineOptions(common.ComponentCeleryBeat, sqla, nil, 0, 0)

	assert.True(t, opts.UseNullPool)

	input := &ConfigInput{
		MetastoreMode: MetastorePassthrough,
		EngineOptions: opts,
	}
	rendered := RenderConfig(ComponentCeleryBeat, input)
	assert.Contains(t, rendered, "from sqlalchemy.pool import NullPool")
	assert.Contains(t, rendered, `"poolclass": NullPool,`)
}
