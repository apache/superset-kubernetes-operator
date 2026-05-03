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
	v1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// EngineOptionsInput holds resolved SQLALCHEMY_ENGINE_OPTIONS for rendering.
// A nil value from ComputeEngineOptions means the section should not be rendered.
type EngineOptionsInput struct {
	UseNullPool bool
	PoolSize    int32
	MaxOverflow int32
	PoolRecycle int32
	PoolPrePing bool
	PoolTimeout int32
}

// ComputeEngineOptions computes the SQLALCHEMY_ENGINE_OPTIONS for a given component.
// Returns nil when the effective preset is "disabled".
//
// The effective spec is resolved as: perComponent > topLevel > nil (balanced default).
// Workers and threads come from the resolved gunicorn/celery configuration and are
// used to compute pool sizes for the performance and aggressive presets.
func ComputeEngineOptions(
	componentType common.ComponentType,
	topLevel *v1alpha1.SQLAlchemyEngineOptionsSpec,
	perComponent *v1alpha1.SQLAlchemyEngineOptionsSpec,
	workers, threads int32,
) *EngineOptionsInput {
	effective := topLevel
	if perComponent != nil {
		effective = perComponent
	}

	preset := PresetBalanced
	if effective != nil && effective.Preset != nil {
		preset = *effective.Preset
	}

	if preset == PresetDisabled {
		return nil
	}

	if alwaysNullPool(componentType) && preset != PresetConservative {
		preset = PresetConservative
	}

	if preset == PresetConservative {
		result := &EngineOptionsInput{UseNullPool: true}
		applyExplicitOverrides(result, effective)
		return result
	}

	poolSize := computePoolSize(componentType, preset, workers, threads)
	result := &EngineOptionsInput{
		PoolSize:    poolSize,
		MaxOverflow: -1,
		PoolRecycle: 3600,
	}
	applyExplicitOverrides(result, effective)
	return result
}

func alwaysNullPool(ct common.ComponentType) bool {
	switch ct {
	case common.ComponentCeleryBeat, common.ComponentInit:
		return true
	default:
		return false
	}
}

func computePoolSize(ct common.ComponentType, preset string, workers, threads int32) int32 {
	switch ct {
	case common.ComponentWebServer:
		switch preset {
		case PresetPerformance:
			return workers
		case PresetAggressive:
			return workers * threads
		default:
			return 1
		}

	case common.ComponentCeleryWorker:
		switch preset {
		case PresetPerformance, PresetAggressive:
			return workers // workers = celery concurrency for celery components
		default:
			return 1
		}

	case common.ComponentMcpServer:
		switch preset {
		case PresetPerformance:
			return 10
		case PresetAggressive:
			return 20
		default:
			return 5
		}

	default:
		return 1
	}
}

func applyExplicitOverrides(result *EngineOptionsInput, spec *v1alpha1.SQLAlchemyEngineOptionsSpec) {
	if spec == nil {
		return
	}
	if spec.PoolSize != nil {
		result.PoolSize = *spec.PoolSize
	}
	if spec.MaxOverflow != nil {
		result.MaxOverflow = *spec.MaxOverflow
	}
	if spec.PoolRecycle != nil {
		result.PoolRecycle = *spec.PoolRecycle
	}
	if spec.PoolPrePing != nil {
		result.PoolPrePing = *spec.PoolPrePing
	}
	if spec.PoolTimeout != nil {
		result.PoolTimeout = *spec.PoolTimeout
	}
}
