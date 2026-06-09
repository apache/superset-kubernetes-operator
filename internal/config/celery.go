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
	"fmt"

	v1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

const defaultPool = "prefork"

// ResolvedCelery holds fully-resolved Celery worker parameters.
type ResolvedCelery struct {
	Disabled           bool
	Concurrency        int32
	Pool               string
	Optimization       string
	MaxTasksPerChild   int32
	MaxMemoryPerChild  int32
	PrefetchMultiplier int32
	SoftTimeLimit      int32
	TimeLimit          int32
}

// ResolveCelery resolves a CeleryWorkerProcessSpec into concrete values.
// When spec is nil, balanced defaults are used.
func ResolveCelery(spec *v1alpha1.CeleryWorkerProcessSpec) ResolvedCelery {
	preset := PresetBalanced
	if spec != nil && spec.Preset != nil {
		preset = *spec.Preset
	}
	if preset == PresetDisabled {
		return ResolvedCelery{Disabled: true}
	}

	concurrency := celeryPresetDefaults(preset)
	r := ResolvedCelery{
		Concurrency:        concurrency,
		Pool:               defaultPool,
		Optimization:       "fair",
		MaxTasksPerChild:   0,
		MaxMemoryPerChild:  0,
		PrefetchMultiplier: 4,
		SoftTimeLimit:      0,
		TimeLimit:          0,
	}

	if spec == nil {
		return r
	}

	setIf(&r.Concurrency, spec.Concurrency)
	setIf(&r.Pool, spec.Pool)
	setIf(&r.Optimization, spec.Optimization)
	setIf(&r.MaxTasksPerChild, spec.MaxTasksPerChild)
	setIf(&r.MaxMemoryPerChild, spec.MaxMemoryPerChild)
	setIf(&r.PrefetchMultiplier, spec.PrefetchMultiplier)
	setIf(&r.SoftTimeLimit, spec.SoftTimeLimit)
	setIf(&r.TimeLimit, spec.TimeLimit)

	return r
}

func celeryPresetDefaults(preset string) int32 {
	switch preset {
	case PresetConservative:
		return 2
	case PresetPerformance:
		return 8
	case PresetAggressive:
		return 16
	default:
		return 4
	}
}

// Command returns the celery worker command args.
func (c *ResolvedCelery) Command() []string {
	cmd := []string{
		"celery", "--app=superset.tasks.celery_app:app", "worker",
		fmt.Sprintf("--pool=%s", c.Pool),
		"-O", c.Optimization,
		"-c", fmt.Sprintf("%d", c.Concurrency),
	}
	cmd = appendIntFlag(cmd, "--max-tasks-per-child=%d", c.MaxTasksPerChild)
	cmd = appendIntFlag(cmd, "--max-memory-per-child=%d", c.MaxMemoryPerChild)
	cmd = appendIntFlag(cmd, "--prefetch-multiplier=%d", c.PrefetchMultiplier)
	cmd = appendIntFlag(cmd, "--soft-time-limit=%d", c.SoftTimeLimit)
	cmd = appendIntFlag(cmd, "--time-limit=%d", c.TimeLimit)
	return cmd
}

// appendIntFlag appends a formatted flag to cmd only when val is positive.
func appendIntFlag(cmd []string, format string, val int32) []string {
	if val > 0 {
		cmd = append(cmd, fmt.Sprintf(format, val))
	}
	return cmd
}
