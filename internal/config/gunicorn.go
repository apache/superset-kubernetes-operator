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

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

const (
	PresetDisabled     = "disabled"
	PresetConservative = "conservative"
	PresetBalanced     = "balanced"
	PresetPerformance  = "performance"
	PresetAggressive   = "aggressive"

	defaultWorkerClass = "gthread"

	EnvServerWorkerAmount        = "SERVER_WORKER_AMOUNT"
	EnvServerThreadsAmount       = "SERVER_THREADS_AMOUNT"
	EnvServerWorkerClass         = "SERVER_WORKER_CLASS"
	EnvGunicornTimeout           = "GUNICORN_TIMEOUT"
	EnvGunicornKeepAlive         = "GUNICORN_KEEPALIVE"
	EnvWorkerMaxRequests         = "WORKER_MAX_REQUESTS"
	EnvWorkerMaxRequestsJitter   = "WORKER_MAX_REQUESTS_JITTER"
	EnvServerLimitRequestLine    = "SERVER_LIMIT_REQUEST_LINE"
	EnvServerLimitRequestFieldSz = "SERVER_LIMIT_REQUEST_FIELD_SIZE"
	EnvGunicornLogLevel          = "GUNICORN_LOGLEVEL"
)

// ResolvedGunicorn holds fully-resolved Gunicorn parameters.
type ResolvedGunicorn struct {
	Disabled              bool
	Workers               int32
	Threads               int32
	WorkerClass           string
	Timeout               int32
	KeepAlive             int32
	MaxRequests           int32
	MaxRequestsJitter     int32
	LimitRequestLine      int32
	LimitRequestFieldSize int32
	LogLevel              string
}

// ResolveGunicorn resolves a GunicornSpec into concrete values.
// When spec is nil, balanced defaults are used.
func ResolveGunicorn(spec *v1alpha1.GunicornSpec) ResolvedGunicorn {
	preset := PresetBalanced
	if spec != nil && spec.Preset != nil {
		preset = *spec.Preset
	}
	if preset == PresetDisabled {
		return ResolvedGunicorn{Disabled: true}
	}

	workers, threads := gunicornPresetDefaults(preset)
	r := ResolvedGunicorn{
		Workers:               workers,
		Threads:               threads,
		WorkerClass:           defaultWorkerClass,
		Timeout:               60,
		KeepAlive:             2,
		MaxRequests:           0,
		MaxRequestsJitter:     0,
		LimitRequestLine:      0,
		LimitRequestFieldSize: 0,
		LogLevel:              "info",
	}

	if spec == nil {
		return r
	}

	if spec.Workers != nil {
		r.Workers = *spec.Workers
	}
	if spec.Threads != nil {
		r.Threads = *spec.Threads
	}
	if spec.WorkerClass != nil {
		r.WorkerClass = *spec.WorkerClass
	}
	if spec.Timeout != nil {
		r.Timeout = *spec.Timeout
	}
	if spec.KeepAlive != nil {
		r.KeepAlive = *spec.KeepAlive
	}
	if spec.MaxRequests != nil {
		r.MaxRequests = *spec.MaxRequests
	}
	if spec.MaxRequestsJitter != nil {
		r.MaxRequestsJitter = *spec.MaxRequestsJitter
	}
	if spec.LimitRequestLine != nil {
		r.LimitRequestLine = *spec.LimitRequestLine
	}
	if spec.LimitRequestFieldSize != nil {
		r.LimitRequestFieldSize = *spec.LimitRequestFieldSize
	}
	if spec.LogLevel != nil {
		r.LogLevel = *spec.LogLevel
	}

	return r
}

func gunicornPresetDefaults(preset string) (workers, threads int32) {
	switch preset {
	case PresetConservative:
		return 1, 4
	case PresetPerformance:
		return 4, 8
	case PresetAggressive:
		return 8, 16
	default:
		return 2, 8
	}
}

// EnvVars returns the Gunicorn env vars for injection into the web server container.
func (g *ResolvedGunicorn) EnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: EnvServerWorkerAmount, Value: fmt.Sprintf("%d", g.Workers)},
		{Name: EnvServerThreadsAmount, Value: fmt.Sprintf("%d", g.Threads)},
		{Name: EnvServerWorkerClass, Value: g.WorkerClass},
		{Name: EnvGunicornTimeout, Value: fmt.Sprintf("%d", g.Timeout)},
		{Name: EnvGunicornKeepAlive, Value: fmt.Sprintf("%d", g.KeepAlive)},
		{Name: EnvWorkerMaxRequests, Value: fmt.Sprintf("%d", g.MaxRequests)},
		{Name: EnvWorkerMaxRequestsJitter, Value: fmt.Sprintf("%d", g.MaxRequestsJitter)},
		{Name: EnvServerLimitRequestLine, Value: fmt.Sprintf("%d", g.LimitRequestLine)},
		{Name: EnvServerLimitRequestFieldSz, Value: fmt.Sprintf("%d", g.LimitRequestFieldSize)},
		{Name: EnvGunicornLogLevel, Value: g.LogLevel},
	}
}
