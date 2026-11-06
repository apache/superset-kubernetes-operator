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

package controller

import (
	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// lifecycleLogLevel resolves the level shared by all lifecycle tasks.
func lifecycleLogLevel(spec *supersetv1alpha1.SupersetSpec) string {
	var lifecycle *supersetv1alpha1.LogLevel
	if spec.Lifecycle != nil {
		lifecycle = spec.Lifecycle.LogLevel
	}
	return supersetconfig.ResolveLogLevel(lifecycle, spec.LogLevel)
}

// appendLogLevelEnv injects the env vars that carry a component's resolved
// level to loggers that don't read Superset's LOG_LEVEL.
func appendLogLevelEnv(componentType naming.ComponentType, level string, injected *resolution.OperatorInjected) {
	if componentType == naming.ComponentWebServer {
		// Separate from the Gunicorn tuning presets, so injected even with preset disabled.
		injected.Env = append(injected.Env, corev1.EnvVar{Name: supersetconfig.EnvGunicornLogLevel, Value: level})
	}
}

// withCeleryDefaultLogLevel adds --loglevel to the fixed default commands of
// the Celery worker (used with preset disabled) and Celery Beat. A user-set
// command replaces the default entirely and must pass --loglevel itself.
func withCeleryDefaultLogLevel(componentType naming.ComponentType, command []string, level string) []string {
	if componentType != naming.ComponentCeleryWorker && componentType != naming.ComponentCeleryBeat {
		return command
	}
	return append(command, supersetconfig.CeleryLogLevelFlag(level))
}
