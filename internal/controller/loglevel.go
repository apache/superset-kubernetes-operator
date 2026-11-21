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
	"strings"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// lifecycleLogLevel resolves the level shared by all lifecycle tasks.
func lifecycleLogLevel(spec *supersetv1alpha1.SupersetSpec) supersetv1alpha1.LogLevel {
	var lifecycle *supersetv1alpha1.LogLevel
	if spec.Lifecycle != nil {
		lifecycle = spec.Lifecycle.LogLevel
	}
	return supersetconfig.ResolveLogLevel(lifecycle, spec.LogLevel)
}

// appendLogLevelEnv injects the env vars that carry a component's resolved
// level to loggers that don't follow Superset's LOG_LEVEL. Only Flower needs
// one. At startup, Flower calls Tornado's enable_pretty_logging without a
// logger argument, which sets the root logger to Tornado's logging option.
// That overrides the rendered LOG_LEVEL, just as Celery's --loglevel does
// for the worker and Beat, so Flower gets the level as FLOWER_LOGGING.
// Server-process loggers (Gunicorn, websocket, FastMCP) keep their own
// native settings and are not driven by logLevel.
func appendLogLevelEnv(componentType naming.ComponentType, level supersetv1alpha1.LogLevel, injected *resolution.OperatorInjected) {
	if componentType == naming.ComponentCeleryFlower {
		injected.Env = append(injected.Env, corev1.EnvVar{Name: supersetconfig.EnvCeleryFlowerLogLevel, Value: supersetconfig.PythonLogLevel(level)})
	}
}

// withCeleryDefaultLogLevel adds --loglevel to the fixed default commands of
// the Celery worker (used with preset disabled) and Celery Beat. A user-set
// command replaces the default entirely and must pass --loglevel itself.
func withCeleryDefaultLogLevel(componentType naming.ComponentType, command []string, level supersetv1alpha1.LogLevel) []string {
	if componentType != naming.ComponentCeleryWorker && componentType != naming.ComponentCeleryBeat {
		return command
	}
	return append(command, supersetconfig.CeleryLogLevelFlag(level))
}

// debugLoggingTargets returns the names of the workloads that run at debug
// It resolves the level the same way the workloads do (component logLevel,
// then spec.logLevel, then info) and
// checks each enabled Python component, by component type.
// The websocket server is skipped because logLevel doesn't apply to it.
// It also checks lifecycle tasks, unless lifecycle is disabled.
func debugLoggingTargets(superset *supersetv1alpha1.Superset) []string {
	var targets []string
	for _, desc := range componentDescriptors {
		if !desc.hasPythonConfig {
			continue
		}
		accessor := desc.extract(&superset.Spec)
		if accessor == nil {
			continue
		}
		if supersetconfig.ResolveLogLevel(accessor.logLevel, superset.Spec.LogLevel) == supersetv1alpha1.LogLevelDebug {
			targets = append(targets, string(desc.componentType))
		}
	}
	if !isLifecycleDisabled(superset) && lifecycleLogLevel(&superset.Spec) == supersetv1alpha1.LogLevelDebug {
		targets = append(targets, "lifecycle")
	}
	return targets
}

// reconcileDebugLoggingCondition sets the DebugLogging condition while any
// app log level is debug outside Development, and removes it otherwise.
// It reflects spec state only and doesn't affect Ready or Available.
// The Warning Event is emitted only when the condition appears
func (r *SupersetReconciler) reconcileDebugLoggingCondition(superset, origSuperset *supersetv1alpha1.Superset) {
	var targets []string
	if !isDevEnvironment(&superset.Spec) {
		targets = debugLoggingTargets(superset)
	}
	if len(targets) == 0 {
		removeCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeDebugLogging)
		return
	}

	message := "Log level is debug outside Development for: " + strings.Join(targets, ", ")
	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeDebugLogging,
		metav1.ConditionTrue, "DebugLogLevel", message, superset.Generation)
	if !apimeta.IsStatusConditionTrue(origSuperset.Status.Conditions, supersetv1alpha1.ConditionTypeDebugLogging) {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeWarning, "DebugLogging", "Reconcile", "%s", message)
	}
}
