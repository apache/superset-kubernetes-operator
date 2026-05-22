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
)

// initInputs returns the inputs that contribute to init's task checksum.
//
// Init re-runs whenever the lifecycle Job's mounted artifacts change:
//   - Image: a different binary may produce different init results
//   - ConfigHash: rendered superset_config.py — covers featureFlags, celery,
//     lifecycle.config, lifecycle sqlaEngineOptions, valkey cache layout, etc.
//   - EnvHash: spec-derived env vars injected into the Job pod — covers
//     secret/metastore/valkey refs and lifecycle admin user credentials.
//     These are env-only because the rendered Python references env var
//     *names*, not values; without this hash, changing spec.metastore.host
//     would silently leave init pointed at the wrong database.
//   - Trigger: the manual `lifecycle.init.trigger` opaque string.
//
// Hashing the rendered config and resolved env vars directly — rather than a
// hand-curated list of spec fields — keeps the checksum honest as new
// config-rendering or env-injection fields are added.
func (r *SupersetReconciler) initInputs(superset *supersetv1alpha1.Superset) any {
	trigger := ""
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Init != nil {
		trigger = derefOrDefault(superset.Spec.Lifecycle.Init.Trigger, "")
	}
	return struct {
		Image      string
		ConfigHash string
		EnvHash    string
		Trigger    string
	}{
		Image:      resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset)),
		ConfigHash: computeChecksum(renderLifecycleTaskConfig(superset)),
		EnvHash:    computeChecksum(initTaskEnv(superset)),
		Trigger:    trigger,
	}
}

// initTaskEnv returns the spec-derived env vars injected into the init task
// pod. The slice mirrors what buildStandardTaskFlatSpec assembles via
// buildOperatorInjected for taskType == taskTypeInit. forceReload is
// intentionally excluded — it is a per-component rollout knob, not a lifecycle
// re-run signal, and the prior configChecksum-based behavior also did not
// re-run init on forceReload changes.
func initTaskEnv(superset *supersetv1alpha1.Superset) []corev1.EnvVar {
	env := collectSecretEnvVars(&superset.Spec, superset.Name)
	return append(env, collectLifecycleInitEnvVars(superset.Spec.Lifecycle)...)
}

// defaultInitCommand returns the user override, or the standard
// `superset init` flow (with admin-user / load-examples appended in dev mode
// via buildInitCommand).
func defaultInitCommand(superset *supersetv1alpha1.Superset) []string {
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Init != nil && len(superset.Spec.Lifecycle.Init.Command) > 0 {
		return superset.Spec.Lifecycle.Init.Command
	}
	var initSpec *supersetv1alpha1.InitTaskSpec
	if superset.Spec.Lifecycle != nil {
		initSpec = superset.Spec.Lifecycle.Init
	}
	return buildInitCommand(initSpec)
}
