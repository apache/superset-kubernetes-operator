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
	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// initInputs returns the init-specific inputs that contribute to its step
// checksum. Init is the config-sensitive task: rendered superset_config.py
// changes propagate via configChecksum so feature/config changes re-run init
// without re-running the upstream migrate/rotate steps.
func (r *SupersetReconciler) initInputs(superset *supersetv1alpha1.Superset, configChecksum string) any {
	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	trigger := ""
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Init != nil {
		trigger = derefOrDefault(superset.Spec.Lifecycle.Init.Trigger, "")
	}
	return struct {
		Image          string
		ConfigChecksum string
		Trigger        string
	}{
		Image:          currentImage,
		ConfigChecksum: configChecksum,
		Trigger:        trigger,
	}
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
