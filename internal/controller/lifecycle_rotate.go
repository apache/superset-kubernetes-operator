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

// rotateInputs returns the rotate-specific inputs that contribute to its step
// checksum. Rotation fires on secret-key transitions: changing either the
// current secretKey/secretKeyFrom or the previousSecretKey/previousSecretKeyFrom
// triggers a re-run.
func (r *SupersetReconciler) rotateInputs(superset *supersetv1alpha1.Superset) any {
	currentImage := resolveLifecycleImage(&superset.Spec.Image, lifecycleImageOverride(superset))
	trigger := ""
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Rotate != nil {
		trigger = derefOrDefault(superset.Spec.Lifecycle.Rotate.Trigger, "")
	}
	return struct {
		Image                 string
		Trigger               string
		SecretKey             string
		SecretKeyFrom         *corev1.SecretKeySelector
		PreviousSecretKey     string
		PreviousSecretKeyFrom *corev1.SecretKeySelector
	}{
		Image:                 currentImage,
		Trigger:               trigger,
		SecretKey:             derefOrDefault(superset.Spec.SecretKey, ""),
		SecretKeyFrom:         superset.Spec.SecretKeyFrom,
		PreviousSecretKey:     derefOrDefault(superset.Spec.PreviousSecretKey, ""),
		PreviousSecretKeyFrom: superset.Spec.PreviousSecretKeyFrom,
	}
}

// defaultRotateCommand returns the user override or the standard
// `superset re-encrypt-secrets` command.
func defaultRotateCommand(superset *supersetv1alpha1.Superset) []string {
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Rotate != nil && len(superset.Spec.Lifecycle.Rotate.Command) > 0 {
		return superset.Spec.Lifecycle.Rotate.Command
	}
	return []string{"/bin/sh", "-c", "superset re-encrypt-secrets"}
}
