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

package resolution

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// TestMergeContainerTemplate_OperatorEnvNotOverridable guards the threat-model
// claim that operator-injected transport env vars cannot be hijacked by a CR
// author. A user who sets podTemplate.container.env entries named
// SUPERSET_OPERATOR__* must not be able to replace the operator-resolved secret
// references — otherwise they could point SECRET_KEY/DB/Valkey at an attacker
// value. The merge applies operator env last (op.Env), so it always wins.
func TestMergeContainerTemplate_OperatorEnvNotOverridable(t *testing.T) {
	operatorEnv := []corev1.EnvVar{
		{Name: common.EnvSecretKey, ValueFrom: secretEnvSource("app-secret", "secret-key")},
		{Name: common.EnvDBPass, ValueFrom: secretEnvSource("db-secret", "password")},
		{Name: common.EnvValkeyPass, ValueFrom: secretEnvSource("vk-secret", "password")},
	}

	for _, name := range []string{common.EnvSecretKey, common.EnvDBPass, common.EnvValkeyPass} {
		t.Run(name, func(t *testing.T) {
			// User attempts to override the operator var with an inline value.
			comp := &supersetv1alpha1.ContainerTemplate{
				Env: []corev1.EnvVar{{Name: name, Value: "attacker-controlled"}},
			}
			op := &OperatorInjected{Env: operatorEnv}

			result := MergeContainerTemplate(comp, nil, op)

			got := findEnv(result.Env, name)
			if got == nil {
				t.Fatalf("expected env %s in merged result", name)
			}
			if got.Value != "" {
				t.Errorf("env %s has inline Value %q; operator secretKeyRef was overridden by user input", name, got.Value)
			}
			if got.ValueFrom == nil || got.ValueFrom.SecretKeyRef == nil {
				t.Fatalf("env %s lost its operator secretKeyRef: %+v", name, got)
			}
		})
	}
}

// TestMergePodTemplate_OperatorLabelsNotOverridable guards that operator-managed
// pod labels (e.g. superset.apache.org/parent, used for NetworkPolicy
// instance isolation and label-based discovery) cannot be clobbered by a
// user-supplied podTemplate. operatorLabels are merged last and must win.
func TestMergePodTemplate_OperatorLabelsNotOverridable(t *testing.T) {
	operatorLabels := map[string]string{
		common.LabelKeyParent: "real-parent",
		common.LabelKeyName:   common.LabelValueApp,
	}
	comp := &supersetv1alpha1.PodTemplate{
		Labels: map[string]string{
			common.LabelKeyParent: "attacker-parent",
			common.LabelKeyName:   "attacker-app",
			"team":                "data",
		},
	}

	result := MergePodTemplate(comp, nil, operatorLabels, &OperatorInjected{})

	if got := result.Labels[common.LabelKeyParent]; got != "real-parent" {
		t.Errorf("label %s = %q, want real-parent (operator protected)", common.LabelKeyParent, got)
	}
	if got := result.Labels[common.LabelKeyName]; got != common.LabelValueApp {
		t.Errorf("label %s = %q, want %q (operator protected)", common.LabelKeyName, got, common.LabelValueApp)
	}
	// Non-protected user labels still merge through.
	if got := result.Labels["team"]; got != "data" {
		t.Errorf("label team = %q, want data (user label preserved)", got)
	}
}

func secretEnvSource(name, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Key:                  key,
		},
	}
}

func findEnv(envs []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range envs {
		if envs[i].Name == name {
			return &envs[i]
		}
	}
	return nil
}
