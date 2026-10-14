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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestBuildDeploymentSpec_CeleryWorkerLivenessProbeIgnoresPortOverride(t *testing.T) {
	cfg, _ := componentResourceConfig(common.ComponentCeleryWorker)
	spec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
		PodTemplate: &supersetv1alpha1.PodTemplate{Container: &supersetv1alpha1.ContainerTemplate{
			Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9808, Protocol: corev1.ProtocolTCP}},
		}},
	}
	got := buildDeploymentSpec(spec, cfg.deployConfig, nil, map[string]string{"app": "test"}).Template.Spec.Containers[0].LivenessProbe
	if got == nil || got.Exec == nil || got.HTTPGet != nil || got.TCPSocket != nil {
		t.Fatalf("expected exec probe to survive port override, got %+v", got)
	}
	if !slices.Equal(got.Exec.Command, cfg.deployConfig.DefaultLivenessProbe.Exec.Command) {
		t.Errorf("exec command changed: got %q", got.Exec.Command)
	}
}
