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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestCeleryWorkerDefaultLivenessProbe(t *testing.T) {
	cfg, ok := componentResourceConfig(common.ComponentCeleryWorker)
	if !ok {
		t.Fatal("celery worker resource config not found")
	}
	probe := cfg.deployConfig.DefaultLivenessProbe
	if probe == nil || probe.Exec == nil {
		t.Fatalf("expected exec liveness probe, got %+v", probe)
	}
	cmd := probe.Exec.Command
	if len(cmd) != 3 || cmd[0] != bootstrapShell || cmd[1] != "-c" {
		t.Fatalf("expected %s -c <script>, got %q", bootstrapShell, cmd)
	}
	for _, want := range []string{"--app=superset.tasks.celery_app:app", "inspect ping", `--destination "celery@$HOSTNAME"`} {
		if !strings.Contains(cmd[2], want) {
			t.Errorf("probe script %q missing %q", cmd[2], want)
		}
	}
	if !slices.Contains(cfg.deployConfig.DefaultCommand, "--app=superset.tasks.celery_app:app") {
		t.Errorf("probe app does not match worker command %q", cfg.deployConfig.DefaultCommand)
	}
	if probe.InitialDelaySeconds != 120 || probe.PeriodSeconds != 60 || probe.TimeoutSeconds != 60 || probe.FailureThreshold != 3 {
		t.Errorf("unexpected probe timings: %+v", probe)
	}
	if cfg.deployConfig.DefaultReadinessProbe != nil || cfg.deployConfig.DefaultStartupProbe != nil {
		t.Error("celery worker serves no traffic and should have no default readiness or startup probe")
	}

	beat, ok := componentResourceConfig(common.ComponentCeleryBeat)
	if !ok {
		t.Fatal("celery beat resource config not found")
	}
	if beat.deployConfig.DefaultLivenessProbe != nil {
		t.Error("celery beat does not answer inspect ping and should have no default liveness probe")
	}
}

func TestBuildDeploymentSpec_CeleryWorkerLivenessProbe(t *testing.T) {
	cfg, _ := componentResourceConfig(common.ComponentCeleryWorker)
	labels := map[string]string{"app": "test"}
	newSpec := func(container *supersetv1alpha1.ContainerTemplate) *supersetv1alpha1.FlatComponentSpec {
		return &supersetv1alpha1.FlatComponentSpec{
			Image:       supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			PodTemplate: &supersetv1alpha1.PodTemplate{Container: container},
		}
	}

	t.Run("default applies", func(t *testing.T) {
		got := buildDeploymentSpec(newSpec(nil), cfg.deployConfig, nil, labels).Template.Spec.Containers[0].LivenessProbe
		if got == nil || got.Exec == nil {
			t.Fatalf("expected default exec liveness probe, got %+v", got)
		}
	})

	t.Run("user ports leave exec probe unchanged", func(t *testing.T) {
		spec := newSpec(&supersetv1alpha1.ContainerTemplate{
			Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9808, Protocol: corev1.ProtocolTCP}},
		})
		got := buildDeploymentSpec(spec, cfg.deployConfig, nil, labels).Template.Spec.Containers[0].LivenessProbe
		if got == nil || got.Exec == nil || got.HTTPGet != nil || got.TCPSocket != nil {
			t.Fatalf("expected exec probe to survive port override, got %+v", got)
		}
		if !slices.Equal(got.Exec.Command, cfg.deployConfig.DefaultLivenessProbe.Exec.Command) {
			t.Errorf("exec command changed: got %q", got.Exec.Command)
		}
	})

	t.Run("user probe replaces default", func(t *testing.T) {
		user := &corev1.Probe{Exec: &corev1.ExecAction{Command: []string{"true"}}, PeriodSeconds: 30}
		got := buildDeploymentSpec(newSpec(&supersetv1alpha1.ContainerTemplate{LivenessProbe: user}), cfg.deployConfig, nil, labels).
			Template.Spec.Containers[0].LivenessProbe
		if got == nil || got.Exec == nil || !slices.Equal(got.Exec.Command, []string{"true"}) || got.PeriodSeconds != 30 {
			t.Fatalf("expected user probe, got %+v", got)
		}
	})
}
