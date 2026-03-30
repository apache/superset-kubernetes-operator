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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestBuildDeploymentSpec_Defaults(t *testing.T) {
	replicas := int32(2)
	spec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{
			Repository: "apache/superset",
			Tag:        "latest",
			PullPolicy: corev1.PullIfNotPresent,
		},
		Replicas: &replicas,
	}
	labels := map[string]string{
		common.LabelKeyName:      common.LabelValueApp,
		common.LabelKeyComponent: string(common.ComponentWebServer),
		common.LabelKeyInstance:  "test-web",
	}
	cfg := DeploymentConfig{
		ContainerName:  common.Container,
		DefaultCommand: []string{"/usr/bin/run-server.sh"},
		DefaultPorts: []corev1.ContainerPort{
			{Name: common.PortNameHTTP, ContainerPort: common.PortWebServer, Protocol: corev1.ProtocolTCP},
		},
	}

	result := buildDeploymentSpec(spec, cfg, nil, labels)

	if *result.Replicas != 2 {
		t.Errorf("expected 2 replicas, got %d", *result.Replicas)
	}

	container := result.Template.Spec.Containers[0]
	if container.Name != common.Container {
		t.Errorf("expected container name %s, got %s", common.Container, container.Name)
	}
	if container.Image != "apache/superset:latest" {
		t.Errorf("expected image apache/superset:latest, got %s", container.Image)
	}
	if len(container.Command) != 1 || container.Command[0] != "/usr/bin/run-server.sh" {
		t.Errorf("expected default command, got %v", container.Command)
	}
	if len(container.Ports) != 1 || container.Ports[0].ContainerPort != common.PortWebServer {
		t.Errorf("expected default port %d, got %v", common.PortWebServer, container.Ports)
	}
}

func TestBuildDeploymentSpec_CommandOverride(t *testing.T) {
	spec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{
			Repository: "apache/superset",
			Tag:        "latest",
			PullPolicy: corev1.PullIfNotPresent,
		},
		PodTemplate: &supersetv1alpha1.PodTemplate{
			Container: &supersetv1alpha1.ContainerTemplate{
				Command: []string{"custom", "command"},
				Args:    []string{"--flag"},
			},
		},
	}
	labels := map[string]string{"app": "test"}
	cfg := DeploymentConfig{
		ContainerName:  "test",
		DefaultCommand: []string{"default"},
		DefaultArgs:    []string{"--default"},
	}

	result := buildDeploymentSpec(spec, cfg, nil, labels)
	container := result.Template.Spec.Containers[0]

	if len(container.Command) != 2 || container.Command[0] != "custom" {
		t.Errorf("expected custom command, got %v", container.Command)
	}
	if len(container.Args) != 1 || container.Args[0] != "--flag" {
		t.Errorf("expected custom args, got %v", container.Args)
	}
}

func TestBuildDeploymentSpec_ForceReplicas(t *testing.T) {
	specReplicas := int32(5)
	spec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{
			Repository: "apache/superset",
			Tag:        "latest",
			PullPolicy: corev1.PullIfNotPresent,
		},
		Replicas: &specReplicas,
	}
	labels := map[string]string{"app": "test"}
	forcedReplicas := int32(1)
	cfg := DeploymentConfig{
		ContainerName:  "test",
		DefaultCommand: []string{"test"},
		ForceReplicas:  &forcedReplicas,
	}

	result := buildDeploymentSpec(spec, cfg, nil, labels)

	if *result.Replicas != 1 {
		t.Errorf("expected forced 1 replica, got %d", *result.Replicas)
	}
}

func TestBuildDeploymentSpec_HPAEnabled_NoReplicas(t *testing.T) {
	spec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{
			Repository: "apache/superset",
			Tag:        "latest",
			PullPolicy: corev1.PullIfNotPresent,
		},
		Autoscaling: &supersetv1alpha1.AutoscalingSpec{
			MaxReplicas: 10,
		},
	}
	labels := map[string]string{"app": "test"}
	cfg := DeploymentConfig{
		ContainerName:  "test",
		DefaultCommand: []string{"test"},
	}

	result := buildDeploymentSpec(spec, cfg, nil, labels)

	if result.Replicas != nil {
		t.Errorf("expected nil replicas when HPA is enabled, got %d", *result.Replicas)
	}
}

func TestBuildServiceSpec_Defaults(t *testing.T) {
	labels := map[string]string{"app": "test"}
	result := buildServiceSpec(nil, labels, 8088, 8088)

	if result.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("expected ClusterIP, got %s", result.Type)
	}
	if result.Ports[0].Port != 8088 {
		t.Errorf("expected port 8088, got %d", result.Ports[0].Port)
	}
	if result.Ports[0].TargetPort != intstr.FromInt32(8088) {
		t.Errorf("expected targetPort 8088, got %v", result.Ports[0].TargetPort)
	}
}

func TestBuildServiceSpec_CustomValues(t *testing.T) {
	labels := map[string]string{"app": "test"}
	port := int32(9090)
	nodePort := int32(30090)
	svcSpec := &supersetv1alpha1.ComponentServiceSpec{
		Type:     corev1.ServiceTypeNodePort,
		Port:     &port,
		NodePort: &nodePort,
	}
	result := buildServiceSpec(svcSpec, labels, 8088, 8088)

	if result.Type != corev1.ServiceTypeNodePort {
		t.Errorf("expected NodePort, got %s", result.Type)
	}
	if result.Ports[0].Port != 9090 {
		t.Errorf("expected port 9090, got %d", result.Ports[0].Port)
	}
	if result.Ports[0].NodePort != 30090 {
		t.Errorf("expected nodePort 30090, got %d", result.Ports[0].NodePort)
	}
	if result.Ports[0].TargetPort != intstr.FromInt32(8088) {
		t.Errorf("expected targetPort 8088 (container port, not service port), got %v", result.Ports[0].TargetPort)
	}
}

func TestBuildServiceSpec_CustomContainerPort(t *testing.T) {
	labels := map[string]string{"app": "test"}
	port := int32(443)
	svcSpec := &supersetv1alpha1.ComponentServiceSpec{
		Port: &port,
	}
	result := buildServiceSpec(svcSpec, labels, 9090, 8088)

	if result.Ports[0].Port != 443 {
		t.Errorf("expected service port 443, got %d", result.Ports[0].Port)
	}
	if result.Ports[0].TargetPort != intstr.FromInt32(9090) {
		t.Errorf("expected targetPort 9090 (custom container port), got %v", result.Ports[0].TargetPort)
	}
}

func TestResolveContainerPort(t *testing.T) {
	t.Run("nil spec", func(t *testing.T) {
		got := resolveContainerPort(nil, 8088)
		if got != 8088 {
			t.Errorf("expected 8088, got %d", got)
		}
	})
	t.Run("no container ports", func(t *testing.T) {
		spec := &supersetv1alpha1.FlatComponentSpec{}
		got := resolveContainerPort(spec, 8088)
		if got != 8088 {
			t.Errorf("expected 8088, got %d", got)
		}
	})
	t.Run("custom container port", func(t *testing.T) {
		spec := &supersetv1alpha1.FlatComponentSpec{
			PodTemplate: &supersetv1alpha1.PodTemplate{
				Container: &supersetv1alpha1.ContainerTemplate{
					Ports: []corev1.ContainerPort{{ContainerPort: 9090}},
				},
			},
		}
		got := resolveContainerPort(spec, 8088)
		if got != 9090 {
			t.Errorf("expected 9090, got %d", got)
		}
	})
}

func TestBuildDeploymentSpec_OperatorLabelsCannotBeOverridden(t *testing.T) {
	spec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{
			Repository: "apache/superset",
			Tag:        "latest",
			PullPolicy: corev1.PullIfNotPresent,
		},
		PodTemplate: &supersetv1alpha1.PodTemplate{
			Labels: map[string]string{
				common.LabelKeyComponent: "attacker-value",
				common.LabelKeyName:      "attacker-app",
				"custom-label":           "allowed",
			},
		},
	}
	selectorLabels := map[string]string{
		common.LabelKeyName:      common.LabelValueApp,
		common.LabelKeyComponent: string(common.ComponentWebServer),
		common.LabelKeyInstance:  "my-instance",
	}
	cfg := DeploymentConfig{
		ContainerName:  common.Container,
		DefaultCommand: []string{"run"},
	}

	result := buildDeploymentSpec(spec, cfg, nil, selectorLabels)

	podLabels := result.Template.Labels
	if podLabels[common.LabelKeyComponent] != string(common.ComponentWebServer) {
		t.Errorf("operator label %s was overridden: got %q, want %q",
			common.LabelKeyComponent, podLabels[common.LabelKeyComponent], string(common.ComponentWebServer))
	}
	if podLabels[common.LabelKeyName] != common.LabelValueApp {
		t.Errorf("operator label %s was overridden: got %q, want %q",
			common.LabelKeyName, podLabels[common.LabelKeyName], common.LabelValueApp)
	}
	if podLabels["custom-label"] != "allowed" {
		t.Errorf("user custom label should be preserved, got %q", podLabels["custom-label"])
	}

	for k, v := range selectorLabels {
		if podLabels[k] != v {
			t.Errorf("pod label %s=%q does not match selector label %s=%q — Deployment will fail", k, podLabels[k], k, v)
		}
	}
}

func TestPreserveServiceAllocatedFields(t *testing.T) {
	familyPolicy := corev1.IPFamilyPolicySingleStack
	desired := corev1.ServiceSpec{
		Type:  corev1.ServiceTypeClusterIP,
		Ports: []corev1.ServicePort{{Port: 9090}},
	}
	existing := corev1.ServiceSpec{
		Type:           corev1.ServiceTypeClusterIP,
		ClusterIP:      "10.0.0.12",
		ClusterIPs:     []string{"10.0.0.12"},
		IPFamilies:     []corev1.IPFamily{corev1.IPv4Protocol},
		IPFamilyPolicy: &familyPolicy,
	}

	preserveServiceAllocatedFields(&desired, existing)

	if desired.ClusterIP != existing.ClusterIP {
		t.Errorf("expected ClusterIP to be preserved, got %q", desired.ClusterIP)
	}
	if len(desired.ClusterIPs) != 1 || desired.ClusterIPs[0] != "10.0.0.12" {
		t.Errorf("expected ClusterIPs to be preserved, got %v", desired.ClusterIPs)
	}
	if desired.IPFamilyPolicy == nil || *desired.IPFamilyPolicy != familyPolicy {
		t.Errorf("expected IPFamilyPolicy to be preserved, got %v", desired.IPFamilyPolicy)
	}
}
