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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// TestBuildDeploymentSpec covers how buildDeploymentSpec turns a resolved
// FlatComponentSpec + DeploymentConfig into a Deployment spec: image/command/port
// defaults, command overrides, replica handling (forced, HPA-cleared), pod-level
// resources, operator-label protection, and probe port retargeting.
func TestBuildDeploymentSpec(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
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
	})

	t.Run("command override", func(t *testing.T) {
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
	})

	t.Run("force replicas", func(t *testing.T) {
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
	})

	t.Run("HPA enabled clears replicas", func(t *testing.T) {
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
	})

	t.Run("pod-level resources", func(t *testing.T) {
		spec := &supersetv1alpha1.FlatComponentSpec{
			Image: supersetv1alpha1.ImageSpec{
				Repository: "apache/superset",
				Tag:        "latest",
				PullPolicy: corev1.PullIfNotPresent,
			},
			PodTemplate: &supersetv1alpha1.PodTemplate{
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("4"),
						corev1.ResourceMemory: resource.MustParse("8Gi"),
					},
				},
			},
		}
		labels := map[string]string{"app": "test"}
		cfg := DeploymentConfig{
			ContainerName:  "test",
			DefaultCommand: []string{"test"},
		}

		result := buildDeploymentSpec(spec, cfg, nil, labels)

		podResources := result.Template.Spec.Resources
		if podResources == nil {
			t.Fatal("expected pod-level resources to be set")
		}
		if podResources.Requests.Cpu().String() != "2" {
			t.Errorf("expected pod CPU request 2, got %s", podResources.Requests.Cpu())
		}
		if podResources.Limits.Memory().String() != "8Gi" {
			t.Errorf("expected pod memory limit 8Gi, got %s", podResources.Limits.Memory())
		}
	})

	t.Run("operator labels cannot be overridden", func(t *testing.T) {
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
	})

	t.Run("retargets default probes to overridden port", func(t *testing.T) {
		spec := &supersetv1alpha1.FlatComponentSpec{
			Image: supersetv1alpha1.ImageSpec{
				Repository: "apache/superset",
				Tag:        "latest",
				PullPolicy: corev1.PullIfNotPresent,
			},
			PodTemplate: &supersetv1alpha1.PodTemplate{
				Container: &supersetv1alpha1.ContainerTemplate{
					Ports: []corev1.ContainerPort{
						{Name: common.PortNameHTTP, ContainerPort: 9999, Protocol: corev1.ProtocolTCP},
					},
				},
			},
		}
		labels := map[string]string{"app": "test"}
		cfg := DeploymentConfig{
			ContainerName:  common.Container,
			DefaultCommand: []string{"/usr/bin/run-server.sh"},
			DefaultPorts: []corev1.ContainerPort{
				{Name: common.PortNameHTTP, ContainerPort: common.PortWebServer, Protocol: corev1.ProtocolTCP},
			},
			DefaultLivenessProbe:  httpProbe("/health", common.PortWebServer, 15),
			DefaultReadinessProbe: httpProbe("/health", common.PortWebServer, 5),
			DefaultStartupProbe:   tcpProbe(common.PortWebServer, 15),
		}

		result := buildDeploymentSpec(spec, cfg, nil, labels)
		container := result.Template.Spec.Containers[0]

		if got := container.LivenessProbe.HTTPGet.Port; got != intstr.FromInt32(9999) {
			t.Errorf("liveness probe port: got %v, want 9999", got)
		}
		if got := container.ReadinessProbe.HTTPGet.Port; got != intstr.FromInt32(9999) {
			t.Errorf("readiness probe port: got %v, want 9999", got)
		}
		if got := container.StartupProbe.TCPSocket.Port; got != intstr.FromInt32(9999) {
			t.Errorf("startup probe port: got %v, want 9999", got)
		}

		// Defaults should not be mutated by retargeting.
		if cfg.DefaultLivenessProbe.HTTPGet.Port != intstr.FromInt32(common.PortWebServer) {
			t.Errorf("default liveness probe was mutated: got %v", cfg.DefaultLivenessProbe.HTTPGet.Port)
		}
	})

	t.Run("keeps default probes when ports not overridden", func(t *testing.T) {
		spec := &supersetv1alpha1.FlatComponentSpec{
			Image: supersetv1alpha1.ImageSpec{
				Repository: "apache/superset",
				Tag:        "latest",
				PullPolicy: corev1.PullIfNotPresent,
			},
		}
		labels := map[string]string{"app": "test"}
		cfg := DeploymentConfig{
			ContainerName:  common.Container,
			DefaultCommand: []string{"/usr/bin/run-server.sh"},
			DefaultPorts: []corev1.ContainerPort{
				{Name: common.PortNameHTTP, ContainerPort: common.PortWebServer, Protocol: corev1.ProtocolTCP},
			},
			DefaultLivenessProbe: httpProbe("/health", common.PortWebServer, 15),
		}

		result := buildDeploymentSpec(spec, cfg, nil, labels)
		container := result.Template.Spec.Containers[0]

		if got := container.LivenessProbe.HTTPGet.Port; got != intstr.FromInt32(common.PortWebServer) {
			t.Errorf("liveness probe port: got %v, want %d", got, common.PortWebServer)
		}
	})

	t.Run("user probe wins over retargeting", func(t *testing.T) {
		userProbe := httpProbe("/custom", 7777, 1)
		spec := &supersetv1alpha1.FlatComponentSpec{
			Image: supersetv1alpha1.ImageSpec{
				Repository: "apache/superset",
				Tag:        "latest",
				PullPolicy: corev1.PullIfNotPresent,
			},
			PodTemplate: &supersetv1alpha1.PodTemplate{
				Container: &supersetv1alpha1.ContainerTemplate{
					Ports: []corev1.ContainerPort{
						{Name: common.PortNameHTTP, ContainerPort: 9999, Protocol: corev1.ProtocolTCP},
					},
					LivenessProbe: userProbe,
				},
			},
		}
		labels := map[string]string{"app": "test"}
		cfg := DeploymentConfig{
			ContainerName:        common.Container,
			DefaultCommand:       []string{"/usr/bin/run-server.sh"},
			DefaultLivenessProbe: httpProbe("/health", common.PortWebServer, 15),
		}

		result := buildDeploymentSpec(spec, cfg, nil, labels)
		container := result.Template.Spec.Containers[0]

		if got := container.LivenessProbe.HTTPGet.Port; got != intstr.FromInt32(7777) {
			t.Errorf("user liveness probe port should be preserved: got %v, want 7777", got)
		}
		if container.LivenessProbe.HTTPGet.Path != "/custom" {
			t.Errorf("user liveness probe path should be preserved: got %q", container.LivenessProbe.HTTPGet.Path)
		}
	})
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

// TestBuildServiceSpec covers how buildServiceSpec maps a ComponentServiceSpec to a
// Service spec: ClusterIP defaults, custom type/port/nodePort, and that the target
// port follows the container port rather than the service port.
func TestBuildServiceSpec(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
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
	})

	t.Run("custom values", func(t *testing.T) {
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
	})

	t.Run("custom container port", func(t *testing.T) {
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
	})
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
