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
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/apache/superset-kubernetes-operator/internal/common"
)

var celeryBeatSingletonReplica = int32(1)

func httpProbe(path string, port int32, initialDelay int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: path,
				Port: intstr.FromInt32(port),
			},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    3,
	}
}

func tcpProbe(port int32, initialDelay int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt32(port),
			},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    3,
	}
}

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// ComponentResourceDef defines the resource defaults for one component.
type ComponentResourceDef struct {
	Name   string
	config componentReconcilerConfig
}

// ComponentResourceDefs returns the resource defaults for all components.
func ComponentResourceDefs() []ComponentResourceDef {
	return []ComponentResourceDef{
		{
			Name: "superset-webserver",
			config: componentReconcilerConfig{
				componentName: string(common.ComponentWebServer),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"/usr/bin/run-server.sh"},
					DefaultPorts: []corev1.ContainerPort{
						{Name: common.PortNameHTTP, ContainerPort: common.PortWebServer, Protocol: corev1.ProtocolTCP},
					},
					DefaultLivenessProbe:  httpProbe("/health", common.PortWebServer, 15),
					DefaultReadinessProbe: httpProbe("/health", common.PortWebServer, 5),
					DefaultStartupProbe:   httpProbe("/health", common.PortWebServer, 15),
				},
				defaultPort: 0,
				hasConfig:   true,
				hasScaling:  true,
			},
		},
		{
			Name: "superset-celeryworker",
			config: componentReconcilerConfig{
				componentName: string(common.ComponentCeleryWorker),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"celery", "--app=superset.tasks.celery_app:app", "worker", "--pool=prefork", "-O", "fair", "-c", "4"},
				},
				defaultPort: 0,
				hasConfig:   true,
				hasScaling:  true,
			},
		},
		{
			Name: "superset-celerybeat",
			config: componentReconcilerConfig{
				componentName: string(common.ComponentCeleryBeat),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"celery", "--app=superset.tasks.celery_app:app", "beat", "--pidfile", "/tmp/celerybeat.pid", "--schedule", "/tmp/celerybeat-schedule"},
					ForceReplicas:  &celeryBeatSingletonReplica,
				},
				defaultPort: 0,
				hasConfig:   true,
				hasScaling:  false,
			},
		},
		{
			Name: "superset-celeryflower",
			config: componentReconcilerConfig{
				componentName: string(common.ComponentCeleryFlower),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"/bin/sh", "-c", "exec celery --app=superset.tasks.celery_app:app flower --url_prefix=\"$SUPERSET_OPERATOR__FLOWER_URL_PREFIX\""},
					DefaultPorts: []corev1.ContainerPort{
						{Name: common.PortNameHTTP, ContainerPort: common.PortCeleryFlower, Protocol: corev1.ProtocolTCP},
					},
					DefaultLivenessProbe:  httpProbe("/api/workers", common.PortCeleryFlower, 15),
					DefaultReadinessProbe: httpProbe("/api/workers", common.PortCeleryFlower, 5),
				},
				defaultPort: common.PortCeleryFlower,
				hasConfig:   true,
				hasScaling:  true,
			},
		},
		{
			Name: "superset-websocket",
			config: componentReconcilerConfig{
				componentName: string(common.ComponentWebsocketServer),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"node", "websocket_server.js"},
					DefaultPorts: []corev1.ContainerPort{
						{Name: common.PortNameWebsocket, ContainerPort: common.PortWebsocket, Protocol: corev1.ProtocolTCP},
					},
					DefaultLivenessProbe:  httpProbe("/health", common.PortWebsocket, 15),
					DefaultReadinessProbe: httpProbe("/health", common.PortWebsocket, 5),
				},
				defaultPort: common.PortWebsocket,
				hasConfig:   false,
				hasScaling:  true,
			},
		},
		{
			Name: "superset-mcpserver",
			config: componentReconcilerConfig{
				componentName: string(common.ComponentMcpServer),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"superset", "mcp", "run", "--host", "0.0.0.0", "--port", "8088"},
					DefaultPorts: []corev1.ContainerPort{
						{Name: common.PortNameMcp, ContainerPort: common.PortMcpServer, Protocol: corev1.ProtocolTCP},
					},
					DefaultLivenessProbe:  tcpProbe(common.PortMcpServer, 15),
					DefaultReadinessProbe: tcpProbe(common.PortMcpServer, 5),
				},
				defaultPort: common.PortMcpServer,
				hasConfig:   true,
				hasScaling:  true,
			},
		},
	}
}

func componentResourceConfig(componentType common.ComponentType) (componentReconcilerConfig, bool) {
	for _, def := range ComponentResourceDefs() {
		if def.config.componentName == string(componentType) {
			return def.config, true
		}
	}
	return componentReconcilerConfig{}, false
}
