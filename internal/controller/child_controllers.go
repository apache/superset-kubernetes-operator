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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryworkers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryworkers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetcelerybeats,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetcelerybeats/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryflowers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryflowers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebsocketservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebsocketservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetmcpservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetmcpservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

var celeryBeatSingletonReplica = int32(1)

// ChildControllerDef defines the data needed to register a child controller.
type ChildControllerDef struct {
	Name   string
	config childReconcilerConfig
	newObj func() ChildCR
}

// ChildControllerDefs returns the definitions for all child controllers.
func ChildControllerDefs() []ChildControllerDef {
	return []ChildControllerDef{
		{
			Name: "superset-webserver",
			config: childReconcilerConfig{
				componentName: string(common.ComponentWebServer),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"/usr/bin/run-server.sh"},
					DefaultPorts: []corev1.ContainerPort{
						{Name: common.PortNameHTTP, ContainerPort: common.PortWebServer, Protocol: corev1.ProtocolTCP},
					},
				},
				defaultPort: common.PortWebServer,
				hasConfig:   true,
				hasScaling:  true,
			},
			newObj: func() ChildCR { return &supersetv1alpha1.SupersetWebServer{} },
		},
		{
			Name: "superset-celeryworker",
			config: childReconcilerConfig{
				componentName: string(common.ComponentCeleryWorker),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"celery", "--app=superset.tasks.celery_app:app", "worker", "--pool=prefork", "-O", "fair", "-c", "4"},
				},
				defaultPort: 0,
				hasConfig:   true,
				hasScaling:  true,
			},
			newObj: func() ChildCR { return &supersetv1alpha1.SupersetCeleryWorker{} },
		},
		{
			Name: "superset-celerybeat",
			config: childReconcilerConfig{
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
			newObj: func() ChildCR { return &supersetv1alpha1.SupersetCeleryBeat{} },
		},
		{
			Name: "superset-celeryflower",
			config: childReconcilerConfig{
				componentName: string(common.ComponentCeleryFlower),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"/bin/sh", "-c", "exec celery --app=superset.tasks.celery_app:app flower --url_prefix=\"$SUPERSET_OPERATOR__FLOWER_URL_PREFIX\""},
					DefaultPorts: []corev1.ContainerPort{
						{Name: common.PortNameHTTP, ContainerPort: common.PortCeleryFlower, Protocol: corev1.ProtocolTCP},
					},
				},
				defaultPort: common.PortCeleryFlower,
				hasConfig:   true,
				hasScaling:  true,
			},
			newObj: func() ChildCR { return &supersetv1alpha1.SupersetCeleryFlower{} },
		},
		{
			Name: "superset-websocket",
			config: childReconcilerConfig{
				componentName: string(common.ComponentWebsocketServer),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"node", "websocket_server.js"},
					DefaultPorts: []corev1.ContainerPort{
						{Name: common.PortNameWebsocket, ContainerPort: common.PortWebsocket, Protocol: corev1.ProtocolTCP},
					},
				},
				defaultPort: common.PortWebsocket,
				hasConfig:   false,
				hasScaling:  true,
			},
			newObj: func() ChildCR { return &supersetv1alpha1.SupersetWebsocketServer{} },
		},
		{
			Name: "superset-mcpserver",
			config: childReconcilerConfig{
				componentName: string(common.ComponentMcpServer),
				deployConfig: DeploymentConfig{
					ContainerName:  common.Container,
					DefaultCommand: []string{"superset", "mcp", "run", "--host", "0.0.0.0", "--port", "8088"},
					DefaultPorts: []corev1.ContainerPort{
						{Name: common.PortNameMcp, ContainerPort: common.PortMcpServer, Protocol: corev1.ProtocolTCP},
					},
				},
				defaultPort: common.PortMcpServer,
				hasConfig:   true,
				hasScaling:  true,
			},
			newObj: func() ChildCR { return &supersetv1alpha1.SupersetMcpServer{} },
		},
	}
}

// NewChildReconciler creates a ChildReconciler from a ChildControllerDef.
func NewChildReconciler(c client.Client, s *runtime.Scheme, r record.EventRecorder, def ChildControllerDef) *ChildReconciler {
	return &ChildReconciler{
		Client:   c,
		Scheme:   s,
		Recorder: r,
		Config:   def.config,
		NewObj:   def.newObj,
	}
}
