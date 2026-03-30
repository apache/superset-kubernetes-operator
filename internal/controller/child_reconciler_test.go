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
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestBuildChecksumAnnotations(t *testing.T) {
	tests := []struct {
		name     string
		checksum string
		wantLen  int
	}{
		{"empty", "", 0},
		{"non-empty", "sha256:abc", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildChecksumAnnotations(tt.checksum)
			if len(result) != tt.wantLen {
				t.Errorf("expected %d annotations, got %d", tt.wantLen, len(result))
			}
			if tt.wantLen > 0 && result[common.AnnotationConfigChecksum] != tt.checksum {
				t.Errorf("expected config-checksum=%s, got %s", tt.checksum, result[common.AnnotationConfigChecksum])
			}
		})
	}
}

func TestConvertComponent(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		if convertComponent(nil) != nil {
			t.Error("expected nil for nil componentAccessor")
		}
	})

	t.Run("zero value", func(t *testing.T) {
		result := convertComponent(&componentAccessor{})
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.Replicas != nil {
			t.Error("expected nil replicas for zero value")
		}
	})

	t.Run("complete", func(t *testing.T) {
		replicas := int32(3)
		a := &componentAccessor{
			podTemplate: &supersetv1alpha1.PodTemplate{
				NodeSelector: map[string]string{"zone": "us-east"},
				Container: &supersetv1alpha1.ContainerTemplate{
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
					},
					Env:            []corev1.EnvVar{{Name: "X", Value: "Y"}},
					Command:        []string{"cmd"},
					Args:           []string{"--arg"},
					LivenessProbe:  &corev1.Probe{InitialDelaySeconds: 10},
					ReadinessProbe: &corev1.Probe{InitialDelaySeconds: 5},
					StartupProbe:   &corev1.Probe{InitialDelaySeconds: 15},
				},
			},
			replicas: &replicas,
		}

		result := convertComponent(a)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if *result.Replicas != 3 {
			t.Errorf("expected replicas 3, got %d", *result.Replicas)
		}
		if result.PodTemplate == nil || result.PodTemplate.Container == nil {
			t.Fatal("expected pod template with container")
		}
		ct := result.PodTemplate.Container
		if ct.Resources == nil {
			t.Error("expected resources to be set")
		}
		if len(ct.Env) != 1 || ct.Env[0].Name != "X" {
			t.Error("expected env to be converted")
		}
		if result.PodTemplate.NodeSelector["zone"] != "us-east" {
			t.Error("expected nodeSelector to be converted")
		}
		if len(ct.Command) != 1 || ct.Command[0] != "cmd" {
			t.Errorf("expected command [cmd], got %v", ct.Command)
		}
		if len(ct.Args) != 1 || ct.Args[0] != "--arg" {
			t.Errorf("expected args [--arg], got %v", ct.Args)
		}
		if ct.LivenessProbe == nil || ct.LivenessProbe.InitialDelaySeconds != 10 {
			t.Error("expected livenessProbe")
		}
		if ct.ReadinessProbe == nil || ct.ReadinessProbe.InitialDelaySeconds != 5 {
			t.Error("expected readinessProbe")
		}
		if ct.StartupProbe == nil || ct.StartupProbe.InitialDelaySeconds != 15 {
			t.Error("expected startupProbe")
		}
	})
}

func TestConvertComponent_AllDescriptorExtractors(t *testing.T) {
	replicas := int32(5)

	// Test each descriptor's extract function produces a valid accessor
	// that convertComponent can process.
	spec := supersetv1alpha1.SupersetSpec{
		WebServer: &supersetv1alpha1.WebServerComponentSpec{
			ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{Replicas: &replicas},
		},
		CeleryWorker: &supersetv1alpha1.CeleryWorkerComponentSpec{
			ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{Replicas: &replicas},
		},
		CeleryBeat: &supersetv1alpha1.CeleryBeatComponentSpec{},
		CeleryFlower: &supersetv1alpha1.CeleryFlowerComponentSpec{
			ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{Replicas: &replicas},
		},
		McpServer: &supersetv1alpha1.McpServerComponentSpec{
			ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{Replicas: &replicas},
		},
		WebsocketServer: &supersetv1alpha1.WebsocketServerComponentSpec{
			ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{Replicas: &replicas},
		},
	}

	for _, desc := range componentDescriptors {
		a := desc.extract(&spec)
		if a == nil {
			t.Errorf("%s: expected non-nil accessor", desc.componentType)
			continue
		}
		result := convertComponent(a)
		if result == nil {
			t.Errorf("%s: expected non-nil result", desc.componentType)
			continue
		}
	}

	// Verify nil extraction when component is absent.
	emptySpec := supersetv1alpha1.SupersetSpec{}
	for _, desc := range componentDescriptors {
		a := desc.extract(&emptySpec)
		if a != nil {
			t.Errorf("%s: expected nil accessor for absent component", desc.componentType)
		}
	}
}

func TestDeployConfigs_ComponentSpecific(t *testing.T) {
	// Verify each component's deploy config has the right container name and defaults.
	expectations := map[string]struct {
		containerName string
		hasPorts      bool
		forceReplica  bool
	}{
		string(common.ComponentWebServer):       {common.Container, true, false},
		string(common.ComponentCeleryWorker):    {common.Container, false, false},
		string(common.ComponentCeleryBeat):      {common.Container, false, true},
		string(common.ComponentCeleryFlower):    {common.Container, true, false},
		string(common.ComponentWebsocketServer): {common.Container, true, false},
		string(common.ComponentMcpServer):       {common.Container, true, false},
	}

	for _, def := range ChildControllerDefs() {
		cfg := def.config.deployConfig
		exp, ok := expectations[def.config.componentName]
		if !ok {
			t.Errorf("no expectations for component %s", def.config.componentName)
			continue
		}
		t.Run(def.config.componentName, func(t *testing.T) {
			if cfg.ContainerName != exp.containerName {
				t.Errorf("expected container name %s, got %s", exp.containerName, cfg.ContainerName)
			}
			if exp.hasPorts && len(cfg.DefaultPorts) == 0 {
				t.Error("expected default ports")
			}
			if !exp.hasPorts && len(cfg.DefaultPorts) != 0 {
				t.Error("expected no default ports")
			}
			if exp.forceReplica && (cfg.ForceReplicas == nil || *cfg.ForceReplicas != 1) {
				t.Error("expected ForceReplicas=1")
			}
			if !exp.forceReplica && cfg.ForceReplicas != nil {
				t.Error("expected nil ForceReplicas")
			}
			if len(cfg.DefaultCommand) == 0 {
				t.Error("expected non-empty default command")
			}
		})
	}
}

func TestChildReconcilerConfig_ComponentSpecific(t *testing.T) {
	// Verify each childReconcilerConfig has the correct settings.
	expectations := map[string]struct {
		hasConfig bool
		hasPort   bool
		scaling   bool
	}{
		string(common.ComponentWebServer):       {true, true, true},
		string(common.ComponentCeleryWorker):    {true, false, true},
		string(common.ComponentCeleryBeat):      {true, false, false},
		string(common.ComponentCeleryFlower):    {true, true, true},
		string(common.ComponentMcpServer):       {true, true, true},
		string(common.ComponentWebsocketServer): {false, true, true},
	}

	for _, def := range ChildControllerDefs() {
		cfg := def.config
		exp, ok := expectations[cfg.componentName]
		if !ok {
			t.Errorf("no expectations for component %s", cfg.componentName)
			continue
		}
		t.Run(cfg.componentName, func(t *testing.T) {
			if cfg.hasConfig != exp.hasConfig {
				t.Errorf("expected hasConfig=%v, got %v", exp.hasConfig, cfg.hasConfig)
			}
			hasPort := cfg.defaultPort > 0
			if hasPort != exp.hasPort {
				t.Errorf("expected hasPort=%v, got %v", exp.hasPort, hasPort)
			}
			if cfg.hasScaling != exp.scaling {
				t.Errorf("expected hasScaling=%v, got %v", exp.scaling, cfg.hasScaling)
			}
			if cfg.componentName == "" {
				t.Error("expected non-empty componentName")
			}
		})
	}
}

// --- New comprehensive tests for refactored DRY utilities ---

func TestDescriptors_ApplySpecFunctions(t *testing.T) {
	replicas := int32(3)
	svc := &supersetv1alpha1.ComponentServiceSpec{
		Type: corev1.ServiceTypeLoadBalancer,
		Port: common.Ptr(int32(9090)),
	}
	hpa := &supersetv1alpha1.AutoscalingSpec{
		MaxReplicas: 10,
		MinReplicas: common.Ptr(int32(2)),
		Metrics: []autoscalingv2.MetricSpec{
			{Type: autoscalingv2.ResourceMetricSourceType},
		},
	}
	pdb := &supersetv1alpha1.PDBSpec{
		MinAvailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
	}
	flat := supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{
			Repository: "apache/superset",
			Tag:        "4.1.0",
		},
		Replicas:            &replicas,
		Autoscaling:         hpa,
		PodDisruptionBudget: pdb,
	}

	for _, desc := range componentDescriptors {
		t.Run(string(desc.componentType), func(t *testing.T) {
			obj := desc.newChild()
			if obj == nil {
				t.Fatal("newChild() returned nil")
			}

			accessor := &componentAccessor{
				replicas: &replicas,
				service:  svc,
			}

			desc.applySpec(obj, flat, "rendered-config", "sha256:abc", accessor)

			// Verify the flat spec was applied to the correct child type.
			switch desc.componentType {
			case common.ComponentWebServer:
				ww := obj.(*supersetv1alpha1.SupersetWebServer)
				if ww.Spec.Image.Tag != "4.1.0" {
					t.Errorf("expected tag 4.1.0, got %s", ww.Spec.Image.Tag)
				}
				if ww.Spec.Config != "rendered-config" {
					t.Errorf("expected config rendered-config, got %q", ww.Spec.Config)
				}
				if ww.Spec.ConfigChecksum != "sha256:abc" {
					t.Errorf("expected checksum sha256:abc, got %q", ww.Spec.ConfigChecksum)
				}
				if ww.Spec.Service == nil || ww.Spec.Service.Type != corev1.ServiceTypeLoadBalancer {
					t.Error("expected service to be set with LoadBalancer type")
				}
				if ww.Spec.Autoscaling == nil || ww.Spec.Autoscaling.MaxReplicas != 10 {
					t.Error("expected autoscaling to be set")
				}
				if ww.Spec.PodDisruptionBudget == nil || ww.Spec.PodDisruptionBudget.MinAvailable.IntVal != 1 {
					t.Error("expected PDB to be set")
				}

			case common.ComponentCeleryWorker:
				cw := obj.(*supersetv1alpha1.SupersetCeleryWorker)
				if cw.Spec.Image.Tag != "4.1.0" {
					t.Errorf("expected tag 4.1.0, got %s", cw.Spec.Image.Tag)
				}
				if cw.Spec.Config != "rendered-config" {
					t.Errorf("expected config, got %q", cw.Spec.Config)
				}
				if cw.Spec.ConfigChecksum != "sha256:abc" {
					t.Errorf("expected checksum, got %q", cw.Spec.ConfigChecksum)
				}
				if cw.Spec.Autoscaling == nil {
					t.Error("expected autoscaling to be set")
				}
				if cw.Spec.PodDisruptionBudget == nil {
					t.Error("expected PDB to be set")
				}

			case common.ComponentCeleryBeat:
				cb := obj.(*supersetv1alpha1.SupersetCeleryBeat)
				if cb.Spec.Config != "rendered-config" {
					t.Errorf("expected config, got %q", cb.Spec.Config)
				}
				if cb.Spec.ConfigChecksum != "sha256:abc" {
					t.Errorf("expected checksum, got %q", cb.Spec.ConfigChecksum)
				}
				if cb.Spec.Autoscaling != nil {
					t.Error("CeleryBeat should not have autoscaling (singleton)")
				}
				if cb.Spec.PodDisruptionBudget != nil {
					t.Error("CeleryBeat should not have PDB (singleton)")
				}

			case common.ComponentCeleryFlower:
				cf := obj.(*supersetv1alpha1.SupersetCeleryFlower)
				if cf.Spec.Config != "rendered-config" {
					t.Errorf("expected config, got %q", cf.Spec.Config)
				}
				if cf.Spec.ConfigChecksum != "sha256:abc" {
					t.Errorf("expected checksum, got %q", cf.Spec.ConfigChecksum)
				}
				if cf.Spec.Service == nil || cf.Spec.Service.Type != corev1.ServiceTypeLoadBalancer {
					t.Error("expected service to be set")
				}
				if cf.Spec.Autoscaling == nil {
					t.Error("expected autoscaling to be set")
				}
				if cf.Spec.PodDisruptionBudget == nil {
					t.Error("expected PDB to be set")
				}

			case common.ComponentMcpServer:
				ms := obj.(*supersetv1alpha1.SupersetMcpServer)
				if ms.Spec.Config != "rendered-config" {
					t.Errorf("expected config, got %q", ms.Spec.Config)
				}
				if ms.Spec.ConfigChecksum != "sha256:abc" {
					t.Errorf("expected checksum, got %q", ms.Spec.ConfigChecksum)
				}
				if ms.Spec.Service == nil || ms.Spec.Service.Type != corev1.ServiceTypeLoadBalancer {
					t.Error("expected service to be set")
				}
				if ms.Spec.Autoscaling == nil {
					t.Error("expected autoscaling to be set")
				}
				if ms.Spec.PodDisruptionBudget == nil {
					t.Error("expected PDB to be set")
				}

			case common.ComponentWebsocketServer:
				wss := obj.(*supersetv1alpha1.SupersetWebsocketServer)
				if wss.Spec.Image.Tag != "4.1.0" {
					t.Errorf("expected tag 4.1.0, got %s", wss.Spec.Image.Tag)
				}
				// WebsocketServer has NO Config/ConfigChecksum fields
				if wss.Spec.Service == nil || wss.Spec.Service.Type != corev1.ServiceTypeLoadBalancer {
					t.Error("expected service to be set")
				}
				if wss.Spec.Autoscaling == nil {
					t.Error("expected autoscaling to be set")
				}
				if wss.Spec.PodDisruptionBudget == nil {
					t.Error("expected PDB to be set")
				}
			}
		})
	}
}

func TestDescriptors_SetStatusFunctions(t *testing.T) {
	for _, desc := range componentDescriptors {
		t.Run(string(desc.componentType)+"/set", func(t *testing.T) {
			m := &supersetv1alpha1.ComponentStatusMap{}
			status := &supersetv1alpha1.ComponentRefStatus{Ready: "1/1", Ref: "test-ref"}

			desc.setStatus(m, status)

			got := getStatusForComponent(desc.componentType, m)
			if got == nil {
				t.Fatal("expected status to be set")
			}
			if got.Ready != "1/1" {
				t.Errorf("expected Ready=1/1, got %s", got.Ready)
			}
			if got.Ref != "test-ref" {
				t.Errorf("expected Ref=test-ref, got %s", got.Ref)
			}
		})

		t.Run(string(desc.componentType)+"/clear", func(t *testing.T) {
			m := &supersetv1alpha1.ComponentStatusMap{}
			// Set it first.
			desc.setStatus(m, &supersetv1alpha1.ComponentRefStatus{Ready: "1/1", Ref: "test"})
			// Clear it.
			desc.setStatus(m, nil)

			got := getStatusForComponent(desc.componentType, m)
			if got != nil {
				t.Error("expected status to be nil after clearing")
			}
		})
	}
}

// getStatusForComponent returns the ComponentRefStatus for a given component type.
func getStatusForComponent(ct common.ComponentType, m *supersetv1alpha1.ComponentStatusMap) *supersetv1alpha1.ComponentRefStatus {
	switch ct {
	case common.ComponentWebServer:
		return m.WebServer
	case common.ComponentCeleryWorker:
		return m.CeleryWorker
	case common.ComponentCeleryBeat:
		return m.CeleryBeat
	case common.ComponentCeleryFlower:
		return m.CeleryFlower
	case common.ComponentMcpServer:
		return m.McpServer
	case common.ComponentWebsocketServer:
		return m.WebsocketServer
	default:
		return nil
	}
}

// childReconcilerScheme builds a scheme with all types needed for reconcileChildResources tests.
func childReconcilerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := testScheme(t)
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(appsv1): %v", err)
	}
	if err := autoscalingv2.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(autoscalingv2): %v", err)
	}
	if err := policyv1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(policyv1): %v", err)
	}
	return s
}

func TestReconcileChildResources_Configs(t *testing.T) {
	replicas := int32(1)
	makeSpec := func() *supersetv1alpha1.FlatComponentSpec {
		return &supersetv1alpha1.FlatComponentSpec{
			Image: supersetv1alpha1.ImageSpec{
				Repository: "apache/superset",
				Tag:        "latest",
				PullPolicy: corev1.PullIfNotPresent,
			},
			Replicas: &replicas,
		}
	}

	tests := []struct {
		name           string
		cfg            childReconcilerConfig
		owner          client.Object
		config         string
		configChecksum string
		service        *supersetv1alpha1.ComponentServiceSpec
		autoscaling    *supersetv1alpha1.AutoscalingSpec
		pdb            *supersetv1alpha1.PDBSpec
		expectCM       bool
		expectService  bool
		expectHPA      bool
		expectPDB      bool
	}{
		{
			name: "WebServer-like: hasConfig=true, defaultPort>0, hasScaling=true",
			cfg: childReconcilerConfig{
				componentName: "web-server",
				deployConfig:  DeploymentConfig{ContainerName: "superset"},
				defaultPort:   8088,
				hasConfig:     true,
				hasScaling:    true,
			},
			owner:          &supersetv1alpha1.SupersetWebServer{ObjectMeta: metav1.ObjectMeta{Name: "test-ws", Namespace: "default"}},
			config:         "import os\n",
			configChecksum: "sha256:abc",
			autoscaling: &supersetv1alpha1.AutoscalingSpec{
				MaxReplicas: 5,
				Metrics:     []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType}},
			},
			pdb: &supersetv1alpha1.PDBSpec{
				MinAvailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			},
			expectCM:      true,
			expectService: true,
			expectHPA:     true,
			expectPDB:     true,
		},
		{
			name: "CeleryWorker-like: hasConfig=true, defaultPort=0, hasScaling=true, no HPA/PDB",
			cfg: childReconcilerConfig{
				componentName: "celery-worker",
				deployConfig:  DeploymentConfig{ContainerName: "superset"},
				defaultPort:   0,
				hasConfig:     true,
				hasScaling:    true,
			},
			owner:          &supersetv1alpha1.SupersetCeleryWorker{ObjectMeta: metav1.ObjectMeta{Name: "test-cw", Namespace: "default"}},
			config:         "import os\n",
			configChecksum: "sha256:def",
			expectCM:       true,
			expectService:  false,
			expectHPA:      false,
			expectPDB:      false,
		},
		{
			name: "CeleryBeat-like: hasConfig=true, defaultPort=0, hasScaling=false",
			cfg: childReconcilerConfig{
				componentName: "celery-beat",
				deployConfig:  DeploymentConfig{ContainerName: "superset"},
				defaultPort:   0,
				hasConfig:     true,
				hasScaling:    false,
			},
			owner:          &supersetv1alpha1.SupersetCeleryBeat{ObjectMeta: metav1.ObjectMeta{Name: "test-cb", Namespace: "default"}},
			config:         "import os\n",
			configChecksum: "sha256:ghi",
			expectCM:       true,
			expectService:  false,
			expectHPA:      false,
			expectPDB:      false,
		},
		{
			name: "WebsocketServer-like: hasConfig=false, defaultPort>0, hasScaling=true",
			cfg: childReconcilerConfig{
				componentName: "websocket-server",
				deployConfig:  DeploymentConfig{ContainerName: "superset"},
				defaultPort:   8080,
				hasConfig:     false,
				hasScaling:    true,
			},
			owner:         &supersetv1alpha1.SupersetWebsocketServer{ObjectMeta: metav1.ObjectMeta{Name: "test-wss", Namespace: "default"}},
			config:        "",
			expectCM:      false,
			expectService: true,
			expectHPA:     false,
			expectPDB:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := childReconcilerScheme(t)

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.owner).
				Build()

			recorder := record.NewFakeRecorder(10)

			spec := makeSpec()

			err := reconcileChildResources(
				context.Background(), c, scheme, recorder, tt.owner,
				spec, tt.cfg,
				tt.config, tt.configChecksum,
				tt.service, tt.autoscaling, tt.pdb,
			)
			if err != nil {
				t.Fatalf("reconcileChildResources() error: %v", err)
			}

			ctx := context.Background()
			resourceBaseName := common.ResourceBaseName(tt.owner.GetName(), common.ComponentType(tt.cfg.componentName))
			ns := tt.owner.GetNamespace()

			// Check ConfigMap.
			cm := &corev1.ConfigMap{}
			cmErr := c.Get(ctx, types.NamespacedName{Name: common.ConfigMapName(resourceBaseName), Namespace: ns}, cm)
			if tt.expectCM {
				if cmErr != nil {
					t.Errorf("expected ConfigMap to exist: %v", cmErr)
				} else if cm.Data["superset_config.py"] != tt.config {
					t.Errorf("expected ConfigMap data %q, got %q", tt.config, cm.Data["superset_config.py"])
				}
			} else {
				if cmErr == nil {
					t.Error("expected NO ConfigMap")
				} else if !errors.IsNotFound(cmErr) {
					t.Errorf("unexpected error checking ConfigMap: %v", cmErr)
				}
			}

			// Check Deployment (always created).
			deploy := &appsv1.Deployment{}
			if err := c.Get(ctx, types.NamespacedName{Name: resourceBaseName, Namespace: ns}, deploy); err != nil {
				t.Errorf("expected Deployment to exist: %v", err)
			}

			// Check Service.
			svc := &corev1.Service{}
			svcErr := c.Get(ctx, types.NamespacedName{Name: resourceBaseName, Namespace: ns}, svc)
			if tt.expectService {
				if svcErr != nil {
					t.Errorf("expected Service to exist: %v", svcErr)
				}
			} else {
				if !errors.IsNotFound(svcErr) && svcErr != nil {
					t.Errorf("unexpected Service error: %v", svcErr)
				} else if svcErr == nil {
					t.Error("expected NO Service")
				}
			}

			// Check HPA.
			hpa := &autoscalingv2.HorizontalPodAutoscaler{}
			hpaErr := c.Get(ctx, types.NamespacedName{Name: resourceBaseName, Namespace: ns}, hpa)
			if tt.expectHPA {
				if hpaErr != nil {
					t.Errorf("expected HPA to exist: %v", hpaErr)
				}
			} else if hpaErr == nil {
				t.Error("expected NO HPA")
			}

			// Check PDB.
			pdbObj := &policyv1.PodDisruptionBudget{}
			pdbErr := c.Get(ctx, types.NamespacedName{Name: resourceBaseName, Namespace: ns}, pdbObj)
			if tt.expectPDB {
				if pdbErr != nil {
					t.Errorf("expected PDB to exist: %v", pdbErr)
				}
			} else if pdbErr == nil {
				t.Error("expected NO PDB")
			}
		})
	}
}

func TestReconcileChildService_OperatorLabelsCannotBeOverridden(t *testing.T) {
	scheme := testScheme(t)
	owner := &supersetv1alpha1.SupersetWebServer{
		ObjectMeta: metav1.ObjectMeta{Name: "test-web", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()

	svcSpec := &supersetv1alpha1.ComponentServiceSpec{
		Labels: map[string]string{
			common.LabelKeyInstance:  "attacker-value",
			common.LabelKeyComponent: "attacker-component",
			"custom-label":           "allowed",
		},
	}

	err := reconcileChildService(context.Background(), c, scheme, owner,
		svcSpec, string(common.ComponentWebServer), common.PortWebServer, common.PortWebServer, owner.GetName())
	if err != nil {
		t.Fatalf("reconcileChildService: %v", err)
	}

	svc := &corev1.Service{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web", Namespace: "default"}, svc); err != nil {
		t.Fatalf("expected Service to exist: %v", err)
	}

	if svc.Labels[common.LabelKeyInstance] != "test-web" {
		t.Errorf("operator label %s was overridden: got %q, want %q",
			common.LabelKeyInstance, svc.Labels[common.LabelKeyInstance], "test-web")
	}
	if svc.Labels[common.LabelKeyComponent] != string(common.ComponentWebServer) {
		t.Errorf("operator label %s was overridden: got %q, want %q",
			common.LabelKeyComponent, svc.Labels[common.LabelKeyComponent], string(common.ComponentWebServer))
	}
	if svc.Labels["custom-label"] != "allowed" {
		t.Errorf("user custom label should be preserved, got %q", svc.Labels["custom-label"])
	}
}
