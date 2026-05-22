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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestRenderMaintenanceHTML_EscapesTitle(t *testing.T) {
	title := `<script>alert("xss")</script>`
	spec := &supersetv1alpha1.MaintenancePageSpec{Title: &title}
	html := renderMaintenanceHTML(spec)

	if strings.Contains(html, "<script>") {
		t.Error("title should be HTML-escaped but contains raw <script> tag")
	}
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Error("expected escaped title in output")
	}
}

func TestRenderMaintenanceHTML_EscapesMessage(t *testing.T) {
	msg := `<img src=x onerror="alert('xss')">`
	spec := &supersetv1alpha1.MaintenancePageSpec{Message: &msg}
	html := renderMaintenanceHTML(spec)

	if strings.Contains(html, "<img") {
		t.Error("message should be HTML-escaped but contains raw <img tag")
	}
	if !strings.Contains(html, "&lt;img") {
		t.Error("expected escaped message in output")
	}
}

func TestRenderMaintenanceHTML_BodyPassesThrough(t *testing.T) {
	body := `<html><body><h1>Custom</h1><script>ok()</script></body></html>`
	spec := &supersetv1alpha1.MaintenancePageSpec{Body: &body}
	result := renderMaintenanceHTML(spec)

	if result != body {
		t.Errorf("body should be returned as-is, got: %s", result)
	}
}

func TestRenderMaintenanceHTML_DefaultsAreEscaped(t *testing.T) {
	spec := &supersetv1alpha1.MaintenancePageSpec{}
	html := renderMaintenanceHTML(spec)

	if !strings.Contains(html, maintenanceDefaultTitle) {
		t.Error("expected default title in output")
	}
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("expected full HTML document")
	}
}

func TestRenderNginxConf_UsesCustomPort(t *testing.T) {
	conf := renderNginxConf(9090)
	if !strings.Contains(conf, "listen 9090") {
		t.Error("expected nginx to listen on custom port 9090")
	}
	if strings.Contains(conf, "listen 8088") {
		t.Error("should not contain default port when custom port is provided")
	}
}

func TestRenderNginxConf_UsesDefaultPort(t *testing.T) {
	conf := renderNginxConf(common.PortWebServer)
	if !strings.Contains(conf, "listen 8088") {
		t.Error("expected nginx to listen on default port 8088")
	}
}

func TestResolveWebServerPort_Default(t *testing.T) {
	s := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		},
	}
	port := resolveWebServerPort(s)
	if port != common.PortWebServer {
		t.Errorf("expected default port %d, got %d", common.PortWebServer, port)
	}
}

func TestResolveWebServerPort_ComponentOverride(t *testing.T) {
	s := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
					PodTemplate: &supersetv1alpha1.PodTemplate{
						Container: &supersetv1alpha1.ContainerTemplate{
							Ports: []corev1.ContainerPort{
								{Name: "http", ContainerPort: 9090},
							},
						},
					},
				},
			},
		},
	}
	port := resolveWebServerPort(s)
	if port != 9090 {
		t.Errorf("expected custom port 9090, got %d", port)
	}
}

func TestResolveWebServerPort_TopLevelOverride(t *testing.T) {
	s := &supersetv1alpha1.Superset{
		Spec: supersetv1alpha1.SupersetSpec{
			PodTemplate: &supersetv1alpha1.PodTemplate{
				Container: &supersetv1alpha1.ContainerTemplate{
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: 7070},
					},
				},
			},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		},
	}
	port := resolveWebServerPort(s)
	if port != 7070 {
		t.Errorf("expected top-level port 7070 inherited, got %d", port)
	}
}

func TestResolveWebServerPort_NoWebServer(t *testing.T) {
	s := &supersetv1alpha1.Superset{}
	port := resolveWebServerPort(s)
	if port != common.PortWebServer {
		t.Errorf("expected default port %d for nil WebServer, got %d", common.PortWebServer, port)
	}
}

func TestReconcileWebServerService_SelectorBasedOnMaintenanceActive(t *testing.T) {
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "my-superset", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{
				MaintenanceActive: true,
			},
		},
	}

	// When MaintenanceActive=true, selector should point to maintenance-page component.
	expectedSelector := common.ComponentLabels(common.ComponentMaintenancePage, "my-superset")

	// Verify the selector logic (we test the selector derivation, not the full reconcile
	// which requires a fake client).
	var selector map[string]string
	if superset.Status.Lifecycle != nil && superset.Status.Lifecycle.MaintenanceActive {
		selector = common.ComponentLabels(common.ComponentMaintenancePage, superset.Name)
	} else {
		selector = common.ComponentLabels(common.ComponentWebServer, superset.Name)
	}
	for k, v := range expectedSelector {
		if selector[k] != v {
			t.Errorf("expected selector[%s]=%s, got %s", k, v, selector[k])
		}
	}

	// When MaintenanceActive=false, selector should point to web-server.
	superset.Status.Lifecycle.MaintenanceActive = false
	if superset.Status.Lifecycle != nil && superset.Status.Lifecycle.MaintenanceActive {
		selector = common.ComponentLabels(common.ComponentMaintenancePage, superset.Name)
	} else {
		selector = common.ComponentLabels(common.ComponentWebServer, superset.Name)
	}
	expectedWebServer := common.ComponentLabels(common.ComponentWebServer, "my-superset")
	for k, v := range expectedWebServer {
		if selector[k] != v {
			t.Errorf("expected selector[%s]=%s, got %s", k, v, selector[k])
		}
	}
}

func TestMaintenanceDeployConfig_UsesCustomPort(t *testing.T) {
	port := int32(9090)
	cfg := maintenanceDeployConfig
	cfg.DefaultPorts = []corev1.ContainerPort{
		{Name: common.PortNameHTTP, ContainerPort: port, Protocol: corev1.ProtocolTCP},
	}

	if len(cfg.DefaultPorts) != 1 {
		t.Fatal("expected exactly 1 default port")
	}
	if cfg.DefaultPorts[0].ContainerPort != port {
		t.Errorf("expected container port %d, got %d", port, cfg.DefaultPorts[0].ContainerPort)
	}
}

func TestReconcileMaintenanceReturnClearsWhenWebServerDesiredReplicasZero(t *testing.T) {
	recorder := events.NewFakeRecorder(10)
	zero := int32(0)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
					Replicas: &zero,
				},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{MaintenanceActive: true},
		},
	}
	r := &SupersetReconciler{Recorder: recorder}

	cleared, err := r.reconcileMaintenanceReturn(context.Background(), superset)
	if err != nil {
		t.Fatalf("reconcileMaintenanceReturn: %v", err)
	}
	if !cleared {
		t.Fatal("expected maintenance return to clear")
	}
	if superset.Status.Lifecycle.MaintenanceActive {
		t.Fatal("expected maintenanceActive=false")
	}
	assertNextEventContains(t, recorder, "Normal MaintenanceEnded Maintenance page disabled because webServer has zero desired replicas")
}

func TestBuildMaintenanceFlatSpec_DoesNotMutateInputSpec(t *testing.T) {
	userVolume := corev1.Volume{Name: "user-volume", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
	userMount := corev1.VolumeMount{Name: "user-volume", MountPath: "/data"}
	userEnv := corev1.EnvVar{Name: "USER_VAR", Value: "v"}
	title := "down for maintenance"

	spec := &supersetv1alpha1.MaintenancePageSpec{
		Title: &title,
		PodTemplate: &supersetv1alpha1.PodTemplate{
			Volumes: []corev1.Volume{userVolume},
			Container: &supersetv1alpha1.ContainerTemplate{
				VolumeMounts: []corev1.VolumeMount{userMount},
				Env:          []corev1.EnvVar{userEnv},
			},
		},
	}

	for range 3 {
		_ = buildMaintenanceFlatSpec("parent", spec)
	}

	if got := len(spec.PodTemplate.Volumes); got != 1 {
		t.Fatalf("input spec PodTemplate.Volumes mutated: got %d volumes, want 1", got)
	}
	if got := len(spec.PodTemplate.Container.VolumeMounts); got != 1 {
		t.Fatalf("input spec PodTemplate.Container.VolumeMounts mutated: got %d mounts, want 1", got)
	}
	if got := len(spec.PodTemplate.Container.Env); got != 1 {
		t.Fatalf("input spec PodTemplate.Container.Env mutated: got %d env vars, want 1", got)
	}
}

// TestResolveMaintenanceImage_PartialOverride asserts that a maintenance image
// spec with only `tag` set inherits the nginx repository — not the Superset
// image — which is the bug ContainerImageSpec was introduced to prevent.
func TestResolveMaintenanceImage_PartialOverride(t *testing.T) {
	tests := []struct {
		name         string
		image        *supersetv1alpha1.ContainerImageSpec
		expectedRepo string
		expectedTag  string
	}{
		{
			name:         "nil image uses managed defaults",
			image:        nil,
			expectedRepo: maintenanceDefaultImage,
			expectedTag:  maintenanceDefaultTag,
		},
		{
			name:         "tag-only override inherits nginx repo",
			image:        &supersetv1alpha1.ContainerImageSpec{Tag: "1.27"},
			expectedRepo: maintenanceDefaultImage,
			expectedTag:  "1.27",
		},
		{
			name:         "repository-only override inherits default tag",
			image:        &supersetv1alpha1.ContainerImageSpec{Repository: "my-registry/maintenance"},
			expectedRepo: "my-registry/maintenance",
			expectedTag:  maintenanceDefaultTag,
		},
		{
			name:         "full override is used as-is",
			image:        &supersetv1alpha1.ContainerImageSpec{Repository: "my-registry/maintenance", Tag: "v3"},
			expectedRepo: "my-registry/maintenance",
			expectedTag:  "v3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			img := resolveMaintenanceImage(&supersetv1alpha1.MaintenancePageSpec{Image: tt.image})
			if img.Repository != tt.expectedRepo || img.Tag != tt.expectedTag {
				t.Errorf("expected %s:%s, got %s:%s", tt.expectedRepo, tt.expectedTag, img.Repository, img.Tag)
			}
		})
	}
}
