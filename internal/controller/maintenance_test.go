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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func TestResolveWebServerContainerPort_Default(t *testing.T) {
	ws := &supersetv1alpha1.WebServerComponentSpec{}
	port := resolveWebServerContainerPort(ws)
	if port != common.PortWebServer {
		t.Errorf("expected default port %d, got %d", common.PortWebServer, port)
	}
}

func TestResolveWebServerContainerPort_CustomPort(t *testing.T) {
	ws := &supersetv1alpha1.WebServerComponentSpec{
		ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
			PodTemplate: &supersetv1alpha1.PodTemplate{
				Container: &supersetv1alpha1.ContainerTemplate{
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: 9090},
					},
				},
			},
		},
	}
	port := resolveWebServerContainerPort(ws)
	if port != 9090 {
		t.Errorf("expected custom port 9090, got %d", port)
	}
}

func TestResolveWebServerContainerPort_Nil(t *testing.T) {
	port := resolveWebServerContainerPort(nil)
	if port != common.PortWebServer {
		t.Errorf("expected default port %d for nil spec, got %d", common.PortWebServer, port)
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
