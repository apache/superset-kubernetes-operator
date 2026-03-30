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

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestWebServerServiceRef(t *testing.T) {
	customPort := int32(9090)
	tests := []struct {
		name     string
		superset *supersetv1alpha1.Superset
		wantName string
		wantPort int32
	}{
		{
			"defaults",
			&supersetv1alpha1.Superset{
				ObjectMeta: metav1.ObjectMeta{Name: "my-superset"},
				Spec:       supersetv1alpha1.SupersetSpec{WebServer: &supersetv1alpha1.WebServerComponentSpec{}},
			},
			"my-superset-web-server", common.PortWebServer,
		},
		{
			"custom port",
			&supersetv1alpha1.Superset{
				ObjectMeta: metav1.ObjectMeta{Name: "my-superset"},
				Spec: supersetv1alpha1.SupersetSpec{WebServer: &supersetv1alpha1.WebServerComponentSpec{
					Service: &supersetv1alpha1.ComponentServiceSpec{Port: &customPort},
				}},
			},
			"my-superset-web-server", 9090,
		},
		{
			"nil webServer",
			&supersetv1alpha1.Superset{
				ObjectMeta: metav1.ObjectMeta{Name: "my-superset"},
				Spec:       supersetv1alpha1.SupersetSpec{},
			},
			"my-superset-web-server", common.PortWebServer,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, port := webServerServiceRef(tt.superset)
			if name != tt.wantName {
				t.Errorf("expected name %s, got %s", tt.wantName, name)
			}
			if port != tt.wantPort {
				t.Errorf("expected port %d, got %d", tt.wantPort, port)
			}
		})
	}
}

func TestReconcileNetworking_NothingEnabled(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       minimalSupersetSpec(),
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileNetworking(context.Background(), superset); err != nil {
		t.Fatalf("reconcileNetworking: %v", err)
	}
}

func TestReconcileNetworking_GatewayEnabled_CreatesHTTPRoute(t *testing.T) {
	scheme := testScheme(t)

	gwNamespace := gatewayv1.Namespace("gateway-system")
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Init:      &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Networking: &supersetv1alpha1.NetworkingSpec{
				Gateway: &supersetv1alpha1.GatewaySpec{
					GatewayRef: gatewayv1.ParentReference{
						Name:      "my-gateway",
						Namespace: &gwNamespace,
					},
					Hostnames: []gatewayv1.Hostname{"superset.example.com"},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileNetworking(context.Background(), superset); err != nil {
		t.Fatalf("reconcileNetworking: %v", err)
	}

	route := &gatewayv1.HTTPRoute{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, route); err != nil {
		t.Fatalf("expected HTTPRoute: %v", err)
	}

	if len(route.Spec.Hostnames) != 1 || route.Spec.Hostnames[0] != "superset.example.com" {
		t.Errorf("expected hostname superset.example.com, got %v", route.Spec.Hostnames)
	}
	if len(route.Spec.ParentRefs) != 1 || route.Spec.ParentRefs[0].Name != "my-gateway" {
		t.Errorf("expected parentRef my-gateway")
	}

	// Should have at least a web server rule with "/" path.
	if len(route.Spec.Rules) < 1 {
		t.Fatalf("expected at least 1 rule, got %d", len(route.Spec.Rules))
	}
	lastRule := route.Spec.Rules[len(route.Spec.Rules)-1]
	if *lastRule.Matches[0].Path.Value != "/" {
		t.Errorf("expected last rule path /, got %s", *lastRule.Matches[0].Path.Value)
	}
	expectedSvc := gatewayv1.ObjectName("test-web-server")
	if lastRule.BackendRefs[0].Name != expectedSvc {
		t.Errorf("expected backend test-web-server, got %s", lastRule.BackendRefs[0].Name)
	}
}

func TestReconcileHTTPRoute_WithWebsocket(t *testing.T) {
	scheme := testScheme(t)

	gwNamespace := gatewayv1.Namespace("gateway-system")
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:           supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer:       &supersetv1alpha1.WebServerComponentSpec{},
			WebsocketServer: &supersetv1alpha1.WebsocketServerComponentSpec{},
			Init:            &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Networking: &supersetv1alpha1.NetworkingSpec{
				Gateway: &supersetv1alpha1.GatewaySpec{
					GatewayRef: gatewayv1.ParentReference{
						Name:      "my-gateway",
						Namespace: &gwNamespace,
					},
					Hostnames: []gatewayv1.Hostname{"superset.example.com"},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileHTTPRoute(context.Background(), superset); err != nil {
		t.Fatalf("reconcileHTTPRoute: %v", err)
	}

	route := &gatewayv1.HTTPRoute{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, route); err != nil {
		t.Fatalf("expected HTTPRoute: %v", err)
	}

	// With websocket enabled, should have: /ws rule first, then / rule.
	if len(route.Spec.Rules) < 2 {
		t.Fatalf("expected at least 2 rules (ws + default), got %d", len(route.Spec.Rules))
	}
	wsRule := route.Spec.Rules[0]
	if *wsRule.Matches[0].Path.Value != "/ws" {
		t.Errorf("expected first rule path /ws, got %s", *wsRule.Matches[0].Path.Value)
	}
	expectedWsSvc := gatewayv1.ObjectName("test-websocket-server")
	if wsRule.BackendRefs[0].Name != expectedWsSvc {
		t.Errorf("expected ws backend test-websocket-server, got %s", wsRule.BackendRefs[0].Name)
	}
}

func TestReconcileHTTPRoute_WithMcpServer(t *testing.T) {
	scheme := testScheme(t)

	gwNamespace := gatewayv1.Namespace("gateway-system")
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			McpServer: &supersetv1alpha1.McpServerComponentSpec{},
			Init:      &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Networking: &supersetv1alpha1.NetworkingSpec{
				Gateway: &supersetv1alpha1.GatewaySpec{
					GatewayRef: gatewayv1.ParentReference{
						Name:      "my-gateway",
						Namespace: &gwNamespace,
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileHTTPRoute(context.Background(), superset); err != nil {
		t.Fatalf("reconcileHTTPRoute: %v", err)
	}

	route := &gatewayv1.HTTPRoute{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, route); err != nil {
		t.Fatalf("expected HTTPRoute: %v", err)
	}

	// With MCP enabled, should have /mcp rule and / rule.
	if len(route.Spec.Rules) < 2 {
		t.Fatalf("expected at least 2 rules (mcp + default), got %d", len(route.Spec.Rules))
	}
	mcpRule := route.Spec.Rules[0]
	if *mcpRule.Matches[0].Path.Value != "/mcp" {
		t.Errorf("expected first rule path /mcp, got %s", *mcpRule.Matches[0].Path.Value)
	}
	expectedMcpSvc := gatewayv1.ObjectName("test-mcp-server")
	if mcpRule.BackendRefs[0].Name != expectedMcpSvc {
		t.Errorf("expected mcp backend test-mcp-server, got %s", mcpRule.BackendRefs[0].Name)
	}
}

func TestReconcileHTTPRoute_WithCeleryFlower(t *testing.T) {
	scheme := testScheme(t)

	gwNamespace := gatewayv1.Namespace("gateway-system")
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:        supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer:    &supersetv1alpha1.WebServerComponentSpec{},
			CeleryFlower: &supersetv1alpha1.CeleryFlowerComponentSpec{},
			Init:         &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Networking: &supersetv1alpha1.NetworkingSpec{
				Gateway: &supersetv1alpha1.GatewaySpec{
					GatewayRef: gatewayv1.ParentReference{
						Name:      "my-gateway",
						Namespace: &gwNamespace,
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileHTTPRoute(context.Background(), superset); err != nil {
		t.Fatalf("reconcileHTTPRoute: %v", err)
	}

	route := &gatewayv1.HTTPRoute{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, route); err != nil {
		t.Fatalf("expected HTTPRoute: %v", err)
	}

	if len(route.Spec.Rules) < 2 {
		t.Fatalf("expected at least 2 rules (flower + default), got %d", len(route.Spec.Rules))
	}
	flowerRule := route.Spec.Rules[0]
	if *flowerRule.Matches[0].Path.Value != "/flower" {
		t.Errorf("expected first rule path /flower, got %s", *flowerRule.Matches[0].Path.Value)
	}
	expectedFlowerSvc := gatewayv1.ObjectName("test-celery-flower")
	if flowerRule.BackendRefs[0].Name != expectedFlowerSvc {
		t.Errorf("expected flower backend test-celery-flower, got %s", flowerRule.BackendRefs[0].Name)
	}
	if *flowerRule.BackendRefs[0].Port != common.PortCeleryFlower {
		t.Errorf("expected port %d, got %d", common.PortCeleryFlower, *flowerRule.BackendRefs[0].Port)
	}
}

func TestReconcileHTTPRoute_CustomGatewayPaths(t *testing.T) {
	scheme := testScheme(t)

	gwNamespace := gatewayv1.Namespace("gateway-system")
	customWsPath := "/websocket"
	customMcpPath := "/api/mcp"
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			WebsocketServer: &supersetv1alpha1.WebsocketServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{},
				ComponentSpec:         supersetv1alpha1.ComponentSpec{},
				Service:               &supersetv1alpha1.ComponentServiceSpec{GatewayPath: &customWsPath},
			},
			McpServer: &supersetv1alpha1.McpServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{},
				ComponentSpec:         supersetv1alpha1.ComponentSpec{},
				Service:               &supersetv1alpha1.ComponentServiceSpec{GatewayPath: &customMcpPath},
			},
			Init: &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Networking: &supersetv1alpha1.NetworkingSpec{
				Gateway: &supersetv1alpha1.GatewaySpec{
					GatewayRef: gatewayv1.ParentReference{
						Name:      "my-gateway",
						Namespace: &gwNamespace,
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileHTTPRoute(context.Background(), superset); err != nil {
		t.Fatalf("reconcileHTTPRoute: %v", err)
	}

	route := &gatewayv1.HTTPRoute{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, route); err != nil {
		t.Fatalf("expected HTTPRoute: %v", err)
	}

	if len(route.Spec.Rules) != 3 {
		t.Fatalf("expected 3 rules (ws + mcp + default), got %d", len(route.Spec.Rules))
	}
	if *route.Spec.Rules[0].Matches[0].Path.Value != "/websocket" {
		t.Errorf("expected custom ws path /websocket, got %s", *route.Spec.Rules[0].Matches[0].Path.Value)
	}
	if *route.Spec.Rules[1].Matches[0].Path.Value != "/api/mcp" {
		t.Errorf("expected custom mcp path /api/mcp, got %s", *route.Spec.Rules[1].Matches[0].Path.Value)
	}
}

func TestResolveGatewayPath(t *testing.T) {
	custom := "/custom"
	tests := []struct {
		name        string
		svc         *supersetv1alpha1.ComponentServiceSpec
		defaultPath string
		want        string
	}{
		{"nil service", nil, "/ws", "/ws"},
		{"nil gatewayPath", &supersetv1alpha1.ComponentServiceSpec{}, "/ws", "/ws"},
		{"custom path", &supersetv1alpha1.ComponentServiceSpec{GatewayPath: &custom}, "/ws", "/custom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveGatewayPath(tt.svc, tt.defaultPath)
			if got != tt.want {
				t.Errorf("resolveGatewayPath() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestReconcileIngress_CreatesIngress(t *testing.T) {
	scheme := testScheme(t)

	className := "nginx"
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Init:      &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Networking: &supersetv1alpha1.NetworkingSpec{
				Ingress: &supersetv1alpha1.IngressSpec{
					ClassName: &className,
					Hosts: []supersetv1alpha1.IngressHost{
						{
							Host: "superset.example.com",
							Paths: []supersetv1alpha1.IngressPath{
								{Path: "/", PathType: pathTypePtr(networkingv1.PathTypePrefix)},
							},
						},
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileIngress(context.Background(), superset); err != nil {
		t.Fatalf("reconcileIngress: %v", err)
	}

	ingress := &networkingv1.Ingress{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, ingress); err != nil {
		t.Fatalf("expected Ingress: %v", err)
	}

	if *ingress.Spec.IngressClassName != "nginx" {
		t.Errorf("expected className nginx, got %s", *ingress.Spec.IngressClassName)
	}
	if len(ingress.Spec.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(ingress.Spec.Rules))
	}
	if ingress.Spec.Rules[0].Host != "superset.example.com" {
		t.Errorf("expected host superset.example.com, got %s", ingress.Spec.Rules[0].Host)
	}
	paths := ingress.Spec.Rules[0].HTTP.Paths
	if len(paths) != 1 || paths[0].Path != "/" {
		t.Errorf("expected path /, got %v", paths)
	}
	if paths[0].Backend.Service.Name != "test-web-server" {
		t.Errorf("expected backend test-web-server, got %s", paths[0].Backend.Service.Name)
	}
	if paths[0].Backend.Service.Port.Number != common.PortWebServer {
		t.Errorf("expected port %d, got %d", common.PortWebServer, paths[0].Backend.Service.Port.Number)
	}
}

func TestReconcileIngress_HostFallback(t *testing.T) {
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Init:      &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Networking: &supersetv1alpha1.NetworkingSpec{
				Ingress: &supersetv1alpha1.IngressSpec{
					Host: "fallback.example.com",
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileIngress(context.Background(), superset); err != nil {
		t.Fatalf("reconcileIngress: %v", err)
	}

	ingress := &networkingv1.Ingress{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, ingress); err != nil {
		t.Fatalf("expected Ingress: %v", err)
	}

	if len(ingress.Spec.Rules) != 1 {
		t.Fatalf("expected 1 rule from host fallback, got %d", len(ingress.Spec.Rules))
	}
	if ingress.Spec.Rules[0].Host != "fallback.example.com" {
		t.Errorf("expected host fallback.example.com, got %s", ingress.Spec.Rules[0].Host)
	}
	// Default path "/" should be created.
	paths := ingress.Spec.Rules[0].HTTP.Paths
	if len(paths) != 1 || paths[0].Path != "/" {
		t.Errorf("expected default path /, got %v", paths)
	}
}

func TestReconcileIngress_WithTLS(t *testing.T) {
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Init:      &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Networking: &supersetv1alpha1.NetworkingSpec{
				Ingress: &supersetv1alpha1.IngressSpec{
					Hosts: []supersetv1alpha1.IngressHost{
						{Host: "superset.example.com"},
					},
					TLS: []networkingv1.IngressTLS{
						{
							Hosts:      []string{"superset.example.com"},
							SecretName: "superset-tls",
						},
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileIngress(context.Background(), superset); err != nil {
		t.Fatalf("reconcileIngress: %v", err)
	}

	ingress := &networkingv1.Ingress{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, ingress); err != nil {
		t.Fatalf("expected Ingress: %v", err)
	}

	if len(ingress.Spec.TLS) != 1 {
		t.Fatalf("expected 1 TLS entry, got %d", len(ingress.Spec.TLS))
	}
	if ingress.Spec.TLS[0].SecretName != "superset-tls" {
		t.Errorf("expected TLS secret superset-tls, got %s", ingress.Spec.TLS[0].SecretName)
	}
}

func TestReconcileNetworking_GatewayDisabled_CleansUpHTTPRoute(t *testing.T) {
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       minimalSupersetSpec(),
	}

	existingRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default",
			Labels: parentLabels("test"),
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, existingRoute).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileNetworking(context.Background(), superset); err != nil {
		t.Fatalf("reconcileNetworking: %v", err)
	}

	route := &gatewayv1.HTTPRoute{}
	err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, route)
	if !errors.IsNotFound(err) {
		t.Fatalf("expected HTTPRoute to be deleted, got: %v", err)
	}
}

func TestReconcileNetworking_IngressDisabled_CleansUpIngress(t *testing.T) {
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       minimalSupersetSpec(),
	}

	existingIngress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default",
			Labels: parentLabels("test"),
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, existingIngress).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileNetworking(context.Background(), superset); err != nil {
		t.Fatalf("reconcileNetworking: %v", err)
	}

	ingress := &networkingv1.Ingress{}
	err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, ingress)
	if !errors.IsNotFound(err) {
		t.Fatalf("expected Ingress to be deleted, got: %v", err)
	}
}

func TestDeleteByLabels_SkipsUnlabeledResource(t *testing.T) {
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}

	// Ingress with same name but no operator labels — not discoverable.
	unlabeledIngress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, unlabeledIngress).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	err := r.deleteByLabels(context.Background(), "default", parentLabels("test"),
		func() client.ObjectList { return &networkingv1.IngressList{} }, "")
	if err != nil {
		t.Fatalf("expected no error: %v", err)
	}

	// Resource should still exist — it has no matching labels.
	ingress := &networkingv1.Ingress{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, ingress); err != nil {
		t.Fatalf("expected unlabeled Ingress to be preserved, got: %v", err)
	}
}

func pathTypePtr(pt networkingv1.PathType) *networkingv1.PathType { return &pt }
