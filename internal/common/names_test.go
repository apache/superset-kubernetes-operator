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

package common

import (
	"testing"
)

func TestDerivedName(t *testing.T) {
	tests := []struct {
		parent string
		suffix string
		want   string
	}{
		{"my-superset", SuffixWebServer, "my-superset-web-server"},
		{"my-superset", SuffixCeleryWorker, "my-superset-celery-worker"},
		{"my-superset", SuffixCeleryBeat, "my-superset-celery-beat"},
		{"my-superset", SuffixCeleryFlower, "my-superset-celery-flower"},
		{"my-superset", SuffixWebsocketServer, "my-superset-websocket-server"},
		{"my-superset", SuffixMcpServer, "my-superset-mcp-server"},
		{"test", SuffixConfig, "test-config"},
		{"test", SuffixNetworkPolicy, "test-netpol"},
	}

	for _, tt := range tests {
		t.Run(tt.parent+tt.suffix, func(t *testing.T) {
			got := DerivedName(tt.parent, tt.suffix)
			if got != tt.want {
				t.Errorf("DerivedName(%q, %q) = %q, want %q", tt.parent, tt.suffix, got, tt.want)
			}
		})
	}
}

func TestSubResourceName(t *testing.T) {
	tests := []struct {
		instanceName string
		suffix       string
		want         string
	}{
		{"my-superset", "web-server", "my-superset-web-server"},
		{"my-superset", "celery-worker", "my-superset-celery-worker"},
		{"my-superset", "init", "my-superset-init"},
		{"custom", "ws", "custom-ws"},
	}

	for _, tt := range tests {
		t.Run(tt.instanceName+"-"+tt.suffix, func(t *testing.T) {
			got := SubResourceName(tt.instanceName, tt.suffix)
			if got != tt.want {
				t.Errorf("SubResourceName(%q, %q) = %q, want %q", tt.instanceName, tt.suffix, got, tt.want)
			}
		})
	}
}

func TestResourceBaseName(t *testing.T) {
	tests := []struct {
		name          string
		instanceName  string
		componentType ComponentType
		want          string
	}{
		{"web-server", "my-superset", ComponentWebServer, "my-superset-web-server"},
		{"celery-worker", "my-superset", ComponentCeleryWorker, "my-superset-celery-worker"},
		{"custom name", "frontend", ComponentWebServer, "frontend-web-server"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResourceBaseName(tt.instanceName, tt.componentType)
			if got != tt.want {
				t.Errorf("ResourceBaseName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConfigMapName(t *testing.T) {
	tests := []struct {
		instanceName string
		want         string
	}{
		{"my-superset-web-server", "my-superset-web-server-config"},
		{"my-superset-celery-worker", "my-superset-celery-worker-config"},
		{"my-superset-mcp-server", "my-superset-mcp-server-config"},
		{"test", "test-config"},
	}

	for _, tt := range tests {
		t.Run(tt.instanceName, func(t *testing.T) {
			got := ConfigMapName(tt.instanceName)
			if got != tt.want {
				t.Errorf("ConfigMapName(%q) = %q, want %q", tt.instanceName, got, tt.want)
			}
		})
	}
}

func TestComponentLabels(t *testing.T) {
	tests := []struct {
		name      string // also the expected app.kubernetes.io/component value
		component ComponentType
	}{
		{"web-server", ComponentWebServer},
		{"celery-worker", ComponentCeleryWorker},
		{"celery-beat", ComponentCeleryBeat},
		{"celery-flower", ComponentCeleryFlower},
		{"websocket-server", ComponentWebsocketServer},
		{"mcp-server", ComponentMcpServer},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels := ComponentLabels(tt.component, "my-superset")

			// Verify all 3 label keys are present.
			if len(labels) != 3 {
				t.Fatalf("expected 3 labels, got %d", len(labels))
			}

			if got := labels["app.kubernetes.io/name"]; got != "superset" {
				t.Errorf("expected app.kubernetes.io/name=superset, got %s", got)
			}
			if got := labels["app.kubernetes.io/component"]; got != tt.name {
				t.Errorf("expected app.kubernetes.io/component=%s, got %s", tt.name, got)
			}
			if got := labels["app.kubernetes.io/instance"]; got != "my-superset" {
				t.Errorf("expected app.kubernetes.io/instance=my-superset, got %s", got)
			}
		})
	}
}
