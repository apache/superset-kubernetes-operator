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

func TestChildName(t *testing.T) {
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
			got := ChildName(tt.parent, tt.suffix)
			if got != tt.want {
				t.Errorf("ChildName(%q, %q) = %q, want %q", tt.parent, tt.suffix, got, tt.want)
			}
		})
	}
}

func TestSubResourceName(t *testing.T) {
	tests := []struct {
		childName string
		suffix    string
		want      string
	}{
		{"my-superset", "web-server", "my-superset-web-server"},
		{"my-superset", "celery-worker", "my-superset-celery-worker"},
		{"my-superset", "init", "my-superset-init"},
		{"custom", "ws", "custom-ws"},
	}

	for _, tt := range tests {
		t.Run(tt.childName+"-"+tt.suffix, func(t *testing.T) {
			got := SubResourceName(tt.childName, tt.suffix)
			if got != tt.want {
				t.Errorf("SubResourceName(%q, %q) = %q, want %q", tt.childName, tt.suffix, got, tt.want)
			}
		})
	}
}

func TestResourceBaseName(t *testing.T) {
	tests := []struct {
		name          string
		childName     string
		componentType ComponentType
		want          string
	}{
		{"web-server", "my-superset", ComponentWebServer, "my-superset-web-server"},
		{"celery-worker", "my-superset", ComponentCeleryWorker, "my-superset-celery-worker"},
		{"custom name", "frontend", ComponentWebServer, "frontend-web-server"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResourceBaseName(tt.childName, tt.componentType)
			if got != tt.want {
				t.Errorf("ResourceBaseName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConfigMapName(t *testing.T) {
	tests := []struct {
		childName string
		want      string
	}{
		{"my-superset-web-server", "my-superset-web-server-config"},
		{"my-superset-celery-worker", "my-superset-celery-worker-config"},
		{"my-superset-mcp-server", "my-superset-mcp-server-config"},
		{"test", "test-config"},
	}

	for _, tt := range tests {
		t.Run(tt.childName, func(t *testing.T) {
			got := ConfigMapName(tt.childName)
			if got != tt.want {
				t.Errorf("ConfigMapName(%q) = %q, want %q", tt.childName, got, tt.want)
			}
		})
	}
}

func TestComponentLabels(t *testing.T) {
	tests := []struct {
		name      string
		component ComponentType
		instance  string
	}{
		{"web-server", ComponentWebServer, "my-superset"},
		{"celery-worker", ComponentCeleryWorker, "my-superset"},
		{"celery-beat", ComponentCeleryBeat, "my-superset"},
		{"celery-flower", ComponentCeleryFlower, "my-superset"},
		{"websocket-server", ComponentWebsocketServer, "my-superset"},
		{"mcp-server", ComponentMcpServer, "my-superset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels := ComponentLabels(tt.component, tt.instance)

			// Verify all 3 label keys are present.
			if len(labels) != 3 {
				t.Fatalf("expected 3 labels, got %d", len(labels))
			}

			// Verify name label.
			if labels[LabelKeyName] != LabelValueApp {
				t.Errorf("expected %s=%s, got %s", LabelKeyName, LabelValueApp, labels[LabelKeyName])
			}

			// Verify component label.
			if labels[LabelKeyComponent] != string(tt.component) {
				t.Errorf("expected %s=%s, got %s", LabelKeyComponent, string(tt.component), labels[LabelKeyComponent])
			}

			// Verify instance label.
			if labels[LabelKeyInstance] != tt.instance {
				t.Errorf("expected %s=%s, got %s", LabelKeyInstance, tt.instance, labels[LabelKeyInstance])
			}
		})
	}
}
