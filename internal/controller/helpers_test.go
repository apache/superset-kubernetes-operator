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

	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestComponentLabels(t *testing.T) {
	labels := componentLabels(string(common.ComponentWebServer), "my-superset-web-server")

	if labels[common.LabelKeyName] != common.LabelValueApp {
		t.Errorf("expected name=%s, got %s", common.LabelValueApp, labels[common.LabelKeyName])
	}
	if labels[common.LabelKeyComponent] != string(common.ComponentWebServer) {
		t.Errorf("expected component=%s, got %s", string(common.ComponentWebServer), labels[common.LabelKeyComponent])
	}
	if labels[common.LabelKeyInstance] != "my-superset-web-server" {
		t.Errorf("expected instance=my-superset-web-server, got %s", labels[common.LabelKeyInstance])
	}
}

func TestMergeLabels(t *testing.T) {
	base := map[string]string{"a": "1", "b": "2"}
	extra := map[string]string{"b": "overridden", "c": "3"}

	merged := mergeLabels(base, extra)
	if len(merged) != 3 {
		t.Fatalf("expected 3 labels, got %d", len(merged))
	}
	if merged["a"] != "1" {
		t.Errorf("expected a=1, got %s", merged["a"])
	}
	if merged["b"] != "overridden" {
		t.Errorf("expected b=overridden, got %s", merged["b"])
	}

	// Nil extra returns base labels.
	merged2 := mergeLabels(base, nil)
	if merged2["a"] != "1" {
		t.Errorf("expected base labels returned for nil extra, got %v", merged2)
	}
}

func TestMergeAnnotations(t *testing.T) {
	base := map[string]string{"a": "1"}
	extra := map[string]string{"b": "2"}

	merged := mergeAnnotations(base, extra)
	if len(merged) != 2 {
		t.Fatalf("expected 2 annotations, got %d", len(merged))
	}

	// Both nil returns nil.
	if mergeAnnotations(nil, nil) != nil {
		t.Error("expected nil for both-nil input")
	}
}
