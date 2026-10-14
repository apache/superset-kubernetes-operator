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

package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestParseWatchNamespaces(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{"empty", "", nil},
		{"whitespace only", "   ", nil},
		{"single", "team-a", []string{"team-a"}},
		{"single with whitespace", "  team-a  ", []string{"team-a"}},
		{"csv", "team-a,team-b", []string{"team-a", "team-b"}},
		{"csv with whitespace", "team-a, team-b , team-c", []string{"team-a", "team-b", "team-c"}},
		{"empty entries skipped", "team-a, ,team-b,", []string{"team-a", "team-b"}},
		{"duplicates collapsed", "team-a,team-a,team-b", []string{"team-a", "team-b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseWatchNamespaces(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d namespaces, want %d: %v", len(got), len(tc.want), sortedKeys(got))
			}
			for _, ns := range tc.want {
				if _, ok := got[ns]; !ok {
					t.Errorf("missing namespace %q; got %v", ns, sortedKeys(got))
				}
			}
		})
	}
}

func TestCacheOptionsScopesPodsToParentLabel(t *testing.T) {
	namespaces := map[string]cache.Config{"team-a": {}}
	opts, err := cacheOptions(namespaces)
	if err != nil {
		t.Fatalf("cacheOptions: %v", err)
	}
	if _, ok := opts.DefaultNamespaces["team-a"]; !ok || len(opts.DefaultNamespaces) != 1 {
		t.Errorf("DefaultNamespaces = %v, want only team-a", sortedKeys(opts.DefaultNamespaces))
	}

	var podSelector labels.Selector
	for obj, byObj := range opts.ByObject {
		if _, ok := obj.(*corev1.Pod); ok {
			podSelector = byObj.Label
		}
	}
	if podSelector == nil {
		t.Fatal("no Pod label selector in ByObject")
	}

	tests := []struct {
		name   string
		labels labels.Set
		want   bool
	}{
		{"component pod", labels.Set{naming.LabelKeyParent: "demo", naming.LabelKeyComponent: "web-server"}, true},
		{"lifecycle task pod", labels.Set{naming.LabelKeyParent: "demo", naming.LabelKeyComponent: "init"}, true},
		{"unrelated pod", labels.Set{"app": "other"}, false},
		{"no labels", labels.Set{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := podSelector.Matches(tc.labels); got != tc.want {
				t.Errorf("Matches(%v) = %v, want %v", tc.labels, got, tc.want)
			}
		})
	}
}
