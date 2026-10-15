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
