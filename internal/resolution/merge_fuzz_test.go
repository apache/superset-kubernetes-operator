/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package resolution

import (
	"maps"
	"reflect"
	"testing"
)

// FuzzMergeMaps checks that MergeMaps produces the union of its inputs with
// last-writer-wins precedence for any label/annotation keys and values, and
// that it is idempotent. See docs/contributing/development-guidelines.md (Fuzzing).
func FuzzMergeMaps(f *testing.F) {
	f.Add("a", "1", "b", "2", "c", "3", "9")
	f.Add("", "", "", "", "", "", "")
	f.Add("k", "first", "k", "second", "k", "third", "fourth")

	f.Fuzz(func(t *testing.T, k1, v1, k2, v2, k3, v3, v4 string) {
		// Construct overlapping maps so precedence (later map wins) is exercised.
		inputs := []map[string]string{
			{k1: v1},
			{k2: v2, k3: v3},
			{k1: v4},
		}

		// Reference union with last-writer-wins, mirroring MergeMaps' contract.
		want := map[string]string{}
		for _, m := range inputs {
			maps.Copy(want, m)
		}

		got := MergeMaps(inputs...)

		if len(want) == 0 {
			if got != nil {
				t.Fatalf("MergeMaps of empty maps = %v, want nil", got)
			}
			return
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("MergeMaps = %v, want %v", got, want)
		}

		// Idempotent: merging the same inputs again yields an equal map, and
		// re-merging the result is a no-op copy.
		if again := MergeMaps(inputs...); !reflect.DeepEqual(again, got) {
			t.Fatalf("MergeMaps not idempotent: %v vs %v", again, got)
		}
		if reMerged := MergeMaps(got); !reflect.DeepEqual(reMerged, got) {
			t.Fatalf("MergeMaps(MergeMaps(x)) = %v, want %v", reMerged, got)
		}
	})
}
