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

package controller

import "testing"

// FuzzCompareVersions exercises CompareVersions with arbitrary image tags. The
// tags come from the (trusted) Superset CR spec, so this is a robustness check:
// the semver parsing must never panic and the result must obey the ordering
// contract. See docs/contributing/development-guidelines.md (Fuzzing).
func FuzzCompareVersions(f *testing.F) {
	seeds := [][2]string{
		{"4.0.0", "4.0.1"},
		{"4.1.0", "4.0.0"},
		{"4.0.0", "4.0.0"},
		{"v4.0.0", "4.0.0"},
		{"4.0.0-rc1", "4.0.0"},
		{"latest", "4.0.0"},
		{"sha-abc123", "sha-def456"},
		{"", "x"},
		{"", ""},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1])
	}

	isValid := func(d VersionDirection) bool {
		switch d {
		case DirectionUpgrade, DirectionDowngrade, DirectionRebuild, DirectionUnknown:
			return true
		default:
			return false
		}
	}

	f.Fuzz(func(t *testing.T, oldTag, newTag string) {
		got := CompareVersions(oldTag, newTag)
		if !isValid(got) {
			t.Fatalf("CompareVersions(%q, %q) = %q, not a defined VersionDirection", oldTag, newTag, got)
		}

		// Comparing a tag to itself is always a rebuild.
		if self := CompareVersions(oldTag, oldTag); self != DirectionRebuild {
			t.Fatalf("CompareVersions(%q, %q) = %q, want Rebuild", oldTag, oldTag, self)
		}

		// Swapping the arguments must invert the direction: an upgrade one way is
		// a downgrade the other, while Rebuild and Unknown are symmetric.
		reverse := CompareVersions(newTag, oldTag)
		want := map[VersionDirection]VersionDirection{
			DirectionUpgrade:   DirectionDowngrade,
			DirectionDowngrade: DirectionUpgrade,
			DirectionRebuild:   DirectionRebuild,
			DirectionUnknown:   DirectionUnknown,
		}[got]
		if reverse != want {
			t.Fatalf("CompareVersions(%q, %q) = %q but CompareVersions(%q, %q) = %q, want %q",
				oldTag, newTag, got, newTag, oldTag, reverse, want)
		}
	})
}
