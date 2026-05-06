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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		name     string
		oldTag   string
		newTag   string
		expected VersionDirection
	}{
		{name: "upgrade patch", oldTag: "4.0.0", newTag: "4.0.1", expected: DirectionUpgrade},
		{name: "upgrade minor", oldTag: "4.0.0", newTag: "4.1.0", expected: DirectionUpgrade},
		{name: "upgrade major", oldTag: "4.0.0", newTag: "5.0.0", expected: DirectionUpgrade},
		{name: "downgrade patch", oldTag: "4.0.1", newTag: "4.0.0", expected: DirectionDowngrade},
		{name: "downgrade minor", oldTag: "4.1.0", newTag: "4.0.0", expected: DirectionDowngrade},
		{name: "downgrade major", oldTag: "5.0.0", newTag: "4.0.0", expected: DirectionDowngrade},
		{name: "same version", oldTag: "4.0.0", newTag: "4.0.0", expected: DirectionRebuild},
		{name: "v-prefix upgrade", oldTag: "v4.0.0", newTag: "v4.1.0", expected: DirectionUpgrade},
		{name: "v-prefix downgrade", oldTag: "v4.1.0", newTag: "v4.0.0", expected: DirectionDowngrade},
		{name: "v-prefix same", oldTag: "v4.0.0", newTag: "v4.0.0", expected: DirectionRebuild},
		{name: "mixed v-prefix upgrade", oldTag: "4.0.0", newTag: "v4.1.0", expected: DirectionUpgrade},
		{name: "pre-release to release", oldTag: "4.0.0-rc1", newTag: "4.0.0", expected: DirectionUpgrade},
		{name: "release to pre-release", oldTag: "4.0.0", newTag: "4.1.0-rc1", expected: DirectionUpgrade},
		{name: "pre-release ordering", oldTag: "4.0.0-rc1", newTag: "4.0.0-rc2", expected: DirectionUpgrade},
		{name: "non-semver old", oldTag: "latest", newTag: "4.0.0", expected: DirectionUnknown},
		{name: "non-semver new", oldTag: "4.0.0", newTag: "latest", expected: DirectionUnknown},
		{name: "both non-semver", oldTag: "latest", newTag: "dev-abc123", expected: DirectionUnknown},
		{name: "sha tags", oldTag: "sha-abc123", newTag: "sha-def456", expected: DirectionUnknown},
		{name: "branch tags", oldTag: "main", newTag: "feature-x", expected: DirectionUnknown},
		{name: "different non-semver", oldTag: "latest", newTag: "latest", expected: DirectionRebuild},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareVersions(tt.oldTag, tt.newTag)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestImageRef(t *testing.T) {
	assert.Equal(t, "docker.io/superset:4.0.0", ImageRef("docker.io/superset", "4.0.0"))
	assert.Equal(t, "ghcr.io/apache/superset:latest", ImageRef("ghcr.io/apache/superset", "latest"))
}
