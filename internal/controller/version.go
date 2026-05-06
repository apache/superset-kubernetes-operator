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
	"strings"

	"github.com/Masterminds/semver/v3"
)

// VersionDirection represents the relationship between two image versions.
type VersionDirection string

const (
	DirectionUpgrade   VersionDirection = "Upgrade"
	DirectionDowngrade VersionDirection = "Downgrade"
	DirectionRebuild   VersionDirection = "Rebuild"
	DirectionUnknown   VersionDirection = "Unknown"
)

// CompareVersions determines the direction between two image tags.
// Returns Upgrade if newTag > oldTag, Downgrade if newTag < oldTag,
// Rebuild if equal, or Unknown if either tag is not valid semver.
func CompareVersions(oldTag, newTag string) VersionDirection {
	if oldTag == newTag {
		return DirectionRebuild
	}

	oldVer, oldErr := semver.NewVersion(strings.TrimPrefix(oldTag, "v"))
	newVer, newErr := semver.NewVersion(strings.TrimPrefix(newTag, "v"))

	if oldErr != nil || newErr != nil {
		return DirectionUnknown
	}

	if newVer.GreaterThan(oldVer) {
		return DirectionUpgrade
	}
	if newVer.LessThan(oldVer) {
		return DirectionDowngrade
	}
	return DirectionRebuild
}

// ImageRef returns the canonical "repository:tag" string for an image.
func ImageRef(repository, tag string) string {
	return repository + ":" + tag
}
