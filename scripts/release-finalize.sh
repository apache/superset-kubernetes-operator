#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Finalize a release after the ASF vote passes: tag the final version
# on the release branch.
#
# Usage:
#   scripts/release-finalize.sh <version>
#
# Example:
#   scripts/release-finalize.sh 0.2.0    # creates tag v0.2.0

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BOLD='\033[1m'
RESET='\033[0m'

die()  { echo -e "${RED}error:${RESET} $*" >&2; exit 1; }
info() { echo -e "${GREEN}==>${RESET} $*"; }

VERSION="${1:-}"
[[ -n "$VERSION" ]] || die "usage: $0 <version>"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "version must be semver (e.g., 0.2.0), got: $VERSION"

[[ -f Makefile ]] || die "must be run from the repository root"

TAG="v${VERSION}"
BRANCH="release/${VERSION}"

# --- preflight checks ---
if [[ -n "$(git status --porcelain)" ]]; then
  die "working tree is not clean — commit or stash changes first"
fi

CURRENT_BRANCH=$(git branch --show-current)
if [[ "$CURRENT_BRANCH" != "$BRANCH" ]]; then
  die "must be on branch ${BRANCH} (currently on ${CURRENT_BRANCH})"
fi

LAST_RC=$(git tag -l "v${VERSION}-rc*" | sort -V | tail -1 || true)
if [[ -z "$LAST_RC" ]]; then
  die "no RC tags found for v${VERSION} — run scripts/release-rc.sh first"
fi

if git rev-parse "${TAG}" >/dev/null 2>&1; then
  die "tag ${TAG} already exists"
fi

MAKEFILE_VERSION=$(grep -E '^VERSION \?=' Makefile | head -1 | awk '{print $3}')
if [[ "$MAKEFILE_VERSION" != "$VERSION" ]]; then
  die "Makefile VERSION is ${MAKEFILE_VERSION}, expected ${VERSION}"
fi

# --- tag ---
info "Last RC was ${LAST_RC}"
info "Creating final release tag ${TAG} at HEAD ($(git rev-parse --short HEAD))"
git tag -a "${TAG}" -m "Release ${TAG}"

# --- done ---
echo ""
echo -e "${BOLD}Release ${TAG} is tagged.${RESET}"
echo ""
echo "Next steps:"
echo "  1. Push the tag:  git push origin ${TAG}"
echo ""
echo "The release workflow will push the ${VERSION} and latest images to GHCR."
