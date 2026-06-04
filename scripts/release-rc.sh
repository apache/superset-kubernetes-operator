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

# Prepare a release candidate: create release branch, bump version,
# regenerate manifests, run checks, commit, and tag.
#
# Usage:
#   scripts/release-rc.sh <version> [--expect-rc <n>]
#
# Examples:
#   scripts/release-rc.sh 0.2.0               # first RC on 0.2 → v0.2.0-rc1
#   scripts/release-rc.sh 0.2.0               # again   → v0.2.0-rc2
#   scripts/release-rc.sh 0.2.0 --expect-rc 1

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
RESET='\033[0m'

die()  { echo -e "${RED}error:${RESET} $*" >&2; exit 1; }
info() { echo -e "${GREEN}==>${RESET} $*"; }
warn() { echo -e "${YELLOW}warning:${RESET} $*"; }

# Cross-platform in-place sed. BSD sed (macOS) requires `-i ''`; GNU sed (Linux)
# rejects an empty string after `-i`. Detect once and dispatch.
if sed --version >/dev/null 2>&1; then
  sed_inplace() { sed -i "$@"; }
else
  sed_inplace() { sed -i '' "$@"; }
fi

# --- parse args ---
VERSION=""
EXPECT_RC=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --expect-rc) EXPECT_RC="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 <version> [--expect-rc <n>]"
      exit 0
      ;;
    -*)  die "unknown flag: $1" ;;
    *)   VERSION="$1"; shift ;;
  esac
done

[[ -n "$VERSION" ]] || die "usage: $0 <version> [--expect-rc <n>]"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "version must be semver (e.g., 0.2.0), got: $VERSION"
if [[ -n "$EXPECT_RC" ]]; then
  [[ "$EXPECT_RC" =~ ^[1-9][0-9]*$ ]] || die "--expect-rc must be a positive integer, got: $EXPECT_RC"
fi

# --- preflight checks ---
command -v helm >/dev/null 2>&1 || die "helm is required but not installed"
[[ -f Makefile ]] || die "must be run from the repository root"
CHANGELOG_FILE="docs/reference/releases.md"
[[ -f "$CHANGELOG_FILE" ]] || die "release notes file is missing: ${CHANGELOG_FILE}"
grep -Eq "^##[[:space:]]+\\[${VERSION//./\\.}\\]([[:space:]]|$)" "$CHANGELOG_FILE" \
  || die "${CHANGELOG_FILE} must contain a release heading for ${VERSION} (expected: ## [${VERSION}])"

if [[ -n "$(git status --porcelain)" ]]; then
  die "working tree is not clean — commit or stash changes first"
fi

BRANCH="${VERSION%.*}"
CURRENT_BRANCH=$(git branch --show-current)

# --- create or switch to release branch ---
if git show-ref --verify --quiet "refs/heads/${BRANCH}"; then
  info "Switching to existing branch ${BRANCH}"
  git checkout "${BRANCH}"
else
  if [[ "$CURRENT_BRANCH" != "main" ]]; then
    die "must be on main to create a new release branch (currently on ${CURRENT_BRANCH})"
  fi
  info "Creating release branch ${BRANCH} from main"
  git checkout -b "${BRANCH}"
fi

# --- determine RC number ---
LAST_RC=$(git tag -l "v${VERSION}-rc*" | sort -V | tail -1 || true)
if [[ -n "$LAST_RC" ]]; then
  LAST_N="${LAST_RC##*-rc}"
  RC_N=$((LAST_N + 1))
else
  RC_N=1
fi
TAG="v${VERSION}-rc${RC_N}"
if [[ -n "$EXPECT_RC" && "$RC_N" -ne "$EXPECT_RC" ]]; then
  die "next RC would be rc${RC_N}, but --expect-rc ${EXPECT_RC} was provided"
fi
info "Preparing ${TAG}"

# --- bump operator version in Makefile ---
CURRENT_VERSION=$(grep -E '^VERSION \?=' Makefile | head -1 | awk '{print $3}')
if [[ "$CURRENT_VERSION" != "$VERSION" ]]; then
  info "Updating Makefile VERSION: ${CURRENT_VERSION} → ${VERSION}"
  sed_inplace "s/^VERSION ?= .*/VERSION ?= ${VERSION}/" Makefile
else
  info "Makefile VERSION already set to ${VERSION}"
fi

# --- bump chart version metadata ---
CURRENT_CHART_VERSION=$(grep -E '^version:' charts/superset-operator/Chart.yaml | head -1 | awk '{print $2}')
if [[ "$CURRENT_CHART_VERSION" != "$VERSION" ]]; then
  info "Updating Chart.yaml version: ${CURRENT_CHART_VERSION} → ${VERSION}"
  sed_inplace "s/^version: .*/version: ${VERSION}/" charts/superset-operator/Chart.yaml
else
  info "Chart.yaml version already set to ${VERSION}"
fi
CURRENT_CHART_APP_VERSION=$(grep -E '^appVersion:' charts/superset-operator/Chart.yaml | head -1 | awk '{print $2}' | tr -d '"')
if [[ "$CURRENT_CHART_APP_VERSION" != "$VERSION" ]]; then
  info "Updating Chart.yaml appVersion: ${CURRENT_CHART_APP_VERSION} → ${VERSION}"
  sed_inplace "s/^appVersion: .*/appVersion: \"${VERSION}\"/" charts/superset-operator/Chart.yaml
else
  info "Chart.yaml appVersion already set to ${VERSION}"
fi

# --- regenerate generated artifacts ---
info "Regenerating generated artifacts (manifests, deepcopy, helm CRDs, API docs, supported-versions, make-commands)"
make codegen

# --- run checks ---
info "Verifying Apache license headers"
make check-license

info "Running golangci-lint"
make lint

info "Running unit and integration tests"
make test-unit test-integration

info "Linting Helm chart"
make helm-lint

info "Building docs"
make docs-build

# --- commit ---
CHANGED_FILES=$(git diff --name-only)
if [[ -n "$CHANGED_FILES" ]]; then
  info "Committing version bump and regenerated files"
  git add -A
  git commit -m "chore: prepare ${TAG}

Update VERSION to ${VERSION} and regenerate manifests."
else
  info "No file changes to commit"
fi

# --- tag ---
if git rev-parse "${TAG}" >/dev/null 2>&1; then
  die "tag ${TAG} already exists"
fi
info "Creating tag ${TAG}"
git tag -a "${TAG}" -m "Release candidate ${TAG}"

# --- done ---
echo ""
echo -e "${BOLD}Release candidate ${TAG} is ready.${RESET}"
echo ""
echo "Next steps:"
echo "  1. Review the commit:  git log --oneline -3"
echo "  2. Build and verify source artifacts:  scripts/release-source.sh"
echo "  3. Push branch + tag only after artifact verification:  git push origin ${BRANCH} ${TAG}"
echo ""
echo "The release workflow will build and push the ${VERSION}-rc${RC_N} image to GHCR."
