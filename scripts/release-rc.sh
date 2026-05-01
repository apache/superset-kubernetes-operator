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
#   scripts/release-rc.sh <version> [--chart-version <chart-version>]
#
# Examples:
#   scripts/release-rc.sh 0.2.0               # first RC → v0.2.0-rc1
#   scripts/release-rc.sh 0.2.0               # again   → v0.2.0-rc2
#   scripts/release-rc.sh 0.3.0 --chart-version 0.2.0

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
RESET='\033[0m'

die()  { echo -e "${RED}error:${RESET} $*" >&2; exit 1; }
info() { echo -e "${GREEN}==>${RESET} $*"; }
warn() { echo -e "${YELLOW}warning:${RESET} $*"; }

# --- parse args ---
VERSION=""
CHART_VERSION=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --chart-version) CHART_VERSION="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 <version> [--chart-version <chart-version>]"
      exit 0
      ;;
    -*)  die "unknown flag: $1" ;;
    *)   VERSION="$1"; shift ;;
  esac
done

[[ -n "$VERSION" ]] || die "usage: $0 <version> [--chart-version <chart-version>]"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "version must be semver (e.g., 0.2.0), got: $VERSION"
if [[ -n "$CHART_VERSION" ]]; then
  [[ "$CHART_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "chart version must be semver, got: $CHART_VERSION"
fi

# --- preflight checks ---
command -v helm >/dev/null 2>&1 || die "helm is required but not installed"
[[ -f Makefile ]] || die "must be run from the repository root"

if [[ -n "$(git status --porcelain)" ]]; then
  die "working tree is not clean — commit or stash changes first"
fi

BRANCH="release/${VERSION}"
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
info "Preparing ${TAG}"

# --- bump operator version in Makefile ---
CURRENT_VERSION=$(grep -E '^VERSION \?=' Makefile | head -1 | awk '{print $3}')
if [[ "$CURRENT_VERSION" != "$VERSION" ]]; then
  info "Updating Makefile VERSION: ${CURRENT_VERSION} → ${VERSION}"
  sed -i '' "s/^VERSION ?= .*/VERSION ?= ${VERSION}/" Makefile
else
  info "Makefile VERSION already set to ${VERSION}"
fi

# --- optionally bump chart version ---
if [[ -n "$CHART_VERSION" ]]; then
  info "Updating Chart.yaml version: → ${CHART_VERSION}"
  sed -i '' "s/^version: .*/version: ${CHART_VERSION}/" charts/superset-operator/Chart.yaml
fi

# --- regenerate manifests ---
info "Regenerating manifests and code"
make manifests generate

# --- run checks ---
info "Running tests"
make test-unit test-integration

info "Linting Helm chart"
make helm-lint

info "Building docs"
make docs-build

# --- package Helm chart ---
info "Packaging Helm chart"
make helm

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
echo "  2. Push branch + tag:  git push origin ${BRANCH} ${TAG}"
echo ""
echo "The release workflow will build and push the ${VERSION}-rc${RC_N} image to GHCR."
