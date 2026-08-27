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
#
# Sync hack/tool-checksums.txt with the SHA-256 checksums of the tool binaries
# pinned in the Makefile (RUMDL_VERSION, GOLANGCI_LINT_VERSION,
# OPERATOR_SDK_VERSION, OPM_VERSION) and in scripts/install-helm-unittest.sh
# (HELM_UNITTEST_VERSION).
# Makefile recipes and install-helm-unittest.sh verify every download against
# that file via scripts/verify-tool-checksum.sh, so these binaries — executed in
# the CI lint/helm-lint jobs, the pre-commit hook, on developer machines, and on
# the release manager's workstation — are pinned to immutable in-repo digests
# instead of whatever a mutable release tag serves at install time. Mirrors
# scripts/sync-helm-checksum.sh.
#
# The pins are trusted at sync time: run this on a version bump, review the
# Renovate/release notes, and commit the regenerated file.
#
# rumdl, golangci-lint, operator-sdk, and helm-unittest publish per-release
# checksum manifests, which this script fetches; operator-registry (opm) does
# not, so its binaries are downloaded and hashed locally (a few hundred MB —
# sync runs are rare).
#
# Usage:
#   sync-tool-checksums.sh [--check|--write]
#
# --check (default): exit non-zero with a diff if the file is out of sync.
# --write:           rewrite hack/tool-checksums.txt in place.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAKEFILE="${REPO_ROOT}/Makefile"
CHECKSUMS="${REPO_ROOT}/hack/tool-checksums.txt"

mode="${1:---check}"
case "${mode}" in --check|--write) ;; *) echo "usage: $0 [--check|--write]" >&2; exit 2 ;; esac

command -v curl >/dev/null || { echo "curl required" >&2; exit 1; }

# Read a "VAR ?= value" default out of the Makefile.
read_version() {
  sed -nE "s/^$1 \?= ([^ ]+)$/\1/p" "${MAKEFILE}" | head -n1
}

RUMDL_VERSION="$(read_version RUMDL_VERSION)"
GOLANGCI_LINT_VERSION="$(read_version GOLANGCI_LINT_VERSION)"
OPERATOR_SDK_VERSION="$(read_version OPERATOR_SDK_VERSION)"
OPM_VERSION="$(read_version OPM_VERSION)"
[ -n "${RUMDL_VERSION}" ]        || { echo "could not read RUMDL_VERSION from ${MAKEFILE}" >&2; exit 1; }
[ -n "${GOLANGCI_LINT_VERSION}" ] || { echo "could not read GOLANGCI_LINT_VERSION from ${MAKEFILE}" >&2; exit 1; }
[ -n "${OPERATOR_SDK_VERSION}" ] || { echo "could not read OPERATOR_SDK_VERSION from ${MAKEFILE}" >&2; exit 1; }
[ -n "${OPM_VERSION}" ]          || { echo "could not read OPM_VERSION from ${MAKEFILE}" >&2; exit 1; }

require_sha() {
  printf '%s' "$1" | grep -Eq '^[a-f0-9]{64}$' \
    || { echo "unexpected checksum for $2: $1" >&2; exit 1; }
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT
out="${tmpdir}/tool-checksums.txt"

# Preserve the license/usage header of the existing file (everything up to
# the first pinned entry), then regenerate all entries.
awk '$1 ~ /^[a-f0-9]{64}$/ {exit} {print}' "${CHECKSUMS}" > "${out}"

# rumdl: one .sha256 manifest per release asset.
for triple in \
  x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu \
  x86_64-apple-darwin aarch64-apple-darwin; do
  asset="rumdl-${RUMDL_VERSION}-${triple}.tar.gz"
  sha="$(curl -fsSL "https://github.com/rvben/rumdl/releases/download/${RUMDL_VERSION}/${asset}.sha256" | awk '{print $1; exit}')"
  require_sha "${sha}" "${asset}"
  printf '%s  %s/%s\n' "${sha}" "${RUMDL_VERSION}" "${asset}" >> "${out}"
done

# golangci-lint: a single checksums manifest covers all release archives.
gcl_want="${GOLANGCI_LINT_VERSION#v}"
gcl_manifest="$(curl -fsSL "https://github.com/golangci/golangci-lint/releases/download/${GOLANGCI_LINT_VERSION}/golangci-lint-${gcl_want}-checksums.txt")"
for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
  asset="golangci-lint-${gcl_want}-${platform}.tar.gz"
  sha="$(printf '%s\n' "${gcl_manifest}" | awk -v a="${asset}" '$2 == a {print $1; exit}')"
  require_sha "${sha}" "${asset}"
  printf '%s  %s/%s\n' "${sha}" "${GOLANGCI_LINT_VERSION}" "${asset}" >> "${out}"
done

# operator-sdk: a single checksums.txt manifest covers all release assets.
sdk_manifest="$(curl -fsSL "https://github.com/operator-framework/operator-sdk/releases/download/${OPERATOR_SDK_VERSION}/checksums.txt")"
for asset in \
  operator-sdk_linux_amd64 operator-sdk_linux_arm64 \
  operator-sdk_darwin_amd64 operator-sdk_darwin_arm64; do
  sha="$(printf '%s\n' "${sdk_manifest}" | awk -v a="${asset}" '$2 == a {print $1; exit}')"
  require_sha "${sha}" "${asset}"
  printf '%s  %s/%s\n' "${sha}" "${OPERATOR_SDK_VERSION}" "${asset}" >> "${out}"
done

# opm (operator-registry): no published checksum manifest — download the
# binaries and hash them locally.
for asset in linux-amd64-opm linux-arm64-opm darwin-amd64-opm darwin-arm64-opm; do
  curl -fsSL -o "${tmpdir}/${asset}" \
    "https://github.com/operator-framework/operator-registry/releases/download/${OPM_VERSION}/${asset}"
  if command -v sha256sum >/dev/null 2>&1; then
    sha="$(sha256sum "${tmpdir}/${asset}" | awk '{print $1}')"
  else
    sha="$(shasum -a 256 "${tmpdir}/${asset}" | awk '{print $1}')"
  fi
  require_sha "${sha}" "${asset}"
  printf '%s  %s/%s\n' "${sha}" "${OPM_VERSION}" "${asset}" >> "${out}"
  rm -f "${tmpdir}/${asset}"
done

# helm-unittest: a single checksum manifest (helm-unittest-checksum.sha) covers
# all release archives. Its version is pinned in the install script (invoked
# standalone, so it cannot read a Makefile variable).
HELM_UNITTEST_VERSION="$(sed -nE 's/^HELM_UNITTEST_VERSION="\$\{HELM_UNITTEST_VERSION:-([^}]+)\}"$/\1/p' "${REPO_ROOT}/scripts/install-helm-unittest.sh" | head -n1)"
[ -n "${HELM_UNITTEST_VERSION}" ] || { echo "could not read HELM_UNITTEST_VERSION from install-helm-unittest.sh" >&2; exit 1; }
hu_want="${HELM_UNITTEST_VERSION#v}"
hu_manifest="$(curl -fsSL "https://github.com/helm-unittest/helm-unittest/releases/download/${HELM_UNITTEST_VERSION}/helm-unittest-checksum.sha")"
for plat in linux-amd64 linux-arm64 macos-amd64 macos-arm64; do
  asset="helm-unittest-${plat}-${hu_want}.tgz"
  # Manifest lines look like "<sha> *./_dist/<asset>", so match the line
  # containing the asset name and take its 64-hex field (tolerates the
  # binary-mode "*" marker and the "./_dist/" path prefix).
  sha="$(printf '%s\n' "${hu_manifest}" | awk -v a="${asset}" 'index($0, a) {for (i=1;i<=NF;i++) if ($i ~ /^[a-f0-9]{64}$/) {print $i; exit}}')"
  require_sha "${sha}" "${asset}"
  printf '%s  %s/%s\n' "${sha}" "${HELM_UNITTEST_VERSION}" "${asset}" >> "${out}"
done

case "${mode}" in
  --write)
    if cmp -s "${out}" "${CHECKSUMS}"; then
      echo "hack/tool-checksums.txt already pins the correct checksums"
    else
      cp "${out}" "${CHECKSUMS}"
      echo "updated hack/tool-checksums.txt for rumdl ${RUMDL_VERSION}, golangci-lint ${GOLANGCI_LINT_VERSION}, operator-sdk ${OPERATOR_SDK_VERSION}, opm ${OPM_VERSION}, helm-unittest ${HELM_UNITTEST_VERSION}"
    fi
    ;;
  --check)
    if ! diff -u "${CHECKSUMS}" "${out}"; then
      echo "hack/tool-checksums.txt is out of sync with the pinned tool versions." >&2
      echo "Run 'make sync-tool-checksums' to update." >&2
      exit 1
    fi
    ;;
esac
