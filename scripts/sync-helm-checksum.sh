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
# Sync the Helm tarball SHA-256 pinned in scripts/install-helm.sh with the
# checksum that Helm publishes for the pinned HELM_VERSION / HELM_PLATFORM.
#
# Helm does not attach its release tarballs to GitHub releases (only detached
# signatures live there); the tarballs and their checksums are served from
# get.helm.sh. Renovate therefore tracks only HELM_VERSION, and this script
# keeps HELM_SHA256 in sync — mirroring scripts/sync-supported-versions.sh.
#
# Usage:
#   sync-helm-checksum.sh [--check|--write]
#
# --check (default): exit non-zero with a diff if the pinned SHA is out of sync.
# --write:           rewrite the SHA in scripts/install-helm.sh in place.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${REPO_ROOT}/scripts/install-helm.sh"

mode="${1:---check}"
case "${mode}" in --check|--write) ;; *) echo "usage: $0 [--check|--write]" >&2; exit 2 ;; esac

command -v curl >/dev/null || { echo "curl required" >&2; exit 1; }

# Read the shell defaults out of install-helm.sh (VAR="${VAR:-default}").
read_default() {
  sed -nE "s/^$1=\"\\\$\{$1:-([^}]*)\}\"$/\1/p" "${SCRIPT}" | head -n1
}

HELM_VERSION="$(read_default HELM_VERSION)"
HELM_PLATFORM="$(read_default HELM_PLATFORM)"
CURRENT_SHA="$(read_default HELM_SHA256)"
[ -n "${HELM_VERSION}" ]  || { echo "could not read HELM_VERSION from ${SCRIPT}" >&2; exit 1; }
[ -n "${HELM_PLATFORM}" ] || { echo "could not read HELM_PLATFORM from ${SCRIPT}" >&2; exit 1; }

# get.helm.sh publishes "<sha>  <filename>" alongside each tarball. awk pulls the
# first field, which also handles a bare-hash ".sha256" file if the suffix moves.
sha_url="https://get.helm.sh/helm-${HELM_VERSION}-${HELM_PLATFORM}.tar.gz.sha256sum"
NEW_SHA="$(curl -fsSL "${sha_url}" | awk '{print $1; exit}')"
printf '%s' "${NEW_SHA}" | grep -Eq '^[a-f0-9]{64}$' \
  || { echo "unexpected checksum content from ${sha_url}: ${NEW_SHA}" >&2; exit 1; }

case "${mode}" in
  --write)
    if [ "${CURRENT_SHA}" = "${NEW_SHA}" ]; then
      echo "install-helm.sh already pins the correct SHA-256 for Helm ${HELM_VERSION}"
    else
      sed -i.bak -E "s|^(HELM_SHA256=\"\\\$\{HELM_SHA256:-)[a-f0-9]*(\}\")|\1${NEW_SHA}\2|" "${SCRIPT}"
      rm -f "${SCRIPT}.bak"
      echo "updated HELM_SHA256 to ${NEW_SHA} for Helm ${HELM_VERSION}"
    fi
    ;;
  --check)
    if [ "${CURRENT_SHA}" != "${NEW_SHA}" ]; then
      echo "install-helm.sh HELM_SHA256 is out of sync with Helm ${HELM_VERSION} (${HELM_PLATFORM})." >&2
      echo "  pinned:    ${CURRENT_SHA}" >&2
      echo "  published: ${NEW_SHA}" >&2
      echo "Run 'make sync-helm-checksum' to update." >&2
      exit 1
    fi
    ;;
esac
