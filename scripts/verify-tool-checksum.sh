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

# Verify a downloaded tool artifact against the SHA-256 pinned in
# hack/tool-checksums.txt (entry format: "<sha256>  <version>/<asset-name>").
#
# Fails closed: an artifact with no pinned entry is an error, not a pass —
# a checksum served by the same origin as the download can be swapped along
# with the artifact, so only the in-repo pin counts (mirroring
# scripts/install-yq.sh / install-oras.sh / install-helm.sh).
#
# Usage: verify-tool-checksum.sh <file> <version>/<asset-name>

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECKSUMS="${REPO_ROOT}/hack/tool-checksums.txt"

file="${1:?usage: verify-tool-checksum.sh <file> <version>/<asset-name>}"
name="${2:?usage: verify-tool-checksum.sh <file> <version>/<asset-name>}"

expected="$(awk -v name="${name}" '$1 ~ /^[a-f0-9]{64}$/ && $2 == name {print $1; exit}' "${CHECKSUMS}")"
if [ -z "${expected}" ]; then
  echo "No pinned SHA-256 for ${name} in hack/tool-checksums.txt." >&2
  echo "Run 'make sync-tool-checksums' and commit the result to pin it." >&2
  rm -f "${file}"
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "${file}" | awk '{print $1}')"
else
  actual="$(shasum -a 256 "${file}" | awk '{print $1}')"
fi

if [ "${actual}" != "${expected}" ]; then
  echo "SHA-256 mismatch for ${name}:" >&2
  echo "  pinned:     ${expected}" >&2
  echo "  downloaded: ${actual}" >&2
  echo "Refusing to install; the upstream artifact does not match the in-repo pin." >&2
  rm -f "${file}"
  exit 1
fi
