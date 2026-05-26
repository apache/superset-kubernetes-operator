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
# Sync .github/supported-k8s.json with the kindest/node images published
# by the currently pinned kind release (read from .github/workflows/test.yaml).
#
# Picks the two newest Kubernetes minors that the pinned kind release ships
# node images for, taking the highest patch per minor, and updates the
# `supported` array. Auto-nulls `next` if its minor is now covered by kind.
#
# Usage:
#   sync-supported-versions.sh [--check|--write]
#
# --check (default): exit non-zero with a diff if the JSON is out of sync.
# --write:           rewrite the JSON in place.
#
# Set GITHUB_TOKEN to avoid GitHub API rate limits (60 req/hr unauth).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE="${REPO_ROOT}/.github/supported-k8s.json"
WORKFLOW="${REPO_ROOT}/.github/workflows/test.yaml"

mode="${1:---check}"
case "${mode}" in --check|--write) ;; *) echo "usage: $0 [--check|--write]" >&2; exit 2 ;; esac

command -v jq >/dev/null   || { echo "jq required" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl required" >&2; exit 1; }

KIND_VERSION="$(awk '/^[[:space:]]*KIND_VERSION:/ {print $2; exit}' "${WORKFLOW}")"
[ -n "${KIND_VERSION}" ] || { echo "could not read KIND_VERSION from ${WORKFLOW}" >&2; exit 1; }

api="https://api.github.com/repos/kubernetes-sigs/kind/releases/tags/${KIND_VERSION}"
auth=()
[ -n "${GITHUB_TOKEN:-}" ] && auth=(-H "Authorization: Bearer ${GITHUB_TOKEN}")

body="$(curl -fsSL ${auth[@]+"${auth[@]}"} -H 'Accept: application/vnd.github+json' "${api}" | jq -r .body)"

# Extract the highest-patch node image per Kubernetes minor, then take the
# top two minors. Format per row: "MINOR FULL_IMAGE".
top2="$(printf '%s\n' "${body}" \
  | grep -oE 'kindest/node:v[0-9]+\.[0-9]+\.[0-9]+@sha256:[a-f0-9]{64}' \
  | sort -u \
  | sed -E 's|^kindest/node:v([0-9]+\.[0-9]+)\.([0-9]+)@.*|\1 \2 &|' \
  | sort -k1,1V -k2,2n \
  | awk '{ best[$1] = $3 } END { for (k in best) print k" "best[k] }' \
  | sort -k1,1Vr \
  | head -2)"

[ -n "${top2}" ] || { echo "no kindest/node images found in release notes for ${KIND_VERSION}" >&2; exit 1; }
[ "$(printf '%s\n' "${top2}" | wc -l | tr -d ' ')" -eq 2 ] \
  || { echo "expected 2 minor versions, got:" >&2; printf '%s\n' "${top2}" >&2; exit 1; }

new_supported="$(printf '%s\n' "${top2}" \
  | jq -R -s 'split("\n") | map(select(length>0)) | map(split(" ") | {minor: .[0], node_image: .[1]})')"

new_json="$(
  jq --argjson sup "${new_supported}" '
    (.supported = $sup)
    | (.next.minor // null) as $nextMinor
    | if $nextMinor != null and ([.supported[].minor] | index($nextMinor)) != null
        then .next = null
        else .
      end
  ' "${SOURCE}"
)"

case "${mode}" in
  --write)
    printf '%s\n' "${new_json}" > "${SOURCE}"
    echo "updated ${SOURCE} from kind ${KIND_VERSION}"
    ;;
  --check)
    if ! diff -u <(jq -S . "${SOURCE}") <(printf '%s\n' "${new_json}" | jq -S .); then
      echo >&2
      echo "supported-k8s.json is out of sync with kind ${KIND_VERSION}." >&2
      echo "Run 'make sync-supported-versions' to update." >&2
      exit 1
    fi
    ;;
esac
