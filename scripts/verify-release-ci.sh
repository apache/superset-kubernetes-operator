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

# Release publish gate: verify that the branch-protection required status checks
# all passed on the commit being released before the release workflow publishes
# anything.
#
# The required checks are read from .asf.yaml (the single source of truth for
# branch protection) rather than hard-coded here, so the gate follows whatever
# main requires. Those same workflows run on release branches (see the branch
# filters in ci.yaml/test.yaml/license.yml), so the tagged commit — the release
# branch HEAD — carries their check runs.
#
# E2E is intentionally NOT among the gated checks. The E2E matrix jobs use
# dynamic names (e.g. "E2E (1.34)", "E2E (next, best-effort)") that are
# impractical to pin as branch-protection contexts, so E2E is treated as
# best-effort and does not block a release. If E2E should ever gate releases,
# add its (stabilized) job names to .asf.yaml rather than special-casing them
# here.
#
# Only release tags (refs/tags/v*) are gated; pushes to main and workflow_dispatch
# runs (which publish the throwaway `dev`/`sha-` images) proceed without waiting.
#
# Requires: gh (with GH_TOKEN), yq. Env: GITHUB_REF, GITHUB_SHA, GITHUB_REPOSITORY.

set -euo pipefail

REF="${GITHUB_REF:-}"
SHA="${GITHUB_SHA:?GITHUB_SHA is required}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
TIMEOUT_SECONDS="${CI_GATE_TIMEOUT_SECONDS:-1800}" # 30 minutes
POLL_SECONDS="${CI_GATE_POLL_SECONDS:-20}"

case "${REF}" in
  refs/tags/v*) ;;
  *) echo "Ref ${REF:-<none>} is not a release tag; skipping the CI gate."; exit 0 ;;
esac

mapfile -t REQUIRED < <(yq -r \
  '.github.protected_branches.main.required_status_checks.contexts[]' .asf.yaml)
if [ "${#REQUIRED[@]}" -eq 0 ]; then
  echo "No required_status_checks contexts found in .asf.yaml" >&2
  exit 1
fi

echo "Gating release of ${SHA} on required checks:"
printf '  - %s\n' "${REQUIRED[@]}"

deadline=$(( SECONDS + TIMEOUT_SECONDS ))
while :; do
  # name<TAB>status<TAB>conclusion<TAB>started_at for every check run on the commit.
  runs="$(gh api --paginate "repos/${REPO}/commits/${SHA}/check-runs" \
    -q '.check_runs[] | [.name, .status, (.conclusion // ""), .started_at] | @tsv')"

  pending=()
  failed=()
  for name in "${REQUIRED[@]}"; do
    # Most recent run for this check name (ISO-8601 started_at sorts lexically).
    latest="$(printf '%s\n' "${runs}" | awk -F'\t' -v n="${name}" '$1==n' | sort -t$'\t' -k4 | tail -1)"
    if [ -z "${latest}" ]; then
      pending+=("${name} (not started)")
      continue
    fi
    status="$(printf '%s' "${latest}" | cut -f2)"
    conclusion="$(printf '%s' "${latest}" | cut -f3)"
    if [ "${status}" != "completed" ]; then
      pending+=("${name} (${status})")
    elif [ "${conclusion}" != "success" ]; then
      failed+=("${name} -> ${conclusion}")
    fi
  done

  if [ "${#failed[@]}" -gt 0 ]; then
    echo "Refusing to publish — required checks did not pass on ${SHA}:" >&2
    printf '  - %s\n' "${failed[@]}" >&2
    exit 1
  fi
  if [ "${#pending[@]}" -eq 0 ]; then
    echo "All required checks passed on ${SHA}; proceeding with the release."
    exit 0
  fi
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    echo "Timed out after ${TIMEOUT_SECONDS}s waiting for required checks on ${SHA}:" >&2
    printf '  - %s\n' "${pending[@]}" >&2
    echo "Push the release branch and let CI finish before pushing the tag." >&2
    exit 1
  fi
  echo "Waiting on: ${pending[*]}"
  sleep "${POLL_SECONDS}"
done
