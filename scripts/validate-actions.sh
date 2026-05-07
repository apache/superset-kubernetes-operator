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

# Validates that all GitHub Actions referenced in workflow files are on the
# ASF-approved allowlist: https://github.com/apache/infrastructure-actions
#
# On PRs: fails if any action is not on the allowlist (blocks unapproved bumps).
# On schedule/main: also warns about actions pinned to older versions when a
# newer approved version is available.

set -euo pipefail

ALLOWLIST_URL="https://raw.githubusercontent.com/apache/infrastructure-actions/main/approved_patterns.yml"

allowlist=$(curl -sfL "$ALLOWLIST_URL" | grep -E '^- ' | sed 's/^- //')

error_file=$(mktemp)
trap 'rm -f "$error_file"' EXIT

for workflow in .github/workflows/*.yml .github/workflows/*.yaml; do
  [ -f "$workflow" ] || continue

  grep -n 'uses:' "$workflow" | grep -v 'uses: \./' | while IFS= read -r line; do
    lineno=$(echo "$line" | cut -d: -f1)
    # Extract action reference, trim whitespace and inline comments
    action=$(echo "$line" | sed 's/.*uses:[[:space:]]*//' | sed 's/[[:space:]]*#.*//' | tr -d ' ')

    [ -z "$action" ] && continue

    owner_repo=$(echo "$action" | cut -d@ -f1)
    ref=$(echo "$action" | cut -d@ -f2)
    owner=$(echo "$owner_repo" | cut -d/ -f1)

    # GitHub-owned actions are implicitly allowed by the ASF org policy
    if [ "$owner" = "actions" ] || [ "$owner" = "github" ]; then
      continue
    fi

    matched=false
    while IFS= read -r pattern; do
      pattern_repo=$(echo "$pattern" | cut -d@ -f1)
      pattern_ref=$(echo "$pattern" | cut -d@ -f2)

      if [ "$owner_repo" = "$pattern_repo" ]; then
        if [ "$pattern_ref" = "*" ] || [ "$ref" = "$pattern_ref" ]; then
          matched=true
          break
        fi
      fi
    done <<< "$allowlist"

    if [ "$matched" = "false" ]; then
      echo "::error file=${workflow},line=${lineno}::Action not on ASF allowlist: ${action}"
      echo "1" >> "$error_file"
    fi
  done
done

if [ -s "$error_file" ]; then
  count=$(wc -l < "$error_file" | tr -d ' ')
  echo ""
  echo "Found ${count} action(s) not on the ASF allowlist."
  echo "See: https://github.com/apache/infrastructure-actions/blob/main/approved_patterns.yml"
  exit 1
fi

echo "All actions are on the ASF allowlist."
