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

# CI glue for .github/workflows/docs.yml.
#
#   run-docs-ci.sh env            print DOC_REF, DRY_RUN, and DOCS_FORCE for >> "$GITHUB_ENV"
#   run-docs-ci.sh prepare        prepare current docs links or load backfill docs
#   run-docs-ci.sh publish DIR    build slices, then assemble them into DIR
#   run-docs-ci.sh build SITE_DIR build once and save slices under SITE_DIR
#   run-docs-ci.sh assemble SITE_DIR PUBLISH_DIR merge saved slices into PUBLISH_DIR
#
# `prepare`, `build`, `assemble`, and `publish` read GITHUB_EVENT_NAME,
# GITHUB_REF_TYPE and DOC_REF (the release tag for tag pushes and backfills,
# otherwise the branch name). `build`, `assemble`, and `publish` also read DOCS_FORCE.
# Requires jq (preinstalled on GitHub-hosted runners).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Release builds are tag pushes and backfills; branch builds are main pushes and PRs.
is_release() { [[ "$GITHUB_EVENT_NAME" == workflow_dispatch || "$GITHUB_REF_TYPE" == tag ]]; }

# Sets VERSION (X.Y.Z) and MINOR (X.Y) from DOC_REF, or exits. Final tags only, never RCs.
release_vars() {
  [[ "$DOC_REF" =~ ^v([0-9]+\.[0-9]+\.[0-9]+)$ ]] || {
    echo "not a final release tag: $DOC_REF" >&2
    exit 1
  }
  VERSION="${BASH_REMATCH[1]}"
  MINOR="${VERSION%.*}"
}

# Workflow inputs come from the event payload instead of a ${{ }} expression, so
# user-supplied text never becomes shell source. GITHUB_ENV is line-based, so a
# release ref is validated BEFORE it is printed: a crafted value cannot add variables.
emit_env() {
  local ref dry_run force
  ref="$(jq -r '.inputs.ref // empty' "$GITHUB_EVENT_PATH")"
  dry_run="$(jq -r '.inputs.dry_run // false' "$GITHUB_EVENT_PATH")"
  force="$(jq -r '.inputs.force // false' "$GITHUB_EVENT_PATH")"
  [[ "$dry_run" == true || "$dry_run" == false ]] || {
    echo "invalid dry_run input: expected true or false" >&2
    exit 1
  }
  [[ "$force" == true || "$force" == false ]] || {
    echo "invalid force input: expected true or false" >&2
    exit 1
  }
  DOC_REF="${ref:-$GITHUB_REF_NAME}"
  if is_release; then release_vars; fi
  printf 'DOC_REF=%s\nDRY_RUN=%s\nDOCS_FORCE=%s\n' "$DOC_REF" "$dry_run" "$force"
}

reject_docs_symlinks() {
  if [[ -n "$(find docs -type l -print -quit)" ]]; then
    echo "error: docs contains symlinks, which are not supported in published documentation:" >&2
    find docs -type l -print >&2
    exit 1
  fi
}

prepare() (
  [[ "${GITHUB_ACTIONS:-}" == "true" ]] || {
    echo "error: prepare is CI-only; use a temporary docs copy for local previews" >&2
    exit 2
  }
  reject_docs_symlinks
  if [[ "$GITHUB_EVENT_NAME" == workflow_dispatch ]]; then
    release_vars
    git fetch --no-tags origin "refs/tags/$DOC_REF"
    # Swap in the tag's pages but keep today's other files, so anything the configs
    # reference (logo, extra CSS, ...) exists even if the tag predates it. javascripts/
    # and stylesheets/ stay as they are: they hold the current version switcher.
    # Zensical's strict build still requires every page in today's nav to exist in the tag.
    find docs -name '*.md' -delete
    git archive FETCH_HEAD docs | tar -x --exclude='docs/javascripts' --exclude='docs/stylesheets'
    reject_docs_symlinks
    # Historical backfills keep version-to-version links relative to their own version.
    python3 scripts/prepare_docs_links.py --docs-dir docs --allow-in-place
  else
    python3 scripts/prepare_docs_links.py \
      --docs-dir docs \
      --allow-in-place \
      --versioned-prefix latest
  fi
)

target_pairs() {
  if is_release; then
    release_vars
    printf 'zensical-versioned.toml\t%s\n' "$MINOR"
  else
    printf 'zensical-unversioned.toml\troot\n'
    printf 'zensical-versioned.toml\tmain\n'
  fi
}

publish() (
  local dir="$1" site_dir
  site_dir="$(mktemp -d)"
  trap 'rm -rf "$site_dir"' EXIT
  build "$site_dir"
  assemble "$site_dir" "$dir"
)

build() {
  local site_dir="$1" config target
  mkdir -p "$site_dir"
  local release_args=() force_args=()
  local targets
  targets="$(target_pairs)"
  if is_release; then
    release_vars
    release_args=(--release "$VERSION")
    if [[ "$GITHUB_EVENT_NAME" == workflow_dispatch && "${DOCS_FORCE:-false}" == true ]]; then
      force_args+=(--force)
    fi
  fi
  while IFS=$'\t' read -r config target; do
    bash scripts/publish-docs.sh \
      --config "$config" \
      --target "$target" \
      "${release_args[@]+"${release_args[@]}"}" \
      "${force_args[@]+"${force_args[@]}"}" \
      --build-only \
      --artifact-dir "$site_dir/$target" \
      < /dev/null
  done <<< "$targets"
}

assemble() {
  local site_dir="$1" publish_dir="$2" config target
  local release_args=() force_args=()
  local targets
  targets="$(target_pairs)"
  mkdir -p "$publish_dir"
  if is_release; then
    release_vars
    release_args=(--release "$VERSION")
    if [[ "$GITHUB_EVENT_NAME" == workflow_dispatch && "${DOCS_FORCE:-false}" == true ]]; then
      force_args+=(--force)
    fi
  fi
  while IFS=$'\t' read -r config target; do
    bash scripts/publish-docs.sh \
      --config "$config" \
      --target "$target" \
      --publish-dir "$publish_dir" \
      "${release_args[@]+"${release_args[@]}"}" \
      --assemble-only \
      --artifact-dir "$site_dir/$target" \
      "${force_args[@]+"${force_args[@]}"}" \
      < /dev/null
  done <<< "$targets"
}

case "${1:-}" in
  env) emit_env ;;
  prepare) prepare ;;
  publish) [[ $# -eq 2 ]] || { echo "usage: $0 publish DIR" >&2; exit 2; }; publish "$2" ;;
  build) [[ $# -eq 2 ]] || { echo "usage: $0 build SITE_DIR" >&2; exit 2; }; build "$2" ;;
  assemble) [[ $# -eq 3 ]] || { echo "usage: $0 assemble SITE_DIR PUBLISH_DIR" >&2; exit 2; }; assemble "$2" "$3" ;;
  *) echo "usage: $0 env | prepare | publish DIR | build SITE_DIR | assemble SITE_DIR PUBLISH_DIR" >&2; exit 2 ;;
esac
