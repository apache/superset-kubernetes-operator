#!/usr/bin/env bash
# CI glue for .github/workflows/docs.yml.
#
#   run-docs-ci.sh env            print DOC_REF and DRY_RUN for >> "$GITHUB_ENV"
#   run-docs-ci.sh prepare        prepare current docs links or load backfill docs
#   run-docs-ci.sh publish DIR    build the right slice(s) of the site into DIR
#
# `prepare` and `publish` read GITHUB_EVENT_NAME, GITHUB_REF_TYPE and DOC_REF
# (the release tag for tag pushes and backfills, otherwise the branch name).
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
  local ref dry_run
  ref="$(jq -r '.inputs.ref // empty' "$GITHUB_EVENT_PATH")"
  dry_run="$(jq -r '.inputs.dry_run // false' "$GITHUB_EVENT_PATH")"
  DOC_REF="${ref:-$GITHUB_REF_NAME}"
  if is_release; then release_vars; fi
  printf 'DOC_REF=%s\nDRY_RUN=%s\n' "$DOC_REF" "$dry_run"
}

prepare() {
  if [[ "$GITHUB_EVENT_NAME" == workflow_dispatch ]]; then
    release_vars
    git fetch --no-tags origin "$DOC_REF"
    local assets
    assets="$(mktemp -d)"
    cp docs/javascripts/version-switcher.js docs/stylesheets/version-banner.css "$assets/"
    rm -rf docs
    git archive FETCH_HEAD docs | tar -x
    mkdir -p docs/javascripts docs/stylesheets
    cp "$assets/version-switcher.js" docs/javascripts/
    cp "$assets/version-banner.css" docs/stylesheets/
    # Historical backfills keep version-to-version links relative to their own version.
    python3 scripts/prepare-docs-links.py --docs-dir docs
  else
    python3 scripts/prepare-docs-links.py --docs-dir docs --versioned-prefix latest
  fi
}

publish() {
  local dir="$1"
  mkdir -p "$dir"
  if is_release; then
    release_vars
    scripts/publish-docs.sh --config zensical-versioned.toml --target "$MINOR" --publish-dir "$dir" --release "$VERSION"
  else
    scripts/publish-docs.sh --config zensical-unversioned.toml --target root --publish-dir "$dir"
    scripts/publish-docs.sh --config zensical-versioned.toml --target main --publish-dir "$dir"
  fi
}

case "${1:-}" in
  env) emit_env ;;
  prepare) prepare ;;
  publish) [[ $# -eq 2 ]] || { echo "usage: $0 publish DIR" >&2; exit 2; }; publish "$2" ;;
  *) echo "usage: $0 env | prepare | publish DIR" >&2; exit 2 ;;
esac