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

# Build or assemble one slice of the docs site.
#
#   root   unversioned pages, built from main      -> <publish-dir>/
#   main   unreleased docs                         -> <publish-dir>/main/
#   X.Y    a minor release (patches overwrite it)  -> <publish-dir>/X.Y/
#
# Versioned targets also refresh versions.json and the latest/ redirect tree;
# the site root remains the unversioned main-branch site.
# All version logic lives in docs_versions.py; this script only orchestrates.
#
# Only run with --release for final tags after the release vote, never for RCs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VERSIONS_PY="$SCRIPT_DIR/docs_versions.py"

usage() {
  cat >&2 <<'EOF'
Usage: publish-docs.sh --config FILE --target TARGET --publish-dir DIR [options]

  --config FILE        Zensical config for this target (one per build)
  --target TARGET      root, main, or a minor version such as 0.3
  --publish-dir DIR    directory that holds the assembled site
  --release X.Y.Z      final release being published (versioned targets only)
  --site-dir DIR       build output dir (default: $ROOT_DIR/site, or $SITE_DIR)
  --build-only         build once and save output in --artifact-dir
  --assemble-only      assemble a previously built --artifact-dir
  --artifact-dir DIR   directory containing/storing the built site
  --force              allow overwriting a minor with an older patch
EOF
}

die() { echo "error: $*" >&2; exit 2; }
log() { echo "==> $*"; }

CONFIG="" TARGET="" PUBLISH_DIR="" RELEASE="" FORCE=""
BUILD_ONLY="" ASSEMBLE_ONLY="" ARTIFACT_DIR="" BUILD_CONFIG="" check_dir=""
SITE_DIR="${SITE_DIR:-$ROOT_DIR/site}"
cleanup() {
  [[ -z "$BUILD_CONFIG" ]] || rm -f "$BUILD_CONFIG"
  [[ -z "$check_dir" ]] || rm -rf "$check_dir"
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --force) FORCE=1; shift ;;
    --build-only) BUILD_ONLY=1; shift ;;
    --assemble-only) ASSEMBLE_ONLY=1; shift ;;
    --config|--target|--publish-dir|--release|--site-dir|--artifact-dir)
      [[ $# -ge 2 ]] || { usage; die "$1 requires a value"; }
      case "$1" in
        --config) CONFIG="$2" ;;
        --target) TARGET="$2" ;;
        --publish-dir) PUBLISH_DIR="$2" ;;
        --release) RELEASE="$2" ;;
        --site-dir) SITE_DIR="$2" ;;
        --artifact-dir) ARTIFACT_DIR="$2" ;;
      esac
      shift 2 ;;
    *) usage; die "unknown argument: $1" ;;
  esac
done

[[ -n "$CONFIG" && -n "$TARGET" ]] || { usage; die "missing required argument"; }
[[ -f "$CONFIG" ]] || die "config does not exist: $CONFIG"
[[ -z "$BUILD_ONLY" || -z "$ASSEMBLE_ONLY" ]] || die "--build-only and --assemble-only are mutually exclusive"
if [[ -z "$BUILD_ONLY" ]]; then
  [[ -n "$PUBLISH_DIR" ]] || die "--publish-dir is required unless --build-only is set"
fi
if [[ -n "$BUILD_ONLY" || -n "$ASSEMBLE_ONLY" ]]; then
  [[ -n "$ARTIFACT_DIR" ]] || die "--artifact-dir is required for build/assemble modes"
fi

for tool in python3 rsync; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done
if [[ -z "$ASSEMBLE_ONLY" && -z "${ZENSICAL:-}" ]]; then
  if [[ -x "$ROOT_DIR/venv/bin/zensical" ]]; then
    ZENSICAL="$ROOT_DIR/venv/bin/zensical"
  elif [[ -x "$ROOT_DIR/.venv/bin/zensical" ]]; then
    ZENSICAL="$ROOT_DIR/.venv/bin/zensical"
  else
    ZENSICAL="zensical"
  fi
fi
if [[ -z "$ASSEMBLE_ONLY" ]]; then
  command -v "$ZENSICAL" >/dev/null 2>&1 || { echo "zensical executable not found: $ZENSICAL" >&2; exit 127; }
fi

CONFIG="$(cd "$(dirname "$CONFIG")" && pwd)/$(basename "$CONFIG")"
if [[ -n "$PUBLISH_DIR" ]]; then
  mkdir -p "$PUBLISH_DIR"
  PUBLISH_DIR="$(cd "$PUBLISH_DIR" && pwd)"
fi

# Shared arguments for docs_versions.py. The "${arr[@]+...}" form keeps empty
# arrays safe under `set -u` on older bash.
common=(--target "$TARGET")
[[ -z "$PUBLISH_DIR" ]] || common+=(--publish-dir "$PUBLISH_DIR")
[[ -z "$RELEASE" ]] || common+=(--release "$RELEASE")
check_args=("${common[@]}")
[[ -z "$FORCE" ]] || check_args+=(--force)
finalize_args=("${common[@]}")

# Fail fast, before the (slow) build, on bad targets, RC versions, or downgrades.
if [[ -z "$BUILD_ONLY" ]]; then
  python3 "$VERSIONS_PY" check "${check_args[@]}"
else
  check_dir="$(mktemp -d)"
  validate_args=(--publish-dir "$check_dir" --target "$TARGET")
  [[ -z "$RELEASE" ]] || validate_args+=(--release "$RELEASE")
  python3 "$VERSIONS_PY" check "${validate_args[@]}"
fi

BUILD_CONFIG="$CONFIG"
if [[ "$TARGET" != root && -z "$ASSEMBLE_ONLY" ]]; then
  # Canonical URLs and sitemap entries must include the output version directory.
  base_site_url='site_url = "https://apache.github.io/superset-kubernetes-operator/"'
  [[ "$(grep -Fxc "$base_site_url" "$CONFIG" || true)" == 1 ]] || \
    die "expected exactly one canonical site_url in $CONFIG before rewriting target $TARGET"
  BUILD_CONFIG="$(mktemp "$(dirname "$CONFIG")/.zensical-versioned.XXXXXX.toml")"
  sed "s|^site_url = \"https://apache.github.io/superset-kubernetes-operator/\"|site_url = \"https://apache.github.io/superset-kubernetes-operator/$TARGET/\"|" \
    "$CONFIG" > "$BUILD_CONFIG"
  expected_site_url="site_url = \"https://apache.github.io/superset-kubernetes-operator/$TARGET/\""
  grep -Fq "$expected_site_url" "$BUILD_CONFIG" || die "could not set the versioned site_url in $CONFIG"
fi

if [[ -z "$ASSEMBLE_ONLY" ]]; then
  log "Building '$TARGET' with $(basename "$CONFIG")"
  "$ZENSICAL" build --clean --strict --config-file "$BUILD_CONFIG"
  [[ -d "$SITE_DIR" ]] || die "build output not found: $SITE_DIR (does site_dir in the config match --site-dir?)"
  [[ -f "$SITE_DIR/search.json" ]] || die "build produced no search.json (is search enabled in the config?)"
  if [[ -n "$BUILD_ONLY" ]]; then
    mkdir -p "$ARTIFACT_DIR"
    rsync -a --delete "$SITE_DIR/" "$ARTIFACT_DIR/"
    log "Saved built site in $ARTIFACT_DIR"
    exit 0
  fi
else
  SITE_DIR="$ARTIFACT_DIR"
  [[ -d "$SITE_DIR" ]] || die "built site not found: $SITE_DIR"
  [[ -f "$SITE_DIR/search.json" ]] || die "built site has no search.json: $SITE_DIR"
fi

if [[ "$TARGET" == root ]]; then
  log "Syncing root site into $PUBLISH_DIR"
  # Anchored excludes protect only the top-level versioned output from --delete;
  # they do not skip same-named files elsewhere in the source tree.
  rsync -a --delete \
    --exclude='/.git' \
    --exclude='/main/' \
    --exclude='/[0-9]*.[0-9]*/' \
    --exclude='/latest/' \
    --exclude='/versions.json' \
    --exclude='/search-unversioned.json' \
    "$SITE_DIR/" "$PUBLISH_DIR/"
else
  log "Syncing into $PUBLISH_DIR/$TARGET/"
  mkdir -p "$PUBLISH_DIR/$TARGET"
  rsync -a --delete "$SITE_DIR/" "$PUBLISH_DIR/$TARGET/"
fi

touch "$PUBLISH_DIR/.nojekyll"
python3 "$VERSIONS_PY" finalize "${finalize_args[@]}"
log "Done"
