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

# Build historical versioned docs from temporary branch snapshots and serve
# the assembled site. The active worktree is never checked out or modified.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
if (($#)); then
  BRANCHES=("$@")
else
  BRANCHES=(0.1 0.2 0.3)
fi
PUBLISH_DIR="$(mktemp -d "${TMPDIR:-/tmp}/superset-operator-docs-branches.XXXXXX")"
SOURCE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/superset-operator-docs-sources.XXXXXX")"

cleanup() {
  rm -rf "$SOURCE_DIR" "$PUBLISH_DIR"
}
trap cleanup EXIT INT TERM

for tool in git python3 rsync; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "error: required tool not found: $tool" >&2
    exit 127
  }
done

if [[ -z "${ZENSICAL:-}" ]]; then
  if [[ -x "$ROOT_DIR/venv/bin/zensical" ]]; then
    ZENSICAL="$ROOT_DIR/venv/bin/zensical"
  elif [[ -x "$ROOT_DIR/.venv/bin/zensical" ]]; then
    ZENSICAL="$ROOT_DIR/.venv/bin/zensical"
  else
    ZENSICAL="zensical"
  fi
fi

echo "==> Building the current root site"
"$SCRIPT_DIR/publish-docs.sh" \
  --config "$ROOT_DIR/zensical.toml" \
  --target root \
  --publish-dir "$PUBLISH_DIR"

echo "==> Building the current main version"
"$SCRIPT_DIR/publish-docs.sh" \
  --config "$ROOT_DIR/zensical-versioned.toml" \
  --target main \
  --publish-dir "$PUBLISH_DIR"

for branch_spec in "${BRANCHES[@]}"; do
  branch="${branch_spec%%:*}"
  release=""
  source_ref="$branch"
  if [[ "$branch_spec" == *:* ]]; then
    release="${branch_spec#*:}"
    if [[ "$release" == *:* ]]; then
      source_ref="${release#*:}"
      release="${release%%:*}"
    fi
  fi
  [[ -n "$branch" && -n "$source_ref" ]] || {
    echo "error: expected VERSION[:RELEASE[:SOURCE_REF]], got: $branch_spec" >&2
    exit 2
  }
  source="$SOURCE_DIR/$branch"

  git -C "$ROOT_DIR" rev-parse --verify --quiet "${source_ref}^{tree}" >/dev/null || {
    echo "error: source ref does not exist: $source_ref" >&2
    exit 2
  }
  echo "==> Extracting $source_ref as version $branch"
  mkdir -p "$source"
  git -C "$ROOT_DIR" archive --format=tar "$source_ref" | tar -xf - -C "$source"

  # Historical branches predate the custom versioning files. Overlay only the
  # current versioning machinery and its small theme assets into the temporary
  # snapshot; the documentation source remains exactly that branch's source.
  cp "$ROOT_DIR/zensical-versioned.toml" "$source/zensical-versioned.toml"
  cp "$ROOT_DIR/scripts/docs_versions.py" "$source/scripts/docs_versions.py"
  cp "$ROOT_DIR/scripts/prepare-historical-docs.py" "$source/scripts/prepare-historical-docs.py"
  cp "$ROOT_DIR/scripts/publish-docs.sh" "$source/scripts/publish-docs.sh"
  mkdir -p "$source/docs/javascripts" "$source/docs/stylesheets"
  cp "$ROOT_DIR/docs/javascripts/version-switcher.js" "$source/docs/javascripts/version-switcher.js"
  cp "$ROOT_DIR/docs/stylesheets/extra.css" "$source/docs/stylesheets/extra.css"
  cp "$ROOT_DIR/docs/stylesheets/version-banner.css" "$source/docs/stylesheets/version-banner.css"
  python3 "$source/scripts/prepare-historical-docs.py" --docs-dir "$source/docs"

  args=(
    --config "$source/zensical-versioned.toml"
    --target "$branch"
    --publish-dir "$PUBLISH_DIR"
  )
  [[ -z "$release" ]] || args+=(--release "$release")
  ZENSICAL="$ZENSICAL" "$source/scripts/publish-docs.sh" "${args[@]}"
done

address="${DOCS_PREVIEW_ADDR:-127.0.0.1:8765}"
host="${address%:*}"
port="${address##*:}"
echo "Serving assembled documentation from $PUBLISH_DIR on http://$address"
python3 -m http.server "$port" --bind "$host" --directory "$PUBLISH_DIR"
