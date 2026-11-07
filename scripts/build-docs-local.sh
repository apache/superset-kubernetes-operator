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
# Build the root and versioned documentation site in a temporary directory.
# Usage:
#   ./scripts/build-docs-local.sh [--serve] [--version-path X.Y]
#   --serve       serve the assembled site at http://localhost:8000/superset-kubernetes-operator/
#   --version-path X.Y  also publish the current docs under a minor-version path such as 0.3
#                       (the default build already includes the main slice)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/superset-operator-docs.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT
SERVE=0
TARGET=main
while [[ $# -gt 0 ]]; do
  case "$1" in
    --serve) SERVE=1; shift ;;
    --version-path)
      [[ $# -ge 2 ]] || { echo "usage: $0 [--serve] [--version-path X.Y]" >&2; exit 2; }
      TARGET="$2"
      shift 2
      ;;
    *) echo "usage: $0 [--serve] [--version-path X.Y]" >&2; exit 2 ;;
  esac
done
[[ "$TARGET" == main || "$TARGET" =~ ^[0-9]+\.[0-9]+$ ]] || {
  echo "error: version path must be a minor version such as 0.3" >&2
  exit 2
}

cp -R "$ROOT_DIR/docs" "$WORK_DIR/docs"
mkdir "$WORK_DIR/scripts"
cp "$ROOT_DIR/scripts/run-docs-ci.sh" \
  "$ROOT_DIR/scripts/publish-docs.sh" \
  "$ROOT_DIR/scripts/docs_versions.py" \
  "$ROOT_DIR/scripts/prepare_docs_links.py" \
  "$WORK_DIR/scripts/"
cp "$ROOT_DIR/zensical-unversioned.toml" "$ROOT_DIR/zensical-versioned.toml" "$WORK_DIR/"
cd "$WORK_DIR"

# The publishing script runs from this temporary copy, so pass through the
# executable selected from the original checkout/environment.
if [[ -x "$ROOT_DIR/venv/bin/zensical" ]]; then
  export ZENSICAL="$ROOT_DIR/venv/bin/zensical"
elif [[ -x "$ROOT_DIR/.venv/bin/zensical" ]]; then
  export ZENSICAL="$ROOT_DIR/.venv/bin/zensical"
elif command -v zensical >/dev/null 2>&1; then
  export ZENSICAL="$(command -v zensical)"
fi

export GITHUB_ACTIONS=true
export GITHUB_EVENT_NAME=push
export GITHUB_REF_TYPE=branch
export DOC_REF=main
bash scripts/run-docs-ci.sh prepare
bash scripts/run-docs-ci.sh publish "$WORK_DIR/assembled"
if [[ "$TARGET" != main ]]; then
  bash scripts/publish-docs.sh \
    --config zensical-versioned.toml \
    --target "$TARGET" \
    --publish-dir "$WORK_DIR/assembled"
fi
if [[ "$SERVE" == 1 ]]; then
  mkdir "$WORK_DIR/public"
  ln -s "$WORK_DIR/assembled" "$WORK_DIR/public/superset-kubernetes-operator"
  echo "==> Serving assembled documentation at http://localhost:8000/superset-kubernetes-operator/"
  echo "==> Press Ctrl-C to stop serving; the temporary build will then be removed"
  python3 -m http.server 8000 --directory "$WORK_DIR/public"
else
  echo "==> Assembled docs checked; temporary build will be removed when this command exits"
  echo "==> Use DOCS_SERVE=true with the make target to serve the temporary build"
fi
