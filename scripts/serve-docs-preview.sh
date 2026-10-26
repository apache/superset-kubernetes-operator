#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
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

# Build the same URL layout used in production and serve it locally.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PUBLISH_DIR="$(mktemp -d "${TMPDIR:-/tmp}/superset-operator-docs.XXXXXX")"
trap 'rm -rf "$PUBLISH_DIR"' EXIT

"$SCRIPT_DIR/publish-docs.sh" \
  --config "$ROOT_DIR/zensical.toml" \
  --target root \
  --publish-dir "$PUBLISH_DIR"
"$SCRIPT_DIR/publish-docs.sh" \
  --config "$ROOT_DIR/zensical-versioned.toml" \
  --target main \
  --publish-dir "$PUBLISH_DIR"

address="${DOCS_PREVIEW_ADDR:-127.0.0.1:8765}"
host="${address%:*}"
port="${address##*:}"
echo "Serving assembled documentation from $PUBLISH_DIR on http://$address"
python3 -m http.server "$port" --bind "$host" --directory "$PUBLISH_DIR"
