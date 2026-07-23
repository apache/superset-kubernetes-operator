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

# Installs the helm-unittest plugin, pinned to a known version. Idempotent:
# skips the install when the pinned version is already present.

set -euo pipefail

# renovate: datasource=github-releases depName=helm-unittest/helm-unittest
HELM_UNITTEST_VERSION="${HELM_UNITTEST_VERSION:-v1.1.1}"

installed="$(helm plugin list 2>/dev/null | awk '$1 == "unittest" { print $2 }')"
want="${HELM_UNITTEST_VERSION#v}"
if [ "${installed}" = "${want}" ]; then
  echo "helm-unittest ${HELM_UNITTEST_VERSION} already installed"
  exit 0
fi

# helm 4 verifies plugin provenance by default and the upstream plugin does not
# ship the required metadata yet, so skip verification there. helm 3 has no such
# flag, so only pass it when running helm >= 4.
verify_flag=""
helm_major="$(helm version --short 2>/dev/null | sed -E 's/^v?([0-9]+)\..*/\1/')"
if [ -n "${helm_major}" ] && [ "${helm_major}" -ge 4 ]; then
  verify_flag="--verify=false"
fi

# A stale/partial plugin dir breaks reinstall; remove it first.
helm plugin uninstall unittest >/dev/null 2>&1 || true

# shellcheck disable=SC2086
helm plugin install https://github.com/helm-unittest/helm-unittest \
  --version "${HELM_UNITTEST_VERSION}" ${verify_flag}
