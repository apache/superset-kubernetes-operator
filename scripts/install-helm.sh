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

set -euo pipefail

# renovate: datasource=github-releases depName=helm/helm
HELM_VERSION="${HELM_VERSION:-v4.2.3}"
HELM_PLATFORM="${HELM_PLATFORM:-linux-amd64}"
HELM_SHA256="${HELM_SHA256:-e9b88b4ee95b18c706839c28d3a0220e5bc470e9cd9262410c90793c45ff8b7c}"

archive="helm-${HELM_VERSION}-${HELM_PLATFORM}.tar.gz"
url="https://get.helm.sh/${archive}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

curl -fsSL "${url}" -o "${tmpdir}/${archive}"
printf '%s  %s\n' "${HELM_SHA256}" "${tmpdir}/${archive}" | sha256sum -c -
tar -xzf "${tmpdir}/${archive}" -C "${tmpdir}"
sudo install -m 0755 "${tmpdir}/${HELM_PLATFORM}/helm" /usr/local/bin/helm
