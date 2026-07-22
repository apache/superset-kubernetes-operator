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

# renovate: datasource=github-release-attachments depName=oras-project/oras versioning=semver-coerced
ORAS_VERSION="${ORAS_VERSION:-v1.3.3}"
ORAS_VERSION_NO_PREFIX="${ORAS_VERSION#v}"
ORAS_PLATFORM="${ORAS_PLATFORM:-linux_amd64}"
ORAS_SHA256="${ORAS_SHA256:-9ce999f8d2de03fc03968b29d743077a58783e545e5eaa53917ca177352d0e59}"

archive="oras_${ORAS_VERSION_NO_PREFIX}_${ORAS_PLATFORM}.tar.gz"
url="https://github.com/oras-project/oras/releases/download/${ORAS_VERSION}/${archive}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

curl -fsSL "${url}" -o "${tmpdir}/${archive}"
printf '%s  %s\n' "${ORAS_SHA256}" "${tmpdir}/${archive}" | sha256sum -c -
tar -xzf "${tmpdir}/${archive}" -C "${tmpdir}" oras
sudo install -m 0755 "${tmpdir}/oras" /usr/local/bin/oras
