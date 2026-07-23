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

# Installs the mikefarah/yq YAML processor, pinned by version and checksum.
# The pinned SHA256 is for the default linux_amd64 asset (the CI platform);
# override YQ_PLATFORM and YQ_SHA256 together to install elsewhere.

set -euo pipefail

# renovate: datasource=github-release-attachments depName=mikefarah/yq
YQ_VERSION="${YQ_VERSION:-v4.53.3}"
YQ_SHA256="${YQ_SHA256:-fa52a4e758c63d38299163fbdd1edfb4c4963247918bf9c1c5d31d84789eded4}"
YQ_PLATFORM="${YQ_PLATFORM:-linux_amd64}"

asset="yq_${YQ_PLATFORM}"
url="https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/${asset}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

curl -fsSL "${url}" -o "${tmpdir}/yq"
printf '%s  %s\n' "${YQ_SHA256}" "${tmpdir}/yq" | sha256sum -c -
sudo install -m 0755 "${tmpdir}/yq" /usr/local/bin/yq
