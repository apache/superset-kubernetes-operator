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

# Generate release email drafts. The script prints templates only; it does not
# send mail.

set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }

usage() {
  cat <<EOF
Usage:
  $0 vote
  $0 result
  $0 announce

Run from the release commit. The script infers v<version>-rc<n> and/or
v<version> tags from HEAD.
EOF
}

MODE="${1:-}"
[[ -n "$MODE" ]] || { usage >&2; die "mode is required"; }
shift || true
[[ $# -eq 0 ]] || die "unexpected arguments: $*"

PROJECT="apache-superset-kubernetes-operator"
DISPLAY_NAME="Apache Superset Kubernetes Operator"
DIST_DEV_BASE="https://dist.apache.org/repos/dist/dev/superset"
DIST_RELEASE_BASE="https://dist.apache.org/repos/dist/release/superset"
GITHUB_BASE="https://github.com/apache/superset-kubernetes-operator"
DOWNLOADS_BASE="https://superset.apache.org/downloads/"

version_re() {
  printf "%s" "$1" | sed 's/[.]/\\./g'
}

latest_rc_tag_on_head() {
  git tag --points-at HEAD \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+-rc[1-9][0-9]*$' \
    | awk -F-rc '{ print $2 ":" $0 }' \
    | sort -t: -k1,1n \
    | tail -1 \
    | cut -d: -f2- \
    || true
}

final_tag_on_head() {
  git tag --points-at HEAD \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | tail -1 \
    || true
}

require_rc() {
  RC_TAG="$(latest_rc_tag_on_head)"
  [[ -n "$RC_TAG" ]] || die "no v<version>-rc<n> tag found on HEAD"
  VERSION="${RC_TAG#v}"
  VERSION="${VERSION%-rc*}"
  RC="${RC_TAG##*-rc}"
}

require_final() {
  FINAL_TAG="$(final_tag_on_head)"
  [[ -n "$FINAL_TAG" ]] || die "no v<version> final tag found on HEAD"
  VERSION="${FINAL_TAG#v}"
}

case "$MODE" in
  vote)
    require_rc
    cat <<EOF
Subject: [VOTE] Release ${DISPLAY_NAME} ${VERSION}-rc${RC}

Hello,

This is a vote to release ${DISPLAY_NAME} ${VERSION} based on candidate rc${RC}.

The release candidate artifacts are staged at:
  ${DIST_DEV_BASE}/kubernetes-operator-${VERSION}-rc${RC}/

The git tag is:
  ${GITHUB_BASE}/releases/tag/v${VERSION}-rc${RC}

PGP keys used for signing are listed in:
  ${DIST_RELEASE_BASE}/KEYS

To verify the candidate (Go 1.26+, Helm, and Apache Rat must be installed
locally; make will fetch additional Go modules and tools on first use):

  curl -O ${DIST_DEV_BASE}/kubernetes-operator-${VERSION}-rc${RC}/${PROJECT}-${VERSION}-rc${RC}-source.tar.gz{,.asc,.sha512}
  curl -O ${DIST_RELEASE_BASE}/KEYS
  gpg --import KEYS
  gpg --verify ${PROJECT}-${VERSION}-rc${RC}-source.tar.gz{.asc,}
  shasum -a 512 -c ${PROJECT}-${VERSION}-rc${RC}-source.tar.gz.sha512
  tar -xzf ${PROJECT}-${VERSION}-rc${RC}-source.tar.gz
  cd ${PROJECT}-${VERSION}/
  make test-unit helm-lint check-license

Notable changes are documented in:
  ${GITHUB_BASE}/blob/v${VERSION}-rc${RC}/docs/reference/releases.md

The vote will be open for at least 72 hours.

[ ] +1 release this package
[ ]  0 no opinion
[ ] -1 do not release this package because ...

Thanks,
<release manager>
EOF
    ;;

  result)
    require_rc
    cat <<EOF
Subject: [RESULT][VOTE] Release ${DISPLAY_NAME} ${VERSION}-rc${RC}

Hello,

The vote to release ${DISPLAY_NAME} ${VERSION} based on candidate rc${RC}
has passed.

The vote tally is:

Binding +1:
- <name>
- <name>
- <name>

Non-binding +1:
- <name>

0:
- <name>

-1:
- <name and reason, or "None">

The release will now be finalized.

Thanks,
<release manager>
EOF
    ;;

  announce)
    require_final
    cat <<EOF
Subject: [ANNOUNCE] Apache Superset Kubernetes Operator ${VERSION} released

The Apache Superset team is pleased to announce the release of
${DISPLAY_NAME} ${VERSION}.

The signed source release is available at:
  ${DIST_RELEASE_BASE}/kubernetes-operator-${VERSION}/

Downloads and verification instructions are available at:
  ${DOWNLOADS_BASE}

Release notes are available at:
  ${GITHUB_BASE}/blob/v${VERSION}/docs/reference/releases.md

Thanks to everyone who contributed to this release.

The Apache Superset team
EOF
    ;;

  -h|--help)
    usage
    ;;

  *)
    usage >&2
    die "unknown mode: ${MODE}"
    ;;
esac
