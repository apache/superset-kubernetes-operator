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

# Generate ASF source-release artifacts (tarball, detached signature,
# SHA-512 checksum) for an operator release candidate or final release.
#
# Usage:
#   scripts/release-source.sh                         # infer tag from HEAD
#   scripts/release-source.sh <version> --rc <n>      # produce a specific RC
#   scripts/release-source.sh --finalize              # promote voted RC → final
#
# In RC mode the script:
#   1. Archives the v<version>-rc<n> git tag into
#      apache-superset-kubernetes-operator-<version>-rc<n>-source.tar.gz
#   2. Detached-signs it with the newest usable local apache.org secret key
#      (override with --gpg-key <id>)
#   3. Writes a SHA-512 checksum file with a *bare* filename so verifiers can
#      run `shasum -a 512 -c <file>.sha512` after a plain download.
#
# In finalize mode the script:
#   1. Requires HEAD to have both v<version> and v<version>-rc<n> tags.
#   2. Re-derives the final filenames from the RC artifacts: copies the tarball
#      + .asc (detached signatures verify file contents, not the filename, so
#      the same signature stays valid) and regenerates the .sha512 file (since
#      shasum embeds the filename).
#   2. Verifies the result before exiting.
#
# Run from the repository root.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
RESET='\033[0m'

die()  { echo -e "${RED}error:${RESET} $*" >&2; exit 1; }
info() { echo -e "${GREEN}==>${RESET} $*"; }
warn() { echo -e "${YELLOW}warning:${RESET} $*"; }

usage() {
  cat <<EOF
Usage:
  $0 [<version>] [--rc <n>] [--gpg-key <id>] [--out-dir <dir>]
  $0 [<version>] --finalize [--rc-dir <dir>] [--gpg-key <id>] [--out-dir <dir>]

Options:
  <version>         Release version. Defaults to the release tag on HEAD.
  --rc <n>          Build artifacts for v<version>-rc<n>. Defaults to the
                    latest local v<version>-rc<n> tag on HEAD.
  --finalize        Promote RC artifacts in --rc-dir to final filenames.
                    Defaults when HEAD has a final v<version> tag.
  --rc-dir <dir>    Directory containing the staged RC artifacts. Defaults to
                    ./dist/<version>-rc<n> when a matching RC tag is on HEAD.
  --gpg-key <id>    GPG key id/fingerprint for the .asc signature.
                    Defaults to the newest usable local secret key with an
                    apache.org UID.
  --out-dir <dir>   Output directory. Defaults to ./dist/<version>-rc<n> in
                    RC mode and ./dist/<version> in finalize mode.
EOF
}

VERSION=""
RC=""
MODE=""
RC_DIR=""
GPG_KEY=""
OUT_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rc)        RC="$2"; MODE="rc"; shift 2 ;;
    --finalize)  MODE="finalize"; shift ;;
    --rc-dir)    RC_DIR="$2"; shift 2 ;;
    --gpg-key)   GPG_KEY="$2"; shift 2 ;;
    --out-dir)   OUT_DIR="$2"; shift 2 ;;
    -h|--help)   usage; exit 0 ;;
    -*)          die "unknown flag: $1" ;;
    *)           VERSION="$1"; shift ;;
  esac
done

command -v gpg     >/dev/null 2>&1 || die "gpg is required"
command -v git     >/dev/null 2>&1 || die "git is required"

PROJECT="apache-superset-kubernetes-operator"

detect_apache_signing_key() {
  local now
  now=$(date +%s)
  gpg --list-secret-keys --with-colons --fingerprint 2>/dev/null | awk -F: -v now="$now" '
    /^sec:/ {
      fpr = ""
      created = $6
      expires = $7
      usable = ($2 != "e" && $2 != "r" && (expires == "" || expires > now) && $12 ~ /[sS]/)
    }
    /^fpr:/ && fpr == "" { fpr = $10 }
    /^uid:/ && usable && $10 ~ /<[^>]+@apache[.]org>/ && fpr != "" {
      print created ":" fpr
      fpr = ""
    }
  ' | sort -t: -k1,1n | tail -1 | cut -d: -f2
}

signing_key() {
  if [[ -n "$GPG_KEY" ]]; then
    printf "%s\n" "$GPG_KEY"
  else
    detect_apache_signing_key
  fi
}

command -v shasum  >/dev/null 2>&1 || die "shasum is required"

version_re() {
  printf "%s" "$1" | sed 's/[.]/\\./g'
}

latest_rc_tag_on_head() {
  local version="$1"
  local pattern

  if [[ -n "$version" ]]; then
    pattern="^v$(version_re "$version")-rc[1-9][0-9]*$"
  else
    pattern='^v[0-9]+\.[0-9]+\.[0-9]+-rc[1-9][0-9]*$'
  fi

  git tag --points-at HEAD \
    | grep -E "$pattern" \
    | awk -F-rc '{ print $2 ":" $0 }' \
    | sort -t: -k1,1n \
    | tail -1 \
    | cut -d: -f2- \
    || true
}

final_tag_on_head() {
  local version="$1"
  local pattern

  if [[ -n "$version" ]]; then
    pattern="^v$(version_re "$version")$"
  else
    pattern='^v[0-9]+\.[0-9]+\.[0-9]+$'
  fi

  git tag --points-at HEAD | grep -E "$pattern" | tail -1 || true
}

infer_release_identity() {
  local final_tag rc_tag

  final_tag="$(final_tag_on_head "$VERSION")"
  rc_tag="$(latest_rc_tag_on_head "$VERSION")"

  if [[ -z "$MODE" ]]; then
    if [[ -n "$final_tag" ]]; then
      MODE="finalize"
    else
      MODE="rc"
    fi
  fi

  case "$MODE" in
    rc)
      if [[ -z "$rc_tag" ]]; then
        die "no RC tag found on HEAD; create one with scripts/release-rc.sh first"
      fi
      if [[ -z "$VERSION" ]]; then
        VERSION="${rc_tag#v}"
        VERSION="${VERSION%-rc*}"
      fi
      if [[ -z "$RC" ]]; then
        RC="${rc_tag##*-rc}"
      fi
      ;;

    finalize)
      if [[ -z "$final_tag" ]]; then
        die "no final release tag found on HEAD; create one with scripts/release-finalize.sh first"
      fi
      if [[ -z "$rc_tag" ]]; then
        die "no matching RC tag found on HEAD; final artifacts must promote the voted RC bytes"
      fi
      if [[ -z "$VERSION" ]]; then
        VERSION="${final_tag#v}"
      fi
      if [[ -z "$RC" && -n "$rc_tag" ]]; then
        RC="${rc_tag##*-rc}"
      fi
      ;;
  esac
}

infer_release_identity

[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "version must be semver (e.g., 0.2.0), got: $VERSION"
[[ "$MODE" == "rc" || "$MODE" == "finalize" ]] || die "unknown mode: ${MODE}"

# Compute the SHA-512 with a bare filename inside the file (no path) so
# downstream `shasum -a 512 -c <file>.sha512` works after a plain download.
write_sha512() {
  local file="$1"
  local dir base
  dir=$(dirname "$file")
  base=$(basename "$file")
  ( cd "$dir" && shasum -a 512 "$base" > "${base}.sha512" )
}

verify_artifacts() {
  local dir="$1"
  local base="$2"
  info "Verifying ${base}"
  ( cd "$dir" && shasum -a 512 -c "${base}.sha512" )
  ( cd "$dir" && gpg --verify "${base}.asc" "${base}" )
}

case "$MODE" in
  rc)
    [[ "$RC" =~ ^[1-9][0-9]*$ ]] || die "rc must be a positive integer"

    TAG="v${VERSION}-rc${RC}"
    BASE="${PROJECT}-${VERSION}-rc${RC}-source.tar.gz"
    : "${OUT_DIR:=dist/${VERSION}-rc${RC}}"
    mkdir -p "$OUT_DIR"
    OUT="${OUT_DIR}/${BASE}"

    git rev-parse --verify "${TAG}^{tag}" >/dev/null 2>&1 \
      || die "git tag ${TAG} does not exist; create it with scripts/release-rc.sh first"
    [[ "$(git rev-parse "${TAG}^{commit}")" == "$(git rev-parse HEAD)" ]] \
      || die "git tag ${TAG} does not point to HEAD"

    info "Archiving ${TAG} → ${OUT}"
    git archive --format=tar.gz \
      --prefix="${PROJECT}-${VERSION}/" \
      -o "${OUT}" "${TAG}"

    info "Signing ${BASE}"
    SIGNING_KEY="$(signing_key)"
    [[ -n "$SIGNING_KEY" ]] || die "no usable local apache.org secret signing key found; rerun with --gpg-key <fingerprint>"
    info "Using GPG key ${SIGNING_KEY}"
    gpg --armor --local-user "$SIGNING_KEY" \
        --output "${OUT}.asc" --detach-sig "${OUT}"

    info "Computing SHA-512"
    write_sha512 "${OUT}"

    verify_artifacts "${OUT_DIR}" "${BASE}"

    echo ""
    echo -e "${BOLD}Source artifacts ready in ${OUT_DIR}/${RESET}"
    echo "  ${BASE}"
    echo "  ${BASE}.asc"
    echo "  ${BASE}.sha512"
    ;;

  finalize)
    if [[ -z "$RC_DIR" ]]; then
      [[ "$RC" =~ ^[1-9][0-9]*$ ]] \
        || die "cannot infer --rc-dir because no matching RC tag was found on HEAD"
      RC_DIR="dist/${VERSION}-rc${RC}"
      info "Using inferred RC artifact directory ${RC_DIR}"
    fi
    [[ -d "$RC_DIR" ]] || die "rc directory does not exist: ${RC_DIR}"
    : "${OUT_DIR:=dist/${VERSION}}"
    mkdir -p "$OUT_DIR"

    # Locate the RC tarball and derive the final filename from the version arg.
    RC_TARBALL=$(find "$RC_DIR" -maxdepth 1 -type f \
                  -name "${PROJECT}-${VERSION}-rc*-source.tar.gz" | head -1)
    [[ -n "$RC_TARBALL" ]] || die "no RC tarball found in ${RC_DIR}"
    RC_BASE=$(basename "$RC_TARBALL")
    [[ -f "${RC_TARBALL}.asc" ]]    || die "missing detached signature: ${RC_BASE}.asc"
    [[ -f "${RC_TARBALL}.sha512" ]] || die "missing checksum file: ${RC_BASE}.sha512"

    FINAL_BASE="${PROJECT}-${VERSION}-source.tar.gz"
    FINAL_OUT="${OUT_DIR}/${FINAL_BASE}"

    info "Copying ${RC_BASE} → ${FINAL_BASE} (bytes preserved)"
    cp "${RC_TARBALL}" "${FINAL_OUT}"

    info "Copying detached signature (signature verifies contents, not filename)"
    cp "${RC_TARBALL}.asc" "${FINAL_OUT}.asc"

    info "Regenerating SHA-512 for the renamed file"
    write_sha512 "${FINAL_OUT}"

    verify_artifacts "${OUT_DIR}" "${FINAL_BASE}"

    echo ""
    echo -e "${BOLD}Final source artifacts ready in ${OUT_DIR}/${RESET}"
    echo "  ${FINAL_BASE}"
    echo "  ${FINAL_BASE}.asc"
    echo "  ${FINAL_BASE}.sha512"
    ;;
esac
