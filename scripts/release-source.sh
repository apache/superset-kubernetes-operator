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
#   scripts/release-source.sh <version> --rc <n>     # produce RC artifacts
#   scripts/release-source.sh <version> --finalize \
#       --rc-dir <dir>                                # promote RC → final
#
# In RC mode the script:
#   1. Archives the v<version>-rc<n> git tag into
#      apache-superset-kubernetes-operator-<version>-rc<n>-source.tar.gz
#   2. Detached-signs it with the user's default GPG key (override with
#      --gpg-key <id>)
#   3. Writes a SHA-512 checksum file with a *bare* filename so verifiers can
#      run `shasum -a 512 -c <file>.sha512` after a plain download.
#
# In --finalize mode the script:
#   1. Re-derives the final filenames from the RC-staged artifacts in
#      <rc-dir>: copies the tarball + .asc (detached signatures verify file
#      contents, not the filename, so the same signature stays valid) and
#      regenerates the .sha512 file (since shasum embeds the filename).
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
  $0 <version> --rc <n> [--gpg-key <id>] [--out-dir <dir>]
  $0 <version> --finalize --rc-dir <dir> [--gpg-key <id>] [--out-dir <dir>]

Options:
  --rc <n>          Build artifacts for v<version>-rc<n>.
  --finalize        Promote RC artifacts in --rc-dir to final filenames.
  --rc-dir <dir>    Directory containing the staged RC artifacts.
  --gpg-key <id>    GPG key id/fingerprint for the .asc signature.
                    Required for --finalize when re-signing is needed; in RC
                    mode the GPG default is used when this is omitted.
  --out-dir <dir>   Output directory (default: ./dist).
EOF
}

VERSION=""
RC=""
MODE=""
RC_DIR=""
GPG_KEY=""
OUT_DIR="dist"

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

[[ -n "$VERSION" ]] || { usage >&2; die "version is required"; }
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "version must be semver (e.g., 0.2.0), got: $VERSION"
[[ -n "$MODE" ]] || { usage >&2; die "either --rc <n> or --finalize is required"; }

command -v gpg     >/dev/null 2>&1 || die "gpg is required"
command -v shasum  >/dev/null 2>&1 || die "shasum is required"
command -v git     >/dev/null 2>&1 || die "git is required"

mkdir -p "$OUT_DIR"
PROJECT="apache-superset-kubernetes-operator"

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
    [[ -n "$RC" ]] || die "--rc requires a number"
    [[ "$RC" =~ ^[0-9]+$ ]] || die "rc must be a positive integer"

    TAG="v${VERSION}-rc${RC}"
    BASE="${PROJECT}-${VERSION}-rc${RC}-source.tar.gz"
    OUT="${OUT_DIR}/${BASE}"

    git rev-parse --verify "${TAG}^{tag}" >/dev/null 2>&1 \
      || die "git tag ${TAG} does not exist; create it with scripts/release-rc.sh first"

    info "Archiving ${TAG} → ${OUT}"
    git archive --format=tar.gz \
      --prefix="${PROJECT}-${VERSION}/" \
      -o "${OUT}" "${TAG}"

    info "Signing ${BASE}"
    if [[ -n "$GPG_KEY" ]]; then
      gpg --armor --local-user "$GPG_KEY" \
          --output "${OUT}.asc" --detach-sig "${OUT}"
    else
      gpg --armor --output "${OUT}.asc" --detach-sig "${OUT}"
    fi

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
    [[ -n "$RC_DIR" ]] || die "--finalize requires --rc-dir"
    [[ -d "$RC_DIR" ]] || die "rc directory does not exist: ${RC_DIR}"

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
