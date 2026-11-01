#!/usr/bin/env bash
# Build one slice of the docs site and merge it into a publish directory.
#
#   root   unversioned pages, built from main      -> <publish-dir>/
#   main   unreleased docs                         -> <publish-dir>/main/
#   X.Y    a minor release (patches overwrite it)  -> <publish-dir>/X.Y/
#
# Versioned targets also refresh versions.json and the latest/ redirect tree;
# the site root remains the unversioned main-branch site.
# All version logic lives in docs_versions.py; this script only orchestrates.
#
# Only run with --release for final tags after the release vote, never for RCs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VERSIONS_PY="$SCRIPT_DIR/docs_versions.py"

usage() {
  cat >&2 <<'EOF'
Usage: publish-docs.sh --config FILE --target TARGET --publish-dir DIR [options]

  --config FILE        Zensical config for this target (one per build)
  --target TARGET      root, main, or a minor version such as 0.3
  --publish-dir DIR    directory that holds the assembled site
  --release X.Y.Z      final release being published (versioned targets only)
  --site-dir DIR       build output dir; must match site_dir in the config
                       (default: $ROOT_DIR/site, or $SITE_DIR)
  --force              allow overwriting a minor with an older patch
EOF
}

die() { echo "error: $*" >&2; exit 2; }
log() { echo "==> $*"; }

CONFIG="" TARGET="" PUBLISH_DIR="" RELEASE="" FORCE=""
SITE_DIR="${SITE_DIR:-$ROOT_DIR/site}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --force) FORCE=1; shift ;;
    --config|--target|--publish-dir|--release|--site-dir)
      [[ $# -ge 2 ]] || { usage; die "$1 requires a value"; }
      case "$1" in
        --config) CONFIG="$2" ;;
        --target) TARGET="$2" ;;
        --publish-dir) PUBLISH_DIR="$2" ;;
        --release) RELEASE="$2" ;;
        --site-dir) SITE_DIR="$2" ;;
      esac
      shift 2 ;;
    *) usage; die "unknown argument: $1" ;;
  esac
done

[[ -n "$CONFIG" && -n "$TARGET" && -n "$PUBLISH_DIR" ]] || { usage; die "missing required argument"; }
[[ -f "$CONFIG" ]] || die "config does not exist: $CONFIG"

for tool in python3 rsync; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
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
command -v "$ZENSICAL" >/dev/null 2>&1 || { echo "zensical executable not found: $ZENSICAL" >&2; exit 127; }

CONFIG="$(cd "$(dirname "$CONFIG")" && pwd)/$(basename "$CONFIG")"
mkdir -p "$PUBLISH_DIR"
PUBLISH_DIR="$(cd "$PUBLISH_DIR" && pwd)"

# Shared arguments for docs_versions.py. The "${arr[@]+...}" form keeps empty
# arrays safe under `set -u` on older bash.
common=(--publish-dir "$PUBLISH_DIR" --target "$TARGET")
[[ -z "$RELEASE" ]] || common+=(--release "$RELEASE")
check_args=("${common[@]}")
[[ -z "$FORCE" ]] || check_args+=(--force)
finalize_args=("${common[@]}")

# Fail fast, before the (slow) build, on bad targets, RC versions, or downgrades.
python3 "$VERSIONS_PY" check "${check_args[@]}"

log "Building '$TARGET' with $(basename "$CONFIG")"
"$ZENSICAL" build --clean --strict --config-file "$CONFIG"
[[ -d "$SITE_DIR" ]] || die "build output not found: $SITE_DIR (does site_dir in the config match --site-dir?)"

if [[ "$TARGET" == root ]]; then
  log "Syncing root site into $PUBLISH_DIR"
  # Anchored excludes protect only the top-level versioned output from --delete;
  # they do not skip same-named files elsewhere in the source tree.
  rsync -a --delete \
    --exclude='/.git' \
    --exclude='/main/' \
    --exclude='/[0-9]*.[0-9]*/' \
    --exclude='/latest/' \
    --exclude='/versions.json' \
    "$SITE_DIR/" "$PUBLISH_DIR/"
else
  log "Syncing into $PUBLISH_DIR/$TARGET/"
  mkdir -p "$PUBLISH_DIR/$TARGET"
  rsync -a --delete "$SITE_DIR/" "$PUBLISH_DIR/$TARGET/"
fi

touch "$PUBLISH_DIR/.nojekyll"
python3 "$VERSIONS_PY" finalize "${finalize_args[@]}"
log "Done"
