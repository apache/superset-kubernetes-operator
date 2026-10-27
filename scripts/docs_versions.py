#!/usr/bin/env python3
"""Version bookkeeping for the multi-version documentation site.

Site layout (under the publish directory)::

    /                 unversioned pages (built from ``main``), including Home
    /main/            unreleased versioned docs
    /0.3/, /0.4/ ...  one directory per *minor* release (patch releases overwrite their minor)
    /latest/          redirect pages pointing into the newest final minor, or ``main`` before first release
    /versions.json    manifest read by docs/javascripts/version-switcher.js

Versioned builds have no Home page, so each version root (``/0.3/``) is a
redirect to ``LANDING_PAGE``.

``versions.json`` schema (a flat list, newest minor first, ``main`` last)::

    [{"version": "0.4", "title": "0.4", "aliases": ["latest"], "release": "0.4.1"},
     {"version": "0.3", "title": "0.3", "aliases": []},
     {"version": "main", "title": "main (unreleased)", "aliases": []}]

Before the first final release, ``main`` has the ``latest`` alias and the
``latest/`` directory redirects to ``main/``. A final release moves the alias
to the newest minor and replaces those redirects.

The schema is intentionally a small, stable manifest. Nothing here depends on
an external versioning tool. Do not add fields the JS does not understand
without updating it.

The version grammar (``MINOR_RE``, ``main``, ``latest``) is duplicated in
version-switcher.js; this file is the source of truth.

Commands (called by publish-docs.sh)::

    check     validate target/release and refuse patch downgrades (run before building)
    finalize  update versions.json, write the version landing page, regenerate latest/ redirects
"""

from __future__ import annotations

import argparse
import html
import json
import posixpath
import re
import shutil
import sys
from pathlib import Path

ROOT = "root"
MAIN = "main"
LATEST = "latest"
MAIN_TITLE = "main (unreleased)"
MANIFEST = "versions.json"
LANDING_PAGE = "getting-started"  # where each version root (and latest/) redirects to

MINOR_RE = re.compile(r"^\d+\.\d+$")
FINAL_RELEASE_RE = re.compile(r"^\d+\.\d+\.\d+$")  # no pre-release suffixes: RCs are never published


class DocsVersionError(Exception):
    """A user-facing error (bad arguments, refused downgrade, corrupt manifest)."""


# --------------------------------------------------------------------------- #
# Version helpers
# --------------------------------------------------------------------------- #

def version_key(version: str) -> tuple[int, ...]:
    """Numeric sort key, so 0.10 sorts after 0.9."""
    return tuple(int(part) for part in version.split("."))


def validate_args(target: str, release: str | None = None) -> None:
    if target not in (ROOT, MAIN) and not MINOR_RE.fullmatch(target):
        raise DocsVersionError(f"invalid target {target!r}: expected 'root', 'main' or a minor like '0.3'")
    if release is None:
        return
    if target in (ROOT, MAIN):
        raise DocsVersionError("--release is only valid for minor-version targets")
    if not FINAL_RELEASE_RE.fullmatch(release):
        raise DocsVersionError(
            f"--release must be a final X.Y.Z version, got {release!r} (release candidates are never published)"
        )
    if release.rsplit(".", 1)[0] != target:
        raise DocsVersionError(f"--release {release} does not belong to target {target}")


# --------------------------------------------------------------------------- #
# Manifest
# --------------------------------------------------------------------------- #

def load_manifest(publish_dir: Path) -> list[dict]:
    path = publish_dir / MANIFEST
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        return []
    except json.JSONDecodeError as exc:
        raise DocsVersionError(f"{path} is not valid JSON: {exc}") from exc
    if not isinstance(data, list):
        raise DocsVersionError(f"{path} must contain a JSON array")
    return [e for e in data if isinstance(e, dict) and isinstance(e.get("version"), str)]


def check_not_downgrade(entries: list[dict], target: str, release: str | None, force: bool = False) -> None:
    """``0.3.1`` overwrites ``0.3``; publishing ``0.3.0`` afterwards would silently roll it back."""
    if release is None or force:
        return
    existing = next((e.get("release") for e in entries if e["version"] == target), None)
    if existing and version_key(release) < version_key(existing):
        raise DocsVersionError(
            f"{target}/ already contains {existing}; refusing to overwrite it with older {release} (use --force)"
        )


def build_manifest(entries: list[dict], target: str, release: str | None = None) -> list[dict]:
    """Return the new manifest: minors newest-first, then ``main`` as latest only before release."""
    by_version = {e["version"]: dict(e) for e in entries}
    by_version[MAIN] = {**by_version.get(MAIN, {}), "version": MAIN, "title": MAIN_TITLE, "aliases": []}

    if target not in (ROOT, MAIN):
        entry = by_version.get(target, {})
        entry.update(version=target, title=target, aliases=[])
        if release:
            entry["release"] = release
        by_version[target] = entry

    minors = sorted((v for v in by_version if MINOR_RE.fullmatch(v)), key=version_key, reverse=True)
    for index, version in enumerate(minors):
        by_version[version]["aliases"] = [LATEST] if index == 0 else []
    by_version[MAIN]["aliases"] = [] if minors else [LATEST]
    return [by_version[v] for v in minors] + [by_version[MAIN]]


def write_manifest(publish_dir: Path, entries: list[dict]) -> None:
    (publish_dir / MANIFEST).write_text(json.dumps(entries, indent=2) + "\n")


def latest_minor(entries: list[dict]) -> str | None:
    return next(
        (
            e["version"]
            for e in entries
            if MINOR_RE.fullmatch(e["version"]) and LATEST in e.get("aliases", [])
        ),
        None,
    )


def latest_version(entries: list[dict]) -> str | None:
    """Return the latest alias target, including ``main`` before the first release."""
    return next(
        (e["version"] for e in entries if LATEST in e.get("aliases", [])),
        None,
    )


# --------------------------------------------------------------------------- #
# Redirect pages
# --------------------------------------------------------------------------- #

def _absolute(site_url: str | None, path: str) -> str:
    return f"{site_url.rstrip('/')}/{path}" if site_url else path


def redirect_page(relative_url: str, label: str, canonical: str) -> str:
    """A redirect that keeps ``?query`` and ``#hash`` (a bare meta refresh drops them).

    ``label`` names the destination, e.g. "the latest documentation". The meta
    refresh lives in <noscript> so it cannot race the script.
    """
    rel_attr = html.escape(relative_url, quote=True)
    label_html = html.escape(label)
    return (
        "<!doctype html>\n"
        '<meta charset="utf-8">\n'
        f"<title>Redirecting to {label_html}</title>\n"
        f'<link rel="canonical" href="{html.escape(canonical, quote=True)}">\n'
        f'<noscript><meta http-equiv="refresh" content="0; url={rel_attr}"></noscript>\n'
        f"<script>location.replace(new URL({json.dumps(relative_url)}, location.href).href"
        " + location.search + location.hash)</script>\n"
        f'<p>Redirecting to <a href="{rel_attr}">{label_html}</a>…</p>\n'
    )


def write_version_landing(publish_dir: Path, version: str, site_url: str | None = None) -> None:
    """Create the version root page, which redirects to LANDING_PAGE (versioned docs have no Home)."""
    version_root = publish_dir / version
    version_root.mkdir(parents=True, exist_ok=True)
    canonical = _absolute(site_url, f"{version}/{LANDING_PAGE}/") if site_url else f"{LANDING_PAGE}/"
    (version_root / "index.html").write_text(
        redirect_page(f"{LANDING_PAGE}/", f"the {version} documentation", canonical)
    )


def write_latest_redirects(publish_dir: Path, latest: str, site_url: str | None = None) -> int:
    """Mirror ``<latest>/`` as redirect pages under ``latest/``. Returns the page count."""
    alias_root = publish_dir / LATEST
    source_root = publish_dir / latest
    shutil.rmtree(alias_root, ignore_errors=True)

    count = 0
    for source in sorted(source_root.rglob("*.html")):
        relative = source.relative_to(source_root)
        if relative.as_posix() == "404.html":
            continue
        parent = relative.parent.as_posix()
        if relative.as_posix() == "index.html":
            # The version root is itself a redirect; skip that hop.
            real, suffix = f"{latest}/{LANDING_PAGE}", "/"
        elif relative.name == "index.html":
            real, suffix = posixpath.normpath(f"{latest}/{parent}"), "/"
        else:
            real, suffix = f"{latest}/{relative.as_posix()}", ""
        start = posixpath.normpath(f"{LATEST}/{parent}")
        relative_url = posixpath.relpath(real, start) + suffix

        out = alias_root / relative
        out.parent.mkdir(parents=True, exist_ok=True)
        canonical = _absolute(site_url, real + suffix) if site_url else relative_url
        out.write_text(redirect_page(relative_url, "the latest documentation", canonical))
        count += 1
    return count


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #

def cmd_check(args: argparse.Namespace) -> None:
    validate_args(args.target, args.release)
    check_not_downgrade(load_manifest(args.publish_dir), args.target, args.release, args.force)


def cmd_finalize(args: argparse.Namespace) -> None:
    validate_args(args.target, args.release)
    publish_dir: Path = args.publish_dir

    if args.target != ROOT:
        write_version_landing(publish_dir, args.target, args.site_url)
        entries = build_manifest(load_manifest(publish_dir), args.target, args.release)
        write_manifest(publish_dir, entries)
        latest = latest_version(entries)
        if latest and (publish_dir / latest).is_dir():
            pages = write_latest_redirects(publish_dir, latest, args.site_url)
            print(f"==> latest -> {latest} ({pages} redirect pages)")
        else:
            shutil.rmtree(publish_dir / LATEST, ignore_errors=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--publish-dir", type=Path, required=True)
        p.add_argument("--target", required=True)
        p.add_argument("--release")

    check = sub.add_parser("check", help="validate arguments before building")
    common(check)
    check.add_argument("--force", action="store_true", help="allow overwriting a minor with an older patch")
    check.set_defaults(func=cmd_check)

    finalize = sub.add_parser("finalize", help="update manifest and redirects after syncing")
    common(finalize)
    finalize.add_argument("--site-url", help="absolute site URL, used for canonical links")
    finalize.set_defaults(func=cmd_finalize)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except DocsVersionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())