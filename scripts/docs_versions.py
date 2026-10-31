#!/usr/bin/env python3
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

"""Version bookkeeping for the multi-version documentation site.

Layout under the publish directory::

    /                 shared pages (built from ``main``), including Home
    /main/            unreleased versioned docs
    /0.3/, /0.4/ ...  one directory per minor release; patches overwrite their minor
    /latest/          redirect pages into the newest minor (into ``main`` before the first release)
    /versions.json    manifest read by docs/javascripts/version-switcher.js

Versioned builds have no Home page, so each version root redirects to ``LANDING_PAGE``.

versions.json is newest minor first, ``main`` last. Before the first release,
``main`` carries the ``latest`` alias::

    [{"version": "0.4", "title": "0.4", "aliases": ["latest"], "release": "0.4.1"},
     {"version": "0.3", "title": "0.3", "aliases": []},
     {"version": "main", "title": "main (unreleased)", "aliases": []}]

The version grammar is duplicated in version-switcher.js; this file is the source of truth.

Usage (normally run by publish-docs.sh, not by hand)::

    docs_versions.py check    --publish-dir DIR --target TARGET [--release X.Y.Z] [--force]
    docs_versions.py finalize --publish-dir DIR --target TARGET [--release X.Y.Z]

    check     validate target/release and refuse patch downgrades; run before building
    finalize  update versions.json, write the version landing page, regenerate latest/
              redirects; run after the site is synced into DIR

TARGET is ``root``, ``main`` or a minor such as ``0.3``; ``--release`` is only valid
for a minor and must be a final X.Y.Z of that minor. Example::

    python3 scripts/docs_versions.py finalize --publish-dir /tmp/site --target 0.3 --release 0.3.1
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
LANDING_PAGE = "getting-started"

MINOR_RE = re.compile(r"^\d+\.\d+$")
FINAL_RELEASE_RE = re.compile(r"^\d+\.\d+\.\d+$")  # no pre-release suffix: RCs are never published


class DocsVersionError(Exception):
    """A user-facing error (bad arguments, refused downgrade, corrupt manifest)."""


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
    for index, entry in enumerate(data):
        if not isinstance(entry, dict) or not isinstance(entry.get("version"), str):
            raise DocsVersionError(f"{path} entry {index} must be an object with a string 'version'")
    return data


def check_not_downgrade(entries: list[dict], target: str, release: str | None, force: bool = False) -> None:
    """``0.3.1`` overwrites ``0.3``, so publishing ``0.3.0`` afterwards would silently roll it back."""
    if release is None or force:
        return
    existing = next((e.get("release") for e in entries if e["version"] == target), None)
    if existing and version_key(release) < version_key(existing):
        raise DocsVersionError(
            f"{target}/ already contains {existing}; refusing to overwrite it with older {release} (use --force)"
        )


def build_manifest(entries: list[dict], target: str, release: str | None = None) -> list[dict]:
    """Minors newest-first; the newest gets ``latest``, or ``main`` when there are none."""
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


def latest_version(entries: list[dict]) -> str | None:
    """The version carrying the ``latest`` alias (``main`` before the first release)."""
    return next(
        (e["version"] for e in entries if LATEST in e.get("aliases", [])),
        None,
    )


def redirect_page(relative_url: str, label: str) -> str:
    """A redirect that keeps ``?query`` and ``#hash``, which a bare meta refresh drops.

    The meta refresh sits in <noscript> so it cannot race the script.
    """
    rel_attr = html.escape(relative_url, quote=True)
    label_html = html.escape(label)
    return (
        "<!doctype html>\n"
        '<meta charset="utf-8">\n'
        f"<title>Redirecting to {label_html}</title>\n"
        f'<noscript><meta http-equiv="refresh" content="0; url={rel_attr}"></noscript>\n'
        f"<script>location.replace(new URL({json.dumps(relative_url)}, location.href).href"
        " + location.search + location.hash)</script>\n"
        f'<p>Redirecting to <a href="{rel_attr}">{label_html}</a>…</p>\n'
    )


def write_version_landing(publish_dir: Path, version: str) -> None:
    """Versioned builds have no Home page, so the version root redirects to LANDING_PAGE."""
    version_root = publish_dir / version
    version_root.mkdir(parents=True, exist_ok=True)
    (version_root / "index.html").write_text(
        redirect_page(f"{LANDING_PAGE}/", f"the {version} documentation")
    )


def write_latest_redirects(publish_dir: Path, latest: str) -> int:
    """Mirror ``<latest>/`` as redirect pages under ``latest/``; returns the page count."""
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
        out.write_text(redirect_page(relative_url, "the latest documentation"))
        count += 1
    return count


def cmd_check(args: argparse.Namespace) -> None:
    validate_args(args.target, args.release)
    check_not_downgrade(load_manifest(args.publish_dir), args.target, args.release, args.force)


def cmd_finalize(args: argparse.Namespace) -> None:
    validate_args(args.target, args.release)
    publish_dir: Path = args.publish_dir

    if args.target != ROOT:
        write_version_landing(publish_dir, args.target)
        entries = build_manifest(load_manifest(publish_dir), args.target, args.release)
        write_manifest(publish_dir, entries)
        latest = latest_version(entries)
        if latest and (publish_dir / latest).is_dir():
            pages = write_latest_redirects(publish_dir, latest)
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