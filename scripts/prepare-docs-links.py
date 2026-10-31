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

"""Rewrite relative documentation links for the assembled multi-version site.

Docs are written with plain relative .md links; this makes them work across sections:
  * any page -> shared page        absolute URL under SITE_PATH_PREFIX
  * shared page -> versioned page  absolute URL under /<prefix>/ (for example latest)
  * versioned -> versioned         left relative, so each version stays self-contained

Only inline markdown links are handled (not reference-style, raw HTML, or code
fences). Files are rewritten in place: run it in CI or on a throwaway copy of docs/.

Usage::

    prepare-docs-links.py --docs-dir DIR [--versioned-prefix PREFIX]

    --docs-dir          the docs/ tree to rewrite
    --versioned-prefix  also rewrite shared -> versioned links under /PREFIX/ (use
                        ``latest`` for normal builds; omit for historical backfills,
                        where shared pages are not built)

Locally, always work on a copy::

    cp -r docs /tmp/docs-links
    python3 scripts/prepare-docs-links.py --docs-dir /tmp/docs-links --versioned-prefix latest

SHARED_PAGES must match ``exclude_docs`` in zensical-versioned.toml and the
pages built by zensical-unversioned.toml.
"""

from __future__ import annotations

import argparse
import posixpath
import re
from pathlib import Path, PurePosixPath

# GitHub Pages serves this project under its repository name.
SITE_PATH_PREFIX = "/superset-kubernetes-operator"

# Source path (relative to docs/) -> URL path at the site root.
SHARED_PAGES = {
    "index.md": "/",
    "reference/downloads.md": "/reference/downloads/",
    "reference/releases.md": "/reference/releases/",
    "reference/security.md": "/reference/security/",
    "contributing/development-setup.md": "/contributing/development-setup/",
    "contributing/development-guidelines.md": "/contributing/development-guidelines/",
    "contributing/releasing.md": "/contributing/releasing/",
}
# [text](target) or [text](target "title")
MARKDOWN_LINK = re.compile(r"\]\((?P<target>[^)\s]+)(?P<title>\s+(?:\"[^\"]*\"|'[^']*'))?\)")


def versioned_page_urls(docs_dir: Path, prefix: str) -> dict[str, str]:
    pages = {}
    for path in docs_dir.rglob("*.md"):
        relative = path.relative_to(docs_dir).as_posix()
        if relative in SHARED_PAGES:
            continue
        if relative.endswith("/index.md"):
            page = relative[: -len("index.md")]
        else:
            page = relative[: -len(".md")] + "/"
        pages[relative] = f"/{prefix}/{page}"
    return pages


def rewrite_file(
    path: Path,
    docs_dir: Path,
    versioned_pages: dict[str, str] | None = None,
) -> bool:
    relative = PurePosixPath(path.relative_to(docs_dir).as_posix())
    # Only shared pages point at versioned pages by URL. A versioned page's links to
    # other versioned pages must stay relative or they would jump to another version.
    link_to_versioned = versioned_pages if relative.as_posix() in SHARED_PAGES else None
    original = path.read_text()

    def replace(match: re.Match[str]) -> str:
        link_path, separator, fragment = match.group("target").partition("#")
        if not link_path or link_path.startswith(("/", "#")) or "://" in link_path:
            return match.group(0)
        resolved = posixpath.normpath(posixpath.join(str(relative.parent), link_path))
        page = SHARED_PAGES.get(resolved)
        if page is None and link_to_versioned is not None:
            page = link_to_versioned.get(resolved)
        if page is None:
            return match.group(0)
        target = f"{SITE_PATH_PREFIX}{page}" + (f"#{fragment}" if separator else "")
        return f"]({target}{match.group('title') or ''})"

    rewritten = MARKDOWN_LINK.sub(replace, original)
    if rewritten == original:
        return False
    path.write_text(rewritten)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--docs-dir", type=Path, required=True)
    parser.add_argument(
        "--versioned-prefix",
        help="rewrite links from shared pages to versioned pages under this URL prefix (for example, latest)",
    )
    args = parser.parse_args()
    prefix = args.versioned_prefix.strip("/") if args.versioned_prefix else None
    if args.versioned_prefix is not None and not prefix:
        parser.error("--versioned-prefix must contain a path segment")
    versioned_pages = versioned_page_urls(args.docs_dir, prefix) if prefix else None
    count = sum(
        rewrite_file(path, args.docs_dir, versioned_pages)
        for path in args.docs_dir.rglob("*.md")
        if not path.is_symlink()  # backfills extract old tags; never write through a link
    )
    print(f"==> Rewrote links in {count} documentation files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())