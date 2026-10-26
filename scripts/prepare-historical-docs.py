#!/usr/bin/env python3
"""Rewrite relative links to shared root pages in documentation from older tags.

Only needed when backfilling tags cut BEFORE the docs were split into
versioned and unversioned sections: those tags link to the shared pages with
relative paths, which break once the pages live at the site root. Tags released
after the split already use absolute links. Delete this script (and the
workflow step that calls it) once the backfill is done.

Handled: inline markdown links, with or without a title.
Not handled: reference-style definitions and raw HTML <a href>. A leftover
broken link should surface as a warning in the strict build.

SHARED_PAGES must stay in sync with the pages excluded from the versioned
build (``exclude_docs`` in zensical-versioned.toml) and built by zensical.toml.
"""

from __future__ import annotations

import argparse
import posixpath
import re
from pathlib import Path, PurePosixPath

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


def rewrite_file(path: Path, docs_dir: Path, base_url: str = "") -> bool:
    relative = PurePosixPath(path.relative_to(docs_dir).as_posix())
    original = path.read_text()

    def replace(match: re.Match[str]) -> str:
        link_path, separator, fragment = match.group("target").partition("#")
        if not link_path or link_path.startswith(("/", "#")) or "://" in link_path:
            return match.group(0)
        resolved = posixpath.normpath(posixpath.join(str(relative.parent), link_path))
        page = SHARED_PAGES.get(resolved)
        if page is None:
            return match.group(0)
        target = f"{base_url}{page}" + (f"#{fragment}" if separator else "")
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
        "--site-url",
        default="",
        help="absolute site URL to prefix links with; omit only if the site is served from a domain root",
    )
    args = parser.parse_args()
    base_url = args.site_url.rstrip("/")
    count = sum(rewrite_file(path, args.docs_dir, base_url) for path in args.docs_dir.rglob("*.md"))
    print(f"==> Rewrote {count} historical documentation files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())