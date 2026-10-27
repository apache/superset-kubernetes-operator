#!/usr/bin/env python3
"""Rewrite relative documentation links for the assembled multi-version site.

Link rules (docs are written with plain relative .md links; this runs in CI only):
  * any page -> shared page          absolute root URL (shared pages live at the site root)
  * shared page -> versioned page    /<prefix>/... (only with --versioned-prefix, e.g. latest)
  * versioned page -> versioned page left relative, so every version stays self-contained

Handled: inline markdown links, with or without a title.
Not handled: reference-style definitions, raw HTML <a href>, and links inside
code fences (they are rewritten like any other text). A leftover broken link
should surface as a warning in the strict build.

This rewrites files in place. Run it on a CI checkout or a throwaway copy of
docs/, never on a working tree you want to keep.

SHARED_PAGES must stay in sync with the pages excluded from the versioned
build (``exclude_docs`` in zensical-versioned.toml) and built by
``zensical-unversioned.toml``.
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
    base_url: str = "",
    versioned_pages: dict[str, str] | None = None,
) -> bool:
    relative = PurePosixPath(path.relative_to(docs_dir).as_posix())
    # Only shared pages point at versioned pages by URL; a versioned page's links to
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
    parser.add_argument(
        "--versioned-prefix",
        help="rewrite links from shared pages to versioned pages under this URL prefix (for example, latest)",
    )
    args = parser.parse_args()
    base_url = args.site_url.rstrip("/")
    prefix = args.versioned_prefix.strip("/") if args.versioned_prefix else None
    if args.versioned_prefix is not None and not prefix:
        parser.error("--versioned-prefix must contain a path segment")
    versioned_pages = versioned_page_urls(args.docs_dir, prefix) if prefix else None
    count = sum(
        rewrite_file(path, args.docs_dir, base_url, versioned_pages)
        for path in args.docs_dir.rglob("*.md")
        if not path.is_symlink()  # backfills extract old tags; never write through a link
    )
    print(f"==> Rewrote links in {count} documentation files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())