#!/usr/bin/env python3
"""Rewrite relative documentation links for the assembled multi-version site.

Docs are written with plain relative .md links; this makes them work across sections:
  * any page -> shared page        absolute URL under SITE_PATH_PREFIX
  * shared page -> versioned page  absolute URL under /<prefix>/ (for example latest)
  * versioned -> versioned         left relative, so each version stays self-contained

Only inline markdown links are handled (not reference-style, raw HTML, or code
fences), and doc file names must not contain spaces or parentheses. Files are
rewritten in place: run it in CI or on a throwaway copy of docs/. Query strings
and fragments are preserved when a local page link is rewritten.

Usage::

    prepare_docs_links.py --docs-dir DIR [--versioned-prefix PREFIX]

    --docs-dir          the docs/ tree to rewrite
    --versioned-prefix  also rewrite shared -> versioned links under /PREFIX/ (use
                        ``latest`` for normal builds; omit for historical backfills,
                        where shared pages are not built)

Locally, always work on a copy::

    cp -r docs /tmp/docs-links
    python3 scripts/prepare_docs_links.py --docs-dir /tmp/docs-links --versioned-prefix latest

SHARED_PAGES must match ``exclude_docs`` in zensical-versioned.toml and the
pages built by zensical-unversioned.toml; test_docs_scripts.py checks both ways.
"""

from __future__ import annotations

import argparse
import posixpath
import re
from pathlib import Path, PurePosixPath

# GitHub Pages serves this project under its repository name (must match site_url).
SITE_PATH_PREFIX = "/superset-kubernetes-operator"

# Pages built at the site root, as paths relative to docs/
SHARED_PAGES = frozenset(
    {
        "index.md",
        "reference/downloads.md",
        "reference/releases.md",
        "reference/security.md",
        "contributing/development-setup.md",
        "contributing/development-guidelines.md",
        "contributing/releasing.md",
    }
)
# [text](target) or [text](target "title")
MARKDOWN_LINK = re.compile(r"\]\((?P<target>[^)\s]+)(?P<title>\s+(?:\"[^\"]*\"|'[^']*'))?\)")
FENCE_START = re.compile(r"^ {0,3}(`{3,}|~{3,})")
FENCE_END = re.compile(r"^ {0,3}(`+|~+)[ \t]*(?:\r?\n)?$")


def rewrite_outside_fenced_code(text: str, replace) -> str:
    """Rewrite links in Markdown text while preserving fenced code examples."""
    output = []
    fence_char = None
    fence_length = 0
    for line in text.splitlines(keepends=True):
        if fence_char is None:
            opening = FENCE_START.match(line)
            if opening:
                fence_char = opening.group(1)[0]
                fence_length = len(opening.group(1))
                output.append(line)
            else:
                output.append(MARKDOWN_LINK.sub(replace, line))
            continue

        output.append(line)
        closing = FENCE_END.match(line)
        if closing and closing.group(1)[0] == fence_char and len(closing.group(1)) >= fence_length:
            fence_char = None
            fence_length = 0
    return "".join(output)


def page_url(relative: str, prefix: str = "") -> str:
    """URL path of a docs page, e.g. reference/security.md -> /reference/security/."""
    if relative == "index.md" or relative.endswith("/index.md"):
        page = relative[: -len("index.md")]
    else:
        page = relative[: -len(".md")] + "/"
    return f"/{prefix}/{page}" if prefix else f"/{page}"


def versioned_page_urls(docs_dir: Path, prefix: str) -> dict[str, str]:
    return {
        relative: page_url(relative, prefix)
        for relative in (path.relative_to(docs_dir).as_posix() for path in docs_dir.rglob("*.md"))
        if relative not in SHARED_PAGES
    }


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
        target = match.group("target")
        path_and_query, fragment_separator, fragment = target.partition("#")
        link_path, query_separator, query = path_and_query.partition("?")
        if not link_path or link_path.startswith(("/", "#")) or "://" in link_path:
            return match.group(0)
        resolved = posixpath.normpath(posixpath.join(str(relative.parent), link_path))
        if resolved in SHARED_PAGES:
            page = page_url(resolved)
        elif link_to_versioned is not None:
            page = link_to_versioned.get(resolved)
        else:
            page = None
        if page is None:
            return match.group(0)
        rewritten_target = f"{SITE_PATH_PREFIX}{page}"
        if query_separator:
            rewritten_target += f"?{query}"
        if fragment_separator:
            rewritten_target += f"#{fragment}"
        return f"]({rewritten_target}{match.group('title') or ''})"

    rewritten = rewrite_outside_fenced_code(original, replace)
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
