#!/usr/bin/env python3
"""Unit tests for documentation versioning and link preparation."""

import contextlib
import fnmatch
import io
import json
import posixpath
import re
import tempfile
import tomllib
import unittest
from pathlib import Path
from urllib.parse import urlparse

import docs_versions as dv
import prepare_docs_links as phd


REPO_ROOT = Path(__file__).resolve().parent.parent


class DocsVersionsTest(unittest.TestCase):
    def test_switcher_version_segments_match_manifest_rules(self):
        source = (REPO_ROOT / "docs/javascripts/version-switcher.js").read_text()
        match = re.search(r"const VERSION_SEGMENT = /(.+)/;", source)
        self.assertIsNotNone(match, "version-switcher.js must define VERSION_SEGMENT")
        switcher_pattern = re.compile(match.group(1))

        for segment in ("main", "latest", "0.3", "12.34", "0", "0.3.1", "v0.3", "0.x", "٠.٣"):
            with self.subTest(segment=segment):
                python_accepts = segment in (dv.MAIN, dv.LATEST) or bool(dv.MINOR_RE.fullmatch(segment))
                self.assertEqual(bool(switcher_pattern.fullmatch(segment)), python_accepts)

    def test_latest_uses_numeric_not_lexicographic_order(self):
        entries = []
        for minor in ("0.9", "0.10", "0.2"):
            entries = dv.build_manifest(entries, minor)
        self.assertEqual([entry["version"] for entry in entries], ["0.10", "0.9", "0.2", "main"])
        self.assertEqual(dv.latest_version(entries), "0.10")

    def test_main_is_latest_before_first_final_release(self):
        entries = dv.build_manifest([], dv.MAIN)
        self.assertEqual(entries, [{"version": "main", "title": dv.MAIN_TITLE, "aliases": ["latest"]}])
        self.assertEqual(dv.latest_version(entries), "main")
        entries = dv.build_manifest(entries, "0.3", "0.3.0")
        self.assertEqual(entries[-1], {"version": "main", "title": dv.MAIN_TITLE, "aliases": []})
        self.assertEqual(entries[0]["aliases"], ["latest"])
        self.assertEqual(dv.latest_version(entries), "0.3")

    def test_patch_release_overwrites_minor_entry(self):
        entries = dv.build_manifest([], "0.3", "0.3.0")
        entries = dv.build_manifest(entries, "0.3", "0.3.1")
        self.assertEqual([entry["version"] for entry in entries], ["0.3", "main"])
        self.assertEqual(entries[0]["release"], "0.3.1")

    def test_downgrade_refused_unless_forced(self):
        entries = dv.build_manifest([], "0.3", "0.3.2")
        with self.assertRaises(dv.DocsVersionError):
            dv.check_not_downgrade(entries, "0.3", "0.3.1")
        dv.check_not_downgrade(entries, "0.3", "0.3.1", force=True)
        dv.check_not_downgrade(entries, "0.3", "0.3.2")

    def test_invalid_arguments_rejected(self):
        cases = [
            ("0.3", "0.3.0rc1"),
            ("٠.٣", None),
            ("0.3", "0.4.0"),
            ("main", "0.3.0"),
            ("root", "0.3.0"),
            ("v0.3", None),
            ("0.3.1", None),
        ]
        for target, release in cases:
            with self.subTest(target=target, release=release):
                with self.assertRaises(dv.DocsVersionError):
                    dv.validate_args(target, release)

    @staticmethod
    def make_site(root: Path, minor: str = "0.3", landing: bool = False):
        pages = ["index.html", "guide/index.html", "guide/page.html", "404.html"]
        if landing:
            pages.append(f"{dv.LANDING_PAGE}/index.html")
        for relative in pages:
            path = root / minor / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("<html></html>")

    def test_latest_redirects_are_relative_and_keep_hash_and_query(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_site(root)
            self.assertEqual(dv.write_latest_redirects(root, "0.3"), 3)
            index = (root / "latest/index.html").read_text()
            self.assertIn('"../0.3/getting-started/"', index)
            self.assertIn("location.search + location.hash", index)
            self.assertIn('"../../0.3/guide/"', (root / "latest/guide/index.html").read_text())
            self.assertIn('"../../0.3/guide/page.html"', (root / "latest/guide/page.html").read_text())

    def test_latest_redirects_replace_previous_alias(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_site(root, "0.3")
            dv.write_latest_redirects(root, "0.3")
            self.make_site(root, "0.4")
            (root / "0.4/guide/page.html").unlink()
            dv.write_latest_redirects(root, "0.4")
            self.assertFalse((root / "latest/guide/page.html").exists())
            self.assertIn('"../0.4/getting-started/"', (root / "latest/index.html").read_text())

    def test_finalize_writes_landing_manifest_and_latest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_site(root, landing=True)
            args = ["finalize", "--publish-dir", str(root), "--target", "0.3", "--release", "0.3.0"]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(dv.main(args), 0)
            self.assertIn('new URL("getting-started/", location.href)', (root / "0.3/index.html").read_text())
            self.assertEqual(dv.latest_version(dv.load_manifest(root)), "0.3")
            self.assertTrue((root / "latest/index.html").is_file())

    def test_finalize_refuses_a_version_without_the_landing_page(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_site(root)  # no getting-started/ page: the version root would redirect to a 404
            args = ["finalize", "--publish-dir", str(root), "--target", "0.3", "--release", "0.3.0"]
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                self.assertEqual(dv.main(args), 2)
            self.assertIn("getting-started", stderr.getvalue())
            self.assertFalse((root / dv.MANIFEST).exists())

    def test_load_manifest_rejects_malformed_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / dv.MANIFEST).write_text(json.dumps([{"version": "0.3"}, None]))

            with self.assertRaisesRegex(dv.DocsVersionError, "entry 1"):
                dv.load_manifest(root)


class DocumentationLinkPreparationTest(unittest.TestCase):
    """Keeps docs, configs and SHARED_PAGES in sync, and pins the link rewrite rules."""

    @staticmethod
    def project_config(name: str) -> dict:
        with (REPO_ROOT / name).open("rb") as config_file:
            return tomllib.load(config_file)["project"]

    @classmethod
    def excludes(cls, name: str) -> list[str]:
        patterns = cls.project_config(name)["exclude_docs"].splitlines()
        # fnmatch only agrees with gitignore-style matching for plain paths and simple
        # globs, so fail loudly on anything fancier instead of passing or failing wrongly.
        for pattern in patterns:
            if pattern.startswith(("/", "!", "#")) or pattern.endswith("/"):
                raise AssertionError(f"{name}: exclude_docs pattern {pattern!r} is not supported by these tests")
        return patterns

    @staticmethod
    def matches(page: str, patterns: list[str]) -> bool:
        return any(fnmatch.fnmatchcase(page, pattern) for pattern in patterns)

    @staticmethod
    def doc_pages() -> list[str]:
        docs_dir = REPO_ROOT / "docs"
        return sorted(path.relative_to(docs_dir).as_posix() for path in docs_dir.rglob("*.md"))

    def test_shared_pages_match_zensical_configs(self):
        # Shared pages are the ones the versioned build excludes and the unversioned build keeps.
        # Equality also proves every listed page exists and no root-only page is missing.
        versioned_excludes = self.excludes("zensical-versioned.toml")
        unversioned_excludes = self.excludes("zensical-unversioned.toml")
        root_only = {
            page
            for page in self.doc_pages()
            if self.matches(page, versioned_excludes) and not self.matches(page, unversioned_excludes)
        }
        self.assertEqual(root_only, set(phd.SHARED_PAGES))

    def test_site_path_prefix_matches_site_url(self):
        for name in ("zensical-unversioned.toml", "zensical-versioned.toml"):
            site_url = self.project_config(name).get("site_url")
            if not site_url:
                continue
            with self.subTest(config=name):
                self.assertEqual(urlparse(site_url).path.rstrip("/"), phd.SITE_PATH_PREFIX)

    def test_theme_preference_scope_is_shared_across_configs(self):
        configs = ("zensical.toml", "zensical-unversioned.toml", "zensical-versioned.toml")
        scopes = {self.project_config(name)["extra"]["scope"] for name in configs}
        self.assertEqual(scopes, {f"{phd.SITE_PATH_PREFIX}/"})

    def test_unsupported_link_syntax_does_not_cross_sections(self):
        # The rewriter only handles inline markdown links. Reference-style definitions and
        # raw HTML links would silently break if they point into the other section.
        reference = re.compile(r"^\s*\[[^\]]+\]:\s*<?(\S+?)>?(?:\s|$)", re.MULTILINE)
        html_link = re.compile(r"""\b(?:href|src)=["']([^"']+)["']""")
        docs_dir = REPO_ROOT / "docs"

        for page in self.doc_pages():
            text = (docs_dir / page).read_text()
            for target in reference.findall(text) + html_link.findall(text):
                link_path = target.partition("#")[0]
                if not link_path or link_path.startswith("/") or "://" in link_path:
                    continue
                resolved = posixpath.normpath(posixpath.join(posixpath.dirname(page), link_path))
                if not resolved.endswith(".md") or not (docs_dir / resolved).is_file():
                    continue
                with self.subTest(page=page, target=target):
                    self.assertEqual(
                        page in phd.SHARED_PAGES,
                        resolved in phd.SHARED_PAGES,
                        f"{page} links to {resolved} across sections with syntax the rewriter does not handle",
                    )

    def test_shared_links_are_rewritten_under_project_path(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_dir = Path(directory)
            (docs_dir / "reference").mkdir()
            index = docs_dir / "index.md"
            index.write_text("[Shared](reference/security.md#scope)")
            (docs_dir / "reference/security.md").write_text("Shared security page")

            self.assertTrue(phd.rewrite_file(index, docs_dir))
            self.assertEqual(
                index.read_text(),
                "[Shared](/superset-kubernetes-operator/reference/security/#scope)",
            )

    def test_links_inside_fenced_code_blocks_are_not_rewritten(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_dir = Path(directory)
            (docs_dir / "reference").mkdir()
            page = docs_dir / "index.md"
            page.write_text(
                "[Live](reference/security.md)\n\n"
                "```markdown\n[Example](reference/security.md)\n```\n"
            )
            (docs_dir / "reference/security.md").write_text("Shared security page")

            phd.rewrite_file(page, docs_dir)

            self.assertEqual(
                page.read_text(),
                "[Live](/superset-kubernetes-operator/reference/security/)\n\n"
                "```markdown\n[Example](reference/security.md)\n```\n",
            )

    def test_shared_links_to_versioned_pages_use_project_and_version_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_dir = Path(directory)
            (docs_dir / "reference").mkdir()
            (docs_dir / "user-guide").mkdir()
            index = docs_dir / "index.md"
            index.write_text(
                "[Guide home](user-guide/index.md) "
                "[Configuration](user-guide/configuration.md?tab=redis#cache)"
            )
            (docs_dir / "reference/security.md").write_text("Shared security page")
            (docs_dir / "user-guide/index.md").write_text("Guide home")
            (docs_dir / "user-guide/configuration.md").write_text("Versioned page")

            pages = phd.versioned_page_urls(docs_dir, "latest")
            self.assertTrue(phd.rewrite_file(index, docs_dir, versioned_pages=pages))
            self.assertEqual(
                index.read_text(),
                "[Guide home](/superset-kubernetes-operator/latest/user-guide/) "
                "[Configuration](/superset-kubernetes-operator/latest/user-guide/configuration/?tab=redis#cache)",
            )

    def test_versioned_page_keeps_links_to_other_versioned_pages_relative(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_dir = Path(directory)
            (docs_dir / "user-guide").mkdir()
            page = docs_dir / "user-guide/index.md"
            page.write_text("[Configuration](configuration.md)")
            (docs_dir / "user-guide/configuration.md").write_text("Configuration")

            pages = phd.versioned_page_urls(docs_dir, "latest")
            phd.rewrite_file(page, docs_dir, versioned_pages=pages)

            self.assertEqual(page.read_text(), "[Configuration](configuration.md)")


if __name__ == "__main__":
    unittest.main()
