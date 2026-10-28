#!/usr/bin/env python3
"""Unit tests for documentation versioning and link preparation."""

import fnmatch
import importlib.util
import json
import tempfile
import tomllib
import unittest
from pathlib import Path

import docs_versions as dv

prepare_docs_spec = importlib.util.spec_from_file_location(
    "prepare_docs_links", Path(__file__).with_name("prepare-docs-links.py")
)
if prepare_docs_spec is None or prepare_docs_spec.loader is None:
    raise RuntimeError("could not load prepare-docs-links.py")
phd = importlib.util.module_from_spec(prepare_docs_spec)
prepare_docs_spec.loader.exec_module(phd)


class DocsVersionsTest(unittest.TestCase):
    def test_latest_uses_numeric_not_lexicographic_order(self):
        entries = []
        for minor in ("0.9", "0.10", "0.2"):
            entries = dv.build_manifest(entries, minor)
        self.assertEqual([entry["version"] for entry in entries], ["0.10", "0.9", "0.2", "main"])
        self.assertEqual(dv.latest_minor(entries), "0.10")

    def test_main_is_latest_before_first_final_release(self):
        entries = dv.build_manifest([], dv.MAIN)
        self.assertIsNone(dv.latest_minor(entries))
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
            ("0.3", "0.3.0-rc.1"),
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

    def test_valid_arguments_accepted(self):
        for target, release in [("root", None), ("main", None), ("0.3", None), ("0.3", "0.3.4")]:
            with self.subTest(target=target, release=release):
                dv.validate_args(target, release)

    @staticmethod
    def make_site(root: Path, minor: str = "0.3"):
        for relative in ("index.html", "guide/index.html", "guide/page.html", "404.html"):
            path = root / minor / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("<html></html>")

    def test_latest_redirects_are_relative_and_keep_hash_and_query(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_site(root)
            self.assertEqual(dv.write_latest_redirects(root, "0.3", "https://example.org/docs"), 3)
            index = (root / "latest/index.html").read_text()
            self.assertIn('"../0.3/getting-started/"', index)
            self.assertIn("location.search + location.hash", index)
            self.assertIn('href="https://example.org/docs/0.3/getting-started/"', index)
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

    def test_version_landing_does_not_duplicate_home(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_site(root)
            dv.write_version_landing(root, "0.3")
            self.assertIn('"getting-started/"', (root / "0.3/index.html").read_text())
            self.assertFalse((root / "0.3/index.md").exists())

    def test_load_manifest_reads_version_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = [{"version": "0.3", "title": "0.3"}]
            (root / dv.MANIFEST).write_text(json.dumps(expected))

            self.assertEqual(dv.load_manifest(root), expected)

    def test_load_manifest_returns_empty_when_manifest_is_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(dv.load_manifest(Path(directory)), [])

    def test_load_manifest_rejects_malformed_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / dv.MANIFEST).write_text(json.dumps([{"version": "0.3"}, None]))

            with self.assertRaisesRegex(dv.DocsVersionError, "entry 1"):
                dv.load_manifest(root)


class DocumentationLinkPreparationTest(unittest.TestCase):
    def test_shared_page_classification_matches_zensical_configs(self):
        repo_root = Path(__file__).resolve().parent.parent
        with (repo_root / "zensical-versioned.toml").open("rb") as config_file:
            versioned_excludes = tomllib.load(config_file)["project"]["exclude_docs"].splitlines()
        with (repo_root / "zensical-unversioned.toml").open("rb") as config_file:
            unversioned_excludes = tomllib.load(config_file)["project"]["exclude_docs"].splitlines()

        for page in phd.SHARED_PAGES:
            with self.subTest(page=page):
                self.assertTrue(
                    any(fnmatch.fnmatchcase(page, pattern) for pattern in versioned_excludes),
                    f"{page} must be excluded from the versioned build",
                )
                self.assertFalse(
                    any(fnmatch.fnmatchcase(page, pattern) for pattern in unversioned_excludes),
                    f"{page} must be included in the unversioned build",
                )

    def test_rewrite_file_maps_shared_links_to_site_root(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_dir = Path(directory)
            (docs_dir / "reference").mkdir()
            index = docs_dir / "index.md"
            index.write_text("[Shared](reference/security.md#scope)")
            (docs_dir / "reference/security.md").write_text("Shared security page")

            self.assertTrue(phd.rewrite_file(index, docs_dir))
            self.assertEqual(index.read_text(), "[Shared](/reference/security/#scope)")

    def test_rewrite_file_maps_shared_links_to_versioned_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_dir = Path(directory)
            (docs_dir / "reference").mkdir()
            (docs_dir / "user-guide").mkdir()
            (docs_dir / "index.md").write_text("[Versioned](user-guide/configuration.md#cache)")
            (docs_dir / "reference/security.md").write_text("Shared security page")
            (docs_dir / "user-guide/configuration.md").write_text("Versioned page")

            index = docs_dir / "index.md"
            pages = {"user-guide/configuration.md": "/latest/user-guide/configuration/"}
            self.assertTrue(phd.rewrite_file(index, docs_dir, versioned_pages=pages))
            self.assertEqual(index.read_text(), "[Versioned](/latest/user-guide/configuration/#cache)")

    def test_versioned_index_page_maps_to_directory_url(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_dir = Path(directory)
            page = docs_dir / "user-guide/index.md"
            page.parent.mkdir()
            page.write_text("[Home](../getting-started.md)")
            (docs_dir / "getting-started.md").write_text("Getting started")

            versioned_pages = phd.versioned_page_urls(docs_dir, "latest")
            phd.rewrite_file(page, docs_dir, versioned_pages=versioned_pages)

            self.assertEqual(page.read_text(), "[Home](../getting-started.md)")
            self.assertEqual(versioned_pages["user-guide/index.md"], "/latest/user-guide/")


if __name__ == "__main__":
    unittest.main()
