#!/usr/bin/env python3
"""Unit tests for documentation versioning and link preparation."""

import json
import importlib.util
import os
import subprocess
import tempfile
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

    def test_finalize_end_to_end(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_site(root, "0.3")
            self.assertEqual(
                dv.main(["finalize", "--publish-dir", str(root), "--target", "0.3", "--release", "0.3.0"]),
                0,
            )
            manifest = json.loads((root / "versions.json").read_text())
            self.assertEqual(
                manifest[0],
                {"version": "0.3", "title": "0.3", "aliases": ["latest"], "release": "0.3.0"},
            )
            self.assertTrue((root / "0.3/index.html").exists())

    def test_main_finalize_creates_latest_redirects_to_main(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_site(root, "main")
            stale_latest = root / "latest/index.html"
            stale_latest.parent.mkdir(parents=True)
            stale_latest.write_text("stale redirect")
            self.assertEqual(
                dv.main(
                    [
                        "finalize",
                        "--publish-dir",
                        str(root),
                        "--target",
                        "main",
                    ]
                ),
                0,
            )
            manifest = json.loads((root / "versions.json").read_text())
            self.assertEqual(
                manifest,
                [{"version": "main", "title": dv.MAIN_TITLE, "aliases": ["latest"]}],
            )
            self.assertIsNone(dv.latest_minor(manifest))
            self.assertEqual(dv.latest_version(manifest), "main")
            self.assertTrue((root / "latest/index.html").exists())
            self.assertIn('"../main/getting-started/"', (root / "latest/index.html").read_text())
            self.assertTrue((root / "latest/guide/page.html").exists())
            self.assertTrue((root / "main/index.html").exists())

    def test_release_replaces_main_latest_redirects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_site(root, "main")
            dv.main(["finalize", "--publish-dir", str(root), "--target", "main"])
            self.make_site(root, "0.3")

            dv.main(
                ["finalize", "--publish-dir", str(root), "--target", "0.3", "--release", "0.3.0"]
            )

            entries = json.loads((root / "versions.json").read_text())
            self.assertEqual(dv.latest_version(entries), "0.3")
            self.assertIn('"../0.3/getting-started/"', (root / "latest/index.html").read_text())

    def test_cli_reports_errors_with_exit_code_2(self):
        with tempfile.TemporaryDirectory() as directory:
            code = dv.main(
                ["check", "--publish-dir", directory, "--target", "0.3", "--release", "0.3.0rc1"]
            )
            self.assertEqual(code, 2)

    def test_backfill_updates_only_target_version_in_existing_site(self):
        root_dir = Path(__file__).resolve().parent.parent
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            publish_dir = root / "gh-pages"
            site_dir = root / "site"
            fake_zensical = root / "zensical"
            build_source = root / "build-source"

            for relative, content in {
                "index.html": "existing root",
                "0.1/getting-started/index.html": "old 0.1",
                "0.1/obsolete.html": "remove this",
                "0.2/getting-started/index.html": "existing 0.2",
                "main/getting-started/index.html": "existing main",
            }.items():
                path = publish_dir / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)

            entries = dv.build_manifest([], "0.1", "0.1.0")
            entries = dv.build_manifest(entries, "0.2", "0.2.0")
            dv.write_manifest(publish_dir, entries)

            page = build_source / "getting-started/index.html"
            page.parent.mkdir(parents=True, exist_ok=True)
            page.write_text("updated 0.1")
            (build_source / "new-page.html").write_text("new page")
            fake_zensical.write_text(
                "#!/usr/bin/env python3\n"
                "import os\n"
                "import shutil\n"
                "shutil.copytree(os.environ['DOCS_TEST_SITE'], os.environ['SITE_DIR'], dirs_exist_ok=True)\n"
            )
            fake_zensical.chmod(0o755)

            environment = os.environ.copy()
            environment.update(
                {
                    "DOCS_TEST_SITE": str(build_source),
                    "SITE_DIR": str(site_dir),
                    "ZENSICAL": str(fake_zensical),
                }
            )
            subprocess.run(
                [
                    "bash",
                    str(root_dir / "scripts/publish-docs.sh"),
                    "--config",
                    str(root_dir / "zensical-versioned.toml"),
                    "--target",
                    "0.1",
                    "--publish-dir",
                    str(publish_dir),
                    "--release",
                    "0.1.1",
                ],
                check=True,
                cwd=root_dir,
                env=environment,
                capture_output=True,
                text=True,
            )

            self.assertEqual((publish_dir / "index.html").read_text(), "existing root")
            self.assertEqual((publish_dir / "0.2/getting-started/index.html").read_text(), "existing 0.2")
            self.assertEqual((publish_dir / "main/getting-started/index.html").read_text(), "existing main")
            self.assertEqual((publish_dir / "0.1/getting-started/index.html").read_text(), "updated 0.1")
            self.assertTrue((publish_dir / "0.1/new-page.html").exists())
            self.assertFalse((publish_dir / "0.1/obsolete.html").exists())

            updated_entries = json.loads((publish_dir / "versions.json").read_text())
            self.assertEqual(dv.latest_minor(updated_entries), "0.2")
            self.assertEqual(updated_entries[1]["release"], "0.1.1")
            self.assertIn(
                '"../../0.2/getting-started/"',
                (publish_dir / "latest/getting-started/index.html").read_text(),
            )


class DocumentationLinkPreparationTest(unittest.TestCase):
    def test_links_use_the_configured_site_url(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_dir = Path(directory)
            (docs_dir / "reference").mkdir()
            (docs_dir / "user-guide").mkdir()
            index = docs_dir / "index.md"
            index.write_text(
                "[Shared](reference/security.md#scope) "
                "[Versioned](user-guide/configuration.md#cache)"
            )
            (docs_dir / "reference/security.md").write_text("Shared security page")
            (docs_dir / "user-guide/configuration.md").write_text("Versioned page")

            versioned_pages = phd.versioned_page_urls(docs_dir, "latest")
            phd.rewrite_file(
                index,
                docs_dir,
                "https://apache.github.io/superset-kubernetes-operator",
                versioned_pages,
            )

            self.assertEqual(
                index.read_text(),
                "[Shared](https://apache.github.io/superset-kubernetes-operator/reference/security/#scope) "
                "[Versioned](https://apache.github.io/superset-kubernetes-operator/latest/user-guide/configuration/#cache)",
            )

    def test_versioned_links_are_rewritten_only_when_prefix_is_set(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_dir = Path(directory)
            (docs_dir / "reference").mkdir()
            (docs_dir / "user-guide").mkdir()
            (docs_dir / "index.md").write_text(
                "[Shared](reference/security.md#scope) "
                "[Versioned](user-guide/configuration.md#cache)"
            )
            (docs_dir / "reference/security.md").write_text("Shared security page")
            (docs_dir / "user-guide/configuration.md").write_text("Versioned page")

            index = docs_dir / "index.md"
            pages = phd.versioned_page_urls(docs_dir, "latest")
            phd.rewrite_file(index, docs_dir)
            self.assertEqual(
                index.read_text(),
                "[Shared](/reference/security/#scope) "
                "[Versioned](user-guide/configuration.md#cache)",
            )

            index.write_text(
                "[Shared](reference/security.md#scope) "
                "[Versioned](user-guide/configuration.md#cache)"
            )
            phd.rewrite_file(index, docs_dir, versioned_pages=pages)
            self.assertEqual(
                index.read_text(),
                "[Shared](/reference/security/#scope) "
                "[Versioned](/latest/user-guide/configuration/#cache)",
            )

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
