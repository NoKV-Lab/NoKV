#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Mapping tests for checked-in API-absence qualification."""

import subprocess
import unittest
import tempfile
from pathlib import Path

import api_absence_qualification as qualification
import source_bound_producer as producer


class ApiAbsenceQualificationTests(unittest.TestCase):
    def test_scenario_inventory_matches_the_replacement_ledger(self) -> None:
        self.assertEqual(
            set(qualification.SCENARIOS),
            {
                "l02.inode-dentry-client-api-absence",
                "l03.retired-filesystem-type-names-stay-absent",
                "l06.filesystem-emulation-absence",
                "l07.fuse-posix-inode-dentry-absence",
                "l08.filesystem-cli-compatibility-absence",
            },
        )
        for scenario in (
            "l02.inode-dentry-client-api-absence",
            "l03.retired-filesystem-type-names-stay-absent",
            "l06.filesystem-emulation-absence",
            "l07.fuse-posix-inode-dentry-absence",
            "l08.filesystem-cli-compatibility-absence",
        ):
            self.assertTrue(qualification.SCENARIOS[scenario].assertions)

    def test_every_absence_predicate_matches_a_tracked_source(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        for scenario, specification in qualification.SCENARIOS.items():
            for assertion in specification.assertions:
                with self.subTest(scenario=scenario, assertion=assertion.assertion_id):
                    result = producer.execute_static_assertion(assertion, repo=repo)
                    self.assertTrue(result.passed, result.record)

    def test_workspace_graph_scans_every_tracked_cargo_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            (repo / "crates" / "evil").mkdir(parents=True)
            (repo / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/evil"]\n')
            (repo / "crates" / "evil" / "Cargo.toml").write_text(
                '[package]\nname = "evil-product-member"\nversion = "0.1.0"\n'
                '[dependencies]\nnokv-fuse = { path = "../nokv-fuse" }\n'
            )
            (repo / "Cargo.lock").write_text("version = 4\n")
            result = producer.execute_cargo_workspace_assertion(
                qualification.WORKSPACE_GRAPH,
                repo=repo,
                tracked_manifest_lister=lambda _repo: (
                    "Cargo.lock",
                    "Cargo.toml",
                    "crates/evil/Cargo.toml",
                ),
            )

        self.assertFalse(result.passed)
        self.assertEqual(result.record["tracked_manifest_count"], 2)
        self.assertTrue(result.record["forbidden_hits"])

    def test_default_git_inventory_rejects_fuse_smuggled_by_a_workspace_member(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            member = repo / "crates" / "evil"
            member.mkdir(parents=True)
            (repo / "Cargo.toml").write_text(
                '[workspace]\nresolver = "2"\nmembers = ["crates/evil"]\n'
            )
            (member / "Cargo.toml").write_text(
                '[package]\nname = "evil-product-member"\nversion = "0.1.0"\n'
                '[dependencies]\nfuse_alias = { package = "nokv-fuse", '
                'path = "../nokv-fuse" }\n'
            )
            (repo / "Cargo.lock").write_text("version = 4\n")
            subprocess.run(
                ["git", "init", "--quiet"],
                cwd=repo,
                check=True,
                capture_output=True,
                shell=False,
            )
            subprocess.run(
                [
                    "git",
                    "add",
                    "--",
                    "Cargo.lock",
                    "Cargo.toml",
                    "crates/evil/Cargo.toml",
                ],
                cwd=repo,
                check=True,
                capture_output=True,
                shell=False,
            )

            self.assertEqual(
                producer._default_tracked_manifest_lister(repo),
                ("Cargo.lock", "Cargo.toml", "crates/evil/Cargo.toml"),
            )
            result = producer.execute_cargo_workspace_assertion(
                qualification.WORKSPACE_GRAPH,
                repo=repo,
            )

        self.assertFalse(result.passed)
        self.assertEqual(result.record["tracked_manifest_count"], 2)
        self.assertGreater(result.record["forbidden_hit_count"], 0)
        self.assertTrue(
            any(
                hit.startswith("crates/evil/Cargo.toml:")
                for hit in result.record["forbidden_hits"]
            )
        )


if __name__ == "__main__":
    unittest.main()
