#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the source-bound typed qualification producer runtime."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import pwd
import subprocess
import tempfile
import unittest
from pathlib import Path

import source_bound_producer as producer


def canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def environment(
    *,
    producer_id: str,
    evidence_kind: str,
    claims: list[tuple[str, str, str]],
) -> dict[str, str]:
    subjects = {"dependencies": []}
    return {
        "NOKV_QUALIFICATION_OPERATION_ID": "a" * 32,
        "NOKV_QUALIFICATION_PRODUCER": producer_id,
        "NOKV_QUALIFICATION_EVIDENCE_KIND": evidence_kind,
        "NOKV_QUALIFICATION_SOURCE_SHA": "b" * 40,
        "NOKV_QUALIFICATION_COMMAND_ARGV_SHA256": "c" * 64,
        "NOKV_QUALIFICATION_SUBJECTS": json.dumps(subjects),
        "NOKV_QUALIFICATION_SUBJECTS_SHA256": canonical_sha256(subjects),
        "NOKV_QUALIFICATION_CLAIMS": json.dumps(
            [
                {"stable_id": stable_id, "gate": gate, "scenario": scenario}
                for stable_id, gate, scenario in claims
            ]
        ),
        "NOKV_QUALIFICATION_REQUIRED_EVIDENCE_ROLES": json.dumps(["producer-result"]),
    }


class SourceBoundProducerTests(unittest.TestCase):
    def test_executable_path_preserves_cargo_launcher_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rustup = root / "rustup"
            rustup.write_text("launcher\n")
            cargo = root / "cargo"
            cargo.symlink_to(rustup)

            self.assertEqual(producer._executable_path(str(cargo)), cargo)
            self.assertNotEqual(producer._executable_path(str(cargo)), rustup)

    def test_rust_toolchain_subject_is_rederived_and_child_env_is_sanitized(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rustup = root / "rustup"
            rustup.write_bytes(b"real launcher bytes\n")
            rustup.chmod(0o755)
            cargo = root / "cargo"
            rustc = root / "rustc"
            cargo.symlink_to(rustup.name)
            rustc.symlink_to(rustup.name)
            executable_sha256 = hashlib.sha256(rustup.read_bytes()).hexdigest()
            outputs = {
                str(cargo): ("cargo 1.96.0\nrelease: 1.96.0\n", ""),
                str(rustc): ("rustc 1.96.0\nrelease: 1.96.0\n", "warning\n"),
            }

            def tool(name: str) -> dict[str, str]:
                stdout, stderr = outputs[str(root / name)]
                return {
                    "launcher_path": str(root / name),
                    "launcher_kind": "symlink",
                    "resolved_path": str(rustup.resolve()),
                    "resolved_sha256": executable_sha256,
                    "version_verbose_sha256": producer._command_output_sha256(
                        stdout, stderr
                    ),
                }

            subjects: dict[str, object] = {
                "dependencies": [],
                "rust_toolchain": {"cargo": tool("cargo"), "rustc": tool("rustc")},
            }
            calls: list[tuple[list[str], dict[str, object]]] = []

            def version_runner(
                argv: list[str], **kwargs: object
            ) -> subprocess.CompletedProcess[str]:
                calls.append((argv, kwargs))
                stdout, stderr = outputs[argv[0]]
                return subprocess.CompletedProcess(argv, 0, stdout, stderr)

            identity = producer.validate_rust_toolchain(
                subjects,
                repo=root,
                environ={
                    "CARGO": "/attacker/cargo",
                    "RUSTC": "/attacker/rustc",
                    "RUSTC_WRAPPER": "/attacker/wrapper",
                    "RUSTC_WORKSPACE_WRAPPER": "/attacker/workspace-wrapper",
                    "RUSTFLAGS": "--cfg attacker",
                    "CARGO_HOME": "/attacker/cargo-home",
                    "CARGO_BUILD_RUSTC": "/attacker/build-rustc",
                    "CARGO_BUILD_RUSTC_WRAPPER": "/attacker/build-wrapper",
                    "CARGO_ENCODED_RUSTFLAGS": "--cfg\x1fattacker",
                    "RUSTUP_HOME": "/attacker/rustup-home",
                    "RUSTUP_TOOLCHAIN": "attacker",
                    "RUSTUP_DIST_SERVER": "https://attacker.invalid",
                    "RUSTUP_UPDATE_ROOT": "https://attacker.invalid",
                    "RUSTC_BOOTSTRAP": "1",
                    "RUSTDOCFLAGS": "--cfg attacker",
                    "CARGO_ALIAS_TEST": "attacker",
                    "CARGO_BUILD_TARGET": "attacker-target",
                    "CARGO_TARGET_DIR": "/attacker/target",
                    "RUST_BACKTRACE": "full",
                    "RUST_UNKNOWN_ROUTE": "/attacker/rust-route",
                    "HOME": "/attacker/home",
                    "KEEP": "value",
                },
                command_runner=version_runner,
            )

            self.assertEqual(identity.cargo, cargo)
            self.assertEqual(identity.rustc, rustc)
            self.assertEqual(len(identity.evidence), 2)
            self.assertEqual(identity.child_environment["CARGO"], str(cargo))
            self.assertEqual(identity.child_environment["RUSTC"], str(rustc))
            self.assertEqual(identity.child_environment["KEEP"], "value")
            self.assertEqual(
                identity.child_environment["HOME"],
                pwd.getpwuid(os.getuid()).pw_dir,
            )
            for denied in (
                "RUSTC_WRAPPER",
                "RUSTC_WORKSPACE_WRAPPER",
                "RUSTFLAGS",
                "CARGO_HOME",
                "CARGO_BUILD_RUSTC",
                "CARGO_BUILD_RUSTC_WRAPPER",
                "CARGO_ENCODED_RUSTFLAGS",
                "RUSTUP_HOME",
                "RUSTUP_TOOLCHAIN",
                "RUSTUP_DIST_SERVER",
                "RUSTUP_UPDATE_ROOT",
                "RUSTC_BOOTSTRAP",
                "RUSTDOCFLAGS",
                "CARGO_ALIAS_TEST",
                "CARGO_BUILD_TARGET",
                "CARGO_TARGET_DIR",
                "RUST_BACKTRACE",
                "RUST_UNKNOWN_ROUTE",
            ):
                self.assertNotIn(denied, identity.child_environment)
            self.assertEqual(
                [call[0] for call in calls],
                [[str(cargo), "-Vv"], [str(rustc), "-Vv"]],
            )
            self.assertTrue(all(call[1]["shell"] is False for call in calls))
            self.assertTrue(
                all(call[1]["env"] == identity.child_environment for call in calls)
            )

            rustup.write_bytes(b"changed launcher bytes\n")
            with self.assertRaisesRegex(producer.ProducerError, "hash does not match"):
                producer.validate_rust_toolchain(
                    subjects,
                    repo=root,
                    environ={},
                    command_runner=version_runner,
                )

    def test_context_rejects_missing_duplicate_and_foreign_claims(self) -> None:
        expected = {"t01.example": producer.ScenarioContract("T01", "facade-contract")}
        base = environment(
            producer_id="test-producer",
            evidence_kind="unit",
            claims=[("T01", "facade-contract", "t01.example")],
        )
        parsed = producer.load_context(
            base,
            producer_id="test-producer",
            evidence_kind="unit",
            scenarios=expected,
        )
        self.assertEqual(parsed.scenarios, ("t01.example",))

        for claims in (
            [],
            [
                ("T01", "facade-contract", "t01.example"),
                ("T01", "facade-contract", "t01.example"),
            ],
            [("T02", "facade-contract", "t01.example")],
            [("T01", "other-gate", "t01.example")],
            [("T01", "facade-contract", "unknown")],
        ):
            changed = dict(base)
            changed["NOKV_QUALIFICATION_CLAIMS"] = json.dumps(
                [
                    {
                        "stable_id": stable_id,
                        "gate": gate,
                        "scenario": scenario,
                    }
                    for stable_id, gate, scenario in claims
                ]
            )
            with self.subTest(claims=claims), self.assertRaises(producer.ProducerError):
                producer.load_context(
                    changed,
                    producer_id="test-producer",
                    evidence_kind="unit",
                    scenarios=expected,
                )

    def test_all_owned_scenario_maps_exactly_match_the_ledger(self) -> None:
        ledger = json.loads(
            Path(__file__).with_name("pre423_contract_ledger.json").read_text()
        )
        modules = {
            "api-absence": "api_absence_qualification",
            "api-decision": "api_decision_qualification",
            "commit-replay": "commit_replay_qualification",
            "cursor-differential": "cursor_differential_qualification",
            "nokv-agent-unit": "nokv_agent_qualification",
        }
        for producer_id, module_name in modules.items():
            expected_scenarios: set[str] = set()
            for item in ledger["items"]:
                for expectation in item["gate_expectations"].values():
                    profile = ledger["expectation_profiles"][expectation["profile"]]
                    if producer_id in profile["allowed_producers"]:
                        expected_scenarios.update(expectation["scenarios"])
            actual_scenarios = set(importlib.import_module(module_name).SCENARIOS)
            with self.subTest(producer=producer_id):
                self.assertEqual(actual_scenarios, expected_scenarios)

    def test_exact_cargo_test_requires_one_matched_test(self) -> None:
        assertion = producer.RustTestAssertion(
            assertion_id="focused-test",
            package="nokv-agent",
            target_args=("--test", "sdk_facade"),
            test_name="focused_test",
        )
        calls: list[tuple[list[str], dict[str, object]]] = []

        def passing(
            argv: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            calls.append((argv, kwargs))
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=(
                    "running 1 test\n"
                    "test focused_test ... ok\n\n"
                    "test result: ok. 1 passed; 0 failed; 0 ignored; "
                    "0 measured; 13 filtered out\n"
                ),
                stderr="",
            )

        with tempfile.TemporaryDirectory() as directory:
            result = producer.execute_rust_test(
                assertion,
                repo=Path(directory),
                cargo=Path("/usr/bin/cargo"),
                target_dir=Path(directory) / "target",
                timeout_seconds=10,
                command_runner=passing,
            )
        self.assertTrue(result.passed)
        self.assertEqual(result.matched_test_count, 1)
        argv, kwargs = calls[0]
        self.assertEqual(
            argv,
            [
                "/usr/bin/cargo",
                "test",
                "--locked",
                "--color=never",
                "-p",
                "nokv-agent",
                "--target-dir",
                str(Path(directory) / "target"),
                "--test",
                "sdk_facade",
                "focused_test",
                "--",
                "--exact",
            ],
        )
        self.assertIs(kwargs["shell"], False)
        self.assertIs(kwargs["check"], False)
        self.assertEqual(kwargs["timeout"], 10)

        def zero_tests(
            argv: list[str], **_: object
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=(
                    "running 0 tests\n"
                    "test result: ok. 0 passed; 0 failed; 0 ignored; "
                    "0 measured; 14 filtered out\n"
                ),
                stderr="",
            )

        with tempfile.TemporaryDirectory() as directory:
            result = producer.execute_rust_test(
                assertion,
                repo=Path(directory),
                cargo=Path("/usr/bin/cargo"),
                target_dir=Path(directory) / "target",
                timeout_seconds=10,
                command_runner=zero_tests,
            )
        self.assertFalse(result.passed)
        self.assertEqual(result.matched_test_count, 0)

    def test_rust_main_ignores_an_environment_selected_fake_cargo(self) -> None:
        """Only the runner-bound launcher may execute, regardless of CARGO."""

        scenario = "t01.fake-cargo"
        specification = producer.RustScenario(
            contract=producer.ScenarioContract("T01", "facade-contract"),
            assertions=(
                producer.RustTestAssertion(
                    assertion_id="focused-test",
                    package="nokv-agent",
                    target_args=("--lib",),
                    test_name="tests::focused_test",
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake_cargo = root / "attacker-cargo"
            fake_cargo.write_text(
                "#!/usr/bin/env python3\n"
                "import sys\n"
                "name = sys.argv[sys.argv.index('--') - 1]\n"
                "print(f'test {name} ... ok')\n"
                "print('test result: ok. 1 passed; 0 failed; 0 ignored; "
                "0 measured; 0 filtered out')\n"
            )
            fake_cargo.chmod(0o755)
            trusted_cargo = root / "trusted-cargo"
            trusted_rustc = root / "trusted-rustc"
            trusted_cargo.write_bytes(b"trusted cargo launcher\n")
            trusted_rustc.write_bytes(b"trusted rustc launcher\n")
            trusted_cargo.chmod(0o755)
            trusted_rustc.chmod(0o755)
            version_output = {
                str(trusted_cargo): "cargo 1.96.0\nrelease: 1.96.0\n",
                str(trusted_rustc): "rustc 1.96.0\nrelease: 1.96.0\n",
            }

            def tool(path: Path) -> dict[str, str]:
                return {
                    "launcher_path": str(path),
                    "launcher_kind": "regular",
                    "resolved_path": str(path.resolve()),
                    "resolved_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "version_verbose_sha256": producer._command_output_sha256(
                        version_output[str(path)], ""
                    ),
                }

            subjects: dict[str, object] = {
                "dependencies": [],
                "rust_toolchain": {
                    "cargo": tool(trusted_cargo),
                    "rustc": tool(trusted_rustc),
                },
            }
            result_path = root / "producer-result.json"
            rust_environment = environment(
                producer_id="test-producer",
                evidence_kind="unit",
                claims=[("T01", "facade-contract", scenario)],
            )
            rust_environment.update(
                {
                    "CARGO": str(fake_cargo),
                    "CARGO_ALIAS_TEST": "attacker",
                    "CARGO_BUILD_TARGET": "attacker-target",
                    "CARGO_TARGET_DIR": str(root / "attacker-target"),
                    "RUST_BACKTRACE": "full",
                    "RUST_UNKNOWN_ROUTE": "/attacker/rust-route",
                    "HOME": "/attacker/home",
                    "PATH": os.environ.get("PATH", ""),
                }
            )
            rust_environment["NOKV_QUALIFICATION_SUBJECTS"] = json.dumps(subjects)
            rust_environment["NOKV_QUALIFICATION_SUBJECTS_SHA256"] = canonical_sha256(
                subjects
            )
            calls: list[tuple[list[str], dict[str, object]]] = []

            def trusted_runner(
                argv: list[str], **kwargs: object
            ) -> subprocess.CompletedProcess[str]:
                calls.append((argv, kwargs))
                if argv[0] == str(fake_cargo):
                    self.fail("environment-selected fake Cargo was executed")
                if argv[1:] == ["-Vv"]:
                    return subprocess.CompletedProcess(
                        argv, 0, version_output[argv[0]], ""
                    )
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=(
                        "running 1 test\n"
                        "test tests::focused_test ... ok\n"
                        "test result: ok. 1 passed; 0 failed; 0 ignored; "
                        "0 measured; 0 filtered out\n"
                    ),
                    stderr="",
                )

            exit_code = producer.rust_main(
                producer_id="test-producer",
                evidence_kinds=("unit",),
                scenarios={scenario: specification},
                description="fake Cargo regression",
                argv=[
                    "--qualification-result",
                    str(result_path),
                ],
                environ=rust_environment,
                command_runner=trusted_runner,
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(result_path.exists())
            self.assertEqual(
                [call[0][0] for call in calls],
                [str(trusted_cargo), str(trusted_rustc), str(trusted_cargo)],
            )
            cargo_test_argv, cargo_test_kwargs = calls[-1]
            target_dir = cargo_test_argv[cargo_test_argv.index("--target-dir") + 1]
            self.assertNotEqual(target_dir, str(root / "attacker-target"))
            child_environment = cargo_test_kwargs["env"]
            self.assertIsInstance(child_environment, dict)
            assert isinstance(child_environment, dict)
            self.assertEqual(
                child_environment["HOME"], pwd.getpwuid(os.getuid()).pw_dir
            )
            self.assertEqual(child_environment["CARGO"], str(trusted_cargo))
            self.assertEqual(child_environment["RUSTC"], str(trusted_rustc))
            self.assertFalse(
                any(
                    key.startswith("CARGO_")
                    or (key != "RUSTC" and key.startswith("RUST"))
                    for key in child_environment
                )
            )
            result = json.loads(result_path.read_text())
            self.assertEqual(result["subjects"], subjects)

    def test_timeout_and_output_parse_errors_fail_closed(self) -> None:
        assertion = producer.RustTestAssertion(
            assertion_id="focused-test",
            package="nokv-agent",
            target_args=("--lib",),
            test_name="tests::focused_test",
        )

        def timeout(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            raise subprocess.TimeoutExpired(argv, 1, output="partial", stderr="slow")

        def two_tests(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=(
                    "test tests::focused_test ... ok\n"
                    "test other::focused_test ... ok\n"
                    "test result: ok. 2 passed; 0 failed; 0 ignored; "
                    "0 measured; 0 filtered out\n"
                ),
                stderr="",
            )

        def undecodable_output(
            _argv: list[str], **_: object
        ) -> subprocess.CompletedProcess[str]:
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid byte")

        with tempfile.TemporaryDirectory() as directory:
            common = {
                "repo": Path(directory),
                "cargo": Path("/usr/bin/cargo"),
                "target_dir": Path(directory) / "target",
                "timeout_seconds": 1,
            }
            timed_out = producer.execute_rust_test(
                assertion, command_runner=timeout, **common
            )
            overmatched = producer.execute_rust_test(
                assertion, command_runner=two_tests, **common
            )
            undecodable = producer.execute_rust_test(
                assertion, command_runner=undecodable_output, **common
            )
        self.assertFalse(timed_out.passed)
        self.assertTrue(timed_out.timed_out)
        self.assertFalse(overmatched.passed)
        self.assertEqual(overmatched.matched_test_count, 2)
        self.assertFalse(undecodable.passed)
        self.assertIn("decode failed", undecodable.record["stderr_summary"])

    def test_result_is_closed_context_bound_and_create_new(self) -> None:
        context = producer.load_context(
            environment(
                producer_id="test-producer",
                evidence_kind="unit",
                claims=[("T01", "facade-contract", "t01.example")],
            ),
            producer_id="test-producer",
            evidence_kind="unit",
            scenarios={
                "t01.example": producer.ScenarioContract("T01", "facade-contract")
            },
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.json"
            producer.write_producer_result(path, context, "PASS")
            value = json.loads(path.read_text())
            self.assertEqual(
                set(value),
                {
                    "schema",
                    "producer",
                    "evidence_kind",
                    "operation_id",
                    "source_sha",
                    "command_argv_sha256",
                    "subjects",
                    "subjects_sha256",
                    "scenarios",
                },
            )
            self.assertEqual(
                value["scenarios"],
                {
                    "t01.example": {
                        "outcome": "PASS",
                        "evidence_roles": ["producer-result"],
                    }
                },
            )
            with self.assertRaises(producer.ProducerError):
                producer.write_producer_result(path, context, "PASS")

    def test_source_assertions_are_tracked_and_fail_on_forbidden_text(self) -> None:
        assertion = producer.SourceTextAssertion(
            assertion_id="source-bound",
            path="src/lib.rs",
            required=("pub struct WorkspaceClient",),
            forbidden=("Inode", "Dentry"),
        )
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            source = repo / "src" / "lib.rs"
            source.parent.mkdir()
            source.write_text("pub struct WorkspaceClient;\n")
            checked: list[str] = []

            def tracked(_: Path, path: str) -> None:
                checked.append(path)

            passed = producer.execute_source_assertion(
                assertion, repo=repo, tracked_checker=tracked
            )
            self.assertTrue(passed.passed)
            self.assertEqual(checked, ["src/lib.rs"])
            self.assertRegex(
                str(passed.record["source_predicate_sha256"]), r"^[0-9a-f]{64}$"
            )

            source.write_text("pub struct WorkspaceClient; // Inode\n")
            failed = producer.execute_source_assertion(
                assertion, repo=repo, tracked_checker=tracked
            )
            self.assertFalse(failed.passed)

    def test_source_assertions_reject_tracked_symlinks(self) -> None:
        assertion = producer.SourceTextAssertion(
            assertion_id="source-bound",
            path="src/lib.rs",
            required=("pub struct WorkspaceClient",),
        )
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            source = repo / "src" / "real.rs"
            source.parent.mkdir()
            source.write_text("pub struct WorkspaceClient;\n")
            (repo / "src" / "lib.rs").symlink_to(source)

            with self.assertRaisesRegex(producer.ProducerError, "regular file"):
                producer.execute_source_assertion(
                    assertion,
                    repo=repo,
                    tracked_checker=lambda _repo, _path: None,
                )


if __name__ == "__main__":
    unittest.main()
