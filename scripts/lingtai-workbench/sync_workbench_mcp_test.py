#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import contextlib
import copy
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import nokv_runtime as runtime  # noqa: E402
import sync_workbench_mcp as sync  # noqa: E402
import workbench_contract as contract  # noqa: E402


HOLT_REVISION = "b" * 40


def tool_surface(*, include_restore: bool = True) -> list[dict]:
    tools = [
        {
            "name": name,
            "description": f"description for {name}",
            "inputSchema": copy.deepcopy(schema),
        }
        for name, schema in contract.FROZEN_INPUT_SCHEMAS.items()
        if include_restore or name != contract.RESTORE_TOOL
    ]
    return sorted(tools, key=lambda tool: tool["name"])


class SyncWorkbenchMcpTest(unittest.TestCase):
    def make_source(self, root: Path, *, lock_revision: str = HOLT_REVISION) -> Path:
        source = root / "source"
        (source / "crates" / "nokv").mkdir(parents=True)
        (source / "Cargo.toml").write_text(
            "[workspace]\n"
            'members = ["crates/nokv"]\n'
            "[workspace.dependencies]\n"
            f'holt = {{ git = "https://github.com/NoKV-Lab/holt.git", rev = "{HOLT_REVISION}" }}\n',
            encoding="utf-8",
        )
        (source / "Cargo.lock").write_text(
            "version = 4\n\n"
            "[[package]]\n"
            'name = "holt"\n'
            'version = "0.8.1"\n'
            'source = "git+https://github.com/NoKV-Lab/holt.git'
            f'?rev={lock_revision}#{lock_revision}"\n',
            encoding="utf-8",
        )
        (source / "crates" / "nokv" / "Cargo.toml").write_text(
            '[package]\nname = "nokv"\nversion = "0.1.0"\n',
            encoding="utf-8",
        )
        (source / ".gitignore").write_text("/target/\n", encoding="utf-8")
        subprocess.run(["git", "init", "-q"], cwd=source, check=True)
        subprocess.run(["git", "add", "."], cwd=source, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.name=NoKV Test",
                "-c",
                "user.email=nokv-test@example.invalid",
                "commit",
                "-q",
                "-m",
                "test fixture",
            ],
            cwd=source,
            check=True,
        )
        return source

    def source_revision(self, source: Path) -> str:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=source, text=True
        ).strip()

    def make_project(self, root: Path) -> tuple[Path, Path]:
        project = root / "project"
        agent = project / ".lingtai" / "coordinator"
        agent.mkdir(parents=True)
        (agent / "init.json").write_text('{"mcp": {}}\n', encoding="utf-8")
        return project, agent

    def make_binary(self, root: Path, tools: list[dict], name: str = "nokv") -> Path:
        binary = root / name
        binary.parent.mkdir(parents=True, exist_ok=True)
        response = {"jsonrpc": "2.0", "id": 1, "result": {"tools": tools}}
        binary.write_text(
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import sys\n"
            "for line in sys.stdin:\n"
            "    json.loads(line)\n"
            f"    print(json.dumps({response!r}, separators=(',', ':')))\n",
            encoding="utf-8",
        )
        os.chmod(binary, 0o755)
        return binary

    def run_sync(self, *args: str) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = sync.main(list(args))
        return code, stdout.getvalue(), stderr.getvalue()

    def sync_args(self, project: Path, source: Path, binary: Path) -> tuple[str, ...]:
        build_info = binary.with_name(f"{binary.name}.build-info.json")
        runtime.write_build_info(
            build_info,
            runtime.source_identity(source, self.source_revision(source)),
            binary,
        )
        return (
            "--project",
            str(project),
            "--agent",
            "coordinator",
            "--nokv-bin",
            str(binary),
            "--build-info",
            str(build_info),
            "--revision",
            self.source_revision(source),
            "--timeout-seconds",
            "5",
        )

    def test_sync_stages_gates_locks_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            project, agent = self.make_project(root)
            binary = self.make_binary(root, tool_surface())

            first = self.run_sync(*self.sync_args(project, source, binary))
            self.assertEqual(first[0], 0, first[2])
            lock_path = agent / sync.LOCK_NAME
            lock_before = lock_path.read_bytes()
            init_before = (agent / "init.json").read_bytes()
            registry_before = (agent / "mcp_registry.jsonl").read_bytes()

            lock = json.loads(lock_before)
            command = Path(lock["artifact"]["command"])
            self.assertTrue(command.is_file())
            self.assertIn(self.source_revision(source), command.parts)
            self.assertEqual(runtime.sha256_file(command), lock["artifact"]["sha256"])
            self.assertEqual(lock["source"]["holt_git_commit"], HOLT_REVISION)
            self.assertEqual(lock["contract"]["tool_count"], 17)
            self.assertEqual(lock["launch"]["workbench_root"], "/agents/coordinator/wb")

            second = self.run_sync(*self.sync_args(project, source, binary))
            self.assertEqual(second[0], 0, second[2])
            self.assertEqual(lock_path.read_bytes(), lock_before)
            self.assertEqual((agent / "init.json").read_bytes(), init_before)
            self.assertEqual(
                (agent / "mcp_registry.jsonl").read_bytes(), registry_before
            )
            self.assertIn("already synchronized", second[1])

    def test_missing_restore_fails_before_agent_configuration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            project, agent = self.make_project(root)
            binary = self.make_binary(root, tool_surface(include_restore=False))
            init_before = (agent / "init.json").read_bytes()

            result = self.run_sync(*self.sync_args(project, source, binary))

            self.assertEqual(result[0], 1)
            self.assertIn("missing=['workbench_restore']", result[2])
            self.assertEqual((agent / "init.json").read_bytes(), init_before)
            self.assertFalse((agent / "mcp_registry.jsonl").exists())
            self.assertFalse((agent / sync.LOCK_NAME).exists())

    def test_probe_only_never_changes_agent_registration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            project, agent = self.make_project(root)
            installed_binary = self.make_binary(root, tool_surface(), "nokv-a")
            installed = self.run_sync(
                *self.sync_args(project, source, installed_binary)
            )
            self.assertEqual(installed[0], 0, installed[2])
            paths = (
                agent / "mcp_registry.jsonl",
                agent / "init.json",
                agent / sync.LOCK_NAME,
            )
            before = {path: path.read_bytes() for path in paths}

            candidate_tools = tool_surface()
            for tool in candidate_tools:
                tool["description"] += " candidate"
            candidate = self.make_binary(root, candidate_tools, "nokv-b")
            accepted = self.run_sync(
                *self.sync_args(project, source, candidate), "--probe-only"
            )

            self.assertEqual(accepted[0], 0, accepted[2])
            self.assertIn("live_contract_valid: true", accepted[1])
            self.assertEqual({path: path.read_bytes() for path in paths}, before)

            rejected_candidate = self.make_binary(
                root, tool_surface(include_restore=False), "nokv-c"
            )
            rejected = self.run_sync(
                *self.sync_args(project, source, rejected_candidate), "--probe-only"
            )

            self.assertEqual(rejected[0], 1)
            self.assertIn("missing=['workbench_restore']", rejected[2])
            self.assertEqual({path: path.read_bytes() for path in paths}, before)

    def test_schema_digest_change_requires_explicit_acceptance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            project, agent = self.make_project(root)
            binary = self.make_binary(root, tool_surface())
            first = self.run_sync(*self.sync_args(project, source, binary))
            self.assertEqual(first[0], 0, first[2])
            lock_path = agent / sync.LOCK_NAME
            old_lock = json.loads(lock_path.read_text(encoding="utf-8"))
            old_lock["contract"]["tools_schema_sha256"] = "c" * 64
            lock_path.write_text(
                json.dumps(old_lock, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            lock_before = lock_path.read_bytes()
            init_before = (agent / "init.json").read_bytes()

            rejected = self.run_sync(*self.sync_args(project, source, binary))

            self.assertEqual(rejected[0], 1)
            new_digest = contract.expected_contract_evidence()["tools_schema_sha256"]
            self.assertIn(f"--accept-contract-sha256 {new_digest}", rejected[2])
            self.assertEqual(lock_path.read_bytes(), lock_before)
            self.assertEqual((agent / "init.json").read_bytes(), init_before)

            accepted = self.run_sync(
                *self.sync_args(project, source, binary),
                "--accept-contract-sha256",
                new_digest,
            )
            self.assertEqual(accepted[0], 0, accepted[2])
            self.assertNotEqual(lock_path.read_bytes(), lock_before)

    def test_lock_write_failure_rolls_back_both_lingtai_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            project, agent = self.make_project(root)
            first_binary = self.make_binary(root, tool_surface(), "nokv-a")
            second_tools = tool_surface()
            for tool in second_tools:
                tool["description"] += " updated"
            second_binary = self.make_binary(root, second_tools, "nokv-b")
            installed = self.run_sync(*self.sync_args(project, source, first_binary))
            self.assertEqual(installed[0], 0, installed[2])
            paths = (
                agent / "mcp_registry.jsonl",
                agent / "init.json",
                agent / sync.LOCK_NAME,
            )
            before = {path: path.read_bytes() for path in paths}
            real_write = sync.installer.write_text_if_changed
            failed = False

            def fail_first_lock_write(path: Path, text: str) -> bool:
                nonlocal failed
                if path.name == sync.LOCK_NAME and not failed:
                    failed = True
                    raise OSError("injected lock write failure")
                return real_write(path, text)

            with mock.patch.object(
                sync.installer,
                "write_text_if_changed",
                side_effect=fail_first_lock_write,
            ):
                rejected = self.run_sync(
                    *self.sync_args(project, source, second_binary)
                )

            self.assertEqual(rejected[0], 1)
            self.assertIn("injected lock write failure", rejected[2])
            self.assertTrue(failed)
            self.assertEqual({path: path.read_bytes() for path in paths}, before)

    def test_check_detects_locked_binary_replacement(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            project, agent = self.make_project(root)
            binary = self.make_binary(root, tool_surface())
            installed = self.run_sync(*self.sync_args(project, source, binary))
            self.assertEqual(installed[0], 0, installed[2])
            lock = json.loads((agent / sync.LOCK_NAME).read_text(encoding="utf-8"))
            command = Path(lock["artifact"]["command"])
            healthy = self.run_sync(
                "--project",
                str(project),
                "--agent",
                "coordinator",
                "--check",
            )
            self.assertEqual(healthy[0], 0, healthy[2])
            self.assertIn("live_contract_valid: true", healthy[1])

            os.chmod(command, 0o755)
            with command.open("a", encoding="utf-8") as handle:
                handle.write("# replaced\n")

            checked = self.run_sync(
                "--project",
                str(project),
                "--agent",
                "coordinator",
                "--check",
            )

            self.assertEqual(checked[0], 1)
            self.assertIn("replaced in place", checked[2])

    def test_build_info_candidate_is_recorded_as_brew_distribution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            project, agent = self.make_project(root)
            binary = self.make_binary(
                root / "Cellar" / "nokv" / "0.1.0" / "bin",
                tool_surface(),
            )
            identity = runtime.source_identity(source, self.source_revision(source))
            build_info = (
                root
                / "Cellar"
                / "nokv"
                / "0.1.0"
                / "share"
                / "nokv"
                / "build-info.json"
            )
            runtime.write_build_info(build_info, identity, binary)

            wrong_tools = tool_surface()
            wrong_tools[0]["description"] += " wrong binary"
            wrong_binary = self.make_binary(root, wrong_tools, "wrong-nokv")
            rejected = self.run_sync(
                "--project",
                str(project),
                "--agent",
                "coordinator",
                "--nokv-bin",
                str(wrong_binary),
                "--build-info",
                str(build_info),
            )
            self.assertEqual(rejected[0], 1)
            self.assertIn("does not match build-info SHA-256", rejected[2])
            self.assertFalse((agent / "mcp_registry.jsonl").exists())

            result = self.run_sync(
                "--project",
                str(project),
                "--agent",
                "coordinator",
                "--nokv-bin",
                str(binary),
                "--build-info",
                str(build_info),
            )

            self.assertEqual(result[0], 0, result[2])
            lock = json.loads((agent / sync.LOCK_NAME).read_text(encoding="utf-8"))
            self.assertEqual(lock["source"]["distribution"], "brew")

    def test_build_info_digest_is_rechecked_during_staging(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            project, agent = self.make_project(root)
            binary = self.make_binary(root, tool_surface())
            args = self.sync_args(project, source, binary)
            real_stage = sync.stage_runtime

            def replace_before_stage(*stage_args, **stage_kwargs):
                with binary.open("a", encoding="utf-8") as handle:
                    handle.write("# replaced after build-info verification\n")
                return real_stage(*stage_args, **stage_kwargs)

            with mock.patch.object(
                sync, "stage_runtime", side_effect=replace_before_stage
            ):
                rejected = self.run_sync(*args)

            self.assertEqual(rejected[0], 1)
            self.assertIn("binary SHA-256 mismatch", rejected[2])
            self.assertFalse((agent / "mcp_registry.jsonl").exists())
            self.assertFalse((agent / sync.LOCK_NAME).exists())

    def test_holt_manifest_and_lock_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root, lock_revision="c" * 40)
            with self.assertRaisesRegex(ValueError, "Holt revision differs"):
                runtime.source_identity(source, self.source_revision(source))

    def test_build_source_owns_the_locked_cargo_build(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            source_revision = self.source_revision(source)
            ambient_target = root / "ambient-cargo-target"
            stale_candidate = source / "target/lingtai-workbench-source/release/nokv"
            self.make_binary(stale_candidate.parent, tool_surface())
            real_run = subprocess.run

            def complete_build(command, **kwargs):
                if command[0] != "cargo":
                    return real_run(command, **kwargs)
                self.assertFalse(kwargs["check"])
                self.assertIn("--locked", command)
                self.assertIn("--release", command)
                target_dir = Path(command[command.index("--target-dir") + 1])
                self.assertEqual(
                    target_dir, source.resolve() / "target/lingtai-workbench-source"
                )
                self.assertFalse((target_dir / "release/nokv").exists())
                self.make_binary(target_dir / "release", tool_surface())
                return subprocess.CompletedProcess(command, 0)

            with (
                mock.patch.dict(os.environ, {"CARGO_TARGET_DIR": str(ambient_target)}),
                mock.patch.object(
                    sync.subprocess, "run", side_effect=complete_build
                ) as run,
            ):
                candidate, identity = sync.build_source_candidate(
                    source,
                    revision=source_revision,
                    allow_dirty=False,
                )

            self.assertEqual(candidate, stale_candidate.resolve())
            self.assertEqual(identity.nokv_git_commit, source_revision)
            cargo_calls = [
                call
                for call in run.call_args_list
                if call.args and call.args[0][0] == "cargo"
            ]
            self.assertEqual(len(cargo_calls), 1)

    def test_interrupted_agent_update_is_recovered_before_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, agent = self.make_project(root)
            paths = sync._transaction_files(agent)
            original = {
                name: path.read_text(encoding="utf-8") if path.exists() else None
                for name, path in paths.items()
            }
            desired = {
                "mcp_registry.jsonl": '{"name":"nokv-workbench"}\n',
                "init.json": '{"mcp":{"nokv-workbench":{}}}\n',
                sync.LOCK_NAME: '{"schema":"desired"}\n',
            }
            transaction = {
                "schema": sync.TRANSACTION_SCHEMA,
                "original": original,
                "desired": desired,
            }
            (agent / sync.TRANSACTION_NAME).write_text(
                json.dumps(transaction), encoding="utf-8"
            )
            paths["mcp_registry.jsonl"].write_text(
                desired["mcp_registry.jsonl"], encoding="utf-8"
            )

            self.assertTrue(sync.recover_interrupted_update(agent))
            self.assertFalse((agent / sync.TRANSACTION_NAME).exists())
            for name, path in paths.items():
                if original[name] is None:
                    self.assertFalse(path.exists())
                else:
                    self.assertEqual(path.read_text(encoding="utf-8"), original[name])

    def test_failed_rollback_retains_journal_for_next_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self.make_source(root)
            project, agent = self.make_project(root)
            first_binary = self.make_binary(root, tool_surface(), "nokv-a")
            second_tools = tool_surface()
            for tool in second_tools:
                tool["description"] += " updated"
            second_binary = self.make_binary(root, second_tools, "nokv-b")
            installed = self.run_sync(*self.sync_args(project, source, first_binary))
            self.assertEqual(installed[0], 0, installed[2])
            paths = sync._transaction_files(agent)
            before = {name: path.read_bytes() for name, path in paths.items()}
            real_write = sync.installer.write_text_if_changed
            registry_writes = 0

            def fail_update_and_registry_rollback(path: Path, text: str) -> bool:
                nonlocal registry_writes
                if path.name == "mcp_registry.jsonl":
                    registry_writes += 1
                    if registry_writes == 2:
                        raise OSError("injected registry rollback failure")
                if path.name == sync.LOCK_NAME:
                    raise OSError("injected lock update failure")
                return real_write(path, text)

            with mock.patch.object(
                sync.installer,
                "write_text_if_changed",
                side_effect=fail_update_and_registry_rollback,
            ):
                rejected = self.run_sync(
                    *self.sync_args(project, source, second_binary)
                )

            journal = agent / sync.TRANSACTION_NAME
            self.assertEqual(rejected[0], 1)
            self.assertIn("recovery journal retained", rejected[2])
            self.assertTrue(journal.is_file())
            self.assertNotEqual(
                paths["mcp_registry.jsonl"].read_bytes(),
                before["mcp_registry.jsonl"],
            )

            self.assertTrue(sync.recover_interrupted_update(agent))
            self.assertFalse(journal.exists())
            self.assertEqual(
                {name: path.read_bytes() for name, path in paths.items()}, before
            )

    def test_contract_digest_ignores_order_and_descriptions(self):
        original = tool_surface()
        changed = copy.deepcopy(list(reversed(original)))
        for tool in changed:
            tool["description"] = "new prose"

        self.assertEqual(
            contract.contract_evidence(original),
            contract.contract_evidence(changed),
        )

    def test_contract_rejects_duplicate_tools_and_nullable_restore(self):
        duplicate = tool_surface()
        duplicate.append(copy.deepcopy(duplicate[0]))
        with self.assertRaisesRegex(contract.WorkbenchContractError, "duplicate"):
            contract.validate_tool_contract(duplicate)

        nullable = tool_surface()
        restore = next(
            tool for tool in nullable if tool["name"] == contract.RESTORE_TOOL
        )
        restore["inputSchema"]["properties"]["at_snapshot"]["anyOf"].append(
            {"type": "null"}
        )
        with self.assertRaisesRegex(
            contract.WorkbenchContractError, "workbench_restore inputSchema differs"
        ):
            contract.validate_tool_contract(nullable)


if __name__ == "__main__":
    unittest.main()
