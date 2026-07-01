#!/usr/bin/env python3
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


sys.dont_write_bytecode = True
MODULE_PATH = Path(__file__).with_name("install_workbench_mcp.py")


def load_module():
    spec = importlib.util.spec_from_file_location("install_workbench_mcp", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class InstallWorkbenchMcpTest(unittest.TestCase):
    def setUp(self):
        self.module = load_module()

    def make_agent(
        self,
        root: Path,
        name: str = "coordinator",
        init: dict | None = None,
        registry: list[dict] | None = None,
        running: bool | None = None,
    ):
        agent_dir = root / ".lingtai" / name
        agent_dir.mkdir(parents=True)
        (agent_dir / "init.json").write_text(
            json.dumps(init or {"mcp": {}}, indent=2) + "\n",
            encoding="utf-8",
        )
        if running is not None:
            (agent_dir / ".status.json").write_text(
                json.dumps(
                    {
                        "identity": {"agent_name": name},
                        "runtime": {"running": running},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        if registry is not None:
            (agent_dir / "mcp_registry.jsonl").write_text(
                "".join(json.dumps(record) + "\n" for record in registry),
                encoding="utf-8",
            )
        return agent_dir

    def config(self):
        return self.module.InstallConfig(
            nokv_bin="/repo/target/debug/nokv",
            server_bind="127.0.0.1:7799",
            object_backend="rustfs",
            s3_endpoint="http://127.0.0.1:9000",
            s3_bucket="nokv-lingtai-workbench",
            workbench_root="/workbenches",
        )

    def test_install_adds_registry_and_init_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            agent_dir = self.make_agent(Path(tmp))

            result = self.module.configure_agent(agent_dir, self.config())

            self.assertTrue(result.registry_changed)
            self.assertTrue(result.init_changed)
            registry = [
                json.loads(line)
                for line in (agent_dir / "mcp_registry.jsonl").read_text().splitlines()
            ]
            self.assertEqual([record["name"] for record in registry], ["nokv-workbench"])
            self.assertEqual(registry[0]["transport"], "stdio")
            self.assertEqual(
                registry[0]["args"],
                [
                    "--server-bind",
                    "127.0.0.1:7799",
                    "--object-backend",
                    "rustfs",
                    "--s3-endpoint",
                    "http://127.0.0.1:9000",
                    "--s3-bucket",
                    "nokv-lingtai-workbench",
                    "mcp",
                    "--profile",
                    "workbench",
                    "--workbench-root",
                    "/workbenches",
                ],
            )
            init = json.loads((agent_dir / "init.json").read_text())
            self.assertEqual(init["mcp"]["nokv-workbench"]["command"], "/repo/target/debug/nokv")
            self.assertEqual(init["mcp"]["nokv-workbench"]["args"], registry[0]["args"])

    def test_install_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            agent_dir = self.make_agent(Path(tmp))

            first = self.module.configure_agent(agent_dir, self.config())
            registry_before = (agent_dir / "mcp_registry.jsonl").read_text()
            init_before = (agent_dir / "init.json").read_text()
            second = self.module.configure_agent(agent_dir, self.config())

            self.assertTrue(first.registry_changed)
            self.assertTrue(first.init_changed)
            self.assertFalse(second.registry_changed)
            self.assertFalse(second.init_changed)
            self.assertEqual((agent_dir / "mcp_registry.jsonl").read_text(), registry_before)
            self.assertEqual((agent_dir / "init.json").read_text(), init_before)

    def test_install_replaces_existing_nokv_workbench_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            old_record = {
                "name": "nokv-workbench",
                "summary": "old",
                "transport": "stdio",
                "command": "/old/nokv",
                "args": ["mcp"],
                "source": "old",
            }
            other_record = {
                "name": "imap",
                "summary": "mail",
                "transport": "stdio",
                "command": "python",
                "args": ["-m", "imap"],
                "source": "local",
            }
            agent_dir = self.make_agent(
                Path(tmp),
                init={
                    "mcp": {
                        "nokv-workbench": {
                            "type": "stdio",
                            "command": "/old/nokv",
                            "args": ["mcp"],
                        },
                        "imap": {"type": "stdio", "command": "python", "args": ["-m", "imap"]},
                    }
                },
                registry=[old_record, other_record, old_record],
            )

            result = self.module.configure_agent(agent_dir, self.config())

            self.assertTrue(result.registry_changed)
            self.assertTrue(result.init_changed)
            registry = [
                json.loads(line)
                for line in (agent_dir / "mcp_registry.jsonl").read_text().splitlines()
            ]
            self.assertEqual([record["name"] for record in registry], ["nokv-workbench", "imap"])
            self.assertEqual(registry[0]["command"], "/repo/target/debug/nokv")
            init = json.loads((agent_dir / "init.json").read_text())
            self.assertEqual(init["mcp"]["nokv-workbench"]["command"], "/repo/target/debug/nokv")
            self.assertEqual(init["mcp"]["imap"]["command"], "python")

    def test_resolve_agent_dir_selects_running_coordinator_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator = self.make_agent(root, "coordinator(codex-gpt-5.4)", running=True)
            self.make_agent(root, "scribe", running=True)

            resolved = self.module.resolve_agent_dir(root, None, None)

            self.assertEqual(resolved, coordinator)

    def test_resolve_agent_dir_selects_single_agent_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            only_agent = self.make_agent(root, "scout", running=False)

            resolved = self.module.resolve_agent_dir(root, None, None)

            self.assertEqual(resolved, only_agent)

    def test_resolve_agent_dir_rejects_ambiguous_non_coordinator_agents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_agent(root, "scout", running=True)
            self.make_agent(root, "scribe", running=True)

            with self.assertRaisesRegex(ValueError, "multiple LingTai agents"):
                self.module.resolve_agent_dir(root, None, None)


if __name__ == "__main__":
    unittest.main()
