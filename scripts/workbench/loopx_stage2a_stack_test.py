#!/usr/bin/env python3
"""Unit tests for the LoopX Stage 2A stack bring-up script (no processes are started)."""

from __future__ import annotations

import io
import json
import re
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import loopx_stage2a_stack as stack

HEX32 = re.compile(r"^[0-9a-f]{32}$")


def counted_hex() -> stack.StackIdentity:
    counter = iter(range(1, 100))

    def token_hex(length: int) -> str:
        value = next(counter)
        return f"{value:0{length * 2}x}"

    return stack.fresh_identity(token_hex)


def fixed_endpoints() -> stack.Endpoints:
    ports = iter((23791, 23801, 29001, 29011, 27101))
    return stack.fresh_endpoints(lambda: next(ports))


class IdentityTest(unittest.TestCase):
    def test_ids_are_hex32_and_names_carry_non_colliding_prefixes(self) -> None:
        identity = counted_hex()
        for value in (identity.root_id, identity.shard_id, identity.agent_id):
            self.assertRegex(value, HEX32)
        self.assertTrue(identity.etcd_prefix.startswith("/lx-"))
        self.assertTrue(identity.bucket.startswith("lx-"))
        self.assertTrue(identity.object_root.startswith("rt-"))
        self.assertTrue(identity.access_key.startswith("ak"))
        self.assertTrue(identity.secret_key.startswith("sk"))
        self.assertTrue(identity.workbench.startswith("wb"))
        self.assertTrue(identity.node.startswith("nd-"))

    def test_random_identity_never_collides_with_ladder_vocabulary(self) -> None:
        for _ in range(200):
            identity = stack.fresh_identity()
            endpoints = fixed_endpoints()
            config = stack.client_config(identity, endpoints)
            env = stack.live_env(
                identity, endpoints, config_path=Path("/tmp/x/nokv-client.json"), python=Path("/tmp/x/bin/python")
            )
            self.assertEqual(stack.privacy_collisions(config, env), [])

    def test_privacy_check_reports_a_leaf_inside_ladder_vocabulary(self) -> None:
        identity = counted_hex()
        endpoints = fixed_endpoints()
        config = stack.client_config(identity, endpoints)
        config["object_store"]["bucket"] = "nokv_live"
        env = stack.live_env(identity, endpoints, config_path=Path("/tmp/c.json"), python=Path("/tmp/p"))
        collisions = stack.privacy_collisions(config, env)
        self.assertEqual(
            collisions,
            [
                "'nokv_live' is a substring of ladder vocabulary 's0.nokv_live_matrix'",
                "'nokv_live' is a substring of ladder vocabulary 's2a.nokv_live_qualification'",
            ],
        )


class CommandShapeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.identity = counted_hex()
        self.endpoints = fixed_endpoints()
        self.binary = Path("/opt/nokv/bin/nokv")

    def test_provision_carries_agent_id_and_positional_shard(self) -> None:
        argv = stack.provision_command(self.binary, self.identity, self.endpoints)
        self.assertEqual(argv[0], str(self.binary))
        self.assertEqual(argv[-2:], ["provision", self.identity.shard_id])
        self.assertIn("--agent-id", argv)
        self.assertEqual(argv[argv.index("--agent-id") + 1], self.identity.agent_id)
        self.assertEqual(argv[argv.index("--etcd-endpoint") + 1], "http://127.0.0.1:23791")
        self.assertEqual(argv[argv.index("--etcd-key-prefix") + 1], self.identity.etcd_prefix)

    def test_serve_creates_fresh_metadata_and_never_carries_an_agent_id(self) -> None:
        argv = stack.server_command(self.binary, self.identity, self.endpoints, Path("/stack/metadata"))
        self.assertEqual(argv[-1], "serve")
        self.assertNotIn("--agent-id", argv)
        self.assertEqual(argv[argv.index("--metadata-create") + 1], "/stack/metadata")
        self.assertEqual(argv[argv.index("--bind") + 1], "127.0.0.1:27101")
        self.assertEqual(argv[argv.index("--advertise-endpoint") + 1], "127.0.0.1:27101")
        self.assertEqual(argv[argv.index("--node-id") + 1], self.identity.node)
        self.assertEqual(argv[argv.index("--lifecycle-interval-millis") + 1], "100")

    def test_etcd_is_a_single_new_member_on_loopback(self) -> None:
        argv = stack.etcd_command(Path("/usr/local/bin/etcd"), Path("/stack/etcd"), "nd-01", self.endpoints)
        self.assertEqual(argv[argv.index("--listen-client-urls") + 1], "http://127.0.0.1:23791")
        self.assertEqual(argv[argv.index("--initial-cluster") + 1], "nd-01=http://127.0.0.1:23801")
        self.assertEqual(argv[argv.index("--initial-cluster-state") + 1], "new")

    def test_moto_listens_on_the_object_port_only(self) -> None:
        argv = stack.moto_command(Path("/venv/bin/python"), self.endpoints)
        self.assertEqual(argv, ["/venv/bin/python", "-m", "moto.server", "-H", "127.0.0.1", "-p", "29001"])

    def test_secret_flag_values_are_redacted(self) -> None:
        argv = stack.redact_argv(stack.provision_command(self.binary, self.identity, self.endpoints))
        self.assertNotIn(self.identity.secret_key, argv)
        self.assertTrue(any(item.startswith("<redacted:sha256:") for item in argv))
        self.assertIn(self.identity.access_key, argv)


class LoopXContractTest(unittest.TestCase):
    """The files must match what LoopX's helper and ladder read, key for key."""

    def setUp(self) -> None:
        self.identity = counted_hex()
        self.endpoints = fixed_endpoints()

    def test_client_config_has_exactly_the_helper_keys(self) -> None:
        config = stack.client_config(self.identity, self.endpoints)
        self.assertEqual(set(config), {"root_id", "routing", "object_store", "workbench_root"})
        self.assertEqual(set(config["routing"]), {"kind", "endpoints", "key_prefix", "lease_ttl_seconds"})
        self.assertEqual(config["routing"]["kind"], "etcd")
        self.assertEqual(config["routing"]["endpoints"], ["http://127.0.0.1:23791"])
        self.assertEqual(
            set(config["object_store"]),
            {"kind", "bucket", "region", "root", "endpoint", "access_key_id", "secret_access_key"},
        )
        self.assertEqual(config["object_store"]["kind"], "s3")
        self.assertEqual(config["object_store"]["endpoint"], "http://127.0.0.1:29001")
        self.assertEqual(config["workbench_root"], stack.WORKBENCH_ROOT)

    def test_live_env_covers_both_ladder_gates(self) -> None:
        env = stack.live_env(
            self.identity, self.endpoints, config_path=Path("/stack/nokv-client.json"), python=Path("/venv/bin/python")
        )
        self.assertEqual(
            list(env),
            [
                "NOKV_COORDINATION_LIVE",
                "NOKV_ETCD",
                "NOKV_ETCD_PREFIX",
                "NOKV_ROOT_ID",
                "NOKV_BUCKET",
                "NOKV_OBJECT_ENDPOINT",
                "NOKV_OBJECT_ROOT",
                "NOKV_OBJECT_KEY",
                "NOKV_OBJECT_SECRET",
                "LOOPX_NOKV_AUTHORITY_LIVE",
                "LOOPX_NOKV_AUTHORITY_CONFIG_JSON",
                "LOOPX_NOKV_AUTHORITY_PYTHON",
                "LOOPX_NOKV_AUTHORITY_WORKBENCH",
            ],
        )
        self.assertEqual(env["NOKV_COORDINATION_LIVE"], "1")
        self.assertEqual(env["LOOPX_NOKV_AUTHORITY_LIVE"], "1")
        self.assertEqual(env["LOOPX_NOKV_AUTHORITY_CONFIG_JSON"], "/stack/nokv-client.json")
        self.assertEqual(env["LOOPX_NOKV_AUTHORITY_WORKBENCH"], self.identity.workbench)

    def test_env_rendering_is_shell_safe(self) -> None:
        rendered = stack.render_env({"A": "plain", "B": "with space", "C": "quo'te"})
        self.assertEqual(rendered, "export A=plain\nexport B='with space'\nexport C='quo'\"'\"'te'\n")

    def test_ladder_commands_source_the_env_and_name_both_live_rows(self) -> None:
        commands = stack.ladder_commands(Path("/stack"), "/venv/bin/python")
        self.assertEqual(len(commands), 3)
        self.assertIn("source /stack/live.env", commands[0])
        self.assertIn("--row s0.nokv_live_matrix --row s2a.nokv_live_qualification", commands[1])
        self.assertIn("--allow-unverified", commands[2])


class PlanTest(unittest.TestCase):
    def test_plan_is_json_and_carries_no_secret(self) -> None:
        identity = counted_hex()
        planned = stack.plan(
            binary=Path("/opt/nokv/bin/nokv"),
            python=Path("/venv/bin/python"),
            stack_dir=Path("/stack"),
            identity=identity,
            endpoints=fixed_endpoints(),
            object_store="moto",
        )
        text = json.dumps(planned)
        self.assertNotIn(identity.secret_key, text)
        self.assertNotIn(identity.workbench, text)
        self.assertEqual(planned["privacy_collisions"], [])
        self.assertEqual(planned["files"], ["nokv-client.json", "live.env", "stack.json", "next-steps.txt"])
        self.assertEqual(planned["commands"]["serve"][-1], "serve")
        self.assertEqual(len(planned["client_config_sha256"]), 64)

    def test_plan_subcommand_prints_json_without_touching_the_stack_dir(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            stack_dir = Path(root) / "stack"
            out = io.StringIO()
            with redirect_stdout(out):
                code = stack.main(
                    [
                        "plan",
                        "--stack-dir",
                        str(stack_dir),
                        "--python",
                        "/venv/bin/python",
                        "--object-store",
                        "moto",
                    ]
                )
            self.assertEqual(code, 0)
            self.assertFalse(stack_dir.exists())
            self.assertEqual(json.loads(out.getvalue())["object_store"], "moto")


class FailClosedTest(unittest.TestCase):
    def test_down_and_status_refuse_a_directory_without_state(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            for command in ("down", "status"):
                err = io.StringIO()
                with redirect_stderr(err):
                    code = stack.main([command, "--stack-dir", root])
                self.assertEqual(code, 2)
                self.assertIn("nothing to act on", err.getvalue())

    def test_up_refuses_a_non_empty_stack_dir(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / "leftover").write_text("x", encoding="utf-8")
            err = io.StringIO()
            with redirect_stderr(err):
                code = stack.main(["up", "--stack-dir", root, "--python", "/venv/bin/python"])
            self.assertEqual(code, 2)
            self.assertIn("not empty", err.getvalue())

    def test_status_rejects_an_unknown_state_schema(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / stack.STATE_FILE).write_text(json.dumps({"schema": "other"}), encoding="utf-8")
            err = io.StringIO()
            with redirect_stderr(err):
                code = stack.main(["status", "--stack-dir", root])
            self.assertEqual(code, 2)
            self.assertIn("unknown state schema", err.getvalue())


if __name__ == "__main__":
    unittest.main()
