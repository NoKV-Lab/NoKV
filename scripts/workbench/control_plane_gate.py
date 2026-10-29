#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Control plane over mutual TLS on one host.

The gate starts one etcd that requires client certificates and has RBAC
enabled, creates an etcd user and role for the operator (`root`), one for the
shard owner, and one per Agent, provisions two roots of two tenants on one
shard, serves the shard over RPC TLS, and drives the native `nokv` binary with
each etcd identity. The owner's role covers only its shard's records plus the
read-only placement, binding and tenant records it serves from; an Agent's
role covers exactly the keys its root routes through. Certificates come from
the `openssl` command; the etcd server, the owner, and every client are signed
by the gate's CA, and a certificate's Common Name is its etcd user.

A scenario is graded from exit codes, the stderr failure envelope, oracle
read-backs through the operator's identity, and `etcdctl` probes that show
what each role can and cannot touch. Private keys never enter the evidence
directory's retained files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

from cli_contract_gate import (
    REPOSITORY,
    Call,
    ContractFailure,
    NotQualified,
    Stack,
    free_port,
    new_id,
    require,
    verdict,
)

SCHEMA = "nokv.control_plane_gate.v1"
SERVER_NAME = "owner.test"
WORKBENCH = "control"
# One etcd user per identity; the certificate's Common Name names the user.
IDENTITIES = ("root", "owner", "agent", "stranger", "nobody")
# The exact keys one Agent's role grants, relative to the deployment prefix,
# per root and per shard. docs/control-plane-credentials.md lists the same.
AGENT_ROOT_KEYS = ("roots", "root-object-namespaces", "root-agents", "root-presentations")
AGENT_SHARD_KEYS = ("logical-shards",)
OWNER_SHARD_KEYS = ("logical-shards", "logical-shard-recovery", "sessions")
OWNER_READ_PREFIXES = ("roots", "root-object-namespaces", "root-agents", "root-tenants", "root-presentations")


def openssl(directory: Path, *arguments: str) -> None:
    done = subprocess.run(["openssl", *arguments], cwd=directory, capture_output=True, text=True)
    if done.returncode != 0:
        raise NotQualified(f"openssl {arguments[0]} failed: {done.stderr[-400:]}")


class ControlPki:
    """One CA that signs the etcd server, the owner, and every client."""

    def __init__(self, directory: Path) -> None:
        directory.mkdir()
        self.directory = directory
        self.p256 = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes"]
        openssl(directory, "req", "-x509", *self.p256, "-subj", "/CN=nokv-control-gate-ca", "-days", "2",
                "-addext", "basicConstraints=critical,CA:TRUE",
                "-addext", "keyUsage=critical,keyCertSign",
                "-keyout", "ca.key", "-out", "ca.pem")
        # A second CA that etcd does not trust, for the wrong-issuer check.
        openssl(directory, "req", "-x509", *self.p256, "-subj", "/CN=other-ca", "-days", "2",
                "-addext", "basicConstraints=critical,CA:TRUE",
                "-addext", "keyUsage=critical,keyCertSign",
                "-keyout", "other-ca.key", "-out", "other-ca.pem")
        self.issue("etcd", "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n")
        self.issue("owner-rpc", f"subjectAltName=DNS:{SERVER_NAME}\nextendedKeyUsage=serverAuth\n", cn=SERVER_NAME)
        for name in IDENTITIES:
            self.issue(name, "extendedKeyUsage=clientAuth\n")
        self.issue("outsider", "extendedKeyUsage=clientAuth\n", ca="other-ca")

    def issue(self, name: str, extensions: str, *, cn: str | None = None, ca: str = "ca") -> None:
        openssl(self.directory, "req", *self.p256, "-subj", f"/CN={cn or name}",
                "-keyout", f"{name}.key", "-out", f"{name}.csr")
        (self.directory / f"{name}.ext").write_text(extensions + "basicConstraints=critical,CA:FALSE\n")
        openssl(self.directory, "x509", "-req", "-in", f"{name}.csr", "-CA", f"{ca}.pem", "-CAkey", f"{ca}.key",
                "-CAcreateserial", "-days", "2", "-extfile", f"{name}.ext", "-out", f"{name}.pem")

    def pair(self, name: str) -> tuple[Path, Path]:
        return self.directory / f"{name}.pem", self.directory / f"{name}.key"


class ControlStack(Stack):
    """The CLI contract gate's stack on an etcd that authenticates every client
    by certificate and authorizes it by role. The owner advertises its own
    port: the frame-level proxy cannot read TLS records."""

    def __init__(self, args: argparse.Namespace, evidence: Path) -> None:
        super().__init__(args, evidence)
        self.pki = ControlPki(evidence / "pki")
        self.prefix = "/nokv/control-gate-" + secrets.token_hex(4)
        self.ids["other_root"] = new_id()
        self.ids["other_shard"] = new_id()
        self.tenants = {"agent": new_id(), "stranger": new_id()}
        self.principals = {"agent": new_id(), "stranger": new_id()}
        self.etcd_port = free_port()
        self.etcd_url = f"https://127.0.0.1:{self.etcd_port}"
        self.etcd: subprocess.Popen | None = None
        self.identity = "agent"
        self.rpc_identity: str | None = "agent"

    # etcd.

    def start_etcd(self) -> None:
        peer = f"http://127.0.0.1:{free_port()}"
        certificate, key = self.pki.pair("etcd")
        argv = [
            "etcd", "--name", "nokv-control-gate", "--data-dir", str(self.evidence / "etcd-data"),
            "--listen-client-urls", self.etcd_url, "--advertise-client-urls", self.etcd_url,
            "--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer,
            "--initial-cluster", f"nokv-control-gate={peer}", "--initial-cluster-state", "new",
            "--cert-file", str(certificate), "--key-file", str(key),
            "--trusted-ca-file", str(self.pki.directory / "ca.pem"), "--client-cert-auth=true",
            "--log-level", "warn",
        ]
        try:
            self.etcd = subprocess.Popen(
                argv, stdout=(self.evidence / "etcd.log").open("wb"), stderr=subprocess.STDOUT
            )
        except FileNotFoundError as missing:
            raise NotQualified(f"etcd is required: {missing}")
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            if self.etcdctl("root", "endpoint", "health").returncode == 0:
                return
            if self.etcd.poll() is not None:
                break
            time.sleep(0.2)
        raise NotQualified("etcd did not become healthy over TLS")

    def etcdctl(self, identity: str, *arguments: str) -> subprocess.CompletedProcess:
        certificate, key = self.pki.pair(identity)
        argv = [
            "etcdctl", "--endpoints", self.etcd_url, "--cacert", str(self.pki.directory / "ca.pem"),
            "--cert", str(certificate), "--key", str(key), "--dial-timeout", "5s",
            "--command-timeout", "10s", *arguments,
        ]
        done = subprocess.run(argv, capture_output=True, text=True, timeout=self.timeout)
        self.write("etcdctl.jsonl", {
            "identity": identity, "arguments": arguments, "exit": done.returncode,
            "stdout": done.stdout[-2000:], "stderr": done.stderr[-600:],
        })
        return done

    def admin(self, *arguments: str) -> str:
        done = self.etcdctl("root", *arguments)
        if done.returncode != 0:
            raise NotQualified(f"etcdctl {' '.join(arguments[:3])} failed: {done.stderr[-400:]}")
        return done.stdout

    def key(self, family: str, identity: str) -> str:
        return f"{self.prefix}/{family}/{identity}"

    def grant_agent(self, role: str, root: str, shard: str) -> None:
        for family in AGENT_ROOT_KEYS:
            self.admin("role", "grant-permission", role, "read", self.key(family, root))
        for family in AGENT_SHARD_KEYS:
            self.admin("role", "grant-permission", role, "read", self.key(family, shard))

    def install_rbac(self) -> None:
        """Users and roles, then `auth enable`; from then on every request is
        authenticated by its certificate's Common Name."""
        self.admin("user", "add", "root", "--no-password")
        if self.etcdctl("root", "role", "add", "root").returncode != 0:
            self.admin("role", "get", "root")
        self.admin("user", "grant-role", "root", "root")
        shard, other_shard = self.ids["shard"], self.ids["other_shard"]
        self.admin("role", "add", "nokv-owner")
        for family in OWNER_SHARD_KEYS:
            self.admin("role", "grant-permission", "nokv-owner", "readwrite", self.key(family, shard))
        for family in OWNER_READ_PREFIXES:
            self.admin("role", "grant-permission", "nokv-owner", "--prefix=true", "read", f"{self.prefix}/{family}/")
        # The tenant record itself and everything below it: ids are fixed
        # width, so no other tenant's keys share this prefix.
        for tenant in self.tenants.values():
            self.admin("role", "grant-permission", "nokv-owner", "--prefix=true", "read", f"{self.prefix}/tenants/{tenant}")
        self.admin("user", "add", "owner", "--no-password")
        self.admin("user", "grant-role", "owner", "nokv-owner")
        self.admin("role", "add", "nokv-agent")
        self.grant_agent("nokv-agent", self.ids["root"], shard)
        self.admin("user", "add", "agent", "--no-password")
        self.admin("user", "grant-role", "agent", "nokv-agent")
        self.admin("role", "add", "nokv-stranger")
        self.grant_agent("nokv-stranger", self.ids["other_root"], other_shard)
        self.admin("user", "add", "stranger", "--no-password")
        self.admin("user", "grant-role", "stranger", "nokv-stranger")
        self.admin("auth", "enable")
        require(self.etcdctl("root", "auth", "status").stdout.startswith("Authentication Status: true"),
                "etcd authentication is enabled", None)

    # The CLI.

    def etcd_options(self, identity: str, *, ca: str = "ca") -> list[str]:
        certificate, key = self.pki.pair(identity)
        return [
            "--etcd-endpoint", self.etcd_url, "--etcd-key-prefix", self.prefix,
            "--etcd-tls-ca", str(self.pki.directory / f"{ca}.pem"),
            "--etcd-tls-cert", str(certificate), "--etcd-tls-key", str(key),
        ]

    def rpc_options(self, identity: str) -> list[str]:
        certificate, key = self.pki.pair(identity)
        return [
            "--rpc-tls-ca", str(self.pki.directory / "ca.pem"),
            "--rpc-tls-cert", str(certificate), "--rpc-tls-key", str(key),
            "--rpc-tls-server-name", SERVER_NAME,
        ]

    def run(self, label: str, arguments: list[str], client_options: list[str] = (), *,
            identity: str | None = None, rpc: str | None = None, root: str | None = None,
            etcd: list[str] | None = None) -> Call:
        identity = identity or self.identity
        connects = arguments[0] not in ("provision", "identity", "serve")
        self.route = [
            "--root-id", root or self.ids["root"],
            *(self.etcd_options(identity) if etcd is None else etcd),
            *(self.rpc_options(rpc or identity) if connects else []),
        ]
        return super().run(label, arguments, client_options)

    def provision(self) -> None:
        for root, shard in ((self.ids["root"], self.ids["shard"]), (self.ids["other_root"], self.ids["other_shard"])):
            call = self.run(f"provision-{root[:8]}", ["provision", shard, "--workbench-root", "/agents/control/wb"],
                            identity="root", root=root)
            if call.exit != 0:
                raise NotQualified(f"provision failed: {call.stderr[-600:]}")

    def identity_admin(self, label: str, *arguments: str, identity: str = "root") -> Call:
        return self.run(label, ["identity", *arguments], identity=identity)

    def register_tenants(self) -> None:
        expires = str(int(time.time()) + 3600)
        for name, root in (("agent", self.ids["root"]), ("stranger", self.ids["other_root"])):
            tenant, principal = self.tenants[name], self.principals[name]
            certificate, _ = self.pki.pair(name)
            for label, arguments in (
                (f"tenant-{name}", ["tenant", "create", tenant]),
                (f"principal-{name}", ["principal", "create", tenant, principal]),
                (f"credential-{name}", ["credential", "add", tenant, principal, "--certificate", str(certificate),
                                        "--expires-at-unix", expires]),
                (f"role-{name}", ["role", "grant", tenant, principal, "--role", "writer", "--tenant-wide"]),
                (f"bind-{name}", ["root", "bind", root, tenant]),
            ):
                self.success(self.identity_admin(label, *arguments))

    def start_owner(self) -> None:
        self.owner_starts += 1
        certificate, key = self.pki.pair("owner-rpc")
        argv = [
            str(self.binary), "--root-id", self.ids["root"], *self.etcd_options("owner"), *self.objects,
            "--rpc-tls-cert", str(certificate), "--rpc-tls-key", str(key),
            "--bind", f"127.0.0.1:{self.owner_port}", "--advertise-endpoint", f"127.0.0.1:{self.owner_port}",
            "--node-id", "control-gate-owner",
            "--metadata-reopen" if self.owner_starts > 1 else "--metadata-create", str(self.metadata),
            "serve",
        ]
        self.owner = subprocess.Popen(
            argv, stdout=(self.evidence / f"owner-{self.owner_starts}.log").open("wb"),
            stderr=subprocess.STDOUT, env=self.env,
        )
        self.wait_tcp(self.owner_port, "owner")
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            probe = self.run("owner-ready", ["operation", "status", new_id()])
            if '"NotFound"' in probe.stderr:
                return
            if self.owner.poll() is not None:
                break
            time.sleep(0.2)
        raise NotQualified(f"owner {self.owner_starts} did not become ready: {probe.stderr[-400:]}")

    def close(self) -> None:
        super().close()
        if self.etcd is not None and self.etcd.poll() is None:
            self.etcd.kill()
            self.etcd.wait(timeout=self.timeout)
        # Evidence keeps the certificates, never the private keys.
        for key in self.pki.directory.glob("*.key"):
            key.unlink()

    def refused(self, label: str, arguments: list[str], klass: str, **client: Any) -> dict[str, Any]:
        return self.failure(self.run(label, arguments, **client), klass, "operator")


# Scenarios.


def partial_tls_options_are_invalid_input(stack: ControlStack) -> dict[str, Any]:
    """C0: the three --etcd-tls-* options are given together or not at all,
    and only with etcd routing; a usage error sends nothing."""
    certificate, key = stack.pki.pair("agent")
    observed = {}
    for name, etcd in (
        ("ca-only", ["--etcd-endpoint", stack.etcd_url, "--etcd-tls-ca", str(stack.pki.directory / "ca.pem")]),
        ("no-key", ["--etcd-endpoint", stack.etcd_url, "--etcd-tls-ca", str(stack.pki.directory / "ca.pem"),
                    "--etcd-tls-cert", str(certificate)]),
        ("plaintext-endpoint", ["--etcd-endpoint", f"http://127.0.0.1:{stack.etcd_port}",
                                *stack.etcd_options("agent")[2:]]),
        ("local-routing", ["--control-dir", str(stack.control), "--etcd-tls-ca", str(stack.pki.directory / "ca.pem"),
                           "--etcd-tls-cert", str(certificate), "--etcd-tls-key", str(key)]),
    ):
        call = stack.run(f"c0-{name}", ["operation", "status", new_id()], etcd=etcd)
        observed[name] = stack.failure(call, "invalid_input", "fix_input")
        require(call.connections == 0 and not call.requests, f"c0-{name}: no request left the process", call.requests)
    return observed


def agent_round_trip(stack: ControlStack) -> dict[str, Any]:
    """C1: the Agent's least-privilege etcd role routes, and the owner's role
    serves: create, publish, and read back over both TLS planes."""
    stack.success(stack.run("c1-create", ["workbench", "workbench_create", json.dumps({"id": WORKBENCH})]))
    put = stack.success(stack.put("c1-put", WORKBENCH, "control.txt", new_id(), b"control plane"))
    stat = stack.stat("c1-stat", WORKBENCH, "control.txt")
    require(stat.get("exists") is True and stat.get("generation") == put.get("generation"),
            "c1: the published path reads back", [put, stat])
    require(stack.body("c1-body", WORKBENCH, "control.txt") == b"control plane", "c1: the body reads back", stat)
    return {"put": put, "stat": stat}


def agent_identity_cannot_administer(stack: ControlStack) -> dict[str, Any]:
    """C2: `nokv identity` with the Agent's etcd identity is permission_denied,
    and the control plane keeps no trace of the attempt."""
    tenant = new_id()
    envelope = stack.refused("c2-tenant-create", ["identity", "tenant", "create", tenant], "permission_denied",
                             identity="agent")
    probe = stack.etcdctl("root", "get", "--prefix", f"{stack.prefix}/tenants/{tenant}")
    require(probe.returncode == 0 and probe.stdout.strip() == "", "c2: no tenant record was written", probe.stdout)
    return {"refusal": envelope}


def another_tenants_root_is_denied(stack: ControlStack) -> dict[str, Any]:
    """C3: an Agent of tenant B cannot route to tenant A's root, and the
    refusal names no owner endpoint; tenant A's Agent cannot route to B."""
    observed = {}
    for label, identity, root in (
        ("c3-stranger-reads-a", "stranger", stack.ids["root"]),
        ("c3-agent-reads-b", "agent", stack.ids["other_root"]),
    ):
        call = stack.run(label, ["workspace-path", "stat", WORKBENCH, "outputs", "control.txt"],
                         identity=identity, root=root)
        envelope = stack.failure(call, "permission_denied", "operator")
        require(f"127.0.0.1:{stack.owner_port}" not in call.stderr and "endpoint" not in envelope.get("message", ""),
                f"{label}: the refusal reveals no route", envelope)
        require(call.connections == 0, f"{label}: no owner connection was made", call.connections)
        observed[label] = envelope
    return observed


def role_matrix_holds(stack: ControlStack) -> dict[str, Any]:
    """C4: etcdctl probes of what each role may touch. The Agent reads its
    routing keys and nothing else; the owner writes only its shard."""
    root, shard = stack.ids["root"], stack.ids["shard"]
    # "value": the read succeeds and returns the record; "allowed": the read
    # succeeds, whether or not the owner has written the key yet.
    probes = [
        ("agent", "value", "get", stack.key("roots", root)),
        ("agent", "value", "get", stack.key("logical-shards", shard)),
        ("agent", "denied", "get", stack.key("logical-shard-recovery", shard)),
        ("agent", "denied", "get", stack.key("sessions", shard)),
        ("agent", "denied", "get", stack.key("roots", stack.ids["other_root"])),
        ("agent", "denied", "get", "--prefix", f"{stack.prefix}/roots/"),
        ("agent", "denied", "get", "--prefix", f"{stack.prefix}/tenants/{stack.tenants['agent']}/"),
        ("agent", "denied", "put", stack.key("roots", root), "tampered"),
        ("agent", "denied", "put", stack.key("logical-shards", shard), "tampered"),
        ("owner", "allowed", "get", stack.key("logical-shard-recovery", shard)),
        ("owner", "value", "get", stack.key("roots", root)),
        ("owner", "value", "get", "--prefix", f"{stack.prefix}/tenants/{stack.tenants['agent']}/"),
        ("owner", "denied", "put", stack.key("logical-shards", stack.ids["other_shard"]), "tampered"),
        ("owner", "denied", "put", stack.key("roots", root), "tampered"),
        ("owner", "denied", "put", f"{stack.prefix}/tenants/{stack.tenants['agent']}/revision", "0"),
        ("stranger", "value", "get", stack.key("roots", stack.ids["other_root"])),
        ("stranger", "denied", "get", stack.key("roots", root)),
    ]
    observed = []
    for identity, expected, *arguments in probes:
        done = stack.etcdctl(identity, *arguments)
        if done.returncode != 0:
            actual = "denied" if "permission denied" in done.stderr else f"error: {done.stderr[-120:]}"
        else:
            actual = "value" if done.stdout.strip() else "allowed"
        observed.append({"identity": identity, "arguments": arguments, "expected": expected, "actual": actual})
        accepted = actual == expected or (expected == "allowed" and actual == "value")
        require(accepted, f"c4: {identity} {' '.join(arguments[:2])} is {expected}", observed[-1])
    return {"probes": observed}


def unaccepted_identities_are_refused(stack: ControlStack) -> dict[str, Any]:
    """C5: a trusted certificate whose Common Name is no etcd user holds no
    role, so etcd answers permission denied; a certificate etcd does not
    trust is rejected in the handshake, as unauthenticated; a CA that does
    not sign etcd's certificate is a usage error; a plaintext endpoint
    against the TLS listener is an unreachable server. None reveals an
    owner, and none reaches one."""
    status = ["operation", "status", new_id()]
    plaintext = ["--etcd-endpoint", f"http://127.0.0.1:{stack.etcd_port}", "--etcd-key-prefix", stack.prefix]
    observed = {
        "unregistered-cn": stack.refused("c5-nobody", status, "permission_denied", identity="nobody", rpc="agent"),
        "untrusted-certificate": stack.refused("c5-outsider", status, "unauthenticated", identity="outsider",
                                               rpc="agent"),
        "wrong-ca": stack.failure(stack.run("c5-wrong-ca", status, etcd=stack.etcd_options("agent", ca="other-ca")),
                                  "invalid_input", "fix_input"),
        "plaintext": stack.failure(stack.run("c5-plaintext", status, etcd=plaintext), "transient", "resubmit_same"),
    }
    require("UnknownCA" in observed["untrusted-certificate"]["message"], "c5: the handshake alert is named",
            observed["untrusted-certificate"])
    for name, envelope in observed.items():
        require(f"127.0.0.1:{stack.owner_port}" not in envelope.get("message", ""), f"c5-{name}: no route leaks",
                envelope)
    return observed


def deleted_user_is_refused_at_once(stack: ControlStack) -> dict[str, Any]:
    """C6: deleting the Agent's etcd user denies its next command: a trusted
    certificate alone holds no role."""
    stat = ["workspace-path", "stat", WORKBENCH, "outputs", "control.txt"]
    stack.success(stack.run("c6-before", stat))
    stack.admin("user", "delete", "agent")
    started = time.monotonic()
    envelope = stack.refused("c6-after", stat, "permission_denied")
    elapsed = time.monotonic() - started
    stack.admin("user", "add", "agent", "--no-password")
    stack.admin("user", "grant-role", "agent", "nokv-agent")
    stack.success(stack.run("c6-restored", stat))
    return {"refusal": envelope, "elapsed_seconds": round(elapsed, 3)}


SCENARIOS: dict[str, Callable[[ControlStack], dict]] = {
    "c0_partial_tls_options_are_invalid_input": partial_tls_options_are_invalid_input,
    "c1_agent_round_trip": agent_round_trip,
    "c2_agent_identity_cannot_administer": agent_identity_cannot_administer,
    "c3_another_tenants_root_is_denied": another_tenants_root_is_denied,
    "c4_role_matrix_holds": role_matrix_holds,
    "c5_unaccepted_identities_are_refused": unaccepted_identities_are_refused,
    "c6_deleted_user_is_refused_at_once": deleted_user_is_refused_at_once,
}


def run_scenario(stack: ControlStack, scenario: Callable[[ControlStack], dict]) -> dict[str, Any]:
    try:
        return {"status": "PASS", "evidence": scenario(stack)}
    except ContractFailure as failure:
        return {"status": "FAIL", "reason": str(failure)}
    except NotQualified as gap:
        return {"status": "NOT QUALIFIED", "reason": str(gap)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--nokv-bin", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--object-bucket", required=True)
    endpoint = parser.add_mutually_exclusive_group(required=True)
    endpoint.add_argument("--object-endpoint", help="an existing S3 endpoint; AWS_* credentials from the environment")
    endpoint.add_argument("--moto-server", help="start this moto_server with generated credentials")
    parser.add_argument("--object-root")
    parser.add_argument("--timeout", type=float, default=45)
    parser.add_argument("--scenario", action="append", choices=sorted(SCENARIOS))
    args = parser.parse_args()
    evidence = Path(args.evidence_dir).resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    binary = Path(args.nokv_bin).resolve()
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "binary": {
            "path": str(binary),
            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "version": subprocess.run([str(binary), "version", "--json"], capture_output=True, text=True).stdout,
        },
        "source": subprocess.run(["git", "-C", str(REPOSITORY), "rev-parse", "HEAD"],
                                 capture_output=True, text=True).stdout.strip(),
        "scenarios": {},
    }
    stack = None
    try:
        stack = ControlStack(args, evidence)
        stack.start_etcd()
        stack.install_rbac()
        stack.provision()
        stack.register_tenants()
        stack.start_owner()
        for name in args.scenario or SCENARIOS:
            report["scenarios"][name] = run_scenario(stack, SCENARIOS[name])
            print(f"{name}: {report['scenarios'][name]['status']}", flush=True)
        report["overall_status"] = verdict(report["scenarios"], "pass")
    except (ContractFailure, NotQualified) as gap:
        report["overall_status"] = "NOT QUALIFIED"
        report["reason"] = str(gap)
    finally:
        if stack is not None:
            stack.close()
        (evidence / "qualification.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"overall: {report['overall_status']}")
    return 0 if report["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
