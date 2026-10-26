#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Workspace RPC over TLS on one host.

The gate provisions one root on a `--control-dir` control store, registers
client certificates as credentials of a tenant's principals, grants them roles,
and serves the root over TLS. Before the root is bound to that tenant, the
owner must refuse to serve at all. The gate then drives the native `nokv`
binary as a registered writer, with an unregistered certificate, without TLS,
with a mismatched owner name, with a revoked credential, as a reader, as a
second writer of the same root, and in a session whose role is revoked. Once
the root is bound to its tenant, an owner without TLS must refuse to serve it. Each
refusal must be exactly one `operator_required` failure line, and a read-back
by a writer must show that the refused write changed nothing. Certificates come
from the `openssl` command.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from cli_contract_gate import (
    REPOSITORY,
    Call,
    ContractFailure,
    NotQualified,
    Stack,
    new_id,
    require,
    verdict,
)

SCHEMA = "nokv.rpc_tls_gate.v1"
SERVER_NAME = "owner.test"
WORKBENCH = "tls"


def openssl(directory: Path, *arguments: str) -> None:
    done = subprocess.run(["openssl", *arguments], cwd=directory, capture_output=True, text=True)
    if done.returncode != 0:
        raise NotQualified(f"openssl {arguments[0]} failed: {done.stderr[-400:]}")


class Pki:
    """A CA that signs the owner's certificate, and self-signed clients."""

    def __init__(self, directory: Path) -> None:
        directory.mkdir()
        self.directory = directory
        p256 = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes"]
        openssl(directory, "req", "-x509", *p256, "-subj", "/CN=nokv-gate-ca", "-days", "2",
                "-addext", "basicConstraints=critical,CA:TRUE",
                "-addext", "keyUsage=critical,keyCertSign",
                "-keyout", "ca.key", "-out", "ca.pem")
        openssl(directory, "req", *p256, "-subj", f"/CN={SERVER_NAME}",
                "-keyout", "owner.key", "-out", "owner.csr")
        (directory / "owner.ext").write_text(
            f"subjectAltName=DNS:{SERVER_NAME}\n"
            "basicConstraints=critical,CA:FALSE\n"
            "extendedKeyUsage=serverAuth\n"
        )
        openssl(directory, "x509", "-req", "-in", "owner.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                "-CAcreateserial", "-days", "2", "-extfile", "owner.ext", "-out", "owner.pem")
        self.p256 = p256

    def client(self, name: str) -> tuple[Path, Path]:
        openssl(self.directory, "req", "-x509", *self.p256, "-subj", f"/CN={name}", "-days", "2",
                "-addext", "basicConstraints=critical,CA:FALSE",
                "-keyout", f"{name}.key", "-out", f"{name}.pem")
        return self.directory / f"{name}.pem", self.directory / f"{name}.key"


class TlsStack(Stack):
    """The CLI contract gate's stack, served over TLS. The owner advertises its
    own port: the frame-level proxy cannot read TLS records."""

    def __init__(self, args: argparse.Namespace, evidence: Path) -> None:
        super().__init__(args, evidence)
        self.pki = Pki(evidence / "pki")
        self.clients = {
            name: self.pki.client(name)
            for name in ("agent", "stranger", "rotated", "reader", "peer", "session", "operator", "fourth")
        }
        self.tenant = new_id()
        self.principals = {
            name: new_id() for name in ("agent", "reader", "peer", "session", "operator", "fourth")
        }
        self.principal = self.principals["agent"]

    def tls_options(self, client: str | None, server_name: str) -> list[str]:
        if client is None:
            return []
        certificate, key = self.clients[client]
        return [
            "--rpc-tls-ca", str(self.pki.directory / "ca.pem"),
            "--rpc-tls-cert", str(certificate),
            "--rpc-tls-key", str(key),
            "--rpc-tls-server-name", server_name,
        ]

    def run(self, label: str, arguments: list[str], *, tls: str | None = "agent",
            server_name: str = SERVER_NAME, agent_id: str | None = None) -> Call:
        # provision and identity write the control store and never connect.
        connects = arguments[0] not in ("provision", "identity")
        options = self.tls_options(tls, server_name) if connects else []
        if agent_id is None:
            return super().run(label, [*options, *arguments])
        configured, self.ids["agent"] = self.ids["agent"], agent_id
        try:
            return super().run(label, [*options, *arguments])
        finally:
            self.ids["agent"] = configured

    def identity(self, label: str, *arguments: str) -> dict[str, Any]:
        return self.success(self.run(label, ["identity", *arguments]))

    def register(self, client: str, principal: str = "agent") -> dict[str, Any]:
        certificate, _ = self.clients[client]
        return self.identity(
            f"credential-{client}", "credential", "add", self.tenant, self.principals[principal],
            "--certificate", str(certificate), "--expires-at-unix", str(int(time.time()) + 3600),
        )

    def role(self, action: str, principal: str, role: str, scope: list[str]) -> dict[str, Any]:
        return self.identity(
            f"role-{action}-{principal}-{role}", "role", action, self.tenant,
            self.principals[principal], "--role", role, *scope,
        )

    def spawn_owner(self, log: str, *, reopen: bool, tls: bool = True) -> subprocess.Popen:
        tls_options = [
            "--rpc-tls-cert", str(self.pki.directory / "owner.pem"),
            "--rpc-tls-key", str(self.pki.directory / "owner.key"),
        ] if tls else []
        argv = [
            str(self.binary), *self.route, *self.objects, *tls_options,
            "--bind", f"127.0.0.1:{self.owner_port}",
            "--advertise-endpoint", f"127.0.0.1:{self.owner_port}",
            "--node-id", "rpc-tls-owner",
            "--metadata-reopen" if reopen else "--metadata-create", str(self.metadata),
            "serve",
        ]
        return subprocess.Popen(
            argv, stdout=(self.evidence / log).open("wb"), stderr=subprocess.STDOUT, env=self.env
        )

    def start_owner(self) -> None:
        self.owner_starts += 1
        self.owner = self.spawn_owner(f"owner-{self.owner_starts}.log", reopen=self.owner_starts > 1)
        self.wait_tcp(self.owner_port, "owner")
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            probe = self.run("owner-ready", ["operation", "status", new_id()])
            if '"NotFound"' in probe.stderr:
                return
            if self.owner.poll() is not None:
                break
            time.sleep(0.2)
        raise NotQualified(f"owner {self.owner_starts} did not become ready")

    def close(self) -> None:
        super().close()
        # Evidence keeps the certificates, never the private keys.
        for key in self.pki.directory.glob("*.key"):
            key.unlink()

    def refused(self, label: str, path: str, klass: str = "unauthenticated", **client: Any) -> dict[str, Any]:
        """A write by this client fails as `klass` and changes nothing."""
        arguments = self.put_arguments(label, WORKBENCH, path, new_id(), label.encode())
        envelope = self.failure(self.run(label, arguments, **client), klass, "operator")
        oracle = self.stat(f"{label}-oracle", WORKBENCH, path)
        require(oracle.get("exists") is False, f"{label}: the refused write left nothing", oracle)
        return envelope


def owner_refuses_an_unbound_root(stack: TlsStack) -> dict[str, Any]:
    """T0: an owner with a root that no tenant owns does not serve over TLS."""
    owner = stack.spawn_owner("owner-unbound.log", reopen=False)
    try:
        code = owner.wait(timeout=stack.timeout)
    except subprocess.TimeoutExpired:
        owner.kill()
        owner.wait(timeout=stack.timeout)
        raise ContractFailure("t0: an owner whose root has no tenant kept running")
    log = (stack.evidence / "owner-unbound.log").read_text(errors="replace")
    require(code != 0 and "is not bound to a tenant" in log,
            "t0: the owner names the unbound root and exits", [code, log[-600:]])
    return {"exit": code, "log": log[-600:]}


def plaintext_owner_refuses_a_tenant_root(stack: TlsStack) -> dict[str, Any]:
    """T0b: once a tenant owns the root, an owner without TLS does not serve it."""
    owner = stack.spawn_owner("owner-plaintext.log", reopen=False, tls=False)
    try:
        code = owner.wait(timeout=stack.timeout)
    except subprocess.TimeoutExpired:
        owner.kill()
        owner.wait(timeout=stack.timeout)
        raise ContractFailure("t0b: a plaintext owner served a tenant-bound root")
    log = (stack.evidence / "owner-plaintext.log").read_text(errors="replace")
    require(code != 0 and "served only over RPC TLS" in log,
            "t0b: the plaintext owner names the tenant-bound root and exits", [code, log[-600:]])
    return {"exit": code, "log": log[-600:]}


def registered_client_round_trip(stack: TlsStack) -> dict[str, Any]:
    """T1: the registered client creates, publishes, and reads back over TLS."""
    stack.success(stack.run("t1-create", ["workbench", "workbench_create", json.dumps({"id": WORKBENCH})]))
    put = stack.success(stack.put("t1-put", WORKBENCH, "registered.txt", new_id(), b"registered"))
    stat = stack.stat("t1-stat", WORKBENCH, "registered.txt")
    require(stat.get("exists") is True and stat.get("generation") == put.get("generation"),
            "t1: the published path reads back", [put, stat])
    return {"put": put, "stat": stat}


def unregistered_certificate_refused(stack: TlsStack) -> dict[str, Any]:
    """T2: a certificate that is no credential of the tenant is refused."""
    envelope = stack.refused("t2-stranger", "stranger.txt", tls="stranger")
    require("not a current credential" in envelope["message"], "t2: the refusal names the credential", envelope)
    return envelope


def plaintext_client_refused(stack: TlsStack) -> dict[str, Any]:
    """T3: a client without TLS is told that the owner requires it."""
    envelope = stack.refused("t3-plaintext", "plaintext.txt", tls=None)
    require("requires TLS" in envelope["message"], "t3: the refusal names TLS", envelope)
    return envelope


def mismatched_owner_name_refused(stack: TlsStack) -> dict[str, Any]:
    """T4: a client that expects another owner name rejects the certificate."""
    envelope = stack.refused("t4-other-name", "other-name.txt", klass="operator_required", server_name="other.test")
    require("RPC TLS failed" in envelope["message"], "t4: the refusal names TLS", envelope)
    return envelope


def revoked_credential_refused(stack: TlsStack) -> dict[str, Any]:
    """T5: a credential works until it is revoked, and no new connection
    authenticates after that."""
    added = stack.register("rotated")
    # The owner rereads its tenant's records every second; a new credential
    # works from the next refresh.
    time.sleep(2.5)
    stack.stat("t5-before-revoke", WORKBENCH, "registered.txt")
    stack.success(stack.run("t5-before-revoke-as-rotated",
                            ["workspace-path", "stat", WORKBENCH, "outputs", "registered.txt"],
                            tls="rotated"))
    revoked = stack.identity("t5-revoke", "credential", "revoke", stack.tenant, added["fingerprint"])
    require(revoked.get("revoked") is True, "t5: the credential is revoked", revoked)
    time.sleep(2.5)
    envelope = stack.refused("t5-revoked", "revoked.txt", tls="rotated")
    require("not a current credential" in envelope["message"], "t5: the refusal names the credential", envelope)
    return envelope


def reader_cannot_write(stack: TlsStack) -> dict[str, Any]:
    """T6: a reader reads, and its write is refused without effect."""
    stack.success(stack.run("t6-read", ["workspace-path", "stat", WORKBENCH, "outputs", "registered.txt"],
                            tls="reader"))
    envelope = stack.refused("t6-write", "reader.txt", klass="permission_denied", tls="reader")
    require(envelope["code"] == "PermissionDenied", "t6: the refusal is PermissionDenied", envelope)
    return envelope


def two_principals_write_one_root(stack: TlsStack) -> dict[str, Any]:
    """T7: two principals write the same root. The second passes an AgentId
    the root is not bound to: over TLS the owner authorizes the principal and
    the CLI checks no AgentId binding."""
    first = stack.success(stack.put("t7-agent", WORKBENCH, "agent.txt", new_id(), b"agent"))
    arguments = stack.put_arguments("t7-peer", WORKBENCH, "peer.txt", new_id(), b"peer")
    second = stack.success(stack.run("t7-peer", arguments, tls="peer", agent_id=new_id()))
    for label, path, client in [("t7-agent-reads-peer", "peer.txt", "agent"),
                                ("t7-peer-reads-agent", "agent.txt", "peer")]:
        stat = stack.success(stack.run(label, ["workspace-path", "stat", WORKBENCH, "outputs", path],
                                       tls=client))
        require(stat.get("exists") is True, f"{label}: the other writer's path reads back", stat)
    return {"agent": first, "peer": second}


def revoked_role_stops_an_open_session(stack: TlsStack) -> dict[str, Any]:
    """T8: a role revoked while a session is open stops that session's next
    write within the owner's refresh interval."""
    argv = [str(stack.binary), *stack.route, *stack.objects,
            *stack.tls_options("session", SERVER_NAME), "workbench-session"]
    session = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                               env=stack.env)

    def call(index: int) -> dict[str, Any]:
        request = {"id": str(index), "tool": "workbench_put_file", "arguments": {
            "id": WORKBENCH, "section": "outputs", "path": f"session-{index}.txt",
            "text": f"session {index}", "replace": False}}
        session.stdin.write(json.dumps(request) + "\n")
        session.stdin.flush()
        return json.loads(session.stdout.readline())

    try:
        before = call(1)
        require("result" in before, "t8: the session writes while its role holds", before)
        stack.role("revoke", "session", "writer", ["--tenant-wide"])
        time.sleep(2.5)
        after = call(2)
    finally:
        session.stdin.close()
        session.wait(timeout=stack.timeout)
    require(after.get("class") == "permission_denied" and after.get("error", {}).get("code") == "PermissionDenied",
            "t8: the open session's next write is permission_denied", after)
    oracle = stack.stat("t8-oracle", WORKBENCH, "session-2.txt")
    require(oracle.get("exists") is False, "t8: the refused write left nothing", oracle)
    return {"before": before, "after": after}


COMMIT_ARGUMENTS = {
    "id": WORKBENCH,
    "manifest": {"dataset": "tls-gate", "task": "attribution"},
    "content_digest_uri": "sha256:" + hashlib.sha256(b"attribution").hexdigest(),
    "replace": False,
}


def tool(stack: TlsStack, label: str, name: str, arguments: dict[str, Any], **client: Any) -> Call:
    return stack.run(label, ["workbench", name, json.dumps(arguments)], **client)


def another_principal_presenting_an_operation_id_observes_or_conflicts(stack: TlsStack) -> dict[str, Any]:
    """T9: an operation id belongs to the principal that claimed it. Another
    principal presenting it with the same inputs is answered from the
    existing operation without a write, and with other inputs is refused;
    either way the path keeps the first principal's revision and the
    operation keeps its initiator."""
    operation_id = new_id()
    first = stack.success(stack.put("t9-agent", WORKBENCH, "t9.txt", operation_id, b"agent"))
    same = stack.success(stack.run(
        "t9-peer-same", stack.put_arguments("t9-peer-same", WORKBENCH, "t9.txt", operation_id, b"agent"),
        tls="peer"))
    require(same.get("commit_version") is None and same.get("artifact_revision_id") == first.get("artifact_revision_id"),
            "t9: the same inputs are answered from the existing operation, not written again", [first, same])
    other = stack.failure(stack.run(
        "t9-peer-other", stack.put_arguments("t9-peer-other", WORKBENCH, "t9.txt", operation_id, b"peer"),
        tls="peer"), "identity_conflict", "do_not_retry")
    status = stack.success(stack.run("t9-status", ["operation", "status", operation_id], tls="peer"))
    require(status.get("initiator") == stack.principals["agent"], "t9: the operation keeps its initiator", status)
    oracle = stack.assert_live("t9", WORKBENCH, "t9.txt", b"agent", first)
    return {"first": first, "same": same, "other": other, "status": status, "oracle": oracle}


def operation_status_names_the_initiator(stack: TlsStack) -> dict[str, Any]:
    """T10: an operation's status names the principal that began it, to any
    reader of the root."""
    operation_id = new_id()
    stack.success(stack.put("t10-put", WORKBENCH, "t10.txt", operation_id, b"attributed"))
    status = stack.success(stack.run("t10-status", ["operation", "status", operation_id], tls="reader"))
    require(status.get("initiator") == stack.principals["agent"], "t10: the status names the initiator", status)
    return status


def only_the_minter_or_an_operator_renews_a_snapshot(stack: TlsStack) -> dict[str, Any]:
    """T11: a snapshot is renewed or retired by the principal that minted it
    or by an operator; another writer is refused and changes nothing."""
    committed = stack.success(tool(stack, "t11-commit", "workbench_commit", COMMIT_ARGUMENTS))
    minted = stack.success(tool(stack, "t11-mint", "workbench_snapshot",
                                {"id": WORKBENCH, "name": "t11", "ttl_days": 1}))
    renew = {"id": WORKBENCH, "name": "t11", "ttl_days": 2}
    refused = stack.failure(tool(stack, "t11-peer-renew", "workbench_snapshot_renew", renew, tls="peer"),
                            "permission_denied", "operator")
    require(refused["code"] == "PermissionDenied", "t11: the refusal is PermissionDenied", refused)
    by_operator = stack.success(tool(stack, "t11-operator-renew", "workbench_snapshot_renew", renew,
                                     tls="operator"))
    by_minter = stack.success(tool(stack, "t11-agent-renew", "workbench_snapshot_renew",
                                   {**renew, "ttl_days": 3}))
    retire = {"id": WORKBENCH, "name": "t11"}
    stack.failure(tool(stack, "t11-peer-retire", "workbench_snapshot_retire", retire, tls="peer"),
                  "permission_denied", "operator")
    retired = stack.success(tool(stack, "t11-agent-retire", "workbench_snapshot_retire", retire))
    require(retired.get("retired") is True, "t11: the minter retires its snapshot", retired)
    return {"commit": committed, "minted": minted, "refused": refused, "operator": by_operator,
            "minter": by_minter, "retired": retired}


def an_identical_commit_replays_across_principals(stack: TlsStack) -> dict[str, Any]:
    """T12: a commit's identity derives from its content, so another principal
    committing the same content gets the first commit back as a replay, not
    a second commit."""
    first = stack.success(tool(stack, "t12-agent-commit", "workbench_commit", COMMIT_ARGUMENTS))
    second = stack.success(tool(stack, "t12-peer-commit", "workbench_commit", COMMIT_ARGUMENTS, tls="peer"))
    require(second.get("idempotent_replay") is True
            and second.get("commit_identity") == first.get("commit_identity"),
            "t12: the other principal's identical commit replays the first", [first, second])
    return {"first": first, "second": second}


def four_principals_write_one_root_concurrently(stack: TlsStack) -> dict[str, Any]:
    """T13: four principals write the same root at once; every write lands and
    every operation names its own principal."""
    writers = ["agent", "peer", "fourth", "operator"]

    def write(client: str) -> dict[str, Any]:
        operation_id = new_id()
        arguments = stack.put_arguments(f"t13-{client}", WORKBENCH, f"t13-{client}.txt", operation_id,
                                        client.encode())
        put = stack.success(stack.run(f"t13-{client}", arguments, tls=client))
        status = stack.success(stack.run(f"t13-{client}-status", ["operation", "status", operation_id],
                                         tls=client))
        return {"put": put, "status": status, "operation_id": operation_id}

    with ThreadPoolExecutor(max_workers=len(writers)) as pool:
        results = dict(zip(writers, pool.map(write, writers)))
    for client, result in results.items():
        require(result["status"].get("initiator") == stack.principals[client],
                f"t13: {client}'s operation names {client}", result["status"])
        stack.assert_live(f"t13-{client}", WORKBENCH, f"t13-{client}.txt", client.encode(), result["put"])
    return results


SCENARIOS: dict[str, Callable[[TlsStack], dict]] = {
    "t1_registered_client_round_trip": registered_client_round_trip,
    "t2_unregistered_certificate_refused": unregistered_certificate_refused,
    "t3_plaintext_client_refused": plaintext_client_refused,
    "t4_mismatched_owner_name_refused": mismatched_owner_name_refused,
    "t5_revoked_credential_refused": revoked_credential_refused,
    "t6_reader_cannot_write": reader_cannot_write,
    "t7_two_principals_write_one_root": two_principals_write_one_root,
    "t8_revoked_role_stops_an_open_session": revoked_role_stops_an_open_session,
    "t9_another_principal_presenting_an_operation_id_observes_or_conflicts":
        another_principal_presenting_an_operation_id_observes_or_conflicts,
    "t10_operation_status_names_the_initiator": operation_status_names_the_initiator,
    "t11_only_the_minter_or_an_operator_renews_a_snapshot": only_the_minter_or_an_operator_renews_a_snapshot,
    "t12_an_identical_commit_replays_across_principals": an_identical_commit_replays_across_principals,
    "t13_four_principals_write_one_root_concurrently": four_principals_write_one_root_concurrently,
}


def run_scenario(stack: TlsStack, scenario: Callable[[TlsStack], dict]) -> dict[str, Any]:
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
        stack = TlsStack(args, evidence)
        stack.provision()
        stack.identity("tenant", "tenant", "create", stack.tenant)
        for name, principal in stack.principals.items():
            stack.identity(f"principal-{name}", "principal", "create", stack.tenant, principal)
            stack.register(name, name)
        stack.role("grant", "agent", "writer", ["--tenant-wide"])
        stack.role("grant", "reader", "reader", ["--tenant-wide"])
        stack.role("grant", "session", "writer", ["--tenant-wide"])
        stack.role("grant", "operator", "operator", ["--tenant-wide"])
        stack.role("grant", "fourth", "writer", ["--tenant-wide"])
        report["scenarios"]["t0_owner_refuses_an_unbound_root"] = run_scenario(stack, owner_refuses_an_unbound_root)
        stack.identity("root", "root", "bind", stack.ids["root"], stack.tenant)
        report["scenarios"]["t0b_plaintext_owner_refuses_a_tenant_root"] = run_scenario(
            stack, plaintext_owner_refuses_a_tenant_root
        )
        # A root-scoped grant needs the root bound first.
        stack.role("grant", "peer", "writer", ["--root", stack.ids["root"]])
        stack.start_owner()
        for name, scenario in SCENARIOS.items():
            report["scenarios"][name] = run_scenario(stack, scenario)
        for name, result in report["scenarios"].items():
            print(f"{name}: {result['status']}", flush=True)
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
