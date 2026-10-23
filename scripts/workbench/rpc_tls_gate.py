#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Workspace RPC over TLS on one host.

The gate provisions one root on a `--control-dir` control store, registers a
client certificate as a credential of a tenant, and serves the root over TLS.
Before the root is bound to that tenant, the owner must refuse to serve at all.
The gate then drives the native `nokv` binary as the registered client, with an
unregistered certificate, without TLS, with a mismatched owner name, and with a
revoked credential. Each refusal must be exactly one `operator_required`
failure line, and a read-back by the registered client must show that the
refused write changed nothing. Certificates come from the `openssl` command.
"""

from __future__ import annotations

import argparse
import hashlib
import json
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
        self.clients = {name: self.pki.client(name) for name in ("agent", "stranger", "rotated")}
        self.tenant, self.principal = new_id(), new_id()

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

    def run(self, label: str, arguments: list[str], *, client: bool = True,
            tls: str | None = "agent", server_name: str = SERVER_NAME) -> Call:
        options = self.tls_options(tls, server_name) if client else []
        return super().run(label, [*options, *arguments], client=client)

    def identity(self, label: str, *arguments: str) -> dict[str, Any]:
        return self.success(self.run(label, ["identity", *arguments], client=False))

    def register(self, client: str) -> dict[str, Any]:
        certificate, _ = self.clients[client]
        return self.identity(
            f"credential-{client}", "credential", "add", self.tenant, self.principal,
            "--certificate", str(certificate), "--expires-at-unix", str(int(time.time()) + 3600),
        )

    def spawn_owner(self, log: str, *, reopen: bool) -> subprocess.Popen:
        argv = [
            str(self.binary), *self.route, *self.objects,
            "--rpc-tls-cert", str(self.pki.directory / "owner.pem"),
            "--rpc-tls-key", str(self.pki.directory / "owner.key"),
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

    def refused(self, label: str, path: str, **client: Any) -> dict[str, Any]:
        """A write by this client fails before any request and changes nothing."""
        arguments = self.put_arguments(label, WORKBENCH, path, new_id(), label.encode())
        envelope = self.failure(self.run(label, arguments, **client), "operator_required", "operator")
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
    envelope = stack.refused("t4-other-name", "other-name.txt", server_name="other.test")
    require("RPC TLS failed" in envelope["message"], "t4: the refusal names TLS", envelope)
    return envelope


def revoked_credential_refused(stack: TlsStack) -> dict[str, Any]:
    """T5: a credential works until it is revoked, and no new connection
    authenticates after that."""
    added = stack.register("rotated")
    stack.stat("t5-before-revoke", WORKBENCH, "registered.txt")
    stack.success(stack.run("t5-before-revoke-as-rotated",
                            ["workspace-path", "stat", WORKBENCH, "outputs", "registered.txt"],
                            tls="rotated"))
    revoked = stack.identity("t5-revoke", "credential", "revoke", stack.tenant, added["fingerprint"])
    require(revoked.get("revoked") is True, "t5: the credential is revoked", revoked)
    envelope = stack.refused("t5-revoked", "revoked.txt", tls="rotated")
    require("not a current credential" in envelope["message"], "t5: the refusal names the credential", envelope)
    return envelope


SCENARIOS: dict[str, Callable[[TlsStack], dict]] = {
    "t1_registered_client_round_trip": registered_client_round_trip,
    "t2_unregistered_certificate_refused": unregistered_certificate_refused,
    "t3_plaintext_client_refused": plaintext_client_refused,
    "t4_mismatched_owner_name_refused": mismatched_owner_name_refused,
    "t5_revoked_credential_refused": revoked_credential_refused,
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
        stack.identity("principal", "principal", "create", stack.tenant, stack.principal)
        stack.register("agent")
        report["scenarios"]["t0_owner_refuses_an_unbound_root"] = run_scenario(stack, owner_refuses_an_unbound_root)
        stack.identity("root", "root", "bind", stack.ids["root"], stack.tenant)
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
