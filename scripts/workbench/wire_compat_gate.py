#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Wire-compatibility matrix gate: qualify the pre-operation handshake.

Drives one bounded release-binary matrix against a real etcd and S3-compatible
object store:

  * new client <-> new server  (same-version control)
  * old client  -> new server  (a valid v2 request receives the exact
    schema-upgrade rejection, then the legacy CLI reports that rejection)
  * old client <-> old server  (same-version control)
  * new client  -> old server  (first an unadopted Agent-binding guard, then
    an adopted client that reaches the old server and observes its
    fail-closed handshake behavior)

Wire-level assertions use hand-encoded frames over a raw socket so the matrix
does not depend on internal crate APIs:

  * a ClientHello declaring a foreign schema receives an Incompatible
    handshake that advertises the server's exact schema, without any request
    dispatch;
  * a structurally valid legacy v2 request receives the exact v2
    PreconditionFailed upgrade response and leaves the server healthy.

The legacy source archive is content-pinned; the gate verifies its digest and
the declared legacy binary digest before execution, records both identities,
and verifies that neither artifact drifts during the matrix.

Every scenario writes one evidence entry; any failure exits nonzero.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import struct
import subprocess
import sys
import time

MAGIC = b"NOKVHS1\0"
KIND_CLIENT_HELLO = 1
KIND_ACCEPTED = 2
KIND_INCOMPATIBLE = 3
HANDSHAKE_PAYLOAD_BYTES = 48
LEGACY_V2_SCHEMA = "nokv.workspace.rpc.v2"
LEGACY_CLIENT_UPGRADE_MESSAGE = (
    "this NoKV client uses an unsupported workspace RPC schema; "
    "upgrade the NoKV client"
)
SHA256_HEX = re.compile(r"[0-9a-f]{64}")


def encode_handshake(kind: int, schema: str) -> bytes:
    schema_bytes = schema.encode("ascii")
    assert len(schema_bytes) <= 32, "schema must fit the fixed handshake width"
    payload = bytearray(HANDSHAKE_PAYLOAD_BYTES)
    payload[0:8] = MAGIC
    payload[8] = kind
    payload[9] = len(schema_bytes)
    payload[16 : 16 + len(schema_bytes)] = schema_bytes
    return struct.pack(">I", HANDSHAKE_PAYLOAD_BYTES) + bytes(payload)


def decode_handshake(frame: bytes) -> tuple[int, str]:
    assert len(frame) == 4 + HANDSHAKE_PAYLOAD_BYTES, "unexpected handshake frame"
    payload = frame[4:]
    assert payload[0:8] == MAGIC, "handshake magic missing"
    kind = payload[8]
    schema_len = payload[9]
    schema = payload[16 : 16 + schema_len].decode("ascii")
    return kind, schema


def encode_messagepack_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) <= 31:
        return bytes([0xA0 | len(encoded)]) + encoded
    if len(encoded) <= 0xFF:
        return b"\xD9" + bytes([len(encoded)]) + encoded
    if len(encoded) <= 0xFFFF:
        return b"\xDA" + struct.pack(">H", len(encoded)) + encoded
    raise ValueError("messagepack string is too large for the compatibility gate")


def encode_messagepack_uint(value: int) -> bytes:
    if value < 0:
        raise ValueError("messagepack unsigned integer must not be negative")
    if value <= 0x7F:
        return bytes([value])
    if value <= 0xFF:
        return b"\xCC" + bytes([value])
    if value <= 0xFFFF:
        return b"\xCD" + struct.pack(">H", value)
    if value <= 0xFFFFFFFF:
        return b"\xCE" + struct.pack(">I", value)
    if value <= 0xFFFFFFFFFFFFFFFF:
        return b"\xCF" + struct.pack(">Q", value)
    raise ValueError("messagepack unsigned integer exceeds u64")


def encode_messagepack_u8_array(value: bytes) -> bytes:
    if len(value) <= 15:
        prefix = bytes([0x90 | len(value)])
    elif len(value) <= 0xFFFF:
        prefix = b"\xDC" + struct.pack(">H", len(value))
    else:
        raise ValueError("messagepack byte array is too large for the compatibility gate")
    return prefix + b"".join(encode_messagepack_uint(item) for item in value)


def encode_messagepack_map(items: list[tuple[str, bytes]]) -> bytes:
    if len(items) > 15:
        raise ValueError("messagepack map is too large for the compatibility gate")
    return bytes([0x80 | len(items)]) + b"".join(
        encode_messagepack_string(key) + value for key, value in items
    )


def encode_legacy_v2_route(
    root_id: bytes,
    logical_shard_id: bytes,
    placement_generation: int,
    owner_epoch: int,
) -> bytes:
    if len(root_id) != 16 or len(logical_shard_id) != 16:
        raise ValueError("legacy v2 route ids must be exactly 16 bytes")
    if placement_generation == 0 or owner_epoch == 0:
        raise ValueError("legacy v2 route generation and epoch must be nonzero")
    return encode_messagepack_map([
        ("root_id", encode_messagepack_u8_array(root_id)),
        ("logical_shard_id", encode_messagepack_u8_array(logical_shard_id)),
        ("placement_generation", encode_messagepack_uint(placement_generation)),
        ("owner_epoch", encode_messagepack_uint(owner_epoch)),
    ])


def encode_legacy_v2_business_frame(
    root_id: bytes,
    logical_shard_id: bytes,
    placement_generation: int,
    owner_epoch: int,
    request_id: bytes,
) -> bytes:
    """Encode a complete public v2 CreateWorkspace request.

    The server deliberately parses only the public envelope before rejecting
    it, but every route, request-id, and operation field is valid so this is
    not a malformed-frame probe.
    """
    if len(request_id) != 16:
        raise ValueError("legacy v2 request id must be exactly 16 bytes")
    operation = encode_messagepack_map([
        ("operation", encode_messagepack_string("create_workspace")),
        ("request", encode_messagepack_map([
            ("workbench", encode_messagepack_string("wire-compat-legacy-probe")),
            ("workspace_incarnation_id", encode_messagepack_u8_array(bytes([0x55]) * 16)),
        ])),
    ])
    body = encode_messagepack_map([
        ("schema", encode_messagepack_string(LEGACY_V2_SCHEMA)),
        ("payload", encode_messagepack_map([
            ("route", encode_legacy_v2_route(
                root_id, logical_shard_id, placement_generation, owner_epoch
            )),
            ("request_id", encode_messagepack_u8_array(request_id)),
            ("operation", operation),
        ])),
    ])
    return struct.pack(">I", len(body)) + body


def encode_legacy_v2_upgrade_response(
    root_id: bytes,
    logical_shard_id: bytes,
    placement_generation: int,
    owner_epoch: int,
    request_id: bytes,
) -> bytes:
    """Encode the exact public v2 rejection emitted by the current server."""
    body = encode_messagepack_map([
        ("schema", encode_messagepack_string(LEGACY_V2_SCHEMA)),
        ("payload", encode_messagepack_map([
            ("route", encode_legacy_v2_route(
                root_id, logical_shard_id, placement_generation, owner_epoch
            )),
            ("request_id", encode_messagepack_u8_array(request_id)),
            ("commit_version", b"\xC0"),
            ("replayed", b"\xC2"),
            ("outcome", encode_messagepack_map([
                ("status", encode_messagepack_string("failure")),
                ("body", encode_messagepack_map([
                    ("code", encode_messagepack_string("precondition_failed")),
                    ("message", encode_messagepack_string(LEGACY_CLIENT_UPGRADE_MESSAGE)),
                    ("retryable", b"\xC2"),
                    ("conflict", b"\xC0"),
                    ("current_generation", b"\xC0"),
                    ("route_hint", b"\xC0"),
                ])),
            ])),
        ])),
    ])
    return struct.pack(">I", len(body)) + body


def read_exact(sock: socket.socket, count: int, timeout: float) -> bytes:
    sock.settimeout(timeout)
    chunks = []
    remaining = count
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("peer closed with zero bytes")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(sock: socket.socket, timeout: float) -> bytes:
    head = read_exact(sock, 4, timeout)
    length = struct.unpack(">I", head)[0]
    return head + read_exact(sock, length, timeout)


def run_cli(binary: str, args: list[str], timeout: float) -> tuple[int, str, str]:
    proc = subprocess.run(
        [binary, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return proc.returncode, proc.stdout, proc.stderr


def digest_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_pinned_digest(path: str, expected: str, label: str) -> str:
    if SHA256_HEX.fullmatch(expected) is None:
        raise ValueError(f"{label} digest must be 64 lowercase hexadecimal characters")
    if not os.path.isfile(path):
        raise ValueError(f"{label} is missing: {path}")
    actual = digest_file(path)
    if actual != expected:
        raise ValueError(
            f"{label} digest mismatch: expected {expected}, observed {actual}"
        )
    return actual


class Evidence:
    def __init__(self, path: str):
        self.path = path
        self.entries: list[dict] = []
        self.artifacts: dict[str, dict[str, str]] = {}

    def record(self, scenario: str, passed: bool, detail: str) -> None:
        self.entries.append(
            {"scenario": scenario, "passed": passed, "detail": detail[:2000]}
        )

    def record_artifact(
        self,
        name: str,
        path: str,
        sha256: str,
        expected_sha256: str,
    ) -> None:
        self.artifacts[name] = {
            "path": os.path.basename(path),
            "sha256": sha256,
            "expected_sha256": expected_sha256,
        }

    def payload(self) -> dict:
        return {
            "wire_compat_matrix": self.entries,
            "legacy_artifacts": self.artifacts,
        }

    def write(self) -> None:
        with open(self.path, "w", encoding="utf-8") as handle:
            json.dump(self.payload(), handle, indent=2)


def wait_for_port(port: int, seconds: float) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--new-binary", required=True)
    parser.add_argument("--old-binary", required=True)
    parser.add_argument("--legacy-source", required=True)
    parser.add_argument("--legacy-source-sha256", required=True)
    parser.add_argument("--legacy-binary-sha256", required=True)
    parser.add_argument("--etcd-endpoint", default="http://127.0.0.1:2379")
    parser.add_argument("--object-bucket", required=True)
    parser.add_argument("--object-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--object-region", default="us-east-1")
    parser.add_argument("--object-access-key-id", default="rustfsadmin")
    parser.add_argument("--object-secret-access-key", default="rustfsadmin")
    parser.add_argument("--object-root-new", required=True)
    parser.add_argument("--object-root-old", required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--port", type=int, default=7750)
    args = parser.parse_args()

    os.makedirs(args.evidence_dir, exist_ok=True)
    evidence = Evidence(os.path.join(args.evidence_dir, "wire_compat_evidence.json"))
    endpoint = f"127.0.0.1:{args.port}"

    try:
        legacy_source_sha256 = verify_pinned_digest(
            args.legacy_source,
            args.legacy_source_sha256,
            "legacy source archive",
        )
        legacy_binary_sha256 = verify_pinned_digest(
            args.old_binary,
            args.legacy_binary_sha256,
            "legacy binary",
        )
    except ValueError as error:
        evidence.record("legacy_artifact_identity", False, str(error))
        evidence.write()
        print(json.dumps(evidence.payload(), indent=2))
        return 1
    evidence.record_artifact(
        "source",
        args.legacy_source,
        legacy_source_sha256,
        args.legacy_source_sha256,
    )
    evidence.record_artifact(
        "binary",
        args.old_binary,
        legacy_binary_sha256,
        args.legacy_binary_sha256,
    )
    evidence.record(
        "legacy_artifact_identity",
        True,
        "pinned legacy source and declared legacy binary digests verified before execution",
    )

    def base_args(root_id: str, object_root: str, agent: bool) -> list[str]:
        common = [
            "--root-id", root_id,
            "--etcd-endpoint", args.etcd_endpoint,
            "--object-bucket", args.object_bucket,
            "--object-endpoint", args.object_endpoint,
            "--object-region", args.object_region,
            "--object-root", object_root,
            "--object-access-key-id", args.object_access_key_id,
            "--object-secret-access-key", args.object_secret_access_key,
        ]
        if agent:
            common += ["--agent-id", args.agent_id]
        return common

    new_root = os.urandom(16).hex()
    new_shard = os.urandom(16).hex()
    old_root = os.urandom(16).hex()
    old_shard = os.urandom(16).hex()
    server_log = os.path.join(args.evidence_dir, "server.log")

    # --- provision the new deployment and start the new server ------------
    provision = subprocess.run(
        [args.new_binary, *base_args(new_root, args.object_root_new, True), "provision", new_shard],
        capture_output=True, text=True, timeout=30,
    )
    if provision.returncode != 0:
        evidence.record("new_provision", False, provision.stderr[-400:])
        evidence.write()
        print(json.dumps(evidence.payload(), indent=2))
        return 1
    evidence.record("new_provision", True, "new deployment provisioned")

    server = subprocess.Popen(
        [args.new_binary,
         *base_args(new_root, args.object_root_new, True),
         "--node-id", "wire-compat-new",
         "--advertise-endpoint", endpoint,
         "--metadata-create", os.path.join(args.evidence_dir, "meta-new"),
         "serve"],
        stdout=open(server_log, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    if not wait_for_port(args.port, 20.0):
        evidence.record("new_server_start", False, "server never listened on the port")
        evidence.write()
        print(json.dumps(evidence.payload(), indent=2))
        return 1

    def new_find(root_id: str, object_root: str) -> tuple[int, str, str]:
        return run_cli(
            args.new_binary,
            [*base_args(root_id, object_root, True),
             "--workbench-root", "/agents/wire-compat/wb",
             "workbench", "workbench_find",
             '{"committed":null,"manifest_pattern":null,"include_manifest":false}'],
            timeout=30,
        )

    def old_find(root_id: str, object_root: str) -> tuple[int, str, str]:
        return run_cli(
            args.old_binary,
            [*base_args(root_id, object_root, False),
             "--workbench-root", "/agents/wire-compat/wb",
             "workbench", "workbench_find",
             '{"committed":null,"manifest_pattern":null,"include_manifest":false}'],
            timeout=30,
        )

    # --- 1. same-version control: new <-> new -----------------------------
    code, out, err = new_find(new_root, args.object_root_new)
    if code != 0 or "match_count" not in out:
        evidence.record("new_client_new_server", False, f"exit {code}: {out[:200]} {err[:200]}")
    else:
        evidence.record("new_client_new_server", True, "find succeeded against the new server")

    # --- 2. foreign ClientHello -> Incompatible, no dispatch ---------------
    try:
        with socket.create_connection(("127.0.0.1", args.port), timeout=5) as sock:
            sock.sendall(encode_handshake(KIND_CLIENT_HELLO, "nokv.workspace.rpc.v2"))
            frame = read_frame(sock, timeout=5)
        kind, schema = decode_handshake(frame)
        if kind != KIND_INCOMPATIBLE or schema != "nokv.workspace.rpc.v9":
            evidence.record("handshake_incompatible", False,
                            f"kind={kind} schema={schema!r}, expected Incompatible + v9")
        else:
            evidence.record("handshake_incompatible", True,
                            "foreign ClientHello answered Incompatible advertising v9")
    except Exception as error:  # noqa: BLE001 - evidence, not control flow
        evidence.record("handshake_incompatible", False, f"{type(error).__name__}: {error}")

    # --- 3. valid legacy v2 request -> exact upgrade rejection -------------
    legacy_request_id = bytes([0x44]) * 16
    legacy_root_id = bytes.fromhex(new_root)
    legacy_shard_id = bytes.fromhex(new_shard)
    legacy_request = encode_legacy_v2_business_frame(
        legacy_root_id,
        legacy_shard_id,
        placement_generation=2,
        owner_epoch=1,
        request_id=legacy_request_id,
    )
    expected_rejection = encode_legacy_v2_upgrade_response(
        legacy_root_id,
        legacy_shard_id,
        placement_generation=2,
        owner_epoch=1,
        request_id=legacy_request_id,
    )
    try:
        with socket.create_connection(("127.0.0.1", args.port), timeout=5) as sock:
            sock.sendall(legacy_request)
            frame = read_frame(sock, timeout=5)
        if frame != expected_rejection:
            evidence.record(
                "legacy_v2_request_upgrade_rejection",
                False,
                "legacy rejection differed from the exact v2 schema-upgrade response: "
                f"expected_sha256={hashlib.sha256(expected_rejection).hexdigest()} "
                f"actual_sha256={hashlib.sha256(frame).hexdigest()}",
            )
        else:
            code, out, err = new_find(new_root, args.object_root_new)
            if code != 0 or "match_count" not in out:
                evidence.record(
                    "legacy_v2_request_upgrade_rejection",
                    False,
                    f"server unhealthy after legacy rejection: exit {code}: {out[:200]} {err[:200]}",
                )
            else:
                evidence.record(
                    "legacy_v2_request_upgrade_rejection",
                    True,
                    "valid v2 request reached the server, received the exact upgrade rejection, "
                    "and left the server healthy",
                )
    except Exception as error:  # noqa: BLE001
        evidence.record(
            "legacy_v2_request_upgrade_rejection",
            False,
            f"{type(error).__name__}: {error}",
        )

    # --- 4. old CLI reaches the new server and reports its rejection -------
    code, out, err = run_cli(
        args.old_binary,
        [*base_args(new_root, args.object_root_new, False),
         "--workbench-root", "/agents/wire-compat/wb",
         "workbench", "workbench_find",
         '{"committed":null,"manifest_pattern":null,"include_manifest":false}'],
        timeout=30,
    )
    schema_upgrade = LEGACY_CLIENT_UPGRADE_MESSAGE in (out + err)
    if code == 0 or not schema_upgrade:
        evidence.record(
            "old_client_new_server",
            False,
            f"exit {code}, exact-schema-upgrade={schema_upgrade}: {(out + err)[:200]}",
        )
    else:
        evidence.record(
            "old_client_new_server",
            True,
            "old CLI reached the new server and rendered the exact schema-upgrade rejection",
        )

    server.terminate()
    server.wait(timeout=10)

    # --- 5. old <-> old control (same-version) ----------------------------
    provision_old = subprocess.run(
        [args.old_binary, *base_args(old_root, args.object_root_old, False), "provision", old_shard],
        capture_output=True, text=True, timeout=30,
    )
    if provision_old.returncode != 0:
        evidence.record("old_provision", False, provision_old.stderr[-400:])
        evidence.write()
        print(json.dumps(evidence.payload(), indent=2))
        return 1
    evidence.record("old_provision", True, "old deployment provisioned")

    server_old = subprocess.Popen(
        [args.old_binary,
         *base_args(old_root, args.object_root_old, False),
         "--node-id", "wire-compat-old",
         "--advertise-endpoint", endpoint,
         "--metadata-create", os.path.join(args.evidence_dir, "meta-old"),
         "serve"],
        stdout=open(server_log, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    if not wait_for_port(args.port, 20.0):
        evidence.record("old_server_start", False, "old server never listened on the port")
        evidence.write()
        print(json.dumps(evidence.payload(), indent=2))
        return 1

    code, out, err = old_find(old_root, args.object_root_old)
    if code != 0 or "match_count" not in out:
        evidence.record("old_client_old_server", False, f"exit {code}: {out[:200]} {err[:200]}")
    else:
        evidence.record("old_client_old_server", True, "old CLI served by the old server")

    # --- 6a. current client -> unadopted old deployment --------------------
    code, out, err = new_find(old_root, args.object_root_old)
    unadopted_guard = "no durable Agent binding" in (out + err)
    if code == 0 or not unadopted_guard:
        evidence.record(
            "new_client_old_server_unadopted_guard",
            False,
            f"exit {code}, Agent-binding-guard={unadopted_guard}: {(out + err)[:200]}",
        )
    else:
        evidence.record(
            "new_client_old_server_unadopted_guard",
            True,
            "unadopted legacy root was rejected before routing by the Agent-binding guard",
        )

    # --- 6b. adopted current client reaches the old server -----------------
    adoption = subprocess.run(
        [args.new_binary,
         *base_args(old_root, args.object_root_old, True),
         "provision", old_shard,
         "--adopt-legacy-object-namespace",
         "--adopt-legacy-agent-binding"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if adoption.returncode != 0:
        evidence.record(
            "new_client_old_server_adopted_reaches_server",
            False,
            f"legacy-root adoption failed: exit {adoption.returncode}: "
            f"{adoption.stdout[-200:]} {adoption.stderr[-200:]}",
        )
    else:
        code, out, err = new_find(old_root, args.object_root_old)
        combined = out + err
        reached_old_server = "workspace RPC handshake ended early" in combined
        if code == 0 or not reached_old_server or "no durable Agent binding" in combined:
            evidence.record(
                "new_client_old_server_adopted_reaches_server",
                False,
                f"exit {code}, handshake-ended-early={reached_old_server}: {combined[:200]}",
            )
        else:
            old_code, old_out, old_err = old_find(old_root, args.object_root_old)
            if old_code != 0 or "match_count" not in old_out:
                evidence.record(
                    "new_client_old_server_adopted_reaches_server",
                    False,
                    "old server was unhealthy after receiving the adopted current client's "
                    f"ClientHello: exit {old_code}: {old_out[:200]} {old_err[:200]}",
                )
            else:
                evidence.record(
                    "new_client_old_server_adopted_reaches_server",
                    True,
                    "adopted current client reached the old server, whose v2 decoder "
                    "fail-closed after the ClientHello while remaining healthy",
                )

    server_old.terminate()
    server_old.wait(timeout=10)

    try:
        final_source_sha256 = digest_file(args.legacy_source)
        final_binary_sha256 = digest_file(args.old_binary)
        if (
            final_source_sha256 != legacy_source_sha256
            or final_binary_sha256 != legacy_binary_sha256
        ):
            evidence.record(
                "legacy_artifact_digest_stability",
                False,
                "legacy artifacts changed during qualification: "
                f"source={final_source_sha256}, binary={final_binary_sha256}",
            )
        else:
            evidence.record(
                "legacy_artifact_digest_stability",
                True,
                "legacy source and binary digests remained unchanged throughout qualification",
            )
    except OSError as error:
        evidence.record("legacy_artifact_digest_stability", False, str(error))

    evidence.write()
    print(json.dumps(evidence.payload(), indent=2))
    return 0 if all(entry["passed"] for entry in evidence.entries) else 1


if __name__ == "__main__":
    sys.exit(main())
