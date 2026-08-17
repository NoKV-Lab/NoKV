"""Immutable checkpoint helpers over one path-native NoKV Workbench.

Shard artifacts are published first. The versioned ``_manifest.json`` artifact
is create-only and is the checkpoint commit point: discovery ignores every
step without that manifest. Exact response-loss replay uses deterministic SDK
operation and artifact-revision identities derived from canonical inputs.
"""

from __future__ import annotations

import hashlib
import json
from typing import Iterable, Mapping, Optional


MANIFEST_NAME = "_manifest.json"
MANIFEST_VERSION = 1
MANIFEST_SCHEMA = "nokv.workbench.checkpoint.v1"
DEFAULT_BASE = "outputs/checkpoints"


def _digest_uri(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(bytes(data)).hexdigest()


def _identity(domain: bytes, *values: str) -> str:
    hasher = hashlib.sha256()
    hasher.update(domain)
    for value in values:
        encoded = value.encode("utf-8")
        hasher.update(len(encoded).to_bytes(8, "big"))
        hasher.update(encoded)
    return hasher.digest()[:16].hex()


def _component(field: str, value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in (".", "..")
        or "/" in value
        or "\\" in value
        or "\x00" in value
    ):
        raise ValueError(f"{field} must be one non-empty path component")
    return value


def _base_prefix(base: str) -> str:
    if not isinstance(base, str) or not base or base.startswith("/") or base.endswith("/"):
        raise ValueError("checkpoint base must be a section-prefixed relative prefix")
    parts = base.split("/")
    if parts[0] not in ("input", "scripts", "outputs", "logs", "metadata"):
        raise ValueError("checkpoint base must start with a Workbench section")
    if any(part in ("", ".", "..") or "\\" in part or "\x00" in part for part in parts):
        raise ValueError("checkpoint base contains an invalid path component")
    return "/".join(parts)


def _validate_shard_name(name: str) -> None:
    _component("shard name", name)
    if name == MANIFEST_NAME:
        raise ValueError(f"shard name {name!r} is reserved for the checkpoint manifest")


def checkpoint_prefix(
    run: str, step: int, base: str = DEFAULT_BASE
) -> str:
    step = int(step)
    if step < 0:
        raise ValueError("checkpoint step must be non-negative")
    return f"{_base_prefix(base)}/{_component('run', run)}/step_{step}"


def shard_path(
    run: str, step: int, name: str, base: str = DEFAULT_BASE
) -> str:
    _validate_shard_name(name)
    return f"{checkpoint_prefix(run, step, base)}/{name}"


def publish_shard(
    client,
    workbench: str,
    run: str,
    step: int,
    name: str,
    data: bytes,
    *,
    base: str = DEFAULT_BASE,
    producer: str = "nokv-checkpoint",
    meta: Optional[Mapping] = None,
) -> dict:
    path = shard_path(run, step, name, base)
    data = bytes(data)
    body_digest = _digest_uri(data)
    identity_values = (workbench, path, body_digest)
    outcome = client.publish_bytes(
        workbench,
        path,
        data,
        content_type="application/octet-stream",
        producer=producer,
        manifest_identity=f"{run}/step_{int(step)}/{name}",
        operation_id=_identity(b"nokv.python.checkpoint.shard.operation.v1\0", *identity_values),
        artifact_revision_id=_identity(
            b"nokv.python.checkpoint.shard.revision.v1\0", *identity_values
        ),
    )
    entry = {
        "name": name,
        "path": path,
        "size": len(data),
        "generation": int(outcome["generation"]),
        "body_digest": body_digest,
    }
    if meta is not None:
        entry["meta"] = dict(meta)
    return entry


def _validated_shards(run: str, step: int, base: str, shards: Iterable[dict]) -> list[dict]:
    result = []
    names = set()
    for raw in shards:
        shard = dict(raw)
        name = shard.get("name")
        _validate_shard_name(name)
        if name in names:
            raise ValueError(f"duplicate checkpoint shard {name!r}")
        names.add(name)
        expected_path = shard_path(run, step, name, base)
        if shard.get("path") != expected_path:
            raise ValueError(f"checkpoint shard {name!r} has a path outside its step")
        if not isinstance(shard.get("size"), int) or shard["size"] < 0:
            raise ValueError(f"checkpoint shard {name!r} has an invalid size")
        if not isinstance(shard.get("generation"), int) or shard["generation"] <= 0:
            raise ValueError(f"checkpoint shard {name!r} has an invalid generation")
        digest = shard.get("body_digest")
        if (
            not isinstance(digest, str)
            or not digest.startswith("sha256:")
            or len(digest) != 71
            or any(character not in "0123456789abcdef" for character in digest[7:])
        ):
            raise ValueError(f"checkpoint shard {name!r} has an invalid body digest")
        result.append(shard)
    result.sort(key=lambda shard: shard["name"])
    return result


def commit_checkpoint(
    client,
    workbench: str,
    run: str,
    step: int,
    shards: Iterable[dict],
    *,
    base: str = DEFAULT_BASE,
    extra: Optional[Mapping] = None,
    producer: str = "nokv-checkpoint",
) -> dict:
    prefix = checkpoint_prefix(run, step, base)
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "manifest_version": MANIFEST_VERSION,
        "workbench": workbench,
        "run": run,
        "step": int(step),
        "shards": _validated_shards(run, int(step), base, shards),
        "extra": dict(extra) if extra else {},
    }
    body = json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    path = f"{prefix}/{MANIFEST_NAME}"
    body_digest = _digest_uri(body)
    identity_values = (workbench, path, body_digest)
    client.publish_bytes(
        workbench,
        path,
        body,
        content_type="application/json",
        producer=producer,
        manifest_identity=f"{run}/step_{int(step)}/manifest",
        operation_id=_identity(
            b"nokv.python.checkpoint.manifest.operation.v1\0", *identity_values
        ),
        artifact_revision_id=_identity(
            b"nokv.python.checkpoint.manifest.revision.v1\0", *identity_values
        ),
    )
    return manifest


def publish_checkpoint(
    client,
    workbench: str,
    run: str,
    step: int,
    shards: Mapping[str, bytes],
    *,
    base: str = DEFAULT_BASE,
    extra: Optional[Mapping] = None,
    producer: str = "nokv-checkpoint",
) -> dict:
    entries = [
        publish_shard(
            client, workbench, run, step, name, data, base=base, producer=producer
        )
        for name, data in shards.items()
    ]
    return commit_checkpoint(
        client,
        workbench,
        run,
        step,
        entries,
        base=base,
        extra=extra,
        producer=producer,
    )


def _all_artifacts(client, workbench: str, prefix: str, snapshot_id: Optional[int]):
    cursor = None
    read_version = None
    while True:
        page = client.list(
            workbench,
            prefix,
            True,
            cursor,
            1_000,
            read_version,
            snapshot_id,
        )
        for entry in page["entries"]:
            if entry["kind"] == "artifact":
                yield entry
        cursor = page["next_cursor"]
        if cursor is None:
            return
        read_version = int(page["read_version"])


def latest_step(
    client,
    workbench: str,
    run: str,
    *,
    base: str = DEFAULT_BASE,
    snapshot_id: Optional[int] = None,
) -> Optional[int]:
    prefix = f"{_base_prefix(base)}/{_component('run', run)}"
    committed = []
    marker = "/" + MANIFEST_NAME
    for entry in _all_artifacts(client, workbench, prefix, snapshot_id):
        path = entry["path"]
        if not path.endswith(marker):
            continue
        relative = path[len(prefix) + 1 : -len(marker)]
        if relative.startswith("step_") and relative[len("step_") :].isdigit():
            committed.append(int(relative[len("step_") :]))
    return max(committed) if committed else None


def _validate_manifest(manifest: dict, workbench: str, run: str, step: int, base: str) -> dict:
    if manifest.get("schema") != MANIFEST_SCHEMA or manifest.get("manifest_version") != 1:
        raise RuntimeError("checkpoint manifest has an unsupported schema")
    if (
        manifest.get("workbench") != workbench
        or manifest.get("run") != run
        or manifest.get("step") != step
    ):
        raise RuntimeError("checkpoint manifest identity differs from its requested path")
    manifest["shards"] = _validated_shards(run, step, base, manifest.get("shards", []))
    return manifest


def resolve_checkpoint(
    client,
    workbench: str,
    run: str,
    step: Optional[int] = None,
    *,
    base: str = DEFAULT_BASE,
    snapshot_id: Optional[int] = None,
) -> dict:
    if step is None:
        step = latest_step(
            client, workbench, run, base=base, snapshot_id=snapshot_id
        )
        if step is None:
            raise FileNotFoundError(f"no committed checkpoint for run {run!r}")
    path = f"{checkpoint_prefix(run, int(step), base)}/{MANIFEST_NAME}"
    body = client.read(workbench, path, snapshot_id)["bytes"]
    try:
        manifest = json.loads(bytes(body).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("checkpoint manifest is not canonical JSON") from error
    return _validate_manifest(manifest, workbench, run, int(step), base)


def load_shard(client, manifest: dict, name: str, *, snapshot_id: Optional[int] = None) -> bytes:
    for shard in manifest.get("shards", []):
        if shard.get("name") != name:
            continue
        outcome = client.read(manifest["workbench"], shard["path"], snapshot_id)
        metadata = outcome["metadata"]
        if (
            int(metadata["generation"]) != int(shard["generation"])
            or int(metadata["logical_size"]) != int(shard["size"])
            or metadata["body_digest"] != shard["body_digest"]
        ):
            raise RuntimeError(f"checkpoint shard {name!r} differs from its manifest")
        body = bytes(outcome["bytes"])
        if len(body) != int(shard["size"]) or _digest_uri(body) != shard["body_digest"]:
            raise RuntimeError(f"checkpoint shard {name!r} failed exact byte validation")
        return body
    raise KeyError(name)


def load_checkpoint(
    client,
    workbench: str,
    run: str,
    step: Optional[int] = None,
    *,
    base: str = DEFAULT_BASE,
    snapshot_id: Optional[int] = None,
) -> dict:
    manifest = resolve_checkpoint(
        client,
        workbench,
        run,
        step,
        base=base,
        snapshot_id=snapshot_id,
    )
    return {
        "manifest": manifest,
        "shards": {
            shard["name"]: load_shard(
                client, manifest, shard["name"], snapshot_id=snapshot_id
            )
            for shard in manifest["shards"]
        },
    }


def materialize_checkpoint(
    client,
    workbench: str,
    run: str,
    step: int,
    local_directory: str,
    *,
    base: str = DEFAULT_BASE,
    snapshot_id: Optional[int] = None,
):
    return client.materialize(
        workbench,
        local_directory,
        checkpoint_prefix(run, step, base),
        snapshot_id,
    )


def collect_checkpoint(
    client,
    local_directory: str,
    workbench: str,
    run: str,
    step: int,
    *,
    base: str = DEFAULT_BASE,
    producer: str = "nokv-checkpoint",
):
    """Collect local files as uncommitted checkpoint shards.

    The caller must validate the returned shard set and call
    :func:`commit_checkpoint`; the manifest remains the explicit commit point.
    """

    return client.collect(
        local_directory,
        workbench,
        checkpoint_prefix(run, step, base),
        producer=producer,
    )
