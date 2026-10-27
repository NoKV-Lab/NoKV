"""Non-executable, versioned metadata codec for the NoKV torch DCP adapter.

This codec deliberately accepts only PyTorch's built-in DCP metadata model and
the storage receipts produced by this adapter. It never imports a type named by
checkpoint data, and it does not read legacy pickle metadata.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache

import torch
from torch.distributed.checkpoint.metadata import (
    BytesStorageMetadata,
    ChunkStorageMetadata,
    Metadata,
    MetadataIndex,
    TensorProperties,
    TensorStorageMetadata,
)

try:
    from torch.distributed.checkpoint.metadata import StorageMeta
except ImportError:  # PyTorch 2.2 has no StorageMeta.
    @dataclass(frozen=True)
    class StorageMeta:
        checkpoint_id: str | None = None
        save_id: str | None = None
        load_id: str | None = None
        modules: list[str] | None = None

from .dcp_payload import SUPPORTED_DTYPE_NAMES


SCHEMA = "nokv.torch.dcp.metadata.v1"
MAX_METADATA_BYTES = 64 * 1024 * 1024
_MAX_DIMENSION = (1 << 63) - 1
_SHARD_NAME = re.compile(r"shard_(0|[1-9][0-9]*)\.dcp\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class ShardStorageInfo:
    path: str
    offset: int
    length: int
    generation: int
    body_digest: str
    size: int


def _invalid(reason: str) -> ValueError:
    return ValueError(f"invalid NoKV DCP metadata: {reason}")


def _object(value, fields: set[str], context: str) -> dict:
    if type(value) is not dict or set(value) != fields:
        raise _invalid(f"{context} must have exactly {sorted(fields)!r}")
    return value


def _list(value, context: str) -> list:
    if type(value) is not list:
        raise _invalid(f"{context} must be a list")
    return value


def _string(value, context: str, *, nullable: bool = False):
    if value is None and nullable:
        return None
    if type(value) is not str:
        raise _invalid(f"{context} must be a string")
    return value


def _integer(value, context: str, *, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= _MAX_DIMENSION:
        raise _invalid(
            f"{context} must be an integer from {minimum} to {_MAX_DIMENSION}"
        )
    return value


def _boolean(value, context: str) -> bool:
    if type(value) is not bool:
        raise _invalid(f"{context} must be a boolean")
    return value


def _shape(value, context: str) -> torch.Size:
    dimensions = _list(value, context)
    if len(dimensions) > 64:
        raise _invalid(f"{context} has too many dimensions")
    return torch.Size(
        [
            _integer(dimension, f"{context}[{index}]")
            for index, dimension in enumerate(dimensions)
        ]
    )


@lru_cache(maxsize=3)
def _symbols(symbol_type: type) -> dict[str, object]:
    return {
        str(value)[len("torch.") :]: value
        for value in vars(torch).values()
        if type(value) is symbol_type and str(value).startswith("torch.")
    }


def _symbol_name(value, symbol_type: type, context: str) -> str:
    if type(value) is not symbol_type:
        raise _invalid(f"{context} has an unsupported PyTorch type")
    name = str(value)[len("torch.") :]
    if _symbols(symbol_type).get(name) is not value:
        raise _invalid(f"{context} has an unsupported PyTorch value")
    return name


def _symbol(value, symbol_type: type, context: str):
    name = _string(value, context)
    result = _symbols(symbol_type).get(name)
    if result is None:
        raise _invalid(f"{context} has an unsupported PyTorch value")
    return result


def _index_document(index: MetadataIndex) -> dict:
    if type(index) is not MetadataIndex:
        raise _invalid("storage index must be a PyTorch MetadataIndex")
    return {
        "fqn": index.fqn,
        "offset": None if index.offset is None else list(index.offset),
        "index_hint": index.index,
    }


def _storage_document(info: ShardStorageInfo) -> dict:
    if type(info) is not ShardStorageInfo:
        raise _invalid("storage receipt must be a NoKV ShardStorageInfo")
    return {
        "path": info.path,
        "offset": info.offset,
        "length": info.length,
        "generation": info.generation,
        "body_digest": info.body_digest,
        "size": info.size,
    }


def _state_document(fqn: str, value) -> dict:
    if type(fqn) is not str:
        raise _invalid("state dictionary key must be a string")
    if type(value) is BytesStorageMetadata:
        return {"fqn": fqn, "kind": "bytes"}
    if (
        type(value) is not TensorStorageMetadata
        or type(value.properties) is not TensorProperties
    ):
        raise _invalid(
            f"state dictionary entry {fqn!r} has an unsupported metadata type"
        )
    properties = value.properties
    return {
        "fqn": fqn,
        "kind": "tensor",
        "properties": {
            "dtype": _symbol_name(properties.dtype, torch.dtype, "dtype"),
            "layout": _symbol_name(properties.layout, torch.layout, "layout"),
            "requires_grad": properties.requires_grad,
            "memory_format": _symbol_name(
                properties.memory_format, torch.memory_format, "memory_format"
            ),
            "pin_memory": properties.pin_memory,
        },
        "size": list(value.size),
        "chunks": [
            {"offsets": list(chunk.offsets), "sizes": list(chunk.sizes)}
            for chunk in value.chunks
        ],
    }


def _planner_document(planner_data):
    if planner_data is None:
        return None
    if type(planner_data) is not dict:
        raise _invalid("planner_data must be a mapping of flattened paths")
    result = {}
    for fqn, path in planner_data.items():
        if type(fqn) is not str or type(path) not in (tuple, list):
            raise _invalid("planner_data contains an unsupported path")
        result[fqn] = list(path)
    return result


def _storage_meta_document(storage_meta):
    if storage_meta is None:
        return None
    if type(storage_meta) is not StorageMeta:
        raise _invalid("storage_meta must be a PyTorch StorageMeta")
    return {
        "checkpoint_id": storage_meta.checkpoint_id,
        "save_id": storage_meta.save_id,
        "load_id": storage_meta.load_id,
        "modules": storage_meta.modules,
    }


def encode_metadata(metadata: Metadata) -> bytes:
    """Encode only supported DCP metadata as canonical JSON bytes."""
    if type(metadata) is not Metadata:
        raise _invalid("expected a PyTorch DCP Metadata object")
    if (
        type(metadata.state_dict_metadata) is not dict
        or type(metadata.storage_data) is not dict
    ):
        raise _invalid("state_dict_metadata and storage_data must be mappings")
    if any(type(fqn) is not str for fqn in metadata.state_dict_metadata):
        raise _invalid("state dictionary keys must be strings")
    state = [
        _state_document(fqn, value)
        for fqn, value in sorted(metadata.state_dict_metadata.items())
    ]
    storage = [
        {"index": _index_document(index), "info": _storage_document(info)}
        for index, info in metadata.storage_data.items()
    ]
    storage.sort(
        key=lambda entry: (
            entry["index"]["fqn"],
            entry["index"]["offset"] is not None,
            entry["index"]["offset"] or [],
        )
    )
    document = {
        "schema": SCHEMA,
        "metadata_version": 1,
        "state_dict_metadata": state,
        "planner_data": _planner_document(metadata.planner_data),
        "storage_data": storage,
        "storage_meta": _storage_meta_document(getattr(metadata, "storage_meta", None)),
        "dcp_version": getattr(metadata, "version", None),
    }
    try:
        body = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise _invalid("contains unsupported values") from exc
    decode_metadata(body)
    return body


def _no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _invalid(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_constant(value):
    raise _invalid(f"JSON constant {value!r} is unsupported")


def _read_state(entries: list) -> dict:
    result = {}
    for position, raw in enumerate(entries):
        fields = (
            {"fqn", "kind"}
            if type(raw) is dict and raw.get("kind") == "bytes"
            else {"fqn", "kind", "properties", "size", "chunks"}
        )
        item = _object(raw, fields, f"state_dict_metadata[{position}]")
        fqn = _string(item["fqn"], "state dictionary key")
        if fqn in result:
            raise _invalid(f"duplicate state dictionary key {fqn!r}")
        kind = _string(item["kind"], f"state dictionary entry {fqn!r} kind")
        if kind == "bytes":
            result[fqn] = BytesStorageMetadata()
            continue
        if kind != "tensor":
            raise _invalid(f"state dictionary entry {fqn!r} has an unsupported kind")
        props = _object(
            item["properties"],
            {"dtype", "layout", "requires_grad", "memory_format", "pin_memory"},
            f"tensor {fqn!r} properties",
        )
        dtype_name = _string(props["dtype"], "dtype")
        layout_name = _string(props["layout"], "layout")
        if dtype_name not in SUPPORTED_DTYPE_NAMES or layout_name != "strided":
            raise _invalid(f"tensor {fqn!r} has an unsupported dtype or layout")
        properties = TensorProperties(
            dtype=_symbol(dtype_name, torch.dtype, "dtype"),
            layout=_symbol(layout_name, torch.layout, "layout"),
            requires_grad=_boolean(props["requires_grad"], "requires_grad"),
            memory_format=_symbol(
                props["memory_format"], torch.memory_format, "memory_format"
            ),
            pin_memory=_boolean(props["pin_memory"], "pin_memory"),
        )
        size = _shape(item["size"], f"tensor {fqn!r} size")
        chunks = []
        for number, raw_chunk in enumerate(
            _list(item["chunks"], f"tensor {fqn!r} chunks")
        ):
            chunk = _object(
                raw_chunk, {"offsets", "sizes"}, f"tensor {fqn!r} chunk {number}"
            )
            offsets = _shape(chunk["offsets"], "chunk offsets")
            sizes = _shape(chunk["sizes"], "chunk sizes")
            if len(offsets) != len(size) or len(sizes) != len(size):
                raise _invalid(f"tensor {fqn!r} chunk dimensions differ from its size")
            if any(
                offset + length > bound
                for offset, length, bound in zip(offsets, sizes, size)
            ):
                raise _invalid(f"tensor {fqn!r} chunk exceeds its size")
            chunks.append(ChunkStorageMetadata(offsets=offsets, sizes=sizes))
        result[fqn] = TensorStorageMetadata(
            properties=properties, size=size, chunks=chunks
        )
    return result


def _read_planner(raw, state: dict):
    if raw is None:
        return None
    if type(raw) is not dict:
        raise _invalid("planner_data must be an object or null")
    if set(raw) != set(state):
        raise _invalid("planner_data keys differ from state dictionary keys")
    result = {}
    for fqn, raw_path in raw.items():
        path = _list(raw_path, f"planner_data[{fqn!r}]")
        if not path or len(path) > 64:
            raise _invalid(f"planner_data[{fqn!r}] has an invalid length")
        for part in path:
            if type(part) is str:
                continue
            if type(part) is int and 0 <= part <= 100_000:
                continue
            raise _invalid(f"planner_data[{fqn!r}] has an invalid component")
        if ".".join(map(str, path)) != fqn:
            raise _invalid(f"planner_data[{fqn!r}] differs from its flattened key")
        result[fqn] = tuple(path)
    return result


def _read_storage_meta(raw):
    if raw is None:
        return None
    value = _object(
        raw, {"checkpoint_id", "save_id", "load_id", "modules"}, "storage_meta"
    )
    modules = [
        _string(module, "storage module")
        for module in _list(value["modules"], "storage_meta.modules")
    ]
    return StorageMeta(
        checkpoint_id=_string(value["checkpoint_id"], "checkpoint_id", nullable=True),
        save_id=_string(value["save_id"], "save_id", nullable=True),
        load_id=_string(value["load_id"], "load_id", nullable=True),
        modules=modules,
    )


def _read_storage(entries: list, state: dict) -> dict:
    result = {}
    shard_receipts = {}
    for position, raw in enumerate(entries):
        item = _object(raw, {"index", "info"}, f"storage_data[{position}]")
        raw_index = _object(
            item["index"], {"fqn", "offset", "index_hint"}, "storage index"
        )
        fqn = _string(raw_index["fqn"], "storage index fqn")
        if fqn not in state:
            raise _invalid(f"storage index {fqn!r} has no state dictionary entry")
        offset = (
            None
            if raw_index["offset"] is None
            else _shape(raw_index["offset"], "storage index offset")
        )
        hint = raw_index["index_hint"]
        if hint is not None:
            hint = _integer(hint, "storage index hint")
        value = state[fqn]
        if type(value) is BytesStorageMetadata:
            if offset is not None or hint is not None:
                raise _invalid(f"bytes entry {fqn!r} has a tensor index")
        else:
            if offset is None or offset not in [
                chunk.offsets for chunk in value.chunks
            ]:
                raise _invalid(f"tensor entry {fqn!r} has an unknown chunk offset")
            if hint is not None and (
                hint >= len(value.chunks) or value.chunks[hint].offsets != offset
            ):
                raise _invalid(f"tensor entry {fqn!r} has an invalid index hint")
        index = MetadataIndex(fqn, offset, hint)
        if index in result:
            raise _invalid(f"duplicate storage index for {fqn!r}")
        raw_info = _object(
            item["info"],
            {"path", "offset", "length", "generation", "body_digest", "size"},
            "storage receipt",
        )
        path = _string(raw_info["path"], "shard path")
        if _SHARD_NAME.fullmatch(path) is None:
            raise _invalid("shard path must be a rank-owned shard name")
        info = ShardStorageInfo(
            path=path,
            offset=_integer(raw_info["offset"], "storage offset"),
            length=_integer(raw_info["length"], "storage length", minimum=1),
            generation=_integer(raw_info["generation"], "shard generation", minimum=1),
            body_digest=_string(raw_info["body_digest"], "shard body digest"),
            size=_integer(raw_info["size"], "shard size", minimum=1),
        )
        if _DIGEST.fullmatch(info.body_digest) is None:
            raise _invalid("shard body digest is invalid")
        if info.offset > info.size - info.length:
            raise _invalid("storage range exceeds its shard")
        receipt = (info.generation, info.body_digest, info.size)
        if path in shard_receipts and shard_receipts[path] != receipt:
            raise _invalid(f"storage receipts disagree for shard {path!r}")
        shard_receipts[path] = receipt
        result[index] = info
    return result


def decode_metadata(body: bytes) -> Metadata:
    """Reject unknown, malformed, and legacy pickle metadata without executing it."""
    if type(body) is not bytes or len(body) > MAX_METADATA_BYTES:
        raise _invalid("metadata must be bounded bytes")
    try:
        document = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_no_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError(
            "unsupported NoKV DCP metadata format; legacy pickle checkpoints are rejected"
        ) from exc
    data = _object(
        document,
        {
            "schema",
            "metadata_version",
            "state_dict_metadata",
            "planner_data",
            "storage_data",
            "storage_meta",
            "dcp_version",
        },
        "root",
    )
    if (
        data["schema"] != SCHEMA
        or type(data["metadata_version"]) is not int
        or data["metadata_version"] != 1
    ):
        raise ValueError("unsupported NoKV DCP metadata schema or version")
    state = _read_state(_list(data["state_dict_metadata"], "state_dict_metadata"))
    planner = _read_planner(data["planner_data"], state)
    storage = _read_storage(_list(data["storage_data"], "storage_data"), state)
    storage_meta = _read_storage_meta(data["storage_meta"])
    version = _string(data["dcp_version"], "dcp_version", nullable=True)
    metadata = Metadata(
        state_dict_metadata=state,
        planner_data=planner,
        storage_data=storage,
    )
    metadata.storage_meta = storage_meta
    metadata.version = version
    return metadata
