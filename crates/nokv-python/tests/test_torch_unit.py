"""Torch DCP compatibility invariants with lightweight stubs."""

from __future__ import annotations

import importlib
import importlib.util
import io
import json
import os
import pickle
import struct
import sys
import types
from collections import OrderedDict
from dataclasses import dataclass, field

import pytest


class _FakeDistributed:
    def __init__(self):
        self._available = True
        self._initialized = False
        self._rank = 0
        self._raise = None

    def is_available(self):
        return self._available

    def is_initialized(self):
        return self._initialized

    def get_rank(self):
        if self._raise is not None:
            raise self._raise
        return self._rank


def _load_torch_module():
    torch = types.ModuleType("torch")
    torch.__version__ = "2.14.1"
    torch.distributed = _FakeDistributed()
    torch.Tensor = type("Tensor", (), {})
    torch.Size = tuple
    torch.strided = object()
    for dtype in (
        "bool",
        "uint8",
        "int8",
        "int16",
        "int32",
        "int64",
        "float16",
        "bfloat16",
        "float32",
        "float64",
        "complex64",
        "complex128",
    ):
        setattr(torch, dtype, object())
    torch.load = lambda buffer, **kwargs: buffer.read()
    torch.save = lambda value, buffer: buffer.write(value)

    class Future:
        def set_result(self, value):
            self.value = value

    torch.futures = types.SimpleNamespace(Future=Future)
    sys.modules["torch"] = torch

    utils = types.ModuleType("torch.utils")
    data = types.ModuleType("torch.utils.data")
    data.Dataset = type("Dataset", (), {})
    data.IterableDataset = type("IterableDataset", (), {})
    data.get_worker_info = lambda: None
    utils.data = data
    sys.modules["torch.utils"] = utils
    sys.modules["torch.utils.data"] = data

    dcp = types.ModuleType("torch.distributed.checkpoint")
    dcp.StorageReader = type("StorageReader", (), {})
    dcp.StorageWriter = type("StorageWriter", (), {})
    dcp.WriteItemType = types.SimpleNamespace(BYTE_IO="BYTE_IO")
    dcp.LoadItemType = types.SimpleNamespace(BYTE_IO="BYTE_IO")
    dcp.WriteResult = type(
        "WriteResult",
        (),
        {
            "__init__": lambda self, index, size_in_bytes, storage_data: (
                setattr(self, "index", index),
                setattr(self, "size_in_bytes", size_in_bytes),
                setattr(self, "storage_data", storage_data),
                None,
            )[-1]
        },
    )
    sys.modules["torch.distributed.checkpoint"] = dcp
    torch.distributed.checkpoint = dcp

    metadata_module = types.ModuleType("torch.distributed.checkpoint.metadata")

    @dataclass
    class BytesStorageMetadata:
        pass

    @dataclass
    class ChunkStorageMetadata:
        offsets: tuple
        sizes: tuple

    @dataclass
    class TensorProperties:
        dtype: object = None
        layout: object = None
        requires_grad: bool = False
        memory_format: object = None
        pin_memory: bool = False

    @dataclass
    class TensorStorageMetadata:
        properties: TensorProperties
        size: tuple
        chunks: list

    @dataclass(frozen=True)
    class MetadataIndex:
        fqn: str
        offset: tuple | None = None
        index: int | None = field(default=None, compare=False, hash=False)

    @dataclass
    class StorageMeta:
        checkpoint_id: str | None = None
        save_id: str | None = None
        load_id: str | None = None
        modules: list = field(default_factory=list)

    @dataclass
    class Metadata:
        state_dict_metadata: dict
        planner_data: object = None
        storage_data: object = None
        storage_meta: object = None
        version: str | None = None

    for cls in (
        BytesStorageMetadata,
        ChunkStorageMetadata,
        TensorProperties,
        TensorStorageMetadata,
        MetadataIndex,
        StorageMeta,
        Metadata,
    ):
        setattr(metadata_module, cls.__name__, cls)
    sys.modules["torch.distributed.checkpoint.metadata"] = metadata_module

    pkg_dir = os.path.join(os.path.dirname(__file__), "..", "python", "nokv")
    package = types.ModuleType("nokv")
    package.__path__ = [pkg_dir]
    sys.modules["nokv"] = package
    checkpoint_stub = types.ModuleType("nokv.checkpoint")
    checkpoint_stub.MANIFEST_NAME = "_manifest.json"
    checkpoint_stub._digest_uri = lambda data: "sha256:" + "0" * 64
    checkpoint_stub._identity = lambda domain, *values: "0" * 32
    checkpoint_stub.publish_shard = lambda *args, **kwargs: None
    checkpoint_stub.checkpoint_prefix = (
        lambda run, step, base="outputs/checkpoints": f"{base}/{run}/step_{step}"
    )
    sys.modules["nokv.checkpoint"] = checkpoint_stub

    spec = importlib.util.spec_from_file_location(
        "nokv.torch", os.path.join(pkg_dir, "torch.py")
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["nokv.torch"] = module
    spec.loader.exec_module(module)
    return torch, module, metadata_module


torch, nokv_torch, dcp_metadata = _load_torch_module()


def test_global_rank_zero_only_before_process_group_initialization():
    assert nokv_torch._global_rank() == 0
    torch.distributed._initialized = True
    torch.distributed._rank = 4
    try:
        assert nokv_torch._global_rank() == 4
    finally:
        torch.distributed._initialized = False


def test_initialized_rank_failure_propagates_exactly():
    failure = RuntimeError("process group broken")
    torch.distributed._initialized = True
    torch.distributed._raise = failure
    try:
        with pytest.raises(RuntimeError, match="process group broken"):
            nokv_torch._global_rank()
    finally:
        torch.distributed._initialized = False
        torch.distributed._raise = None


class _Info:
    def __init__(
        self,
        path,
        offset,
        length,
        generation=1,
        body_digest="sha256:" + "0" * 64,
        size=8,
    ):
        self.path = path
        self.offset = offset
        self.length = length
        self.generation = generation
        self.body_digest = body_digest
        self.size = size


class _ReadItem:
    type = "BYTE_IO"

    def __init__(self, storage_index):
        self.storage_index = storage_index


class _Client:
    def read_ranges_batch(self, workbench, requests, snapshot_id=None):
        return [[b"short"]]


def test_short_batch_result_fails_loudly():
    reader = nokv_torch.WorkbenchStorageReader(_Client(), "wb", "run", 1)
    reader.storage_data = {
        0: _Info("rank0.dcp", 0, 4),
        1: _Info("rank0.dcp", 4, 4),
    }
    plan = types.SimpleNamespace(items=[_ReadItem(0), _ReadItem(1)])
    with pytest.raises(RuntimeError, match="1 ranges for 2 requested"):
        reader.read_data(plan, planner=None)


class _RoundTripClient:
    def __init__(self):
        self.bodies = {}
        self.generations = {}

    def publish_bytes(self, workbench, path, data, **kwargs):
        del kwargs
        self.bodies[(workbench, path)] = bytes(data)
        self.generations[(workbench, path)] = 99
        return {"generation": 99}

    def read(self, workbench, path, snapshot_id=None):
        del snapshot_id
        body = self.bodies[(workbench, path)]
        return {"bytes": body}

    def read_ranges_batch(self, workbench, requests, snapshot_id=None):
        del snapshot_id
        result = []
        for path, ranges, expected_generation, _gap in requests:
            assert self.generations[(workbench, path)] == expected_generation
            body = self.bodies[(workbench, path)]
            result.append([body[offset : offset + length] for offset, length in ranges])
        return result


class _WriteItem:
    type = "BYTE_IO"

    def __init__(self, index):
        self.index = dcp_metadata.MetadataIndex(str(index))


class _WritePlanner:
    def __init__(self, values):
        self.values = values

    def resolve_data(self, item):
        return io.BytesIO(self.values[int(item.index.fqn)])


class _ReadPlanner:
    def __init__(self):
        self.loaded = {}

    def load_bytes(self, item, buffer):
        self.loaded[int(item.storage_index.fqn)] = buffer.read()


def test_two_rank_writer_reader_roundtrip_preserves_exact_bytes(monkeypatch):
    client = _RoundTripClient()

    def publish_shard(client, workbench, run, step, name, data, **kwargs):
        del kwargs
        path = f"outputs/checkpoints/{run}/step_{step}/{name}"
        generation = len(client.generations) + 1
        client.bodies[(workbench, path)] = bytes(data)
        client.generations[(workbench, path)] = generation
        return {
            "generation": generation,
            "body_digest": f"sha256:{generation:064x}",
            "size": len(data),
        }

    monkeypatch.setattr(nokv_torch, "publish_shard", publish_shard)
    rank_results = []
    for rank, value in enumerate((b"rank-zero", b"rank-one-longer")):
        torch.distributed._initialized = True
        torch.distributed._rank = rank
        writer = nokv_torch.WorkbenchStorageWriter(client, "wb", "run", 12)
        future = writer.write_data(
            types.SimpleNamespace(items=[_WriteItem(rank)]),
            _WritePlanner({rank: value}),
        )
        rank_results.append(future.value)
    torch.distributed._initialized = False

    metadata = dcp_metadata.Metadata(
        state_dict_metadata={
            str(rank): dcp_metadata.BytesStorageMetadata() for rank in range(2)
        },
        planner_data={str(rank): (str(rank),) for rank in range(2)},
        storage_data={},
    )
    writer.finish(metadata, rank_results)
    reader = nokv_torch.WorkbenchStorageReader(client, "wb", "run", 12)
    restored_metadata = reader.read_metadata()
    reader.set_up_storage_reader(restored_metadata, True)
    planner = _ReadPlanner()
    reader.read_data(
        types.SimpleNamespace(
            items=[
                _ReadItem(dcp_metadata.MetadataIndex("0")),
                _ReadItem(dcp_metadata.MetadataIndex("1")),
            ]
        ),
        planner,
    )
    assert planner.loaded == {0: b"rank-zero", 1: b"rank-one-longer"}


def _valid_metadata_body():
    index = dcp_metadata.MetadataIndex("value")
    info = nokv_torch.ShardStorageInfo(
        path="shard_0.dcp",
        offset=0,
        length=5,
        generation=1,
        body_digest="sha256:" + "0" * 64,
        size=5,
    )
    metadata = dcp_metadata.Metadata(
        state_dict_metadata={"value": dcp_metadata.BytesStorageMetadata()},
        planner_data={"value": ("value",)},
        storage_data={index: info},
    )
    return nokv_torch.encode_metadata(metadata)


def test_metadata_is_versioned_json_and_roundtrips():
    body = _valid_metadata_body()
    assert body.startswith(b"{")
    restored = nokv_torch.decode_metadata(body)
    assert restored.planner_data == {"value": ("value",)}
    assert list(restored.storage_data.values())[0].path == "shard_0.dcp"


def test_metadata_encoding_is_independent_of_rank_result_order():
    first = dcp_metadata.Metadata(
        state_dict_metadata={
            "a": dcp_metadata.BytesStorageMetadata(),
            "b": dcp_metadata.BytesStorageMetadata(),
        },
        planner_data={"b": ("b",), "a": ("a",)},
        storage_data={
            dcp_metadata.MetadataIndex("b"): nokv_torch.ShardStorageInfo(
                "shard_1.dcp", 0, 1, 2, "sha256:" + "b" * 64, 1
            ),
            dcp_metadata.MetadataIndex("a"): nokv_torch.ShardStorageInfo(
                "shard_0.dcp", 0, 1, 1, "sha256:" + "a" * 64, 1
            ),
        },
    )
    second = dcp_metadata.Metadata(
        state_dict_metadata=dict(reversed(list(first.state_dict_metadata.items()))),
        planner_data=dict(reversed(list(first.planner_data.items()))),
        storage_data=dict(reversed(list(first.storage_data.items()))),
    )
    assert nokv_torch.encode_metadata(first) == nokv_torch.encode_metadata(second)


def test_legacy_pickle_metadata_is_rejected_without_side_effects(capsys):

    class Probe:
        def __reduce__(self):
            return (print, ("UNTRUSTED_CHECKPOINT_EXECUTED",))

    payload = pickle.dumps(Probe())
    client = _RoundTripClient()
    client.bodies[("wb", "outputs/checkpoints/run/step_1/_manifest.json")] = payload
    reader = nokv_torch.WorkbenchStorageReader(client, "wb", "run", 1)
    with pytest.raises(ValueError, match="legacy pickle checkpoints are rejected"):
        reader.read_metadata()
    assert "UNTRUSTED_CHECKPOINT_EXECUTED" not in capsys.readouterr().out


@pytest.mark.parametrize(
    "mutate",
    [
        lambda doc: doc.update(metadata_version=True),
        lambda doc: doc.update(schema="nokv.torch.dcp.metadata.v2"),
        lambda doc: doc.update(unexpected="field"),
        lambda doc: doc["planner_data"].update(value=[100_001]),
        lambda doc: doc["storage_data"][0]["info"].update(path="../other.dcp"),
        lambda doc: doc["storage_data"][0]["info"].update(length=6),
        lambda doc: doc["storage_data"][0]["index"].update(offset=[0]),
    ],
)
def test_malformed_metadata_is_rejected(mutate):
    document = json.loads(_valid_metadata_body())
    mutate(document)
    with pytest.raises(ValueError, match="(invalid|unsupported) NoKV DCP metadata"):
        nokv_torch.decode_metadata(json.dumps(document).encode())


def test_duplicate_json_keys_are_rejected():
    with pytest.raises(ValueError, match="duplicate JSON key"):
        nokv_torch.decode_metadata(b'{"schema":"x","schema":"y"}')


def test_old_pytorch_release_fails_closed(monkeypatch):
    monkeypatch.setattr(torch, "__version__", "2.1.9")
    with pytest.raises(ImportError, match="requires PyTorch >=2.2.0"):
        nokv_torch.WorkbenchStorageReader(_RoundTripClient(), "wb", "run", 1)


def test_unparseable_pytorch_version_fails_closed(monkeypatch):
    monkeypatch.setattr(torch, "__version__", "main")
    with pytest.raises(ImportError, match="cannot parse PyTorch version 'main'"):
        nokv_torch.WorkbenchStorageReader(_RoundTripClient(), "wb", "run", 1)


@pytest.mark.parametrize(
    ("version", "entry_point"),
    [
        ("2.2.2", "load_state_dict"),
        ("2.3.0", "load_state_dict"),
        ("2.3.1+cu121", "load_state_dict"),
        ("2.4.0", "load"),
        ("2.9.0", "load"),
        ("2.10.0rc1", "load"),
        ("2.10.0+cpu", "load"),
        ("2.11.0.dev20261001", "load"),
    ],
)
def test_load_checkpoint_uses_dcp_load_from_pytorch_2_4(
    monkeypatch, version, entry_point
):
    dcp = sys.modules["torch.distributed.checkpoint"]
    calls = []
    for name in ("load", "load_state_dict"):
        monkeypatch.setattr(
            dcp,
            name,
            lambda state, name=name, **kwargs: calls.append((name, kwargs)),
            raising=False,
        )
    monkeypatch.setattr(torch, "__version__", version)
    nokv_torch.load_checkpoint({}, _RoundTripClient(), "wb", "run", 1, no_dist=True)
    assert [name for name, _ in calls] == [entry_point]
    kwargs = calls[0][1]
    assert kwargs["no_dist"] is True
    assert type(kwargs["storage_reader"]) is nokv_torch.WorkbenchStorageReader


class _Shaped:
    def __init__(self, shape):
        self.shape = shape
        self.copied_from = None

    def detach(self):
        return self

    def copy_(self, other):
        self.copied_from = other


def test_tensor_payload_shape_must_equal_the_read_plan_shape(monkeypatch):
    payloads = iter((_Shaped((1, 3)), _Shaped((2, 3))))
    monkeypatch.setattr(nokv_torch, "decode_payload", lambda blob, kind: next(payloads))
    index = dcp_metadata.MetadataIndex("weight")
    item = types.SimpleNamespace(type="TENSOR", storage_index=index, dest_index=index)
    plan = types.SimpleNamespace(items=[item])
    reader = nokv_torch.WorkbenchStorageReader(_Client(), "wb", "run", 1)
    reader.storage_data = {index: _Info("shard_0.dcp", 0, 5)}
    target = _Shaped((2, 3))
    committed = []
    planner = types.SimpleNamespace(
        resolve_tensor=lambda read_item: target,
        commit_tensor=lambda read_item, tensor: committed.append(tensor),
    )
    # (1, 3) would broadcast into (2, 3) through copy_(); it must fail instead.
    with pytest.raises(
        RuntimeError,
        match=r"'weight' payload shape \(1, 3\) differs from its read plan shape \(2, 3\)",
    ):
        reader.read_data(plan, planner)
    assert target.copied_from is None and committed == []
    reader.read_data(plan, planner)
    assert target.copied_from.shape == (2, 3) and committed == [target]


def test_missing_msgpack_names_the_torch_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "msgpack", None)
    monkeypatch.delitem(sys.modules, "nokv.dcp_payload")
    with pytest.raises(
        ImportError, match=r"requires msgpack; install the nokv\[torch\] extra"
    ):
        importlib.import_module("nokv.dcp_payload")


def test_dtypes_absent_from_the_installed_torch_are_rejected():
    supported = sys.modules["nokv.dcp_payload"].SUPPORTED_DTYPE_NAMES
    # The stub torch predates uint16 and float8, like PyTorch releases before 2.3.
    assert {"uint16", "uint64", "float8_e4m3fn", "float8_e5m2"}.isdisjoint(supported)
    assert {"bool", "uint8", "bfloat16", "complex128"} <= supported
    document = {
        "schema": "nokv.torch.dcp.payload.v1",
        "kind": "tensor",
        "dtype": "float8_e4m3fn",
        "shape": [1],
        "byteorder": sys.byteorder,
    }
    with pytest.raises(ValueError, match="tensor dtype is unsupported"):
        nokv_torch.decode_payload(_payload_document(document, trailing=b"\x00"), "tensor")


def _payload_document(document, *, trailing=b""):
    header = json.dumps(document).encode()
    return b"NOKV-DCP-PAYLOAD\x01" + struct.pack("<I", len(header)) + header + trailing


def test_payload_is_versioned_and_rejects_malformed_values():
    value = OrderedDict([("step", 4), ("note", b"ok")])
    body = nokv_torch.encode_payload(value, "bytes")
    assert nokv_torch.decode_payload(body, "bytes") == value
    start = len(b"NOKV-DCP-PAYLOAD\x01") + 4
    document = json.loads(body[start:])
    for changed in (
        {**document, "schema": "nokv.torch.dcp.payload.v2"},
        {**document, "kind": "tensor"},
        {**document, "value": {"type": "callable", "value": "print"}},
        {**document, "value": {"type": "bytes", "value": "!!!"}},
    ):
        with pytest.raises(ValueError, match="invalid NoKV DCP payload"):
            nokv_torch.decode_payload(_payload_document(changed), "bytes")
    with pytest.raises(ValueError, match="trailing data"):
        nokv_torch.decode_payload(_payload_document(document, trailing=b"x"), "bytes")
    with pytest.raises(ValueError, match="duplicate JSON key"):
        header = b'{"schema":"x","schema":"y"}'
        nokv_torch.decode_payload(
            b"NOKV-DCP-PAYLOAD\x01" + struct.pack("<I", len(header)) + header,
            "bytes",
        )
    deeply_nested = b"[" * 3000 + b"0" + b"]" * 3000
    with pytest.raises(ValueError, match="invalid NoKV DCP payload"):
        nokv_torch.decode_payload(
            b"NOKV-DCP-PAYLOAD\x01"
            + struct.pack("<I", len(deeply_nested))
            + deeply_nested,
            "bytes",
        )


def test_messagepack_payload_rejects_duplicate_keys_extensions_and_trailing_data():
    body = nokv_torch.encode_payload({"step": 4, "note": b"ok"}, "bytes")
    start = len(b"NOKV-DCP-PAYLOAD\x01")
    header_length = struct.unpack_from("<I", body, start)[0]
    header_end = start + 4 + header_length
    document = json.loads(body[start + 4 : header_end])
    assert document["codec"] == "msgpack"
    assert nokv_torch.decode_payload(body, "bytes") == {"step": 4, "note": b"ok"}
    for bad in (
        b"\x82\xa1a\x01\xa1a\x02",
        b"\xd4\x01\x00",
        body[header_end:] + b"\x00",
    ):
        with pytest.raises(ValueError, match="invalid NoKV DCP payload"):
            nokv_torch.decode_payload(_payload_document(document, trailing=bad), "bytes")


@pytest.mark.parametrize("protocol", range(6))
def test_legacy_pickle_shard_protocols_are_rejected_without_execution(
    protocol, capsys
):
    class Probe:
        def __reduce__(self):
            return (print, ("UNTRUSTED_CHECKPOINT_EXECUTED",))

    body = pickle.dumps(Probe(), protocol=protocol)
    with pytest.raises(ValueError, match="unknown or legacy payload format"):
        nokv_torch.decode_payload(body, "bytes")
    assert "UNTRUSTED_CHECKPOINT_EXECUTED" not in capsys.readouterr().out


def test_deeply_nested_metadata_is_rejected_as_value_error():
    body = b"[" * 3000 + b"0" + b"]" * 3000
    with pytest.raises(ValueError, match="invalid NoKV DCP metadata|legacy pickle checkpoints are rejected"):
        nokv_torch.decode_metadata(body)
