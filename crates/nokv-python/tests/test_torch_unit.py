"""Torch DCP compatibility invariants with lightweight stubs."""

import importlib.util
import io
import os
import sys
import types

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
    torch.distributed = _FakeDistributed()
    torch.load = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("unexpected torch.load")
    )
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
    return torch, module


torch, nokv_torch = _load_torch_module()


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
    def __init__(self, path, offset, length, generation=1, body_digest="sha256:" + "0" * 64, size=8):
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
        self.index = index


class _WritePlanner:
    def __init__(self, values):
        self.values = values

    def resolve_data(self, item):
        return io.BytesIO(self.values[item.index])


class _ReadPlanner:
    def __init__(self):
        self.loaded = {}

    def load_bytes(self, item, buffer):
        self.loaded[item.storage_index] = buffer.read()


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

    metadata = types.SimpleNamespace(storage_data={})
    writer.finish(metadata, rank_results)
    reader = nokv_torch.WorkbenchStorageReader(client, "wb", "run", 12)
    restored_metadata = reader.read_metadata()
    reader.set_up_storage_reader(restored_metadata, True)
    planner = _ReadPlanner()
    reader.read_data(
        types.SimpleNamespace(items=[_ReadItem(0), _ReadItem(1)]), planner
    )
    assert planner.loaded == {0: b"rank-zero", 1: b"rank-one-longer"}
