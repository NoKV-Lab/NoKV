"""Optional torch.distributed.checkpoint adapter for one NoKV Workbench.

Each rank publishes one immutable shard. The coordinator publishes serialized
DCP metadata last as the create-only commit point. Reads use the SDK-owned
bounded range-batch primitive and fail if any requested range is omitted.
"""

from __future__ import annotations

import io
import pickle
from dataclasses import dataclass
from typing import Optional

from .checkpoint import (
    MANIFEST_NAME,
    _digest_uri,
    _identity,
    checkpoint_prefix,
    publish_shard,
)

try:
    import torch
except ImportError as exc:  # pragma: no cover - torch is optional.
    raise ImportError("nokv.torch requires PyTorch") from exc

try:
    from torch.distributed.checkpoint import StorageReader as _StorageReader
    from torch.distributed.checkpoint import StorageWriter as _StorageWriter

    _HAS_DCP = True
except ImportError:  # pragma: no cover - old torch builds may omit DCP.
    _StorageReader = object
    _StorageWriter = object
    _HAS_DCP = False


def _require_dcp() -> None:
    if not _HAS_DCP:
        raise ImportError("torch.distributed.checkpoint is unavailable")


def _resolve(name: str, modules):
    for module in modules:
        try:
            imported = __import__(module, fromlist=[name])
            return getattr(imported, name)
        except (ImportError, AttributeError):
            continue
    raise ImportError(f"could not locate torch DCP type {name!r}")


def _write_item_type():
    return _resolve(
        "WriteItemType",
        ("torch.distributed.checkpoint", "torch.distributed.checkpoint.metadata"),
    )


def _write_result_cls():
    return _resolve(
        "WriteResult",
        (
            "torch.distributed.checkpoint",
            "torch.distributed.checkpoint.storage",
            "torch.distributed.checkpoint.planner",
            "torch.distributed.checkpoint.metadata",
        ),
    )


@dataclass
class _StorageInfo:
    path: str
    offset: int
    length: int
    generation: int
    body_digest: str
    size: int


class WorkbenchStorageWriter(_StorageWriter):  # type: ignore[misc,valid-type]
    def __init__(
        self,
        client,
        workbench: str,
        run: str,
        step: int,
        *,
        base: str = "outputs/checkpoints",
    ):
        _require_dcp()
        self.client = client
        self.workbench = workbench
        self.run = run
        self.step = int(step)
        self.base = base

    @classmethod
    def validate_checkpoint_id(cls, checkpoint_id) -> bool:  # pragma: no cover
        return True

    def reset(self, checkpoint_id=None) -> None:
        del checkpoint_id

    def set_up_storage_writer(self, is_coordinator: bool) -> None:
        self._is_coordinator = bool(is_coordinator)

    def storage_meta(self):  # pragma: no cover - optional across torch versions.
        try:
            storage_meta = _resolve(
                "StorageMeta",
                (
                    "torch.distributed.checkpoint",
                    "torch.distributed.checkpoint.metadata",
                ),
            )
        except ImportError:
            return None
        identity = f"{self.workbench}/{self.run}/{self.step}"
        return storage_meta(checkpoint_id=identity, save_id=identity)

    def prepare_local_plan(self, plan):
        return plan

    def prepare_global_plan(self, plans):
        return plans

    def write_data(self, plan, planner):
        item_type = _write_item_type()
        rank = _global_rank()
        name = f"shard_{rank}.dcp"
        buffer = io.BytesIO()
        pending = []
        for item in plan.items:
            offset = buffer.tell()
            data = planner.resolve_data(item)
            if item.type == item_type.BYTE_IO:
                buffer.write(data.getbuffer())
            else:
                torch.save(data, buffer)
            pending.append((item, offset, buffer.tell() - offset))

        entry = publish_shard(
            self.client,
            self.workbench,
            self.run,
            self.step,
            name,
            buffer.getvalue(),
            base=self.base,
            producer="nokv-dcp",
        )
        results = [
            _write_result(
                item,
                length,
                _StorageInfo(
                    path=name,
                    offset=offset,
                    length=length,
                    generation=entry["generation"],
                    body_digest=entry["body_digest"],
                    size=entry["size"],
                ),
            )
            for item, offset, length in pending
        ]
        future = torch.futures.Future()
        future.set_result(results)
        return future

    def finish(self, metadata, results) -> None:
        storage_data = {}
        for rank_results in results:
            for result in rank_results:
                storage_data[result.index] = result.storage_data
        metadata.storage_data = storage_data
        body = pickle.dumps(metadata, protocol=pickle.HIGHEST_PROTOCOL)
        path = f"{checkpoint_prefix(self.run, self.step, self.base)}/{MANIFEST_NAME}"
        digest = _digest_uri(body)
        values = (self.workbench, path, digest)
        self.client.publish_bytes(
            self.workbench,
            path,
            body,
            content_type="application/x-pytorch-checkpoint",
            producer="nokv-dcp",
            manifest_identity=f"{self.run}/step_{self.step}/dcp-metadata",
            operation_id=_identity(b"nokv.python.dcp.manifest.operation.v1\0", *values),
            artifact_revision_id=_identity(
                b"nokv.python.dcp.manifest.revision.v1\0", *values
            ),
        )


class WorkbenchStorageReader(_StorageReader):  # type: ignore[misc,valid-type]
    def __init__(
        self,
        client,
        workbench: str,
        run: str,
        step: int,
        *,
        base: str = "outputs/checkpoints",
        snapshot_id: Optional[int] = None,
        max_gap_bytes: int = 0,
    ):
        _require_dcp()
        self.client = client
        self.workbench = workbench
        self.run = run
        self.step = int(step)
        self.base = base
        self.snapshot_id = snapshot_id
        self.max_gap_bytes = int(max_gap_bytes)
        self.storage_data = {}

    @classmethod
    def validate_checkpoint_id(cls, checkpoint_id) -> bool:  # pragma: no cover
        return True

    def reset(self, checkpoint_id=None) -> None:
        del checkpoint_id
        self.storage_data = {}

    def read_metadata(self):
        path = f"{checkpoint_prefix(self.run, self.step, self.base)}/{MANIFEST_NAME}"
        body = self.client.read(self.workbench, path, self.snapshot_id)["bytes"]
        metadata = pickle.loads(bytes(body))
        self.storage_data = metadata.storage_data
        return metadata

    def set_up_storage_reader(self, metadata, is_coordinator: bool) -> None:
        del is_coordinator
        self.storage_data = metadata.storage_data

    def prepare_local_plan(self, plan):
        return plan

    def prepare_global_plan(self, plans):
        return plans

    def read_data(self, plan, planner):
        item_type = _write_item_type()
        per_shard = {}
        for read_item in plan.items:
            info = self.storage_data[read_item.storage_index]
            per_shard.setdefault(info.path, []).append((read_item, info))

        for name, requested in per_shard.items():
            first = requested[0][1]
            for _, info in requested[1:]:
                if (
                    info.generation != first.generation
                    or info.body_digest != first.body_digest
                    or info.size != first.size
                ):
                    raise RuntimeError(f"DCP storage metadata disagrees for shard {name!r}")
            path = f"{checkpoint_prefix(self.run, self.step, self.base)}/{name}"
            ranges = [(int(info.offset), int(info.length)) for _, info in requested]
            batches = self.client.read_ranges_batch(
                self.workbench,
                [(path, ranges, int(first.generation), self.max_gap_bytes)],
                self.snapshot_id,
            )
            if len(batches) != 1:
                raise RuntimeError(
                    f"read_ranges_batch returned {len(batches)} artifacts for 1 requested"
                )
            blobs = batches[0]
            if len(blobs) != len(requested):
                raise RuntimeError(
                    f"read_ranges_batch returned {len(blobs)} ranges for "
                    f"{len(requested)} requested ranges of {path!r}"
                )
            for index in range(len(requested)):
                read_item, _ = requested[index]
                blob = blobs[index]
                if read_item.type == item_type.BYTE_IO:
                    planner.load_bytes(read_item, io.BytesIO(blob))
                else:
                    tensor = torch.load(io.BytesIO(blob), map_location="cpu")
                    tensor = _narrow_to_read_item(tensor, read_item)
                    target = planner.resolve_tensor(read_item).detach()
                    target.copy_(tensor)
                    planner.commit_tensor(read_item, target)

        future = torch.futures.Future()
        future.set_result(None)
        return future


def save_checkpoint(
    state_dict,
    client,
    workbench: str,
    run: str,
    step: int,
    *,
    base: str = "outputs/checkpoints",
    **kwargs,
):
    import torch.distributed.checkpoint as dcp

    return dcp.save(
        state_dict,
        storage_writer=WorkbenchStorageWriter(
            client, workbench, run, step, base=base
        ),
        **kwargs,
    )


def load_checkpoint(
    state_dict,
    client,
    workbench: str,
    run: str,
    step: int,
    *,
    base: str = "outputs/checkpoints",
    snapshot_id: Optional[int] = None,
    **kwargs,
):
    import torch.distributed.checkpoint as dcp

    return dcp.load(
        state_dict,
        storage_reader=WorkbenchStorageReader(
            client,
            workbench,
            run,
            step,
            base=base,
            snapshot_id=snapshot_id,
        ),
        **kwargs,
    )


def _global_rank() -> int:
    if not torch.distributed.is_available() or not torch.distributed.is_initialized():
        return 0
    return torch.distributed.get_rank()


def _write_result(item, length, storage_info):
    result = _write_result_cls()
    return result(index=item.index, size_in_bytes=length, storage_data=storage_info)


def _narrow_to_read_item(tensor, read_item):
    lengths = getattr(read_item, "lengths", None)
    offsets = getattr(read_item, "storage_offsets", None)
    if not lengths or not offsets:
        return tensor
    for dimension, (offset, length) in enumerate(zip(offsets, lengths)):
        tensor = tensor.narrow(dimension, int(offset), int(length))
    return tensor
