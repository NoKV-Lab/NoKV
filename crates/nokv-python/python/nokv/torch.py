"""Optional torch.distributed.checkpoint adapter for one NoKV Workbench.

Each rank publishes one immutable shard. The coordinator publishes serialized
DCP metadata last as the create-only commit point. Reads use the SDK-owned
bounded range-batch primitive and fail if any requested range is omitted.
"""

from __future__ import annotations

import io
import re
from collections import OrderedDict
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

if _HAS_DCP:
    from .dcp_metadata import ShardStorageInfo, decode_metadata, encode_metadata
    from .dcp_payload import decode_payload, encode_payload


def _torch_version() -> tuple[int, int, int]:
    """Numeric major.minor.patch prefix of ``torch.__version__``.

    Nightly, release-candidate, and local-build suffixes such as
    ``2.11.0.dev20261001``, ``2.10.0rc1``, and ``2.10.0+cpu`` are ignored.
    """
    version = re.match(r"(\d+)\.(\d+)\.(\d+)", str(torch.__version__))
    if version is None:
        raise ImportError(
            f"nokv.torch cannot parse PyTorch version {torch.__version__!r}"
        )
    return tuple(int(part) for part in version.groups())


def _require_dcp() -> None:
    if not _HAS_DCP:
        raise ImportError("torch.distributed.checkpoint is unavailable")
    if _torch_version() < (2, 2, 0):
        raise ImportError("nokv.torch requires PyTorch >=2.2.0")


def _legacy_dcp_load() -> bool:
    # PyTorch 2.2 load() gathers state_dict keys across ranks before it honours
    # no_dist, and PyTorch 2.3 load() discards nested non-tensor values after
    # the read (fixed in 2.4 by state_dict_loader assigning the loaded copy).
    # Both must load through the deprecated load_state_dict entry point.
    # From 2.4 on, load() is correct and is the only entry point that drives
    # Stateful values, so every later release uses it.
    return _torch_version() < (2, 4, 0)


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
        (
            "torch.distributed.checkpoint",
            "torch.distributed.checkpoint.planner",
            "torch.distributed.checkpoint.metadata",
        ),
    )


def _load_item_type():
    return _resolve(
        "LoadItemType",
        ("torch.distributed.checkpoint", "torch.distributed.checkpoint.planner"),
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


def _safe_payload(value, active: set[int], depth: int = 0):
    """Copy only built-in, side-effect-free DCP byte values."""
    if depth > 64:
        raise ValueError("DCP byte value is nested too deeply")
    if value is None or type(value) in (
        bool,
        int,
        float,
        complex,
        str,
        bytes,
        bytearray,
    ):
        return value
    if type(value) is torch.Tensor:
        if vars(value):
            raise ValueError("DCP byte tensor has unsupported Python attributes")
        return value
    if type(value) not in (dict, OrderedDict, list, tuple):
        raise ValueError(f"DCP byte value has unsupported type {type(value).__name__}")
    identity = id(value)
    if identity in active:
        raise ValueError("DCP byte value contains a cycle")
    active.add(identity)
    try:
        if type(value) in (dict, OrderedDict):
            result = type(value)()
            for key, item in value.items():
                if type(key) not in (bool, int, float, complex, str, bytes):
                    raise ValueError("DCP byte value has an unsupported dictionary key")
                result[key] = _safe_payload(item, active, depth + 1)
            return result
        elements = [_safe_payload(item, active, depth + 1) for item in value]
        return tuple(elements) if type(value) is tuple else elements
    finally:
        active.remove(identity)


def _load_writer_payload(data: bytes):
    # resolve_data() is supplied by the caller's local DCP planner. This is the
    # only torch.load in the adapter; no checkpoint bytes ever reach it.
    value = torch.load(io.BytesIO(data), map_location="cpu", weights_only=True)
    return _safe_payload(value, set())

def _writer_byte_value(item, planner):
    # The built-in planner already holds the original Python value. Reading
    # it directly avoids a local pickle round-trip and limits the slow safe
    # unpickler to caller-supplied planners with no standard lookup contract.
    try:
        from torch.distributed.checkpoint.default_planner import DefaultSavePlanner
    except ImportError:  # pragma: no cover - handled by older optional builds.
        DefaultSavePlanner = None
    if DefaultSavePlanner is not None and type(planner) is DefaultSavePlanner:
        return _safe_payload(planner.lookup_object(item.index), set())
    data = planner.resolve_data(item)
    return _load_writer_payload(bytes(data.getbuffer()))

def _load_default_planner_bytes(planner, read_item, value) -> bool:
    # PyTorch's built-in load_bytes() writes this value into the same planner
    # fields after torch.load(). Set the checked value directly so ordinary DCP
    # loads never create or consume pickle on the reader path.
    try:
        from torch.distributed.checkpoint.default_planner import DefaultLoadPlanner
        from torch.distributed.checkpoint._nested_dict import set_element
    except ImportError:  # pragma: no cover - handled by older optional builds.
        return False
    if type(planner) is not DefaultLoadPlanner:
        return False
    fqn = read_item.dest_index.fqn
    if planner.flatten_state_dict:
        set_element(planner.original_state_dict, planner.mappings[fqn], value)
    else:
        planner.state_dict[fqn] = value
    return True


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
            if item.type == item_type.BYTE_IO:
                value = _writer_byte_value(item, planner)
                buffer.write(encode_payload(value, "bytes"))
            else:
                data = planner.resolve_data(item)
                if not isinstance(data, torch.Tensor):
                    raise ValueError("DCP tensor item must be a torch.Tensor")
                tensor = data.detach()
                if type(tensor) is not torch.Tensor or vars(tensor):
                    raise ValueError(
                        "DCP tensor item must resolve to a plain torch.Tensor"
                    )
                buffer.write(encode_payload(tensor, "tensor"))
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
                ShardStorageInfo(
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
        body = encode_metadata(metadata)
        path = f"{checkpoint_prefix(self.run, self.step, self.base)}/{MANIFEST_NAME}"
        digest = _digest_uri(body)
        values = (self.workbench, path, digest)
        self.client.publish_bytes(
            self.workbench,
            path,
            body,
            content_type="application/json",
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
        metadata = decode_metadata(bytes(body))
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
        item_type = _load_item_type()
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
                    raise RuntimeError(
                        f"DCP storage metadata disagrees for shard {name!r}"
                    )
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
                if len(blob) != requested[index][1].length:
                    raise RuntimeError(
                        f"DCP range for {path!r} has an unexpected length"
                    )
                if read_item.type == item_type.BYTE_IO:
                    value = decode_payload(bytes(blob), "bytes")
                    if not _load_default_planner_bytes(planner, read_item, value):
                        safe_bytes = io.BytesIO()
                        # Custom planners retain their byte API. These bytes are
                        # locally serialized from the checked value, never copied
                        # from the artifact.
                        torch.save(value, safe_bytes)
                        safe_bytes.seek(0)
                        planner.load_bytes(read_item, safe_bytes)
                else:
                    tensor = decode_payload(bytes(blob), "tensor")
                    tensor = _narrow_to_read_item(tensor, read_item)
                    target = planner.resolve_tensor(read_item).detach()
                    # copy_() broadcasts, so a shard whose tensor disagrees with
                    # the read plan must fail here instead of filling the target.
                    if tuple(tensor.shape) != tuple(target.shape):
                        raise RuntimeError(
                            f"DCP tensor {read_item.dest_index.fqn!r} payload shape "
                            f"{tuple(tensor.shape)} differs from its read plan shape "
                            f"{tuple(target.shape)}"
                        )
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
        storage_writer=WorkbenchStorageWriter(client, workbench, run, step, base=base),
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

    reader = WorkbenchStorageReader(
        client,
        workbench,
        run,
        step,
        base=base,
        snapshot_id=snapshot_id,
    )
    load = dcp.load_state_dict if _legacy_dcp_load() else dcp.load
    return load(
        state_dict,
        storage_reader=reader,
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
