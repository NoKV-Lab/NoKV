"""Real PyTorch DCP behavior, isolated from the lightweight unit test stubs.

Every test here drives PyTorch in a subprocess because test_torch_unit.py
installs a stub ``torch`` in this interpreter. A missing PyTorch skips these
tests unless ``NOKV_TEST_REQUIRE_TORCH=1`` (CI sets it); an installed PyTorch
that cannot run them is a failure, never a skip.
"""

import json
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_PACKAGE_DIR = Path(__file__).resolve().parents[1] / "python" / "nokv"
_INSTALLED_WHEEL = os.environ.get("NOKV_TEST_INSTALLED_WHEEL") == "1"
# Dtypes that older PyTorch releases lack; the adapter supports whichever the
# installed release defines.
_EXTENDED_DTYPES = (
    "uint16",
    "uint32",
    "uint64",
    "float8_e4m3fn",
    "float8_e4m3fnuz",
    "float8_e5m2",
    "float8_e5m2fnuz",
)

_PROBE = textwrap.dedent(
    r"""
    import json
    import sys

    try:
        import torch
    except ImportError as exc:
        print(json.dumps({"torch": None, "error": str(exc)}))
        sys.exit(0)
    report = {"torch": torch.__version__, "dcp_error": None}
    try:
        import torch.distributed.checkpoint
    except ImportError as exc:
        report["dcp_error"] = str(exc)
    report["dtypes"] = [
        name for name in sys.argv[1:] if getattr(torch, name, None) is not None
    ]
    print(json.dumps(report))
    """
)

_PRELUDE = textwrap.dedent(
    r"""
    import os
    import re
    import sys
    import types

    if os.environ.get("NOKV_TEST_INSTALLED_WHEEL") != "1":
        package = types.ModuleType("nokv")
        package.__path__ = [sys.argv[1]]
        sys.modules["nokv"] = package

    import torch
    import torch.distributed.checkpoint as dcp

    TORCH_VERSION = tuple(
        int(part)
        for part in re.match(r"(\d+)\.(\d+)\.(\d+)", torch.__version__).groups()
    )
    # PyTorch 2.2 and 2.3 load through the deprecated load_state_dict(), which
    # needs the explicit single-process flag; 2.4 and later load through
    # dcp.load(), which detects the missing process group itself. Only the 2.2
    # save() needs the flag: 2.3 save() rejects it and later releases detect.
    LEGACY_LOAD = TORCH_VERSION < (2, 4, 0)
    LOAD_KWARGS = {"no_dist": True} if LEGACY_LOAD else {}
    SAVE_KWARGS = {"no_dist": True} if TORCH_VERSION < (2, 3, 0) else {}


    def same_bits(actual, expected):
        return (
            actual.dtype == expected.dtype
            and actual.shape == expected.shape
            and torch.equal(
                actual.contiguous().reshape(-1).view(torch.uint8),
                expected.contiguous().reshape(-1).view(torch.uint8),
            )
        )


    class Client:
        def __init__(self):
            self.files = {}
            self.generations = {}
            self.next_generation = 1

        def publish_bytes(self, workbench, path, data, **kwargs):
            key = (workbench, path)
            if key in self.files:
                raise FileExistsError(path)
            self.files[key] = bytes(data)
            self.generations[key] = self.next_generation
            self.next_generation += 1
            return {"generation": self.generations[key]}

        def read(self, workbench, path, snapshot_id=None):
            return {"bytes": self.files[workbench, path]}

        def read_ranges_batch(self, workbench, requests, snapshot_id=None):
            result = []
            for path, ranges, generation, _max_gap in requests:
                assert self.generations[workbench, path] == generation
                body = self.files[workbench, path]
                result.append(
                    [body[offset : offset + length] for offset, length in ranges]
                )
            return result
    """
)

_ROUNDTRIP_AND_ATTACKS = textwrap.dedent(
    r"""
    import hashlib
    import io
    import json
    import warnings
    from contextlib import redirect_stdout

    from torch.distributed.checkpoint.api import CheckpointException
    from torch.distributed.checkpoint.stateful import Stateful
    from nokv.torch import (
        WorkbenchStorageReader,
        WorkbenchStorageWriter,
        load_checkpoint,
    )
    from nokv.dcp_payload import decode_payload, encode_payload

    def load_state(state, reader):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            load_checkpoint(
                state,
                reader.client,
                reader.workbench,
                reader.run,
                reader.step,
                **LOAD_KWARGS,
            )
        deprecated = [
            str(entry.message)
            for entry in caught
            if "load_state_dict" in str(entry.message)
        ]
        # From PyTorch 2.4 on, loading must go through dcp.load(), which is
        # neither deprecated nor skips Stateful values.
        assert LEGACY_LOAD or not deprecated, deprecated

    for tensor in (
        torch.arange(12, dtype=torch.float32).reshape(3, 4).T,
        torch.arange(5, dtype=torch.int64),
        torch.arange(5, dtype=torch.bfloat16),
        torch.tensor([True, False]),
        torch.tensor([1 + 2j], dtype=torch.complex64),
        torch.empty((0, 3), dtype=torch.float16),
        torch.tensor(3.5),
    ):
        restored_tensor = decode_payload(encode_payload(tensor, "tensor"), "tensor")
        assert torch.equal(restored_tensor, tensor)
        assert type(restored_tensor) is torch.Tensor

    class Counter(Stateful):
        def __init__(self, value):
            self.value = value

        def state_dict(self):
            return {"value": self.value}

        def load_state_dict(self, state_dict):
            self.value = state_dict["value"]

    client = Client()
    source = {
        "model": {"weight": torch.arange(6, dtype=torch.float32).reshape(2, 3)},
        "optimizer": {"step": 4, "scale": 0.5},
        "notes": [1, "x"],
    }
    restored = {
        "model": {"weight": torch.zeros(2, 3)},
        "optimizer": {"step": 0, "scale": 0.0},
        "notes": [],
    }
    if not LEGACY_LOAD:
        source["counter"] = Counter(7)
        restored["counter"] = Counter(0)
    original_torch_load = torch.load
    def reject_writer_pickle(*args, **kwargs):
        raise AssertionError("the standard writer must not unpickle planner bytes")
    torch.load = reject_writer_pickle
    try:
        dcp.save(
            source,
            storage_writer=WorkbenchStorageWriter(client, "wb", "run", 1),
            **SAVE_KWARGS,
        )
    finally:
        torch.load = original_torch_load
    metadata_key = ("wb", "outputs/checkpoints/run/step_1/_manifest.json")
    assert json.loads(client.files[metadata_key])["schema"] == "nokv.torch.dcp.metadata.v1"
    assert client.files[("wb", "outputs/checkpoints/run/step_1/shard_0.dcp")].startswith(
        b"NOKV-DCP-PAYLOAD\x01"
    )
    original_torch_save = torch.save
    def reject_reader_pickle(*args, **kwargs):
        raise AssertionError("the standard reader must not serialize checked values")
    torch.save = reject_reader_pickle
    try:
        load_state(restored, WorkbenchStorageReader(client, "wb", "run", 1))
    finally:
        torch.save = original_torch_save
    assert torch.equal(restored["model"]["weight"], source["model"]["weight"])
    assert restored["optimizer"] == source["optimizer"], restored
    assert restored["notes"] == source["notes"], restored
    if not LEGACY_LOAD:
        # Only dcp.load() calls load_state_dict() on Stateful values.
        assert restored["counter"].value == 7, restored["counter"].value

    saved_metadata = client.files[metadata_key]
    malformed = json.loads(saved_metadata)
    tensor_metadata = next(
        item for item in malformed["state_dict_metadata"] if item["kind"] == "tensor"
    )
    tensor_metadata["properties"]["dtype"] = "unknown.dtype"
    client.files[metadata_key] = json.dumps(malformed).encode()
    try:
        WorkbenchStorageReader(client, "wb", "run", 1).read_metadata()
    except ValueError:
        pass
    else:
        raise AssertionError("unknown tensor dtype was accepted")
    client.files[metadata_key] = saved_metadata

    class AllowedGlobalProbe:
        def __reduce__(self):
            return print, ("ALLOWED_GLOBAL_PROBE",)

    has_safe_globals = hasattr(torch.serialization, "add_safe_globals")
    if has_safe_globals:
        torch.serialization.add_safe_globals([print])
        torch.serialization.add_safe_globals(
            [(print, "torch._utils._rebuild_tensor_v2")]
        )
        safe_globals_before = set(torch.serialization.get_safe_globals())

    class RejectedProbe:
        def __reduce__(self):
            return print, ("UNTRUSTED_CHECKPOINT_EXECUTED",)

    def pickled(value):
        buffer = io.BytesIO()
        torch.save(value, buffer)
        return buffer.getvalue()

    def poison_shard(client, run, body):
        metadata_key = ("wb", f"outputs/checkpoints/{run}/step_1/_manifest.json")
        document = json.loads(client.files[metadata_key])
        info = document["storage_data"][0]["info"]
        shard_key = ("wb", f"outputs/checkpoints/{run}/step_1/{info['path']}")
        client.files[shard_key] = body
        info["offset"] = 0
        info["length"] = len(body)
        info["size"] = len(body)
        info["body_digest"] = "sha256:" + hashlib.sha256(body).hexdigest()
        client.files[metadata_key] = json.dumps(document).encode()

    legacy_format = "unknown or legacy payload format"
    attack_cases = [
        ("attack_bytes", {"notes": 7}, {"notes": 0}, pickled(RejectedProbe()), legacy_format),
        (
            "attack_tensor",
            {"weight": torch.ones(2)},
            {"weight": torch.zeros(2)},
            pickled(RejectedProbe()),
            legacy_format,
        ),
        (
            "attack_override",
            {"weight": torch.ones(2)},
            {"weight": torch.zeros(2)},
            pickled(torch.ones(2)),
            legacy_format,
        ),
        # A well-formed tensor payload whose shape disagrees with the read plan
        # must fail closed instead of being broadcast into the target.
        (
            "attack_shape",
            {"scale": torch.tensor(3.5)},
            {"scale": torch.tensor(0.0)},
            encode_payload(torch.tensor([7.0]), "tensor"),
            "payload shape (1,) differs from its read plan shape ()",
        ),
    ]
    if has_safe_globals:
        attack_cases.append(
            (
                "attack_allowed_bytes",
                {"notes": 7},
                {"notes": 0},
                pickled(AllowedGlobalProbe()),
                legacy_format,
            )
        )
    for run, source, destination, body, expected in attack_cases:
        client = Client()
        dcp.save(
            source,
            storage_writer=WorkbenchStorageWriter(client, "wb", run, 1),
            **SAVE_KWARGS,
        )
        poison_shard(client, run, body)
        captured = io.StringIO()
        with redirect_stdout(captured):
            try:
                load_state(destination, WorkbenchStorageReader(client, "wb", run, 1))
            except CheckpointException as exc:
                # Each failure is (exception, stack summary).
                messages = [str(failure[0]) for failure in exc.failures.values()]
                assert any(expected in message for message in messages), (run, messages)
            else:
                raise AssertionError(f"malicious DCP {run} payload was accepted")
        for value in destination.values():
            if isinstance(value, torch.Tensor):
                assert torch.equal(value, torch.zeros_like(value)), run
            else:
                assert value == 0, run
        assert "ALLOWED_GLOBAL_PROBE" not in captured.getvalue()
        assert "UNTRUSTED_CHECKPOINT_EXECUTED" not in captured.getvalue()
        if has_safe_globals:
            assert set(torch.serialization.get_safe_globals()) == safe_globals_before
    """
)

_EXTENDED_DTYPES_ROUNDTRIP = textwrap.dedent(
    r"""
    import json

    from nokv.torch import WorkbenchStorageWriter, load_checkpoint
    from nokv.dcp_payload import decode_payload, encode_payload

    tensors = {}
    for name in sys.argv[2:]:
        dtype = getattr(torch, name, None)
        if dtype is None:
            continue
        if name.startswith("float8"):
            tensors[name] = (torch.arange(6, dtype=torch.float32).reshape(2, 3) / 4).to(dtype)
        else:
            tensors[name] = (torch.arange(6, dtype=torch.int64).reshape(2, 3) * 37).to(dtype)
    for name, tensor in tensors.items():
        restored = decode_payload(encode_payload(tensor, "tensor"), "tensor")
        assert same_bits(restored, tensor), name

    client = Client()
    dcp.save(
        dict(tensors),
        storage_writer=WorkbenchStorageWriter(client, "wb", "dtypes", 1),
        **SAVE_KWARGS,
    )
    destination = {
        name: torch.zeros(2, 3, dtype=tensor.dtype) for name, tensor in tensors.items()
    }
    load_checkpoint(destination, client, "wb", "dtypes", 1, **LOAD_KWARGS)
    for name, tensor in tensors.items():
        assert same_bits(destination[name], tensor), name
    print(json.dumps({"covered": sorted(tensors)}))
    """
)


def _numeric_version(version: str) -> tuple:
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", version)
    if match is None:
        pytest.fail(f"cannot parse the installed PyTorch version {version!r}")
    return tuple(int(part) for part in match.groups())


def _real_torch() -> dict:
    """Probe the interpreter's PyTorch and skip only when it is absent."""
    probe = subprocess.run(
        [sys.executable, "-c", _PROBE, *_EXTENDED_DTYPES],
        capture_output=True,
        text=True,
        check=False,
    )
    assert probe.returncode == 0, probe.stdout + probe.stderr
    report = json.loads(probe.stdout.strip().splitlines()[-1])
    if report["torch"] is None:
        if os.environ.get("NOKV_TEST_REQUIRE_TORCH") == "1":
            pytest.fail(
                "NOKV_TEST_REQUIRE_TORCH=1 but PyTorch is not installed: "
                + report["error"]
            )
        pytest.skip("PyTorch is not installed")
    if report["dcp_error"] is not None:
        pytest.fail(
            f"PyTorch {report['torch']} is installed but "
            f"torch.distributed.checkpoint is unavailable: {report['dcp_error']}"
        )
    report["version"] = _numeric_version(report["torch"])
    if report["version"] < (2, 2, 0):
        pytest.fail(
            f"PyTorch {report['torch']} is installed but nokv.torch requires >=2.2.0"
        )
    return report


def _run_real_dcp(script: str, tmp_path: Path, *args: str) -> subprocess.CompletedProcess:
    result = subprocess.run(
        [sys.executable, "-c", _PRELUDE + script, str(_PACKAGE_DIR), *args],
        capture_output=True,
        text=True,
        cwd=tmp_path if _INSTALLED_WHEEL else None,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def test_real_dcp_roundtrip_and_malicious_byte_rejection(tmp_path):
    _real_torch()
    _run_real_dcp(_ROUNDTRIP_AND_ATTACKS, tmp_path)


def test_real_dcp_extended_dtypes_roundtrip(tmp_path):
    report = _real_torch()
    available = sorted(report["dtypes"])
    if not available:
        pytest.skip(
            f"PyTorch {report['torch']} defines none of {', '.join(_EXTENDED_DTYPES)}"
        )
    if report["version"] >= (2, 3, 0):
        # Every extended dtype exists from PyTorch 2.3 on; a missing one here
        # would be a probe defect rather than a reason to skip.
        assert available == sorted(_EXTENDED_DTYPES), available
    result = _run_real_dcp(_EXTENDED_DTYPES_ROUNDTRIP, tmp_path, *_EXTENDED_DTYPES)
    covered = json.loads(result.stdout.strip().splitlines()[-1])["covered"]
    assert covered == available
