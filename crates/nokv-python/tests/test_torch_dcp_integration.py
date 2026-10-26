"""Real PyTorch DCP behavior, isolated from the lightweight unit test stubs."""

import subprocess
import sys
import textwrap
import os
from pathlib import Path

import pytest


def test_real_dcp_roundtrip_and_malicious_byte_rejection(tmp_path):
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "import torch; import torch.distributed.checkpoint; "
            "assert tuple(map(int, torch.__version__.split('+')[0].split('.')[:3])) "
            ">= (2, 2, 0)",
        ],
        capture_output=True,
        text=True,
    )
    if probe.returncode != 0:
        pytest.skip("real PyTorch DCP is not installed")

    package_dir = Path(__file__).resolve().parents[1] / "python" / "nokv"
    script = textwrap.dedent(
        """
        import hashlib
        import io
        import json
        import os
        import sys
        import types
        from contextlib import redirect_stdout
        from pathlib import Path

        if os.environ.get("NOKV_TEST_INSTALLED_WHEEL") != "1":
            package = types.ModuleType("nokv")
            package.__path__ = [sys.argv[1]]
            sys.modules["nokv"] = package

        import torch
        import torch.distributed.checkpoint as dcp
        from torch.distributed.checkpoint.api import CheckpointException
        from nokv.torch import (
            WorkbenchStorageReader,
            WorkbenchStorageWriter,
            load_checkpoint,
        )
        from nokv.dcp_payload import decode_payload, encode_payload
        dcp_kwargs = (
            {"no_dist": True}
            if tuple(map(int, torch.__version__.split("+")[0].split(".")[:2])) < (2, 10)
            else {}
        )
        if dcp_kwargs:
            # PyTorch 2.2 load() gathers keys before it checks no_dist.
            torch.distributed.init_process_group(
                "gloo",
                init_method=Path(sys.argv[2]).parent.joinpath("dcp-group").as_uri(),
                rank=0,
                world_size=1,
            )
        def load_state(state, reader):
            return load_checkpoint(
                state,
                reader.client,
                reader.workbench,
                reader.run,
                reader.step,
                **dcp_kwargs,
            )

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
                    result.append([body[offset : offset + length] for offset, length in ranges])
                return result

        client = Client()
        source = {
            "model": {"weight": torch.arange(6, dtype=torch.float32).reshape(2, 3)},
            "optimizer": {"step": 4, "scale": 0.5},
            "notes": [1, "x"],
        }
        original_torch_load = torch.load
        def reject_writer_pickle(*args, **kwargs):
            raise AssertionError("the standard writer must not unpickle planner bytes")
        torch.load = reject_writer_pickle
        try:
            dcp.save(
                source,
                storage_writer=WorkbenchStorageWriter(client, "wb", "run", 1),
                **dcp_kwargs,
            )
        finally:
            torch.load = original_torch_load
        metadata_key = ("wb", "outputs/checkpoints/run/step_1/_manifest.json")
        assert json.loads(client.files[metadata_key])["schema"] == "nokv.torch.dcp.metadata.v1"
        assert client.files[("wb", "outputs/checkpoints/run/step_1/shard_0.dcp")].startswith(
            b"NOKV-DCP-PAYLOAD\\x01"
        )
        restored = {
            "model": {"weight": torch.zeros(2, 3)},
            "optimizer": {"step": 0, "scale": 0.0},
            "notes": [],
        }
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

        def poison_shard(client, run, attack_value):
            metadata_key = ("wb", f"outputs/checkpoints/{run}/step_1/_manifest.json")
            document = json.loads(client.files[metadata_key])
            info = document["storage_data"][0]["info"]
            shard_key = ("wb", f"outputs/checkpoints/{run}/step_1/{info['path']}")
            malicious = io.BytesIO()
            torch.save(attack_value, malicious)
            body = malicious.getvalue()
            client.files[shard_key] = body
            info["offset"] = 0
            info["length"] = len(body)
            info["size"] = len(body)
            info["body_digest"] = "sha256:" + hashlib.sha256(body).hexdigest()
            client.files[metadata_key] = json.dumps(document).encode()

        attack_cases = [
            ("attack_bytes", {"notes": 7}, {"notes": 0}, RejectedProbe()),
            (
                "attack_tensor",
                {"weight": torch.ones(2)},
                {"weight": torch.zeros(2)},
                RejectedProbe(),
            ),
            (
                "attack_override",
                {"weight": torch.ones(2)},
                {"weight": torch.zeros(2)},
                torch.ones(2),
            ),
        ]
        if has_safe_globals:
            attack_cases.append(
                (
                    "attack_allowed_bytes",
                    {"notes": 7},
                    {"notes": 0},
                    AllowedGlobalProbe(),
                )
            )
        for run, source, destination, attack_value in attack_cases:
            client = Client()
            dcp.save(
                source,
                storage_writer=WorkbenchStorageWriter(client, "wb", run, 1),
                **dcp_kwargs,
            )
            poison_shard(client, run, attack_value)
            captured = io.StringIO()
            with redirect_stdout(captured):
                try:
                    load_state(
                        destination, WorkbenchStorageReader(client, "wb", run, 1)
                    )
                except CheckpointException:
                    pass
                else:
                    raise AssertionError(f"malicious DCP {run} payload was accepted")
            assert "ALLOWED_GLOBAL_PROBE" not in captured.getvalue()
            assert "UNTRUSTED_CHECKPOINT_EXECUTED" not in captured.getvalue()
            if has_safe_globals:
                assert set(torch.serialization.get_safe_globals()) == safe_globals_before
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(package_dir), str(tmp_path / "executed")],
        capture_output=True,
        text=True,
        cwd=tmp_path if os.environ.get("NOKV_TEST_INSTALLED_WHEEL") == "1" else None,
    )
    assert result.returncode == 0, result.stdout + result.stderr
