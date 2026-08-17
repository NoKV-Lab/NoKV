"""Path-native Workbench checkpoint compatibility tests."""

import importlib.util
import os

import pytest


def _load_checkpoint_module():
    path = os.path.join(
        os.path.dirname(__file__), "..", "python", "nokv", "checkpoint.py"
    )
    spec = importlib.util.spec_from_file_location("nokv_checkpoint", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


checkpoint = _load_checkpoint_module()


class FakeClient:
    def __init__(self):
        self.files = {}
        self.generations = {}
        self.operations = {}
        self.next_generation = 1
        self.snapshots = {}
        self.materialize_calls = []
        self.collect_calls = []

    def publish_bytes(self, workbench, path, data, **kwargs):
        operation_id = kwargs.get("operation_id")
        frozen = (workbench, path, bytes(data), tuple(sorted(kwargs.items())))
        if operation_id in self.operations:
            previous, outcome = self.operations[operation_id]
            if previous != frozen:
                raise RuntimeError("operation replay mismatch")
            return {**outcome, "replayed": True}
        key = (workbench, path)
        if key in self.files:
            raise FileExistsError(path)
        generation = self.next_generation
        self.next_generation += 1
        self.files[key] = bytes(data)
        self.generations[key] = generation
        outcome = {
            "path": path,
            "generation": generation,
            "body_digest": checkpoint._digest_uri(data),
            "logical_size": len(data),
            "replayed": False,
        }
        self.operations[operation_id] = (frozen, outcome)
        return outcome

    def read(self, workbench, path, snapshot_id=None):
        files, generations = self._view(snapshot_id)
        key = (workbench, path)
        if key not in files:
            raise FileNotFoundError(path)
        body = files[key]
        return {
            "metadata": {
                "path": path,
                "generation": generations[key],
                "body_digest": checkpoint._digest_uri(body),
                "logical_size": len(body),
            },
            "bytes": body,
        }

    def list(
        self,
        workbench,
        prefix=None,
        recursive=False,
        cursor=None,
        limit=1000,
        expected_read_version=None,
        snapshot_id=None,
    ):
        files, generations = self._view(snapshot_id)
        entries = []
        for (candidate_workbench, path), body in sorted(files.items()):
            if candidate_workbench != workbench or not path.startswith(prefix + "/"):
                continue
            entries.append(
                {
                    "kind": "artifact",
                    "path": path,
                    "generation": generations[(workbench, path)],
                    "body_digest": checkpoint._digest_uri(body),
                    "logical_size": len(body),
                }
            )
        return {"entries": entries, "next_cursor": None, "read_version": 7}

    def freeze(self, snapshot_id):
        self.snapshots[snapshot_id] = (dict(self.files), dict(self.generations))

    def _view(self, snapshot_id):
        return self.snapshots[snapshot_id] if snapshot_id is not None else (self.files, self.generations)

    def materialize(self, workbench, local_directory, prefix=None, snapshot_id=None):
        self.materialize_calls.append((workbench, local_directory, prefix, snapshot_id))
        return []

    def collect(self, local_directory, workbench, prefix=None, **kwargs):
        self.collect_calls.append((local_directory, workbench, prefix, kwargs))
        return []


def test_publish_resolve_load_and_highest_committed_step():
    client = FakeClient()
    checkpoint.publish_checkpoint(client, "wb", "run", 1, {"rank0": b"old"})
    checkpoint.publish_checkpoint(client, "wb", "run", 5, {"rank0": b"new"})
    checkpoint.publish_shard(client, "wb", "run", 7, "rank0", b"partial")
    assert checkpoint.latest_step(client, "wb", "run") == 5
    resolved = checkpoint.resolve_checkpoint(client, "wb", "run")
    assert resolved["step"] == 5
    loaded = checkpoint.load_checkpoint(client, "wb", "run")
    assert loaded["shards"] == {"rank0": b"new"}


def test_distributed_shards_stay_invisible_until_manifest_commit():
    client = FakeClient()
    entries = [
        checkpoint.publish_shard(client, "wb", "run", 9, f"rank{rank}", bytes([rank]))
        for rank in range(3)
    ]
    assert checkpoint.latest_step(client, "wb", "run") is None
    checkpoint.commit_checkpoint(client, "wb", "run", 9, entries)
    assert checkpoint.latest_step(client, "wb", "run") == 9
    assert checkpoint.load_checkpoint(client, "wb", "run")["shards"] == {
        "rank0": b"\x00",
        "rank1": b"\x01",
        "rank2": b"\x02",
    }


def test_invalid_shard_names_fail_before_publication():
    client = FakeClient()
    for bad in ("_manifest.json", "../escape", "a/b", ".", "..", "", "x\x00y"):
        with pytest.raises(ValueError):
            checkpoint.publish_shard(client, "wb", "run", 1, bad, b"data")
    assert client.files == {}


def test_exact_manifest_commit_replays_with_deterministic_identity():
    client = FakeClient()
    entry = checkpoint.publish_shard(client, "wb", "run", 3, "rank0", b"bytes")
    first = checkpoint.commit_checkpoint(client, "wb", "run", 3, [entry])
    second = checkpoint.commit_checkpoint(client, "wb", "run", 3, [entry])
    assert first == second


def test_distributed_manifest_identity_is_independent_of_rank_gather_order():
    client = FakeClient()
    entries = [
        checkpoint.publish_shard(client, "wb", "run", 4, "rank1", b"one"),
        checkpoint.publish_shard(client, "wb", "run", 4, "rank0", b"zero"),
    ]
    first = checkpoint.commit_checkpoint(client, "wb", "run", 4, entries)
    second = checkpoint.commit_checkpoint(client, "wb", "run", 4, reversed(entries))
    assert first == second
    assert [entry["name"] for entry in first["shards"]] == ["rank0", "rank1"]


def test_snapshot_materialize_and_collect_delegate_to_client():
    client = FakeClient()
    checkpoint.materialize_checkpoint(
        client, "wb", "run", 3, "/tmp/dest", snapshot_id=17
    )
    checkpoint.collect_checkpoint(client, "/tmp/src", "wb", "run", 4)
    assert client.materialize_calls == [
        ("wb", "/tmp/dest", "outputs/checkpoints/run/step_3", 17)
    ]
    assert client.collect_calls[0][:3] == (
        "/tmp/src",
        "wb",
        "outputs/checkpoints/run/step_4",
    )


def test_snapshot_load_stays_frozen_and_live_generation_drift_fails_loudly():
    client = FakeClient()
    checkpoint.publish_checkpoint(client, "wb", "run", 8, {"rank0": b"frozen"})
    client.freeze(17)
    key = ("wb", "outputs/checkpoints/run/step_8/rank0")
    client.files[key] = b"changed"
    client.generations[key] += 1

    assert checkpoint.load_checkpoint(
        client, "wb", "run", snapshot_id=17
    )["shards"] == {"rank0": b"frozen"}
    with pytest.raises(RuntimeError, match="differs from its manifest"):
        checkpoint.load_checkpoint(client, "wb", "run")
