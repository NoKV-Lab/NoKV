"""Workbench-scoped fsspec compatibility behavior without a native build."""

import importlib.util
import os
import sys
import types

import pytest


def _load_fsspec_module():
    stub = types.ModuleType("nokv._native")
    stub.Client = type("Client", (), {})
    pkg_dir = os.path.join(os.path.dirname(__file__), "..", "python", "nokv")
    package = types.ModuleType("nokv")
    package.__path__ = [pkg_dir]
    sys.modules["nokv"] = package
    sys.modules["nokv._native"] = stub
    spec = importlib.util.spec_from_file_location(
        "nokv.fsspec", os.path.join(pkg_dir, "fsspec.py")
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["nokv.fsspec"] = module
    spec.loader.exec_module(module)
    return module


fsspec_mod = _load_fsspec_module()


class FakeClient:
    def __init__(self):
        self.files = {}
        self.generations = {}
        self.next_generation = 1
        self.batch_requests = []

    def stat(self, workbench, path, snapshot_id=None):
        key = (workbench, path)
        if key not in self.files:
            raise FileNotFoundError(path)
        return {
            "path": path,
            "generation": self.generations[key],
            "logical_size": len(self.files[key]),
        }

    def read(self, workbench, path, snapshot_id=None):
        metadata = self.stat(workbench, path, snapshot_id)
        return {"metadata": metadata, "bytes": self.files[(workbench, path)]}

    def exists(self, workbench, path, snapshot_id=None):
        return (workbench, path) in self.files

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
        del cursor, limit, expected_read_version, snapshot_id
        entries = []
        seen = set()
        marker = prefix + "/"
        for (candidate_workbench, path), body in sorted(self.files.items()):
            if candidate_workbench != workbench or not path.startswith(marker):
                continue
            remainder = path[len(marker) :]
            if not recursive and "/" in remainder:
                child = marker + remainder.split("/", 1)[0]
                if child not in seen:
                    seen.add(child)
                    entries.append({"kind": "prefix", "path": child})
                continue
            entries.append(
                {
                    "kind": "artifact",
                    "path": path,
                    "generation": self.generations[(workbench, path)],
                    "logical_size": len(body),
                    "body_digest": "sha256:fake",
                }
            )
        return {"entries": entries, "next_cursor": None, "read_version": 7}

    def read_range(self, workbench, path, offset, length, snapshot_id=None):
        body = self.read(workbench, path, snapshot_id)["bytes"]
        if offset < 0 or length < 0 or offset + length > len(body):
            raise ValueError("range outside artifact")
        return {"metadata": self.stat(workbench, path), "bytes": body[offset : offset + length]}

    def read_ranges_batch(self, workbench, requests, snapshot_id=None):
        self.batch_requests.append((workbench, requests, snapshot_id))
        result = []
        for path, ranges, expected_generation, _max_gap_bytes in requests:
            metadata = self.stat(workbench, path, snapshot_id)
            if expected_generation is not None and metadata["generation"] != expected_generation:
                raise RuntimeError("generation conflict")
            body = self.files[(workbench, path)]
            result.append([body[offset : offset + length] for offset, length in ranges])
        return result

    def rename(self, workbench, source, destination, expected_generation):
        source_key = (workbench, source)
        destination_key = (workbench, destination)
        if self.generations.get(source_key) != expected_generation:
            raise RuntimeError("generation conflict")
        if destination_key in self.files:
            raise FileExistsError(destination)
        self.files[destination_key] = self.files.pop(source_key)
        self.generations[destination_key] = self.generations.pop(source_key)
        return {"source": source, "destination": destination}

    def remove(self, workbench, path, expected_generation):
        key = (workbench, path)
        if self.generations.get(key) != expected_generation:
            raise RuntimeError("generation conflict")
        self.files.pop(key)
        self.generations.pop(key)
        return {"removed": True}

    def publish_bytes(self, workbench, path, data, **kwargs):
        key = (workbench, path)
        expected = kwargs.get("expected_generation")
        if expected is None:
            if key in self.files:
                raise FileExistsError(path)
        elif self.generations.get(key) != expected:
            raise RuntimeError("generation conflict")
        self.files[key] = bytes(data)
        generation = self.next_generation
        self.next_generation += 1
        self.generations[key] = generation
        return {"path": path, "generation": generation, "logical_size": len(data)}


def test_read_write_and_exclusive_create_modes_are_accepted():
    for mode in ("rb", "r", "wb", "w", "xb"):
        fsspec_mod._validate_open_mode(mode)


def test_append_update_text_and_unknown_modes_are_rejected():
    for mode in (
        "ab",
        "a",
        "r+",
        "rb+",
        "w+",
        "wb+",
        "a+",
        "rt",
        "zb",
        "br",
        "rbb",
    ):
        with pytest.raises(ValueError):
            fsspec_mod._validate_open_mode(mode)


@pytest.mark.parametrize(
    "path",
    (
        "",
        "/outputs/a.bin",
        "unknown/a.bin",
        "outputs/../a.bin",
        "outputs//a.bin",
        "outputs\\a.bin",
        "outputs/outputs/a.bin",
        "outputs/a\x00bin",
    ),
)
def test_adapter_rejects_paths_outside_one_virtual_section(path):
    with pytest.raises(ValueError):
        fsspec_mod._workbench_path(path)


def test_write_read_range_and_batch_preserve_exact_order():
    client = FakeClient()
    fs = fsspec_mod.WorkbenchFileSystem(client=client, workbench="run-1")
    with fs.open("outputs/data.bin", "wb") as handle:
        handle.write(b"abcdefghij")
    assert fs.cat_file("outputs/data.bin") == b"abcdefghij"
    assert (
        fs.cat_file(
            "outputs/data.bin",
            2,
            7,
            expected_generation=1,
            max_gap_bytes=64,
        )
        == b"cdefg"
    )
    assert fs.read_ranges_batch(
        [("outputs/data.bin", [(7, 2), (0, 3)], 1, 4)]
    ) == [[b"hi", b"abc"]]
    assert client.batch_requests == [
        ("run-1", [("outputs/data.bin", [(2, 5)], 1, 64)], None),
        ("run-1", [("outputs/data.bin", [(7, 2), (0, 3)], 1, 4)], None)
    ]


def test_exclusive_create_collision_and_generation_fenced_replace():
    client = FakeClient()
    fs = fsspec_mod.WorkbenchFileSystem(client=client, workbench="run-1")
    with fs.open("outputs/data.bin", "xb") as handle:
        handle.write(b"v1")
    with pytest.raises(FileExistsError):
        with fs.open("outputs/data.bin", "xb") as handle:
            handle.write(b"collision")

    first = fs.open("outputs/data.bin", "wb")
    second = fs.open("outputs/data.bin", "wb")
    first.write(b"v2")
    first.close()
    second.write(b"stale")
    with pytest.raises(RuntimeError, match="generation conflict"):
        second.close()
    assert fs.cat_file("outputs/data.bin") == b"v2"


def test_virtual_sections_and_artifact_prefixes_have_no_durable_directory_rows():
    client = FakeClient()
    fs = fsspec_mod.WorkbenchFileSystem(client=client, workbench="run-1")
    assert fs.info("outputs") == {"name": "outputs", "size": 0, "type": "directory"}
    fs.makedirs("outputs/empty", exist_ok=True)
    assert client.files == {}
    fs.rmdir("outputs/empty")

    with fs.open("outputs/sub/data.bin", "wb") as handle:
        handle.write(b"bytes")
    assert fs.info("outputs/sub")["type"] == "directory"
    assert fs.ls("outputs", detail=False) == ["outputs/sub"]
    with pytest.raises(OSError, match="not empty"):
        fs.rmdir("outputs/sub")


def test_move_and_remove_delegate_to_generation_fenced_atomic_methods():
    client = FakeClient()
    fs = fsspec_mod.WorkbenchFileSystem(client=client, workbench="run-1")
    with fs.open("outputs/a.bin", "wb") as handle:
        handle.write(b"bytes")
    fs.mv("outputs/a.bin", "outputs/b.bin")
    assert not fs.exists("outputs/a.bin")
    assert fs.cat_file("outputs/b.bin") == b"bytes"
    fs.rm_file("outputs/b.bin")
    assert not fs.exists("outputs/b.bin")


def test_permissions_mounts_and_recursive_prefix_mutation_are_rejected():
    fs = fsspec_mod.WorkbenchFileSystem(client=FakeClient(), workbench="run-1")
    with pytest.raises(ValueError, match="mode"):
        fs.makedirs("outputs/prefix", mode=0o755)
    with pytest.raises(ValueError, match="recursive"):
        fs.rm("outputs/prefix", recursive=True)
    for name in ("chmod", "chown", "mount"):
        assert name not in type(fs).__dict__


def test_snapshot_scoped_adapter_rejects_every_mutation():
    fs = fsspec_mod.WorkbenchFileSystem(
        client=FakeClient(), workbench="run-1", snapshot_id=7
    )
    for operation in (
        lambda: fs.open("outputs/a.bin", "wb"),
        lambda: fs.makedirs("outputs/prefix", exist_ok=True),
        lambda: fs.rmdir("outputs/prefix"),
        lambda: fs.mv("outputs/a.bin", "outputs/b.bin"),
        lambda: fs.rm_file("outputs/a.bin"),
    ):
        with pytest.raises(ValueError, match="read-only"):
            operation()
