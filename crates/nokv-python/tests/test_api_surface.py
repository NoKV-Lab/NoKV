"""Installed Python surface checks for the Workbench-scoped SDK."""

import inspect

import nokv


def test_versioned_workbench_surface():
    assert nokv.API_VERSION == 1
    assert nokv.__all__ == [
        "API_VERSION",
        "Client",
        "ObjectStoreConfig",
        "RoutingConfig",
        "WorkbenchFileSystem",
        "checkpoint",
    ]
    assert hasattr(nokv.RoutingConfig, "static")
    assert hasattr(nokv.RoutingConfig, "etcd")
    assert hasattr(nokv.Client, "create_workspace")
    assert hasattr(nokv.Client, "stat")
    assert hasattr(nokv.Client, "list")
    assert "expected_read_version" in inspect.signature(nokv.Client.list).parameters
    assert hasattr(nokv.Client, "exists")
    assert hasattr(nokv.Client, "remove")
    assert hasattr(nokv.Client, "rename")
    assert hasattr(nokv.Client, "publish_bytes")
    assert hasattr(nokv.Client, "publish_file")
    assert hasattr(nokv.Client, "read")
    assert hasattr(nokv.Client, "read_range")
    assert hasattr(nokv.Client, "read_ranges_batch")
    assert "snapshot_id" in inspect.signature(nokv.Client.read).parameters
    assert "snapshot_id" in inspect.signature(nokv.Client.read_range).parameters
    assert hasattr(nokv.Client, "snapshot")
    assert hasattr(nokv.Client, "renew_snapshot")
    assert hasattr(nokv.Client, "retire_snapshot")
    assert hasattr(nokv.Client, "list_snapshots")
    assert hasattr(nokv.Client, "search")
    assert hasattr(nokv.Client, "aggregate")
    assert hasattr(nokv.Client, "catalog")
    assert hasattr(nokv.Client, "find_workspaces")
    assert hasattr(nokv.Client, "materialize")
    assert hasattr(nokv.Client, "collect")

def test_retired_filesystem_types_stay_absent():
    for removed in (
        "NoKvFsClient",
        "NoKVFileSystem",
        "RangeBatchPlan",
        "RangeBatchReader",
        "ReadBuffer",
    ):
        assert not hasattr(nokv, removed)


def test_torch_adapter_is_lazy_and_optional():
    assert "torch" not in nokv.__all__
