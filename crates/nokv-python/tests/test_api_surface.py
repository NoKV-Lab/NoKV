"""Focused Python surface checks for the Agent workspace SDK."""

import inspect

import nokv


def test_agent_workspace_surface_only():
    assert nokv.__all__ == ["Client", "RoutingConfig", "ObjectStoreConfig"]
    assert hasattr(nokv.RoutingConfig, "static")
    assert hasattr(nokv.RoutingConfig, "etcd")
    assert hasattr(nokv.Client, "create_workspace")
    assert hasattr(nokv.Client, "stat")
    assert hasattr(nokv.Client, "list")
    assert "expected_read_version" in inspect.signature(nokv.Client.list).parameters
    assert hasattr(nokv.Client, "remove")
    assert hasattr(nokv.Client, "publish_bytes")
    assert hasattr(nokv.Client, "publish_file")
    assert hasattr(nokv.Client, "read")
    assert hasattr(nokv.Client, "read_range")
    assert hasattr(nokv.Client, "search")
    assert hasattr(nokv.Client, "aggregate")
    assert hasattr(nokv.Client, "catalog")
    assert hasattr(nokv.Client, "find_workspaces")
    assert hasattr(nokv.Client, "materialize")
    assert hasattr(nokv.Client, "collect")

    for removed in (
        "NoKvFsClient",
        "RangeBatchPlan",
        "RangeBatchReader",
        "ReadBuffer",
        "checkpoint",
        "torch",
    ):
        assert not hasattr(nokv, removed)
