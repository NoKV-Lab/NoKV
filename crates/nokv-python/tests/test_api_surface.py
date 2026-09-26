"""Installed Python surface checks for the Workbench-scoped SDK."""

import inspect

import nokv


def test_versioned_workbench_surface():
    assert nokv.API_VERSION == 1
    assert nokv.__all__ == [
        "API_VERSION",
        "AppendError",
        "Client",
        "ObjectStoreConfig",
        "RoutingConfig",
        "WorkbenchFileSystem",
        "WorkspaceIncarnationMismatch",
        "checkpoint",
    ]
    assert issubclass(nokv.WorkspaceIncarnationMismatch, RuntimeError)
    assert nokv.WorkspaceIncarnationMismatch("m", "a" * 32).expected == "a" * 32
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
    assert hasattr(nokv.Client, "append_bytes")
    assert hasattr(nokv.Client, "operation_status")
    assert list(inspect.signature(nokv.Client.operation_status).parameters) == [
        "self", "operation_id"
    ]
    inspection = inspect.signature(nokv.Client.operation_inspect).parameters
    assert list(inspection) == ["self", "operation_id", "cursor", "limit"]
    assert inspection["cursor"].default is None
    assert inspection["cursor"].kind is inspect.Parameter.KEYWORD_ONLY
    assert inspection["limit"].default == 32
    recovery = inspect.signature(nokv.Client.operation_recover).parameters
    assert list(recovery) == ["self", "operation_id", "expected_state_digest"]
    assert recovery["expected_state_digest"].default is None
    assert "object_store=None" in (nokv.Client.__text_signature__ or "")
    append = inspect.signature(nokv.Client.append_bytes).parameters
    assert append["operation_id"].default is inspect.Parameter.empty
    assert append["content_type"].default is None
    assert append["expected_workspace_incarnation_id"].default is None
    assert append["max_logical_size"].default is None
    for method in (nokv.Client.publish_bytes, nokv.Client.publish_file):
        parameters = inspect.signature(method).parameters
        assert "expected_workspace_incarnation_id" in parameters
        assert parameters["expected_workspace_incarnation_id"].default is None
    assert hasattr(nokv.Client, "read")
    assert hasattr(nokv.Client, "read_range")
    assert hasattr(nokv.Client, "read_ranges_batch")
    assert "snapshot_id" in inspect.signature(nokv.Client.read).parameters
    assert "snapshot_id" in inspect.signature(nokv.Client.read_range).parameters
    assert hasattr(nokv.Client, "snapshot")
    assert hasattr(nokv.Client, "renew_snapshot")
    assert hasattr(nokv.Client, "retire_snapshot")
    assert hasattr(nokv.Client, "list_snapshots")


def test_lifecycle_surface_is_installed():
    """Commit and restore must ship in the wheel: without them a Python
    caller cannot freeze a campaign or reconstruct a decision point, and has
    to shell out to the CLI for exactly the two steps that make the record
    citable."""
    import inspect

    assert hasattr(nokv.Client, "commit")
    assert hasattr(nokv.Client, "restore")
    restore = inspect.signature(nokv.Client.restore).parameters
    # Either source can be named. A snapshot is a lease and expires; a commit
    # is durable, so a decision point that outlives the lease needs at_commit.
    assert "at_snapshot" in restore
    assert "at_commit" in restore
    commit = inspect.signature(nokv.Client.commit).parameters
    assert "replace" in commit
    # Lifecycle calls record a presentation root in the durable manifest, and
    # the client refuses to guess it. pyo3 exposes the constructor's named
    # parameters through the text signature rather than through inspect.
    assert "workbench_root" in (nokv.Client.__text_signature__ or "")
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


def test_append_error_keeps_identity_and_observed_state():
    error = nokv.AppendError("reply lost", "a" * 32, None, "AppendUnresolved", "b" * 32)
    assert isinstance(error, RuntimeError)
    assert error.operation_id == "a" * 32
    assert error.state is None
    assert error.code == "AppendUnresolved"
    assert error.expected == "b" * 32
    assert error.cause_code is None
    assert error.retryable is False
    assert error.next_action == "query_same"
    assert error.publication_operation_id is None
    assert error.expected_state_digest is None
    assert error.recovery_receipt is None
    assert str(error) == "reply lost"
    mismatch = nokv.AppendError(
        "different intent", "a" * 32, None, "AppendUnresolved", "b" * 32,
        "RequestReplayMismatch",
    )
    assert mismatch.cause_code == "RequestReplayMismatch"
    assert mismatch.code == "AppendUnresolved"
    assert mismatch.retryable is False


def test_append_provider_admission_errors_preserve_recovery_contract():
    for cause in (
        "ProviderAdmissionRejected",
        "ProviderAdmissionUnavailable",
        "ProviderAdmissionInconclusive",
    ):
        error = nokv.AppendError(
            "provider admission did not complete", "a" * 32, None,
            "AppendFailed", "b" * 32, cause,
        )
        assert error.code == "AppendFailed"
        assert error.cause_code == cause
        assert error.state is None
        assert error.operation_id == "a" * 32
        assert error.next_action == "query_same"
        assert error.publication_operation_id is None
        assert error.retryable is False


def test_cleanup_error_preserves_one_recovery_request_and_optional_receipt():
    receipt = {
        "operation_id": "a" * 32,
        "publication_operation_id": "b" * 32,
        "cleanup_retry_count": 1,
        "expected_state_digest": "c" * 64,
    }
    for known in (None, receipt):
        error = nokv.AppendError(
            "cleanup reply unknown", "a" * 32, None, "AppendCleanupUnresolved",
            None, "NotOwner", "retry_same_cleanup",
            "b" * 32 if known else None, "c" * 64, known,
        )
        assert error.code == "AppendCleanupUnresolved"
        assert error.cause_code == "NotOwner"
        assert error.operation_id == "a" * 32
        assert error.expected_state_digest == "c" * 64
        assert error.next_action == "retry_same_cleanup"
        assert error.recovery_receipt == known
        assert error.retryable is False
