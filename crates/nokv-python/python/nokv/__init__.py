"""Versioned Python SDK for NoKV Workbenches and immutable artifacts."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as _distribution_version

from . import checkpoint
from ._native import Client, RoutingConfig, ObjectStoreConfig
from .fsspec import WorkbenchFileSystem

API_VERSION = 1

try:
    # The installed distribution version is the NoKV release line the wheel
    # was built from (for example "0.11.0"); it is distinct from API_VERSION,
    # which only changes when the Python surface changes incompatibly.
    __version__ = _distribution_version("nokv")
except PackageNotFoundError:  # pragma: no cover - source tree without metadata
    __version__ = "0+unknown"

__all__ = [
    "API_VERSION",
    "AppendError",
    "Client",
    "ObjectStoreConfig",
    "RoutingConfig",
    "WorkbenchFileSystem",
    "WorkspaceIncarnationMismatch",
    "checkpoint",
]


class AppendError(RuntimeError):
    """An append failed or needs recovery under its original operation identity.

    ``state`` is the observed durable operation state, or ``None`` when it is
    unknown. Neither a timeout nor an unknown state proves that nothing was
    published. Retry the same intent with the same ``operation_id``; do not
    generate a replacement identity to bypass an unresolved operation. Query
    ``Client.operation_status`` and follow its ``next_action``; the exception's
    ``next_action`` is conservatively ``query_same`` for append errors.
    ``AppendCleanupUnresolved`` instead supplies ``retry_same_cleanup`` and an
    ``expected_state_digest``: pass that same digest to ``operation_recover``
    after an uncertain reply. An already observed ``recovery_receipt`` and its
    publication identity are retained even when the later status query fails.
    Without an observed receipt, the publication identity remains ``None``.
    ``cause_code`` preserves the underlying RPC code, for example
    ``RequestReplayMismatch`` for a permanently mismatched intent, or ``None``
    when there is no RPC failure. ``retryable`` is always false: generic retry
    handlers must not turn an unresolved operation into a new action.
    """

    def __init__(
        self,
        message: str,
        operation_id: str,
        state: str | None,
        code: str,
        expected: str | None,
        cause_code: str | None = None,
        next_action: str = "query_same",
        publication_operation_id: str | None = None,
        expected_state_digest: str | None = None,
        recovery_receipt: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.operation_id = operation_id
        self.state = state
        self.code = code
        self.expected = expected
        self.cause_code = cause_code
        self.retryable = False
        self.next_action = next_action
        self.publication_operation_id = publication_operation_id
        self.expected_state_digest = expected_state_digest
        self.recovery_receipt = recovery_receipt


class WorkspaceIncarnationMismatch(RuntimeError):
    """A publish was refused because the workbench is not the expected incarnation.

    Raised by ``Client.publish_bytes`` / ``Client.publish_file`` when
    ``expected_workspace_incarnation_id`` was given and the owner found the
    workbench bound to a different incarnation. The owner evaluates the fence
    atomically with ``expected_generation`` before any durable row or object
    exists, so nothing was written. ``expected`` is the fence the caller sent;
    read the current incarnation back (``find_workspaces`` or ``read`` metadata)
    before deciding whether to retry.
    """

    def __init__(self, message: str, expected: str) -> None:
        super().__init__(message)
        self.expected = expected


def __getattr__(name):
    if name == "torch":
        import importlib

        return importlib.import_module(".torch", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
