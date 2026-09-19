"""Versioned Python SDK for NoKV Workbenches and immutable artifacts."""

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
    "Client",
    "ObjectStoreConfig",
    "RoutingConfig",
    "WorkbenchFileSystem",
    "WorkspaceIncarnationMismatch",
    "checkpoint",
]


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
