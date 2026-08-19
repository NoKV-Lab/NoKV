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
    "checkpoint",
]


def __getattr__(name):
    if name == "torch":
        import importlib

        return importlib.import_module(".torch", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
