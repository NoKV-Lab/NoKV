"""Versioned Python SDK for NoKV Workbenches and immutable artifacts."""

from . import checkpoint
from ._native import Client, RoutingConfig, ObjectStoreConfig
from .fsspec import WorkbenchFileSystem

API_VERSION = 1

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
