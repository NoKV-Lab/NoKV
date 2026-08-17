#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Check exact tracked product boundaries for deliberately absent APIs."""

from source_bound_producer import (
    CargoWorkspaceGraphAssertion,
    ScenarioContract,
    SourceTextAssertion,
    StaticScenario,
    static_main,
)


def _source(
    assertion_id: str,
    path: str,
    *,
    required: tuple[str, ...] = (),
    forbidden: tuple[str, ...] = (),
    before_marker: str | None = None,
) -> SourceTextAssertion:
    return SourceTextAssertion(assertion_id, path, required, forbidden, before_marker)


CLIENT_PATH_NATIVE = _source(
    "client-public-api-is-workspace-routed",
    "crates/nokv-client/src/lib.rs",
    required=("pub use sdk::{ClientCall, ClientOptions, WorkspaceClient};",),
    forbidden=("NoKvFsClient", "Inode", "Dentry", "inode", "dentry"),
)
PYTHON_EXPORTS = _source(
    "python-old-filesystem-type-names-absent",
    "crates/nokv-python/python/nokv/__init__.py",
    required=(
        "Python SDK for NoKV Workbench",
        "from ._native import Client",
        "__all__ =",
    ),
    forbidden=(
        "NoKvFsClient",
        "NoKVFileSystem",
        "RangeBatchPlan",
        "RangeBatchReader",
        "ReadBuffer",
    ),
)
PYTHON_NATIVE_EXPORTS = _source(
    "python-native-old-filesystem-types-absent",
    "crates/nokv-python/src/lib.rs",
    required=(
        "module.add_class::<PythonWorkspaceClient>()?;",
        "module.add_class::<PythonRoutingConfig>()?;",
        "module.add_class::<PythonObjectStoreConfig>()?;",
    ),
    forbidden=(
        "NoKvFsClient",
        "NoKVFileSystem",
        "RangeBatchPlan",
        "RangeBatchReader",
        "ReadBuffer",
    ),
    before_marker="#[cfg(test)]",
)
PYTHON_NO_FILESYSTEM_EMULATION = _source(
    "python-native-boundary-has-no-filesystem-emulation",
    "crates/nokv-python/src/lib.rs",
    required=(
        "The only local-filesystem behavior in this\n//! crate is the explicit materialize/collect adapter.",
    ),
    forbidden=("fsspec", "FUSE", "inode", "dentry", "FileSystem"),
    before_marker="#[cfg(test)]",
)
WORKSPACE_GRAPH = CargoWorkspaceGraphAssertion(
    assertion_id="workspace-product-graph-excludes-fuse",
    forbidden_tokens=("nokv-fuse",),
)
FILESYSTEM_POLICY = _source(
    "product-contract-excludes-fuse-posix-layout",
    "docs/development/code_contract.md",
    required=(
        "FUSE, POSIX emulation, CSI, and arbitrary-root generic filesystem integration\nare outside the NoKV product architecture.",
        "A Python compatibility adapter may implement the\nbounded fsspec protocol only inside one explicit Workbench and its five virtual\nsections",
        "but must not\ncreate durable directory objects or emulate inodes, permissions, mounts, or\nanother namespace.",
        "must not introduce an inode/dentry\nnamespace",
    ),
)
CLI_NO_FILESYSTEM_COMPATIBILITY = _source(
    "cli-command-enum-has-no-filesystem-frontend",
    "crates/nokv/src/cli.rs",
    required=(
        "pub enum Command {",
        "Materialize {",
        "Collect {",
        "WorkspacePath(WorkspacePathCommand)",
    ),
    forbidden=(
        "Mount {",
        "Filesystem {",
        "Mkdir {",
        "Symlink {",
        '"mount" =>',
    ),
    before_marker="#[cfg(test)]",
)


def _scenario(
    stable_id: str,
    *assertions: SourceTextAssertion | CargoWorkspaceGraphAssertion,
) -> StaticScenario:
    return StaticScenario(ScenarioContract(stable_id, "api-absence"), tuple(assertions))


SCENARIOS = {
    "l02.inode-dentry-client-api-absence": _scenario(
        "L02", CLIENT_PATH_NATIVE, WORKSPACE_GRAPH
    ),
    "l03.retired-filesystem-type-names-stay-absent": _scenario(
        "L03", PYTHON_EXPORTS, PYTHON_NATIVE_EXPORTS
    ),
    "l06.filesystem-emulation-absence": _scenario(
        "L06", PYTHON_NO_FILESYSTEM_EMULATION, FILESYSTEM_POLICY
    ),
    "l07.fuse-posix-inode-dentry-absence": _scenario(
        "L07", WORKSPACE_GRAPH, FILESYSTEM_POLICY
    ),
    "l08.filesystem-cli-compatibility-absence": _scenario(
        "L08", CLI_NO_FILESYSTEM_COMPATIBILITY
    ),
}


def main() -> int:
    return static_main(
        producer_id="api-absence",
        scenarios=SCENARIOS,
        description="Check exact tracked NoKV API-absence boundaries.",
    )


if __name__ == "__main__":
    raise SystemExit(main())
