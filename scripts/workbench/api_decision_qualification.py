#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Check exact tracked support and replacement decision contracts."""

from source_bound_producer import (
    ScenarioContract,
    SourceTextAssertion,
    StaticScenario,
    static_main,
)


def _source(
    assertion_id: str,
    path: str,
    *,
    required: tuple[str, ...],
    forbidden: tuple[str, ...] = (),
    before_marker: str | None = None,
) -> SourceTextAssertion:
    return SourceTextAssertion(assertion_id, path, required, forbidden, before_marker)


SNAPSHOT_PROJECTION = _source(
    "agent-snapshot-list-projection-is-current-contract",
    "crates/nokv-agent/src/facade.rs",
    required=('"snapshot_count": values.len()', '"snapshots": values'),
)
NO_CHECKPOINTS = _source(
    "workbench-contract-rejects-checkpoints-jsonl",
    "docs/workbench-contract.md",
    required=(
        "`metadata/checkpoints.jsonl` does not exist in the Workbench namespace, response\nschema, or contract state.",
    ),
)
WORKSPACE_CLIENT = _source(
    "rust-client-replacement-is-workspace-client",
    "crates/nokv-client/src/lib.rs",
    required=("pub use sdk::{ClientCall, ClientOptions, WorkspaceClient};",),
    forbidden=("NoKvFsClient", "Inode", "Dentry"),
)
PYTHON_REPLACEMENT_DECISIONS = _source(
    "python-l03-l06-behaviors-must-be-replaced",
    "docs/development/pre423-contract-ledger.md",
    required=(
        "only `L07` FUSE/POSIX may be\nexcluded",
        "Python byte/range, Workbench-scoped fsspec, checkpoint, and torch\nDCP behaviors in `L03` through `L06` must be replaced and live-tested rather\nthan retired.",
    ),
)
L03_CONTRACT = _source(
    "l03-path-native-python-contract-is-explicit",
    "scripts/workbench/pre423_contract_ledger.json",
    required=(
        '"l03.path-native-python-compatibility-contract"',
        "old filesystem-shaped type names may stay absent but their behavior cannot be retired",
        "l03.python-range-batch-ordering-and-bounds-live",
    ),
)
L04_CONTRACT = _source(
    "l04-workbench-fsspec-contract-is-explicit",
    "scripts/workbench/pre423_contract_ledger.json",
    required=(
        '"l04.workbench-scoped-fsspec-contract"',
        "Provide an explicit Workbench-scoped fsspec adapter with whole-object semantics",
        "l04.fsspec-retry-and-concurrency-live",
    ),
)
L05_CONTRACT = _source(
    "l05-workbench-checkpoint-contract-is-explicit",
    "scripts/workbench/pre423_contract_ledger.json",
    required=(
        '"l05.workbench-checkpoint-compatibility-contract"',
        "Preserve checkpoint shard publication, manifest-as-commit-point visibility",
        "l05.checkpoint-partial-step-invisible-live",
    ),
)
L06_CONTRACT = _source(
    "l06-workbench-dcp-contract-is-explicit",
    "scripts/workbench/pre423_contract_ledger.json",
    required=(
        '"l06.workbench-dcp-adapter-contract"',
        "Preserve the torch.distributed.checkpoint adapter over immutable Workbench publication",
        "l06.dcp-short-range-batch-fails-live",
    ),
)


def _scenario(
    stable_id: str,
    *assertions: SourceTextAssertion,
    nq: str | None = None,
) -> StaticScenario:
    return StaticScenario(
        ScenarioContract(stable_id, "api-decision"), tuple(assertions), nq
    )


SCENARIOS = {
    "t17.snapshot-projection-decision": _scenario(
        "T17", SNAPSHOT_PROJECTION, NO_CHECKPOINTS
    ),
    "c18.no-checkpoints-jsonl-decision": _scenario(
        "C18", NO_CHECKPOINTS, SNAPSHOT_PROJECTION
    ),
    "l01.generic-profile-restoration-contract": _scenario(
        "L01",
        nq="the reviewed policy requires an explicit seven-tool generic Agent MCP profile, but the CLI and nokv-agent schema do not implement it yet",
    ),
    "l02.workspace-client-replacement-decision": _scenario("L02", WORKSPACE_CLIENT),
    "l03.path-native-python-compatibility-contract": _scenario(
        "L03", PYTHON_REPLACEMENT_DECISIONS, L03_CONTRACT
    ),
    "l04.workbench-scoped-fsspec-contract": _scenario(
        "L04", PYTHON_REPLACEMENT_DECISIONS, L04_CONTRACT
    ),
    "l05.workbench-checkpoint-compatibility-contract": _scenario(
        "L05", PYTHON_REPLACEMENT_DECISIONS, L05_CONTRACT
    ),
    "l06.workbench-dcp-adapter-contract": _scenario(
        "L06", PYTHON_REPLACEMENT_DECISIONS, L06_CONTRACT
    ),
    "l08.operational-command-restoration-contract": _scenario(
        "L08",
        nq="the reviewed policy requires nine closed operational recovery outcomes, but the current CLI and operations sources do not implement that support matrix yet",
    ),
}


def main() -> int:
    return static_main(
        producer_id="api-decision",
        scenarios=SCENARIOS,
        description="Check exact tracked NoKV API decision contracts.",
    )


if __name__ == "__main__":
    raise SystemExit(main())
