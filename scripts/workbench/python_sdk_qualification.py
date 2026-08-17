#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Source-bound entrypoint for the installed Python SDK live qualification gap."""

from __future__ import annotations

from typing import Sequence

from source_bound_producer import ScenarioContract
from typed_live_qualification import gap_main


def contract(stable_id: str) -> ScenarioContract:
    return ScenarioContract(stable_id, "python-sdk-e2e")


SCENARIOS = {
    "l03.python-create-publish-read-stat-list-rename-remove-live": contract("L03"),
    "l03.python-replace-generation-and-frozen-snapshot-live": contract("L03"),
    "l03.python-range-batch-ordering-and-bounds-live": contract("L03"),
    "l03.python-retry-concurrency-and-query-live": contract("L03"),
    "l04.fsspec-open-mode-policy-live": contract("L04"),
    "l04.fsspec-virtual-prefix-namespace-live": contract("L04"),
    "l04.fsspec-move-remove-live": contract("L04"),
    "l04.fsspec-write-read-range-live": contract("L04"),
    "l04.fsspec-create-only-and-replace-live": contract("L04"),
    "l04.fsspec-retry-and-concurrency-live": contract("L04"),
    "l05.checkpoint-publish-resolve-load-exact-live": contract("L05"),
    "l05.checkpoint-highest-committed-step-live": contract("L05"),
    "l05.checkpoint-partial-step-invisible-live": contract("L05"),
    "l05.checkpoint-distributed-shard-commit-live": contract("L05"),
    "l05.checkpoint-invalid-shard-name-rejected-live": contract("L05"),
    "l06.dcp-rank-semantics-live": contract("L06"),
    "l06.dcp-rank-failure-propagation-live": contract("L06"),
    "l06.dcp-short-range-batch-fails-live": contract("L06"),
    "l06.dcp-multirank-exact-roundtrip-live": contract("L06"),
}
REASON = (
    "The checked-in Python package has unit coverage, but no source-bound runner "
    "currently installs the built wheel and exercises it against real NoKV, etcd, "
    "and object-store services; unit mocks cannot qualify these live scenarios."
)


def main(argv: Sequence[str] | None = None) -> int:
    return gap_main(
        producer_id="python-sdk",
        scenarios=SCENARIOS,
        dependency_names=("etcd", "object-store", "python-sdk"),
        evidence_roles=("producer-result", "qualification"),
        reason=REASON,
        description=__doc__ or "Python SDK qualification",
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
