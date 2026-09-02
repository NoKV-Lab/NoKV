#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Source-bound entrypoint for the real LingTai MCPClient qualification gap."""

from __future__ import annotations

from typing import Sequence

from source_bound_producer import ScenarioContract
from typed_live_qualification import gap_main


SCENARIOS = {
    "t18.restore-lingtai-restart-reconnect": ScenarioContract("T18", "lingtai-mcp-e2e"),
    "c01.exact-18-tools-lingtai-live": ScenarioContract("C01", "lingtai-mcp-e2e"),
    "c06.agent-root-isolation-lingtai-live": ScenarioContract("C06", "lingtai-mcp-e2e"),
    "c11.pagination-lingtai-live": ScenarioContract("C11", "lingtai-mcp-e2e"),
    "c21.restored-composition-lingtai-live": ScenarioContract("C21", "lingtai-mcp-e2e"),
}
REASON = (
    "No checked-in runner currently drives these scenarios through the real "
    "LingTai MCPClient, including restart and reconnect; static MCP or direct "
    "stdio tests are not live LingTai evidence."
)


def main(argv: Sequence[str] | None = None) -> int:
    return gap_main(
        producer_id="lingtai-mcp",
        scenarios=SCENARIOS,
        dependency_names=("nokv-seed", "lingtai-kernel", "object-store"),
        evidence_roles=("producer-result", "qualification", "mcp-transcript"),
        reason=REASON,
        description=__doc__ or "LingTai MCP qualification",
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
