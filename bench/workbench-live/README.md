<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Live Workbench evidence

The primary executable product-boundary workload lives at
`scripts/workbench/native_cli_workbench.py`. It exercises the full
`nokv workbench <tool> <canonical-json>` CLI boundary against real root
routing, Holt metadata ownership, and an S3-compatible object provider. Its
deterministic runtime evidence directory is under
`target/native-cli-workbench/evidence/` by default; evidence is not checked
into this source directory.

`scripts/workbench/live_workbench.py` separately qualifies the optional MCP
sidecar. It is not evidence that MCP is NoKV's primary integration surface.

These are correctness and interoperability workloads, not performance results.
The native runner records all 18 tool argv/input/result pairs, one deliberate
create-only error, commit replay, frozen reads, restore projections, and the
explicit materialize/collect boundary. Missing external services are
`NOT QUALIFIED`.

See `scripts/workbench/README.md` for commands and
`docs/development/workspace-acceptance.md` for the qualification boundary.
