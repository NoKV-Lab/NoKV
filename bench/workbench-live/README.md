<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Live Workbench evidence

The executable product-boundary workload lives at
`scripts/workbench/live_workbench.py`. It exercises the flat `nokv` binary
against real root routing, Holt metadata ownership, and an S3-compatible object
provider. It reaches the 18 tools as `nokv workbench <tool>` processes graded
against `docs/cli-contract.md`, so its evidence is CLI evidence and says
nothing about the Python SDK path. Its deterministic
runtime evidence directory is under `target/workbench-live/evidence/` by
default; evidence is not checked into this source directory.

This is a correctness and interoperability workload, not a performance result.
It records all 18 tool inputs and exact responses, one deliberate create-only
error, commit replay, frozen reads, restore projections, and the explicit
materialize/collect boundary. Missing external services are `NOT QUALIFIED`.

See `scripts/workbench/README.md` for commands and
`docs/development/workspace-acceptance.md` for the qualification boundary.
