<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# LingTai first-client evidence

The executable product-boundary workload lives at
`scripts/lingtai-workbench/live_first_client.py`. It exercises the flat `nokv`
CLI and MCP adapter against real root routing, Holt metadata ownership, and an
S3-compatible object provider. Its deterministic runtime evidence directory is
under `target/lingtai-workbench/evidence/` by default; evidence is not checked
into this source directory.

This is a correctness and interoperability workload, not a performance result.
It records all 18 tool inputs and exact responses, one deliberate create-only
error, commit replay, frozen reads, restore projections, and the explicit
materialize/collect boundary. Missing external services are `NOT QUALIFIED`.

See `scripts/lingtai-workbench/README.md` for commands and
`docs/development/workspace-acceptance.md` for the qualification boundary.
