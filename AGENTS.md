<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Agent Review Instructions

This repository uses `docs/development/code_contract.md` as the source of truth
for Rust package boundaries, naming, errors, metrics, tests, DCO, and storage
safety review.

## Local Collaboration Direction

Treat LingTai as the active partner and integration target for this repository.
Do not preserve, debug, document, or route around Yanex-specific workflows unless
the user explicitly asks for Yanex work. Yanex artifacts are historical
benchmark/demo material only; they must not drive new NoKV behavior, scripts,
docs, naming, preflight decisions, or compatibility paths.

## Interface Delivery Priority

Treat the native full `nokv` CLI as the primary product and integration
surface. Treat the direct Python SDK as the second-choice embedded surface.
Downstream Agent systems should normally provide their own skills that invoke
the CLI, or use the Python SDK when they need an in-process integration.

The `nokv mcp` Workbench sidecar is deprecated. It is not a supported NoKV
integration surface and must not be presented as one in documentation,
examples, release guidance, or reviews. Do not accept new work that extends it,
and do not restore MCP to any surface ordering: the order is CLI first, Python
SDK second, Rust SDK third.

The sidecar's code still ships because the live qualification harness drives
the product through it. `scripts/workbench/live_workbench.py`,
`scripts/workbench/object_namespace_recovery_gate.py`, and
`scripts/workbench/restore_composition_gate.py` launch `nokv mcp` as a child
process and retain `mcp-transcript.jsonl` as evidence;
`scripts/workbench/lingtai_mcp_qualification.py` is a source-bound gap
declaration that emits a synthetic transcript stub rather than running
anything. Treat those references as harness plumbing, not as a product claim,
and do not delete them while they are the only executed live path. The stable
18-tool Workbench contract itself is independent of MCP and remains
authoritative.

Before reviewing or editing a PR:

1. Read `docs/development/code_contract.md`.
2. Use `docs/development/pr_review_checklist.md`.
3. Inspect the real changed files before relying on README or design docs.
4. Report findings first, ordered by severity.

Check for:

- Scope drift across `nokv-types`, `nokv-meta`, `nokv-object`, `nokv-agent`,
  `nokv-client`, `nokv-server`, `nokv-control`, docs, and example files.
- Missing DCO `Signed-off-by` trailers.
- Package-boundary violations.
- New helpers that reimplement standard library or existing repository helpers.
- Misuse of `utils/` for domain-specific or single-use code.
- Misplaced errors, metrics, stats, validation, recovery, or encoding code.
- Vague file names, type names, interface names, or function names.
- Redundant forwarding wrappers or compatibility shims.
- Metadata durability, object-reference lifetime, watch/snapshot retention, or
  GC ambiguity.
- Missing regression, integration, recovery, or benchmark evidence.

Do not suggest compatibility shims by default. NoKV accepts breaking changes
when they remove ambiguity or reduce long-term maintenance cost. If a
compatibility path is necessary, require a removal condition.
