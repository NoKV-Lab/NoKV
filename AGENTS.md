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

Before reviewing or editing a PR:

1. Read `docs/development/code_contract.md`.
2. Use `docs/development/pr_review_checklist.md`.
3. Inspect the real changed files before relying on README or design docs.
4. Report findings first, ordered by severity.

Check for:

- Scope drift across `nokv-types`, `nokv-meta`, `nokv-object`, `nokv-agent`,
  `nokv-client`, `nokv-fuse`, docs, and example files.
- Missing DCO `Signed-off-by` trailers.
- Package-boundary violations.
- `nokv-agent` depending on `nokv-client`, `nokv-protocol`, or `nokv-control`:
  the agent tool surface stays transport-free, and the embedded
  `impl AgentNamespace for NoKvFs` lives in `nokv-agent` so `nokv-meta` gains no
  reverse dependency on it.
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
