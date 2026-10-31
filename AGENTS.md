<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Agent Review Instructions

This repository uses `docs/development/code_contract.md` as the source of truth
for Rust package boundaries, naming, errors, metrics, tests, DCO, and storage
safety review.

## PR Writing

Follow the authoritative
[PR description format](CONTRIBUTING.md#pr-description-format) and
[pull request template](.github/pull_request_template.md) when creating or
updating a PR. This is the repository's required format, including for agents.

- Write a concise English title as `<type>: <outcome>`.
- Use these body headings in order: `TLDR`, `Problem`, `Consequences`,
  `Example`, `Solution`, `Gain`, `Tradeoffs`, then `Validation`.
- Use short, plain English paragraphs. Explain the concrete problem, user
  impact, and before/after behavior with minimal code details. Avoid file-by-file
  summaries, pasted diffs, repeated explanations, and conversational history.
- Link the issue under `Problem`; explain any permitted exception there.
- Report exact validation commands and results, relevant checks not run, and
  evidence limits. Do not claim unmeasured gains or unverified recovery.
- Keep every heading; briefly explain when a section does not apply. Refresh
  the title and body when the final scope changes.
- Keep DCO sign-offs and all existing engineering and merge requirements.
- Before opening or editing a PR, run `scripts/ci/pr_description.py` against a
  JSON object containing its final `title` and `body`, using
  `--pull-request-json <path>`. The same policy is the required remote
  `pr-description` check; its regression tests are in
  `scripts/ci/pr_description_test.py`.

## Local Collaboration Direction

LingTai is no longer an active partner or a default integration target.
Treat its collaboration announcements and qualification artifacts as historical
records, not as instructions for new integration work. Choose downstream targets
from the user's current scope and verified requirements.
Do not preserve, debug, document, or route around Yanex-specific workflows unless
the user explicitly asks for Yanex work. Yanex artifacts are historical
benchmark/demo material only; they must not drive new NoKV behavior, scripts,
docs, naming, preflight decisions, or compatibility paths.

## Interface Delivery Priority

Treat the native full `nokv` CLI as the primary product and integration
surface. Treat the direct Python SDK as the second-choice embedded surface.
Downstream Agent systems should normally provide their own skills that invoke
the CLI, or use the Python SDK when they need an in-process integration.

The `nokv mcp` Workbench sidecar has been removed; NoKV ships no MCP
transport. Do not reintroduce one, do not present MCP as an integration
surface in documentation, examples, release guidance, or reviews, and do not
restore MCP to any surface ordering: the order is CLI first, Python SDK
second, Rust SDK third.

Every live qualification gate drives the product through the native CLI.
`scripts/workbench/live_workbench.py`,
`scripts/workbench/object_namespace_recovery_gate.py`, and
`scripts/workbench/restore_composition_gate.py` run every Workbench tool call
as its own `nokv workbench <tool>` process, grade results and failures against
`docs/cli-contract.md`, and retain `cli-transcript.jsonl`.
`scripts/workbench/lingtai_mcp_qualification.py` and the `lingtai-mcp`
producer of the pre-#423 ledger are a historical gap declaration for LingTai's
MCP client: the script emits a synthetic transcript stub and runs nothing.
Retiring that evidence class is a ledger change for its own PR. The stable
18-tool Workbench contract is independent of any transport and remains
authoritative.

## Tenant Identity And Access Direction

NoKV is adding tenant identity, authentication, and role-based access control
(RBAC) enforced by the metadata server. The target lets many agents act on one
root, and one agent act on many roots in parallel, each under its own
authenticated principal and roles. Follow the invariants and package ownership
in
[Tenant Identity And Access Control](docs/development/code_contract.md#tenant-identity-and-access-control).

The one-Agent-per-root rule (`RootAgentBinding`, `--agent-id`) is retired as a
design direction. The native CLI still checks it until server-side
authorization replaces it.

- Do not extend the rule or add new dependencies on it.
- Do not describe it, an `AgentId`, a Workbench root, or a presentation path as
  access control.
- Until authentication and authorization ship, the trusted-deployment boundary
  in [SECURITY.md](SECURITY.md) applies; never claim tenant isolation for a
  current release.

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
