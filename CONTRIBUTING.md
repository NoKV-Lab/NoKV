<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Contributing to NoKV

Thanks for contributing. This file is the authoritative contribution guide for this repository.

## Start Here

Welcome, and thanks for your interest in NoKV. Whether this is your first open-source PR or your hundredth, we're glad you're here.

NoKV gives AI agents a filesystem (directories, paths, listing, reading) instead of a database to query. File bodies live as immutable blocks in S3-compatible object storage; NoKV manages the metadata layer on top. The bet: agents burn far fewer tokens walking a directory tree than reasoning over a SQL schema. The agent surface is seven verbs over the namespace: `ls`, `stat`, `find` (navigate), `read`, `grep` (read content), and `catalog`, `aggregate` (summarize). They are exposed to MCP (Model Context Protocol) clients via the `nokv-agent` crate. See the Agent Interface section of the [README](README.md) for the full description of this surface.

### New here? Read these three first (in order)

1. **Why:** [Agents Want Filesystems: Agent-Friendly Interfaces Are a Token-Efficiency Strategy](https://nokv.io/blog/agents-want-filesystems): the product argument for why a filesystem-shaped interface cuts agent token use (~45% fewer tokens, ~39% lower cost) versus a SQL schema.
2. **How:** [List a directory in 131 nanoseconds: holt, the metadata engine inside nokv](https://nokv.io/blog/holt-in-nokv): the metadata engine, an adaptive radix tree built for path-shaped keys.
3. **Evidence:** [Agent Interface Benchmark — NoKV](https://nokv.io/benchmark): 875 Yanex runs, the NoKV namespace vs. raw SQLite, showing 1.83x fewer prompt tokens, 1.63x lower cost, and 4.5/5 vs. 4.4/5 tasks solved.

### Make your first contribution

- Browse [good first issues](https://github.com/NoKV-Lab/NoKV/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22) for a scoped starting point.
- Recommended newcomer track: [#354: MCP server for the agent namespace surface](https://github.com/NoKV-Lab/NoKV/issues/354), which exposes the seven-verb agent surface to MCP clients over stdio.
- For a new bug or feature, open an issue via the [template chooser](https://github.com/NoKV-Lab/NoKV/issues/new/choose). For broad design, onboarding, or meta topics, open a [Discussion](https://github.com/NoKV-Lab/NoKV/discussions) first.

Before you open a PR, read **[Issues and Proposals](#issues-and-proposals)**, **Branch and Commit Conventions** (including DCO sign-off), and **Pull Request Rules** below. Those sections are the source of truth for branch names, commit format, the local make-gate, and review expectations.

### Reporting security issues

Do not open a public issue with exploit details. Follow the private reporting process in [SECURITY.md](SECURITY.md). If private reporting is unavailable, open a minimal public issue asking for a private follow-up channel, with no exploit details, secrets, or proof-of-concept.

## Scope

- Repository: `github.com/NoKV-Lab/NoKV`
- Main branch: `main`
- Main product line: Rust NoKV under `crates/`
- Rust toolchain: stable

## Development Setup

1. Fork on GitHub and clone your fork.
2. Add the upstream remote to keep your fork up to date.
3. Install Rust stable and Node.js for documentation builds.

```bash
git clone https://github.com/NoKV-Lab/NoKV.git
cd NoKV
git remote rename origin upstream
cargo fetch
npm --prefix docs ci
```

If you use a fork-based workflow, add your fork as `origin`.

## Branch and Commit Conventions

Use these branch prefixes:

- `feature/...` for new features
- `fix/...` for bug fixes
- `refactor/...` for non-functional refactors
- `docs/...` for documentation updates
- Commit format: `<type>: <subject>`
- Common types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`
- Keep each commit focused on one logical change.
- Sign every commit with the Developer Certificate of Origin trailer:

```bash
git commit -s -m "feat: add feature"
```

If a local commit is missing the trailer, amend or rebase before opening the PR:

```bash
git commit --amend -s --no-edit
git rebase --signoff origin/main
```

## Local Validation

Run these before opening a PR:

```bash
make fmt
make lint
make test
make docs-build
```

For benchmark-related changes:

```bash
cargo test --workspace --release
```

## Pull Request Rules

- Rebase on latest `upstream/main` before opening or updating a PR.
- PR description must include: what changed, why it changed, and how you validated it (commands + key results).
- Link related issue(s).
- Include docs updates when behavior/config/CLI changes.
- Keep PRs small enough for focused review.
- Keep each PR scoped to one logical boundary. Do not mix metadata model,
  Holt layout, object-store, docs, benchmark, or unrelated refactors.
- Every non-merge commit must include a `Signed-off-by` trailer matching the Developer Certificate of Origin in [`DCO`](./DCO).
- If you use Codex or another agent to review a PR, point it at [`docs/development/code_contract.md`](./docs/development/code_contract.md) and [`docs/development/pr_review_checklist.md`](./docs/development/pr_review_checklist.md).

## Code Guidelines

- Use `rustfmt` formatting and pass `clippy` with warnings denied.
- Add or maintain Rustdoc comments for public APIs when the semantics are not
  obvious from the type name.
- Keep package boundaries clear; avoid cross-package coupling without need.
- Do not mix unrelated refactors with behavior changes in one PR.
- Add tests for every bug fix or behavior change.
- Follow the repository code contract in [`docs/development/code_contract.md`](./docs/development/code_contract.md), including package responsibilities, shared-helper reuse, file naming, type/interface/function naming, error placement, metrics/stats ownership, generated-code discipline, and compatibility rules.
- Prefer breaking changes that remove ambiguity over compatibility wrappers. Add a compatibility shim only when a released RPC, CLI, config, or persisted format requires it, and document the removal condition.

## Testing Expectations

- Unit test for local logic changes.
- Integration test for cross-module behavior changes.
- Bench evidence for performance-sensitive modifications.
- If a test cannot be added, explain why in the PR.

## Issues and Proposals

- Use GitHub Issues for bugs/features.
- Use the repository issue template when opening a new issue.
- For broad design topics, use GitHub Discussions first, then split into implementable issues.

## Documentation Policy

When you change behavior, update related docs in the same PR:

- `README.md`
- `docs/`
- config examples and scripts if flags/config fields changed

## License

By contributing, you agree your contribution is licensed under Apache License 2.0, consistent with this repository.
