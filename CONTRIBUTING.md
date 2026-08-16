<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Contributing to NoKV

Thanks for contributing. This file is the authoritative contribution guide for this repository.

## Start Here

Welcome, and thanks for your interest in NoKV. Whether this is your first open-source PR or your hundredth, we're glad you're here.

NoKV is a distributed workspace and artifact store built specifically for Agent
infrastructure. Holt stores one canonical normalized full-path namespace per
workspace, while immutable revision-owned blocks live in S3-compatible object
storage. The supported product surfaces are the Rust and Python SDKs, the
purpose-built `nokv` CLI, and the exact 18-tool Workbench/MCP contract. NoKV
does not implement a POSIX filesystem, FUSE frontend, or inode/dentry model.

### New here? Read these first (in order)

1. **Product contract:** [Workbench Contract](docs/workbench-contract.md).
2. **Architecture:** [Product Design](docs/product-design.md) and
   [Metadata Schema](docs/metadata-schema.md).
3. **Engineering rules:** [Code Contract](docs/development/code_contract.md)
   [PR Review Checklist](docs/development/pr_review_checklist.md), and
   [Change Governance](docs/development/change-governance.md).

### Make your first contribution

- Browse [good first issues](https://github.com/NoKV-Lab/NoKV/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22) for a scoped starting point.
- For a new bug or feature, open an issue via the [template chooser](https://github.com/NoKV-Lab/NoKV/issues/new/choose). For broad design, onboarding, or meta topics, open a [Discussion](https://github.com/NoKV-Lab/NoKV/discussions) first.

Before you open a PR, read **[Issues and Proposals](#issues-and-proposals)**,
**Branch and Commit Conventions** (including DCO sign-off), and **Pull Request
Rules** below. Those sections are the source of truth for branch names, commit
format, validation, and review expectations.

### Reporting security issues

Do not open a public issue with exploit details. Follow the private reporting process in [SECURITY.md](SECURITY.md). If private reporting is unavailable, open a minimal public issue asking for a private follow-up channel, with no exploit details, secrets, or proof-of-concept.

## Scope

- Repository: `github.com/NoKV-Lab/NoKV`
- Main branch: `main`
- Main product line: Rust NoKV under `crates/`
- Minimum supported Rust version: 1.88

## Development Setup

1. Fork on GitHub and clone your fork.
2. Add the upstream remote to keep your fork up to date.
3. Install Rust 1.88 or newer.

```bash
git clone https://github.com/YOUR_GITHUB_USER/NoKV.git
cd NoKV
git remote add upstream https://github.com/NoKV-Lab/NoKV.git
git fetch upstream
cargo fetch
```

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

Run the repository contract gates before opening a PR:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python3 scripts/workbench/workbench_contract_test.py
git diff --check
```

For benchmark-related changes, follow
[`docs/benchmarks.md`](./docs/benchmarks.md). A release-mode unit test is useful
validation but is not itself a qualified performance result.

The repository currently has no checked-in documentation build configuration.
For documentation or navigation changes, validate local Markdown/image links
and run `git diff --check`.

## Pull Request Rules

- Rebase on latest `upstream/main` before opening or updating a PR.
- PR description must include: what changed, why it changed, and how you validated it (commands + key results).
- Link related issue(s).
- Include docs updates when behavior/config/CLI changes.
- Keep PRs small enough for focused review.
- A PR with 5,000 or fewer GitHub-reported changed lines (additions plus
  deletions) does not require a review.
- A PR with more than 5,000 changed lines requires one core maintainer other
  than the current-head pusher to approve the exact current head. Authors,
  bots, non-core reviewers, duplicate reviewers, dismissed reviews, and
  approvals on older commits do not count. The governance status fails closed
  if it cannot verify these facts.
- Administrators remain subject to every required status check. All per-head PR
  validation jobs are required except Docker image, which runs in the
  background without delaying merge. Project-board automation is not CI.
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
- Prefer direct breaking replacements that remove ambiguity. Do not add
  forwarding aliases, fallback layouts, or parallel execution paths.

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
