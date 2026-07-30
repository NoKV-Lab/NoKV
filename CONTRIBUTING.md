<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Contributing to NoKV

Thanks for contributing. This file is the authoritative contribution guide for this repository.

## Start Here

Welcome, and thanks for your interest in NoKV. Whether this is your first open-source PR or your hundredth, we're glad you're here.

NoKV is an object-backed metadata control plane for durable agent workspaces.
It provides a filesystem-shaped namespace, atomic metadata publication,
snapshots, copy-on-write workspace primitives, and experimental horizontal
path sharding while immutable file bodies remain in S3-compatible object
storage. The `nokv-agent` crate owns transport-free agent schemas and dispatch;
the `nokv` CLI exposes them to MCP clients over stdio. See the [README](README.md)
for the current capability boundary.

### New here? Read these three first (in order)

1. **Product boundary:** [NoKV README](README.md): current, experimental,
   and planned capabilities.
2. **System design:** [Architecture](docs/architecture.md): metadata, object,
   client, FUSE, control-plane, and sharding boundaries.
3. **Engineering contract:** [Code contract](docs/development/code_contract.md):
   package ownership and invariants contributors must preserve.

The [benchmark guide](docs/benchmarks.md) documents performance evidence and
its limits. Historical agent-interface experiments are retained for
reproducibility, but token reduction is not NoKV's product definition.

### Make your first contribution

- Browse [good first issues](https://github.com/NoKV-Lab/NoKV/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22) for a scoped starting point.
- For a new bug or feature, open an issue via the [template chooser](https://github.com/NoKV-Lab/NoKV/issues/new/choose). For broad design, onboarding, or meta topics, open a [Discussion](https://github.com/NoKV-Lab/NoKV/discussions) first.

Before you open a PR, read **[Issues and Proposals](#issues-and-proposals)**, **Branch and Commit Conventions** (including DCO sign-off), and **Pull Request Rules** below. Those sections are the source of truth for branch names, commit format, the local make-gate, and review expectations.

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

Your fork remains `origin`; the canonical NoKV repository is `upstream`.

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
git rebase --signoff upstream/main
```

## Local Validation

For Rust changes, run the repository gate before opening a PR:

```bash
make fmt
make lint
make test
```

For documentation-only changes, check formatting, links, and references in the
files you changed and run:

```bash
git diff --check
```

The repository does not currently ship a VitePress package or a documentation
site build target. Do not report a docs build that a fresh clone cannot run.

For benchmark harness changes, run the relevant package or runner tests and
record the exact workload, topology, cache state, commands, and results. For
Rust benchmark changes, this normally includes:

```bash
cargo test --workspace --release
```

For Python agent-interface runner changes, run its focused test suite from the
repository root:

```bash
python -m unittest discover \
  -s bench/agent-interface/agents_runner \
  -p 'test_*.py'
bash bench/agent-interface/scripts/test_run_phase1_batch.sh
```

Performance claims require raw evidence and a reproducible command; a passing
test suite alone is not benchmark evidence.

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
