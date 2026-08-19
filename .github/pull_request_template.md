<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

## Related Issue

<!--
Every non-trivial PR should be tied to an existing issue so maintainers can
review the agreed problem, scope, and design history before reading the diff.

Use one of:
- Closes #issue_number
- Fixes #issue_number
- Relates to #issue_number

Small exceptions are allowed for typo-only docs edits, CI/dependency chores,
release chores, and maintainer-approved emergency fixes. If this PR has no
issue, explain the exception here.
-->

Closes | Fixes | Relates to #

## Summary

-

## Scope

- [ ] This PR changes one logical boundary only.
- [ ] No unrelated refactor, benchmark, metadata model, Holt layout,
      object-store, agent interface, or docs change is mixed in.
- [ ] The linked issue describes the user-visible problem, design decision, or
      maintenance task this PR resolves.
- [ ] Any breaking change is intentional and documented.
- [ ] No compatibility shim, deprecated alias, or forwarding wrapper was added without a removal condition.

## Change Size And Review

- [ ] I checked GitHub's additions plus deletions for this pull request.
- [ ] If the total is more than 5,000 lines, one core maintainer other than the
      current-head pusher approved the exact current head commit. The author,
      bots, non-core reviewers, duplicate reviewers, dismissed reviews, and
      stale approvals do not count.
- [ ] If this changes a protected CI trust root listed in `.github/CODEOWNERS`,
      one core maintainer other than the current-head pusher approved the exact
      current head regardless of change size.
- [ ] This change is still small enough for a focused review; approval count is
      not being used to justify mixed package or lifecycle boundaries.

## Code Contract (Code Changes Only)

- [ ] Not applicable; this is a docs/config-only change.
- [ ] Package boundaries follow `docs/development/code_contract.md`.
- [ ] Shared helpers reuse the standard library or existing repository helpers.
      New generic helper modules are domain-neutral and tested.
- [ ] File names and file placement follow the code contract.
- [ ] New types, interfaces, structs, fields, and functions use domain-specific names.
- [ ] New errors are in the owning package's `errors.rs` and carry stable error kinds when crossing package boundaries.
- [ ] New metrics/stats are owned by the package that reports or serves them.
- [ ] Metadata changes document durability, atomicity, revision-reference
      lifetime, snapshot/commit/event retention, GC epochs, and fail-closed
      recovery boundaries.
- [ ] Placement or routing changes preserve persisted `RootId -> LogicalShardId`
      affinity, one active writer per shard, owner-epoch fencing, and explicit
      shard-local atomicity; no split root or cross-shard transaction is implied.
- [ ] Agent-interface changes keep the transport-free facade and schemas in
      `nokv-agent`, routing and workflows in `nokv-client`, and the concrete
      backend plus MCP wiring in the `nokv` CLI.

## Claims and Evidence

- [ ] User-facing documentation distinguishes current, experimental, and
      planned capabilities.
- [ ] Security claims do not treat a Workbench root or Agent scope as tenant
      authentication or authorization.
- [ ] Performance claims state the topology, comparison boundary, cache state,
      run count, and raw-evidence location.
- [ ] Benchmark-only behavior does not alter product semantics.

## Validation

<!-- List exact commands and key results. For each relevant check not run,
explain why. Docs-only changes may mark Rust checks not applicable. -->

- Command and result:
- Not run (with reason):

## Contributor Sign-off

- [ ] Every commit in this PR includes a DCO `Signed-off-by` trailer.
