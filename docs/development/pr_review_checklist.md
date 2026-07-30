<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# PR Review Checklist

Findings come first. Do not approve a change because tests pass if it weakens
metadata atomicity, object publish safety, watch/snapshot retention, or package
boundaries.

## Scope

- Does the PR change one logical boundary?
- Are unrelated metadata schema, object, client, FUSE, docs, or example changes
  mixed together?
- Is every behavior change described?
- Does every non-merge commit include `Signed-off-by`?

## Boundaries

- Does the package import direction match the code contract?
- Did a lower layer import a higher layer for convenience?
- Does `nokv-types` remain storage-neutral?
- Does `nokv-meta` keep schema, command execution, Holt binding, and service
  semantics inside the metadata boundary?
- Does `nokv-object` avoid namespace metadata?
- Does `nokv-client` resolve paths through `nokv-meta` instead of importing
  layout or storage internals?
- Does `nokv-agent` own transport-free tool schemas and dispatch, while remote
  implementations stay in `nokv-client` and MCP transport stays in the `nokv`
  CLI?
- Does `nokv-fuse` stay inode-first and call `nokv-meta` rather than the
  path SDK?
- Does `nokv` keep the `nokv` binary thin over `client`/`fuse` instead
  of duplicating metadata semantics?
- Does `nokv-control` contain only shard maps, owner leases, epochs, and
  recovery pointers rather than namespace metadata truth?
- Does path routing and sharding stay above Holt, with Holt remaining a
  shard-local engine?

## Correctness

- Are predicates checked before mutations and applied atomically?
- Can a failed object publish or metadata publish leave user-visible partial
  state?
- Are duplicate request ids deterministic?
- Does remove/replace return old body descriptors when GC needs them?
- Are snapshot/watch retention and history GC rules explicit?
- Do snapshot, copy-on-write, object-reference, and GC-epoch interactions keep
  historical workspace state alive for the required lifetime?
- Does a read path observe a complete dentry projection or fall back safely?
- For sharding changes, is there exactly one active writer per shard, with
  stale epochs rejected at the metadata commit boundary?
- Are checkpoint-image/shared-log recovery, owner handoff, and client
  re-resolution correct when failover is interrupted at each step?
- Is shard-local atomicity explicit? Are cross-shard failure and partial
  progress handled without implying a distributed transaction?
- Does a namespace/workbench jail remain described as a path boundary rather
  than authentication, authorization, or tenant isolation?

## Performance

- Does a hot metadata operation avoid unnecessary history writes?
- Does `ReadDirPlus` hit dentry projection without inode fanout on the common
  path?
- Does prefix-empty use Holt prefix iteration with early exit?
- Does a performance claim name the comparison boundary (L1 or L2), topology,
  cache state, run count, raw evidence, and reproducible command?
- Does benchmark code observe the product without changing product semantics?

## Tests

- Is there a package test for each local invariant?
- Is there a contract test for metadata commands or object-store behavior?
- Are S3/RustFS integration tests env-gated rather than hard-required?
- Are error paths and predicate failures covered?
- For multi-shard changes, are routing, owner loss, epoch fencing, recovery,
  cross-shard errors, and unaffected-shard continuity covered?
- For agent/MCP changes, are schemas, argument rejection, dispatcher behavior,
  remote implementations, and stdio transport tested at their owning layers?

## Documentation and Security

- Do user-facing claims distinguish current, experimental, and planned work?
- Are security requirements aligned with the current trusted-deployment
  boundary, without claiming built-in RBAC, tenant identity, or live workspace
  freeze before they exist?
- Are historical benchmark results dated and separated from current product
  positioning?

## Validation

Run the checks relevant to the changed surface and record exact commands and
results. For Rust code, the baseline is:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check
```

Documentation-only changes may omit Rust checks with a reason, but still need
`git diff --check` and local link/reference validation. Benchmark changes need
their focused runner/package tests plus raw evidence for any performance claim.
