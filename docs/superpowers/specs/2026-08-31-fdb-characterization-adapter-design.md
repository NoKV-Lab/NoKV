<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FoundationDB Characterization Adapter Design

**Decision date:** 2026-08-31

**Status:** Implemented as a non-default characterization adapter; the shared
FoundationDB runtime was subsequently extracted into `nokv-fdb` by the
[dual-runtime serving design](./2026-08-31-dual-runtime-holt-fdb-serving-design.md).

**Qualification:** `NOT QUALIFIED` for NoKV serving.

## Decision

Add a non-default `nokv-meta-fdb` crate that implements the existing
storage-neutral `TxnStore` contract over FoundationDB. The first release is a
characterization adapter: it proves ordered reads, conditional atomic writes,
conflict handling, physical limit enforcement, and error mapping against a
real FoundationDB cluster. It is not selectable by `nokv-server` and cannot
serve a `MetaShard`.

`nokv-fdb` uses `foundationdb-rs` 0.11.0 with the `fdb-7_3` API feature and
owns the binding's synchronous database/transaction wrappers. The metadata
adapter keeps the current synchronous `TxnStore` interface without importing
FoundationDB or its futures directly. No automatic raw transaction retry is
introduced.

This change does not modify etcd, Holt, control-plane routing, CLI options,
Python APIs, or server bootstrap.

## Goals

- Implement `ReadBatch`, `WriteTxn`, `ready`, and `StoreProfile` without
  exposing FoundationDB binding types outside `nokv-fdb`.
- Preserve the exact `TxnStore` semantics for consistent reads, exact checks,
  atomic mutations, conflicts, and unknown commit outcomes.
- Use an unambiguous, ordered physical key encoding for one adapter namespace.
- Enforce FoundationDB key, value, transaction-size, and transaction-duration
  constraints before the adapter can claim conformance.
- Run the reusable `nokv-meta-store::conformance` suite against a real
  FoundationDB namespace.
- Keep default workspace builds independent of `libfdb_c`.

## Non-goals

- No `nokv-server` composition or `StoreConfig` support.
- No `MetaShard` binding, workspace lifecycle tests, failover qualification,
  benchmark qualification, or production readiness claim.
- No large metadata transaction redesign. In particular, this change does not
  split publication or secondary-index maintenance transactions.
- No async refactor of `TxnStore`, `MetaShard`, executor, or server code.
- No provider fallback, provider URI, dynamic plugin, compatibility wrapper,
  or automatic fallback to Holt.
- No FoundationDB tenant, directory layer, tuple layer, POSIX, inode, or dentry
  schema. `nokv-meta` remains the only owner of workspace records and codecs.
- No etcd or Holt cleanup.

## Constraints

The current `TxnStore` interface is synchronous. FoundationDB operations are
future-based, and its C client runs a process-wide network thread. The adapter
must bridge that mismatch without changing server execution in this phase.

FoundationDB limits affected data in one transaction to 10,000,000 bytes,
keys to 10,000 bytes, values to 100,000 bytes, and transactions to about five
seconds. NoKV currently admits valid metadata transactions above the
FoundationDB transaction limit. The adapter must therefore advertise a lower,
truthful profile and remain unavailable to `MetaShard` serving.

The FoundationDB Rust binding can retry transactions and may retry after
definite errors. `TxnStore::commit` instead reports a concurrent check/write
conflict as `Commit::Conflict`, and it must expose an uncertain commit as
`StoreError::OutcomeUnknown`. The first adapter does not use
`Database::run`, automatic idempotency, or an automatic transaction retry loop.

The development machine used for this change does not currently have
`fdbcli` or `libfdb_c`. Default-build validation can run locally. Feature-build
and live-cluster results must be reported as `PASS`, `FAIL`, or `NOT RUN`; an
unexecuted live test is never reported as passing.

## Approaches Considered

### 1. Characterization adapter on the current synchronous SPI — selected

Create one isolated adapter crate, bridge its futures internally, and exercise
the existing conformance suite. This is the smallest change that produces real
FoundationDB evidence while preserving package boundaries. Its synchronous
bridge is temporary and cannot justify a serving claim.

### 2. Async SPI before the adapter — deferred

Replace `TxnStore`, `MetaShard`, and the server call graph with async APIs, then
implement FoundationDB directly. This is the production-oriented direction,
but it combines an adapter with a full-stack execution refactor and is too
large for the first characterization milestone.

### 3. Wire FoundationDB into server bootstrap now — rejected

Add a server selector and rely on runtime errors for unsupported transactions.
This would either lie about `StoreProfile` limits or reject valid NoKV commands
after admission. Provider identity, configuration-digest admission, unknown
outcome reconciliation, and large-transaction decomposition are also absent.

## Package Boundary

The implemented package split has this dependency direction:

```text
nokv-fdb -> foundationdb (feature-gated)
nokv-meta-fdb -> nokv-fdb + nokv-meta-store
```

`nokv-fdb` owns the process runtime, common options and prefix envelope,
database/transaction handles, and error classification. `nokv-meta-fdb` must
not import the FoundationDB binding directly. Neither crate may depend on
`nokv-meta`, `nokv-server`, `nokv-control`, `nokv-meta-holt`, protocol, client,
Agent, Python, or CLI packages. The metadata adapter owns only metadata
keyspace encoding, physical limits, `TxnStore` mapping, and adapter-specific
error mapping.

Both crates are workspace members with no default features. `nokv-fdb/fdb`
activates `foundationdb = 0.11.0`, with upstream default features disabled and
the `fdb-7_3` and `embedded-fdb-include` features enabled;
`nokv-meta-fdb/fdb` forwards only to that feature. The embedded headers allow a
feature-level `cargo check` without a system FoundationDB header; linking and
running still require a compatible `libfdb_c`. Default options, lifecycle,
prefix, keyspace, limit, and error tests compile without the binding.

The code contract has explicit `nokv-fdb` and `nokv-meta-fdb` rows. Public
product docs do not list FoundationDB as a supported serving backend.

## Public Adapter API

`nokv-fdb` always exposes `FdbConnectionOptions`, `FdbStorePrefix`, and common
error classification. With `fdb`, it exposes the process runtime and owned
database/transaction wrappers. `nokv-meta-fdb` always exposes `FdbOptions` and
exposes `FdbStore` only with `fdb`:

```rust
pub struct FdbRuntime { /* one shared process guard */ }

pub struct FdbOptions {
    cluster_file: PathBuf,
    namespace: Vec<u8>,
    transaction_timeout: Duration,
    session_fence: FdbMetadataSessionFence,
}

pub struct FdbStore { /* FoundationDB-only internals */ }

impl FdbStore {
    pub fn open(runtime: &FdbRuntime, options: FdbOptions)
        -> Result<Self, StoreError>;
}
```

`FdbOptions::new` requires an explicit absolute cluster-file path, namespace,
and immutable stable-session predicate. The path must be valid UTF-8 because
the selected binding passes it to the FoundationDB C API. The namespace must
contain 1 through 64 arbitrary bytes. It is a physical isolation token, not a
Workbench, root, logical-shard, or schema identity. The session key must belong
to that prefix; its expected owner epoch and session generation are nonzero.

The default transaction timeout is 4 seconds. Accepted values are 1
millisecond through 4 seconds. There is no retry-count option in the first
adapter because `commit` performs one physical attempt.

`FdbRuntime::start` selects API version 730 and starts the network once. Calls
while it is running share the same guard. Dropping the final runtime, database,
and transaction handle stops the network permanently; a later start fails
instead of attempting an unsupported restart. `FdbStore::open` validates
options, uses that runtime to open the database, and retains an immutable
`StoreProfile`. Open verifies the exact session before returning. It does not
initialize or migrate the NoKV schema.

## Physical Key Encoding

Every key uses this byte layout:

```text
0x15
"nokv-fdb"
0x00
0x01                         # physical encoding version
namespace_len:u8
namespace:[u8; namespace_len]
0x07                         # Metadata subspace tag
0x0002                       # component length
keyspace:u16be               # component bytes
logical_key:bytes
```

The store-token length and component lengths make tokens/components such as
`a` and `ab` disjoint. The common envelope reserves stable tags for system,
catalog, route, session, heartbeat, and metadata subspaces. Big-endian
`Keyspace` encoding preserves numeric keyspace order. Raw logical-key bytes are
appended unchanged, so lexicographic order within a keyspace matches the
`TxnStore` contract.

The store prefix ends after `namespace`; a metadata keyspace prefix adds the
subspace tag plus a length-delimited two-byte keyspace component. Point keys
append the logical key. Prefix range ends use the shortest lexicographic
successor of the complete encoded prefix. An exclusive row cursor starts at
`encoded_row || 0x00`. A delimiter-folded common-prefix cursor skips the
complete common prefix by starting at its lexicographic successor.

The adapter never parses or owns `nokv_workspace` record bytes. The physical
encoding version only versions the adapter envelope.

## Store Profile And Physical Limits

The first profile declares:

```text
authority                 Shared
acknowledgement            SharedCommit
recovery                   StoreAuthority
transaction_target_bytes   900,000
max_reads                  8
max_checks                 1024
max_mutations              1024
max_key_bytes              8205
max_value_bytes            65535
max_read_bytes             4,500,000
max_transaction_bytes      2,900,000
max_result_rows            1024
max_result_bytes           8 MiB
```

The logical key and value limits fit below FoundationDB's physical limits even
with the maximum adapter envelope. The read and hard transaction byte limits
are lower than the current serving profile, so `MetaShard::bind` must reject
this adapter. The 900,000-byte target is future planner input and does not claim
that the current workspace schema fits it. No code weakens the serving limits
to make the adapter bind.

Before physical I/O, the adapter performs checked 64-bit affected-byte
accounting over encoded keys, range endpoints, conflict ranges, and mutation
values. It uses a 9,500,000-byte physical budget, leaving 500,000 bytes below
FoundationDB's hard limit. Point reads and checks charge both range endpoints;
puts and deletes charge the encoded mutation plus conservative write-conflict
range endpoints; range operations charge their endpoints and every returned or
looked-ahead encoded key. The 2,900,000-byte logical write cap, the 64-byte
namespace cap, and the operation-count caps keep the conservative triple-key
write estimate below the physical budget. Requests that exceed either the
logical or physical budget return `StoreError::LimitExceeded` before commit.

FoundationDB remains authoritative if it returns a physical limit error despite
preflight. The adapter maps that error to the matching key, value, read, or
transaction limit and retains a regression test for the estimator.

## Synchronous Bridge And Time Bounds

`FdbStore` holds a `nokv-fdb` database handle. The common wrapper uses
`futures::executor::block_on` on the calling thread; the FDB C client's one
process-global network thread drives the underlying future. Neither crate
creates a Tokio runtime.

Every transaction receives the configured FoundationDB transaction timeout.
The adapter does not call an unbounded retry helper. A definite transient
failure returns `StoreError::Unavailable`, allowing the caller to retry the
same domain request. A maybe-committed error is never retried as a raw physical
transaction.

This bridge is characterization-only. When NoKV replaces `TxnStore` with an
async interface, the same change removes `block_on`; it must not add a second
parallel adapter or retain a compatibility wrapper.

## Read Data Flow

1. Validate the complete `ReadBatch` against the profile.
2. Open one FoundationDB transaction and apply its timeout.
3. Execute every point and range operation against one snapshot of that
   transaction. Separate `read` calls use separate transactions.
4. Decode physical keys by stripping the exact store and keyspace prefix.
5. Fold delimiter scans in raw-key order. Continue fetching until the adapter
   has produced the requested output-row or byte limit plus enough lookahead to
   prove `more`.
6. Validate the assembled `ReadSnapshot` against the original batch before
   returning it.

A scan never uses a large numeric offset. Continuation starts from the encoded
successor of the previous row or common prefix. All network fetches for one
`ReadBatch` remain inside its original FoundationDB transaction so the results
cannot mix read versions.

`ready` opens a transaction, applies the timeout, verifies the exact stable
session, and obtains a read version. It must perform cluster I/O; a
network-thread-only no-op is insufficient.

## Commit Data Flow

1. Validate `WriteTxn` against the profile and physical affected-byte budget.
2. Create one FoundationDB transaction and apply its timeout.
3. Verify the exact stable session key, then evaluate `Value`, `Absent`, and
   `EmptyPrefix` checks with ordinary
   non-snapshot reads so FoundationDB installs the required read-conflict
   ranges.
4. If any check is false, discard the transaction and return
   `Commit::Conflict` without applying mutations.
5. Apply `Put` with `set` and `Delete` with `clear`.
6. Commit exactly once.

The adapter does not use `Database::run`, automatic idempotency, or `on_error`
for commit retries. A concurrent write that invalidates a read-conflict range
causes FoundationDB `not_committed`. The adapter reads the stable session once
in a fresh transaction: a replacement returns `StoreError::Fenced`; an
unchanged session returns `Commit::Conflict`. It does not re-evaluate or retry
the write transaction internally.

## Error Mapping

| FoundationDB outcome | `TxnStore` result |
| --- | --- |
| Successful commit | `Commit::Applied` |
| `not_committed` / error 1020 | `Commit::Conflict` |
| `commit_unknown_result`, error 1021, or another maybe-committed error | `StoreError::OutcomeUnknown { state: UnknownCommit::MayCommit, ... }` |
| Definite timeout, network, cluster, or retryable error before commit | `StoreError::Unavailable` |
| Transaction/key/value too large | matching `StoreError::LimitExceeded` |
| Invalid adapter options | `StoreError::InvalidRequest` |
| Malformed physical result or escaped namespace/keyspace | `StoreError::Corrupt` |

`UnknownCommit::MayCommit` does not poison a shared FoundationDB namespace. The
serving layer would have to reconcile the domain request before issuing another
physical commit; that orchestration is absent, which is another reason the
adapter cannot be wired into the server in this phase.

## Testing

### Default-build tests

- option validation, including absolute UTF-8 cluster files, namespace bounds,
  and timeout bounds
- process-runtime start-once, handle sharing, final shutdown, terminal start
  failure, and no-restart state using a binding-free lifecycle harness
- physical key and range encoding, namespace non-overlap, keyspace ordering,
  arbitrary-byte keys, row cursors, and common-prefix cursors
- logical and physical affected-byte accounting, including overflow cases
- error classification for 1020, 1021, 2101, 2102, 2103, and definite
  unavailable errors
- proof that default workspace tests do not activate or link FoundationDB

### Feature-build tests

With `--features fdb`, compile and test the binding-specific store code against
the selected 7.3 API. Error-boundary tests inject FoundationDB error codes into
the same classifier used by the commit path. They do not claim to reproduce a
real network partition.

### Live FoundationDB tests

An ignored integration test requires `NOKV_TEST_FDB_CLUSTER_FILE`. It creates a
cryptographically unique child namespace under an optional
`NOKV_TEST_FDB_NAMESPACE` base and runs
`nokv_meta_store::conformance::run` across a fresh open and reopen. Additional
live tests cover a real concurrent conflict, ordered binary range scans,
delimiter pagination, and namespace isolation.

Cleanup clears only the generated child namespace by its exact encoded range.
The test never clears the configured base namespace or another user's prefix.
The documented command is:

```bash
cargo test -p nokv-meta-fdb --features fdb \
  --test fdb_conformance -- --ignored --nocapture
```

Real commit-unknown, process-loss, failover, and network-partition tests remain
`NOT QUALIFIED`. Unit-level error injection is evidence for mapping only.

## Documentation And Status

Implementation updates:

- the root workspace manifest and lockfile
- the code-contract package table
- `metadata-store-interface.md` implemented/pending status and validation
  matrix
- the new crate's package documentation and live-test instructions

No README, CLI help, Python API, server flag, or architecture overview may call
FoundationDB a supported backend. Documentation must say
"characterization adapter" and `NOT QUALIFIED` until the separate serving gates
pass.

## Acceptance

The implementation is ready for review when:

- default `cargo fmt --all -- --check` passes
- default `cargo clippy --workspace --all-targets -- -D warnings` passes
- default `cargo test --workspace` passes without `libfdb_c`
- `git diff --check` passes
- both FDB-feature crates compile against the selected binding; linking and
  running require a compatible 7.3 client library and are reported `NOT RUN`
  when that dependency is missing
- the ignored live suite passes against a disposable FoundationDB namespace,
  or is reported `NOT RUN` with the missing cluster/client dependency
- the implementation changes no etcd, Holt, control, server, CLI, or Python
  behavior
- every commit contains a DCO `Signed-off-by` trailer

Passing default unit tests does not upgrade FoundationDB beyond
`NOT QUALIFIED`.

## References

- [FoundationDB known limitations](https://apple.github.io/foundationdb/known-limitations.html)
- [`foundationdb-rs` 0.11.0 manifest](https://github.com/foundationdb-rs/foundationdb-rs/blob/main/foundationdb/Cargo.toml)
- [`foundationdb-rs` database transaction API](https://github.com/foundationdb-rs/foundationdb-rs/blob/main/foundationdb/src/database.rs)
- [JuiceFS FoundationDB adapter](https://github.com/juicedata/juicefs/blob/main/pkg/meta/tkv_fdb.go)
- [NoKV metadata-store interface](../../development/metadata-store-interface.md)

## Follow-up Gates For Serving

The dual-runtime design supersedes the original serving checklist. Remaining
serving work still requires all of these gates:

- persist and validate the provider-neutral store identity and exact prefix
- add catalog, route, ownership session, and heartbeat transactions
- fence every owner-required metadata transaction with the stable session key
- reconcile metadata transaction unknown outcomes at domain-request scope
- qualify workspace behavior, response loss, process loss, failover, and
  representative performance
- wire one explicit FDB store configuration into the server without fallback

None of these gates is implied by completing the characterization adapter.
