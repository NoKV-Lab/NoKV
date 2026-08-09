# FoundationDB provider live qualification

The `foundationdb-provider` feature is optional and non-default. A normal
`cargo test -p nokv-meta` run exercises Holt only and does not qualify the
FoundationDB binding.

The live test requires:

- FoundationDB client library and server version 7.3;
- an explicit cluster file in `NOKV_FDB_CLUSTER_FILE`;
- a unique, non-zero 16-byte namespace seed encoded as 32 hexadecimal
  characters in `NOKV_FDB_TEST_RUN_ID`.

Compile and link the feature first:

```bash
cargo test -p nokv-meta --features foundationdb-provider --no-run --locked
```

Run the one process-owned live harness:

```bash
NOKV_FDB_CLUSTER_FILE=/path/to/fdb.cluster \
NOKV_FDB_TEST_RUN_ID=0123456789abcdeffedcba9876543210 \
cargo test -p nokv-meta --features foundationdb-provider \
  workspace::provider::foundationdb::tests::foundationdb_provider_live_primitives \
  --locked -- --ignored --exact --nocapture
```

The harness uses one FoundationDB network guard and covers fresh namespace
admission, cross-space atomicity, ordered absence and prefix-empty guards,
stale and foreign witnesses, a consistent multi-space read view, exclusive
delimited scans, native transaction-size accounting, exact durable identity
reopen, wrong-identity rejection, and exact command replay after authority
quiescence.

This qualifies only the provider-primitive slice exercised above. It does not
qualify `foundationdb-v1` for Serving: the complete legal `MetadataCommand`
surface can still exceed FoundationDB's native transaction limit, so runtime
admission remains fail-closed until exact domain batching is implemented and
verified.

If the client library, cluster file, unique run id, or live cluster is absent,
even that primitive slice is **NOT QUALIFIED**. A default Holt test pass must
not be reported as FoundationDB evidence.
