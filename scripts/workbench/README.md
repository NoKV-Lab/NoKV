# Workbench qualification

These scripts qualify the public Workbench contract and produce source-bound
evidence. They do not define a third metadata runtime.

## Runtime model

- `holt:///absolute/path` is the standalone profile. One process owns one Holt
  store and the physical Holt lock is the ownership authority.
- `fdb:///absolute/fdb.cluster?prefix=NAME` is the distributed profile. FDB is
  the metadata, catalog, route, session, and lease authority.
- Workspace clients receive one or more `--seed HOST:PORT` options and discover
  the current owner through NoKV RPC. They do not connect to the metadata store.

The FDB runtime remains **NOT QUALIFIED** until the live serving gates in
`docs/development/metadata-store-interface.md` have run against a real FDB
cluster. A unit or dry-run result cannot change that status.

## Local checks

Run the script-level contract tests with Python 3.11 or newer:

```bash
python3 scripts/workbench/workbench_contract_test.py
python3 scripts/workbench/live_workbench_test.py
python3 scripts/workbench/qualification_receipt_test.py
python3 scripts/workbench/qualification_aggregate_test.py
python3 scripts/workbench/qualification_invocation_manifest_test.py
```

The pre-4.23 ledger scripts are retained as historical acceptance bookkeeping.
They may describe superseded evidence, but they are not runtime configuration
or a compatibility path.

## Standalone live qualification

The current live harness formats a fresh Holt store, provisions two independent
roots, starts one NoKV seed/owner process, and exercises the complete Workbench
tool profile plus explicit materialize/collect transfers.

Prerequisites:

- a built `nokv` binary;
- an S3-compatible object service and fresh bucket/prefix;
- a fresh absolute Holt metadata path.

Example:

```bash
cargo build -p nokv --bin nokv
python3 scripts/workbench/live_workbench.py \
  --nokv-bin target/debug/nokv \
  --metadata-dir /absolute/tmp/nokv-holt \
  --object-bucket nokv-live \
  --object-endpoint http://127.0.0.1:9000 \
  --object-access-key-id minioadmin \
  --object-secret-access-key minioadmin \
  --server-bind 127.0.0.1:7750 \
  --advertise-endpoint 127.0.0.1:7750 \
  --evidence-dir target/workbench-live/evidence/run-01
```

The harness records the exact `format`, `provision`, `serve`, seed-discovered
client commands, binary digest, environment, transcript, and qualification
result. It fails closed when the metadata path already exists or a live
dependency cannot be reached.

Dry-run mode validates command construction and records `NOT QUALIFIED`
evidence without claiming a live result:

```bash
python3 scripts/workbench/live_workbench.py --dry-run
```

## Removed recovery harnesses

The former distributed-local-log recovery, object-namespace adoption, and
feature-only restore-crash owners were coupled to the retired control model.
They were removed with that model. Holt recovery is now the store's native
reopen path; distributed durability and takeover belong to FDB and require new
FDB-specific live gates before qualification.
