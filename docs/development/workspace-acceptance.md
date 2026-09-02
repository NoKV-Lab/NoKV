<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Workspace acceptance

NoKV acceptance is evidence-based. Source presence, unit mocks, dry runs, and
design text cannot qualify a live runtime.

## Result classes

- `PASS`: the exact required live scenario completed and retained all required
  source, binary, dependency, transcript, and result evidence.
- `FAIL`: the scenario ran and produced a deterministic contract, integrity,
  or safety violation.
- `NOT QUALIFIED`: the live scenario did not run, a dependency was missing, the
  evidence was incomplete, or the runtime is not yet admitted.

An unavailable FDB cluster, S3-compatible provider, NoKV seed, Python wheel, or
requested binary is `NOT QUALIFIED`, never a synthetic pass.

## Product surfaces

Acceptance covers the same root-routed system through:

- the Rust SDK;
- native CLI Workbench commands;
- the secondary Python SDK;
- the deprecated MCP qualification transport where a historical scenario still
  requires its transcript.

FUSE, POSIX, CSI, inode, dentry, and implicit local-filesystem APIs are outside
the product boundary.

## Route and identity evidence

Every live client scenario records:

- root identity;
- seed endpoint set;
- discovered logical shard and object namespace;
- placement, owner, and session generations;
- selected endpoint and refresh events;
- binary digest and protocol schema.

Clients must obtain dynamic routes from NoKV seeds. A direct metadata-database
read or a static CLI fence is not accepted as a failover test.

`AgentId` is checked while provisioning a root. It is not an authentication
credential presented by each client call. Root isolation is proven by
provisioning two distinct roots and showing that the same Workbench name has
independent state under each root.

## Standalone Holt acceptance

The live Workbench harness:

1. formats one fresh `holt:///absolute/path` store;
2. provisions independent primary and peer roots;
3. starts one Holt owner/seed process;
4. connects clients with `--seed`;
5. runs the complete Workbench profile, snapshot, restore, commit, query,
   materialize, and collect scenarios;
6. stops the process and retains its exact command and evidence bundle.

The Holt result qualifies only the same exclusive namespace. It does not imply
copied-directory, replacement-host, or distributed failover support.

The current harness is
[`scripts/workbench/live_workbench.py`](../../scripts/workbench/live_workbench.py).
Its dry-run mode records `NOT QUALIFIED` evidence.

## Distributed FDB acceptance

FDB serving is **NOT QUALIFIED** until the exact candidate passes the ten live
gate families in
[`metadata-store-interface.md`](metadata-store-interface.md#fdb-live-qualification-gates).

The retained bundle must include at least:

- git revision and dirty state;
- `nokv-fdb` binary digest and build features;
- FDB client and server versions plus cluster topology;
- canonical cluster-file digest and NoKV prefix digest, without credentials;
- object-provider identity and admission receipt;
- all format, provision, serve, client, failure-injection, and cleanup commands;
- control/catalog/session/heartbeat observations around each fault;
- workspace metadata results and protocol transcripts;
- transaction-size, conflict, retry, latency, and throughput measurements;
- final qualification record with no missing evidence role.

At minimum, failure injection covers unknown commit outcomes, pre-activation
crashes, post-activation owner loss, renewal failure, stale-owner writes,
takeover observation restart, provisioning interruption, and seed failure.

## Object safety

Acceptance must prove:

- object namespace admission before mutation;
- immutable create receipts and digest validation;
- no object listing as metadata recovery authority;
- revision-owned object lifetime and reference accounting;
- generation-fenced cleanup;
- quarantine of ambiguous destructive outcomes;
- bounded-memory streaming for large artifact publication and materialization.

Provider success flags alone do not prove an object durable or absent.

## Evidence aggregation

The source-bound producer and aggregate scripts reject:

- unregistered scenarios;
- a producer claiming another producer's scenario;
- missing dependency or binary identities;
- overlapping evidence roles;
- stale or mismatched invocation manifests;
- a claimed `PASS` with unsupported scenarios;
- checksum, transcript, or qualification-result drift.

Historical pre-4.23 ledgers remain audit material. They do not re-enable a
removed runtime, CLI option, or compatibility path.
