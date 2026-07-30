<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Experimental Multi-Shard Fleet Runbook

> **Status: Experimental.** This runbook describes the multi-process shape
> implemented on `main`. Local smoke tests cover routing, owner handoff,
> checkpoint restore, logical-log replay, and stale-owner fencing. It is not a
> production deployment recommendation or evidence of multi-machine throughput,
> partition tolerance, or operational maturity.

A NoKV fleet partitions the namespace by path prefix. Each active owner keeps
one shard in a local Holt database, while an etcd control plane publishes routes
and owner epochs. A shared S3-compatible provider stores file bodies and the
checkpoint/log artifacts used to recover a shard on another node.

Read [Metadata Sharding And Recovery](./metadata-sharding-and-recovery.md)
before operating this mode; its shard-local atomicity and failure semantics are
part of this runbook.

## Components

| Component | Current role | Deployment note |
| --- | --- | --- |
| etcd | shard registrations, routes, owner leases, epochs, and recovery pointers | use a provider-supported topology appropriate to the failure domain; one instance is only a development shape |
| S3-compatible provider | file bodies, Holt checkpoint images, and logical-log segments | shared and reachable by every metadata owner |
| `nokv serve` | one active metadata owner with local Holt state | current CLI shape is one shard per process |
| CLI / Rust SDK / FUSE | route requests from the control-plane shard map | fleet routing is implemented |
| Python binding | connect to one `metadata_addr` | fleet construction from etcd is not implemented |

The control plane stores no inode, dentry, manifest, snapshot, or GC records.
Those remain in each shard's Holt store and, for failover, in its published
checkpoint plus logical-log tail.

## Prerequisites

- Build the CLI with the etcd backend:

  ```sh
  cargo build --release -p nokv --features etcd
  NOKV_BIN=./target/release/nokv
  ```

- Make etcd reachable from each metadata process and each fleet client.
- Make one S3-compatible bucket reachable from every metadata process.
- Choose a default shard (`mount-<N>:/`, index `0`) and non-overlapping subtree
  prefixes with stable, unique `shard_index` values.
- Allocate a distinct local Holt directory for each owner instance. Do not let
  two live processes open the same local directory.
- Protect etcd, object-store, and metadata endpoints at the surrounding
  infrastructure layer. This runbook does not configure NoKV tenant auth or
  RBAC because those fleet controls are not implemented.

The shard index occupies the high bits of every inode minted by that shard. Do
not reuse an index for a different subtree within the same mount.

## 1. Define Shared Arguments

The examples below use shell arrays so the same control and object settings are
passed to every owner:

```sh
ETCD=http://etcd-0:2379,http://etcd-1:2379,http://etcd-2:2379
PREFIX=/nokv/control/example

S3=(
  --object-backend rustfs
  --s3-bucket nokv
  --s3-endpoint http://s3-0:9000
  --s3-access-key-id "$AK"
  --s3-secret-access-key "$SK"
)

CTRL=(
  --mount 1
  --control-backend etcd
  --control-etcd-endpoints "$ETCD"
  --control-etcd-prefix "$PREFIX"
  --control-etcd-lease-ttl-seconds 10
  --metadata-shared-log-prefix metadata/example/shared-log
  --metadata-checkpoint-archive-prefix metadata/example/checkpoints
)
```

Use unique prefixes per environment. The sample access and secret key variables
must come from a secret manager or other deployment-specific credential source.

### Lease TTL

`--control-etcd-lease-ttl-seconds T` drives the owner renewal and self-fencing
window. A shorter value can reduce takeover delay but increases etcd traffic and
sensitivity to transient stalls. The CLI caps its default renewal interval at
`T/3`; a smaller explicit `--shard-owner-renewal-interval-ms` is honored.

Do not choose `T` from local smoke timings alone. Measure clock behavior,
scheduler stalls, control-plane latency, and real network failure modes in the
target environment.

## 2. Start One Owner Per Shard

Start the default shard owner:

```sh
"${NOKV_BIN}" --meta /var/lib/nokv/meta-default --server-bind 0.0.0.0:7740 \
  "${S3[@]}" "${CTRL[@]}" \
  --shard-id "mount-1:/" \
  --shard-index 0 \
  --node-id "metanode-0:7740" \
  serve
```

Start an owner for `/workspace-a`:

```sh
"${NOKV_BIN}" --meta /var/lib/nokv/meta-workspace-a --server-bind 0.0.0.0:7741 \
  "${S3[@]}" "${CTRL[@]}" \
  --shard-id "mount-1:/workspace-a" \
  --shard-index 1 \
  --node-id "metanode-1:7741" \
  serve
```

Operational constraints:

- `--node-id` must be a `host:port` reachable by clients. It is published as
  the current owner endpoint; do not use a loopback address across machines.
- Each `--meta` path is local to that owner process. Cross-node recovery comes
  from the published checkpoint/log chain, not from sharing the Holt directory.
- Registration derives the owned prefix from `--shard-id`. Add subtrees with a
  new prefix, index, local metadata directory, and owner endpoint.
- Register a topology before placing data under a subtree. Online migration of
  an already-populated subtree is not implemented.

## 3. Connect A Fleet-Capable Client

Point the CLI at the control plane rather than a single metadata address:

```sh
"${NOKV_BIN}" "${S3[@]}" \
  --mount 1 \
  --control-backend etcd \
  --control-etcd-endpoints "$ETCD" \
  --control-etcd-prefix "$PREFIX" \
  ls /workspace-a
```

The client obtains `list_shards`, builds a longest-prefix map, and routes
inode-addressed operations from the inode's shard bits. On `NotOwner` or a stale
route it refreshes the control state and retries according to the operation's
retry rules. The same fleet client wiring is available to the Rust SDK and FUSE
frontend.

Do not copy this control-endpoint example into Python. The current Python
constructor accepts one `metadata_addr` and therefore targets one server
endpoint.

## 4. Replace A Failed Owner

Failover restores a shard from shared recovery artifacts; it does not copy the
old node's local Holt directory.

1. Confirm the old owner has been intentionally stopped, released, or its owner
   session has expired.
2. Read the previous epoch from the shard record.
3. Start a replacement with a new local metadata directory and reachable
   endpoint:

   ```sh
   "${NOKV_BIN}" --meta /var/lib/nokv/meta-workspace-a-b --server-bind 0.0.0.0:7742 \
     "${S3[@]}" "${CTRL[@]}" \
     --shard-id "mount-1:/workspace-a" \
     --shard-index 1 \
     --node-id "metanode-2:7742" \
     --failover-from-epoch <previous_epoch> \
     serve
   ```

4. The replacement transactionally acquires the next epoch, restores the latest
   checkpoint, validates and replays the published logical-log tail, reconciles
   grafts, and then marks the shard serving.
5. Fleet clients refresh and route to the new endpoint.

The etcd session comparison and NoKV epoch/lease checks are intended to prevent
both epochs from committing concurrently. The current evidence covers local
process death, lease expiry, and a paused stale process. Treat real
multi-machine partitions and prolonged control-plane failures as unqualified
until they have been tested in the intended infrastructure.

### Ambiguous write failures

In controlled mode, Holt applies a command before the logical-log segment and
control pointer are published. A publication failure can therefore return a
durability error marked `committed=true`. Preserve the request ID, inspect or
retry through the idempotent request path, and do not assume an error means the
namespace mutation did not happen.

A normal success response is returned only after the exact recovery tail has
been archived and published. That claim still depends on the configured object
provider and etcd meeting their durability guarantees.

## 5. Run The Local Gate

The repository gate co-locates all roles on one machine:

```sh
scripts/run-multishard-fleet-smoke.sh
```

It can reuse external etcd and RustFS services:

```sh
NOKV_FLEET_ETCD_ENDPOINTS="$ETCD" \
NOKV_FLEET_RUSTFS_ENDPOINT="http://s3-0:9000" \
NOKV_FLEET_RUSTFS_ACCESS_KEY="$AK" \
NOKV_FLEET_RUSTFS_SECRET_KEY="$SK" \
NOKV_FLEET_SERVER_A_BIND="0.0.0.0:7740" \
NOKV_FLEET_SERVER_B_BIND="0.0.0.0:7741" \
NOKV_FLEET_SERVER_B2_BIND="0.0.0.0:7742" \
NOKV_FLEET_METRICS_JSON=/tmp/fleet-metrics.json \
scripts/run-multishard-fleet-smoke.sh
```

This remains a single-host script even when it uses external services. A
multi-machine exercise must start the owners on separate hosts, drive a client
from another host, and inject failures at the network and machine boundaries.

## 6. Validation Checklist

- Confirm paths under two prefixes reach different owner endpoints and returned
  inodes carry the expected shard indices.
- Confirm a same-shard metadata batch commits atomically and a cross-shard batch
  is rejected before partial execution.
- Stop one owner, wait for the session transition, start the replacement, and
  verify data acknowledged before failure is visible after checkpoint/log
  recovery.
- Inject a response-loss or control-publication failure and verify the client
  handles `committed=true` without creating a duplicate logical operation.
- Run `fsck` for every shard and inspect dangling records; do not assume one
  shard's result covers the fleet.
- Measure shard-local throughput, fleet scaling, object-provider saturation,
  p95/p99 latency, failover time, and skew on the target hardware.
- Test paused owners and network partitions across machines before making an
  availability claim.

## Known Limits

- One active owner per shard; no multi-writer or consensus-replicated metadata.
- Current CLI deployment is one shard per server process.
- Cross-shard `rename`, `hardlink`, clone, and batch transactions are rejected;
  there is no distributed two-phase commit.
- A query rooted above a graft point is not a complete cross-shard aggregate.
- Subtree registration is static; online resharding and live data migration are
  not implemented.
- Python fleet routing is not implemented.
- Built-in tenant authentication, authorization, and network encryption are not
  configured by this runbook.
- Rolling upgrades, mixed-version compatibility, multi-machine chaos, and
  enterprise small-file throughput have not been qualified.
