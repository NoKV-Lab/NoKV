# Workbench preflight

Workbench clients are root-routed. Before running tools, pin the root identity,
NoKV seed endpoints, object namespace provider, and metadata runtime used by
the serving process.

## Client configuration

```text
--root-id HEX32
--seed HOST:PORT [--seed HOST:PORT ...]
--workbench-root /agents/NAME/wb
--object-bucket NAME --object-endpoint URL
```

Seeds return the complete current route. Clients never connect directly to the
metadata store, and there is no static-fence CLI fallback. `AgentId` is a
provisioning identity, not a client credential.

The client preflight verifies the exact root route, object namespace, protocol
schema, and required capability set before object or metadata mutation.

## Standalone Holt

Use a fresh absolute location:

```bash
nokv format --meta-url holt:///absolute/nokv-meta
nokv --root-id HEX32 --agent-id HEX32 \
  --object-bucket NAME --object-endpoint URL \
  provision --meta-url holt:///absolute/nokv-meta
nokv --advertise-endpoint 127.0.0.1:7750 \
  --object-bucket NAME --object-endpoint URL \
  serve --meta-url holt:///absolute/nokv-meta
```

One process owns the one Holt store. Restart reopens that same exact store.
This profile does not claim replacement-host recovery.

## Distributed FDB

Use the feature-enabled binary and exact cluster file/prefix:

```bash
nokv-fdb format --meta-url 'fdb:///absolute/fdb.cluster?prefix=nokv-prod'
nokv-fdb --root-id HEX32 --agent-id HEX32 \
  --object-bucket NAME --object-endpoint URL \
  provision --meta-url 'fdb:///absolute/fdb.cluster?prefix=nokv-prod'
nokv-fdb --node-id node-a --advertise-endpoint 10.0.0.1:7750 \
  --object-bucket NAME --object-endpoint URL \
  serve --meta-url 'fdb:///absolute/fdb.cluster?prefix=nokv-prod'
```

FDB is the catalog, route, session, lease, and metadata authority. The runtime
remains **NOT QUALIFIED** until the real live gates in
[`development/metadata-store-interface.md`](development/metadata-store-interface.md)
pass and retain evidence.

## Qualification labels

- Unit or dry-run only: `NOT QUALIFIED`.
- Missing binary, FDB cluster, object service, seed, or retained evidence:
  `NOT QUALIFIED`.
- Deterministic contract mismatch or integrity failure: `FAIL`.
- `PASS` requires the exact live scenario, binary digest, dependency identity,
  transcript, and evidence roles required by the acceptance contract.
