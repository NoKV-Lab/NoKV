<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV container image

NoKV keeps an agent's workspace (inputs, code, outputs, logs and lineage)
durable and versioned after the agent's sandbox is gone. It publishes
immutable artifact bytes to an S3-compatible object store through a
transactional, path-native metadata control plane. Agents use it through a
native CLI or the Python and Rust SDKs.

This image holds the `nokv` binary. Its entrypoint is `nokv`, and its default
command is `serve`, the owner process that serves one logical shard of roots.
The same image runs the operator commands (`provision`, `identity`) and every
Agent command. It runs as the unprivileged `nokv` user with its home and
working directory at `/var/lib/nokv`, and supports `linux/amd64` and
`linux/arm64`.

```bash
docker pull ghcr.io/nokv-lab/nokv:latest
docker run --rm ghcr.io/nokv-lab/nokv:latest version
```

## What a deployment needs

- **An S3-compatible bucket** (AWS S3, MinIO, RustFS, Ceph RGW). Pass
  `--object-bucket`, and `--object-endpoint` for anything but AWS. The CLI
  reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`
  from the environment. The owner needs read and write under its prefix;
  Agents need read only. The
  [object credentials guide](https://github.com/NoKV-Lab/NoKV/blob/main/docs/object-credentials.md)
  lists the keys and example policies.
- **A control plane** that records roots, routes and owner sessions: an
  etcd cluster (`--etcd-endpoint`), or a control directory on one host
  (`--control-dir`). A control directory must belong to the user that runs
  every NoKV process using it and must not be writable by group or others;
  NoKV creates it that way and refuses one that is not. Processes of
  different users, or on different hosts, share a control plane through etcd.
- **A metadata directory** for the owner's local write-ahead log, on a
  persistent volume.

## Run one owner on one host

The commands below put every container on one Docker network, so container
names resolve, and use a named volume for the control and metadata
directories. They assume an S3-compatible service reachable on that network as
`http://minio:9000`, with a bucket named `nokv` already created, and fixed
identities. Replace the identities with your own 32-hex-digit values and keep
them stable: the root, the Agent and the logical shard are durable names.

```bash
docker network create --subnet 172.28.0.0/24 nokv
IMAGE=ghcr.io/nokv-lab/nokv:latest
ROOT=11111111111111111111111111111111
AGENT=44444444444444444444444444444444
SHARD=22222222222222222222222222222222
COMMON="--root-id $ROOT --control-dir /var/lib/nokv/control --object-bucket nokv --object-endpoint http://minio:9000"
```

Provision the root once. This binds it to the Agent identity and to the
Workbench root its paths present under:

```bash
docker run --rm --network nokv -v nokv:/var/lib/nokv -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  "$IMAGE" $COMMON --agent-id "$AGENT" provision "$SHARD" --workbench-root /agents/demo/wb
```

Start the owner. Inside a container it must bind a non-loopback address and
advertise an IP address and port that clients reach it at. Clients connect to
the advertised address as given and do not resolve host names, so the owner
takes a fixed address on the network:

```bash
docker run -d --name nokv-owner --network nokv --ip 172.28.0.10 -p 7750:7750 -v nokv:/var/lib/nokv \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  "$IMAGE" $COMMON --node-id owner-1 --bind 0.0.0.0:7750 --advertise-endpoint 172.28.0.10:7750 \
  --metadata-create /var/lib/nokv/metadata serve
```

`--metadata-create` starts a new owner. When the same owner restarts on the
same volume, use `--metadata-reopen /var/lib/nokv/metadata` instead, with the
same `--ip`.

Use it as an Agent from the same host and volume:

```bash
docker exec nokv-owner nokv $COMMON --agent-id "$AGENT" \
  workbench workbench_create '{"id":"demo"}'
docker exec nokv-owner nokv $COMMON --agent-id "$AGENT" \
  workspace-path put demo outputs report.txt --operation-id "$(openssl rand -hex 16)" --text "hello"
docker exec nokv-owner nokv $COMMON --agent-id "$AGENT" \
  workspace-path stat demo outputs report.txt
```

Every command prints one JSON document on success, and one JSON failure line
on standard error otherwise, with a class, an exit code and the next action
to take. The [CLI contract](https://github.com/NoKV-Lab/NoKV/blob/main/docs/cli-contract.md)
lists them.

## Tenants, TLS and audit

For many principals on one root, register a tenant with `nokv identity`, bind
the root to it, and serve over RPC TLS with `--rpc-tls-cert` and
`--rpc-tls-key`. The owner then authenticates every client by its
certificate and authorizes every request by the principal's role (`reader`,
`writer`, `operator`). `--audit-log PATH` makes such an owner append one JSON
line per refused connection and denied request. The
[CLI contract](https://github.com/NoKV-Lab/NoKV/blob/main/docs/cli-contract.md)
and the
[control plane credentials guide](https://github.com/NoKV-Lab/NoKV/blob/main/docs/control-plane-credentials.md)
describe the setup, including etcd with mutual TLS.

## Links

- Source and issues: <https://github.com/NoKV-Lab/NoKV>
- Documentation: <https://github.com/NoKV-Lab/NoKV/tree/main/docs>
- Python SDK: <https://github.com/NoKV-Lab/NoKV/blob/main/crates/nokv-python/README.md>
- License: Apache-2.0
