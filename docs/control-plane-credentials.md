<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Control Plane Credentials

NoKV's control plane is etcd. Every process reads it: an Agent to find the
owner of its root, an owner to prove and renew its ownership and to read the
tenant records it enforces, and an operator to provision roots and administer
tenant identity. etcd can authenticate each of them by a client certificate
and limit each to the keys its role needs. This page lists those keys, the
etcd configuration that enforces them, and how each NoKV surface presents its
certificate. The single-host control directory (`--control-dir`) has no
credentials of its own: the directory's file permissions are its boundary, and
it is for one trusted host only. NoKV creates the directory and its files
readable and writable by their owner alone, and every command refuses a
control directory, `sessions` directory or record file that group or other
users can write, or that belongs to a user other than the one running the
command or root. A command that writes the directory also refuses one it
cannot create or write, and creates nothing inside a directory it refuses;
`serve` makes all of these checks before it prepares its metadata store, and
`provision` before it writes its first record. Someone else may have changed
the records of such a directory, so each refusal is `operator_required` (exit
9, `next_action` `operator`) and names the path and the fix: an operator checks
the records, fixes the permissions, and then repeats the command. A running
owner whose control directory becomes unsafe stops serving and exits with that
refusal, and does not resume by itself. One that can no longer write the
directory serves on until its next control-store write (a shared recovery
publication, or the release of its session when it stops), and then exits with
that refusal too. Once the permissions are fixed, a restart by its supervisor
or the operator serves again. Every process that uses a control directory
therefore runs as its owner; processes of different users share a control
plane through etcd.

## What each role touches

All keys live under the configured prefix (`--etcd-key-prefix`, default
`/nokv/control`). `<root>`, `<shard>` and `<tenant>` are the lowercase
hexadecimal `RootId`, `LogicalShardId` and `TenantId`.

| Role | Keys | Access |
| --- | --- | --- |
| Agent (every CLI command except `serve`, `provision` and `identity`) | `roots/<root>`, `root-object-namespaces/<root>`, `root-agents/<root>`, `root-presentations/<root>` for each root it uses | read, exact keys |
| Agent | `logical-shards/<shard>` for the shard of each such root | read, exact key |
| Owner (`serve`) | `logical-shards/<shard>`, `logical-shard-recovery/<shard>`, `sessions/<shard>` of its shard | readwrite |
| Owner (`serve`) | `roots/`, `root-object-namespaces/`, `root-agents/`, `root-tenants/`, `root-presentations/` | read, prefix |
| Owner (`serve`) | `tenants/<tenant>` and everything below it, for each tenant whose roots the shard serves | read, prefix |
| Operator (`provision`, `identity`) | everything under the prefix | readwrite (etcd's `root` role) |

An Agent reads single keys and never lists a prefix. It never reads an owner's
recovery frontier (`logical-shard-recovery/`), an owner session (`sessions/`),
a root's tenant binding, or any tenant record, and it writes nothing: the
route it needs is the routing value of `logical-shards/<shard>`, which the
client reads on its own, apart from the recovery frontier that only the owner
reads. An owner writes only its own shard's three keys; the root placements,
bindings and tenant records it serves from are read-only to it.

## Configuring etcd

Start etcd with a server certificate whose `subjectAltName` carries the
address clients connect to, with the CA that signs every client certificate,
and with client-certificate authentication:

```shell
etcd --listen-client-urls https://127.0.0.1:2379 --advertise-client-urls https://127.0.0.1:2379 \
  --cert-file etcd.pem --key-file etcd.key --trusted-ca-file ca.pem --client-cert-auth=true
```

With `--client-cert-auth`, etcd authenticates a request by its client
certificate's Common Name, as that etcd user, with no password; a user created
with `--no-password` accepts nothing else. etcd's gRPC proxy and gRPC gateway
do not support this, so NoKV processes connect to etcd members directly.

Create the users and roles before enabling authentication. The operator's
certificate has Common Name `root`, so that `etcdctl` and `nokv identity` can
administer the cluster with the same identity:

```shell
etcdctl="etcdctl --endpoints https://127.0.0.1:2379 --cacert ca.pem --cert root.pem --key root.key"
$etcdctl user add root --no-password
$etcdctl role add root
$etcdctl user grant-role root root

# One owner per logical shard.
$etcdctl role add nokv-owner-SHARD
for family in logical-shards logical-shard-recovery sessions; do
  $etcdctl role grant-permission nokv-owner-SHARD readwrite /nokv/control/$family/SHARD
done
for family in roots root-object-namespaces root-agents root-tenants root-presentations; do
  $etcdctl role grant-permission nokv-owner-SHARD --prefix=true read /nokv/control/$family/
done
$etcdctl role grant-permission nokv-owner-SHARD --prefix=true read /nokv/control/tenants/TENANT
$etcdctl user add owner-SHARD --no-password
$etcdctl user grant-role owner-SHARD nokv-owner-SHARD

# One role per root, granted to every Agent that uses the root.
$etcdctl role add nokv-agent-ROOT
for family in roots root-object-namespaces root-agents root-presentations; do
  $etcdctl role grant-permission nokv-agent-ROOT read /nokv/control/$family/ROOT
done
$etcdctl role grant-permission nokv-agent-ROOT read /nokv/control/logical-shards/SHARD
$etcdctl user add AGENT --no-password
$etcdctl user grant-role AGENT nokv-agent-ROOT

$etcdctl auth enable
```

`role add root` fails harmlessly on an etcd that created the role itself.
The tenant grant has no trailing slash: the owner reads the tenant record
`tenants/TENANT` as well as the records below it, and since identifiers have
a fixed width, no other tenant's keys share that prefix.

## How each surface presents its certificate

Every CLI command that names etcd routing takes the same three options, and
`https://` endpoints require them:

```shell
nokv --etcd-endpoint https://127.0.0.1:2379 \
  --etcd-tls-ca ca.pem --etcd-tls-cert AGENT.pem --etcd-tls-key AGENT.key \
  --root-id ROOT ... workbench workbench_stat '{"id":"run-1","path":"result.bin"}'
```

`serve` presents the owner's etcd certificate the same way, next to its
`--rpc-tls-cert` and `--rpc-tls-key` for workspace RPC; `provision` and
`identity` present the operator's. One certificate may serve both planes: the
workspace RPC owner pins a client certificate by fingerprint
(`nokv identity credential add --certificate`), and etcd authenticates its
Common Name, so an Agent can hold one certificate and key and pass them to
both `--rpc-tls-*` and `--etcd-tls-*`. The two are configured separately
because they need not be the same.

The Rust SDK configures the same identity with
`EtcdRouteOptions::new(endpoints).with_tls(EtcdTlsConfig::new(ca_pem, cert_pem, key_pem))`,
and the Python SDK with
`RoutingConfig.etcd(endpoints, tls_ca_pem=..., tls_cert_pem=..., tls_key_pem=...)`
(PEM `bytes`, given together). In both, the PEM bytes are held in memory,
shared between clones, and never printed.

## Failures

A control-plane refusal is reported before any request reaches an owner, with
the classes of the [CLI contract](cli-contract.md):

| What happened | `class` | Exit |
| --- | --- | --- |
| etcd rejected the certificate in the TLS handshake (not signed by etcd's trusted CA, or none presented) and the client received etcd's alert | `unauthenticated` | 10 |
| The same rejection, but the connection closed before the alert reached the client | `transient` | 5 |
| The certificate is trusted, but its Common Name is no etcd user, or the user's roles do not grant the key the command needs | `permission_denied` | 11 |
| etcd's own certificate is not signed by `--etcd-tls-ca` | `invalid_input` | 2 |
| An `--etcd-endpoint` that is not an `http://` or `https://` URL (scheme in lowercase) naming a host, with an optional port in 1..=65535 (refused before any connection) | `invalid_input` | 2 |
| An `--etcd-endpoint` that carries credentials (`user:password@`): etcd never receives them; the refusal shows the endpoint with them redacted | `invalid_input` | 2 |
| etcd is unreachable, or an `http://` endpoint names a TLS listener | `transient` | 5 |
| A `--control-dir` directory, its `sessions` directory or a record file that group or other users can write, that another user owns, or that this process cannot write | `operator_required` | 9 |

etcd answers a trusted certificate with no user the same way as a user whose
role lacks the key, "permission denied", so NoKV reports both as
`permission_denied`; etcd's audit of the request tells them apart. When etcd
rejects the certificate itself it sends a TLS alert (`UnknownCA`,
`BadCertificate`) and closes the connection; whether the client reads the
alert or the closed connection first depends on the platform and timing, so
the same rejection is `unauthenticated`, naming the alert, or `transient`.
Both leave nothing applied and reach no owner; a `transient` answer that
repeats on every attempt with the same certificate is this rejection, and
etcd's log names it. A plaintext endpoint against a TLS listener is a reset
connection to the client, which cannot tell it from an unreachable server.
Deleting a user, or revoking its role, denies that user's next request.

## Scope

- Only certificate authentication is supported: NoKV passes no etcd user name
  or password.
- Roles are granted per key or per prefix by the operator; NoKV does not
  create or verify them. A deployment that grants an Agent a wider role gets
  a wider boundary.
- A plaintext etcd (`http://` endpoints) remains supported for trusted
  single-host and test deployments; it authenticates nobody, and
  [SECURITY.md](../SECURITY.md) says what boundary that leaves.
