# Threat model: tenant identity and authorization

This document applies STRIDE to the workspace RPC path an Agent uses to reach
a NoKV owner, to the control plane the owner and the CLI read, and to the
object store the owner writes on the Agent's behalf. Every threat names the
mechanism that counters it and the test that exercises that mechanism, so a
change to either can be checked against this table. The tests are the live
gates under `scripts/workbench/` (the CLI contract gate `s*`, the RPC TLS gate
`t*`, the control-plane gate `c*`, the Python contract gate `p*`) and the
unit tests named by crate.

## Assets and trust boundaries

- **Roots and their metadata**: the durable state of one root (workspaces,
  paths, artifacts, commits, snapshots, operations), served by one owner per
  logical shard. One owner serves one tenant; every root of its shard is bound
  to that tenant (`nokv serve` refuses to start otherwise, and a root of
  another tenant provisioned later is left unmounted, `t15`).
- **Object bodies**: artifact blocks in the object store. Since R5a the owner
  writes them with its own credentials; an Agent holds read-only object
  credentials and reads through a read-only endpoint (`t14`).
- **Identity records**: tenants, principals, credentials (certificate
  fingerprints with an expiry), role bindings and root bindings, kept in the
  control store (`tenants/<tenant>/...`, `root-tenants/`). Written only by the
  operator's `nokv identity` commands; read by owners (refreshed every second)
  and by `provision`.
- **Routes**: `roots/<root>`, `logical-shards/`, `root-presentations/` in the
  control store. Written by `provision` and by owners; read by every client.
- **Operations**: an operation id names one write and records the principal
  that began it (`initiator`). The id is the Agent's replay key.

Boundaries:

1. **Agent process → owner** over TCP. With `--rpc-tls-cert`, the owner
   accepts TLS 1.3 only, with a client certificate whose SHA-256 fingerprint
   is a current credential of the served tenant. Without TLS, the owner serves
   a deployment identity (`AgentId`) that is a misconfiguration check, not an
   authentication credential; a tenant-bound root is never served in
   plaintext (`t0b`).
2. **Any process → control store**. With a control directory there is no
   access control beyond file permissions. With etcd, R6a binds the owner,
   the operator and each Agent principal to an etcd user with a least-privilege
   role (`c*`).
3. **Owner → object store** with the owner's credentials; **Agent → object
   store** read-only.

## STRIDE table

| Threat | Mechanism | Evidence |
| --- | --- | --- |
| **S**poofing: a client without a certificate reaches a TLS owner | The owner answers a plaintext hello with `TlsRequired` and closes; the CLI reports `unauthenticated` (exit 10) | `t3` |
| Spoofing: an unregistered certificate | The fingerprint is not a credential of the tenant: refused at the handshake, `unauthenticated` | `t2` |
| Spoofing: a certificate current in another tenant | Same refusal; the envelope carries no route, endpoint, epoch or generation | `t21`, Pi `m4` |
| Spoofing: a revoked credential, on a new or an open connection | The owner rereads the tenant's records every second; a revoked credential is refused at the next handshake and the next request of an open connection, which is then closed | `t5`, `t18`, Pi `m5`, Hermes `hm5`, `access::a_revoked_or_expired_credential_stops_an_open_connection` |
| Spoofing: an expired credential | Expiry is part of the credential record; the owner treats an expired credential like a revoked one, with no operator action | `t17` |
| Spoofing: a client that trusts the wrong owner | The client verifies the owner's certificate against the CA it was given and the name it expects; a mismatch fails before any request (`operator_required`) | `t4` |
| Spoofing: a plaintext owner for a tenant-bound root | `nokv serve` without `--rpc-tls-cert` refuses to serve a root a tenant owns | `t0b` |
| **T**ampering: a reader principal writes | Every RPC variant is classified (`access::classify`, an exhaustive match) and checked against the principal's highest role on the root; the Workbench mutation tools, snapshot and restore tools, and `workspace-path put/append/rename/remove` are all refused `permission_denied` (exit 11) with no effect | `t6`, `t16`, Pi `m2`, Hermes `hm2`, `access::each_role_permits_its_own_requests_and_those_of_lesser_roles` |
| Tampering: one principal continues, completes or aborts another's operation | An operation keeps its initiator; another principal presenting the id with other inputs gets `identity_conflict`, with the same inputs is answered from the existing operation without a write | `t9`, `t20` |
| Tampering: another writer renews or retires a snapshot it did not mint | Only the minter or an operator may | `t11` |
| Tampering: an Agent writes object bodies directly | Agent object credentials are read-only; the owner uploads every block (R5a) | `t14`, `c4` (etcd roles) |
| Tampering: a forged commit projection in a raw RPC | The owner recomputes the projection digest from the staged state and refuses a commit whose projection disagrees | `executor` unit tests (forged inline publication) |
| Tampering: a client of another root or tenant writes the control store | etcd roles: an Agent user cannot write `roots/`, `tenants/` or another tenant's keys; a control directory relies on file permissions and is documented as single-host | `c3`, `c4` |
| Tampering: an old-format store opened by a new owner | format 15 refuses to open a 0.12.x store and writes nothing; there is no migration | `nokv-meta` codec and recovery tests |
| **R**epudiation: who made a write | Every operation records the principal that began it; `operation status` reports it to any reader of the root; a put through the Workbench tools carries the caller's operation id | `t10`, `t13`, `t20`, Pi `m1`, `m3`, Hermes `hm1` |
| Repudiation: a write replayed by another principal | A replay returns the original receipt and keeps the original initiator | `t9`, `t12` |
| Repudiation: who tried what and was refused | `serve --audit-log` appends one JSON line per refused connection and per denied request: the peer, the certificate fingerprint, the principal, the root, the RPC, the role it needs, and the reason | `t22`, `nokv-server` `every_refusal_of_a_tls_owner_is_appended_to_its_audit_log` |
| **I**nformation disclosure: probing another tenant's root for its route | The owner refuses before any request; the refusal names the credential, nothing else. Under etcd, an Agent of tenant B cannot read `roots/<root of A>` | `t21`, `c3`, Pi `m4` |
| Information disclosure: private keys in evidence | Gates delete private keys from evidence directories on close; wrappers in the downstream harnesses never print credentials (canary check) | `TlsStack.close`, Hermes harness `credential_canary_absent` |
| Information disclosure: object bodies readable by another tenant | Object credentials are per deployment and read-only for Agents; cross-tenant object isolation is not claimed (one bucket prefix per deployment) | documented limitation, see `docs/object-credentials.md` |
| **D**enial of service: a client that opens a connection and trickles bytes | The handshake has a deadline (`--handshake-timeout-millis`, 5 s by default); a stalled connection is closed at the deadline and does not keep others from being served | `t19` |
| Denial of service: too many connections | `--max-inflight-connections` bounds admitted connections | serve option, unit tests in `nokv-server::server` |
| Denial of service: a revoked principal keeps writing | Revocation takes effect within the owner's refresh interval plus one in-flight request | `t8`, `t20`, Pi `m5` |
| Denial of service: an owner that cannot refresh its policy | After `MAX_POLICY_AGE` without a successful refresh the owner refuses every request as `PolicyStale` rather than serving a stale policy | `access` unit tests |
| **E**levation of privilege: a root-scoped writer on another root | A root role covers only its root; the highest applicable role wins | `access::a_root_role_covers_only_its_root_and_the_highest_role_wins`, `t7` |
| Elevation: a principal of tenant B served by an owner of tenant A | An owner serves one tenant; a root of another tenant on its shard is not mounted | `t15`, `nokv serve` refusal at start |
| Elevation: an Agent provisioning or serving | `provision` and `serve` are operator commands; the downstream wrappers refuse them, and under etcd the Agent user cannot write the keys they need | `c*`, harness wrappers |
| Elevation: a presentation root chosen by the client | The Workbench root is bound by `provision` and read from the control plane; a client that passes another is refused `invalid_input` | `p7`, R1 `RootPresentation` tests |

## Out of scope and known gaps

- **Control directory deployments** have no authentication on the control
  store; the tenant model there protects the owner's RPC surface, not the
  files. Use etcd with mTLS (R6a) when more than one trust domain shares a
  host.
- **Object-store isolation between tenants** is by deployment (bucket and
  prefix), not enforced by NoKV.
- **Audit log integrity**: the refusal audit log is a file the owner's user
  can edit; it is not tamper-evident. Ship it to a store that user cannot
  write when that matters.
- **Forged projections** are covered by unit tests only; the live gates drive
  the CLI, which cannot send a crafted RPC.
- **Continuing another principal's pending append** (`operation recover`)
  is checked by the executor's unit tests only. The live gates cannot leave
  an append pending under TLS: an owner killed during the upload leaves no
  operation record, so a later `recover` by anyone is `not_found`, and the
  frame-level fault proxy cannot read TLS records. A committed append's
  `recover` by another principal reports the committed state, as
  `operation status` does for any reader.
