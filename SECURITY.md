<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Security Policy

## Supported Versions

NoKV is still evolving quickly. Security fixes are expected to land on the latest release line and `main`.

| Version | Supported |
| --- | --- |
| `main` | Yes |
| latest tagged release line | Yes |
| older releases | Best effort only |

See the [latest NoKV release](https://github.com/NoKV-Lab/NoKV/releases/latest)
for the current stable release line. Pre-releases are supported only when they
are explicitly named in a security advisory or release note.

## Current Security Boundary

NoKV's metadata service and control-plane endpoints currently assume a trusted
deployment environment. The current `main` branch can require TLS 1.3 clients
whose certificate is a current credential of the owner's tenant, and then
authorizes every request by the roles of that principal on the request's root.
It does not enforce a live workspace-freeze policy at the service boundary.
Without TLS it authenticates and authorizes no caller, and it refuses to serve
a root that a tenant owns.

The control plane is etcd, and NoKV connects to it over mutual TLS when a
deployment runs etcd with client-certificate authentication: each owner and
Agent process presents its own certificate, etcd authenticates its Common
Name as an etcd user, and that user's roles bound it to the keys its role
needs, so an Agent can read only its root's routing records and an owner can
write only its shard ([control plane credentials](docs/control-plane-credentials.md)).
NoKV does not create those users or roles; a plaintext etcd, or a role wider
than the documented one, authenticates and bounds nobody.

Every object write is the owner's: a client never holds object write
credentials, and sends each block to the owner, which verifies it against the
staged plan before writing it. Give Agent processes prefix-scoped read-only
object credentials (GET and HEAD under the root's prefix); the owner's
credentials write ([object store credentials](docs/object-credentials.md)
lists the keys each role touches and example policies). The object store
itself still authenticates only what the deployment's IAM gives each
credential.

- Workbench-root jailing only contains paths.
- Without TLS, the native CLI's `RootId`-to-`AgentId` check only catches
  misconfigured routing. The SDKs and raw RPC clients do not perform it.

Neither is authentication or authorization.

Deployments must provide network isolation and transport security, protect
service credentials, and configure object-store IAM and encryption appropriate
to their environment. Do not expose NoKV server or control-plane endpoints
directly to untrusted networks.

Server-enforced role-based access control covers workspace RPC over TLS, and
every durable operation records the principal that started it. Access control
for the control plane is etcd's own, configured per the document above
([code contract](docs/development/code_contract.md#tenant-identity-and-access-control)).
Until a release note states otherwise, the boundary above applies to every
release.

## Reporting a Vulnerability

Preferred path:

1. Use [GitHub private vulnerability reporting](https://github.com/NoKV-Lab/NoKV/security/advisories/new).
2. Include the affected version, impact, reproduction steps, and any proof-of-concept details needed to reproduce the issue.

If private reporting is not available:

1. Open a minimal public issue asking for a private security follow-up.
2. Do **not** include exploit details, secrets, crash artifacts with sensitive data, or full weaponized proof-of-concept material in the public issue.

## What to Include

Please include as much of the following as possible:

- affected commit, branch, tag, or release
- component or package path
- configuration needed to trigger the issue
- reproduction steps
- impact assessment
- suggested fix or mitigation, if known

## Response Expectations

- Initial acknowledgement target: within 7 days
- Status update target: within 14 days when the report is actionable

These are project targets, not contractual SLAs.

## Disclosure

Please give the maintainer reasonable time to assess and fix the issue before public disclosure.

When a fix is available, the project may disclose:

- affected versions
- impact summary
- mitigation guidance
- fix commit or release
