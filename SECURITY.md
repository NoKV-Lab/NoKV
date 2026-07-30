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

NoKV's metadata service and control endpoints currently assume a trusted
deployment environment. The current `main` branch does not enforce tenant
identity, role-based access control, or a live workspace-freeze policy at the
service boundary. Path and workbench-root jails constrain namespace access;
they are not a substitute for tenant authentication or authorization.

Deployments must provide network isolation and transport security, protect
service credentials, and configure object-store IAM and encryption appropriate
to their environment. Do not expose NoKV control or metadata endpoints directly
to untrusted networks.

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
