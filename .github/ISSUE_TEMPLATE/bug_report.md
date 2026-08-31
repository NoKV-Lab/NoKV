---
name: Bug Report
about: Create a report to help us improve NoKV
title: '[BUG] '
labels: bug
assignees: ''

---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->


> Security vulnerabilities should not be reported through a public bug issue.
> Follow the [NoKV security policy](https://github.com/NoKV-Lab/NoKV/blob/main/SECURITY.md)
> and use private vulnerability reporting. Redact credentials, object data,
> tenant identifiers, and other sensitive material from all public reports.

## 🐛 Bug Description
A clear and concise description of what the bug is.

## 🛠 Reproduction Steps
Steps to reproduce the behavior:
1. Navigate to '...'
2. Call method '...' with arguments '...'
3. See error '...'

## 📋 Expected Behavior
A clear and concise description of what you expected to happen.

## 📸 Screenshots / Logs
If applicable, add screenshots or paste logs to help explain your problem.

## 💻 Environment
 - OS: [e.g. Linux, macOS]
 - Rust Version: [e.g. 1.88]
 - NoKV Version: [e.g. commit SHA or release tag]
 - Surface: [e.g. native CLI, Python SDK, Rust SDK, Workbench contract, server, materialize/collect]
 - Deployment topology: [e.g. local/direct or routed; include root id, logical shard, placement generation, owner, and epoch if relevant]
 - Object backend and version: [e.g. S3-compatible service and version]
 - Command and relevant configuration: [redact secrets]

## 📎 Diagnostic Evidence

Paste the smallest sanitized log or trace that demonstrates the problem. For
routing or ownership failures, include the normalized path, root id, logical
shard, placement generation, owner endpoint, and epoch when available.

## 🧐 Additional Context
Add any other context about the problem here.
