---
title: NoKV
layout: home
hero:
  name: NoKV
  text: Agent-native distributed workspace and artifact storage.
  tagline: Durable workspaces and recoverable append identities through a native CLI and direct SDKs.
  image:
    src: /img/logo.png
    alt: NoKV
  actions:
    - theme: brand
      text: Durable Append
      link: /append
    - theme: alt
      text: Architecture
      link: /architecture
    - theme: alt
      text: Workbench Contract
      link: /workbench-contract
    - theme: alt
      text: Metadata Schema
      link: /metadata-schema
    - theme: alt
      text: Acceptance Plan
      link: /development/workspace-acceptance
features:
  - title: Stable Agent surface
    details: Use the native CLI first, Python for embedded callers and Rust for native integrations. The frozen 18-tool Workbench facade and the explicit append recovery APIs share one metadata lifecycle.
  - title: Durable append identity
    details: Save an operation ID and its inputs before dispatch. Replacement workers can retrieve the original receipt or resume safely cleaned work under that same ID.
  - title: Path-primary metadata
    details: One normalized full path is namespace truth. Exact artifacts use point reads; child listing uses component-safe delimiter scans.
  - title: Immutable revisions
    details: Stream bytes to S3-compatible storage first, then atomically publish a revision, path, indexes, event, and deterministic replay result.
  - title: Root-local distribution
    details: Persist each Agent root on one logical shard, fence physical owners by epoch, and keep commit, restore, and GC reference ownership local.
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div class="nokv-section">
  <div class="nokv-section-head nokv-section-head--center">
    <p class="nokv-eyebrow">Product boundary</p>
    <h2 class="nokv-h2">Artifact semantics for Agents, without POSIX baggage</h2>
    <p class="nokv-lead">NoKV gives datasets, scripts, logs, outputs, reports,
    checkpoints, and provenance stable path-shaped identities. It intentionally
    does not target FUSE, complete POSIX, CSI, or transparent fsspec access.</p>
  </div>
  <div class="nokv-grid-3">
    <div class="nokv-card">
      <div class="nokv-card-kicker">Application surface</div>
      <h3>CLI · Python SDK · Rust SDK</h3>
      <p>Downstream skills use the native CLI by default; embedded callers use
      the Python SDK, and native integrations use the Rust SDK. The full CLI
      includes the stable 18-tool Workbench facade plus explicit append
      submission and recovery commands.</p>
    </div>
    <div class="nokv-card">
      <div class="nokv-card-kicker">Metadata layer</div>
      <h3>Canonical ordered paths</h3>
      <p>Workspace incarnations gate visibility. Full relative paths are
      authoritative ordered keys; indexes remain derived.</p>
    </div>
    <div class="nokv-card">
      <div class="nokv-card-kicker">Body layer</div>
      <h3>Revision-owned objects</h3>
      <p>Immutable blocks live in S3-compatible storage. Strong references,
      epochs, and fenced GC make sharing and restore safe.</p>
    </div>
  </div>
</div>

<div class="nokv-section nokv-section--tight">
  <div class="nokv-section-head nokv-section-head--center">
    <p class="nokv-eyebrow">Core flow</p>
    <h2 class="nokv-h2">Upload bytes first; publish identity last</h2>
  </div>
  <pre class="nokv-code"><code>Native CLI or direct Python SDK
  -&gt; route RootId to one logical shard
  -&gt; allocate publish operation + immutable revision
  -&gt; stream and verify object blocks
  -&gt; one fenced metadata command publishes:
       path + revision + references + indexes + event + replay result</code></pre>
  <div class="nokv-callout"><strong>Recovery is explicit.</strong>
  Leased snapshots pin MVCC history. Durable commits retain exact revisions.
  Restore stages a new Workbench incarnation and reveals it only after a
  verified member seal. For an interrupted append, query its saved logical ID;
  inspect the staged ledger and request owner cleanup when needed.
  <a href="/append">Follow the append recovery guide</a>.</div>
</div>

## Documentation Map

- Append a durable event: [Caller guide](./append.md),
  [Product Contract](./development/append-product-spec.md), and
  [Qualification Record](./development/append-qualification.md).

- Product and interface: [Product Design](./product-design.md),
  [Architecture](./architecture.md), and
  [Workbench Contract](./workbench-contract.md).
- Storage and distribution: [Metadata Schema](./metadata-schema.md),
  [Object Layout](./object-layout.md), and
  [RustFS Provider Profile](./rustfs.md).
- Workloads and evidence: [AI Training Workload](./ai-training.md),
  [Benchmarks](./benchmarks.md),
  [Live Deployment Preflight](./workbench-preflight.md), and
  [Workspace Acceptance](./development/workspace-acceptance.md).
- Development: [Code Contract](./development/code_contract.md),
  [`nokv-agent` Handbook](./development/nokv-agent.md),
  [PR Review Checklist](./development/pr_review_checklist.md),
  [Change Governance](./development/change-governance.md),
  [Path-Native Metadata Comparison](./development/path-native-metadata-comparison.md), and
  [LoopX Stage 2A Stack](./development/loopx-stage2a-stack.md).
- Storage architecture: [Metadata Store Interface](./development/metadata-store-interface.md).
- Historical collaboration record: [NoKV x LingTai](./announcements/nokv-lingtai-design-partner.md)
  and [Chinese version](./announcements/nokv-lingtai-design-partner.zh-CN.md).

<div class="nokv-section nokv-section--tight">
  <div class="nokv-section-head nokv-section-head--center">
    <p class="nokv-eyebrow">Research workflow</p>
    <h2 class="nokv-h2">Reproducible reconstruction runs</h2>
    <p class="nokv-lead">Seal one immutable input dataset, materialize verified
    files for the local scientific executable, collect declared outputs, and
    compare multiple runs through shared lineage and metadata queries.</p>
  </div>
</div>

<div class="nokv-section nokv-section--tight nokv-cta">
  <div class="nokv-section-head nokv-section-head--center">
    <h2 class="nokv-h2">Read the contracts before the code</h2>
    <p class="nokv-lead">The Workbench contract fixes the upper behavior. The
    metadata schema fixes storage safety, and the acceptance plan defines the
    evidence required for release.</p>
  </div>
  <div class="nokv-actions">
    <a class="nokv-btn nokv-btn--primary" href="/append">Durable append guide <span class="arrow">→</span></a>
    <a class="nokv-btn nokv-btn--ghost" href="/workbench-contract">Workbench contract</a>
    <a class="nokv-btn nokv-btn--ghost" href="/metadata-schema">Metadata schema</a>
    <a class="nokv-btn nokv-btn--ghost" href="/development/workspace-acceptance">Acceptance plan</a>
    <a class="nokv-btn nokv-btn--ghost" href="/development/path-native-metadata-comparison">Path model comparison</a>
  </div>
</div>
