---
title: NoKV
layout: home
hero:
  name: NoKV
  text: Agent-native distributed workspace and artifact storage.
  tagline: A stable Workbench, SDK, CLI, and MCP surface over path-primary Holt metadata and immutable object-backed revisions.
  image:
    src: /img/logo.png
    alt: NoKV
  actions:
    - theme: brand
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
    details: Preserve the complete 18-tool LingTai Workbench contract and expose the same semantics through SDK, custom CLI, and MCP adapters.
  - title: Path-primary Holt metadata
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
      <h3>Workbench · SDK · CLI · MCP</h3>
      <p>Agents keep familiar list/read/search/commit/snapshot/restore behavior
      while storage internals change underneath.</p>
    </div>
    <div class="nokv-card">
      <div class="nokv-card-kicker">Metadata layer</div>
      <h3>Canonical paths in Holt</h3>
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
  <pre class="nokv-code"><code>Agent SDK
  -&gt; route RootId to one logical shard
  -&gt; allocate publish operation + immutable revision
  -&gt; stream and verify object blocks
  -&gt; one fenced Holt command publishes:
       path + revision + references + indexes + event + replay result</code></pre>
  <div class="nokv-callout"><strong>Recovery is explicit.</strong>
  Leased snapshots pin MVCC history. Durable commits retain exact revisions.
  Restore stages a new Workbench incarnation and reveals it only after a
  verified member seal.</div>
</div>

## Documentation Map

- Product and interface: [Product Design](./product-design.md),
  [Architecture](./architecture.md), and
  [Workbench Contract](./workbench-contract.md).
- Storage and distribution: [Metadata Schema](./metadata-schema.md),
  [Object Layout](./object-layout.md), and
  [RustFS Provider Profile](./rustfs.md).
- Workloads and evidence: [AI Training Workload](./ai-training.md),
  [Benchmarks](./benchmarks.md),
  [LingTai Workbench Preflight](./lingtai-workbench-preflight.md), and
  [Workspace Acceptance](./development/workspace-acceptance.md).
- Development: [Code Contract](./development/code_contract.md),
  [`nokv-agent` Handbook](./development/nokv-agent.md),
  [PR Review Checklist](./development/pr_review_checklist.md), and
  [Path-Native Metadata Comparison](./development/path-native-metadata-comparison.md).
- Collaboration record: [NoKV x LingTai](./announcements/nokv-lingtai-design-partner.md)
  and [Chinese version](./announcements/nokv-lingtai-design-partner.zh-CN.md).

<div class="nokv-section nokv-section--tight">
  <div class="nokv-section-head nokv-section-head--center">
    <p class="nokv-eyebrow">First client</p>
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
    <a class="nokv-btn nokv-btn--primary" href="/workbench-contract">Workbench contract <span class="arrow">→</span></a>
    <a class="nokv-btn nokv-btn--ghost" href="/metadata-schema">Metadata schema</a>
    <a class="nokv-btn nokv-btn--ghost" href="/development/workspace-acceptance">Acceptance plan</a>
    <a class="nokv-btn nokv-btn--ghost" href="/development/path-native-metadata-comparison">Path model comparison</a>
  </div>
</div>
