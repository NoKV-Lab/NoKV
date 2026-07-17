---
title: NoKV
layout: home
hero:
  name: NoKV
  text: A Rust filesystem for AI training and agent workspaces.
  tagline: Holt-backed metadata, S3-compatible immutable object bodies, FUSE and SDK paths — a self-contained filesystem with no separate database to run.
  image:
    src: /img/logo.png
    alt: NoKV
  actions:
    - theme: brand
      text: Architecture
      link: /architecture
    - theme: alt
      text: Quick Start
      link: /rustfs
    - theme: alt
      text: LingTai Workbench
      link: /lingtai-workbench-preflight
    - theme: alt
      text: Benchmarks
      link: /benchmarks
features:
  - title: Self-contained metadata
    details: A path-native metadata engine (Holt) built in. No Redis, TiKV, or external database to operate — you run a filesystem, not a filesystem plus a database.
  - title: Atomic checkpoints
    details: Object bytes land first, then metadata publishes atomically as a new generation. Readers see a complete checkpoint or the previous one — never a half-written one.
  - title: Built for AI training
    details: ~127K metadata ops/s, single-scan directory listing, immutable cacheable blocks, dataset snapshots, and typed watch events — shaped around datasets, checkpoints, and agent workspaces.
  - title: Object-backed bodies
    details: File bytes are immutable blocks in S3, RustFS, MinIO, or Ceph RGW. Elastic, cheap, zero-ops byte durability — NoKV owns the namespace.
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div class="nokv-section">
  <div class="nokv-section-head nokv-section-head--center">
    <p class="nokv-eyebrow">How it works</p>
    <h2 class="nokv-h2">A filesystem — not a filesystem plus a database</h2>
    <p class="nokv-lead">NoKV owns namespace truth, metadata transactions, snapshots, watches, and object-reference GC. The object store owns byte durability. The metadata engine is built in — no separate Redis, MySQL, or TiKV cluster to run.</p>
  </div>
  <div class="nokv-grid-3">
    <div class="nokv-card">
      <div class="nokv-card-kicker">Application surface</div>
      <h3>FUSE · SDK · CLI</h3>
      <p>Mount it, call the Rust SDK, or drive it from <code>nokv</code>. One namespace, three front doors.</p>
    </div>
    <div class="nokv-card">
      <div class="nokv-card-kicker">Metadata layer</div>
      <h3>Holt engine</h3>
      <p>Path-native inode &amp; dentry metadata in a built-in ART engine — <code>MetadataCommand</code> transactions, snapshots, and typed watches.</p>
    </div>
    <div class="nokv-card">
      <div class="nokv-card-kicker">Body storage</div>
      <h3>S3-compatible objects</h3>
      <p>File bytes are immutable blocks in RustFS, MinIO, Ceph RGW, or AWS S3 — elastic, cheap, zero-ops durability.</p>
    </div>
  </div>
  <div class="nokv-callout"><strong>Atomic checkpoint publish.</strong> Object bytes upload first, then one metadata commit publishes the dentry, inode, and body manifest as a new generation. A crash in between leaves orphan objects for GC — never a corrupt namespace.</div>
</div>

<div class="nokv-section nokv-section--tight">
  <div class="nokv-section-head nokv-section-head--center">
    <p class="nokv-eyebrow">Benchmarks</p>
    <h2 class="nokv-h2">Single-node, measured end to end</h2>
    <p class="nokv-lead">Release build, full server + RPC + Holt path, local RustFS backend. Distributed numbers need separate runs.</p>
  </div>
  <div class="nokv-stats">
    <div class="nokv-stat"><span class="nokv-stat-num">~127K</span><span class="nokv-stat-label">metadata ops/s — create, batched</span></div>
    <div class="nokv-stat"><span class="nokv-stat-num">~1.1 GB/s</span><span class="nokv-stat-label">checkpoint publish — 1 MiB blocks, conc 16</span></div>
    <div class="nokv-stat"><span class="nokv-stat-num">~3,000</span><span class="nokv-stat-label">dataset samples/s — 16 KiB, conc 16</span></div>
    <div class="nokv-stat"><span class="nokv-stat-num">~1.5 KB</span><span class="nokv-stat-label">resident metadata per file</span></div>
  </div>
  <p class="nokv-fine" style="text-align: center; margin-top: 20px;">A single directory of 65k entries holds the same throughput — the path-native ART doesn't degrade on big directories.</p>
</div>

<div class="nokv-section nokv-section--tight">
  <div class="nokv-section-head nokv-section-head--center">
    <p class="nokv-eyebrow">Comparison</p>
    <h2 class="nokv-h2">Same skeleton as JuiceFS — different metadata</h2>
    <p class="nokv-lead">Object-backed, metadata/data split, FUSE and SDK paths. The difference is the metadata layer and the semantics.</p>
  </div>
  <table class="nokv-table">
    <thead><tr><th>&nbsp;</th><th>JuiceFS</th><th>NoKV</th></tr></thead>
    <tbody>
      <tr><td>Metadata engine</td><td>rents a general DB (Redis / MySQL / TiKV)</td><td><strong>built-in, path-native</strong> (Holt)</td></tr>
      <tr><td>Checkpoint publish</td><td>general POSIX</td><td><strong>first-class atomic primitive</strong></td></tr>
      <tr><td>Block model</td><td>slice + compaction</td><td>immutable + new-generation</td></tr>
      <tr><td>AI-native primitives</td><td>bolted on</td><td>snapshots, typed watch, GC floor</td></tr>
      <tr><td>POSIX completeness</td><td>full</td><td>partial (single-node)</td></tr>
      <tr><td>Maturity</td><td>production, billions of files</td><td>young — single-node, no HA yet</td></tr>
    </tbody>
  </table>
</div>

<div class="nokv-section nokv-section--tight">
  <div class="nokv-section-head nokv-section-head--center">
    <p class="nokv-eyebrow">Quick start</p>
    <h2 class="nokv-h2">Running in a handful of commands</h2>
  </div>
  <pre class="nokv-code"><code><span class="c"># Build the CLI</span>
cargo build --release -p nokv --bin nokv
<span class="c"># A local S3 endpoint (RustFS) — bucket `nokv`, dev creds rustfsadmin</span>
rustfs server --address 127.0.0.1:9000 \
  --access-key rustfsadmin --secret-key rustfsadmin ./rustfs-data &amp;
<span class="c"># Initialize, publish an artifact, read it back</span>
nokv --object-backend rustfs init
nokv --object-backend rustfs put-artifact /runs/1/ckpt.bin ./ckpt.bin
nokv --object-backend rustfs cat /runs/1/ckpt.bin &gt; restored.bin
<span class="c"># Mount with FUSE (macOS needs macFUSE)</span>
nokv --object-backend rustfs mount /tmp/nokv-mount</code></pre>
</div>

<div class="recognition">
  <p class="recognition-label">Recognized in the AI-native storage ecosystem</p>
  <div class="recognition-logos">
    <a href="https://landscape.cncf.io/?group=projects-and-products&amp;item=runtime--cloud-native-storage--nokv" target="_blank" rel="noreferrer">
      <img src="/img/recognition/cncf.svg" alt="CNCF Landscape" />
    </a>
    <a href="https://dbdb.io/db/nokv" target="_blank" rel="noreferrer">
      <img src="/img/recognition/dbdb.svg" alt="DBDB.io Database of Databases" />
    </a>
  </div>
</div>

<div class="nokv-section nokv-section--tight nokv-cta">
  <div class="nokv-section-head nokv-section-head--center">
    <h2 class="nokv-h2">Build on a self-contained filesystem</h2>
    <p class="nokv-lead">Apache-2.0. A usable single-node object-backed filesystem today — a distributed metadata layer with Holt as the shard-local state machine next.</p>
  </div>
  <div class="nokv-actions">
    <a class="nokv-btn nokv-btn--primary" href="/architecture">Read the architecture <span class="arrow">→</span></a>
    <a class="nokv-btn nokv-btn--ghost" href="https://github.com/feichai0017/NoKV">View on GitHub</a>
  </div>
</div>
