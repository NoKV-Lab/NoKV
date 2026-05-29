<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
const rows = [
  {
    need: 'A complete distributed filesystem (FUSE-mountable, full POSIX)',
    alt: 'CephFS, JuiceFS',
    fit:
      'NoKV is not an FS — but JuiceFS-style systems default to Redis / TiKV for metadata, which breaks at scale. NoKV can be JuiceFS\'s metadata backend.',
    tone: 'partner',
  },
  {
    need: 'A production object store',
    alt: 'MinIO, Ceph RGW',
    fit:
      'NoKV is not an object store — object body I/O is not its job. NoKV provides the namespace layer above the object backend (bucket / prefix / version).',
    tone: 'partner',
  },
  {
    need: 'A custom metadata service you are writing on top of FoundationDB / TiKV / etcd',
    alt: '— ',
    fit: 'This is exactly what NoKV replaces. Apache-2.0, server-side primitives, namespace-native.',
    tone: 'replace',
  },
  {
    need: 'A production distributed KV',
    alt: 'TiKV, FoundationDB, CockroachDB',
    fit:
      'NoKV does not compete with them — they own the generic-KV market. NoKV is a metadata-native layer that can run on top of them (or, today, on its own engine).',
    tone: 'partner',
  },
  {
    need: 'Just an embedded ordered KV',
    alt: 'Pebble, Holt',
    fit: 'NoKV is not a drop-in KV. It uses Pebble today and is shaped so our Holt backend can plug in below metadata semantics.',
    tone: 'neutral',
  },
  {
    need: 'A Raft library',
    alt: 'etcd/raft, dragonboat',
    fit:
      'NoKV\'s raftstore (per-region runtime, transport, membership, snapshot install, apply observer) is built on top of etcd/raft RawNode. Owned: the integration. Reused: the consensus core.',
    tone: 'neutral',
  },
]
</script>

<template>
  <section class="compare nokv-section">
    <div class="nokv-section-head">
      <span class="nokv-eyebrow">Why NoKV vs X?</span>
      <h2 class="nokv-h2">
        Layer split, not category overlap.
      </h2>
      <p class="nokv-lead">
        NoKV's value comes from being metadata-native, not generic-KV-with-metadata-glued-on.
        Below: where NoKV slots in versus the systems people usually compare it to.
      </p>
    </div>

    <div class="compare-list nokv-card">
      <article v-for="(r, i) in rows" :key="i" class="cmp-row" :data-tone="r.tone">
        <div class="cmp-need">
          <span class="cmp-label">If you need…</span>
          <p>{{ r.need }}</p>
        </div>
        <div class="cmp-alt">
          <span class="cmp-label">You probably want…</span>
          <p>{{ r.alt }}</p>
        </div>
        <div class="cmp-fit">
          <span class="cmp-label">Where NoKV fits</span>
          <p>{{ r.fit }}</p>
        </div>
      </article>
    </div>
  </section>
</template>

<style scoped>
.compare-list {
  padding: 0;
}

.cmp-row {
  border-top: 1px solid var(--vp-c-divider);
  display: grid;
  gap: 24px;
  grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr) minmax(0, 1.6fr);
  padding: 22px 24px;
  position: relative;
}

.cmp-row:first-child {
  border-top: none;
}

.cmp-row[data-tone='replace']::before {
  background: linear-gradient(180deg, var(--nokv-accent) 0%, var(--nokv-accent-2) 100%);
  bottom: 12px;
  content: '';
  left: 0;
  position: absolute;
  top: 12px;
  width: 3px;
}

.cmp-row[data-tone='replace'] {
  background: color-mix(in srgb, var(--nokv-accent) 6%, transparent);
}

.cmp-label {
  color: var(--vp-c-text-3);
  display: block;
  font-family: var(--vp-font-family-mono);
  font-size: 0.7rem;
  font-weight: 600;
  letter-spacing: 0.1em;
  margin-bottom: 6px;
  text-transform: uppercase;
}

.cmp-need p,
.cmp-alt p,
.cmp-fit p {
  font-size: 0.94rem;
  line-height: 1.55;
  margin: 0;
}

.cmp-need p {
  color: var(--vp-c-text-1);
  font-weight: 600;
}

.cmp-alt p {
  color: var(--vp-c-text-2);
  font-family: var(--vp-font-family-mono);
  font-size: 0.92rem;
}

.cmp-fit p {
  color: var(--vp-c-text-2);
}

.cmp-row[data-tone='replace'] .cmp-fit p {
  color: var(--vp-c-text-1);
}

@media (max-width: 900px) {
  .cmp-row {
    grid-template-columns: 1fr;
    gap: 14px;
    padding: 20px;
  }
}
</style>
