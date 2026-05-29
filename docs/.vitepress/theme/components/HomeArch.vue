<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
import { withBase } from 'vitepress'

const layers = [
  {
    tier: 'L1',
    name: 'fsmeta',
    role: 'Namespace primitives',
    parts: ['Create', 'ReadDirPlus', 'WatchSubtree', 'RenameSubtree', 'SnapshotSubtree'],
  },
  {
    tier: 'L2',
    name: 'meta/root · coordinator · raftstore · percolator',
    role: 'Authority truth + routing + multi-Raft + MVCC 2PC',
    parts: [
      'Mount / SubtreeAuthority / SnapshotEpoch / QuotaFence',
      'Route · TSO · WatchRootEvents',
      'Apply observer + transport',
      'Prewrite / Commit / AssertionNotExist',
    ],
  },
  {
    tier: 'L3',
    name: 'storage/kv · pebble · holt',
    role: 'Replaceable raw ordered KV backend',
    parts: ['storage/kv contract', 'Pebble default', 'Holt target adapter', 'VFS / file support'],
  },
]
</script>

<template>
  <section class="arch nokv-section">
    <div class="nokv-section-head">
      <span class="nokv-eyebrow">Architecture</span>
      <h2 class="nokv-h2">
        Three layers. Hard boundaries. Enforced in code.
      </h2>
      <p class="nokv-lead">
        The fsmeta executor consumes a narrow <code>fsmeta/backend.Store</code>.
        Runtime adapters own local or raftstore wiring. Lower layers do not import
        fsmeta. The raw storage backend never learns that a namespace exists.
      </p>
    </div>

    <div class="arch-grid">
      <div v-for="layer in layers" :key="layer.tier" class="arch-row nokv-card">
        <div class="arch-tier">
          <span class="tier-tag">{{ layer.tier }}</span>
          <div class="tier-meta">
            <h3>{{ layer.name }}</h3>
            <p>{{ layer.role }}</p>
          </div>
        </div>
        <ul class="arch-parts">
          <li v-for="part in layer.parts" :key="part">{{ part }}</li>
        </ul>
      </div>
    </div>

    <div class="arch-cta">
      <a class="nokv-btn nokv-btn--ghost" :href="withBase('/guide/architecture')">
        Explore the full architecture
        <span class="arrow" aria-hidden="true">→</span>
      </a>
      <a class="nokv-btn nokv-btn--ghost" :href="withBase('/guide/control_and_execution_protocols')">
        Control / execution protocols
        <span class="arrow" aria-hidden="true">→</span>
      </a>
    </div>
  </section>
</template>

<style scoped>
.arch-grid {
  display: grid;
  gap: 18px;
}

.arch-row {
  align-items: center;
  display: grid;
  gap: 24px;
  grid-template-columns: minmax(280px, 1fr) minmax(0, 1.4fr);
}

.arch-tier {
  align-items: center;
  display: flex;
  gap: 18px;
}

.tier-tag {
  align-items: center;
  background: linear-gradient(135deg, var(--nokv-accent) 0%, var(--nokv-accent-2) 100%);
  border-radius: 12px;
  color: #fff;
  display: inline-flex;
  font-family: var(--vp-font-family-mono);
  font-size: 1rem;
  font-weight: 700;
  height: 50px;
  justify-content: center;
  letter-spacing: 0.04em;
  min-width: 50px;
  text-shadow: 0 1px 2px rgba(0, 0, 0, 0.18);
}

.tier-meta h3 {
  font-family: var(--vp-font-family-mono);
  font-size: 1.05rem;
  font-weight: 700;
  margin: 0 0 6px;
}

.tier-meta p {
  color: var(--vp-c-text-2);
  font-size: 0.93rem;
  margin: 0;
}

.arch-parts {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  justify-content: flex-end;
  list-style: none;
  margin: 0;
  padding: 0;
}

.arch-parts li {
  background: color-mix(in srgb, var(--vp-c-bg-soft) 70%, transparent);
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  color: var(--vp-c-text-1);
  font-family: var(--vp-font-family-mono);
  font-size: 0.82rem;
  padding: 5px 10px;
}

.arch-cta {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 32px;
}

.nokv-lead code {
  background: var(--vp-c-brand-bg);
  border-radius: 6px;
  color: var(--nokv-accent);
  font-family: var(--vp-font-family-mono);
  font-size: 0.92em;
  padding: 1px 6px;
}

@media (max-width: 800px) {
  .arch-row {
    grid-template-columns: 1fr;
  }

  .arch-parts {
    justify-content: flex-start;
  }
}
</style>
