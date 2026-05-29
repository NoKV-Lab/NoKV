<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
const stats = [
  {
    value: 'fsmeta',
    unit: 'core',
    label: 'Workspace metadata API',
    sub: 'Create, ReadDirPlus, watch, snapshot, atomic publish',
  },
  {
    value: '42×',
    unit: 'speedup',
    label: 'ReadDirPlus vs client-side stitching',
    sub: 'One RTT replaces 1+N round-trips on generic KV',
  },
  {
    value: '178',
    unit: 'ms p50',
    label: 'WatchSubtree end-to-end',
    sub: 'Prefix-scoped, server-pushed change feed',
  },
  {
    value: 'TLC',
    unit: 'verified',
    label: 'Eunomia authority handoff',
    sub: 'Model-checked under finite bounds',
  },
]
</script>

<template>
  <section class="stats nokv-section nokv-section--tight">
    <div class="stats-grid">
      <div v-for="s in stats" :key="s.label" class="stat">
        <div class="stat-value">
          <span class="num">{{ s.value }}</span>
          <span class="unit">{{ s.unit }}</span>
        </div>
        <div class="stat-label">{{ s.label }}</div>
        <div class="stat-sub">{{ s.sub }}</div>
      </div>
    </div>
  </section>
</template>

<style scoped>
.stats {
  border-top: 1px solid var(--vp-c-divider);
  border-bottom: 1px solid var(--vp-c-divider);
}

.stats-grid {
  display: grid;
  gap: 0;
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.stat {
  border-left: 1px solid var(--vp-c-divider);
  padding: 18px 28px;
  position: relative;
}

.stat:first-child {
  border-left: none;
  padding-left: 0;
}

.stat-value {
  align-items: baseline;
  display: flex;
  gap: 8px;
  margin-bottom: 6px;
}

.num {
  color: var(--vp-c-text-1);
  font-family: var(--vp-font-family-mono);
  font-size: clamp(2rem, 3.4vw, 2.8rem);
  font-weight: 800;
  letter-spacing: -0.045em;
  line-height: 1;
}

.unit {
  color: var(--nokv-accent);
  font-family: var(--vp-font-family-mono);
  font-size: 0.92rem;
  font-weight: 600;
  letter-spacing: 0.02em;
}

.stat-label {
  color: var(--vp-c-text-1);
  font-size: 0.96rem;
  font-weight: 700;
  margin-bottom: 4px;
}

.stat-sub {
  color: var(--vp-c-text-3);
  font-size: 0.84rem;
  line-height: 1.45;
}

@media (max-width: 900px) {
  .stats-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 32px 0;
  }

  .stat:nth-child(odd) {
    border-left: none;
    padding-left: 0;
  }

  .stat:nth-child(3),
  .stat:nth-child(4) {
    border-top: 1px solid var(--vp-c-divider);
    padding-top: 28px;
  }
}

@media (max-width: 540px) {
  .stats-grid {
    grid-template-columns: 1fr;
  }

  .stat {
    border-left: none !important;
    padding: 24px 0 0 !important;
  }

  .stat + .stat {
    border-top: 1px solid var(--vp-c-divider);
  }
}
</style>
