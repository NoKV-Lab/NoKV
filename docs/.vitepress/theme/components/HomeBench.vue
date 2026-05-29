<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
const rows = [
  {
    area: 'fsmeta',
    evidence: 'local and distributed workload matrix',
    reason: 'Measures the product path: namespace operations, snapshots, watches, and artifact-style publish.',
  },
  {
    area: 'storage/kv',
    evidence: 'Pebble default backend',
    reason: 'Production-grade ordered KV substrate while NoKV keeps MVCC and metadata semantics above it.',
  },
  {
    area: 'storage/holt',
    evidence: 'owned backend target',
    reason: 'Future adapter point for the Rust Holt engine without changing fsmeta or raftstore semantics.',
  },
  {
    area: 'raftstore',
    evidence: 'correctness, chaos, and soak tests',
    reason: 'Validates the distributed metadata execution framework instead of generic KV microbenchmarks.',
  },
]
</script>

<template>
  <section class="bench nokv-section">
    <div class="nokv-section-head">
      <span class="nokv-eyebrow">Validation focus</span>
      <h2 class="nokv-h2">
        Benchmark the metadata path, not a generic storage-engine contest.
      </h2>
      <p class="nokv-lead">
        NoKV now treats the physical engine as a replaceable ordered-KV backend.
        The meaningful evidence is fsmeta throughput, namespace latency,
        snapshot/watch behavior, and distributed correctness. Pebble is the
        default backend today; Holt is the owned backend target behind the same
        raw storage contract.
      </p>
    </div>

    <div class="bench-card nokv-card">
      <div class="bench-table-wrap">
        <table class="bench-table">
          <thead>
            <tr>
              <th class="th-name">Area</th>
              <th>Evidence</th>
              <th>Why it matters</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="r in rows" :key="r.area">
              <td class="td-name">{{ r.area }}</td>
              <td>{{ r.evidence }}</td>
              <td>{{ r.reason }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p class="bench-fine">
        Run <code>NOKV_FSMETA_BENCH_MODE=local make fsmeta-bench</code> for the
        local matrix, or use Compose mode for the distributed path.
      </p>
    </div>
  </section>
</template>

<style scoped>
.bench-card {
  overflow: hidden;
  padding: 0;
}

.bench-table-wrap {
  overflow-x: auto;
}

.bench-table {
  border-collapse: collapse;
  width: 100%;
}

.bench-table thead th {
  background: color-mix(in srgb, var(--vp-c-bg-soft) 70%, transparent);
  border-bottom: 1px solid var(--vp-c-divider);
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 0.78rem;
  font-weight: 600;
  letter-spacing: 0.08em;
  padding: 16px 18px;
  text-align: left;
  text-transform: uppercase;
}

.bench-table tbody tr + tr {
  border-top: 1px solid var(--vp-c-divider);
}

.bench-table td {
  color: var(--vp-c-text-2);
  font-size: 0.96rem;
  padding: 16px 18px;
  vertical-align: top;
}

.td-name {
  color: var(--vp-c-text-1) !important;
  font-family: var(--vp-font-family-mono);
  font-weight: 700;
  white-space: nowrap;
}

.bench-fine {
  border-top: 1px solid var(--vp-c-divider);
  color: var(--vp-c-text-3);
  font-size: 0.86rem;
  margin: 0;
  padding: 14px 18px 16px;
}

@media (max-width: 720px) {
  .bench-table th,
  .bench-table td {
    padding: 13px 14px;
  }
}
</style>
