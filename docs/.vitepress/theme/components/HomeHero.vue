<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
import { withBase } from 'vitepress'
</script>

<template>
  <section class="hero">
    <div class="nokv-glow" aria-hidden="true" />
    <div class="nokv-glow nokv-glow--right" aria-hidden="true" />
    <div class="nokv-grid-bg" aria-hidden="true" />

    <div class="hero-inner nokv-section nokv-section--tight">
      <div class="hero-copy">
        <span class="hero-eyebrow">
          <span class="status-dot" aria-hidden="true" />
          OPEN SOURCE · APACHE-2.0 · CNCF LANDSCAPE
        </span>

        <h1 class="hero-title">
          Namespace metadata,
          <span class="nokv-gradient-text">purpose-built.</span>
        </h1>

        <p class="hero-lead">
          The metadata substrate that distributed filesystems, object storage, and
          AI dataset pipelines shouldn't have to build themselves. Server-side
          <code>ReadDirPlus</code>, prefix-scoped change feeds, formally-verified
          authority handoff — packaged as a layer instead of a quarter of reinvention.
        </p>

        <div class="hero-pills">
          <span class="nokv-pill">Go 1.26+</span>
          <span class="nokv-pill">gRPC native</span>
          <span class="nokv-pill">multi-Raft</span>
          <span class="nokv-pill">TLA+ verified</span>
        </div>

        <div class="hero-ctas">
          <a class="nokv-btn nokv-btn--primary" :href="withBase('/guide/getting_started')">
            Read the docs
            <span class="arrow" aria-hidden="true">→</span>
          </a>
          <a
            class="nokv-btn nokv-btn--ghost"
            href="https://github.com/feichai0017/NoKV"
            target="_blank"
            rel="noreferrer"
          >
            <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
              <path
                fill="currentColor"
                d="M12 .5C5.73.5.94 5.29.94 11.56c0 4.92 3.19 9.1 7.62 10.58.56.1.76-.24.76-.54v-2c-3.1.67-3.75-1.31-3.75-1.31-.51-1.3-1.25-1.64-1.25-1.64-1.02-.7.08-.69.08-.69 1.13.08 1.72 1.16 1.72 1.16 1.01 1.73 2.65 1.23 3.3.94.1-.73.4-1.23.71-1.51-2.47-.28-5.07-1.24-5.07-5.52 0-1.22.44-2.21 1.16-2.99-.12-.28-.5-1.43.11-2.98 0 0 .95-.3 3.11 1.14a10.86 10.86 0 0 1 5.66 0c2.16-1.44 3.11-1.14 3.11-1.14.61 1.55.23 2.7.11 2.98.72.78 1.16 1.77 1.16 2.99 0 4.29-2.6 5.24-5.08 5.51.41.36.78 1.06.78 2.14v3.17c0 .3.2.65.77.54 4.42-1.48 7.6-5.66 7.6-10.58C23.06 5.29 18.27.5 12 .5Z"
              />
            </svg>
            Star on GitHub
          </a>
        </div>

        <div class="hero-trust">
          <span class="hero-trust-label">Layered above</span>
          <span class="hero-trust-row">
            <span class="trust-chip">FoundationDB-class KV</span>
            <span class="trust-chip">etcd/raft</span>
            <span class="trust-chip">Pebble / Holt backend</span>
          </span>
        </div>
      </div>

      <aside class="hero-terminal" aria-label="Quick usage">
        <header class="term-head">
          <span class="dot dot--red" />
          <span class="dot dot--yellow" />
          <span class="dot dot--green" />
          <span class="term-title">~/nokv · quick start</span>
        </header>
        <div class="term-body">
<pre><span class="c"># 1. Boot a full cluster (meta-root + coordinator + 3 stores)</span>
<span class="cmd">$ docker compose up -d</span>

<span class="c"># 2. Register a mount</span>
<span class="cmd">$ nokv mount register \
    --coordinator-addr 127.0.0.1:2379 \
    --mount default --root-inode 1</span>

<span class="c"># 3. One round-trip namespace fetch via fsmeta</span>
<span class="ok">&gt;</span> ReadDirPlus(mount=<span class="s">"default"</span>, parent=1)
<span class="ok">&lt;</span> 12 entries · 1 RTT · <span class="hi">3.4 ms p50</span>

<span class="c"># 4. Watch a prefix-scoped change feed</span>
<span class="ok">&gt;</span> WatchSubtree(prefix=<span class="s">"/data/*"</span>, cursor=last)
<span class="ok">&lt;</span> live · ack-window=64 · <span class="hi">178 ms p50</span></pre>
        </div>
      </aside>
    </div>
  </section>
</template>

<style scoped>
.hero {
  overflow: hidden;
  position: relative;
}

.hero-inner {
  align-items: center;
  display: grid;
  gap: 56px;
  grid-template-columns: minmax(0, 1.05fr) minmax(0, 1fr);
  padding-top: 96px;
  padding-bottom: 56px;
  position: relative;
  z-index: 1;
}

.hero-eyebrow {
  align-items: center;
  background: color-mix(in srgb, var(--vp-c-bg-soft) 70%, transparent);
  border: 1px solid var(--vp-c-border);
  border-radius: 999px;
  color: var(--vp-c-text-2);
  display: inline-flex;
  font-family: var(--vp-font-family-mono);
  font-size: 0.72rem;
  font-weight: 600;
  gap: 10px;
  letter-spacing: 0.15em;
  margin-bottom: 28px;
  padding: 7px 14px;
  text-transform: uppercase;
}

.status-dot {
  background: #22c55e;
  border-radius: 999px;
  box-shadow: 0 0 8px rgba(34, 197, 94, 0.7);
  height: 8px;
  width: 8px;
}

.hero-title {
  font-size: clamp(2.6rem, 5.5vw, 4.2rem);
  font-weight: 800;
  letter-spacing: -0.04em;
  line-height: 1;
  margin: 0 0 28px;
}

.hero-lead {
  color: var(--vp-c-text-2);
  font-size: clamp(1.05rem, 1.4vw, 1.18rem);
  line-height: 1.65;
  margin: 0 0 28px;
  max-width: 600px;
}

.hero-lead code {
  background: var(--vp-c-brand-bg);
  border-radius: 6px;
  color: var(--nokv-accent);
  font-family: var(--vp-font-family-mono);
  font-size: 0.94em;
  padding: 1px 6px;
}

.hero-pills {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 36px;
}

.hero-ctas {
  align-items: center;
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  margin-bottom: 36px;
}

.hero-trust {
  align-items: center;
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
}

.hero-trust-label {
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 0.78rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.hero-trust-row {
  align-items: center;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.trust-chip {
  background: transparent;
  border: 1px dashed var(--vp-c-border);
  border-radius: 999px;
  color: var(--vp-c-text-2);
  font-size: 0.86rem;
  padding: 4px 12px;
}

/* Terminal panel */

.hero-terminal {
  background: var(--nokv-code-bg);
  border: 1px solid color-mix(in srgb, var(--nokv-accent) 18%, var(--vp-c-border));
  border-radius: 18px;
  box-shadow:
    0 24px 60px -20px rgba(15, 23, 42, 0.45),
    0 0 0 1px rgba(255, 255, 255, 0.02) inset;
  color: var(--nokv-code-text);
  font-family: var(--vp-font-family-mono);
  overflow: hidden;
  position: relative;
}

.hero-terminal::before {
  background: linear-gradient(
    100deg,
    transparent 0%,
    color-mix(in srgb, var(--nokv-accent) 28%, transparent) 50%,
    transparent 100%
  );
  content: '';
  height: 1px;
  inset: 0 0 auto 0;
  position: absolute;
}

.term-head {
  align-items: center;
  background: rgba(255, 255, 255, 0.02);
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
  display: flex;
  gap: 8px;
  padding: 12px 16px;
}

.dot {
  border-radius: 999px;
  height: 11px;
  width: 11px;
}

.dot--red {
  background: #ff5f56;
}
.dot--yellow {
  background: #ffbd2e;
}
.dot--green {
  background: #27c93f;
}

.term-title {
  color: rgba(230, 237, 243, 0.55);
  font-size: 0.82rem;
  margin-left: 12px;
}

.term-body {
  padding: 22px 24px 26px;
}

.term-body pre {
  background: transparent;
  color: var(--nokv-code-text);
  font-family: var(--vp-font-family-mono);
  font-size: 0.92rem;
  line-height: 1.78;
  margin: 0;
  white-space: pre-wrap;
  word-break: break-word;
}

.term-body .c {
  color: #6e7681;
}
.term-body .cmd {
  color: #79c0ff;
}
.term-body .ok {
  color: #56d364;
  font-weight: 700;
}
.term-body .s {
  color: #a5d6ff;
}
.term-body .hi {
  color: #ffa657;
  font-weight: 700;
}

@media (max-width: 960px) {
  .hero-inner {
    grid-template-columns: 1fr;
    gap: 40px;
    padding-top: 56px;
  }

  .hero-terminal {
    margin: 0;
  }
}
</style>
