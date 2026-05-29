<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
import { withBase } from 'vitepress'

const cols = [
  {
    title: 'Product',
    links: [
      { text: 'Overview', href: withBase('/') },
      { text: 'Architecture', href: withBase('/guide/architecture') },
      { text: 'fsmeta API', href: withBase('/guide/fsmeta') },
      { text: 'Benchmarks', href: withBase('/guide/getting_started') },
    ],
  },
  {
    title: 'Docs',
    links: [
      { text: 'Getting Started', href: withBase('/guide/getting_started') },
      { text: 'CLI Reference', href: withBase('/guide/cli') },
      { text: 'Configuration', href: withBase('/guide/config') },
      { text: 'Testing', href: withBase('/guide/testing') },
    ],
  },
  {
    title: 'Community',
    links: [
      { text: 'GitHub', href: 'https://github.com/feichai0017/NoKV' },
      { text: 'Issues', href: 'https://github.com/feichai0017/NoKV/issues' },
      { text: 'Contributing', href: 'https://github.com/feichai0017/NoKV/blob/main/CONTRIBUTING.md' },
      { text: 'DeepWiki', href: 'https://deepwiki.com/feichai0017/NoKV' },
    ],
  },
  {
    title: 'Resources',
    links: [
      { text: 'Formal Specs (spec/)', href: 'https://github.com/feichai0017/NoKV/tree/main/spec' },
      { text: 'Code Contract', href: withBase('/guide/development/code_contract') },
      { text: 'Security Policy', href: 'https://github.com/feichai0017/NoKV/blob/main/SECURITY.md' },
      { text: 'License (Apache-2.0)', href: 'https://github.com/feichai0017/NoKV/blob/main/LICENSE' },
    ],
  },
]
</script>

<template>
  <footer class="footer">
    <div class="footer-inner nokv-section nokv-section--tight">
      <div class="footer-top">
        <div class="footer-brand">
          <img :src="withBase('/img/logo.svg')" alt="NoKV" />
          <div>
            <strong>NoKV</strong>
            <p>
              Open-source namespace metadata substrate for distributed filesystems,
              object storage, and AI dataset metadata.
            </p>
          </div>
        </div>

        <nav class="footer-cols" aria-label="Footer">
          <div v-for="col in cols" :key="col.title" class="footer-col">
            <h4>{{ col.title }}</h4>
            <ul>
              <li v-for="l in col.links" :key="l.text">
                <a :href="l.href" :target="l.href.startsWith('http') ? '_blank' : undefined">{{
                  l.text
                }}</a>
              </li>
            </ul>
          </div>
        </nav>
      </div>

      <div class="footer-bottom">
        <span class="footer-fine">
          Apache-2.0 · fsmeta metadata semantics, distributed execution, and
          Pebble / Holt-class storage backends.
        </span>
        <span class="footer-fine">© 2025 NoKV Contributors</span>
      </div>
    </div>
  </footer>
</template>

<style scoped>
.footer {
  background: var(--vp-c-bg-alt);
  border-top: 1px solid var(--vp-c-divider);
  margin-top: 64px;
}

.footer-top {
  display: grid;
  gap: 48px;
  grid-template-columns: minmax(0, 1.2fr) minmax(0, 2fr);
  padding-bottom: 40px;
}

.footer-brand {
  align-items: flex-start;
  display: flex;
  gap: 16px;
  max-width: 360px;
}

.footer-brand img {
  height: 44px;
  width: 44px;
}

.footer-brand strong {
  color: var(--vp-c-text-1);
  display: block;
  font-size: 1.2rem;
  font-weight: 800;
  letter-spacing: -0.02em;
  margin-bottom: 6px;
}

.footer-brand p {
  color: var(--vp-c-text-2);
  font-size: 0.92rem;
  line-height: 1.55;
  margin: 0;
}

.footer-cols {
  display: grid;
  gap: 32px;
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.footer-col h4 {
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 0.74rem;
  font-weight: 600;
  letter-spacing: 0.12em;
  margin: 0 0 14px;
  text-transform: uppercase;
}

.footer-col ul {
  display: grid;
  gap: 8px;
  list-style: none;
  margin: 0;
  padding: 0;
}

.footer-col a {
  color: var(--vp-c-text-2);
  font-size: 0.92rem;
  text-decoration: none;
  transition: color 0.15s ease;
}

.footer-col a:hover {
  color: var(--nokv-accent);
}

.footer-bottom {
  align-items: center;
  border-top: 1px solid var(--vp-c-divider);
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  justify-content: space-between;
  padding-top: 22px;
}

.footer-fine {
  color: var(--vp-c-text-3);
  font-size: 0.84rem;
}

.footer-fine code {
  background: var(--vp-c-bg-soft);
  border-radius: 4px;
  color: var(--vp-c-text-2);
  font-family: var(--vp-font-family-mono);
  font-size: 0.92em;
  padding: 1px 6px;
}

@media (max-width: 900px) {
  .footer-top {
    grid-template-columns: 1fr;
    gap: 36px;
  }

  .footer-cols {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 28px 24px;
  }
}
</style>
