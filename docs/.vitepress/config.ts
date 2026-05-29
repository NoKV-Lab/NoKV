// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

const SITE_URL = 'https://nokv.io'
const OG_IMAGE = `${SITE_URL}/img/og.png`

export default withMermaid(
  defineConfig({
    base: '/',
    title: 'NoKV',
    titleTemplate: ':title — NoKV',
    description:
      'Open-source namespace metadata substrate for distributed filesystems, object storage, and AI dataset metadata.',
    cleanUrls: true,
    lastUpdated: true,
    appearance: 'dark',

    // Source-code links in docs are absolute GitHub URLs. Keep this as a
    // narrow safety net for any future fragment-only links.
    ignoreDeadLinks: 'localhostLinks',

    head: [
      ['link', { rel: 'icon', type: 'image/svg+xml', href: '/img/logo.svg' }],
      ['link', { rel: 'preconnect', href: 'https://rsms.me/' }],
      ['link', { rel: 'stylesheet', href: 'https://rsms.me/inter/inter.css' }],
      ['meta', { name: 'theme-color', content: '#3b82f6' }],
      ['meta', { property: 'og:type', content: 'website' }],
      ['meta', { property: 'og:url', content: `${SITE_URL}/` }],
      ['meta', { property: 'og:title', content: 'NoKV — Namespace metadata, purpose-built.' }],
      [
        'meta',
        {
          property: 'og:description',
          content:
            'The namespace metadata layer DFS, object storage, and AI dataset pipelines should not have to build themselves. Apache-2.0.',
        },
      ],
      ['meta', { property: 'og:image', content: OG_IMAGE }],
      ['meta', { property: 'og:image:width', content: '1200' }],
      ['meta', { property: 'og:image:height', content: '630' }],
      ['meta', { property: 'og:image:alt', content: 'NoKV — Namespace metadata, purpose-built.' }],
      ['meta', { property: 'og:site_name', content: 'NoKV' }],
      ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
      ['meta', { name: 'twitter:title', content: 'NoKV — Namespace metadata, purpose-built.' }],
      [
        'meta',
        {
          name: 'twitter:description',
          content:
            'The namespace metadata layer DFS, object storage, and AI dataset pipelines should not have to build themselves. Apache-2.0.',
        },
      ],
      ['meta', { name: 'twitter:image', content: OG_IMAGE }],
    ],

    themeConfig: {
      logo: { src: '/img/logo.svg', width: 40, height: 40, alt: 'NoKV' },
      siteTitle: 'NoKV',

      nav: [
        { text: 'Docs', link: '/guide/', activeMatch: '/guide/' },
        {
          text: 'Reference',
          items: [
            { text: 'Architecture', link: '/guide/architecture' },
            { text: 'fsmeta API', link: '/guide/fsmeta' },
            { text: 'CLI', link: '/guide/cli' },
            { text: 'Configuration', link: '/guide/config' },
          ],
        },
        {
          text: 'Resources',
          items: [
            { text: 'Getting Started', link: '/guide/getting_started' },
            { text: 'Testing', link: '/guide/testing' },
            { text: 'Code Contract', link: '/guide/development/code_contract' },
            {
              text: 'Formal Specs (spec/)',
              link: 'https://github.com/feichai0017/NoKV/tree/main/spec',
            },
            {
              text: 'DeepWiki',
              link: 'https://deepwiki.com/feichai0017/NoKV',
            },
          ],
        },
        { text: 'GitHub', link: 'https://github.com/feichai0017/NoKV' },
      ],

      sidebar: {
        '/guide/': [
          {
            text: 'Start here',
            collapsed: false,
            items: [
              { text: 'Overview', link: '/guide/' },
              { text: 'Getting Started', link: '/guide/getting_started' },
            ],
          },
          {
            text: 'Architecture',
            collapsed: false,
            items: [
              { text: 'Architecture', link: '/guide/architecture' },
              { text: 'Runtime Call Chains', link: '/guide/runtime' },
              {
                text: 'Control & Execution Protocols',
                link: '/guide/control_and_execution_protocols',
              },
            ],
          },
          {
            text: 'Namespace Metadata',
            collapsed: false,
            items: [
              { text: 'fsmeta', link: '/guide/fsmeta' },
              { text: 'Rooted Truth (meta/root)', link: '/guide/rooted_truth' },
              { text: 'Coordinator', link: '/guide/coordinator' },
              { text: 'Percolator', link: '/guide/percolator' },
              { text: 'Recovery', link: '/guide/recovery' },
            ],
          },
          {
            text: 'Distributed Runtime',
            collapsed: true,
            items: [{ text: 'Raftstore', link: '/guide/raftstore' }],
          },
          {
            text: 'Storage Backends',
            collapsed: true,
            items: [
              { text: 'Runtime', link: '/guide/runtime' },
              { text: 'Entry Model', link: '/guide/entry' },
              { text: 'VFS', link: '/guide/vfs' },
              { text: 'WAL Boundaries', link: '/guide/wal' },
              { text: 'Error Handling', link: '/guide/errors' },
            ],
          },
          {
            text: 'Experimental',
            collapsed: true,
            items: [
              {
                text: 'Experimental Boundary Plan',
                link: '/guide/experimental_boundary_plan',
              },
              { text: 'Thermos', link: '/guide/thermos' },
            ],
          },
          {
            text: 'Operations & Tooling',
            collapsed: true,
            items: [
              { text: 'Configuration', link: '/guide/config' },
              { text: 'CLI', link: '/guide/cli' },
              { text: 'Cluster Demo', link: '/guide/demo' },
              { text: 'Scripts', link: '/guide/scripts' },
              { text: 'Stats & Observability', link: '/guide/stats' },
              { text: 'Testing', link: '/guide/testing' },
            ],
          },
          {
            text: 'Development',
            collapsed: true,
            items: [
              { text: 'Code Contract', link: '/guide/development/code_contract' },
              { text: 'PR Review Checklist', link: '/guide/development/pr_review_checklist' },
            ],
          },
        ],
      },

      socialLinks: [{ icon: 'github', link: 'https://github.com/feichai0017/NoKV' }],

      editLink: {
        pattern: 'https://github.com/feichai0017/NoKV/edit/main/docs/:path',
        text: 'Edit this page on GitHub',
      },

      search: { provider: 'local' },

      outline: { level: [2, 3] },

      docFooter: {
        prev: 'Previous',
        next: 'Next',
      },
    },

    markdown: {
      lineNumbers: false,
      theme: {
        light: 'github-light',
        dark: 'github-dark-dimmed',
      },
    },

    mermaid: {
      theme: 'default',
    },

    sitemap: {
      hostname: 'https://nokv.io/',
    },

    vite: {
      ssr: {
        noExternal: ['mermaid'],
      },
    },
  }),
)
