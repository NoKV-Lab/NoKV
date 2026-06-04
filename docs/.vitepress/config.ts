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
      'Rust filesystem for AI training and agent workspaces, backed by Holt metadata and S3-compatible object storage.',
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
      ['meta', { property: 'og:title', content: 'NoKV — Rust filesystem for AI training and agent workspaces.' }],
      [
        'meta',
        {
          property: 'og:description',
          content:
            'Holt-backed metadata, S3-compatible object bodies, FUSE, and SDK paths for AI training and agent workspaces. Apache-2.0.',
        },
      ],
      ['meta', { property: 'og:image', content: OG_IMAGE }],
      ['meta', { property: 'og:image:width', content: '1200' }],
      ['meta', { property: 'og:image:height', content: '630' }],
      ['meta', { property: 'og:image:alt', content: 'NoKV — Rust filesystem for AI training and agent workspaces.' }],
      ['meta', { property: 'og:site_name', content: 'NoKV' }],
      ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
      ['meta', { name: 'twitter:title', content: 'NoKV — Rust filesystem for AI training and agent workspaces.' }],
      [
        'meta',
        {
          name: 'twitter:description',
          content:
            'Holt-backed metadata, S3-compatible object bodies, FUSE, and SDK paths for AI training and agent workspaces. Apache-2.0.',
        },
      ],
      ['meta', { name: 'twitter:image', content: OG_IMAGE }],
    ],

    themeConfig: {
      logo: { src: '/img/logo.svg', width: 40, height: 40, alt: 'NoKV' },
      siteTitle: 'NoKV',

      nav: [
        { text: 'Docs', link: '/architecture', activeMatch: '/' },
        {
          text: 'Reference',
          items: [
            { text: 'Product Design', link: '/product-design' },
            { text: 'Architecture', link: '/architecture' },
            { text: 'Metadata Schema', link: '/metadata-schema' },
            { text: 'Object Layout', link: '/object-layout' },
            { text: 'RustFS Backend', link: '/rustfs' },
            { text: 'Benchmarks', link: '/benchmarks' },
          ],
        },
        {
          text: 'Resources',
          items: [
            { text: 'AI Training', link: '/ai-training' },
            { text: 'Checkpointing', link: '/checkpointing' },
            { text: 'Code Contract', link: '/development/code_contract' },
          ],
        },
        { text: 'GitHub', link: 'https://github.com/feichai0017/NoKV' },
      ],

      sidebar: {
        '/': [
          {
            text: 'Product',
            collapsed: false,
            items: [
              { text: 'Architecture', link: '/architecture' },
              { text: 'Product Design', link: '/product-design' },
              { text: 'Metadata Schema', link: '/metadata-schema' },
              { text: 'Object Layout', link: '/object-layout' },
              { text: 'RustFS Backend', link: '/rustfs' },
              { text: 'Benchmarks', link: '/benchmarks' },
              { text: 'AI Training', link: '/ai-training' },
              { text: 'Checkpointing', link: '/checkpointing' },
            ],
          },
          {
            text: 'Development',
            collapsed: true,
            items: [
              { text: 'Code Contract', link: '/development/code_contract' },
              { text: 'PR Review Checklist', link: '/development/pr_review_checklist' },
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
