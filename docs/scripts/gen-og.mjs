// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Generate the 1200x630 Open Graph card for the NoKV landing page.
//
// Run:    npm --prefix docs run gen:og
// Output: docs/public/img/og.png
//
// SVG is rasterized via `sharp`. Fonts fall back to the host's generic
// sans-serif / monospace — the bundled librsvg can't load remote fonts.
// The result looks fine on every social card preview we checked.

import { fileURLToPath } from 'node:url'
import { dirname, resolve } from 'node:path'
import { mkdir } from 'node:fs/promises'
import sharp from 'sharp'

const here = dirname(fileURLToPath(import.meta.url))
const outDir = resolve(here, '..', 'public', 'img')
const outFile = resolve(outDir, 'og.png')

const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#07090f"/>
      <stop offset="1" stop-color="#0a0d18"/>
    </linearGradient>
    <linearGradient id="title-grad" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0" stop-color="#3b82f6"/>
      <stop offset="0.55" stop-color="#06b6d4"/>
      <stop offset="1" stop-color="#fbbf24"/>
    </linearGradient>
    <radialGradient id="orb" cx="0.1" cy="0.05" r="0.55">
      <stop offset="0" stop-color="#3b82f6" stop-opacity="0.42"/>
      <stop offset="1" stop-color="#3b82f6" stop-opacity="0"/>
    </radialGradient>
    <radialGradient id="orb-cyan" cx="1" cy="1" r="0.55">
      <stop offset="0" stop-color="#06b6d4" stop-opacity="0.28"/>
      <stop offset="1" stop-color="#06b6d4" stop-opacity="0"/>
    </radialGradient>
    <pattern id="grid" width="56" height="56" patternUnits="userSpaceOnUse">
      <path d="M 56 0 L 0 0 0 56" stroke="#60a5fa" stroke-opacity="0.06" fill="none"/>
    </pattern>
  </defs>

  <rect width="1200" height="630" fill="url(#bg)"/>
  <rect width="1200" height="630" fill="url(#grid)"/>
  <rect width="1200" height="630" fill="url(#orb)"/>
  <rect width="1200" height="630" fill="url(#orb-cyan)"/>

  <g transform="translate(72,72)">
    <rect x="0" y="0" width="48" height="48" rx="14" fill="#3b82f6" fill-opacity="0.18" stroke="#3b82f6" stroke-opacity="0.4" stroke-width="1"/>
    <circle cx="24" cy="24" r="11" fill="#60a5fa"/>
    <circle cx="24" cy="24" r="5" fill="#0a0d18"/>
    <text x="68" y="34" font-family="sans-serif" font-size="28" font-weight="800" fill="#f1f5f9" letter-spacing="-0.5">NoKV</text>
  </g>

  <g transform="translate(72,232)">
    <rect x="0" y="0" width="14" height="6" rx="3" fill="#60a5fa"/>
    <text x="26" y="6" font-family="monospace" font-size="18" font-weight="700" fill="#60a5fa" letter-spacing="3.2">OPEN SOURCE  ·  APACHE-2.0  ·  RUST FILESYSTEM</text>
  </g>

  <text x="72" y="318" font-family="sans-serif" font-size="68" font-weight="800" fill="#f1f5f9" letter-spacing="-2.4">Rust filesystem for</text>
  <text x="72" y="398" font-family="sans-serif" font-size="68" font-weight="800" fill="url(#title-grad)" letter-spacing="-2.4">AI training and agents.</text>

  <text x="72" y="486" font-family="sans-serif" font-size="26" font-weight="400" fill="#b4becc">Holt metadata  ·  S3-compatible bodies  ·  FUSE and SDK paths.</text>

  <g transform="translate(72,556)" font-family="monospace" font-size="18" fill="#8b94a7">
    <text>github.com/feichai0017/NoKV</text>
    <text x="320">·   Rust</text>
    <text x="430">·   Holt</text>
    <text x="540">·   S3 / RustFS</text>
  </g>

  <rect x="0" y="624" width="1200" height="6" fill="url(#title-grad)"/>
</svg>
`

await mkdir(outDir, { recursive: true })
await sharp(Buffer.from(svg)).png({ compressionLevel: 9 }).toFile(outFile)
console.log(`og.png written to ${outFile}`)
