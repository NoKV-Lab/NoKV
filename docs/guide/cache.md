<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Cache Boundaries

Pebble owns physical block/table caching below `storage/pebble`. NoKV-owned
cache code that remains in the mainline is metadata-derived state under
`fsmeta/cache`, such as dirpage and negative-cache slabs. Those caches are
rebuildable and must not become authoritative namespace truth.
