<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Legacy Manifest Design

The old storage-engine manifest and `nokv manifest` CLI were part of the
removed self-managed LSM path. Raw backends own their own metadata below
`storage/kv`: Pebble uses Pebble's format today, and Holt should keep its own
metadata inside the Holt adapter. NoKV no longer exposes a product-level
manifest inspection surface.
