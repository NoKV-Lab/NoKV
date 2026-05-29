<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Legacy Manifest Design

The old storage-engine manifest and `nokv manifest` CLI were part of the
removed self-managed LSM path. Pebble workdirs use Pebble's own metadata below
`storage/pebble`; NoKV no longer exposes a product-level manifest inspection
surface.
