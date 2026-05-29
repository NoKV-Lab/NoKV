<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Legacy Range Filter Design

The old range-filter optimization belonged to the removed self-managed LSM
table path. It is not part of the mainline storage contract. Pebble and Holt
may each use backend-native filtering internally, but `storage/kv` does not
expose those details.
