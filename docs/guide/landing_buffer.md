<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Legacy Landing Buffer Design

The landing-buffer optimization belonged to the removed self-managed LSM path.
It is not part of the mainline storage architecture. Write buffering now
belongs inside the concrete storage backend, such as Pebble or the future Holt
adapter.
