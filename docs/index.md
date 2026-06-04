---
title: NoKV
layout: home
hero:
  name: NoKV
  text: Rust filesystem for AI training and agent workspaces.
  tagline: Holt-backed metadata, S3-compatible object bodies, FUSE, and SDK paths for modern AI infrastructure.
  actions:
    - theme: brand
      text: Architecture
      link: /architecture
    - theme: alt
      text: Quick Start
      link: /rustfs
    - theme: alt
      text: Benchmarks
      link: /benchmarks
features:
  - title: Filesystem interface
    details: Low-level FUSE, Rust SDK, and CLI entry points over the same inode/dentry metadata model.
  - title: Holt metadata
    details: MetadataCommand predicates, dentry projection, snapshots, typed watches, and object-reference GC.
  - title: S3-compatible bodies
    details: File bytes are chunked into immutable object blocks in RustFS, MinIO, Ceph RGW, AWS S3, or compatible services.
  - title: AI workload shape
    details: Designed around datasets, checkpoint publish, artifact replacement, read-only snapshots, and agent workspace views.
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->
