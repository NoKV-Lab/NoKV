// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package fsmeta is NoKV's workspace namespace metadata plane.
//
// fsmeta sits on top of NoKV as a consumer. It exposes filesystem-shaped
// metadata semantics for AI agent workspaces: inodes, dentries, xattrs, atomic
// cross-directory rename, multi-mount namespace routing, snapshots, and watch
// streams. The same contract can also serve distributed filesystems that need a
// standalone metadata service.
//
// Scope boundary:
//
//   - fsmeta is not a filesystem. It is the namespace metadata kernel that an
//     agent workspace layer, a DFS, or a FUSE frontend consumes. NoKV's pitch is
//     "workspace metadata engine", not "another distributed filesystem".
//
//   - fsmeta does not live under meta/. meta/root is NoKV's own rooted cluster
//     truth: region descriptors, authority grants, and allocator fences.
//     User-facing filesystem metadata is application data: a consumer of NoKV,
//     not part of NoKV's internal truth.
//
//   - fsmeta/model owns the storage-engine-neutral namespace model and public
//     operation shapes. fsmeta/layout owns ordered-key encodings and placement
//     plans. fsmeta/observe owns watch and snapshot observation surfaces. This
//     root package is only the package-level architecture anchor.
//
//   - fsmeta uses runtime adapters for local and distributed storage. It may
//     reuse meta/root's rooted-event substrate only for namespace-level
//     authority where Eunomia semantics apply. Per-inode and per-dentry
//     mutations are data-plane writes, never rooted events.
package fsmeta
