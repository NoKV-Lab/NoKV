// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package file provides low-level file and mmap primitives used by storage
// sidecars such as fsmeta slab caches.
package file

import "github.com/feichai0017/NoKV/storage/vfs"

// Options controls file opening parameters used by storage primitives.
type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Flag     int
	MaxSz    int
	FS       vfs.FS
}
