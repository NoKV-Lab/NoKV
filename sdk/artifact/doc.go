// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package artifact provides a high-level artifact namespace SDK over fsmeta and
// a pluggable body store.
//
// The package owns no fsmeta truth and does not write object bodies into
// fsmeta. It stores compact body references in inode opaque attributes, uses
// fsmeta for path, directory, listing, and delete metadata, and leaves the body
// bytes to the configured BodyStore.
package artifact
