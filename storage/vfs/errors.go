// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package vfs

import "errors"

// ErrRenameNoReplaceUnsupported indicates the platform/filesystem cannot provide
// atomic no-replace rename semantics.
var ErrRenameNoReplaceUnsupported = errors.New("vfs: rename no-replace unsupported")
