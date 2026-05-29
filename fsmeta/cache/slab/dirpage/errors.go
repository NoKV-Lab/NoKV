// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package dirpage

import "errors"

var (
	errPageTruncated   = errors.New("dirpage: record truncated")
	errPageBadMagic    = errors.New("dirpage: bad magic")
	errPageBadVersion  = errors.New("dirpage: unsupported version")
	errPageBadChecksum = errors.New("dirpage: checksum mismatch")
)
