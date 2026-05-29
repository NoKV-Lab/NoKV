// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package vfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncDirDefaultFS(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, SyncDir(nil, dir))
}
