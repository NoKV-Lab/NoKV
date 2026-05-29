// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChecksum(t *testing.T) {
	data := []byte("checksum")
	sum := CalculateChecksum(data)
	require.NoError(t, VerifyChecksum(data, U64ToBytes(sum)))
	require.Error(t, VerifyChecksum(data, []byte{0x00}))
}
