// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeEntryDecodeEntryHelpers(t *testing.T) {
	entry := &Entry{
		Key:       InternalKey(CFDefault, []byte("codec"), 1),
		Value:     encodeValueStruct(ValueStruct{Value: []byte("value")}),
		Meta:      1,
		ExpiresAt: 10,
	}
	var buf bytes.Buffer
	encoded, err := EncodeEntry(&buf, entry)
	require.NoError(t, err)
	require.Greater(t, len(encoded), 0)
	require.GreaterOrEqual(t, EstimateEncodeSize(entry), len(encoded))

	decoded, err := DecodeEntry(encoded)
	require.NoError(t, err)
	require.Equal(t, entry.Key, decoded.Key)
	require.Equal(t, entry.Value, decoded.Value)
	decoded.DecrRef()

	value, header, err := DecodeValueSlice(encoded)
	require.NoError(t, err)
	require.Equal(t, entry.Value, value)
	require.Equal(t, entry.Meta, header.Meta)
	require.Equal(t, entry.ExpiresAt, header.ExpiresAt)
}
