// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package errkind

import (
	"fmt"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/feichai0017/NoKV/storage/wal"
	enginekv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestClassifyMapsEmbeddedEngineErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		kind nokverrors.Kind
	}{
		{
			name: "classified root error passes through",
			err:  nokverrors.New(nokverrors.KindRegionRouting, "route"),
			kind: nokverrors.KindRegionRouting,
		},
		{
			name: "missing key",
			err:  utils.ErrKeyNotFound,
			kind: nokverrors.KindNotFound,
		},
		{
			name: "invalid request",
			err:  fmt.Errorf("validate: %w", utils.ErrInvalidRequest),
			kind: nokverrors.KindInvalidArgument,
		},
		{
			name: "nil value",
			err:  utils.ErrNilValue,
			kind: nokverrors.KindInvalidArgument,
		},
		{
			name: "filesystem capability",
			err:  vfs.ErrRenameNoReplaceUnsupported,
			kind: nokverrors.KindInvalidArgument,
		},
		{
			name: "batch too large",
			err:  utils.ErrTxnTooBig,
			kind: nokverrors.KindResourceExhausted,
		},
		{
			name: "blocked writes retryable",
			err:  utils.ErrBlockedWrites,
			kind: nokverrors.KindRetryable,
		},
		{
			name: "hot key throttle retryable",
			err:  utils.ErrHotKeyWriteThrottle,
			kind: nokverrors.KindRetryable,
		},
		{
			name: "wal backpressure retryable",
			err:  wal.ErrWALBackpressure,
			kind: nokverrors.KindRetryable,
		},
		{
			name: "retained wal segment retryable",
			err:  wal.ErrSegmentRetained,
			kind: nokverrors.KindRetryable,
		},
		{
			name: "closed db aborted",
			err:  utils.ErrDBClosed,
			kind: nokverrors.KindAborted,
		},
		{
			name: "checksum corruption",
			err:  enginekv.ErrBadChecksum,
			kind: nokverrors.KindCorruption,
		},
		{
			name: "wal partial record corruption",
			err:  wal.ErrPartialRecord,
			kind: nokverrors.KindCorruption,
		},
		{
			name: "control flow remains local",
			err:  utils.ErrStop,
			kind: nokverrors.KindUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.kind, Classify(tt.err))
			require.Equal(t, tt.kind, Classify(fmt.Errorf("wrapped: %w", tt.err)))
		})
	}
}

func TestClassifyHandlesNilAndUnknown(t *testing.T) {
	require.Equal(t, nokverrors.KindUnknown, Classify(nil))
	require.Equal(t, nokverrors.KindUnknown, Classify(fmt.Errorf("local unknown")))
}
