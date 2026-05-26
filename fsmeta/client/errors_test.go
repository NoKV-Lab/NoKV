// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestClientErrorsExposeStableKinds(t *testing.T) {
	invalid := []error{
		errAddressRequired,
		errDirectoryReaderRequired,
		errWatchClientRequired,
		errStagedPublishClientRequired,
		errWatchStreamNotConfigured,
		errWatchSessionNotConfigured,
		errRPCClientNotConfigured,
	}
	for _, err := range invalid {
		require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(err))
	}

	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(errWatchEventBeforeReady))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(errConnectionNotReady))
	require.True(t, nokverrors.Retryable(errConnectionNotReady))
}

func TestTranslateRPCErrorOnlyMapsWatchReasonToCursorExpired(t *testing.T) {
	stale := nokverrors.RPCStatusError(nokverrors.KindStaleEpoch, codes.OutOfRange, "stale route", nil)
	got := translateRPCError(stale)
	require.False(t, errors.Is(got, model.ErrWatchCursorExpired))
	require.Equal(t, nokverrors.KindStaleEpoch, nokverrors.KindOf(got))

	watch := nokverrors.RPCStatusError(nokverrors.KindStaleEpoch, codes.OutOfRange, "watch cursor expired", map[string]string{
		fsmetaReasonMetadata: reasonWatchCursorExpired,
	})
	got = translateRPCError(watch)
	require.ErrorIs(t, got, model.ErrWatchCursorExpired)
}
