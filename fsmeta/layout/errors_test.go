// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package layout

import (
	"fmt"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestErrorsExposeStableKinds(t *testing.T) {
	invalid := []error{
		model.ErrInvalidMountID,
		model.ErrInvalidInodeID,
		model.ErrInvalidName,
		model.ErrInvalidSession,
		model.ErrInvalidRequest,
		ErrInvalidKey,
		ErrInvalidKeyKind,
		model.ErrInvalidValue,
		ErrInvalidValueKind,
		model.ErrInvalidPageSize,
	}
	for _, err := range invalid {
		require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(err))
		require.True(t, nokverrors.IsKind(fmt.Errorf("wrapped: %w", err), nokverrors.KindInvalidArgument))
	}

	require.Equal(t, nokverrors.KindAlreadyExists, nokverrors.KindOf(model.ErrExists))
	require.Equal(t, nokverrors.KindNotFound, nokverrors.KindOf(model.ErrNotFound))
	require.Equal(t, nokverrors.KindNotFound, nokverrors.KindOf(model.ErrMountNotRegistered))
	require.Equal(t, nokverrors.KindAborted, nokverrors.KindOf(model.ErrMountRetired))
	require.Equal(t, nokverrors.KindResourceExhausted, nokverrors.KindOf(model.ErrQuotaExceeded))
	require.Equal(t, nokverrors.KindResourceExhausted, nokverrors.KindOf(model.ErrWatchOverflow))
	require.Equal(t, nokverrors.KindStaleEpoch, nokverrors.KindOf(model.ErrWatchCursorExpired))
}
