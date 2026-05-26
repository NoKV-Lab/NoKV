// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestOpenRejectsInvalidSessionCleanupLimit(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr:     "127.0.0.1:1",
		SessionCleanupLimit: model.MaxSessionExpireLimit + 1,
	})
	require.ErrorContains(t, err, "session cleanup limit exceeds maximum")
}

func TestOpenRejectsNegativeLockTTL(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr: "127.0.0.1:1",
		LockTTL:         -time.Millisecond,
	})
	require.ErrorIs(t, err, errLockTTLInvalid)
}
