// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestExecutorGetQuotaUsage(t *testing.T) {
	runner := newFakeRunner()
	key, err := layout.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	value, err := layout.EncodeUsageValue(model.UsageRecord{Bytes: 4096, Inodes: 2})
	require.NoError(t, err)
	runner.data[string(key)] = value
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	usage, err := executor.GetQuotaUsage(context.Background(), model.QuotaUsageRequest{Mount: "vol", Scope: 7})
	require.NoError(t, err)
	require.Equal(t, model.UsageRecord{Bytes: 4096, Inodes: 2}, usage)
}

func TestExecutorGetQuotaUsageReturnsZeroForMissingCounter(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	usage, err := executor.GetQuotaUsage(context.Background(), model.QuotaUsageRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, model.UsageRecord{}, usage)
}
