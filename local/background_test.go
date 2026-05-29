// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/metrics"
	"github.com/stretchr/testify/require"
)

type testStatsCollector struct {
	started      int
	closeErr     error
	closed       int
	regionMetric *metrics.RegionMetrics
}

func (s *testStatsCollector) StartStats() {
	s.started++
}

func (s *testStatsCollector) SetRegionMetrics(rm *metrics.RegionMetrics) {
	s.regionMetric = rm
}

func (s *testStatsCollector) Close() error {
	s.closed++
	return s.closeErr
}

func TestBackgroundServicesLifecycle(t *testing.T) {
	stats := &testStatsCollector{}
	var services backgroundServices
	services.Init(stats)

	compacterStarts := 0
	services.Start(backgroundConfig{
		StartCompacter: func() {
			compacterStarts++
		},
		EnableControlWALWatchdog: true,
	})

	require.Equal(t, 1, compacterStarts)
	require.Equal(t, 1, stats.started)
	require.Same(t, stats, services.StatsCollector())
	require.Empty(t, services.ControlWALWatchdogs())

	rm := metrics.NewRegionMetrics()
	services.SetRegionMetrics(rm)
	require.Same(t, rm, stats.regionMetric)

	require.NoError(t, services.Close())
	require.Equal(t, 1, stats.closed)
	require.Nil(t, services.StatsCollector())
	require.Empty(t, services.ControlWALWatchdogs())
}

func TestBackgroundServicesClosePropagatesStatsError(t *testing.T) {
	stats := &testStatsCollector{closeErr: errors.New("close failed")}
	services := &backgroundServices{}
	services.Init(stats)

	err := services.Close()
	require.Error(t, err)
	require.Contains(t, err.Error(), "stats close: close failed")
	require.Equal(t, 1, stats.closed)
	require.Nil(t, services.StatsCollector())
}
