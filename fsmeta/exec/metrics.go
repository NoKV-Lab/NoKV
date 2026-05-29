// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

// recordUint64Max bumps the running maximum stored in max if value is larger,
// using a CAS loop so concurrent recordX calls do not lose a peak update.
func recordUint64Max(max *atomic.Uint64, value uint64) {
	if max == nil {
		return
	}
	for {
		old := max.Load()
		if value <= old {
			return
		}
		if max.CompareAndSwap(old, value) {
			return
		}
	}
}

// recordSlow attributes one slow-path admission to its slow reason bucket.
func (s *visibleAdmissionCounters) recordSlow(reason compile.SlowReason) {
	s.slowTotal.Add(1)
	switch reason {
	case compile.SlowReasonReadOnly:
		s.slowReadOnlyTotal.Add(1)
	case compile.SlowReasonRangeRead:
		s.slowRangeReadTotal.Add(1)
	case compile.SlowReasonDurabilityBarrier:
		s.slowDurabilityTotal.Add(1)
	case compile.SlowReasonCrossBucket:
		s.slowCrossBucketTotal.Add(1)
	case compile.SlowReasonSharedQuota:
		s.slowSharedQuotaTotal.Add(1)
	case compile.SlowReasonDynamicWriteSet:
		s.slowDynamicWriteTotal.Add(1)
	case compile.SlowReasonMaintenanceScan:
		s.slowMaintenanceTotal.Add(1)
	default:
		s.slowUnknownTotal.Add(1)
	}
}

// recordFallback records one unsupported one-phase attempt. The fallback streak
// is global for this operation kind because physical batch atomicity belongs to
// the backend, not to a filesystem key-placement proof.
func (s *atomicOnePhaseCounters) recordFallback() {
	s.consecutiveFallbacks.Add(1)
}

// recordSuccess clears the fallback streak after the backend accepts a
// one-phase mutation for this operation kind.
func (s *atomicOnePhaseCounters) recordSuccess() {
	s.consecutiveFallbacks.Store(0)
}
