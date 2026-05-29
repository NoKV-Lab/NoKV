// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/utils/mmap"
)

func toMmapAdvice(pattern utils.AccessPattern) mmap.Advice {
	switch pattern {
	case utils.AccessPatternSequential:
		return mmap.AdviceSequential
	case utils.AccessPatternRandom:
		return mmap.AdviceRandom
	case utils.AccessPatternWillNeed:
		return mmap.AdviceWillNeed
	case utils.AccessPatternDontNeed:
		return mmap.AdviceDontNeed
	case utils.AccessPatternNormal, utils.AccessPatternAuto:
		fallthrough
	default:
		return mmap.AdviceNormal
	}
}

// Advise passes an OS-specific access hint down to the mmap layer.
func (m *MmapFile) Advise(pattern utils.AccessPattern) error {
	if m == nil || len(m.Data) == 0 {
		return nil
	}
	err := mmap.MadvisePattern(m.Data, toMmapAdvice(pattern))
	metrics.RecordMmapMadvise(err == nil)
	return err
}
