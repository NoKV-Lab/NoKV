// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"bytes"
	"context"
	"math"
	"sync/atomic"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// QuotaLedger maintains local usage counters without enforcing rooted limits.
type QuotaLedger struct {
	reserveTotal        atomic.Uint64
	usageMutationsTotal atomic.Uint64
}

type quotaSubject struct {
	mount      model.MountID
	mountKeyID model.MountKeyID
	scope      model.InodeID
}

type quotaDelta struct {
	bytes  int64
	inodes int64
}

// NewQuotaLedger constructs an unlimited local quota ledger.
func NewQuotaLedger() *QuotaLedger {
	return &QuotaLedger{}
}

// ReserveQuota implements fsmetaexec.QuotaResolver. Local quotas are unlimited
// and read-derived, so the hot write path does not carry quota counter keys.
func (q *QuotaLedger) ReserveQuota(ctx context.Context, runner fsmetaexec.TxnRunner, changes []fsmetaexec.QuotaChange, startVersion uint64) ([]*kvrpcpb.Mutation, error) {
	if q == nil || runner == nil || len(changes) == 0 {
		return nil, nil
	}
	if _, err := aggregateQuotaChanges(changes); err != nil {
		return nil, err
	}
	q.reserveTotal.Add(1)
	return nil, nil
}

// AllowVisibleQuota keeps QuotaLedger compatible with executors that can
// admit visible writes. The local runtime does not wire a visible committer,
// but unlimited quota still has no durable counter side effects.
func (q *QuotaLedger) AllowVisibleQuota(context.Context, []fsmetaexec.QuotaChange) (bool, error) {
	return true, nil
}

// ReadQuotaUsage derives local unlimited-quota diagnostics from visible dentries
// instead of forcing every metadata write to update a mount-wide counter key.
func (q *QuotaLedger) ReadQuotaUsage(ctx context.Context, runner fsmetaexec.TxnRunner, mount model.MountIdentity, scope model.InodeID, version uint64) (model.UsageRecord, bool, error) {
	if q == nil || runner == nil {
		return model.UsageRecord{}, false, nil
	}
	if version == 0 {
		var err error
		version, err = runner.ReserveTimestamp(ctx, 1)
		if err != nil {
			return model.UsageRecord{}, true, err
		}
	}
	var (
		prefix []byte
		err    error
	)
	if scope == 0 {
		prefix, err = layout.EncodeMountPrefix(mount)
	} else {
		prefix, err = layout.EncodeDentryPrefix(mount, scope)
	}
	if err != nil {
		return model.UsageRecord{}, true, err
	}
	usage, err := q.deriveQuotaUsageFromDentries(ctx, runner, mount, prefix, version)
	return usage, true, err
}

// Stats returns local quota diagnostics.
func (q *QuotaLedger) Stats() map[string]any {
	if q == nil {
		return map[string]any{
			"reserve_total":         uint64(0),
			"usage_mutations_total": uint64(0),
			"limit_policy":          "unlimited",
		}
	}
	return map[string]any{
		"reserve_total":         q.reserveTotal.Load(),
		"usage_mutations_total": q.usageMutationsTotal.Load(),
		"limit_policy":          "unlimited",
	}
}

func (q *QuotaLedger) deriveQuotaUsageFromDentries(ctx context.Context, runner fsmetaexec.TxnRunner, mount model.MountIdentity, prefix []byte, version uint64) (model.UsageRecord, error) {
	var usage model.UsageRecord
	start := append([]byte(nil), prefix...)
	for {
		rows, err := runner.Scan(ctx, start, 256, version)
		if err != nil {
			return model.UsageRecord{}, err
		}
		if len(rows) == 0 {
			return usage, nil
		}
		for _, row := range rows {
			if !bytes.HasPrefix(row.Key, prefix) {
				return usage, nil
			}
			parts, ok := layout.InspectKey(row.Key)
			if !ok || parts.Kind != layout.KeyKindDentry {
				continue
			}
			dentry, err := layout.DecodeDentryValue(row.Value)
			if err != nil {
				return model.UsageRecord{}, err
			}
			inode, ok, err := q.readQuotaInode(ctx, runner, mount, dentry.Inode, version)
			if err != nil {
				return model.UsageRecord{}, err
			}
			if !ok {
				continue
			}
			usage.Bytes = saturatingAddUint64(usage.Bytes, inode.Size)
			usage.Inodes = saturatingAddUint64(usage.Inodes, 1)
		}
		start = nextQuotaScanKey(rows[len(rows)-1].Key)
	}
}

func (q *QuotaLedger) readQuotaInode(ctx context.Context, runner fsmetaexec.TxnRunner, mount model.MountIdentity, inodeID model.InodeID, version uint64) (model.InodeRecord, bool, error) {
	key, err := layout.EncodeInodeKey(mount, inodeID)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	value, ok, err := runner.Get(ctx, key, version)
	if err != nil || !ok {
		return model.InodeRecord{}, ok, err
	}
	inode, err := layout.DecodeInodeValue(value)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func nextQuotaScanKey(key []byte) []byte {
	next := make([]byte, 0, len(key)+1)
	next = append(next, key...)
	return append(next, 0)
}

func aggregateQuotaChanges(changes []fsmetaexec.QuotaChange) (map[quotaSubject]quotaDelta, error) {
	out := make(map[quotaSubject]quotaDelta)
	for _, change := range changes {
		if change.Mount == "" || change.MountKeyID == 0 {
			return nil, model.ErrInvalidMountID
		}
		addQuotaDelta(out, quotaSubject{mount: change.Mount, mountKeyID: change.MountKeyID}, change.Bytes, change.Inodes)
		if change.Scope != 0 {
			addQuotaDelta(out, quotaSubject{mount: change.Mount, mountKeyID: change.MountKeyID, scope: change.Scope}, change.Bytes, change.Inodes)
		}
	}
	for subject, delta := range out {
		if delta.bytes == 0 && delta.inodes == 0 {
			delete(out, subject)
		}
	}
	return out, nil
}

func addQuotaDelta(out map[quotaSubject]quotaDelta, subject quotaSubject, bytesDelta, inodesDelta int64) {
	delta := out[subject]
	delta.bytes = saturatingAddInt64(delta.bytes, bytesDelta)
	delta.inodes = saturatingAddInt64(delta.inodes, inodesDelta)
	out[subject] = delta
}

func saturatingAddInt64(current, delta int64) int64 {
	if delta > 0 && current > math.MaxInt64-delta {
		return math.MaxInt64
	}
	if delta < 0 && current < math.MinInt64-delta {
		return math.MinInt64
	}
	return current + delta
}

func saturatingAddUint64(current, delta uint64) uint64 {
	if delta > math.MaxUint64-current {
		return math.MaxUint64
	}
	return current + delta
}
