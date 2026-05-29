// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package errkind

import (
	stderrors "errors"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/feichai0017/NoKV/storage/wal"
	enginekv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
)

// Classify maps local DB and embedded-engine errors to the stable
// cross-boundary error taxonomy. Engine and utils packages intentionally keep
// their local sentinels and do not import the root errors package; public DB,
// runtime, or RPC boundaries should classify them here before deciding retry,
// abort, or corruption handling. Package-private control-flow sentinels stay
// Unknown and must not escape as public API decisions.
func Classify(err error) nokverrors.Kind {
	if err == nil {
		return nokverrors.KindUnknown
	}
	if kind := nokverrors.KindOf(err); kind != nokverrors.KindUnknown {
		return kind
	}
	switch {
	case stderrors.Is(err, utils.ErrKeyNotFound):
		return nokverrors.KindNotFound
	case stderrors.Is(err, utils.ErrEmptyKey),
		stderrors.Is(err, utils.ErrNilValue),
		stderrors.Is(err, utils.ErrInvalidRequest),
		stderrors.Is(err, vfs.ErrRenameNoReplaceUnsupported):
		return nokverrors.KindInvalidArgument
	case stderrors.Is(err, utils.ErrTxnTooBig):
		return nokverrors.KindResourceExhausted
	case stderrors.Is(err, utils.ErrBlockedWrites),
		stderrors.Is(err, utils.ErrHotKeyWriteThrottle),
		stderrors.Is(err, wal.ErrSegmentRetained),
		stderrors.Is(err, wal.ErrWALBackpressure):
		return nokverrors.KindRetryable
	case stderrors.Is(err, utils.ErrDBClosed):
		return nokverrors.KindAborted
	case stderrors.Is(err, enginekv.ErrChecksumMismatch),
		stderrors.Is(err, enginekv.ErrBadChecksum),
		stderrors.Is(err, enginekv.ErrPartialEntry),
		stderrors.Is(err, wal.ErrPartialRecord),
		stderrors.Is(err, wal.ErrEmptyRecord):
		return nokverrors.KindCorruption
	default:
		return nokverrors.KindUnknown
	}
}
