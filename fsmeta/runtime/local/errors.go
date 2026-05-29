// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

var (
	errWorkDirRequired      = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: work dir is required")
	errDBRequired           = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: local DB is required")
	errMountRequired        = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: mount identity is required")
	errTimestampCount       = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: timestamp count must be > 0")
	errCommitVersion        = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: commit version must be greater than start version")
	errEmptyMutationKey     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: empty mutation key")
	errUnsupportedMutation  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: unsupported mutation op")
	errNonAtomicApplyGroup  = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/local: local DB cannot atomically apply fsmeta mutation group")
	errInvalidInternalEntry = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/local: invalid MVCC internal entry")
	errInvalidAtomicMutate  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: invalid atomic mutate")
	errAtomicPredicate      = nokverrors.New(nokverrors.KindRetryable, "fsmeta/runtime/local: atomic predicate mismatch")
	errInvalidCacheMode     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: invalid cache mode")
)

func txnKeyError(errs ...*kvrpcpb.KeyError) error {
	return nokverrors.NewTxnKeyError(errs...)
}

func txnAbort(err error) error {
	if err == nil {
		return nil
	}
	return txnKeyError(&kvrpcpb.KeyError{Abort: err.Error()})
}

func txnRetryable(err error) error {
	if err == nil {
		return nil
	}
	return txnKeyError(&kvrpcpb.KeyError{Retryable: err.Error()})
}

func txnAlreadyExists(key []byte) error {
	return txnKeyError(&kvrpcpb.KeyError{
		AlreadyExists: &kvrpcpb.KeyAlreadyExists{Key: cloneBytes(key)},
	})
}

func txnCommitExpired(key []byte, commitVersion, minCommitVersion uint64) error {
	return txnKeyError(&kvrpcpb.KeyError{
		CommitTsExpired: &kvrpcpb.CommitTsExpired{
			Key:         cloneBytes(key),
			CommitTs:    commitVersion,
			MinCommitTs: minCommitVersion,
		},
	})
}

func txnUnsupportedMutation(op backend.MutationOp) error {
	return txnAbort(fmt.Errorf("%w: %d", errUnsupportedMutation, op))
}
