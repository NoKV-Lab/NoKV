// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package model

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	ErrInvalidMountID       = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid mount id")
	ErrInvalidInodeID       = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid inode id")
	ErrInvalidName          = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid name")
	ErrInvalidSession       = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid session id")
	ErrInvalidRequest       = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid request")
	ErrInvalidValue         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid value")
	ErrInvalidPageSize      = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid page size")
	ErrExists               = nokverrors.New(nokverrors.KindAlreadyExists, "fsmeta: entry exists")
	ErrNotFound             = nokverrors.New(nokverrors.KindNotFound, "fsmeta: entry not found")
	ErrMountNotRegistered   = nokverrors.New(nokverrors.KindNotFound, "fsmeta: mount is not registered")
	ErrMountRetired         = nokverrors.New(nokverrors.KindAborted, "fsmeta: mount is retired")
	ErrCrossAuthorityRename = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta: rename crosses subtree authority")
	ErrQuotaExceeded        = nokverrors.New(nokverrors.KindResourceExhausted, "fsmeta: quota exceeded")
	ErrWatchOverflow        = nokverrors.New(nokverrors.KindResourceExhausted, "fsmeta: watch backlog overflow")
	ErrWatchCursorExpired   = nokverrors.New(nokverrors.KindStaleEpoch, "fsmeta: watch cursor expired")
)
