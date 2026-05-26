// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"errors"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

var (
	ErrClientRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/runtime: authority client required")
	ErrTableRequired      = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/runtime: authority table required")
	ErrHolderRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/runtime: holder id required")
	ErrTTLInvalid         = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/runtime: authority ttl must be non-negative")
	ErrInvalidResponse    = nokverrors.New(nokverrors.KindProtocolViolation, "experimental/peras/runtime: invalid authority response")
	ErrNotHeld            = nokverrors.New(nokverrors.KindNotLeader, "experimental/peras/runtime: authority is held by another holder")
	ErrRuntimeInvalid     = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/runtime: runtime config is invalid")
	ErrRuntimeClosed      = nokverrors.New(nokverrors.KindUnavailable, "experimental/peras/runtime: runtime is closed")
	ErrPublishRequired    = nokverrors.New(nokverrors.KindStaleEpoch, "experimental/peras/runtime: published flush requires active publish authority")
	ErrInvalidGrant       = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/runtime: invalid authority grant")
	ErrAmbiguousAuthority = nokverrors.New(nokverrors.KindConflict, "experimental/peras/runtime: ambiguous active authority")
	ErrConflictingGrant   = nokverrors.New(nokverrors.KindConflict, "experimental/peras/runtime: conflicting authority grant")
	ErrAuthorityViewStale = nokverrors.New(nokverrors.KindStaleEpoch, "experimental/peras/runtime: active authority view stale")
	ErrVisibleLogClosed   = nokverrors.New(nokverrors.KindUnavailable, "experimental/peras/runtime: visible log is closed")
)

func IsNotHeld(err error) bool {
	return errors.Is(err, ErrNotHeld)
}

func isAdmissionTerminalError(err error) bool {
	return errors.Is(err, model.ErrExists) ||
		errors.Is(err, model.ErrNotFound) ||
		errors.Is(err, model.ErrInvalidRequest) ||
		errors.Is(err, model.ErrInvalidValue)
}
