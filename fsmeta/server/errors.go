// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	fsmetaReasonMetadata       = "fsmeta_reason"
	reasonQuotaExceeded        = "quota_exceeded"
	reasonWatchOverflow        = "watch_overflow"
	reasonWatchCursorExpired   = "watch_cursor_expired"
	reasonMountNotRegistered   = "mount_not_registered"
	reasonMountRetired         = "mount_retired"
	reasonCrossAuthorityRename = "cross_authority_rename"
	reasonNamespaceExists      = "entry_exists"
	reasonNamespaceNotFound    = "entry_not_found"
	reasonInvalidFSMetaInput   = "invalid_fsmeta_input"
	reasonInvalidMountID       = "invalid_mount_id"
	reasonInvalidInodeID       = "invalid_inode_id"
	reasonInvalidName          = "invalid_name"
	reasonInvalidSession       = "invalid_session"
	reasonInvalidRequest       = "invalid_request"
	reasonInvalidKey           = "invalid_key"
	reasonInvalidKeyKind       = "invalid_key_kind"
	reasonInvalidValue         = "invalid_value"
	reasonInvalidValueKind     = "invalid_value_kind"
	reasonInvalidPageSize      = "invalid_page_size"
	reasonServiceUnavailable   = "service_unavailable"
	reasonContextCanceled      = "context_canceled"
	reasonContextDeadline      = "context_deadline"
)

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return nokverrors.RPCStatusError(nokverrors.KindAborted, codes.Canceled, err.Error(), map[string]string{
			fsmetaReasonMetadata: reasonContextCanceled,
		})
	case errors.Is(err, context.DeadlineExceeded):
		return nokverrors.RPCStatusError(nokverrors.KindUnavailable, codes.DeadlineExceeded, err.Error(), map[string]string{
			fsmetaReasonMetadata: reasonContextDeadline,
		})
	}
	var carrier nokverrors.KindCarrier
	if errors.As(err, &carrier) {
		kind := nokverrors.KindOf(err)
		if kind != nokverrors.KindUnknown {
			return nokverrors.RPCStatusError(kind, rpcCodeForKind(kind), err.Error(), fsmetaErrorMetadata(err))
		}
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	kind := nokverrors.KindOf(err)
	return nokverrors.RPCStatusError(kind, rpcCodeForKind(kind), err.Error(), fsmetaErrorMetadata(err))
}

func rpcInvalidArgument(message string) error {
	return nokverrors.RPCStatusError(nokverrors.KindInvalidArgument, codes.InvalidArgument, message, map[string]string{
		fsmetaReasonMetadata: reasonInvalidFSMetaInput,
	})
}

func rpcServiceUnavailable(message string) error {
	return nokverrors.RPCStatusError(nokverrors.KindProtocolViolation, codes.FailedPrecondition, message, map[string]string{
		fsmetaReasonMetadata: reasonServiceUnavailable,
	})
}

func fsmetaErrorMetadata(err error) map[string]string {
	reason := ""
	switch {
	case errors.Is(err, model.ErrQuotaExceeded):
		reason = reasonQuotaExceeded
	case errors.Is(err, model.ErrWatchOverflow):
		reason = reasonWatchOverflow
	case errors.Is(err, model.ErrWatchCursorExpired):
		reason = reasonWatchCursorExpired
	case errors.Is(err, model.ErrMountNotRegistered):
		reason = reasonMountNotRegistered
	case errors.Is(err, model.ErrMountRetired):
		reason = reasonMountRetired
	case errors.Is(err, model.ErrCrossAuthorityRename):
		reason = reasonCrossAuthorityRename
	case errors.Is(err, model.ErrExists):
		reason = reasonNamespaceExists
	case errors.Is(err, model.ErrNotFound):
		reason = reasonNamespaceNotFound
	case errors.Is(err, model.ErrInvalidMountID):
		reason = reasonInvalidMountID
	case errors.Is(err, model.ErrInvalidInodeID):
		reason = reasonInvalidInodeID
	case errors.Is(err, model.ErrInvalidName):
		reason = reasonInvalidName
	case errors.Is(err, model.ErrInvalidSession):
		reason = reasonInvalidSession
	case errors.Is(err, model.ErrInvalidRequest):
		reason = reasonInvalidRequest
	case errors.Is(err, layout.ErrInvalidKey):
		reason = reasonInvalidKey
	case errors.Is(err, layout.ErrInvalidKeyKind):
		reason = reasonInvalidKeyKind
	case errors.Is(err, model.ErrInvalidValue):
		reason = reasonInvalidValue
	case errors.Is(err, layout.ErrInvalidValueKind):
		reason = reasonInvalidValueKind
	case errors.Is(err, model.ErrInvalidPageSize):
		reason = reasonInvalidPageSize
	}
	if reason == "" {
		return nil
	}
	return map[string]string{fsmetaReasonMetadata: reason}
}

func rpcCodeForKind(kind nokverrors.Kind) codes.Code {
	switch kind {
	case nokverrors.KindInvalidArgument:
		return codes.InvalidArgument
	case nokverrors.KindNotFound:
		return codes.NotFound
	case nokverrors.KindAlreadyExists:
		return codes.AlreadyExists
	case nokverrors.KindResourceExhausted:
		return codes.ResourceExhausted
	case nokverrors.KindStaleEpoch:
		return codes.OutOfRange
	case nokverrors.KindConflict,
		nokverrors.KindWriteConflict,
		nokverrors.KindLockConflict,
		nokverrors.KindCommitTsExpired:
		return codes.Aborted
	case nokverrors.KindAborted,
		nokverrors.KindProtocolViolation:
		return codes.FailedPrecondition
	case nokverrors.KindRetryable,
		nokverrors.KindRetryExhausted,
		nokverrors.KindUnavailable,
		nokverrors.KindRouteUnavailable,
		nokverrors.KindRegionRouting,
		nokverrors.KindNotLeader:
		return codes.Unavailable
	case nokverrors.KindCorruption:
		return codes.DataLoss
	default:
		return codes.Internal
	}
}
