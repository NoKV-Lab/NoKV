// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

var visibleClientIDFallbackSeq atomic.Uint64

func visiblePutEffect(key, value []byte) compile.WriteEffect {
	return compile.WriteEffect{Kind: compile.EffectPut, Key: cloneBytes(key), Value: cloneBytes(value)}
}

func visibleDeleteEffect(key []byte) compile.WriteEffect {
	return compile.WriteEffect{Kind: compile.EffectDelete, Key: cloneBytes(key)}
}

func (e *Executor) nextVisibleOperationID(kind fsmeta.OperationKind) VisibleOperationID {
	seq := uint64(1)
	if e != nil {
		seq = e.visibleSeq.Add(1)
	}
	clientID := visibleOperationClientID(kind)
	if e != nil && e.visibleClientID != "" {
		clientID += "/" + e.visibleClientID
	}
	return VisibleOperationID{ClientID: clientID, Seq: seq}
}

func newVisibleClientID() string {
	var entropy [12]byte
	if _, err := rand.Read(entropy[:]); err == nil {
		return hex.EncodeToString(entropy[:])
	}
	return fmt.Sprintf("%x-%x-%x", os.Getpid(), time.Now().UnixNano(), visibleClientIDFallbackSeq.Add(1))
}

func visibleOperationClientID(kind fsmeta.OperationKind) string {
	switch kind {
	case fsmeta.OperationCreate:
		return "fsmeta-exec/create"
	case fsmeta.OperationUpdateInode:
		return "fsmeta-exec/update_inode"
	case fsmeta.OperationRename:
		return "fsmeta-exec/rename"
	case fsmeta.OperationLink:
		return "fsmeta-exec/link"
	case fsmeta.OperationUnlink:
		return "fsmeta-exec/unlink"
	case fsmeta.OperationRemove:
		return "fsmeta-exec/remove"
	case fsmeta.OperationRemoveDirectory:
		return "fsmeta-exec/remove_directory"
	case fsmeta.OperationOpenWriteSession:
		return "fsmeta-exec/open_write_session"
	case fsmeta.OperationHeartbeatSession:
		return "fsmeta-exec/heartbeat_write_session"
	case fsmeta.OperationCloseSession:
		return "fsmeta-exec/close_write_session"
	default:
		return "fsmeta-exec/" + string(kind)
	}
}

func (e *Executor) visibleQuotaMode() compile.QuotaMode {
	if e != nil && e.visibleCommitter == nil && e.quotas != nil {
		return compile.QuotaModeShared
	}
	return compile.QuotaModeNone
}

func (e *Executor) visibleQuotaAllowsCommit(ctx context.Context, changes []QuotaChange) (bool, error) {
	if e == nil || e.quotas == nil || len(changes) == 0 {
		return true, nil
	}
	admitter, ok := e.quotas.(VisibleQuotaAdmitter)
	if !ok {
		return false, nil
	}
	return admitter.AllowVisibleQuota(ctx, changes)
}
