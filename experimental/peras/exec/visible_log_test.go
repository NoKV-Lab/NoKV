// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	"github.com/stretchr/testify/require"
)

func TestVisibleOperationRecordRoundTrip(t *testing.T) {
	record := VisibleOperationRecord{
		EpochID:           7,
		HolderID:          "holder-a",
		GrantID:           "grant-a",
		GrantExpiresNanos: 123456789,
		RootLineage:       VisibleRootLineage{ClusterEpoch: 1, Term: 2, Index: 3, Revision: 4},
		Scope:             testVisibleAuthorityScope(),
		Operation:         testVisibleReplayOperation(OperationID{ClientID: "client", Seq: 9}, []byte("a")),
		TimestampUnixNano: 1234,
	}
	payload, err := EncodeVisibleOperationRecord(record)
	require.NoError(t, err)
	decoded, err := DecodeVisibleOperationRecord(payload)
	require.NoError(t, err)
	require.Equal(t, record, decoded)
}

func TestVisibleOperationRecordEncodeToReusesScratch(t *testing.T) {
	record := VisibleOperationRecord{
		EpochID:           7,
		HolderID:          "holder-a",
		GrantID:           "grant-a",
		GrantExpiresNanos: 123456789,
		RootLineage:       VisibleRootLineage{ClusterEpoch: 1, Term: 2, Index: 3, Revision: 4},
		Scope:             testVisibleAuthorityScope(),
		Operation:         testVisibleReplayOperation(OperationID{ClientID: "client", Seq: 9}, []byte("a")),
		TimestampUnixNano: 1234,
	}
	scratch := make([]byte, 0, visibleOperationRecordEncodedSize(record))
	payload, err := EncodeVisibleOperationRecordTo(scratch, record)
	require.NoError(t, err)
	require.Equal(t, &scratch[:cap(scratch)][0], &payload[:cap(payload)][0])
	decoded, err := DecodeVisibleOperationRecord(payload)
	require.NoError(t, err)
	require.Equal(t, record, decoded)
}

func TestVisibleAppliedRecordRoundTrip(t *testing.T) {
	record := VisibleAppliedRecord{
		EpochID:  7,
		HolderID: "holder-a",
		Ranges: []VisibleAppliedRange{
			{SegmentID: 1, StartOffset: 128, EndOffset: 256},
			{SegmentID: 2, StartOffset: 64, EndOffset: 96},
		},
	}
	payload, err := EncodeVisibleAppliedRecord(record)
	require.NoError(t, err)
	decoded, err := DecodeVisibleAppliedRecord(payload)
	require.NoError(t, err)
	require.Equal(t, record, decoded)
}

func TestVisibleOperationRecordAcceptsOpenSessionReplay(t *testing.T) {
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	req := model.OpenWriteSessionRequest{
		Mount:   mount.MountID,
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	program, err := compile.CompileOpenWriteSessionProgram(req, mount)
	require.NoError(t, err)
	sessionValue, err := layout.EncodeSessionValue(model.SessionRecord{
		Session:       req.Session,
		Inode:         req.Inode,
		ExpiresUnixNs: time.Unix(0, 0).Add(req.TTL).UnixNano(),
	})
	require.NoError(t, err)
	inodeValue, err := layout.EncodeInodeValue(model.InodeRecord{
		Inode:     req.Inode,
		Type:      model.InodeTypeFile,
		LinkCount: 1,
	})
	require.NoError(t, err)
	frontier := proof.ProofFrontier{EpochID: 7, Sequence: 1}
	predicates := program.Compiled.Delta.ReadPredicates
	predicateProofs := []proof.PredicateProof{
		proof.NewPredicateProof(predicates[0].Key, inodeValue, true, 0, proof.ReadSourceOverlay, frontier),
		proof.NewPredicateProof(predicates[1].Key, nil, false, 0, proof.ReadSourceOverlay, frontier),
		proof.NewPredicateProof(predicates[2].Key, nil, false, 0, proof.ReadSourceOverlay, frontier),
	}
	op, err := compile.MaterializeOpenWriteSession(program, compile.OpenWriteSessionValues{
		SessionValue:    sessionValue,
		PredicateProofs: predicateProofs,
	})
	require.NoError(t, err)
	guardProofs, err := compile.GuardProofsFor(op.CompiledOp, predicateProofs, op.Delta.RuntimeGuards)
	require.NoError(t, err)
	op = compile.WithAdmissionProofs(op, predicateProofs, guardProofs)
	replay, err := ReplayOperationFromMaterialized(OperationID{ClientID: "session", Seq: 1}, op)
	require.NoError(t, err)

	record := VisibleOperationRecord{
		EpochID:           7,
		HolderID:          "holder-a",
		GrantID:           "grant-a",
		GrantExpiresNanos: time.Now().Add(time.Hour).UnixNano(),
		RootLineage:       VisibleRootLineage{ClusterEpoch: 1, Term: 2, Index: 3, Revision: 4},
		Scope:             op.Delta.Authority,
		Operation:         replay,
		TimestampUnixNano: time.Now().UnixNano(),
	}
	payload, err := EncodeVisibleOperationRecord(record)
	require.NoError(t, err)
	decoded, err := DecodeVisibleOperationRecord(payload)
	require.NoError(t, err)
	require.Equal(t, replay.PredicateProofDigest, decoded.Operation.PredicateProofDigest)
}

func TestVisibleOperationRecordAcceptsHeartbeatAndCloseSessionReplay(t *testing.T) {
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	session := model.SessionRecord{
		Session:       "writer-1",
		Inode:         44,
		ExpiresUnixNs: time.Now().Add(time.Second).UnixNano(),
	}
	oldValue, err := layout.EncodeSessionValue(session)
	require.NoError(t, err)
	newValue, err := layout.EncodeSessionValue(model.SessionRecord{
		Session:       session.Session,
		Inode:         session.Inode,
		ExpiresUnixNs: time.Now().Add(2 * time.Second).UnixNano(),
	})
	require.NoError(t, err)
	frontier := proof.ProofFrontier{EpochID: 7, Sequence: 2}

	heartbeat, err := compile.CompileHeartbeatWriteSessionProgram(model.HeartbeatWriteSessionRequest{
		Mount:   mount.MountID,
		Inode:   session.Inode,
		Session: session.Session,
		TTL:     time.Second,
	}, mount)
	require.NoError(t, err)
	heartbeatProofs := predicateProofsForKeys(heartbeat.Compiled.Delta.ReadPredicates, oldValue, frontier)
	heartbeatOp, err := compile.MaterializeHeartbeatWriteSession(heartbeat, compile.HeartbeatWriteSessionValues{
		SessionValue:    newValue,
		PredicateProofs: heartbeatProofs,
	})
	require.NoError(t, err)
	requireVisibleOperationRecordEncodesMaterialized(t, heartbeatOp, heartbeatProofs, OperationID{ClientID: "session", Seq: 2})

	closeProgram, err := compile.CompileCloseWriteSessionProgram(model.CloseWriteSessionRequest{
		Mount:   mount.MountID,
		Inode:   session.Inode,
		Session: session.Session,
	}, mount)
	require.NoError(t, err)
	closeProofs := predicateProofsForKeys(closeProgram.Compiled.Delta.ReadPredicates, oldValue, frontier)
	closeOp, err := compile.MaterializeCloseWriteSession(closeProgram, compile.CloseWriteSessionValues{
		DeleteOwner:     true,
		PredicateProofs: closeProofs,
	})
	require.NoError(t, err)
	requireVisibleOperationRecordEncodesMaterialized(t, closeOp, closeProofs, OperationID{ClientID: "session", Seq: 3})
}

func predicateProofsForKeys(predicates []compile.Predicate, value []byte, frontier proof.ProofFrontier) []proof.PredicateProof {
	out := make([]proof.PredicateProof, 0, len(predicates))
	for _, predicate := range predicates {
		out = append(out, proof.NewPredicateProof(predicate.Key, value, true, 0, proof.ReadSourceOverlay, frontier))
	}
	return out
}

func requireVisibleOperationRecordEncodesMaterialized(t *testing.T, op compile.MaterializedOp, predicateProofs []proof.PredicateProof, id OperationID) {
	t.Helper()
	guardProofs, err := compile.GuardProofsFor(op.CompiledOp, predicateProofs, op.Delta.RuntimeGuards)
	require.NoError(t, err)
	op = compile.WithAdmissionProofs(op, predicateProofs, guardProofs)
	replay, err := ReplayOperationFromMaterialized(id, op)
	require.NoError(t, err)
	record := VisibleOperationRecord{
		EpochID:           7,
		HolderID:          "holder-a",
		GrantID:           "grant-a",
		GrantExpiresNanos: time.Now().Add(time.Hour).UnixNano(),
		RootLineage:       VisibleRootLineage{ClusterEpoch: 1, Term: 2, Index: 3, Revision: 4},
		Scope:             op.Delta.Authority,
		Operation:         replay,
		TimestampUnixNano: time.Now().UnixNano(),
	}
	_, err = EncodeVisibleOperationRecord(record)
	require.NoError(t, err)
}

func testVisibleReplayOperation(id OperationID, key []byte) ReplayOperation {
	segment := compile.SegmentPlan{
		MergeKey: compile.SegmentMergeKey{
			MountKeyID:       1,
			HasPrimaryBucket: true,
			PrimaryBucket:    1,
			Install:          compile.SegmentInstallSingleBucket,
			Durability:       compile.DurabilityVisibleOnly,
			FormatVersion:    1,
		},
		Install:               compile.SegmentInstallSingleBucket,
		MaterializeMergeKey:   compile.SegmentMergeKey{MountKeyID: 1, HasPrimaryBucket: true, PrimaryBucket: 1, Install: compile.SegmentInstallSingleBucket, Durability: compile.DurabilityVisibleOnly, FormatVersion: 1},
		MaterializeInstall:    compile.SegmentInstallSingleBucket,
		CanAppend:             true,
		CanMaterialize:        true,
		EstimatedPayloadBytes: 64,
		OperationCount:        1,
		MutationCount:         1,
	}
	atomicity := compile.AtomicityGroup{
		Members:  []compile.MutationID{1},
		Recovery: compile.RecoveryReplayAllOrNothing,
		Digest:   filledDigest(2),
	}
	return ReplayOperation{
		OpID:                 id,
		Kind:                 model.OperationCreate,
		DescriptorDigest:     filledDigest(1),
		PredicateProofDigest: compile.AdmissionProofSetDigest(nil, nil),
		ExecutionPlanDigest:  compile.ExecutionPlanDigest(segment, atomicity, compile.DurabilityVisibleOnly),
		Segment:              segment,
		Atomicity:            atomicity,
		Durability:           compile.DurabilityVisibleOnly,
		Mutations:            []ReplayMutation{{Key: key, Value: []byte("value")}},
	}
}

func testVisibleAuthorityScope() compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      "m",
		MountKeyID: 1,
		Buckets:    []layout.AffinityBucket{},
		Parents:    []model.InodeID{2},
		Inodes:     []model.InodeID{},
	}
}

func filledDigest(seed byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = seed
	}
	return out
}
