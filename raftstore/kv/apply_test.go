// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	"errors"
	"testing"
	"time"

	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"

	local "github.com/feichai0017/NoKV/local"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/txn/mvcc"
	"github.com/feichai0017/NoKV/txn/percolator"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func applyVersionedEntryForApplyTest(t *testing.T, db *local.DB, cf entrykv.ColumnFamily, key []byte, version uint64, value []byte, meta byte, expiresAt uint64) {
	t.Helper()
	entry := entrykv.NewInternalEntry(cf, key, version, entrykv.SafeCopy(nil, value), meta, expiresAt)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*entrykv.Entry{entry}))
}

func TestNewEntryApplierAppliesEntries(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	applier := NewEntryApplier(db)

	raftReq := &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("k1"), Version: 1}},
		}},
	}
	raftData, err := command.Encode(raftReq)
	require.NoError(t, err)

	err = applier([]myraft.Entry{
		{Type: myraft.EntryNormal, Data: raftData},
		{Type: myraft.EntryConfChange},
	})
	require.NoError(t, err)
}

func TestNewEntryApplierRejectsLegacyPayload(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	applier := NewEntryApplier(db)

	cmdData, err := command.Encode(&raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("k1"), Version: 1}},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, applier([]myraft.Entry{{Type: myraft.EntryNormal, Data: cmdData}}))

	err = applier([]myraft.Entry{{Type: myraft.EntryNormal, Data: []byte("legacy")}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported unframed raft payload")
}

func TestLockedErrorMapping(t *testing.T) {
	key := []byte("lock-key")
	require.Nil(t, lockedError(key, nil))

	lock := &mvcc.Lock{
		Primary:     []byte("primary"),
		Ts:          42,
		StartTime:   4200,
		TTL:         9000,
		Kind:        kvrpcpb.Mutation_Put,
		MinCommitTs: 100,
	}
	keyErr := lockedError(key, lock)
	require.NotNil(t, keyErr)
	require.NotNil(t, keyErr.GetLocked())
	require.Equal(t, lock.Primary, keyErr.GetLocked().GetPrimaryLock())
	require.Equal(t, entrykv.SafeCopy(nil, key), keyErr.GetLocked().GetKey())
	require.Equal(t, lock.Ts, keyErr.GetLocked().GetLockVersion())
	require.Equal(t, lock.TTL, keyErr.GetLocked().GetLockTtl())
	require.Equal(t, lock.Kind, keyErr.GetLocked().GetLockType())
	require.Equal(t, lock.MinCommitTs, keyErr.GetLocked().GetMinCommitTs())
}

func TestApplyMVCCMaintenanceAppliesInternalEntries(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
			Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
				Tombstones: []*kvrpcpb.InternalEntryTombstone{
					{
						ColumnFamily: kvrpcpb.InternalEntryTombstone_DEFAULT,
						Key:          []byte("maint-default"),
						Version:      11,
					},
					{
						ColumnFamily: kvrpcpb.InternalEntryTombstone_WRITE,
						Key:          []byte("maint-write"),
						Version:      22,
					},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	require.Equal(t, uint64(2), resp.GetResponses()[0].GetMvccMaintenance().GetAppliedEntries())
	require.Nil(t, resp.GetResponses()[0].GetMvccMaintenance().GetError())

	gotDefault, err := db.GetInternalEntry(entrykv.CFDefault, []byte("maint-default"), 11)
	require.NoError(t, err)
	defer gotDefault.DecrRef()
	require.NotZero(t, gotDefault.Meta&entrykv.BitDelete)

	gotWrite, err := db.GetInternalEntry(entrykv.CFWrite, []byte("maint-write"), 22)
	require.NoError(t, err)
	defer gotWrite.DecrRef()
	require.NotZero(t, gotWrite.Meta&entrykv.BitDelete)
}

func TestApplyInstallPreparedMVCCEntriesAppliesInternalEntries(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	version := uint64(33)
	writeValue := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: version})
	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
			Cmd: &raftcmdpb.Request_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesRequest{
				RoutingKey:    []byte("prepared-value"),
				CommitVersion: version,
				Entries: []*kvrpcpb.PreparedMVCCEntry{
					{
						ColumnFamily: kvrpcpb.PreparedMVCCEntry_DEFAULT,
						Key:          []byte("prepared-value"),
						Version:      version,
						Value:        []byte("value"),
						HasValue:     true,
					},
					{
						ColumnFamily: kvrpcpb.PreparedMVCCEntry_WRITE,
						Key:          []byte("prepared-value"),
						Version:      version,
						Value:        writeValue,
						HasValue:     true,
					},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	installResp := resp.GetResponses()[0].GetInstallPreparedMvcc()
	require.Nil(t, installResp.GetError())
	require.Equal(t, uint64(2), installResp.GetAppliedEntries())
	require.Equal(t, version, installResp.GetCommitVersion())

	gotDefault, err := db.GetInternalEntry(entrykv.CFDefault, []byte("prepared-value"), version)
	require.NoError(t, err)
	defer gotDefault.DecrRef()
	require.Equal(t, []byte("value"), gotDefault.Value)

	gotWrite, err := db.GetInternalEntry(entrykv.CFWrite, []byte("prepared-value"), version)
	require.NoError(t, err)
	defer gotWrite.DecrRef()
	require.Equal(t, writeValue, gotWrite.Value)
}

func TestApplyInstallPreparedMVCCEntriesRejectsMalformedBatch(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
			Cmd: &raftcmdpb.Request_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesRequest{
				RoutingKey:    []byte("prepared-a"),
				CommitVersion: 44,
				Entries: []*kvrpcpb.PreparedMVCCEntry{
					{
						ColumnFamily: kvrpcpb.PreparedMVCCEntry_DEFAULT,
						Key:          []byte("prepared-a"),
						Version:      44,
						Value:        []byte("value"),
						HasValue:     true,
					},
					{
						ColumnFamily: kvrpcpb.PreparedMVCCEntry_DEFAULT,
						Key:          []byte("prepared-b"),
						Version:      45,
						Value:        []byte("must-not-apply"),
						HasValue:     true,
					},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	keyErr := resp.GetResponses()[0].GetInstallPreparedMvcc().GetError()
	require.NotNil(t, keyErr)
	require.Contains(t, keyErr.GetAbort(), "version")
	_, err = db.GetInternalEntry(entrykv.CFDefault, []byte("prepared-a"), 44)
	require.Error(t, err)
	_, err = db.GetInternalEntry(entrykv.CFDefault, []byte("prepared-b"), 45)
	require.Error(t, err)
}

func TestApplyBatchHandlesInstallPreparedMVCCEntriesRequests(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	reqs := []*raftcmdpb.RaftCmdRequest{
		installPreparedMVCCRequestForApplyTest(51, []byte("prepared-batch-a"), []byte("va")),
		installPreparedMVCCRequestForApplyTest(52, []byte("prepared-batch-b"), []byte("vb")),
	}
	resps, err := ApplyBatch(db, nil, reqs)
	require.NoError(t, err)
	require.Len(t, resps, 2)
	for idx, resp := range resps {
		installResp := resp.GetResponses()[0].GetInstallPreparedMvcc()
		require.Nil(t, installResp.GetError())
		require.Equal(t, uint64(1), installResp.GetAppliedEntries())
		require.Equal(t, uint64(51+idx), installResp.GetCommitVersion())
	}

	gotA, err := db.GetInternalEntry(entrykv.CFDefault, []byte("prepared-batch-a"), 51)
	require.NoError(t, err)
	defer gotA.DecrRef()
	require.Equal(t, []byte("va"), gotA.Value)

	gotB, err := db.GetInternalEntry(entrykv.CFDefault, []byte("prepared-batch-b"), 52)
	require.NoError(t, err)
	defer gotB.DecrRef()
	require.Equal(t, []byte("vb"), gotB.Value)
}

func TestNewApplierRejectsFencedVisibleAuthorityWrites(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	key, err := layout.EncodeDentryKey(mount, 42, "artifact")
	require.NoError(t, err)

	applier := NewApplier(db, nil, WithWriteFence(perasFenceForApplyTest{perasFenceTableForApplyTest(t, mount)}))
	resp, err := applier(&raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
			Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: atomicPutForApplyTest(10, 11, key, []byte("value"))},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	atomicResp := resp.GetResponses()[0].GetTryAtomicMutate()
	require.NotNil(t, atomicResp)
	require.Contains(t, atomicResp.GetError().GetRetryable(), "write fence")

	reader := percolator.NewReader(db)
	_, _, err = reader.GetValue(key, 12)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestNewApplierRejectsFsmetaWritesWhenVisibleAuthorityViewIsStale(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	key, err := layout.EncodeDentryKey(mount, 42, "artifact")
	require.NoError(t, err)

	applier := NewApplier(db, nil, WithWriteFence(perasFenceForApplyTest{runtimeperas.NewActiveAuthorities()}))
	resp, err := applier(&raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
			Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: atomicPutForApplyTest(10, 11, key, []byte("value"))},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	atomicResp := resp.GetResponses()[0].GetTryAtomicMutate()
	require.NotNil(t, atomicResp)
	require.Contains(t, atomicResp.GetError().GetRetryable(), "active authority view stale")

	reader := percolator.NewReader(db)
	_, _, err = reader.GetValue(key, 12)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestNewBatchApplierSplitsAroundFencedVisibleAuthorityWrite(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	fencedKey, err := layout.EncodeDentryKey(mount, 42, "artifact")
	require.NoError(t, err)

	applier := NewBatchApplier(db, nil, WithWriteFence(perasFenceForApplyTest{perasFenceTableForApplyTest(t, mount)}))
	resps, err := applier([]*raftcmdpb.RaftCmdRequest{
		{
			Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
				Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: atomicPutForApplyTest(20, 21, []byte("plain-a"), []byte("va"))},
			}},
		},
		{
			Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
				Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: atomicPutForApplyTest(22, 23, fencedKey, []byte("vf"))},
			}},
		},
		{
			Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
				Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: atomicPutForApplyTest(24, 25, []byte("plain-b"), []byte("vb"))},
			}},
		},
	})
	require.NoError(t, err)
	require.Len(t, resps, 3)
	require.Nil(t, resps[0].GetResponses()[0].GetTryAtomicMutate().GetError())
	require.Contains(t, resps[1].GetResponses()[0].GetTryAtomicMutate().GetError().GetRetryable(), "write fence")
	require.Nil(t, resps[2].GetResponses()[0].GetTryAtomicMutate().GetError())

	reader := percolator.NewReader(db)
	value, _, err := reader.GetValue([]byte("plain-a"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("va"), value)
	value, _, err = reader.GetValue([]byte("plain-b"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("vb"), value)
	_, _, err = reader.GetValue(fencedKey, 30)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestApplyTryAtomicMutateCommandMaterializesBothKeys(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
			Cmd: &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateRequest{
				StartVersion:  10,
				CommitVersion: 11,
				Predicates: []*kvrpcpb.AtomicPredicate{
					{Key: []byte("dentry"), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
					{Key: []byte("inode"), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
				},
				Mutations: []*kvrpcpb.Mutation{
					{Op: kvrpcpb.Mutation_Put, Key: []byte("dentry"), Value: []byte("ino=42"), AssertionNotExist: true},
					{Op: kvrpcpb.Mutation_Put, Key: []byte("inode"), Value: []byte("attrs"), AssertionNotExist: true},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	require.Nil(t, resp.GetResponses()[0].GetTryAtomicMutate().GetError())
	require.False(t, resp.GetResponses()[0].GetTryAtomicMutate().GetFallbackToTwoPhaseCommit())
	require.Equal(t, uint64(2), resp.GetResponses()[0].GetTryAtomicMutate().GetAppliedKeys())

	reader := percolator.NewReader(db)
	dentry, _, err := reader.GetValue([]byte("dentry"), 11)
	require.NoError(t, err)
	require.Equal(t, []byte("ino=42"), dentry)
	inode, _, err := reader.GetValue([]byte("inode"), 11)
	require.NoError(t, err)
	require.Equal(t, []byte("attrs"), inode)
}

func TestApplyTryAtomicMutateBatchFusesLocalApply(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := &countingAtomicApplyStore{base: db}
	resp, err := Apply(store, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{
			{
				CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
				Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: atomicPutForApplyTest(40, 41, []byte("batched-a"), []byte("va"))},
			},
			{
				CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
				Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: atomicPutForApplyTest(42, 43, []byte("batched-b"), []byte("vb"))},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 2)
	for _, raftResp := range resp.GetResponses() {
		atomicResp := raftResp.GetTryAtomicMutate()
		require.NotNil(t, atomicResp)
		require.Nil(t, atomicResp.GetError())
		require.False(t, atomicResp.GetFallbackToTwoPhaseCommit())
		require.Equal(t, uint64(1), atomicResp.GetAppliedKeys())
	}
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{2}, store.appliedEntryCounts)
}

func TestApplyBatchFusesTryAtomicMutateAcrossRaftRequests(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := &countingAtomicApplyStore{base: db}
	resps, err := ApplyBatch(store, nil, []*raftcmdpb.RaftCmdRequest{
		{
			Header: &raftcmdpb.CmdHeader{RequestId: 10},
			Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
				Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: atomicPutForApplyTest(60, 61, []byte("batch-entry-a"), []byte("va"))},
			}},
		},
		{
			Header: &raftcmdpb.CmdHeader{RequestId: 11},
			Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
				Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: atomicPutForApplyTest(62, 63, []byte("batch-entry-b"), []byte("vb"))},
			}},
		},
	})
	require.NoError(t, err)
	require.Len(t, resps, 2)
	require.Equal(t, uint64(10), resps[0].GetHeader().GetRequestId())
	require.Equal(t, uint64(11), resps[1].GetHeader().GetRequestId())
	for _, resp := range resps {
		atomicResp := resp.GetResponses()[0].GetTryAtomicMutate()
		require.NotNil(t, atomicResp)
		require.Nil(t, atomicResp.GetError())
		require.False(t, atomicResp.GetFallbackToTwoPhaseCommit())
		require.Equal(t, uint64(1), atomicResp.GetAppliedKeys())
	}
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{2}, store.appliedEntryCounts)

	reader := percolator.NewReader(db)
	value, _, err := reader.GetValue([]byte("batch-entry-a"), 70)
	require.NoError(t, err)
	require.Equal(t, []byte("va"), value)
	value, _, err = reader.GetValue([]byte("batch-entry-b"), 70)
	require.NoError(t, err)
	require.Equal(t, []byte("vb"), value)
}

func TestApplyPrewriteBatchFusesLocalApply(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := &countingAtomicApplyStore{base: db}
	resp, err := Apply(store, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{
			{
				CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
				Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: prewriteForApplyTest(50, []byte("prewrite-batched-a"), []byte("va"))},
			},
			{
				CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
				Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: prewriteForApplyTest(52, []byte("prewrite-batched-b"), []byte("vb"))},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 2)
	for _, raftResp := range resp.GetResponses() {
		prewriteResp := raftResp.GetPrewrite()
		require.NotNil(t, prewriteResp)
		require.Empty(t, prewriteResp.GetErrors())
	}
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{2}, store.appliedEntryCounts)
}

func TestApplyTryAtomicMutateCommandAppliesPebbleBatch(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	startVersion := uint64(30)
	commitVersion := uint64(31)
	dentryKey, inodeKey := []byte("apply-atomic-dentry"), []byte("apply-atomic-inode")
	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
			Cmd: &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateRequest{
				StartVersion:  startVersion,
				CommitVersion: commitVersion,
				Predicates: []*kvrpcpb.AtomicPredicate{
					{Key: dentryKey, Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
					{Key: inodeKey, Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
				},
				Mutations: []*kvrpcpb.Mutation{
					{Op: kvrpcpb.Mutation_Put, Key: dentryKey, Value: []byte("ino=42"), AssertionNotExist: true},
					{Op: kvrpcpb.Mutation_Put, Key: inodeKey, Value: []byte("attrs"), AssertionNotExist: true},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	atomicResp := resp.GetResponses()[0].GetTryAtomicMutate()
	require.NotNil(t, atomicResp)
	require.Nil(t, atomicResp.GetError())
	require.False(t, atomicResp.GetFallbackToTwoPhaseCommit())
	require.Equal(t, uint64(2), atomicResp.GetAppliedKeys())

	reader := percolator.NewReader(db)
	value, _, err := reader.GetValue(dentryKey, commitVersion)
	require.NoError(t, err)
	require.Equal(t, []byte("ino=42"), value)
	value, _, err = reader.GetValue(inodeKey, commitVersion)
	require.NoError(t, err)
	require.Equal(t, []byte("attrs"), value)
}

func atomicPutForApplyTest(startVersion, commitVersion uint64, key, value []byte) *kvrpcpb.TryAtomicMutateRequest {
	return &kvrpcpb.TryAtomicMutateRequest{
		StartVersion:  startVersion,
		CommitVersion: commitVersion,
		Predicates: []*kvrpcpb.AtomicPredicate{
			{Key: entrykv.SafeCopy(nil, key), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
		},
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: entrykv.SafeCopy(nil, key), Value: entrykv.SafeCopy(nil, value), AssertionNotExist: true},
		},
	}
}

func installPreparedMVCCRequestForApplyTest(version uint64, key, value []byte) *raftcmdpb.RaftCmdRequest {
	return &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
			Cmd: &raftcmdpb.Request_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesRequest{
				RoutingKey:    entrykv.SafeCopy(nil, key),
				CommitVersion: version,
				Entries: []*kvrpcpb.PreparedMVCCEntry{{
					ColumnFamily: kvrpcpb.PreparedMVCCEntry_DEFAULT,
					Key:          entrykv.SafeCopy(nil, key),
					Version:      version,
					Value:        entrykv.SafeCopy(nil, value),
					HasValue:     true,
				}},
			}},
		}},
	}
}

func prewriteForApplyTest(startVersion uint64, key, value []byte) *kvrpcpb.PrewriteRequest {
	return &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: entrykv.SafeCopy(nil, key), Value: entrykv.SafeCopy(nil, value)},
		},
		PrimaryLock:  entrykv.SafeCopy(nil, key),
		StartVersion: startVersion,
		LockTtl:      1000,
	}
}

type countingAtomicApplyStore struct {
	base               *local.DB
	applyCalls         int
	appliedEntryCounts []int
}

func (s *countingAtomicApplyStore) ApplyInternalEntries(entries []*entrykv.Entry) error {
	s.applyCalls++
	s.appliedEntryCounts = append(s.appliedEntryCounts, len(entries))
	return s.base.ApplyInternalEntries(entries)
}

func (s *countingAtomicApplyStore) GetInternalEntry(cf entrykv.ColumnFamily, key []byte, version uint64) (*entrykv.Entry, error) {
	return s.base.GetInternalEntry(cf, key, version)
}

func (s *countingAtomicApplyStore) NewInternalIterator(opt *entrykv.Options) entrykv.Iterator {
	return s.base.NewInternalIterator(opt)
}

func perasFenceTableForApplyTest(t *testing.T, mount model.MountIdentity) *runtimeperas.ActiveAuthorities {
	t.Helper()
	table := runtimeperas.NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{{
		GrantID:         "grant-apply-test",
		EpochID:         1,
		HolderID:        "holder-a",
		Scope:           rootproto.VisibleAuthorityScope{MountID: string(mount.MountID), MountKeyID: uint64(mount.MountKeyID)},
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
	}}))
	return table
}

type perasFenceForApplyTest struct {
	authorities *runtimeperas.ActiveAuthorities
}

func (f perasFenceForApplyTest) FenceKey(key []byte, now time.Time) (WriteFenceDecision, error) {
	grant, ok, err := f.authorities.FencesKey(key, now)
	if err != nil || !ok {
		return WriteFenceDecision{}, err
	}
	return WriteFenceDecision{Fenced: true, Reason: grant.GrantID}, nil
}

func TestApplyMVCCMaintenanceRejectsMalformedBatch(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
			Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
				Tombstones: []*kvrpcpb.InternalEntryTombstone{
					{ColumnFamily: kvrpcpb.InternalEntryTombstone_ColumnFamily(99), Key: []byte("bad-cf"), Version: 1},
					{ColumnFamily: kvrpcpb.InternalEntryTombstone_DEFAULT, Key: []byte("must-not-apply"), Version: 1},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	keyErr := resp.GetResponses()[0].GetMvccMaintenance().GetError()
	require.NotNil(t, keyErr)
	require.Contains(t, keyErr.GetAbort(), "column family")
	_, err = db.GetInternalEntry(entrykv.CFDefault, []byte("must-not-apply"), 1)
	require.Error(t, err)
}

func TestApplyMVCCMaintenancePropagatesStoreBatchError(t *testing.T) {
	storeErr := errors.New("store batch failed")
	store := &failingMaintenanceStore{err: storeErr}
	resp, err := Apply(store, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
			Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
				Tombstones: []*kvrpcpb.InternalEntryTombstone{
					{ColumnFamily: kvrpcpb.InternalEntryTombstone_DEFAULT, Key: []byte("a"), Version: 1},
					{ColumnFamily: kvrpcpb.InternalEntryTombstone_WRITE, Key: []byte("b"), Version: 2},
				},
			}},
		}},
	})
	require.ErrorIs(t, err, storeErr)
	require.Nil(t, resp)
	require.Equal(t, 1, store.calls)
	require.Equal(t, 2, store.entries)
}

type failingMaintenanceStore struct {
	err     error
	calls   int
	entries int
}

func (s *failingMaintenanceStore) ApplyInternalEntries(entries []*entrykv.Entry) error {
	s.calls++
	s.entries += len(entries)
	return s.err
}

func (s *failingMaintenanceStore) GetInternalEntry(entrykv.ColumnFamily, []byte, uint64) (*entrykv.Entry, error) {
	return nil, errors.New("not implemented")
}

func (s *failingMaintenanceStore) NewInternalIterator(*entrykv.Options) entrykv.Iterator {
	return nil
}

func TestHandleScanShortValueCarriesExpiresAt(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("short-ttl")
	startTs := uint64(11)
	commitTs := uint64(22)
	expiresAt := ^uint64(0)
	write := mvcc.EncodeWrite(mvcc.Write{
		Kind:       kvrpcpb.Mutation_Put,
		StartTs:    startTs,
		ShortValue: []byte("short-v"),
		ExpiresAt:  expiresAt,
	})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, commitTs, write, 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      30,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.GetKvs(), 1)
	require.Equal(t, key, resp.GetKvs()[0].GetKey())
	require.Equal(t, []byte("short-v"), resp.GetKvs()[0].GetValue())
	require.Equal(t, expiresAt, resp.GetKvs()[0].GetExpiresAt())
}

func TestHandleScanCommittedLockDoesNotHideVisiblePut(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("scan-lock-visible-put")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value1"), 0, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 20, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Put,
		StartTs: 10,
	}), 0, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 30, nil, entrykv.BitDelete, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 40, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Lock,
		StartTs: 30,
	}), 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      50,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	require.Len(t, resp.GetKvs(), 1)
	require.Equal(t, key, resp.GetKvs()[0].GetKey())
	require.Equal(t, []byte("value1"), resp.GetKvs()[0].GetValue())
}

func TestHandleScanCommittedLockDoesNotCreateVisibleKey(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("scan-lock-only")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, nil, entrykv.BitDelete, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 20, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Lock,
		StartTs: 10,
	}), 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      30,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	require.Empty(t, resp.GetKvs())
}

func TestHandleScanRollbackMarkerDoesNotHideVisiblePut(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("scan-rollback-visible-put")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value1"), 0, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 20, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Put,
		StartTs: 10,
	}), 0, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 30, nil, entrykv.BitDelete, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 30, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Rollback,
		StartTs: 30,
	}), 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      40,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	require.Len(t, resp.GetKvs(), 1)
	require.Equal(t, key, resp.GetKvs()[0].GetKey())
	require.Equal(t, []byte("value1"), resp.GetKvs()[0].GetValue())
}

func TestHandleScanSkipsExpiredShortValue(t *testing.T) {
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("short-expired")
	write := mvcc.EncodeWrite(mvcc.Write{
		Kind:       kvrpcpb.Mutation_Put,
		StartTs:    11,
		ShortValue: []byte("short-v"),
		ExpiresAt:  1, // definitely expired
	})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 22, write, 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      30,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.GetKvs())
}
