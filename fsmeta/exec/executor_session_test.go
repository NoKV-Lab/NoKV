// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestExecutorOpenWriteSessionVisibleCommitBypassesRaftCommit(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	committer := &fakeVisibleCommitter{}
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithVisibleAuthorityAdmitter(&fakeVisibleAdmitter{owned: true}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	record, err := executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)

	require.Equal(t, model.SessionRecord{Session: "writer-1", Inode: 22, ExpiresUnixNs: 200}, record)
	require.Equal(t, 1, committer.calls)
	require.Len(t, committer.deltas, 1)
	require.Len(t, committer.deltas[0].WriteEffects, 2)
	for _, effect := range committer.deltas[0].WriteEffects {
		require.Equal(t, compile.EffectPut, effect.Kind)
		require.NotEmpty(t, effect.Key)
		require.NotEmpty(t, effect.Value)
		decoded, err := layout.DecodeSessionValue(effect.Value)
		require.NoError(t, err)
		require.Equal(t, record, decoded)
	}
	require.Empty(t, runner.mutations, "session visible commit must bypass the current Raft commit")

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 1)
	requireVisibleCommitStatUint(t, stats, "success_total", 1)
	requireVisibleCommitStatUint(t, stats, "skip_non_concrete_total", 0)
}

func TestExecutorOpenWriteSessionVisibleUsesCreateSessionOwnerFact(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestVisibleCommitter(t, runner)
	inode := testInodeForParentBucket(t, model.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{inode}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	created, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	runner.getCalls = 0

	opened, err := executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   created.Inode.Inode,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)

	require.Equal(t, model.SessionRecord{Session: "writer-1", Inode: created.Inode.Inode, ExpiresUnixNs: 200}, opened)
	require.Equal(t, 0, runner.getCalls, "create facts prove both the inode owner key and the per-inode session namespace are absent")
	require.Empty(t, runner.mutations)
	require.Equal(t, uint64(2), committer.Stats()["commit_total"])
}

func TestExecutorWriteSessionLifecycleVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	opened, err := executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(200), opened.ExpiresUnixNs)

	heartbeat, err := executor.HeartbeatWriteSession(context.Background(), model.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     200 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(300), heartbeat.ExpiresUnixNs)

	sessionKey, err := layout.EncodeSessionKey(testMountIdentity, 22, "writer-1")
	require.NoError(t, err)
	ownerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, 22)
	require.NoError(t, err)
	owner, ok, err := executor.readSessionByKey(context.Background(), testMountIdentity, ownerKey, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, heartbeat, owner)

	err = executor.CloseWriteSession(context.Background(), model.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
	})
	require.NoError(t, err)

	_, deleted, ok := committer.GetVisibleOverlay(sessionKey)
	require.True(t, ok)
	require.True(t, deleted)
	_, deleted, ok = committer.GetVisibleOverlay(ownerKey)
	require.True(t, ok)
	require.True(t, deleted)
	require.Empty(t, runner.mutations, "Visible session lifecycle should stay entirely inside visible overlay")

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 3)
	requireVisibleCommitStatUint(t, stats, "success_total", 3)
}

func TestExecutorWriteSessionLifecycle(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	now := time.Unix(0, 100)
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return now }))
	require.NoError(t, err)

	opened, err := executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, model.SessionRecord{Session: "writer-1", Inode: 22, ExpiresUnixNs: 200}, opened)

	sessionKey, err := layout.EncodeSessionKey(testMountIdentity, 22, "writer-1")
	require.NoError(t, err)
	ownerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, 22)
	require.NoError(t, err)
	require.Contains(t, runner.data, string(sessionKey))
	require.Contains(t, runner.data, string(ownerKey))

	_, err = executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-2",
		TTL:     150 * time.Nanosecond,
	})
	require.ErrorIs(t, err, model.ErrExists)

	heartbeat, err := executor.HeartbeatWriteSession(context.Background(), model.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     200 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(300), heartbeat.ExpiresUnixNs)
	stored, ok, err := executor.readSessionByKey(context.Background(), testMountIdentity, ownerKey, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(300), stored.ExpiresUnixNs)

	err = executor.CloseWriteSession(context.Background(), model.CloseWriteSessionRequest{Mount: "vol", Inode: 22, Session: "writer-1"})
	require.NoError(t, err)
	require.NotContains(t, runner.data, string(sessionKey))
	require.NotContains(t, runner.data, string(ownerKey))
}

func TestExecutorWriteSessionLifecycleUsesAtomicMutateWithValuePredicates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedInode(t, runner.fakeRunner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	now := time.Unix(0, 100)
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return now }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	_, err = executor.HeartbeatWriteSession(context.Background(), model.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     200 * time.Nanosecond,
	})
	require.NoError(t, err)
	err = executor.CloseWriteSession(context.Background(), model.CloseWriteSessionRequest{Mount: "vol", Inode: 22, Session: "writer-1"})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 3)
	require.Empty(t, base.mutations)
	stats := executor.Stats()
	requireAtomicStatUint(t, stats, model.OperationOpenWriteSession, "success_total", 1)
	requireAtomicStatUint(t, stats, model.OperationHeartbeatSession, "success_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCloseSession, "success_total", 1)
	require.Equal(t, backend.PredicateValueEquals, runner.atomicCalls[1].predicates[0].Kind)
	require.Equal(t, backend.PredicateValueEquals, runner.atomicCalls[2].predicates[0].Kind)
}

func TestExecutorOpenWriteSessionUsesAtomicMutateForStaleSessionCleanup(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedInode(t, runner.fakeRunner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	oldRecord := model.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	oldValue, err := layout.EncodeSessionValue(oldRecord)
	require.NoError(t, err)
	oldSessionKey, err := layout.EncodeSessionKey(testMountIdentity, 22, "writer-old")
	require.NoError(t, err)
	ownerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, 22)
	require.NoError(t, err)
	runner.data[string(oldSessionKey)] = oldValue
	runner.data[string(ownerKey)] = oldValue
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-new",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	require.NotContains(t, runner.data, string(oldSessionKey))
	requireAtomicStatUint(t, executor.Stats(), model.OperationOpenWriteSession, "success_total", 1)
	require.Equal(t, backend.PredicateValueEquals, runner.atomicCalls[0].predicates[2].Kind)
}

func TestExecutorWriteSessionRejectsNonPositiveTTL(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
	})
	require.ErrorIs(t, err, model.ErrInvalidRequest)
	require.Empty(t, runner.mutations)

	seedSession(t, runner, "vol", model.SessionRecord{Session: "writer-live", Inode: 22, ExpiresUnixNs: 500})
	_, err = executor.HeartbeatWriteSession(context.Background(), model.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-live",
		TTL:     -time.Nanosecond,
	})
	require.ErrorIs(t, err, model.ErrInvalidRequest)
}

func TestExecutorOpenWriteSessionComputesExpiryInsideRetryAttempt(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	sessionKey, err := layout.EncodeSessionKey(testMountIdentity, 22, "writer-1")
	require.NoError(t, err)
	runner.mutateErrs = []error{
		nokverrors.NewTxnKeyError(&kvrpcpb.KeyError{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				Key:         sessionKey,
				CommitTs:    2,
				MinCommitTs: 4,
			},
		}),
		nil,
	}
	clockCalls := 0
	executor, err := newTestExecutor(runner, WithClock(func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return time.Unix(0, 100)
		}
		return time.Unix(0, 500)
	}))
	require.NoError(t, err)

	opened, err := executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(600), opened.ExpiresUnixNs)
	stored, ok, err := executor.readSessionByKey(context.Background(), testMountIdentity, sessionKey, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(600), stored.ExpiresUnixNs)
}

func TestExecutorOpenWriteSessionReclaimsExpiredOwner(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	oldRecord := model.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	oldValue, err := layout.EncodeSessionValue(oldRecord)
	require.NoError(t, err)
	oldSessionKey, err := layout.EncodeSessionKey(testMountIdentity, 22, "writer-old")
	require.NoError(t, err)
	ownerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, 22)
	require.NoError(t, err)
	runner.data[string(oldSessionKey)] = oldValue
	runner.data[string(ownerKey)] = oldValue

	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)
	_, err = executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-new",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.NotContains(t, runner.data, string(oldSessionKey))
	newSessionKey, err := layout.EncodeSessionKey(testMountIdentity, 22, "writer-new")
	require.NoError(t, err)
	require.Contains(t, runner.data, string(newSessionKey))
	require.Contains(t, runner.data, string(ownerKey))
}

func TestExecutorOpenWriteSessionDoesNotDeleteReusedLiveSession(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 23, Type: model.InodeTypeFile, LinkCount: 1})
	live := model.SessionRecord{Session: "writer-reused", Inode: 23, ExpiresUnixNs: 500}
	seedSession(t, runner, "vol", live)
	expired := model.SessionRecord{Session: "writer-reused", Inode: 22, ExpiresUnixNs: 50}
	expiredValue, err := layout.EncodeSessionValue(expired)
	require.NoError(t, err)
	expiredOwnerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, expired.Inode)
	require.NoError(t, err)
	runner.data[string(expiredOwnerKey)] = expiredValue
	liveSessionKey, err := layout.EncodeSessionKey(testMountIdentity, live.Inode, live.Session)
	require.NoError(t, err)
	liveOwnerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, live.Inode)
	require.NoError(t, err)
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-new",
		TTL:     100 * time.Nanosecond,
	})

	require.NoError(t, err)
	require.Contains(t, runner.data, string(liveSessionKey))
	require.Contains(t, runner.data, string(liveOwnerKey))
	owner, ok, err := executor.readSessionByKey(context.Background(), testMountIdentity, expiredOwnerKey, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, model.SessionID("writer-new"), owner.Session)
}

func TestExecutorOpenWriteSessionRejectsDirectory(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeDirectory, LinkCount: 1})
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.ErrorIs(t, err, model.ErrInvalidRequest)
	require.Empty(t, runner.mutations)
}

func TestExecutorExpireWriteSessionsDeletesBothIndexes(t *testing.T) {
	runner := newFakeRunner()
	expired := model.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	live := model.SessionRecord{Session: "writer-live", Inode: 23, ExpiresUnixNs: 500}
	seedSession(t, runner, "vol", expired)
	seedSession(t, runner, "vol", live)
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), model.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, model.ExpireWriteSessionsResult{Expired: 1}, result)

	expiredSessionKey, err := layout.EncodeSessionKey(testMountIdentity, expired.Inode, expired.Session)
	require.NoError(t, err)
	expiredOwnerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, expired.Inode)
	require.NoError(t, err)
	liveSessionKey, err := layout.EncodeSessionKey(testMountIdentity, live.Inode, live.Session)
	require.NoError(t, err)
	liveOwnerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, live.Inode)
	require.NoError(t, err)
	require.NotContains(t, runner.data, string(expiredSessionKey))
	require.NotContains(t, runner.data, string(expiredOwnerKey))
	require.Contains(t, runner.data, string(liveSessionKey))
	require.Contains(t, runner.data, string(liveOwnerKey))
}

func TestExecutorExpireWriteSessionsCountsSessionPerInode(t *testing.T) {
	runner := newFakeRunner()
	first := model.SessionRecord{Session: "writer-reused", Inode: 22, ExpiresUnixNs: 50}
	second := model.SessionRecord{Session: "writer-reused", Inode: 23, ExpiresUnixNs: 50}
	seedSession(t, runner, "vol", first)
	seedSession(t, runner, "vol", second)
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), model.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, model.ExpireWriteSessionsResult{Expired: 2}, result)
}

func TestExecutorExpireWriteSessionsDoesNotDeleteReusedLiveSession(t *testing.T) {
	runner := newFakeRunner()
	expired := model.SessionRecord{Session: "writer-reused", Inode: 22, ExpiresUnixNs: 50}
	live := model.SessionRecord{Session: "writer-reused", Inode: 23, ExpiresUnixNs: 500}
	expiredValue, err := layout.EncodeSessionValue(expired)
	require.NoError(t, err)
	liveValue, err := layout.EncodeSessionValue(live)
	require.NoError(t, err)
	sessionKey, err := layout.EncodeSessionKey(testMountIdentity, live.Inode, live.Session)
	require.NoError(t, err)
	expiredOwnerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, expired.Inode)
	require.NoError(t, err)
	liveOwnerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, live.Inode)
	require.NoError(t, err)
	runner.data[string(expiredOwnerKey)] = expiredValue
	runner.data[string(sessionKey)] = liveValue
	runner.data[string(liveOwnerKey)] = liveValue
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), model.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, model.ExpireWriteSessionsResult{}, result)
	require.NotContains(t, runner.data, string(expiredOwnerKey))
	require.Contains(t, runner.data, string(sessionKey))
	require.Contains(t, runner.data, string(liveOwnerKey))
}

func TestExecutorExpireWriteSessionsFlushesVisibleAuthority(t *testing.T) {
	runner := newFakeRunner()
	expired := model.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	seedSession(t, runner, "vol", expired)
	flusher := &fakeVisibleAuthorityFlusher{}
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithVisibleCommitter(flusher),
	)
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), model.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, model.ExpireWriteSessionsResult{Expired: 1}, result)
	require.Equal(t, 1, flusher.flushCalls)
	require.Len(t, flusher.flushScopes, 1)
	require.Equal(t, model.MountID("vol"), flusher.flushScopes[0].Mount)
}

func TestExecutorExpireWriteSessionsUsesVisibleCommitDelete(t *testing.T) {
	runner := newFakeRunner()
	expired := model.SessionRecord{Session: "writer-visible-old", Inode: 22, ExpiresUnixNs: 50}
	seedSession(t, runner, "vol", expired)
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), model.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, model.ExpireWriteSessionsResult{Expired: 1}, result)
	require.Empty(t, runner.mutations)

	sessionKey, err := layout.EncodeSessionKey(testMountIdentity, expired.Inode, expired.Session)
	require.NoError(t, err)
	_, deleted, ok := committer.GetVisibleOverlay(sessionKey)
	require.True(t, ok)
	require.True(t, deleted)
	ownerKey, err := layout.EncodeInodeSessionKey(testMountIdentity, expired.Inode)
	require.NoError(t, err)
	_, deleted, ok = committer.GetVisibleOverlay(ownerKey)
	require.True(t, ok)
	require.True(t, deleted)
}

func BenchmarkExecutorOpenWriteSessionDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorOpenWriteSession(b, runner, executor)
}

func BenchmarkExecutorOpenWriteSessionVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(noopVisibleCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorOpenWriteSession(b, runner, executor)
}
