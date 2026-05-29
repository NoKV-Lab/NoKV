// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	perasfsmeta "github.com/feichai0017/NoKV/experimental/peras/adapters/fsmeta"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	"github.com/feichai0017/NoKV/fsmeta/cache/slab/dirpage"
	"github.com/feichai0017/NoKV/fsmeta/cache/slab/negativecache"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/storage/wal"
	"github.com/stretchr/testify/require"
)

func TestPerasVisibleReadPathBypassesPersistentCachesOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dirPages, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = dirPages.Close() }()
	negatives := negativecache.New(negativecache.Config{GroupKeyFn: func(k []byte) []byte { return k }})
	visibleWAL, err := wal.Open(wal.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = visibleWAL.Close() }()
	visibleLog, err := runtimeperas.NewWALVisibleLog(visibleWAL, wal.DurabilityFlushed)
	require.NoError(t, err)
	perasRuntime, err := runtimeperas.NewRuntime(runtimeperas.Config{
		Authority:         integrationVisibleGrantProvider{},
		Witnesses:         integrationSegmentWitnesses(3),
		VisibleLog:        visibleLog,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer perasRuntime.Close()

	runtime := openRealClusterRuntimeWithOptions(
		t,
		ctx,
		fsmetaexec.WithVisibleAuthorityAdmitter(integrationPerasAdmitter{}),
		fsmetaexec.WithVisibleCommitter(perasfsmeta.NewExecutorCommitter(perasRuntime)),
		fsmetaexec.WithNegativeCache(negatives),
		fsmetaexec.WithDirPageCache(dirPages),
	)
	executor := runtime.executor

	created, err := executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "visible",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096, Mode: 0o644},
	})
	require.NoError(t, err)

	key, err := layout.EncodeDentryKey(runtime.mountIdentity, model.RootInode, "visible")
	require.NoError(t, err)
	negatives.Remember(key)
	lookedUp, err := executor.Lookup(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "visible",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry, lookedUp)
	require.False(t, negatives.Has(key), "visible overlay hit must evict stale negative memo")

	session, err := executor.OpenWriteSession(ctx, model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   created.Inode.Inode,
		Session: "visible-writer",
		TTL:     time.Minute,
	})
	require.NoError(t, err)
	_, err = executor.HeartbeatWriteSession(ctx, model.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   created.Inode.Inode,
		Session: session.Session,
		TTL:     time.Minute,
	})
	require.NoError(t, err)
	require.NoError(t, executor.CloseWriteSession(ctx, model.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   created.Inode.Inode,
		Session: session.Session,
	}))

	first, err := executor.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{{Dentry: created.Dentry, Inode: created.Inode}}, first)
	require.Equal(t, uint64(0), dirPages.Stats().StoreOK)

	second, err := executor.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, first, second)
	stats := dirPages.Stats()
	require.Equal(t, uint64(0), stats.Hits)
	require.Equal(t, uint64(0), stats.StoreOK)
}

type integrationPerasAdmitter struct{}

func (integrationPerasAdmitter) AcquireVisibleAuthority(context.Context, compile.AuthorityScope) (bool, error) {
	return true, nil
}

type integrationVisibleGrantProvider struct{}

func (integrationVisibleGrantProvider) HolderID() string {
	return "integration-holder"
}

func (integrationVisibleGrantProvider) Acquire(context.Context, compile.AuthorityScope) (rootproto.VisibleAuthorityGrant, bool, error) {
	return rootproto.VisibleAuthorityGrant{
		GrantID:          "integration-grant",
		EpochID:          1,
		HolderID:         "integration-holder",
		ExpiresUnixNano:  time.Now().Add(time.Hour).UnixNano(),
		RootClusterEpoch: 1,
		IssuedRootToken: rootproto.AuthorityRootToken{
			Term:     1,
			Index:    1,
			Revision: 1,
		},
		Scope: rootproto.VisibleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 1,
		},
	}, true, nil
}

type integrationSegmentWitness struct {
	id      string
	mu      sync.Mutex
	records []fsperas.SegmentWitnessRecord
}

func integrationSegmentWitnesses(n int) []fsperas.WitnessReplica {
	out := make([]fsperas.WitnessReplica, 0, n)
	for i := range n {
		out = append(out, &integrationSegmentWitness{id: fmt.Sprintf("integration-witness-%d", i)})
	}
	return out
}

func (w *integrationSegmentWitness) ID() string {
	return w.id
}

func (w *integrationSegmentWitness) AppendSegments(_ context.Context, _ compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = append(w.records, records...)
	return nil
}

func (w *integrationSegmentWitness) Probe(_ context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out fsperas.WitnessSnapshot
	for _, record := range w.records {
		if record.EpochID == epochID {
			out.Segments = append(out.Segments, record)
		}
	}
	return out, nil
}
