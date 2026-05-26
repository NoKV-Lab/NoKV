// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"context"
	"net"
	"testing"

	perasraftstore "github.com/feichai0017/NoKV/experimental/peras/adapters/raftstore"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type remoteSegmentWitnessStub struct {
	segment    fsperas.SegmentWitnessRecord
	segments   []fsperas.SegmentWitnessRecord
	scope      compile.AuthorityScope
	snapshot   fsperas.WitnessSnapshot
	probeCalls int
}

func (s *remoteSegmentWitnessStub) AppendSegments(_ context.Context, scope compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	s.scope = scope
	s.segments = append(s.segments, records...)
	if len(records) > 0 {
		s.segment = records[len(records)-1]
	}
	return nil
}

func (s *remoteSegmentWitnessStub) Probe(_ context.Context, _ uint64) (fsperas.WitnessSnapshot, error) {
	s.probeCalls++
	if len(s.snapshot.Segments) > 0 {
		return s.snapshot, nil
	}
	return fsperas.WitnessSnapshot{Segments: append([]fsperas.SegmentWitnessRecord(nil), s.segments...)}, nil
}

func (s *remoteSegmentWitnessStub) ProbeSegment(_ context.Context, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	snapshot, err := s.Probe(context.Background(), ref.EpochID)
	if err != nil {
		return fsperas.SegmentWitnessRecord{}, false, err
	}
	for _, record := range snapshot.Segments {
		if record.EpochID == ref.EpochID && record.SegmentRoot == ref.SegmentRoot && record.SegmentPayloadDigest == ref.SegmentPayloadDigest {
			return record, true, nil
		}
	}
	return fsperas.SegmentWitnessRecord{}, false, nil
}

func TestRemoteSegmentWitnessSingleRecordBatchRoundTrip(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      model.MountID("m1"),
		MountKeyID: 8,
		Buckets:    []layout.AffinityBucket{2},
		Parents:    []model.InodeID{100},
		Inodes:     []model.InodeID{200},
	}
	record := remotePerasSegmentRecord()

	stub := &remoteSegmentWitnessStub{}
	conn, closeServer := startSegmentWitnessServer(t, stub)
	defer closeServer()
	remote, err := newRemoteSegmentWitness("store-1", kvrpcpb.NewSegmentWitnessClient(conn))
	require.NoError(t, err)

	require.NoError(t, remote.AppendSegments(context.Background(), scope, []fsperas.SegmentWitnessRecord{record}))
	require.Equal(t, scope, stub.scope)
	require.Equal(t, record, stub.segment)

	snapshot, err := remote.Probe(context.Background(), record.EpochID)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{record}, snapshot.Segments)
}

func TestRemoteSegmentWitnessBatchRoundTrip(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      model.MountID("m1"),
		MountKeyID: 8,
		Buckets:    []layout.AffinityBucket{2},
		Parents:    []model.InodeID{100},
		Inodes:     []model.InodeID{200},
	}
	first := remotePerasSegmentRecordWithRoot(5)
	second := remotePerasSegmentRecordWithRoot(7)
	stub := &remoteSegmentWitnessStub{}
	conn, closeServer := startSegmentWitnessServer(t, stub)
	defer closeServer()
	remote, err := newRemoteSegmentWitness("store-1", kvrpcpb.NewSegmentWitnessClient(conn))
	require.NoError(t, err)

	require.NoError(t, remote.AppendSegments(context.Background(), scope, []fsperas.SegmentWitnessRecord{first, second}))
	require.Equal(t, scope, stub.scope)
	require.Equal(t, []fsperas.SegmentWitnessRecord{first, second}, stub.segments)

	snapshot, err := remote.Probe(context.Background(), first.EpochID)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{first, second}, snapshot.Segments)
}

func TestRemoteProbeSegmentWitnessSegment(t *testing.T) {
	first := remotePerasSegmentRecordWithRoot(5)
	second := remotePerasSegmentRecordWithRoot(7)
	stub := &remoteSegmentWitnessStub{
		snapshot: fsperas.WitnessSnapshot{Segments: []fsperas.SegmentWitnessRecord{first, second}},
	}
	conn, closeServer := startSegmentWitnessServer(t, stub)
	defer closeServer()
	remote, err := newRemoteSegmentWitness("store-1", kvrpcpb.NewSegmentWitnessClient(conn))
	require.NoError(t, err)

	record, found, err := remote.ProbeSegment(context.Background(), fsperas.WitnessSegmentRef{
		EpochID:              second.EpochID,
		SegmentRoot:          second.SegmentRoot,
		SegmentPayloadDigest: second.SegmentPayloadDigest,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, second, record)
}

func TestRemoteProbeSegmentWitnessReadsPages(t *testing.T) {
	records := make([]fsperas.SegmentWitnessRecord, 0, 40)
	for i := range 40 {
		records = append(records, remotePerasSegmentRecordWithRoot(byte(i+1)))
	}
	stub := &remoteSegmentWitnessStub{
		snapshot: fsperas.WitnessSnapshot{Segments: records},
	}
	conn, closeServer := startSegmentWitnessServer(t, stub)
	defer closeServer()
	remote, err := newRemoteSegmentWitness("store-1", kvrpcpb.NewSegmentWitnessClient(conn))
	require.NoError(t, err)

	snapshot, err := remote.Probe(context.Background(), records[0].EpochID)
	require.NoError(t, err)
	require.Len(t, snapshot.Segments, 40)
	require.Greater(t, stub.probeCalls, 1)
}

func BenchmarkRemoteSegmentWitnessAppendSegmentsSingleRecord(b *testing.B) {
	scope := compile.AuthorityScope{
		Mount:      model.MountID("m1"),
		MountKeyID: 8,
		Buckets:    []layout.AffinityBucket{2},
		Parents:    []model.InodeID{100},
		Inodes:     []model.InodeID{200},
	}
	record := remotePerasSegmentRecord()
	stub := &remoteSegmentWitnessStub{}
	conn, closeServer := startSegmentWitnessServer(b, stub)
	defer closeServer()
	remote, err := newRemoteSegmentWitness("store-1", kvrpcpb.NewSegmentWitnessClient(conn))
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, remote.AppendSegments(context.Background(), scope, []fsperas.SegmentWitnessRecord{record}))
	}
}

func TestRemoteSegmentWitnessRequiresClient(t *testing.T) {
	_, err := newRemoteSegmentWitness("store-1", nil)
	require.ErrorIs(t, err, runtimeperas.ErrRuntimeInvalid)
}

func startSegmentWitnessServer(tb testing.TB, witness perasraftstore.Witness) (*grpc.ClientConn, func()) {
	tb.Helper()
	srv := grpc.NewServer()
	kvrpcpb.RegisterSegmentWitnessServer(srv, perasraftstore.NewWitnessService(witness))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	go func() {
		_ = srv.Serve(lis)
	}()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(tb, err)
	return conn, func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}
}

func remotePerasSegmentRecord() fsperas.SegmentWitnessRecord {
	return remotePerasSegmentRecordWithRoot(5)
}

func remotePerasSegmentRecordWithRoot(rootByte byte) fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = rootByte
	var digest [32]byte
	digest[0] = rootByte + 1
	return fsperas.SegmentWitnessRecord{
		EpochID:              5,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   4096,
		SegmentPointer:       "inline",
		OperationCount:       64,
		EntryCount:           128,
		TimestampUnixNano:    100,
		HolderID:             "holder",
	}
}
