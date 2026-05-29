// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/cache/slab/dirpage"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

type fakeRunner struct {
	nextTS              uint64
	data                map[string][]byte
	mutations           [][]*backend.Mutation
	getCalls            int
	scanVersions        []uint64
	batchVersions       []uint64
	scanErrs            []error
	batchErrs           []error
	timestampErrs       []error
	beforeBatchGet      func()
	mutateErr           error
	mutateErrs          []error
	actualCommitVersion uint64
}

type atomicMutateCall struct {
	primary       []byte
	predicates    []*backend.Predicate
	mutations     []*backend.Mutation
	startVersion  uint64
	commitVersion uint64
}

type fakeAtomicRunner struct {
	*fakeRunner
	handled     bool
	err         error
	atomicCalls []atomicMutateCall
}

type fakeSpeculativeAtomicRunner struct {
	*fakeRunner
	atomicCalls []atomicMutateCall
}

var testMountIdentity = model.MountIdentity{MountID: "vol", MountKeyID: 1}

func testMountIdentityFor(mount model.MountID) model.MountIdentity {
	if mount == testMountIdentity.MountID {
		return testMountIdentity
	}
	return model.MountIdentity{MountID: mount, MountKeyID: 2}
}

func testMountAdmission() MountAdmission {
	return MountAdmission{
		MountID:       testMountIdentity.MountID,
		MountKeyID:    testMountIdentity.MountKeyID,
		RootInode:     model.RootInode,
		SchemaVersion: 1,
	}
}

func newTestExecutor(runner backend.Store, opts ...Option) (*Executor, error) {
	defaults := []Option{WithMountResolver(&fakeMountResolver{records: map[model.MountID]MountAdmission{
		testMountIdentity.MountID: testMountAdmission(),
	}})}
	return New(runner, append(defaults, opts...)...)
}

func requireStatUint(t *testing.T, stats map[string]any, key string, want uint64) {
	t.Helper()
	raw, ok := stats[key]
	require.Truef(t, ok, "missing stat %s", key)
	got, ok := raw.(uint64)
	require.Truef(t, ok, "stat %s has type %T", key, raw)
	require.Equal(t, want, got)
}

func requireAtomicStatUint(t *testing.T, stats map[string]any, kind model.OperationKind, key string, want uint64) {
	t.Helper()
	raw, ok := stats["atomic_one_phase"]
	require.True(t, ok, "missing atomic_one_phase stats")
	byOp, ok := raw.(map[string]any)
	require.Truef(t, ok, "atomic_one_phase has type %T", raw)
	rawOp, ok := byOp[string(kind)]
	require.Truef(t, ok, "missing atomic_one_phase stats for %s", kind)
	opStats, ok := rawOp.(map[string]uint64)
	require.Truef(t, ok, "atomic_one_phase[%s] has type %T", kind, rawOp)
	require.Equal(t, want, opStats[key])
}

func requireVisibleStatUint(t *testing.T, stats map[string]any, key string, want uint64) {
	t.Helper()
	raw, ok := stats["visible_admission"]
	require.True(t, ok, "missing visible_admission stats")
	visibleStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "visible_admission has type %T", raw)
	got, ok := visibleStats[key].(uint64)
	require.Truef(t, ok, "visible_admission[%s] has type %T", key, visibleStats[key])
	require.Equal(t, want, got)
}

func requireVisibleSlowReasonStatUint(t *testing.T, stats map[string]any, reason compile.SlowReason, want uint64) {
	t.Helper()
	raw, ok := stats["visible_admission"]
	require.True(t, ok, "missing visible_admission stats")
	visibleStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "visible_admission has type %T", raw)
	rawReasons, ok := visibleStats["slow_by_reason"]
	require.True(t, ok, "missing visible slow reason stats")
	reasons, ok := rawReasons.(map[string]uint64)
	require.Truef(t, ok, "overlay slow_by_reason has type %T", rawReasons)
	require.Equal(t, want, reasons[string(reason)])
}

func requireVisibleStatBool(t *testing.T, stats map[string]any, key string, want bool) {
	t.Helper()
	raw, ok := stats["visible_admission"]
	require.True(t, ok, "missing visible_admission stats")
	visibleStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "visible_admission has type %T", raw)
	got, ok := visibleStats[key].(bool)
	require.Truef(t, ok, "visible_admission[%s] has type %T", key, visibleStats[key])
	require.Equal(t, want, got)
}

func requireVisibleCommitStatUint(t *testing.T, stats map[string]any, key string, want uint64) {
	t.Helper()
	raw, ok := stats["visible_commit"]
	require.True(t, ok, "missing visible_commit stats")
	visibleStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "visible_commit has type %T", raw)
	got, ok := visibleStats[key].(uint64)
	require.Truef(t, ok, "visible_commit[%s] has type %T", key, visibleStats[key])
	require.Equal(t, want, got)
}

func requireVisibleCommitStatBool(t *testing.T, stats map[string]any, key string, want bool) {
	t.Helper()
	raw, ok := stats["visible_commit"]
	require.True(t, ok, "missing visible_commit stats")
	visibleStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "visible_commit has type %T", raw)
	got, ok := visibleStats[key].(bool)
	require.Truef(t, ok, "visible_commit[%s] has type %T", key, visibleStats[key])
	require.Equal(t, want, got)
}

func txnLockedError(mount model.MountID, parent model.InodeID, name string) error {
	key, err := layout.EncodeDentryKey(testMountIdentityFor(mount), parent, name)
	if err != nil {
		panic(err)
	}
	return nokverrors.NewTxnKeyError(&kvrpcpb.KeyError{
		Locked: &kvrpcpb.Locked{
			PrimaryLock: key,
			Key:         key,
			LockVersion: 10,
			LockTtl:     defaultLockTTL,
		},
	})
}

type fakeMountResolver struct {
	records map[model.MountID]MountAdmission
	err     error
	calls   int
}

type fakeAuthorityResolver struct {
	same  bool
	err   error
	calls int
}

type fakeVisibleAdmitter struct {
	owned  bool
	err    error
	calls  int
	scopes []compile.AuthorityScope
}

type fakeVisibleCommitter struct {
	err             error
	beforeAdmission func()
	calls           int
	ids             []VisibleOperationID
	deltas          []compile.SemanticDelta
}

type testVersionAllocator interface {
	ReserveTimestamp(context.Context, uint64) (uint64, error)
}

type testVisibleCommitter struct {
	holder    *fsperas.Holder
	versions  testVersionAllocator
	view      *fsperas.OverlayView
	submitErr error

	mu          sync.Mutex
	commitTotal uint64
}

type fakeVisibleAuthorityFlusher struct {
	fakeVisibleCommitter
	flushCalls  int
	flushScopes []compile.AuthorityScope
}

type fakeVisibleSnapshotCapturer struct {
	fakeVisibleAuthorityFlusher
	capture         bool
	segmentRefs     []model.SnapshotEvidenceRef
	err             error
	captureVersions []uint64
	captureScopes   []compile.AuthorityScope
}

type noopVisibleCommitter struct{}

type ownedVisibleAdmitter struct{}

type scanOverlayCommitter struct {
	noopVisibleCommitter
	rows             []VisibleOverlayKV
	values           map[string]VisibleOverlayKV
	directoryPresent bool
}

var _ VisibleOverlayReader = scanOverlayCommitter{}

type fakeSubtreePublisher struct {
	starts      []subtreePublishCall
	completes   []subtreePublishCall
	err         error
	startErr    error
	completeErr error
}

type fakeQuotaResolver struct {
	err                error
	changes            [][]QuotaChange
	mutation           *backend.Mutation
	allowVisibleCommit bool
	perasChecks        [][]QuotaChange
}

type fakeInodeAllocator struct {
	next  model.InodeID
	ids   []model.InodeID
	err   error
	calls int
}

func testInodeForParentBucket(t *testing.T, parent model.InodeID, exclude ...model.InodeID) model.InodeID {
	t.Helper()
	target := layout.BucketForInodeID(parent)
	excluded := make(map[model.InodeID]struct{}, len(exclude))
	for _, id := range exclude {
		excluded[id] = struct{}{}
	}
	for id := model.InodeID(2); id < 1_000_000; id++ {
		if _, ok := excluded[id]; ok {
			continue
		}
		if layout.BucketForInodeID(id) == target {
			return id
		}
	}
	t.Fatalf("no inode found for parent bucket %d", target)
	return 0
}

func testInodeForDifferentBucket(t *testing.T, parent model.InodeID, exclude ...model.InodeID) model.InodeID {
	t.Helper()
	target := layout.BucketForInodeID(parent)
	excluded := make(map[model.InodeID]struct{}, len(exclude))
	for _, id := range exclude {
		excluded[id] = struct{}{}
	}
	for id := model.InodeID(2); id < 1_000_000; id++ {
		if _, ok := excluded[id]; ok {
			continue
		}
		if layout.BucketForInodeID(id) != target {
			return id
		}
	}
	t.Fatalf("no inode found outside parent bucket %d", target)
	return 0
}

func (a *fakeInodeAllocator) AllocateCreateInode(context.Context, model.MountIdentity, model.InodeID, string) (model.InodeID, error) {
	a.calls++
	if a.err != nil {
		return 0, a.err
	}
	if len(a.ids) > 0 {
		id := a.ids[0]
		a.ids = a.ids[1:]
		return id, nil
	}
	if a.next == 0 {
		a.next = 22
	}
	id := a.next
	a.next++
	return id, nil
}

type subtreePublishCall struct {
	mount    model.MountID
	root     model.InodeID
	frontier uint64
}

func (q *fakeQuotaResolver) ReserveQuota(_ context.Context, _ backend.Store, changes []QuotaChange, _ uint64) ([]*backend.Mutation, error) {
	q.changes = append(q.changes, append([]QuotaChange(nil), changes...))
	if q.err != nil {
		return nil, q.err
	}
	if q.mutation != nil {
		return []*backend.Mutation{cloneMutation(q.mutation)}, nil
	}
	return nil, nil
}

func (q *fakeQuotaResolver) AllowVisibleQuota(_ context.Context, changes []QuotaChange) (bool, error) {
	q.perasChecks = append(q.perasChecks, append([]QuotaChange(nil), changes...))
	if q.err != nil {
		return false, q.err
	}
	return q.allowVisibleCommit, nil
}

func (p *fakeSubtreePublisher) StartSubtreeHandoff(_ context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	if p.startErr != nil {
		return p.startErr
	}
	if p.err != nil {
		return p.err
	}
	p.starts = append(p.starts, subtreePublishCall{mount: mount, root: root, frontier: frontier})
	return nil
}

func (p *fakeSubtreePublisher) CompleteSubtreeHandoff(_ context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	if p.completeErr != nil {
		return p.completeErr
	}
	if p.err != nil {
		return p.err
	}
	p.completes = append(p.completes, subtreePublishCall{mount: mount, root: root, frontier: frontier})
	return nil
}

func (r *fakeMountResolver) ResolveMount(_ context.Context, mount model.MountID) (MountAdmission, error) {
	r.calls++
	if r.err != nil {
		return MountAdmission{}, r.err
	}
	record, ok := r.records[mount]
	if !ok {
		return MountAdmission{}, model.ErrMountNotRegistered
	}
	return record, nil
}

func (r *fakeAuthorityResolver) SameAuthority(context.Context, model.MountID, model.InodeID, model.InodeID) (bool, error) {
	r.calls++
	if r.err != nil {
		return false, r.err
	}
	return r.same, nil
}

func (a *fakeVisibleAdmitter) AcquireVisibleAuthority(_ context.Context, scope compile.AuthorityScope) (bool, error) {
	a.calls++
	a.scopes = append(a.scopes, compile.AuthorityScope{
		Mount:      scope.Mount,
		MountKeyID: scope.MountKeyID,
		Buckets:    append([]layout.AffinityBucket(nil), scope.Buckets...),
		Parents:    append([]model.InodeID(nil), scope.Parents...),
		Inodes:     append([]model.InodeID(nil), scope.Inodes...),
	})
	if a.err != nil {
		return false, a.err
	}
	return a.owned, nil
}

func (c *fakeVisibleCommitter) SubmitVisible(ctx context.Context, id VisibleOperationID, op compile.MaterializedOp, admission VisibleAdmissionFunc) (VisibleAck, error) {
	if c.beforeAdmission != nil {
		c.beforeAdmission()
	}
	admitted, err := fsperas.AdmitAndSeal(ctx, op, visibleAdmissionFuncForTest(admission), fsperas.AdmissionContext{
		ProofFrontier: proof.ProofFrontier{EpochID: 1, Sequence: id.Seq},
	})
	if err != nil {
		return VisibleAck{}, visibleErrorForTest(err)
	}
	op = admitted
	c.calls++
	c.ids = append(c.ids, id)
	c.deltas = append(c.deltas, op.Delta)
	if c.err != nil {
		return VisibleAck{}, c.err
	}
	return VisibleAck{EpochID: 1, OpID: id, HolderID: "holder-a"}, nil
}

func (c *testVisibleCommitter) SubmitVisible(ctx context.Context, id VisibleOperationID, op compile.MaterializedOp, admission VisibleAdmissionFunc) (VisibleAck, error) {
	if c == nil || c.holder == nil || c.view == nil {
		return VisibleAck{}, fsperas.ErrHolderConfigInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	admitted, err := fsperas.AdmitAndSeal(ctx, op, visibleAdmissionFuncForTest(admission), fsperas.AdmissionContext{
		ProofFrontier: proof.ProofFrontier{EpochID: c.holder.EpochID(), Sequence: id.Seq},
	})
	if err != nil {
		return VisibleAck{}, visibleErrorForTest(err)
	}
	op = admitted
	if c.submitErr != nil {
		return VisibleAck{}, c.submitErr
	}
	ack, _, err := c.holder.Submit(ctx, perasOperationIDForTest(id), op)
	if err != nil {
		return VisibleAck{}, err
	}
	if err := c.view.Add(perasOperationIDForTest(id), op); err != nil {
		return VisibleAck{}, err
	}
	c.commitTotal++
	return visibleAckForTest(ack), nil
}

func (c *testVisibleCommitter) Flush(ctx context.Context) error {
	return c.FlushDurable(ctx)
}

func (c *testVisibleCommitter) FlushDurable(ctx context.Context) error {
	if c == nil || c.holder == nil || c.versions == nil || c.view == nil {
		return fsperas.ErrHolderConfigInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	pending := c.holder.PendingIDs()
	if len(pending) == 0 {
		return nil
	}
	firstVersion, err := c.versions.ReserveTimestamp(ctx, uint64(len(pending)))
	if err != nil {
		return err
	}
	plan, _, err := c.holder.BuildPendingReplayPlan(firstVersion)
	if err != nil {
		return err
	}
	if len(plan.Operations) != len(pending) {
		return fsperas.ErrInvalidPerasSegment
	}
	if err := c.holder.MarkReplayPlanApplied(plan); err != nil {
		return err
	}
	c.view.RemovePlan(plan)
	return nil
}

func (c *testVisibleCommitter) GetVisibleOverlay(key []byte) ([]byte, bool, bool) {
	if c == nil || c.view == nil {
		return nil, false, false
	}
	return c.view.Get(key)
}

func (c *testVisibleCommitter) GetVisibleOverlayView(key []byte) ([]byte, bool, bool) {
	if c == nil || c.view == nil {
		return nil, false, false
	}
	return c.view.GetView(key)
}

func (c *testVisibleCommitter) CaptureVisibleOverlayRead() (uint64, uint64) {
	if c == nil || c.view == nil {
		return 0, 0
	}
	return c.view.Generation(), 0
}

func (c *testVisibleCommitter) GetVisibleOverlayViewAt(overlayGeneration, _ uint64, key []byte) ([]byte, bool, bool) {
	if c == nil || c.view == nil {
		return nil, false, false
	}
	return c.view.GetViewAt(overlayGeneration, key)
}

func (c *testVisibleCommitter) ScanVisibleOverlay(start []byte, limit uint32) []VisibleOverlayKV {
	if c == nil || c.view == nil {
		return nil
	}
	return visibleRowsForTest(c.view.Scan(start, limit))
}

func (c *testVisibleCommitter) ScanVisibleDirectoryAt(overlayGeneration, _ uint64, prefix, start []byte, limit uint32) []VisibleOverlayKV {
	if c == nil || c.view == nil {
		return nil
	}
	return visibleRowsForTest(c.view.ScanDirectoryAt(overlayGeneration, prefix, start, limit))
}

func (c *testVisibleCommitter) HasVisibleDirectoryOverlay(prefix []byte) bool {
	if c == nil || c.view == nil {
		return false
	}
	return c.view.HasDirectory(prefix)
}

func (c *testVisibleCommitter) KeyState(key []byte) (bool, bool) {
	if c == nil || c.view == nil {
		return false, false
	}
	return c.view.KeyState(key)
}

func (c *testVisibleCommitter) DirectoryEmpty(mount model.MountIdentity, inode model.InodeID) bool {
	if c == nil || c.view == nil {
		return false
	}
	return c.view.DirectoryEmpty(mount, inode)
}

func (c *testVisibleCommitter) DirectoryBaseEmpty(mount model.MountIdentity, inode model.InodeID) bool {
	if c == nil || c.view == nil {
		return false
	}
	return c.view.DirectoryBaseEmpty(mount, inode)
}

func (c *testVisibleCommitter) SessionNamespaceEmpty(mount model.MountIdentity, inode model.InodeID) bool {
	if c == nil || c.view == nil {
		return false
	}
	return c.view.SessionNamespaceEmpty(mount, inode)
}

func (c *testVisibleCommitter) RememberKey(key []byte, present bool) {
	if c == nil || c.view == nil {
		return
	}
	c.view.RememberKey(key, present)
}

func (c *testVisibleCommitter) RememberEmptyDirectory(mount model.MountIdentity, inode model.InodeID) {
	if c == nil || c.view == nil {
		return
	}
	c.view.RememberEmptyDirectory(mount, inode)
}

func (c *testVisibleCommitter) ForgetEmptyDirectory(mount model.MountIdentity, inode model.InodeID) {
	if c == nil || c.view == nil {
		return
	}
	c.view.ForgetEmptyDirectory(mount, inode)
}

func (c *testVisibleCommitter) RememberEmptySessionNamespace(mount model.MountIdentity, inode model.InodeID) {
	if c == nil || c.view == nil {
		return
	}
	c.view.RememberEmptySessionNamespace(mount, inode)
}

func (c *testVisibleCommitter) Stats() map[string]any {
	if c == nil {
		return map[string]any{"commit_total": uint64(0)}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]any{"commit_total": c.commitTotal}
}

func (f *fakeVisibleAuthorityFlusher) FlushAuthority(_ context.Context, scope compile.AuthorityScope) error {
	f.flushCalls++
	f.flushScopes = append(f.flushScopes, compile.AuthorityScope{
		Mount:      scope.Mount,
		MountKeyID: scope.MountKeyID,
		Buckets:    append([]layout.AffinityBucket(nil), scope.Buckets...),
		Parents:    append([]model.InodeID(nil), scope.Parents...),
		Inodes:     append([]model.InodeID(nil), scope.Inodes...),
	})
	return nil
}

func (f *fakeVisibleSnapshotCapturer) CaptureVisibleSnapshot(_ context.Context, version uint64, scope compile.AuthorityScope) (model.VisibleSnapshotCapture, bool, error) {
	f.captureVersions = append(f.captureVersions, version)
	f.captureScopes = append(f.captureScopes, cloneTestAuthorityScope(scope))
	if f.err != nil {
		return model.VisibleSnapshotCapture{}, false, f.err
	}
	return model.VisibleSnapshotCapture{Evidence: append([]model.SnapshotEvidenceRef(nil), f.segmentRefs...)}, f.capture, nil
}

func cloneTestAuthorityScope(scope compile.AuthorityScope) compile.AuthorityScope {
	out := scope
	out.Buckets = append([]layout.AffinityBucket(nil), scope.Buckets...)
	out.Parents = append([]model.InodeID(nil), scope.Parents...)
	out.Inodes = append([]model.InodeID(nil), scope.Inodes...)
	return out
}

func (noopVisibleCommitter) SubmitVisible(_ context.Context, id VisibleOperationID, _ compile.MaterializedOp, _ VisibleAdmissionFunc) (VisibleAck, error) {
	return VisibleAck{EpochID: 1, OpID: id, HolderID: "holder-a"}, nil
}

func (c scanOverlayCommitter) GetVisibleOverlay(key []byte) ([]byte, bool, bool) {
	value, deleted, ok := c.GetVisibleOverlayView(key)
	if !ok {
		return nil, false, false
	}
	return append([]byte(nil), value...), deleted, true
}

func (c scanOverlayCommitter) GetVisibleOverlayView(key []byte) ([]byte, bool, bool) {
	if c.values == nil {
		return nil, false, false
	}
	row, ok := c.values[string(key)]
	if !ok {
		return nil, false, false
	}
	return row.Value, row.Delete, true
}

func overlayValueForTest(key, value []byte) VisibleOverlayKV {
	return VisibleOverlayKV{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}
}

func overlayDeleteForTest(key []byte) VisibleOverlayKV {
	return VisibleOverlayKV{
		Key:    append([]byte(nil), key...),
		Delete: true,
	}
}

func overlayMapForTest(rows ...VisibleOverlayKV) map[string]VisibleOverlayKV {
	out := make(map[string]VisibleOverlayKV, len(rows))
	for _, row := range rows {
		out[string(row.Key)] = row
	}
	return out
}

func dentryValueForTest(t *testing.T, parent model.InodeID, name string, inode model.InodeID, typ model.InodeType) []byte {
	t.Helper()
	value, err := layout.EncodeDentryValue(model.DentryRecord{
		Parent: parent,
		Name:   name,
		Inode:  inode,
		Type:   typ,
	})
	require.NoError(t, err)
	return value
}

func inodeValueForTest(t *testing.T, record model.InodeRecord) []byte {
	t.Helper()
	value, err := layout.EncodeInodeValue(record)
	require.NoError(t, err)
	return value
}

func dentryKeyForTest(t *testing.T, mount model.MountID, parent model.InodeID, name string) []byte {
	t.Helper()
	key, err := layout.EncodeDentryKey(testMountIdentityFor(mount), parent, name)
	require.NoError(t, err)
	return key
}

func inodeKeyForTest(t *testing.T, mount model.MountID, inode model.InodeID) []byte {
	t.Helper()
	key, err := layout.EncodeInodeKey(testMountIdentityFor(mount), inode)
	require.NoError(t, err)
	return key
}

func (c scanOverlayCommitter) ScanVisibleOverlay(start []byte, limit uint32) []VisibleOverlayKV {
	out := make([]VisibleOverlayKV, 0, len(c.rows))
	for _, row := range c.rows {
		if bytes.Compare(row.Key, start) < 0 {
			continue
		}
		out = append(out, row)
		if uint32(len(out)) == limit {
			break
		}
	}
	return out
}

func (c scanOverlayCommitter) ScanVisibleDirectory(prefix, start []byte, limit uint32) []VisibleOverlayKV {
	out := make([]VisibleOverlayKV, 0, len(c.rows))
	for _, row := range c.rows {
		if !bytes.HasPrefix(row.Key, prefix) || bytes.Compare(row.Key, start) < 0 {
			continue
		}
		out = append(out, row)
		if uint32(len(out)) == limit {
			break
		}
	}
	return out
}

func (c scanOverlayCommitter) HasVisibleDirectoryOverlay(prefix []byte) bool {
	if c.directoryPresent {
		return true
	}
	for _, row := range c.rows {
		if bytes.HasPrefix(row.Key, prefix) {
			return true
		}
	}
	return false
}

func (ownedVisibleAdmitter) AcquireVisibleAuthority(context.Context, compile.AuthorityScope) (bool, error) {
	return true, nil
}

func perasOperationIDForTest(id VisibleOperationID) fsperas.OperationID {
	return fsperas.OperationID{ClientID: id.ClientID, Seq: id.Seq}
}

func visibleOperationIDForTest(id fsperas.OperationID) VisibleOperationID {
	return VisibleOperationID{ClientID: id.ClientID, Seq: id.Seq}
}

func visibleAckForTest(ack fsperas.VisibleAck) VisibleAck {
	return VisibleAck{
		EpochID:  ack.EpochID,
		OpID:     visibleOperationIDForTest(ack.OpID),
		HolderID: ack.HolderID,
	}
}

func visibleAdmissionFuncForTest(fn VisibleAdmissionFunc) fsperas.AdmissionFunc {
	if fn == nil {
		return nil
	}
	return func(ctx context.Context, op compile.MaterializedOp, admissionCtx fsperas.AdmissionContext) (fsperas.AdmissionResult, bool, error) {
		result, ok, err := fn(ctx, op, VisibleAdmissionContext{ProofFrontier: admissionCtx.ProofFrontier})
		if err != nil || !ok {
			return fsperas.AdmissionResult{}, ok, err
		}
		return fsperas.AdmissionResult{
			PredicateProofs: result.PredicateProofs,
			GuardProofs:     result.GuardProofs,
		}, true, nil
	}
}

func visibleRowsForTest(rows []fsperas.OverlayKV) []VisibleOverlayKV {
	if len(rows) == 0 {
		return nil
	}
	out := make([]VisibleOverlayKV, len(rows))
	for i, row := range rows {
		out[i] = VisibleOverlayKV{
			Key:    row.Key,
			Value:  row.Value,
			Delete: row.Delete,
		}
	}
	return out
}

func visibleErrorForTest(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fsperas.ErrAdmissionRejected):
		return errors.Join(ErrVisibleAdmissionRejected, err)
	case errors.Is(err, fsperas.ErrIneligibleOperation):
		return errors.Join(ErrVisibleIneligibleOperation, err)
	default:
		return err
	}
}

func newFakeRunner() *fakeRunner {
	runner := &fakeRunner{
		nextTS: 1,
		data:   make(map[string][]byte),
	}
	seedInodeValue(runner, testMountIdentity.MountID, model.InodeRecord{
		Inode:     model.RootInode,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	})
	return runner
}

func (r *fakeRunner) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errors.New("zero timestamp reservation")
	}
	if len(r.timestampErrs) > 0 {
		err := r.timestampErrs[0]
		r.timestampErrs = r.timestampErrs[1:]
		if err != nil {
			return 0, err
		}
	}
	first := r.nextTS
	r.nextTS += count
	return first, nil
}

func (r *fakeRunner) Get(_ context.Context, key []byte, _ uint64) ([]byte, bool, error) {
	r.getCalls++
	value, ok := r.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (r *fakeRunner) BatchGet(_ context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	r.batchVersions = append(r.batchVersions, version)
	if r.beforeBatchGet != nil {
		r.beforeBatchGet()
	}
	if len(r.batchErrs) > 0 {
		err := r.batchErrs[0]
		r.batchErrs = r.batchErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := r.data[string(key)]; ok {
			out[string(key)] = append([]byte(nil), value...)
		}
	}
	return out, nil
}

func (r *fakeRunner) Scan(_ context.Context, startKey []byte, limit uint32, version uint64) ([]backend.KV, error) {
	r.scanVersions = append(r.scanVersions, version)
	if len(r.scanErrs) > 0 {
		err := r.scanErrs[0]
		r.scanErrs = r.scanErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	keys := make([][]byte, 0, len(r.data))
	for key := range r.data {
		if bytes.Compare([]byte(key), startKey) >= 0 {
			keys = append(keys, []byte(key))
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	out := make([]backend.KV, 0, limit)
	for _, key := range keys {
		if uint32(len(out)) >= limit {
			break
		}
		out = append(out, backend.KV{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), r.data[string(key)]...),
		})
	}
	return out, nil
}

func (r *fakeRunner) Mutate(_ context.Context, primary []byte, mutations []*backend.Mutation, _, commitVersion, _ uint64) (uint64, error) {
	return r.applyMutations(primary, mutations, commitVersion, r.actualCommitVersion)
}

func (r *fakeRunner) MutateAtCommit(_ context.Context, primary []byte, mutations []*backend.Mutation, _, commitVersion, _ uint64) (uint64, error) {
	return r.applyMutations(primary, mutations, commitVersion, 0)
}

func (r *fakeRunner) applyMutations(primary []byte, mutations []*backend.Mutation, commitVersion, overrideCommitVersion uint64) (uint64, error) {
	if len(r.mutateErrs) > 0 {
		err := r.mutateErrs[0]
		r.mutateErrs = r.mutateErrs[1:]
		if err != nil {
			return 0, err
		}
	}
	if r.mutateErr != nil {
		return 0, r.mutateErr
	}
	cloned := make([]*backend.Mutation, 0, len(mutations))
	hasPrimary := len(mutations) == 0
	for _, mut := range mutations {
		if mut.AssertionNotExist {
			if _, ok := r.data[string(mut.Key)]; ok {
				return 0, model.ErrExists
			}
		}
		if bytes.Equal(mut.Key, primary) {
			hasPrimary = true
		}
		cloned = append(cloned, cloneMutation(mut))
	}
	if !hasPrimary {
		return 0, fmt.Errorf("primary key %q not present in mutations", primary)
	}
	for _, mut := range cloned {
		switch mut.Op {
		case backend.MutationPut:
			r.data[string(mut.Key)] = append([]byte(nil), mut.Value...)
		case backend.MutationDelete:
			delete(r.data, string(mut.Key))
		}
	}
	r.mutations = append(r.mutations, cloned)
	if overrideCommitVersion != 0 {
		return overrideCommitVersion, nil
	}
	return commitVersion, nil
}

func (r *fakeAtomicRunner) TryAtomicMutate(_ context.Context, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, startVersion, commitVersion uint64) (bool, error) {
	r.atomicCalls = append(r.atomicCalls, atomicMutateCall{
		primary:       cloneBytes(primary),
		predicates:    cloneAtomicPredicates(predicates),
		mutations:     cloneMutations(mutations),
		startVersion:  startVersion,
		commitVersion: commitVersion,
	})
	if r.err != nil {
		return true, r.err
	}
	if !r.handled {
		return false, nil
	}
	for _, pred := range predicates {
		if pred == nil {
			continue
		}
		_, exists := r.data[string(pred.Key)]
		switch pred.Kind {
		case backend.PredicateNotExists:
			if exists {
				return true, model.ErrExists
			}
		case backend.PredicateExists:
			if !exists {
				return true, model.ErrNotFound
			}
		case backend.PredicateValueEquals:
			if !exists || !bytes.Equal(r.data[string(pred.Key)], pred.ExpectedValue) {
				return true, model.ErrInvalidValue
			}
		default:
			return true, model.ErrInvalidRequest
		}
	}
	cloned := make([]*backend.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		if mut.AssertionNotExist {
			if _, exists := r.data[string(mut.Key)]; exists {
				return true, model.ErrExists
			}
		}
		cloned = append(cloned, cloneMutation(mut))
	}
	for _, mut := range cloned {
		switch mut.Op {
		case backend.MutationPut:
			r.data[string(mut.Key)] = append([]byte(nil), mut.Value...)
		case backend.MutationDelete:
			delete(r.data, string(mut.Key))
		}
	}
	return true, nil
}

func (r *fakeAtomicRunner) AtomicMutatePreservesReadOrder() bool {
	return true
}

func (r *fakeSpeculativeAtomicRunner) TryAtomicMutate(_ context.Context, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, startVersion, commitVersion uint64) (bool, error) {
	r.atomicCalls = append(r.atomicCalls, atomicMutateCall{
		primary:       cloneBytes(primary),
		predicates:    cloneAtomicPredicates(predicates),
		mutations:     cloneMutations(mutations),
		startVersion:  startVersion,
		commitVersion: commitVersion,
	})
	return true, nil
}

func seedDentry(t *testing.T, runner *fakeRunner, mount model.MountID, parent model.InodeID, name string, inode model.InodeID) {
	t.Helper()
	seedDentryType(t, runner, mount, parent, name, inode, model.InodeTypeFile)
}

func seedDentryType(t *testing.T, runner *fakeRunner, mount model.MountID, parent model.InodeID, name string, inode model.InodeID, typ model.InodeType) {
	t.Helper()
	key, err := layout.EncodeDentryKey(testMountIdentityFor(mount), parent, name)
	require.NoError(t, err)
	if _, exists := runner.data[string(key)]; !exists {
		incrementSeedParentChildCount(t, runner, mount, parent)
	}
	value, err := layout.EncodeDentryValue(model.DentryRecord{
		Parent: parent,
		Name:   name,
		Inode:  inode,
		Type:   typ,
	})
	require.NoError(t, err)
	runner.data[string(key)] = value
}

func seedInode(t *testing.T, runner *fakeRunner, mount model.MountID, record model.InodeRecord) {
	t.Helper()
	seedInodeValue(runner, mount, record)
}

func seedDirectory(t *testing.T, runner *fakeRunner, mount model.MountID, inode model.InodeID) {
	t.Helper()
	seedInodeValue(runner, mount, model.InodeRecord{
		Inode:     inode,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	})
}

func seedInodeValue(runner *fakeRunner, mount model.MountID, record model.InodeRecord) {
	key, err := layout.EncodeInodeKey(testMountIdentityFor(mount), record.Inode)
	if err != nil {
		panic(err)
	}
	value, err := layout.EncodeInodeValue(record)
	if err != nil {
		panic(err)
	}
	runner.data[string(key)] = value
}

func incrementSeedParentChildCount(t *testing.T, runner *fakeRunner, mount model.MountID, parent model.InodeID) {
	t.Helper()
	key, err := layout.EncodeInodeKey(testMountIdentityFor(mount), parent)
	require.NoError(t, err)
	record := model.InodeRecord{
		Inode:     parent,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	}
	if value, ok := runner.data[string(key)]; ok {
		record, err = layout.DecodeInodeValue(value)
		require.NoError(t, err)
		require.Equal(t, model.InodeTypeDirectory, record.Type)
	}
	record.ChildCount++
	value, err := layout.EncodeInodeValue(record)
	require.NoError(t, err)
	runner.data[string(key)] = value
}

func seedSession(t *testing.T, runner *fakeRunner, mount model.MountID, record model.SessionRecord) {
	t.Helper()
	value, err := layout.EncodeSessionValue(record)
	require.NoError(t, err)
	sessionKey, err := layout.EncodeSessionKey(testMountIdentityFor(mount), record.Inode, record.Session)
	require.NoError(t, err)
	ownerKey, err := layout.EncodeInodeSessionKey(testMountIdentityFor(mount), record.Inode)
	require.NoError(t, err)
	runner.data[string(sessionKey)] = value
	runner.data[string(ownerKey)] = value
}

func cloneMutation(mut *backend.Mutation) *backend.Mutation {
	if mut == nil {
		return nil
	}
	return &backend.Mutation{
		Op:                mut.Op,
		Key:               append([]byte(nil), mut.Key...),
		Value:             append([]byte(nil), mut.Value...),
		AssertionNotExist: mut.AssertionNotExist,
		ExpiresAt:         mut.ExpiresAt,
	}
}

type fakeTxnKeyError struct {
	errors []*kvrpcpb.KeyError
}

func (e fakeTxnKeyError) Error() string {
	return "fake txn key error"
}

func (e fakeTxnKeyError) KeyErrors() []*kvrpcpb.KeyError {
	return e.errors
}

func newTestVisibleCommitter(t testing.TB, versions testVersionAllocator) *testVisibleCommitter {
	t.Helper()
	holder, err := fsperas.NewHolder(fsperas.HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	require.NoError(t, err)
	return &testVisibleCommitter{
		holder:   holder,
		versions: versions,
		view:     fsperas.NewOverlayView(),
	}
}

func benchmarkExecutorCreate(b *testing.B, executor *Executor) {
	ctx := context.Background()
	var seq uint64
	b.ReportAllocs()
	for b.Loop() {
		name := "file-" + strconv.FormatUint(seq, 10)
		seq++
		if _, err := executor.Create(ctx, model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   name,
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorCheckpointStorm(b *testing.B, executor *Executor, flusher interface{ FlushDurable(context.Context) error }, files int) {
	ctx := context.Background()
	var batch uint64
	var ops uint64
	b.ReportAllocs()
	for b.Loop() {
		prefix := "ckpt-" + strconv.FormatUint(batch, 10) + "-"
		batch++
		dir, err := executor.Create(ctx, model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   prefix + "dir",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeDirectory},
		})
		if err != nil {
			b.Fatal(err)
		}
		for i := range files {
			if _, err := executor.Create(ctx, model.CreateRequest{
				Mount:  "vol",
				Parent: dir.Inode.Inode,
				Name:   prefix + strconv.Itoa(i),
				Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
			}); err != nil {
				b.Fatal(err)
			}
		}
		if flusher != nil {
			if err := flusher.FlushDurable(ctx); err != nil {
				b.Fatal(err)
			}
		}
		ops += uint64(files + 1)
	}
	if ops > 0 {
		elapsed := b.Elapsed()
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(ops), "ns/metadata_op")
		b.ReportMetric(float64(ops)/elapsed.Seconds(), "metadata_ops/s")
	}
}

func benchmarkExecutorUpdateInode(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		name := "file-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, name, inode)
		benchmarkSeedInode(b, runner, inode)
		if _, err := executor.UpdateInode(ctx, model.UpdateInodeRequest{
			Mount:            "vol",
			Parent:           7,
			Inode:            inode,
			Name:             name,
			SetSize:          true,
			Size:             uint64(i + 4096),
			SetMode:          true,
			Mode:             0o600,
			SetUpdatedUnixNs: true,
			UpdatedUnixNs:    int64(i + 1),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorRename(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		oldName := "old-" + strconv.Itoa(i)
		newName := "new-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, oldName, inode)
		if err := executor.Rename(ctx, model.RenameRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   oldName,
			ToParent:   7,
			ToName:     newName,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorOpenWriteSession(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		benchmarkSeedInode(b, runner, inode)
		if _, err := executor.OpenWriteSession(ctx, model.OpenWriteSessionRequest{
			Mount:   "vol",
			Inode:   inode,
			Session: model.SessionID("writer-" + strconv.Itoa(i)),
			TTL:     100 * time.Nanosecond,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorLink(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	benchmarkSeedDirectory(b, runner, 8)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		fromName := "file-" + strconv.Itoa(i)
		toName := "alias-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, fromName, inode)
		benchmarkSeedInode(b, runner, inode)
		if err := executor.Link(ctx, model.LinkRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   fromName,
			ToParent:   8,
			ToName:     toName,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorUnlink(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		name := "file-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, name, inode)
		benchmarkSeedInodeRecord(b, runner, model.InodeRecord{Inode: inode, Type: model.InodeTypeFile, Size: 4096, LinkCount: 2})
		if err := executor.Unlink(ctx, model.UnlinkRequest{
			Mount:  "vol",
			Parent: 7,
			Name:   name,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSeedDentry(b *testing.B, runner *fakeRunner, parent model.InodeID, name string, inode model.InodeID) {
	b.Helper()
	benchmarkIncrementSeedParentChildCount(b, runner, parent)
	key, err := layout.EncodeDentryKey(testMountIdentity, parent, name)
	if err != nil {
		b.Fatal(err)
	}
	value, err := layout.EncodeDentryValue(model.DentryRecord{
		Parent: parent,
		Name:   name,
		Inode:  inode,
		Type:   model.InodeTypeFile,
	})
	if err != nil {
		b.Fatal(err)
	}
	runner.data[string(key)] = value
}

func benchmarkSeedDirectory(b *testing.B, runner *fakeRunner, inode model.InodeID) {
	b.Helper()
	benchmarkSeedInodeRecord(b, runner, model.InodeRecord{
		Inode:     inode,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	})
}

func benchmarkIncrementSeedParentChildCount(b *testing.B, runner *fakeRunner, parent model.InodeID) {
	b.Helper()
	key, err := layout.EncodeInodeKey(testMountIdentity, parent)
	if err != nil {
		b.Fatal(err)
	}
	record := model.InodeRecord{
		Inode:     parent,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	}
	if value, ok := runner.data[string(key)]; ok {
		record, err = layout.DecodeInodeValue(value)
		if err != nil {
			b.Fatal(err)
		}
		if record.Type != model.InodeTypeDirectory {
			b.Fatalf("seed parent %d is %s", parent, record.Type)
		}
	}
	record.ChildCount++
	value, err := layout.EncodeInodeValue(record)
	if err != nil {
		b.Fatal(err)
	}
	runner.data[string(key)] = value
}

func benchmarkSeedInode(b *testing.B, runner *fakeRunner, inode model.InodeID) {
	b.Helper()
	benchmarkSeedInodeRecord(b, runner, model.InodeRecord{Inode: inode, Type: model.InodeTypeFile, LinkCount: 1})
}

func benchmarkSeedInodeRecord(b *testing.B, runner *fakeRunner, record model.InodeRecord) {
	b.Helper()
	key, err := layout.EncodeInodeKey(testMountIdentity, record.Inode)
	if err != nil {
		b.Fatal(err)
	}
	value, err := layout.EncodeInodeValue(record)
	if err != nil {
		b.Fatal(err)
	}
	runner.data[string(key)] = value
}

type corruptDirPageCache struct{}

func (c *corruptDirPageCache) CurrentEpoch(dirpage.DirectoryKey) uint64 { return 0 }

func (c *corruptDirPageCache) Lookup(dirpage.PageKey, uint64) ([]dirpage.Entry, bool) {
	return []dirpage.Entry{{Name: []byte("stale"), Inode: 999, AttrBlob: []byte("not-an-inode")}}, true
}

func (c *corruptDirPageCache) MaterializeAsync(dirpage.PageKey, uint64, []dirpage.Entry) error {
	return nil
}

func (c *corruptDirPageCache) Invalidate(dirpage.DirectoryKey) uint64 { return 1 }

func (c *corruptDirPageCache) Stats() dirpage.Stats { return dirpage.Stats{} }
