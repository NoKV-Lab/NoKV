// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package testcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	"github.com/feichai0017/NoKV/coordinator/tso"
	local "github.com/feichai0017/NoKV/local"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	myraft "github.com/feichai0017/NoKV/raft"
	raftkv "github.com/feichai0017/NoKV/raftstore/kv"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Node struct {
	StoreID         uint64
	WorkDir         string
	EnableLeaseRead bool
	DB              *local.DB
	LocalMeta       *localmeta.Store
	Server          *serverpkg.Node
}

type NodeConfig struct {
	AllowedModes      []workdirmode.Mode
	StartPeers        bool
	Scheduler         storecontrol.Client
	HeartbeatInterval time.Duration
	EnableLeaseRead   bool
}

type Coordinator struct {
	addr    string
	lis     net.Listener
	server  *grpc.Server
	service *coordserver.Service
}

func OpenStandaloneDB(tb testing.TB, dir string, tweak func(*local.Options)) *local.DB {
	tb.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = dir
	if tweak != nil {
		tweak(opt)
	}
	db, err := local.Open(opt)
	if err != nil {
		tb.Fatalf("open standalone db: %v", err)
	}
	return db
}

func StartNode(tb testing.TB, storeID uint64, dir string, allowedModes []workdirmode.Mode, startPeers bool) *Node {
	tb.Helper()
	return StartNodeWithConfig(tb, storeID, dir, NodeConfig{
		AllowedModes: allowedModes,
		StartPeers:   startPeers,
	})
}

func StartNodeWithConfig(tb testing.TB, storeID uint64, dir string, cfg NodeConfig) *Node {
	tb.Helper()
	localMeta, err := localmeta.OpenLocalStore(dir, nil)
	if err != nil {
		tb.Fatalf("open local meta: %v", err)
	}
	opt := local.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.DurableRaftPointerSnapshot)
	if cfg.AllowedModes != nil {
		opt.AllowedModes = cfg.AllowedModes
	}
	db, err := local.Open(opt)
	if err != nil {
		_ = localMeta.Close()
		tb.Fatalf("open node db: %v", err)
	}
	srv, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC:     db,
			Raft:     raftlog.NewDBLog(db),
			Snapshot: snapshotpkg.NewDBStore(db),
		},
		Store: storepkg.Config{
			StoreID:           storeID,
			LocalMeta:         localMeta,
			Scheduler:         cfg.Scheduler,
			HeartbeatInterval: cfg.HeartbeatInterval,
		},
		Raft:          raftConfig(0, cfg.EnableLeaseRead),
		TransportAddr: "127.0.0.1:0",
	})
	if err != nil {
		_ = db.Close()
		_ = localMeta.Close()
		tb.Fatalf("new server: %v", err)
	}
	node := &Node{StoreID: storeID, WorkDir: dir, EnableLeaseRead: cfg.EnableLeaseRead, DB: db, LocalMeta: localMeta, Server: srv}
	if cfg.StartPeers {
		StartPeers(tb, node)
	}
	return node
}

func StartCoordinator(tb testing.TB) *Coordinator {
	tb.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen coordinator: %v", err)
	}
	svc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), newCoordinatorRootStorage())
	svc.ConfigureAuthorityGrant("c1", time.Hour, 30*time.Minute)
	grpcServer := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(grpcServer, svc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	return &Coordinator{
		addr:    lis.Addr().String(),
		lis:     lis,
		server:  grpcServer,
		service: svc,
	}
}

type coordinatorRootStorage struct {
	mu       sync.Mutex
	snapshot rootview.Snapshot
}

func newCoordinatorRootStorage() *coordinatorRootStorage {
	return &coordinatorRootStorage{
		snapshot: rootview.Snapshot{
			CatchUpState: rootview.CatchUpStateFresh,
		},
	}
}

func (s *coordinatorRootStorage) Load() (rootview.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return rootview.CloneSnapshot(s.snapshot), nil
}

func (s *coordinatorRootStorage) AppendRootEvent(_ context.Context, event rootevent.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyEventLocked(event)
	return nil
}

func (s *coordinatorRootStorage) SaveAllocatorState(_ context.Context, idCurrent, tsCurrent uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idCurrent > s.snapshot.Allocator.IDCurrent {
		s.snapshot.Allocator.IDCurrent = idCurrent
	}
	if tsCurrent > s.snapshot.Allocator.TSCurrent {
		s.snapshot.Allocator.TSCurrent = tsCurrent
	}
	return nil
}

func (s *coordinatorRootStorage) ApplyGrant(_ context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	holderID := strings.TrimSpace(cmd.HolderID)
	switch cmd.Kind {
	case rootproto.GrantActIssue:
		active, _ := coordinatorRootActiveGrantFor(s.snapshot, cmd.RequestedDuties)
		requestedGrantID := strings.TrimSpace(cmd.GrantID)
		if requestedGrantID != "" &&
			active.Present() &&
			active.GrantID == requestedGrantID &&
			active.HolderID == holderID &&
			coordinatorDutyGrantsCover(active.Duties, cmd.RequestedDuties) {
			cert, err := coordinatorRootGrantCertificate(active)
			return s.protocolStateLocked(), cert, err
		}
		if active.Present() && active.HolderID != holderID && active.ActiveAt(cmd.NowUnixNano) {
			return s.protocolStateLocked(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		era := active.Era + 1
		for _, retirement := range s.snapshot.RetiredGrants {
			if retirement.Era >= era {
				era = retirement.Era + 1
			}
		}
		grantID := requestedGrantID
		if grantID == "" {
			grantID = fmt.Sprintf("%s/%d", holderID, era)
		}
		grant := rootproto.AuthorityGrant{
			GrantID:         grantID,
			HolderID:        holderID,
			Era:             era,
			ExpiresUnixNano: cmd.ExpiresUnixNano,
			IssuedRootToken: rootproto.AuthorityRootToken{
				Term:     s.snapshot.RootToken.Cursor.Term,
				Index:    s.snapshot.RootToken.Cursor.Index,
				Revision: s.snapshot.RootToken.Revision,
			},
			Duties: append([]rootproto.DutyGrant(nil), cmd.RequestedDuties...),
		}
		s.applyEventLocked(rootevent.GrantIssued(grant))
		issued, _ := s.snapshot.ActiveGrantByID(grant.GrantID)
		cert, err := coordinatorRootGrantCertificate(issued)
		return s.protocolStateLocked(), cert, err
	case rootproto.GrantActSeal:
		active, ok := s.snapshot.ActiveGrantByID(strings.TrimSpace(cmd.GrantID))
		if !ok || active.HolderID != holderID {
			return s.protocolStateLocked(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		retirement := rootproto.GrantRetirement{
			GrantID:  active.GrantID,
			HolderID: active.HolderID,
			Era:      active.Era,
			Mode:     rootproto.GrantRetirementSealedExact,
			Bounds:   coordinatorDutyGrantsFromUsages(cmd.ExactUsages),
		}
		if len(retirement.Bounds) == 0 {
			retirement.Bounds = append([]rootproto.DutyGrant(nil), active.Duties...)
		}
		s.applyEventLocked(rootevent.GrantSealed(retirement))
		return s.protocolStateLocked(), rootproto.GrantCertificate{}, nil
	case rootproto.GrantActInherit:
		active, ok := coordinatorRootActiveGrantForHolder(s.snapshot, holderID)
		if !ok {
			return s.protocolStateLocked(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		successor := active.GrantID
		for _, predecessor := range cmd.PredecessorGrantIDs {
			s.applyEventLocked(rootevent.GrantInherited(rootproto.GrantInheritance{
				PredecessorGrantID: predecessor,
				SuccessorGrantID:   successor,
			}))
		}
		return s.protocolStateLocked(), rootproto.GrantCertificate{}, nil
	default:
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
	}
}

func (s *coordinatorRootStorage) ApplyVisibleAuthority(_ context.Context, _ rootproto.VisibleAuthorityCommand) (rootstate.State, rootproto.VisibleAuthorityGrant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return rootstate.CloneState(s.snapshot.RootSnapshot().State), rootproto.VisibleAuthorityGrant{}, rootstate.ErrInvalidGrant
}

func (s *coordinatorRootStorage) Refresh() error            { return nil }
func (s *coordinatorRootStorage) Close() error              { return nil }
func (s *coordinatorRootStorage) CanSubmitRootWrites() bool { return true }
func (s *coordinatorRootStorage) LeaderID() uint64          { return 1 }

func (s *coordinatorRootStorage) applyEventLocked(event rootevent.Event) {
	rooted := s.snapshot.RootSnapshot()
	cursor := rootstate.NextCursor(rooted.State.LastCommitted)
	rootstate.ApplyEventToSnapshot(&rooted, cursor, event)
	nextRevision := s.snapshot.RootToken.Revision + 1
	s.snapshot = rootview.SnapshotFromRoot(rooted)
	s.snapshot.RootToken.Revision = nextRevision
}

func (s *coordinatorRootStorage) protocolStateLocked() rootstate.EunomiaState {
	return rootstate.EunomiaState{
		ActiveGrants:      append([]rootproto.AuthorityGrant(nil), s.snapshot.ActiveGrants...),
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), s.snapshot.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), s.snapshot.GrantInheritances...),
		RetiredEraFloors:  rootproto.CloneAuthorityRetiredEraFloors(s.snapshot.RetiredEraFloors),
	}
}

func coordinatorRootActiveGrantFor(snapshot rootview.Snapshot, duties []rootproto.DutyGrant) (rootproto.AuthorityGrant, bool) {
	for _, grant := range snapshot.ActiveGrants {
		for _, duty := range duties {
			if grant.CoversDutyKey(duty.Key()) {
				return grant, true
			}
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func coordinatorRootActiveGrantForHolder(snapshot rootview.Snapshot, holderID string) (rootproto.AuthorityGrant, bool) {
	for _, grant := range snapshot.ActiveGrants {
		if strings.TrimSpace(grant.HolderID) == holderID {
			return grant, true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func coordinatorRootGrantCertificate(grant rootproto.AuthorityGrant) (rootproto.GrantCertificate, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(grant))
	if err != nil {
		return rootproto.GrantCertificate{}, err
	}
	signature := rootproto.SignGrantBytes(payload)
	if len(signature) == 0 {
		return rootproto.GrantCertificate{}, fmt.Errorf("testcluster root grant signing key is not configured")
	}
	return rootproto.GrantCertificate{
		Grant:       grant,
		SignerKeyID: rootproto.GrantSignerKeyID,
		Signature:   signature,
	}, nil
}

func coordinatorDutyGrantsCover(grants, usages []rootproto.DutyGrant) bool {
	for _, usage := range usages {
		var matched bool
		for _, grant := range grants {
			if grant.DutyID == usage.DutyID &&
				rootproto.ScopeEqual(grant.Scope, usage.Scope) &&
				rootproto.DutyBoundCovers(grant.Bound, usage.Bound) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func coordinatorDutyGrantsFromUsages(usages []rootproto.AuthorityUsage) []rootproto.DutyGrant {
	out := make([]rootproto.DutyGrant, 0, len(usages))
	for _, usage := range usages {
		if usage.DutyID == "" {
			continue
		}
		out = append(out, rootproto.DutyGrant{
			DutyID: usage.DutyID,
			Scope:  usage.Scope,
			Bound:  usage.Usage,
		})
	}
	return out
}

func (c *Coordinator) Addr() string {
	if c == nil {
		return ""
	}
	return c.addr
}

func (c *Coordinator) Close(tb testing.TB) {
	tb.Helper()
	if c == nil {
		return
	}
	if c.server != nil {
		c.server.Stop()
		c.server = nil
	}
	if c.lis != nil {
		if err := c.lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			tb.Fatalf("close coordinator listener: %v", err)
		}
		c.lis = nil
	}
}

func (c *Coordinator) PublishRootEvent(tb testing.TB, event rootevent.Event) {
	tb.Helper()
	if c == nil || c.service == nil {
		tb.Fatalf("publish root event: coordinator is not running")
	}
	resp, err := c.service.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	if err != nil {
		tb.Fatalf("publish root event %v: %v", event.Kind, err)
	}
	if resp == nil || !resp.GetAccepted() {
		tb.Fatalf("publish root event %v was not accepted", event.Kind)
	}
}

func (c *Coordinator) JoinStore(tb testing.TB, storeID uint64) {
	tb.Helper()
	c.PublishRootEvent(tb, rootevent.StoreJoined(storeID))
}

func NewScheduler(tb testing.TB, coordAddr string, timeout time.Duration) storecontrol.Client {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cli, err := coordclient.NewGRPCClient(ctx, coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("dial coordinator client %s: %v", coordAddr, err)
	}
	return storecontrol.NewClient(storecontrol.Config{
		Coordinator: cli,
		Timeout:     timeout,
	})
}

func StartPeers(tb testing.TB, node *Node) {
	tb.Helper()
	snapshot := node.LocalMeta.Snapshot()
	ids := make([]uint64, 0, len(snapshot))
	for id := range snapshot {
		if id != 0 {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		meta := snapshot[id]
		var peerID uint64
		for _, p := range meta.Peers {
			if p.StoreID == node.StoreID {
				peerID = p.PeerID
				break
			}
		}
		if peerID == 0 {
			continue
		}
		storage, err := raftlog.NewDBLog(node.DB).Open(meta.ID, node.LocalMeta)
		if err != nil {
			tb.Fatalf("open peer storage: %v", err)
		}
		cfg := peerConfig(node, meta, peerID, storage)
		bootstrapPeers := make([]myraft.Peer, 0, len(meta.Peers))
		for _, p := range meta.Peers {
			bootstrapPeers = append(bootstrapPeers, myraft.Peer{ID: p.PeerID})
		}
		if _, err := node.Server.Store().StartPeer(cfg, bootstrapPeers); err != nil {
			tb.Fatalf("start peer %d: %v", peerID, err)
		}
	}
}

// forwarding-ok: test-cluster Node surface intentionally mirrors Server.Addr().
func (n *Node) Addr() string {
	return n.Server.Addr()
}

func (n *Node) WirePeers(peers map[uint64]string) {
	for peerID, addr := range peers {
		n.Server.Transport().SetPeer(peerID, addr)
	}
}

func (n *Node) BlockPeer(peerID uint64) {
	if n == nil || n.Server == nil || n.Server.Transport() == nil {
		return
	}
	n.Server.Transport().BlockPeer(peerID)
}

func (n *Node) UnblockPeer(peerID uint64) {
	if n == nil || n.Server == nil || n.Server.Transport() == nil {
		return
	}
	n.Server.Transport().UnblockPeer(peerID)
}

func (n *Node) Restart(tb testing.TB, allowedModes []workdirmode.Mode, startPeers bool) {
	tb.Helper()
	workDir := n.WorkDir
	storeID := n.StoreID
	enableLeaseRead := n.EnableLeaseRead
	n.Close(tb)
	restarted := StartNodeWithConfig(tb, storeID, workDir, NodeConfig{
		AllowedModes:    allowedModes,
		StartPeers:      startPeers,
		EnableLeaseRead: enableLeaseRead,
	})
	*n = *restarted
}

func (n *Node) Close(tb testing.TB) {
	tb.Helper()
	if n == nil {
		return
	}
	if n.Server != nil {
		if err := n.Server.Close(); err != nil {
			tb.Fatalf("close server: %v", err)
		}
		n.Server = nil
	}
	if n.DB != nil {
		if err := n.DB.Close(); err != nil {
			tb.Fatalf("close db: %v", err)
		}
		n.DB = nil
	}
	if n.LocalMeta != nil {
		if err := n.LocalMeta.Close(); err != nil {
			tb.Fatalf("close local meta: %v", err)
		}
		n.LocalMeta = nil
	}
}

func FetchRuntimeStatus(tb testing.TB, ctx context.Context, addr string, regionID uint64) *adminpb.RegionRuntimeStatusResponse {
	tb.Helper()
	status, err := TryFetchRuntimeStatus(ctx, addr, regionID)
	if err != nil {
		tb.Fatalf("region runtime status: %v", err)
	}
	return status
}

func TryFetchRuntimeStatus(ctx context.Context, addr string, regionID uint64) (*adminpb.RegionRuntimeStatusResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial admin %s: %w", addr, err)
	}
	defer func() {
		_ = conn.Close()
	}()
	client := adminpb.NewRaftAdminClient(conn)
	status, err := client.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: regionID})
	if err != nil {
		return nil, err
	}
	return status, nil
}

// Wait helpers tolerate transient admin RPC failures because raft membership,
// leader transfer, and process restart tests intentionally create short windows
// where a node is alive but cannot yet answer a stable runtime status.
func WaitForLeaderPeer(tb testing.TB, ctx context.Context, addr string, regionID, peerID uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastStatus *adminpb.RegionRuntimeStatusResponse
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := TryPollRuntimeStatus(ctx, addr, regionID)
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		lastStatus = status
		lastErr = nil
		if status.GetKnown() && status.GetLeader() && status.GetLeaderPeerId() == peerID {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for leader peer %d on region %d at %s (last status=%s, last error=%v)", peerID, regionID, addr, formatRuntimeStatus(lastStatus), lastErr)
}

func WaitForHostedPeer(tb testing.TB, ctx context.Context, addr string, regionID, peerID uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastStatus *adminpb.RegionRuntimeStatusResponse
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := TryPollRuntimeStatus(ctx, addr, regionID)
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		lastStatus = status
		lastErr = nil
		if status.GetKnown() && status.GetHosted() && status.GetLocalPeerId() == peerID && status.GetAppliedIndex() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for hosted peer %d on region %d at %s (last status=%s, last error=%v)", peerID, regionID, addr, formatRuntimeStatus(lastStatus), lastErr)
}

func WaitForNotHosted(tb testing.TB, ctx context.Context, addr string, regionID uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastStatus *adminpb.RegionRuntimeStatusResponse
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := TryPollRuntimeStatus(ctx, addr, regionID)
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		lastStatus = status
		lastErr = nil
		if !status.GetHosted() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for region %d at %s to become not hosted (last status=%s, last error=%v)", regionID, addr, formatRuntimeStatus(lastStatus), lastErr)
}

func FindLeader(tb testing.TB, ctx context.Context, regionID uint64, nodes ...*Node) (*Node, *adminpb.RegionRuntimeStatusResponse) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		node, status, err := TryFindLeader(ctx, regionID, nodes...)
		if err == nil {
			return node, status
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for any leader on region %d (last error=%v, status=%s)", regionID, lastErr, DumpStatus(tb, ctx, regionID, nodes...))
	return nil, nil
}

// TryFindLeader performs one best-effort leader scan without failing the caller's test.
func TryFindLeader(ctx context.Context, regionID uint64, nodes ...*Node) (*Node, *adminpb.RegionRuntimeStatusResponse, error) {
	var errs []error
	statuses := make([]string, 0, len(nodes))
	for _, node := range nodes {
		status, err := TryPollRuntimeStatus(ctx, node.Addr(), regionID)
		if err != nil {
			errs = append(errs, fmt.Errorf("store %d: %w", node.StoreID, err))
			continue
		}
		statuses = append(statuses, fmt.Sprintf("store=%d %s", node.StoreID, formatRuntimeStatus(status)))
		if status.GetKnown() && status.GetLeader() {
			return node, status, nil
		}
	}
	err := fmt.Errorf("no leader for region %d (status=%v)", regionID, statuses)
	if len(errs) > 0 {
		err = fmt.Errorf("%w: %w", err, errors.Join(errs...))
	}
	return nil, nil, err
}

// TryPollRuntimeStatus performs one bounded admin status RPC for polling loops.
func TryPollRuntimeStatus(ctx context.Context, addr string, regionID uint64) (*adminpb.RegionRuntimeStatusResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	return TryFetchRuntimeStatus(callCtx, addr, regionID)
}

func formatRuntimeStatus(status *adminpb.RegionRuntimeStatusResponse) string {
	if status == nil {
		return "<nil>"
	}
	return fmt.Sprintf("known=%v hosted=%v local=%d leader=%v leaderPeer=%d applied=%d", status.GetKnown(), status.GetHosted(), status.GetLocalPeerId(), status.GetLeader(), status.GetLeaderPeerId(), status.GetAppliedIndex())
}

func AssertValue(tb testing.TB, db *local.DB, key, value []byte) {
	tb.Helper()
	entry, err := db.Get(key)
	if err != nil {
		tb.Fatalf("get key %q: %v", key, err)
	}
	if !bytes.Equal(entry.Value, value) {
		tb.Fatalf("value mismatch for key %q: got %q want %q", key, entry.Value, value)
	}
}

func peerConfig(node *Node, meta localmeta.RegionMeta, peerID uint64, storage raftlog.PeerStorage) *peer.Config {
	snapshotStore := snapshotpkg.NewDBStore(node.DB)
	snapshotApply := func(payload []byte) (localmeta.RegionMeta, error) {
		result, err := snapshotStore.ImportSnapshot(payload)
		if err != nil {
			return localmeta.RegionMeta{}, err
		}
		return result.Meta.Region, nil
	}
	return &peer.Config{
		RaftConfig:     raftConfig(peerID, node.EnableLeaseRead),
		Transport:      node.Server.Transport(),
		Apply:          raftkv.NewEntryApplier(node.DB),
		SnapshotExport: snapshotStore.ExportSnapshot,
		SnapshotApply:  snapshotApply,
		Storage:        storage,
		GroupID:        meta.ID,
		Region:         localmeta.CloneRegionMetaPtr(&meta),
	}
}

func raftConfig(peerID uint64, enableLeaseRead bool) myraft.Config {
	cfg := myraft.Config{
		ID:              peerID,
		ElectionTick:    5,
		HeartbeatTick:   1,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
	}
	if enableLeaseRead {
		return peer.EnableLeaseRead(cfg)
	}
	return cfg
}

func DumpStatus(tb testing.TB, ctx context.Context, regionID uint64, nodes ...*Node) string {
	tb.Helper()
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		status, err := TryPollRuntimeStatus(ctx, node.Addr(), regionID)
		if err != nil {
			parts = append(parts, fmt.Sprintf("store=%d err=%v", node.StoreID, err))
			continue
		}
		parts = append(parts, fmt.Sprintf("store=%d %s", node.StoreID, formatRuntimeStatus(status)))
	}
	return fmt.Sprint(parts)
}

func WaitForSchedulerMode(tb testing.TB, node *Node, mode storecontrol.Mode, degraded bool) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := node.Server.Store().SchedulerStatus()
		if status.Mode == mode && status.Degraded == degraded {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for scheduler mode=%s degraded=%v on store %d (last=%+v)", mode, degraded, node.StoreID, node.Server.Store().SchedulerStatus())
}
