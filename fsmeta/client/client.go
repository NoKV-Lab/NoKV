// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	fsmetaReasonMetadata = "fsmeta_reason"

	reasonQuotaExceeded        = "quota_exceeded"
	reasonWatchOverflow        = "watch_overflow"
	reasonWatchCursorExpired   = "watch_cursor_expired"
	reasonMountNotRegistered   = "mount_not_registered"
	reasonMountRetired         = "mount_retired"
	reasonCrossAuthorityRename = "cross_authority_rename"
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
)

// Client is the typed fsmeta client surface consumed by demos and benchmarks.
type Client interface {
	Create(ctx context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error)
	UpdateInode(ctx context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error)
	Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error)
	LookupPlus(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryAttrPair, error)
	ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error)
	ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error)
	WatchSubtree(ctx context.Context, req fsmeta.WatchRequest) (WatchSubscription, error)
	GetReadVersion(ctx context.Context, req fsmeta.ReadVersionRequest) (uint64, error)
	SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error)
	RetireSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error
	GetQuotaUsage(ctx context.Context, req fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error)
	Rename(ctx context.Context, req fsmeta.RenameRequest) error
	RenameReplace(ctx context.Context, req fsmeta.RenameReplaceRequest) (fsmeta.RenameReplaceResult, error)
	RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error
	Link(ctx context.Context, req fsmeta.LinkRequest) error
	Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error
	Remove(ctx context.Context, req fsmeta.RemoveRequest) error
	RemoveDirectory(ctx context.Context, req fsmeta.RemoveDirectoryRequest) error
	OpenWriteSession(ctx context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error)
	HeartbeatWriteSession(ctx context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error)
	CloseWriteSession(ctx context.Context, req fsmeta.CloseWriteSessionRequest) error
	ExpireWriteSessions(ctx context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error)
	Close() error
}

// ClientConfig controls the typed client assembly around the gRPC transport.
type ClientConfig struct {
	LookupCache        LookupCacheConfig
	DisableLookupCache bool
}

// GRPCClient wraps the generated FSMetadata client with typed fsmeta records
// and the default client-side components.
type GRPCClient struct {
	conn   *grpc.ClientConn
	rpc    fsmetapb.FSMetadataClient
	lookup *LookupCache
}

// New wraps an existing generated FSMetadata client.
func New(rpc fsmetapb.FSMetadataClient) *GRPCClient {
	cli, _ := NewWithConfig(rpc, ClientConfig{})
	return cli
}

// NewWithConfig wraps an existing generated FSMetadata client with explicit
// client-side component configuration.
func NewWithConfig(rpc fsmetapb.FSMetadataClient, cfg ClientConfig) (*GRPCClient, error) {
	lookup, err := newLookupCacheForClient(cfg)
	if err != nil {
		return nil, err
	}
	return &GRPCClient{rpc: rpc, lookup: lookup}, nil
}

// NewGRPCClient dials one FSMetadata endpoint and returns a typed client.
func NewGRPCClient(ctx context.Context, addr string, dialOpts ...grpc.DialOption) (*GRPCClient, error) {
	return NewGRPCClientWithConfig(ctx, addr, ClientConfig{}, dialOpts...)
}

// NewGRPCClientWithConfig dials one FSMetadata endpoint and returns a typed
// client with explicit client-side component configuration.
func NewGRPCClientWithConfig(ctx context.Context, addr string, cfg ClientConfig, dialOpts ...grpc.DialOption) (*GRPCClient, error) {
	if addr == "" {
		return nil, errAddressRequired
	}
	lookup, err := newLookupCacheForClient(cfg)
	if err != nil {
		return nil, err
	}
	opts := normalizeDialOptions(dialOpts)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	if err := waitForReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &GRPCClient{
		conn:   conn,
		rpc:    fsmetapb.NewFSMetadataClient(conn),
		lookup: lookup,
	}, nil
}

func newLookupCacheForClient(cfg ClientConfig) (*LookupCache, error) {
	if cfg.DisableLookupCache {
		return nil, nil
	}
	return NewLookupCache(cfg.LookupCache)
}

// LookupCacheStats returns a point-in-time copy of the default lookup cache
// counters. It returns zero counters when lookup caching is disabled.
func (c *GRPCClient) LookupCacheStats() LookupCacheStats {
	if c == nil || c.lookup == nil {
		return LookupCacheStats{}
	}
	return c.lookup.Stats()
}

func (c *GRPCClient) Create(ctx context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.CreateResult{}, err
	}
	resp, err := c.rpc.Create(ctx, createRequestToProto(req))
	if err != nil {
		return fsmeta.CreateResult{}, translateRPCError(err)
	}
	result := fsmeta.CreateResult{
		Dentry: dentryFromProto(resp.GetDentry()),
		Inode:  inodeFromProto(resp.GetInode()),
	}
	c.lookup.Put(req.Mount, result.Dentry)
	return result, nil
}

func (c *GRPCClient) UpdateInode(ctx context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.InodeRecord{}, err
	}
	resp, err := c.rpc.UpdateInode(ctx, updateInodeRequestToProto(req))
	if err != nil {
		return fsmeta.InodeRecord{}, translateRPCError(err)
	}
	return inodeFromProto(resp.GetInode()), nil
}

func (c *GRPCClient) Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if record, ok := c.lookup.Get(req.Mount, req.Parent, req.Name); ok {
		return record, nil
	}
	resp, err := c.rpc.Lookup(ctx, lookupRequestToProto(req))
	if err != nil {
		return fsmeta.DentryRecord{}, translateRPCError(err)
	}
	record := dentryFromProto(resp.GetDentry())
	c.lookup.Put(req.Mount, record)
	return record, nil
}

func (c *GRPCClient) LookupPlus(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryAttrPair, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.DentryAttrPair{}, err
	}
	resp, err := c.rpc.LookupPlus(ctx, lookupRequestToProto(req))
	if err != nil {
		return fsmeta.DentryAttrPair{}, translateRPCError(err)
	}
	pair := pairFromProto(resp.GetEntry())
	c.lookup.Put(req.Mount, pair.Dentry)
	return pair, nil
}

func (c *GRPCClient) ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	if err := c.requireRPC(); err != nil {
		return nil, err
	}
	resp, err := c.rpc.ReadDir(ctx, readDirRequestToProto(req))
	if err != nil {
		return nil, translateRPCError(err)
	}
	out := make([]fsmeta.DentryRecord, 0, len(resp.GetEntries()))
	for _, entry := range resp.GetEntries() {
		out = append(out, dentryFromProto(entry))
	}
	if req.SnapshotVersion == 0 {
		c.lookup.PutMany(req.Mount, out)
	}
	return out, nil
}

func (c *GRPCClient) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	if err := c.requireRPC(); err != nil {
		return nil, err
	}
	resp, err := c.rpc.ReadDirPlus(ctx, readDirRequestToProto(req))
	if err != nil {
		return nil, translateRPCError(err)
	}
	out := make([]fsmeta.DentryAttrPair, 0, len(resp.GetEntries()))
	for _, entry := range resp.GetEntries() {
		out = append(out, pairFromProto(entry))
	}
	if req.SnapshotVersion == 0 {
		for _, pair := range out {
			c.lookup.Put(req.Mount, pair.Dentry)
		}
	}
	return out, nil
}

// WatchSubtree opens a prefix watch stream. When ResumeCursor is set, the
// server replays retained events after that cursor before switching to live
// delivery.
func (c *GRPCClient) WatchSubtree(ctx context.Context, req fsmeta.WatchRequest) (WatchSubscription, error) {
	if err := c.requireRPC(); err != nil {
		return nil, err
	}
	stream, err := c.rpc.WatchSubtree(ctx)
	if err != nil {
		return nil, translateRPCError(err)
	}
	if err := stream.Send(&fsmetapb.WatchAckOrSubscribe{
		Body: &fsmetapb.WatchAckOrSubscribe_Subscribe{Subscribe: watchRequestToProto(req)},
	}); err != nil {
		return nil, translateRPCError(err)
	}
	ready, err := waitForWatchReady(stream)
	if err != nil {
		_ = stream.CloseSend()
		return nil, err
	}
	return &WatchStream{stream: stream, ready: ready}, nil
}

func (c *GRPCClient) GetReadVersion(ctx context.Context, req fsmeta.ReadVersionRequest) (uint64, error) {
	if err := c.requireRPC(); err != nil {
		return 0, err
	}
	resp, err := c.rpc.GetReadVersion(ctx, getReadVersionRequestToProto(req))
	if err != nil {
		return 0, translateRPCError(err)
	}
	return resp.GetReadVersion(), nil
}

func (c *GRPCClient) SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	resp, err := c.rpc.SnapshotSubtree(ctx, snapshotSubtreeRequestToProto(req))
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, translateRPCError(err)
	}
	return snapshotSubtreeTokenFromProto(resp), nil
}

func (c *GRPCClient) RetireSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.RetireSnapshotSubtree(ctx, retireSnapshotSubtreeRequestToProto(token))
	return translateRPCError(err)
}

func (c *GRPCClient) GetQuotaUsage(ctx context.Context, req fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.UsageRecord{}, err
	}
	resp, err := c.rpc.GetQuotaUsage(ctx, quotaUsageRequestToProto(req))
	if err != nil {
		return fsmeta.UsageRecord{}, translateRPCError(err)
	}
	return quotaUsageFromProto(resp), nil
}

func (c *GRPCClient) Rename(ctx context.Context, req fsmeta.RenameRequest) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	from := lookupCacheKey{mount: req.Mount, parent: req.FromParent, name: req.FromName}
	to := lookupCacheKey{mount: req.Mount, parent: req.ToParent, name: req.ToName}
	record, hadSource := c.lookup.peek(from)
	_, err := c.rpc.Rename(ctx, renameRequestToProto(req))
	if err != nil {
		return translateRPCError(err)
	}
	c.lookup.invalidate(from)
	c.lookup.invalidate(to)
	if hadSource {
		record.Parent = req.ToParent
		record.Name = req.ToName
		c.lookup.Put(req.Mount, record)
	}
	return nil
}

func (c *GRPCClient) RenameReplace(ctx context.Context, req fsmeta.RenameReplaceRequest) (fsmeta.RenameReplaceResult, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.RenameReplaceResult{}, err
	}
	from := lookupCacheKey{mount: req.Mount, parent: req.FromParent, name: req.FromName}
	to := lookupCacheKey{mount: req.Mount, parent: req.ToParent, name: req.ToName}
	record, hadSource := c.lookup.peek(from)
	resp, err := c.rpc.RenameReplace(ctx, renameReplaceRequestToProto(req))
	if err != nil {
		return fsmeta.RenameReplaceResult{}, translateRPCError(err)
	}
	c.lookup.invalidate(from)
	c.lookup.invalidate(to)
	if hadSource {
		record.Parent = req.ToParent
		record.Name = req.ToName
		c.lookup.Put(req.Mount, record)
	}
	return renameReplaceResultFromProto(resp), nil
}

func (c *GRPCClient) RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	from := lookupCacheKey{mount: req.Mount, parent: req.FromParent, name: req.FromName}
	to := lookupCacheKey{mount: req.Mount, parent: req.ToParent, name: req.ToName}
	record, hadSource := c.lookup.peek(from)
	_, err := c.rpc.RenameSubtree(ctx, renameSubtreeRequestToProto(req))
	if err != nil {
		return translateRPCError(err)
	}
	c.lookup.invalidate(from)
	c.lookup.invalidate(to)
	if hadSource {
		record.Parent = req.ToParent
		record.Name = req.ToName
		c.lookup.Put(req.Mount, record)
	}
	return nil
}

func (c *GRPCClient) Link(ctx context.Context, req fsmeta.LinkRequest) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.Link(ctx, linkRequestToProto(req))
	if err != nil {
		return translateRPCError(err)
	}
	c.lookup.Invalidate(req.Mount, req.ToParent, req.ToName)
	return nil
}

func (c *GRPCClient) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.Unlink(ctx, unlinkRequestToProto(req))
	if err != nil {
		return translateRPCError(err)
	}
	c.lookup.Invalidate(req.Mount, req.Parent, req.Name)
	return nil
}

func (c *GRPCClient) Remove(ctx context.Context, req fsmeta.RemoveRequest) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.Remove(ctx, removeRequestToProto(req))
	if err != nil {
		return translateRPCError(err)
	}
	c.lookup.Invalidate(req.Mount, req.Parent, req.Name)
	return nil
}

func (c *GRPCClient) RemoveDirectory(ctx context.Context, req fsmeta.RemoveDirectoryRequest) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.RemoveDirectory(ctx, removeDirectoryRequestToProto(req))
	if err != nil {
		return translateRPCError(err)
	}
	c.lookup.Invalidate(req.Mount, req.Parent, req.Name)
	return nil
}

func (c *GRPCClient) OpenWriteSession(ctx context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	resp, err := c.rpc.OpenWriteSession(ctx, openWriteSessionRequestToProto(req))
	if err != nil {
		return fsmeta.SessionRecord{}, translateRPCError(err)
	}
	return sessionFromProto(resp.GetSession()), nil
}

func (c *GRPCClient) HeartbeatWriteSession(ctx context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	resp, err := c.rpc.HeartbeatWriteSession(ctx, heartbeatWriteSessionRequestToProto(req))
	if err != nil {
		return fsmeta.SessionRecord{}, translateRPCError(err)
	}
	return sessionFromProto(resp.GetSession()), nil
}

func (c *GRPCClient) CloseWriteSession(ctx context.Context, req fsmeta.CloseWriteSessionRequest) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.CloseWriteSession(ctx, closeWriteSessionRequestToProto(req))
	return translateRPCError(err)
}

func (c *GRPCClient) ExpireWriteSessions(ctx context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, err
	}
	resp, err := c.rpc.ExpireWriteSessions(ctx, expireWriteSessionsRequestToProto(req))
	if err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, translateRPCError(err)
	}
	return fsmeta.ExpireWriteSessionsResult{Expired: resp.GetExpired()}, nil
}

// WatchSubscription is one typed WatchSubtree client stream.
type WatchSubscription interface {
	Recv() (fsmeta.WatchEvent, error)
	ReadyCursor() fsmeta.WatchCursor
	Ack(fsmeta.WatchCursor) error
	Close() error
}

// WatchStream is the gRPC-backed WatchSubtree stream implementation.
type WatchStream struct {
	stream fsmetapb.FSMetadata_WatchSubtreeClient
	ready  fsmeta.WatchCursor
}

// Recv blocks until the next watch event arrives.
func (s *WatchStream) Recv() (fsmeta.WatchEvent, error) {
	if s == nil || s.stream == nil {
		return fsmeta.WatchEvent{}, errWatchStreamNotConfigured
	}
	for {
		resp, err := s.stream.Recv()
		if err != nil {
			return fsmeta.WatchEvent{}, translateRPCError(err)
		}
		if event := resp.GetEvent(); event != nil {
			return watchEventFromProto(event), nil
		}
		if throttle := resp.GetThrottle(); throttle != nil {
			return fsmeta.WatchEvent{}, fmt.Errorf("%w: %s", fsmeta.ErrWatchOverflow, throttle.GetReason())
		}
		// Ready and catch-up markers are stream-control frames, not user events.
	}
}

func waitForWatchReady(stream fsmetapb.FSMetadata_WatchSubtreeClient) (fsmeta.WatchCursor, error) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return fsmeta.WatchCursor{}, translateRPCError(err)
		}
		if ready := resp.GetReady(); ready != nil {
			return watchCursorFromProto(ready.GetCursor()), nil
		}
		if throttle := resp.GetThrottle(); throttle != nil {
			return fsmeta.WatchCursor{}, fmt.Errorf("%w: %s", fsmeta.ErrWatchOverflow, throttle.GetReason())
		}
		if resp.GetEvent() != nil {
			return fsmeta.WatchCursor{}, errWatchEventBeforeReady
		}
	}
}

// ReadyCursor returns the server frontier after the subscription's initial
// catch-up replay was queued.
func (s *WatchStream) ReadyCursor() fsmeta.WatchCursor {
	if s == nil {
		return fsmeta.WatchCursor{}
	}
	return s.ready
}

// Ack releases back-pressure budget for a received event.
func (s *WatchStream) Ack(cursor fsmeta.WatchCursor) error {
	if s == nil || s.stream == nil {
		return errWatchStreamNotConfigured
	}
	return translateRPCError(s.stream.Send(&fsmetapb.WatchAckOrSubscribe{
		Body: &fsmetapb.WatchAckOrSubscribe_Ack{Ack: &fsmetapb.WatchAck{Cursor: watchCursorToProto(cursor)}},
	}))
}

// AckEvent releases back-pressure budget for a received event.
func (s *WatchStream) AckEvent(evt fsmeta.WatchEvent) error {
	return s.Ack(evt.Cursor)
}

// Close closes the sending side of the watch stream.
func (s *WatchStream) Close() error {
	if s == nil || s.stream == nil {
		return nil
	}
	return s.stream.CloseSend()
}

// Close closes the underlying connection when this client owns one.
func (c *GRPCClient) Close() error {
	if c != nil && c.lookup != nil {
		c.lookup.Clear()
	}
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *GRPCClient) requireRPC() error {
	if c == nil || c.rpc == nil {
		return errRPCClientNotConfigured
	}
	return nil
}

func translateRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		if sentinel := invalidReasonSentinel(fsmetaReason(err)); sentinel != nil {
			return fmt.Errorf("%w: %v", sentinel, err)
		}
		return err
	case codes.AlreadyExists:
		return fmt.Errorf("%w: %v", fsmeta.ErrExists, err)
	case codes.NotFound:
		if fsmetaReason(err) == reasonMountNotRegistered {
			return fmt.Errorf("%w: %v", fsmeta.ErrMountNotRegistered, err)
		}
		return fmt.Errorf("%w: %v", fsmeta.ErrNotFound, err)
	case codes.OutOfRange:
		if fsmetaReason(err) == reasonWatchCursorExpired {
			return fmt.Errorf("%w: %v", fsmeta.ErrWatchCursorExpired, err)
		}
		return err
	case codes.FailedPrecondition:
		switch fsmetaReason(err) {
		case reasonMountRetired:
			return fmt.Errorf("%w: %v", fsmeta.ErrMountRetired, err)
		case reasonCrossAuthorityRename:
			return fmt.Errorf("%w: %v", fsmeta.ErrCrossAuthorityRename, err)
		}
		return err
	case codes.ResourceExhausted:
		switch fsmetaReason(err) {
		case reasonQuotaExceeded:
			return fmt.Errorf("%w: %v", fsmeta.ErrQuotaExceeded, err)
		case reasonWatchOverflow:
			return fmt.Errorf("%w: %v", fsmeta.ErrWatchOverflow, err)
		}
		return err
	default:
		return err
	}
}

func invalidReasonSentinel(reason string) error {
	switch reason {
	case reasonInvalidMountID:
		return fsmeta.ErrInvalidMountID
	case reasonInvalidInodeID:
		return fsmeta.ErrInvalidInodeID
	case reasonInvalidName:
		return fsmeta.ErrInvalidName
	case reasonInvalidSession:
		return fsmeta.ErrInvalidSession
	case reasonInvalidRequest, reasonInvalidFSMetaInput:
		return fsmeta.ErrInvalidRequest
	case reasonInvalidKey:
		return fsmeta.ErrInvalidKey
	case reasonInvalidKeyKind:
		return fsmeta.ErrInvalidKeyKind
	case reasonInvalidValue:
		return fsmeta.ErrInvalidValue
	case reasonInvalidValueKind:
		return fsmeta.ErrInvalidValueKind
	case reasonInvalidPageSize:
		return fsmeta.ErrInvalidPageSize
	default:
		return nil
	}
}

func fsmetaReason(err error) string {
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	if !ok {
		return ""
	}
	return metadata[fsmetaReasonMetadata]
}

// WatchSession wraps a WatchSubscription with event-based ack helpers.
type WatchSession struct {
	sub WatchSubscription
}

// NewWatchSession constructs a helper around one watch subscription.
func NewWatchSession(sub WatchSubscription) *WatchSession {
	return &WatchSession{sub: sub}
}

// Recv receives the next watch event.
func (s *WatchSession) Recv() (fsmeta.WatchEvent, error) {
	if s == nil || s.sub == nil {
		return fsmeta.WatchEvent{}, errWatchSessionNotConfigured
	}
	return s.sub.Recv()
}

// Ack acknowledges the cursor carried by event.
func (s *WatchSession) Ack(event fsmeta.WatchEvent) error {
	if s == nil || s.sub == nil {
		return errWatchSessionNotConfigured
	}
	return s.sub.Ack(event.Cursor)
}

// ReadyCursor returns the server frontier after initial replay.
func (s *WatchSession) ReadyCursor() fsmeta.WatchCursor {
	if s == nil || s.sub == nil {
		return fsmeta.WatchCursor{}
	}
	return s.sub.ReadyCursor()
}

// Close closes the wrapped subscription.
func (s *WatchSession) Close() error {
	if s == nil || s.sub == nil {
		return nil
	}
	return s.sub.Close()
}

func normalizeDialOptions(opts []grpc.DialOption) []grpc.DialOption {
	out := make([]grpc.DialOption, 0, len(opts)+1)
	if len(opts) == 0 {
		out = append(out, grpc.WithTransportCredentials(insecure.NewCredentials()))
		return out
	}
	out = append(out, opts...)
	return out
}

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	if ctx == nil {
		return nil
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return errConnectionNotReady
		}
	}
}
