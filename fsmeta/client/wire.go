// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
)

func createRequestToProto(req fsmeta.CreateRequest) *fsmetapb.CreateRequest {
	return &fsmetapb.CreateRequest{
		Mount:  string(req.Mount),
		Parent: uint64(req.Parent),
		Name:   req.Name,
		Attrs:  createAttrsToProto(req.Attrs),
	}
}

func createAttrsToProto(attrs fsmeta.CreateAttrs) *fsmetapb.CreateInodeAttrs {
	return &fsmetapb.CreateInodeAttrs{
		Type:          inodeTypeToProto(attrs.Type),
		Size:          attrs.Size,
		Mode:          attrs.Mode,
		CreatedUnixNs: attrs.CreatedUnixNs,
		UpdatedUnixNs: attrs.UpdatedUnixNs,
		OpaqueAttrs:   append([]byte(nil), attrs.OpaqueAttrs...),
	}
}

func updateInodeRequestToProto(req fsmeta.UpdateInodeRequest) *fsmetapb.UpdateInodeRequest {
	return &fsmetapb.UpdateInodeRequest{
		Mount:            string(req.Mount),
		Parent:           uint64(req.Parent),
		Inode:            uint64(req.Inode),
		Name:             req.Name,
		SetSize:          req.SetSize,
		Size:             req.Size,
		SetMode:          req.SetMode,
		Mode:             req.Mode,
		SetUpdatedUnixNs: req.SetUpdatedUnixNs,
		UpdatedUnixNs:    req.UpdatedUnixNs,
		SetOpaqueAttrs:   req.SetOpaqueAttrs,
		OpaqueAttrs:      append([]byte(nil), req.OpaqueAttrs...),
	}
}

func lookupRequestToProto(req fsmeta.LookupRequest) *fsmetapb.LookupRequest {
	return &fsmetapb.LookupRequest{
		Mount:  string(req.Mount),
		Parent: uint64(req.Parent),
		Name:   req.Name,
	}
}

func readDirRequestToProto(req fsmeta.ReadDirRequest) *fsmetapb.ReadDirRequest {
	return &fsmetapb.ReadDirRequest{
		Mount:           string(req.Mount),
		Parent:          uint64(req.Parent),
		StartAfter:      req.StartAfter,
		Limit:           req.Limit,
		SnapshotVersion: req.SnapshotVersion,
	}
}

func getReadVersionRequestToProto(req fsmeta.ReadVersionRequest) *fsmetapb.GetReadVersionRequest {
	return &fsmetapb.GetReadVersionRequest{Mount: string(req.Mount)}
}

func snapshotSubtreeRequestToProto(req fsmeta.SnapshotSubtreeRequest) *fsmetapb.SnapshotSubtreeRequest {
	return &fsmetapb.SnapshotSubtreeRequest{
		Mount:     string(req.Mount),
		RootInode: uint64(req.RootInode),
	}
}

func snapshotSubtreeTokenFromProto(resp *fsmetapb.SnapshotSubtreeResponse) fsmeta.SnapshotSubtreeToken {
	if resp == nil {
		return fsmeta.SnapshotSubtreeToken{}
	}
	return fsmeta.SnapshotSubtreeToken{
		Mount:           fsmeta.MountID(resp.GetMount()),
		RootInode:       fsmeta.InodeID(resp.GetRootInode()),
		ReadVersion:     resp.GetReadVersion(),
		RuntimeEvidence: snapshotEvidenceRefsFromProto(resp.GetRuntimeEvidence()),
	}
}

func retireSnapshotSubtreeRequestToProto(token fsmeta.SnapshotSubtreeToken) *fsmetapb.RetireSnapshotSubtreeRequest {
	return &fsmetapb.RetireSnapshotSubtreeRequest{
		Mount:           string(token.Mount),
		RootInode:       uint64(token.RootInode),
		ReadVersion:     token.ReadVersion,
		RuntimeEvidence: snapshotEvidenceRefsToProto(token.RuntimeEvidence),
	}
}

func snapshotEvidenceRefsToProto(refs []fsmeta.SnapshotEvidenceRef) []*fsmetapb.SnapshotEvidenceRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]*fsmetapb.SnapshotEvidenceRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, &fsmetapb.SnapshotEvidenceRef{
			EpochId:       ref.EpochID,
			EvidenceRoot:  append([]byte(nil), ref.EvidenceRoot[:]...),
			PayloadDigest: append([]byte(nil), ref.PayloadDigest[:]...),
		})
	}
	return out
}

func snapshotEvidenceRefsFromProto(refs []*fsmetapb.SnapshotEvidenceRef) []fsmeta.SnapshotEvidenceRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]fsmeta.SnapshotEvidenceRef, 0, len(refs))
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		var parsed fsmeta.SnapshotEvidenceRef
		parsed.EpochID = ref.GetEpochId()
		copy(parsed.EvidenceRoot[:], ref.GetEvidenceRoot())
		copy(parsed.PayloadDigest[:], ref.GetPayloadDigest())
		out = append(out, parsed)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func quotaUsageRequestToProto(req fsmeta.QuotaUsageRequest) *fsmetapb.QuotaUsageRequest {
	return &fsmetapb.QuotaUsageRequest{
		Mount: string(req.Mount),
		Scope: uint64(req.Scope),
	}
}

func quotaUsageFromProto(resp *fsmetapb.QuotaUsageResponse) fsmeta.UsageRecord {
	if resp == nil {
		return fsmeta.UsageRecord{}
	}
	return fsmeta.UsageRecord{Bytes: resp.GetBytes(), Inodes: resp.GetInodes()}
}

func renameRequestToProto(req fsmeta.RenameRequest) *fsmetapb.RenameRequest {
	return &fsmetapb.RenameRequest{
		Mount:      string(req.Mount),
		FromParent: uint64(req.FromParent),
		FromName:   req.FromName,
		ToParent:   uint64(req.ToParent),
		ToName:     req.ToName,
	}
}

func renameSubtreeRequestToProto(req fsmeta.RenameSubtreeRequest) *fsmetapb.RenameSubtreeRequest {
	return &fsmetapb.RenameSubtreeRequest{
		Mount:      string(req.Mount),
		FromParent: uint64(req.FromParent),
		FromName:   req.FromName,
		ToParent:   uint64(req.ToParent),
		ToName:     req.ToName,
	}
}

func linkRequestToProto(req fsmeta.LinkRequest) *fsmetapb.LinkRequest {
	return &fsmetapb.LinkRequest{
		Mount:      string(req.Mount),
		FromParent: uint64(req.FromParent),
		FromName:   req.FromName,
		ToParent:   uint64(req.ToParent),
		ToName:     req.ToName,
	}
}

func unlinkRequestToProto(req fsmeta.UnlinkRequest) *fsmetapb.UnlinkRequest {
	return &fsmetapb.UnlinkRequest{
		Mount:  string(req.Mount),
		Parent: uint64(req.Parent),
		Name:   req.Name,
	}
}

func removeRequestToProto(req fsmeta.RemoveRequest) *fsmetapb.RemoveRequest {
	return &fsmetapb.RemoveRequest{
		Mount:  string(req.Mount),
		Parent: uint64(req.Parent),
		Name:   req.Name,
	}
}

func removeDirectoryRequestToProto(req fsmeta.RemoveDirectoryRequest) *fsmetapb.RemoveDirectoryRequest {
	return &fsmetapb.RemoveDirectoryRequest{
		Mount:  string(req.Mount),
		Parent: uint64(req.Parent),
		Name:   req.Name,
	}
}

func openWriteSessionRequestToProto(req fsmeta.OpenWriteSessionRequest) *fsmetapb.OpenWriteSessionRequest {
	return &fsmetapb.OpenWriteSessionRequest{
		Mount:   string(req.Mount),
		Inode:   uint64(req.Inode),
		Session: string(req.Session),
		TtlNs:   uint64(req.TTL),
	}
}

func heartbeatWriteSessionRequestToProto(req fsmeta.HeartbeatWriteSessionRequest) *fsmetapb.HeartbeatWriteSessionRequest {
	return &fsmetapb.HeartbeatWriteSessionRequest{
		Mount:   string(req.Mount),
		Inode:   uint64(req.Inode),
		Session: string(req.Session),
		TtlNs:   uint64(req.TTL),
	}
}

func closeWriteSessionRequestToProto(req fsmeta.CloseWriteSessionRequest) *fsmetapb.CloseWriteSessionRequest {
	return &fsmetapb.CloseWriteSessionRequest{
		Mount:   string(req.Mount),
		Session: string(req.Session),
		Inode:   uint64(req.Inode),
	}
}

func expireWriteSessionsRequestToProto(req fsmeta.ExpireWriteSessionsRequest) *fsmetapb.ExpireWriteSessionsRequest {
	return &fsmetapb.ExpireWriteSessionsRequest{
		Mount: string(req.Mount),
		Limit: req.Limit,
	}
}

func dentryFromProto(pb *fsmetapb.DentryRecord) fsmeta.DentryRecord {
	if pb == nil {
		return fsmeta.DentryRecord{}
	}
	return fsmeta.DentryRecord{
		Parent: fsmeta.InodeID(pb.GetParent()),
		Name:   pb.GetName(),
		Inode:  fsmeta.InodeID(pb.GetInode()),
		Type:   inodeTypeFromProto(pb.GetType()),
	}
}

func inodeFromProto(pb *fsmetapb.InodeRecord) fsmeta.InodeRecord {
	if pb == nil {
		return fsmeta.InodeRecord{}
	}
	return fsmeta.InodeRecord{
		Inode:         fsmeta.InodeID(pb.GetInode()),
		Type:          inodeTypeFromProto(pb.GetType()),
		Size:          pb.GetSize(),
		Mode:          pb.GetMode(),
		LinkCount:     pb.GetLinkCount(),
		ChildCount:    pb.GetChildCount(),
		CreatedUnixNs: pb.GetCreatedUnixNs(),
		UpdatedUnixNs: pb.GetUpdatedUnixNs(),
		OpaqueAttrs:   append([]byte(nil), pb.GetOpaqueAttrs()...),
	}
}

func sessionFromProto(pb *fsmetapb.SessionRecord) fsmeta.SessionRecord {
	if pb == nil {
		return fsmeta.SessionRecord{}
	}
	return fsmeta.SessionRecord{
		Session:       fsmeta.SessionID(pb.GetSession()),
		Inode:         fsmeta.InodeID(pb.GetInode()),
		ExpiresUnixNs: pb.GetExpiresUnixNs(),
	}
}

func pairFromProto(pb *fsmetapb.DentryAttrPair) fsmeta.DentryAttrPair {
	if pb == nil {
		return fsmeta.DentryAttrPair{}
	}
	return fsmeta.DentryAttrPair{
		Dentry: dentryFromProto(pb.GetDentry()),
		Inode:  inodeFromProto(pb.GetInode()),
	}
}

func watchRequestToProto(req fsmeta.WatchRequest) *fsmetapb.WatchSubtreeRequest {
	return &fsmetapb.WatchSubtreeRequest{
		Mount:              string(req.Mount),
		RootInode:          uint64(req.RootInode),
		KeyPrefix:          append([]byte(nil), req.KeyPrefix...),
		DescendRecursively: req.DescendRecursively,
		ResumeCursor:       watchCursorToProto(req.ResumeCursor),
		BackPressureWindow: req.BackPressureWindow,
	}
}

func watchCursorToProto(cursor fsmeta.WatchCursor) *fsmetapb.WatchCursor {
	return &fsmetapb.WatchCursor{
		RegionId: cursor.RegionID,
		Term:     cursor.Term,
		Index:    cursor.Index,
	}
}

func watchCursorFromProto(cursor *fsmetapb.WatchCursor) fsmeta.WatchCursor {
	if cursor == nil {
		return fsmeta.WatchCursor{}
	}
	return fsmeta.WatchCursor{
		RegionID: cursor.GetRegionId(),
		Term:     cursor.GetTerm(),
		Index:    cursor.GetIndex(),
	}
}

func watchEventFromProto(pb *fsmetapb.WatchEvent) fsmeta.WatchEvent {
	if pb == nil {
		return fsmeta.WatchEvent{}
	}
	return fsmeta.WatchEvent{
		Cursor:        watchCursorFromProto(pb.GetRaftCursor()),
		CommitVersion: pb.GetCommitVersion(),
		Source:        watchEventSourceFromProto(pb.GetSource()),
		Key:           append([]byte(nil), pb.GetKey()...),
	}
}

func watchEventSourceFromProto(source fsmetapb.WatchEventSource) fsmeta.WatchEventSource {
	switch source {
	case fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_COMMIT:
		return fsmeta.WatchEventSourceCommit
	case fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_RESOLVE_LOCK:
		return fsmeta.WatchEventSourceResolveLock
	case fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_RUNTIME_VISIBLE:
		return fsmeta.WatchEventSourceRuntimeVisible
	default:
		return 0
	}
}

func inodeTypeFromProto(typ fsmetapb.InodeType) fsmeta.InodeType {
	switch typ {
	case fsmetapb.InodeType_INODE_TYPE_FILE:
		return fsmeta.InodeTypeFile
	case fsmetapb.InodeType_INODE_TYPE_DIRECTORY:
		return fsmeta.InodeTypeDirectory
	default:
		return ""
	}
}

func inodeTypeToProto(typ fsmeta.InodeType) fsmetapb.InodeType {
	switch typ {
	case fsmeta.InodeTypeFile:
		return fsmetapb.InodeType_INODE_TYPE_FILE
	case fsmeta.InodeTypeDirectory:
		return fsmetapb.InodeType_INODE_TYPE_DIRECTORY
	default:
		return fsmetapb.InodeType_INODE_TYPE_UNSPECIFIED
	}
}
