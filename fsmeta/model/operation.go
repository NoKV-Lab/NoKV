// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package model

import "time"

// OperationKind identifies one metadata operation contract.
type OperationKind string

const (
	OperationCreate           OperationKind = "create"
	OperationUpdateInode      OperationKind = "update_inode"
	OperationLookup           OperationKind = "lookup"
	OperationGetAttr          OperationKind = "getattr"
	OperationReadDir          OperationKind = "readdir"
	OperationReadSession      OperationKind = "read_session"
	OperationSnapshotSubtree  OperationKind = "snapshot_subtree"
	OperationRename           OperationKind = "rename"
	OperationRenameReplace    OperationKind = "rename_replace"
	OperationRenameSubtree    OperationKind = "rename_subtree"
	OperationLink             OperationKind = "link"
	OperationUnlink           OperationKind = "unlink"
	OperationRemove           OperationKind = "remove"
	OperationRemoveDirectory  OperationKind = "remove_directory"
	OperationOpenWriteSession OperationKind = "open_write_session"
	OperationHeartbeatSession OperationKind = "heartbeat_write_session"
	OperationCloseSession     OperationKind = "close_write_session"
	OperationExpireSessions   OperationKind = "expire_write_sessions"
)

type CreateRequest struct {
	Mount  MountID
	Parent InodeID
	Name   string
	Attrs  CreateAttrs
}

type CreateAttrs struct {
	Type          InodeType
	Size          uint64
	Mode          uint32
	CreatedUnixNs int64
	UpdatedUnixNs int64
	OpaqueAttrs   []byte
}

// InodeRecord materializes create-only attributes into the stored inode value.
// Create owns LinkCount and inode identity, so callers cannot smuggle them
// through CreateAttrs.
func (attrs CreateAttrs) InodeRecord(inode InodeID) InodeRecord {
	return InodeRecord{
		Inode:         inode,
		Type:          attrs.Type,
		Size:          attrs.Size,
		Mode:          attrs.Mode,
		LinkCount:     1,
		CreatedUnixNs: attrs.CreatedUnixNs,
		UpdatedUnixNs: attrs.UpdatedUnixNs,
		OpaqueAttrs:   append([]byte(nil), attrs.OpaqueAttrs...),
	}
}

type CreateResult struct {
	Dentry DentryRecord
	Inode  InodeRecord
}

type UpdateInodeRequest struct {
	Mount            MountID
	Parent           InodeID
	Inode            InodeID
	Name             string
	SetSize          bool
	Size             uint64
	SetMode          bool
	Mode             uint32
	SetUpdatedUnixNs bool
	UpdatedUnixNs    int64
	SetOpaqueAttrs   bool
	OpaqueAttrs      []byte
}

type LookupRequest struct {
	Mount  MountID
	Parent InodeID
	Name   string
}

type ReadDirRequest struct {
	Mount           MountID
	Parent          InodeID
	StartAfter      string
	Limit           uint32
	SnapshotVersion uint64
}

type SnapshotSubtreeRequest struct {
	Mount     MountID
	RootInode InodeID
}

type RenameRequest struct {
	Mount      MountID
	FromParent InodeID
	FromName   string
	ToParent   InodeID
	ToName     string
}

type RenameReplaceRequest struct {
	Mount      MountID
	FromParent InodeID
	FromName   string
	ToParent   InodeID
	ToName     string
}

type RenameReplaceResult struct {
	Replaced        bool
	OldDentry       DentryRecord
	OldInode        InodeRecord
	OldInodeDeleted bool
}

type RenameSubtreeRequest struct {
	Mount      MountID
	FromParent InodeID
	FromName   string
	ToParent   InodeID
	ToName     string
}

type LinkRequest struct {
	Mount      MountID
	FromParent InodeID
	FromName   string
	ToParent   InodeID
	ToName     string
}

type UnlinkRequest struct {
	Mount  MountID
	Parent InodeID
	Name   string
}

type RemoveRequest struct {
	Mount  MountID
	Parent InodeID
	Name   string
}

// RemoveResult reports the namespace entry detached by Remove. OldInode is the
// inode record observed before link-count decrement or deletion; InodeDeleted
// tells callers whether the inode record itself was removed.
type RemoveResult struct {
	RemovedDentry DentryRecord
	OldInode      InodeRecord
	InodeDeleted  bool
}

type RemoveDirectoryRequest struct {
	Mount  MountID
	Parent InodeID
	Name   string
}

type OpenWriteSessionRequest struct {
	Mount   MountID
	Inode   InodeID
	Session SessionID
	// TTL is a requested lease duration. The executor derives the persisted
	// absolute expiry from its own clock inside the successful transaction
	// attempt, so caller clock skew and queueing delay cannot shorten a lease.
	TTL time.Duration
}

type HeartbeatWriteSessionRequest struct {
	Mount   MountID
	Inode   InodeID
	Session SessionID
	// TTL is a requested extension duration; SessionRecord.ExpiresUnixNs is the
	// server-issued absolute expiry returned after commit.
	TTL time.Duration
}

type CloseWriteSessionRequest struct {
	Mount   MountID
	Inode   InodeID
	Session SessionID
}

type ExpireWriteSessionsRequest struct {
	Mount MountID
	Limit uint32
}

type ExpireWriteSessionsResult struct {
	Expired uint64
}
