// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package model

// MountID identifies one filesystem namespace hosted inside fsmeta.
type MountID string

// MountKeyID is the compact rooted storage identity for one mount. Public
// APIs keep using MountID; fsmeta storage keys use MountKeyID so every key does
// not repeat the human-readable mount name.
type MountKeyID uint64

// MountIdentity is the boundary object returned by rooted mount admission.
// Code that encodes storage keys must use this identity instead of trusting a
// caller-supplied string mount name.
type MountIdentity struct {
	MountID    MountID
	MountKeyID MountKeyID
}

// InodeID identifies one inode. ID 0 is reserved as invalid.
type InodeID uint64

// ChunkIndex identifies one logical file chunk.
type ChunkIndex uint64

// SessionID identifies one client/session lease record.
type SessionID string

// InodeType describes the user-visible inode kind tracked by fsmeta.
type InodeType string

const (
	InodeTypeFile      InodeType = "file"
	InodeTypeDirectory InodeType = "directory"
)

// InodeRecord is the value stored under an inode key.
type InodeRecord struct {
	Inode         InodeID   `json:"inode"`
	Type          InodeType `json:"type,omitempty"`
	Size          uint64    `json:"size,omitempty"`
	Mode          uint32    `json:"mode,omitempty"`
	LinkCount     uint32    `json:"link_count,omitempty"`
	ChildCount    uint64    `json:"child_count,omitempty"`
	CreatedUnixNs int64     `json:"created_unix_ns,omitempty"`
	UpdatedUnixNs int64     `json:"updated_unix_ns,omitempty"`
	OpaqueAttrs   []byte    `json:"opaque_attrs,omitempty"`
}

// DentryRecord is the value stored under a parent/name dentry key.
type DentryRecord struct {
	Parent InodeID   `json:"parent"`
	Name   string    `json:"name"`
	Inode  InodeID   `json:"inode"`
	Type   InodeType `json:"type,omitempty"`
}

// SessionRecord is the value stored under a writer/session key.
type SessionRecord struct {
	Session       SessionID `json:"session"`
	Inode         InodeID   `json:"inode"`
	ExpiresUnixNs int64     `json:"expires_unix_ns,omitempty"`
}

// UsageRecord is the value stored under quota usage counter keys.
type UsageRecord struct {
	Bytes  uint64 `json:"bytes,omitempty"`
	Inodes uint64 `json:"inodes,omitempty"`
}

// DentryAttrPair is the fused result returned by ReadDirPlus-style operations.
type DentryAttrPair struct {
	Dentry DentryRecord
	Inode  InodeRecord
}

// SnapshotEvidenceRef identifies one runtime-visible artifact retained by a
// SnapshotSubtreeToken. The payload bytes are intentionally not part of the
// public token; another gateway can use the runtime evidence to prove that the
// visible snapshot frontier is still recoverable.
type SnapshotEvidenceRef struct {
	EpochID       uint64
	EvidenceRoot  [32]byte
	PayloadDigest [32]byte
}

// VisibleSnapshotCapture is the runtime-internal evidence returned when a
// visible snapshot has been made durable without forcing immediate install.
type VisibleSnapshotCapture struct {
	Evidence []SnapshotEvidenceRef
}

// Valid reports whether ref can address durable runtime snapshot evidence.
func (r SnapshotEvidenceRef) Valid() bool {
	return r.EpochID != 0 &&
		r.EvidenceRoot != ([32]byte{}) &&
		r.PayloadDigest != ([32]byte{})
}

// ReadVersionRequest asks for an ephemeral MVCC read version. It provides a
// consistent read timestamp only; it does not publish a snapshot epoch or pin
// GC state.
type ReadVersionRequest struct {
	Mount MountID
}

// SnapshotSubtreeToken identifies a durable subtree snapshot epoch. The token
// is published into rooted truth by the fsmeta service boundary and must be
// retired by callers when its GC-retention contract is no longer needed.
type SnapshotSubtreeToken struct {
	Mount           MountID
	MountKeyID      MountKeyID
	RootInode       InodeID
	ReadVersion     uint64
	RuntimeEvidence []SnapshotEvidenceRef
}

// Clone returns a detached snapshot token.
func (t SnapshotSubtreeToken) Clone() SnapshotSubtreeToken {
	t.RuntimeEvidence = cloneSnapshotEvidenceRefs(t.RuntimeEvidence)
	return t
}

func cloneSnapshotEvidenceRefs(refs []SnapshotEvidenceRef) []SnapshotEvidenceRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]SnapshotEvidenceRef, len(refs))
	copy(out, refs)
	return out
}

// QuotaUsageRequest addresses one usage counter. Scope 0 is mount-wide;
// non-zero scopes are direct quota accounting roots.
type QuotaUsageRequest struct {
	Mount MountID
	Scope InodeID
}

const (
	// RootInode is the conventional root inode for one mount.
	RootInode InodeID = 1

	// MaxInodeOpaqueAttrsBytes bounds the application-owned inode payload.
	// It is for compact body references and custom attributes, not object bodies.
	MaxInodeOpaqueAttrsBytes = 16 * 1024

	// DefaultReadDirLimit bounds one ReadDir planning request when callers do not
	// supply an explicit page size.
	DefaultReadDirLimit uint32 = 1024

	// MaxReadDirLimit keeps one directory page bounded before fsmeta grows a
	// streaming API.
	MaxReadDirLimit uint32 = 16 * 1024

	// DefaultSessionExpireLimit bounds one stale-session cleanup pass when the
	// caller does not provide an explicit page size.
	DefaultSessionExpireLimit uint32 = 1024

	// MaxSessionExpireLimit keeps one session cleanup pass bounded.
	MaxSessionExpireLimit uint32 = 16 * 1024
)
