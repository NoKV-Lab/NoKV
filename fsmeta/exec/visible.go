// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

// VisibleOperationID is the runtime-neutral idempotency key for one visible
// metadata operation.
type VisibleOperationID struct {
	ClientID string
	Seq      uint64
}

func (id VisibleOperationID) Valid() bool {
	return id.ClientID != "" && id.Seq != 0
}

// VisibleAck is returned once a runtime has made an operation visible at its
// own visible boundary.
type VisibleAck struct {
	EpochID  uint64
	OpID     VisibleOperationID
	HolderID string
}

type VisibleAdmissionResult struct {
	PredicateProofs []proof.PredicateProof
	GuardProofs     []proof.GuardProof
}

type VisibleAdmissionContext struct {
	ProofFrontier proof.ProofFrontier
}

type VisibleAdmissionFunc func(context.Context, compile.MaterializedOp, VisibleAdmissionContext) (VisibleAdmissionResult, bool, error)

// VisibleOverlayKV is one visible overlay row owned by a runtime. Delete=true
// means the overlay hides a base row.
type VisibleOverlayKV struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// VisibleAuthorityAdmitter is the fsmeta holder-side authority boundary. It is
// intentionally narrower than the root protocol: the executor only asks whether
// a compiled authority scope is locally owned before it can enter the visible
// commit path.
type VisibleAuthorityAdmitter interface {
	AcquireVisibleAuthority(context.Context, compile.AuthorityScope) (owned bool, err error)
}

// VisibleAuthorityRetirer retires authority after pending visible operations
// for the authority have reached the required durability boundary.
type VisibleAuthorityRetirer interface {
	RetireVisibleAuthority(context.Context, ...compile.AuthorityScope) error
}

// VisibleCommitter is the opt-in visible commit boundary. Success replaces the
// ordinary Percolator/Raft commit for this fsmeta operation, so errors are
// returned and never silently fall back after the runtime overlay may already
// include the operation.
type VisibleCommitter interface {
	SubmitVisible(context.Context, VisibleOperationID, compile.MaterializedOp, VisibleAdmissionFunc) (VisibleAck, error)
}

type VisibleOverlayReader interface {
	GetVisibleOverlay(key []byte) (value []byte, deleted bool, ok bool)
	// GetVisibleOverlayView returns overlay-owned bytes. Callers must not mutate
	// the returned value.
	GetVisibleOverlayView(key []byte) (value []byte, deleted bool, ok bool)
	ScanVisibleOverlay(start []byte, limit uint32) []VisibleOverlayKV
}

type VisibleOverlayReadSnapshotReader interface {
	CaptureVisibleOverlayRead() (overlayGeneration, sealedGeneration uint64)
	GetVisibleOverlayViewAt(overlayGeneration, sealedGeneration uint64, key []byte) (value []byte, deleted bool, ok bool)
	ScanVisibleDirectoryAt(overlayGeneration, sealedGeneration uint64, prefix, start []byte, limit uint32) []VisibleOverlayKV
}

type VisibleDirectoryOverlayReader interface {
	ScanVisibleDirectory(prefix, start []byte, limit uint32) []VisibleOverlayKV
}

type VisibleDirectoryOverlayPresence interface {
	HasVisibleDirectoryOverlay(prefix []byte) bool
}

type PendingVisibleDirectoryPresence interface {
	HasPendingVisibleDirectory(prefix []byte) bool
}

type VisibleDirectoryCacheFrontier interface {
	VisibleDirectoryCacheFrontier(prefix []byte) uint64
}

// InstalledVisibleSnapshotCapturer records the installed runtime overlay
// visible at an MVCC snapshot token so later snapshot reads do not consult live
// overlay state.
type InstalledVisibleSnapshotCapturer interface {
	CaptureInstalledVisibleSnapshot(version uint64) error
}

// VisibleSnapshotCapturer lets a runtime capture visible state without forcing
// an authority flush. Runtimes return captured=false when the snapshot cannot be
// made durable at the visible boundary.
type VisibleSnapshotCapturer interface {
	CaptureVisibleSnapshot(context.Context, uint64, compile.AuthorityScope) (model.VisibleSnapshotCapture, bool, error)
}

// VisibleSnapshotOverlayReader serves a captured runtime overlay for a snapshot
// version. It is intentionally separate from the live overlay reader.
type VisibleSnapshotOverlayReader interface {
	GetVisibleSnapshotOverlayView(version uint64, key []byte) (value []byte, deleted bool, ok bool)
	ScanVisibleSnapshotDirectory(version uint64, prefix, start []byte, limit uint32) []VisibleOverlayKV
	HasVisibleSnapshotDirectory(version uint64, prefix []byte) bool
}

type visibleSnapshotRetirer interface {
	RetireVisibleSnapshot(version uint64)
}

type VisibleFlusher interface {
	FlushDurable(context.Context) error
}

type VisibleAuthorityFlusher interface {
	FlushAuthority(context.Context, compile.AuthorityScope) error
}

type VisibleAuthorityDrainer interface {
	DrainVisibleAuthority(context.Context, VisibleAuthorityRetirer, ...compile.AuthorityScope) error
}

type VisiblePredicateIndex interface {
	KeyState(key []byte) (present bool, known bool)
	DirectoryEmpty(mount model.MountIdentity, inode model.InodeID) bool
	DirectoryBaseEmpty(mount model.MountIdentity, inode model.InodeID) bool
	SessionNamespaceEmpty(mount model.MountIdentity, inode model.InodeID) bool
	RememberKey(key []byte, present bool)
	RememberEmptyDirectory(mount model.MountIdentity, inode model.InodeID)
	RememberEmptySessionNamespace(mount model.MountIdentity, inode model.InodeID)
}
