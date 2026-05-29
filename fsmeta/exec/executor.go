// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

const (
	// Percolator TTLs are encoded in milliseconds. fsmeta mutations cross the
	// coordinator TSO path plus raft apply queues, so the default must cover
	// short commit stalls instead of letting read-side lock resolution roll
	// back a live metadata transaction.
	defaultLockTTL uint64 = uint64(30 * time.Second / time.Millisecond)

	// Non-lock conflicts are retried by count because fresh timestamps normally
	// make progress immediately. Live locks are bounded separately by the lock
	// TTL so fsmeta does not leak ordinary Percolator lock waits to callers.
	maxTxnContentionRetries  = 32
	maxReadContentionRetries = 32

	txnContentionRetryBaseBackoff = time.Millisecond
	txnContentionRetryMaxBackoff  = 100 * time.Millisecond
	maxTxnLockRetryBudget         = time.Hour
)

// InodeAllocator assigns Create inode IDs. The executor allocates once before
// transaction retry so a retry cannot publish a different inode for the same
// logical Create after a conflict or ambiguous transport error.
type InodeAllocator interface {
	AllocateCreateInode(ctx context.Context, mount model.MountIdentity, parent model.InodeID, name string) (model.InodeID, error)
}

// MountAdmission is the executor's mount-admission view.
type MountAdmission struct {
	MountID       model.MountID
	MountKeyID    model.MountKeyID
	RootInode     model.InodeID
	SchemaVersion uint32
	Retired       bool
}

func (m MountAdmission) Identity() model.MountIdentity {
	return model.MountIdentity{MountID: m.MountID, MountKeyID: m.MountKeyID}
}

// MountResolver checks rooted mount lifecycle before mutating fsmeta data.
type MountResolver interface {
	ResolveMount(context.Context, model.MountID) (MountAdmission, error)
}

// SubtreeHandoffPublisher publishes rooted subtree authority handoff events for
// successful authority-aware namespace mutations.
type SubtreeHandoffPublisher interface {
	StartSubtreeHandoff(context.Context, model.MountID, model.InodeID, uint64) error
	CompleteSubtreeHandoff(context.Context, model.MountID, model.InodeID, uint64) error
}

// SubtreeAuthorityResolver decides whether an ordinary data-plane rename stays
// inside one rooted authority. Cross-authority moves must use RenameSubtree so
// root can advance authority eras explicitly.
type SubtreeAuthorityResolver interface {
	SameAuthority(context.Context, model.MountID, model.InodeID, model.InodeID) (bool, error)
}

type VisibleQuotaAdmitter interface {
	AllowVisibleQuota(context.Context, []QuotaChange) (bool, error)
}

// Executor interprets fsmeta operation plans against a backend.Store.
type Executor struct {
	runner                  backend.Store
	inodes                  InodeAllocator
	mounts                  MountResolver
	quotas                  QuotaResolver
	subtrees                SubtreeHandoffPublisher
	authorities             SubtreeAuthorityResolver
	visibleAuthority        VisibleAuthorityAdmitter
	visibleCommitter        VisibleCommitter
	visibleClientID         string
	lockTTL                 uint64
	now                     func() time.Time
	readRetriesTotal        atomic.Uint64
	readRetryExhaustedTotal atomic.Uint64
	txnRetriesTotal         atomic.Uint64
	txnRetryExhaustedTotal  atomic.Uint64
	createTotal             atomic.Uint64
	visibleAdmission        visibleAdmissionCounters
	visibleCommit           visibleCommitCounters
	visibleDirectoryRead    visibleDirectoryReadCounters
	visibleSeq              atomic.Uint64
	atomicOnePhase          map[model.OperationKind]*atomicOnePhaseCounters
}

// Option configures an Executor.
type Option func(*Executor)

// WithLockTTL overrides the Percolator lock TTL used by mutating operations.
func WithLockTTL(ttl uint64) Option {
	return func(e *Executor) {
		if ttl > 0 {
			e.lockTTL = ttl
		}
	}
}

// WithClock overrides the wall clock used for write-session expiry.
func WithClock(now func() time.Time) Option {
	return func(e *Executor) {
		if now != nil {
			e.now = now
		}
	}
}

// WithMountResolver enables rooted mount lifecycle admission for mutating
// fsmeta operations.
func WithMountResolver(resolver MountResolver) Option {
	return func(e *Executor) {
		e.mounts = resolver
	}
}

// WithQuotaResolver enables rooted quota-fence admission for resource-creating
// fsmeta operations.
func WithQuotaResolver(resolver QuotaResolver) Option {
	return func(e *Executor) {
		e.quotas = resolver
	}
}

// WithInodeAllocator enables server-side inode assignment for Create.
func WithInodeAllocator(allocator InodeAllocator) Option {
	return func(e *Executor) {
		e.inodes = allocator
	}
}

// WithSubtreeHandoffPublisher enables rooted subtree authority era advancement
// for RenameSubtree.
func WithSubtreeHandoffPublisher(publisher SubtreeHandoffPublisher) Option {
	return func(e *Executor) {
		e.subtrees = publisher
	}
}

// WithSubtreeAuthorityResolver enables admission for ordinary Rename. Without
// a resolver, the executor uses the current single-authority mount model.
func WithSubtreeAuthorityResolver(resolver SubtreeAuthorityResolver) Option {
	return func(e *Executor) {
		e.authorities = resolver
	}
}

// WithVisibleAuthorityAdmitter enables holder-authority admission for
// visible-commit-eligible mutations.
func WithVisibleAuthorityAdmitter(admitter VisibleAuthorityAdmitter) Option {
	return func(e *Executor) {
		e.visibleAuthority = admitter
	}
}

// WithVisibleCommitter enables visible commits. This option is intentionally
// explicit so production callers choose the visible-commit contract.
func WithVisibleCommitter(committer VisibleCommitter) Option {
	return func(e *Executor) {
		e.visibleCommitter = committer
	}
}

// New constructs an fsmeta executor.
func New(runner backend.Store, opts ...Option) (*Executor, error) {
	if runner == nil {
		return nil, errRunnerRequired
	}
	executor := &Executor{
		runner:          runner,
		lockTTL:         defaultLockTTL,
		visibleClientID: newVisibleClientID(),
		atomicOnePhase:  newAtomicOnePhaseCounters(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(executor)
		}
	}
	return executor, nil
}
