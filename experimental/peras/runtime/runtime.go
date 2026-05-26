// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/utils"
)

const (
	defaultPerasSegmentWitnessRetries      = 3
	defaultPerasSegmentWitnessRetryBackoff = 20 * time.Millisecond
	defaultPerasSegmentBatchSize           = 512
	defaultPerasSegmentMaxReplayOperations = 512
	defaultPerasSegmentMaxReplayMutations  = 4096
	defaultPerasSegmentMaxPayloadBytes     = 512 << 10
	defaultPerasSegmentCatalogRouteBudget  = 4
	defaultSegmentWitnessBatchMaxBytes     = 32 << 20
	defaultPerasSegmentCatalogScanLimit    = 128
	// A materialized drain expands each replay mutation into MVCC records.
	// Keep the request below the local write-batch entry cap while preserving
	// large catalog-only segments on the common path.
	defaultPerasMaterializeMaxReplayMutations = 20
	defaultPerasSegmentFlushEvery             = 20 * time.Millisecond
	defaultPerasBackgroundFlushTimeout        = 30 * time.Second
	defaultPerasBackgroundErrorBackoff        = time.Second
	defaultPerasRootRecoveryInstallTimeout    = 2 * time.Second
	defaultPerasSegmentInstallRetries         = 24
	defaultPerasSegmentInstallRetryBackoff    = 10 * time.Millisecond
	defaultPerasSegmentInstallMaxBackoff      = 500 * time.Millisecond
	defaultPerasSegmentInstallStaleBackoff    = time.Millisecond
	defaultPerasSegmentInstallStaleMaxBackoff = 10 * time.Millisecond
	defaultPerasVisibleWatchQueue             = 65536
)

type Config struct {
	Authority                  GrantProvider
	Witnesses                  []fsperas.WitnessReplica
	Installer                  SegmentInstaller
	CatalogScanner             SegmentCatalogScanner
	WatchPublisher             perasWatchPublisher
	VisibleLog                 fsperas.VisibleLog
	Quorum                     int
	SegmentWitnessRetries      int
	SegmentWitnessRetryBackoff time.Duration
	SegmentBatchSize           int
	AdmissionPendingLimit      int
	SegmentMaxReplayOperations int
	SegmentMaxReplayMutations  int
	// MaterializeMaxReplayMutations bounds the per-segment mutation count
	// when the install chain materializes segments into base MVCC. Zero falls
	// back to
	// defaultPerasMaterializeMaxReplayMutations, which is intentionally
	// small (the distributed install path serializes each materialize
	// install as one raft commit and prefers many small commits). Local
	// runtimes that materialize into a single-process base MVCC can set
	// this to SegmentMaxReplayMutations to avoid fragmenting visible work
	// into tiny segments.
	MaterializeMaxReplayMutations int
	SegmentMaxPayloadBytes        uint64
	SegmentCatalogRouteBudget     int
	SegmentFlushEvery             time.Duration
	BackgroundFlushTimeout        time.Duration
	BackgroundErrorBackoff        time.Duration
	SegmentInstallParallelism     int
	SegmentFlushParallelism       int
	// VisibleSnapshotCapture lets SnapshotSubtree capture the holder-local
	// visible overlay without first flushing an authority. This is only safe
	// for runtimes whose visible WAL is an accepted durability boundary.
	VisibleSnapshotCapture bool
	// QuorumVisibleSnapshotCapture lets SnapshotSubtree capture a visible
	// snapshot after pending operations in the snapshot authority scope have
	// reached segment witness quorum. The actual segment install remains
	// asynchronous.
	QuorumVisibleSnapshotCapture bool
	Now                          func() time.Time
}

// Runtime is the fsmeta runtime bridge from compiler deltas to visible overlay
// commits and segment install. Distributed runtimes require witness quorum
// evidence; local runtimes may bypass that stage when no rooted seal can be
// published.
type Runtime struct {
	authority              GrantProvider
	seals                  SealProvider
	witnesses              []fsperas.WitnessReplica
	witness                *witnessSignLayer
	installer              SegmentInstaller
	finalizer              SegmentFinalizer
	catalog                SegmentCatalogScanner
	watch                  perasWatchPublisher
	visibleLog             fsperas.VisibleLog
	quorum                 int
	retries                int
	backoff                time.Duration
	batchSize              int
	admitLimit             int
	maxOps                 int
	maxReplay              int
	materializeMaxReplay   int
	maxPayload             uint64
	routeBudget            int
	installN               int
	flushN                 int
	materialize            bool
	visibleSnapshots       bool
	quorumVisibleSnapshots bool
	flushEvery             time.Duration
	bgTimeout              time.Duration
	bgBackoff              time.Duration
	now                    func() time.Time

	commitMu   sync.RWMutex
	flushMu    sync.Mutex
	bgLaunchMu sync.Mutex
	bgRunning  atomic.Bool
	bgNext     atomic.Int64
	visibleSeq atomic.Uint64
	witnessSeq atomic.Int64
	closed     atomic.Bool
	closer     *utils.Closer
	flushTask  *utils.PeriodicTask
	watchQueue *utils.MPSCQueue[observe.WatchEvent]
	installQ   *perasInstallLane
	sealQ      *visibleSealLane

	// Lock order for multi-lock paths:
	// commitMu -> flushMu -> drainMu -> admissionMu -> epochTable.mu -> readState.mu -> runtimeMetrics.statsMu.
	// Lane workers communicate through queues and must not take commitMu.
	drainMu     sync.Mutex
	drainCond   *sync.Cond
	drainNextID uint64
	drainUses   []visibleAuthorityUse
	drainScopes []compile.AuthorityScope

	admissionMu   sync.Mutex
	admissionCond *sync.Cond

	epochs  *epochTable
	latches *fsperas.AdmissionLatches

	read *readState

	metrics runtimeMetrics
}

type perasCompletion struct {
	epochID    uint64
	completion fsperas.SegmentCompletion
}

type perasFlushJob struct {
	scope       compile.AuthorityScope
	plan        fsperas.ReplayPlan
	segment     fsperas.PerasSegment
	payload     []byte
	digest      [32]byte
	install     compile.InstallPlan
	materialize bool
	cursor      InstallCursor
}

type perasFlushBatch struct {
	holder          *fsperas.Holder
	scope           compile.AuthorityScope
	plan            fsperas.ReplayPlan
	jobs            []perasFlushJob
	witnessUnixNano int64
	publishDecision publishDecision
	publishErr      error
}

type perasFrozenPlan struct {
	holder *fsperas.Holder
	scope  compile.AuthorityScope
	plan   fsperas.ReplayPlan
}

func configureSegmentWitnesses(cfg Config) ([]fsperas.WitnessReplica, int, error) {
	if len(cfg.Witnesses) == 0 {
		if cfg.Quorum != 0 {
			return nil, 0, ErrRuntimeInvalid
		}
		if _, ok := cfg.Authority.(SealPublisher); ok {
			return nil, 0, ErrRuntimeInvalid
		}
		if _, ok := cfg.Authority.(SealProvider); ok {
			return nil, 0, ErrRuntimeInvalid
		}
		return nil, 0, nil
	}
	witnesses := make([]fsperas.WitnessReplica, 0, len(cfg.Witnesses))
	seen := make(map[string]struct{}, len(cfg.Witnesses))
	for _, witness := range cfg.Witnesses {
		if witness == nil || witness.ID() == "" {
			return nil, 0, ErrRuntimeInvalid
		}
		if _, ok := seen[witness.ID()]; ok {
			return nil, 0, ErrRuntimeInvalid
		}
		seen[witness.ID()] = struct{}{}
		witnesses = append(witnesses, witness)
	}
	quorum := cfg.Quorum
	if quorum == 0 {
		quorum = len(witnesses)/2 + 1
	}
	if quorum <= 0 || quorum > len(witnesses) {
		return nil, 0, ErrRuntimeInvalid
	}
	return witnesses, quorum, nil
}

func (c *Runtime) usesSegmentWitness() bool {
	return c != nil && c.witness != nil
}

func NewRuntime(cfg Config) (*Runtime, error) {
	if cfg.Authority == nil || cfg.Authority.HolderID() == "" {
		return nil, ErrRuntimeInvalid
	}
	witnesses, quorum, err := configureSegmentWitnesses(cfg)
	if err != nil {
		return nil, err
	}
	retries := cfg.SegmentWitnessRetries
	if retries == 0 {
		retries = defaultPerasSegmentWitnessRetries
	}
	if retries < 0 {
		return nil, ErrRuntimeInvalid
	}
	backoff := cfg.SegmentWitnessRetryBackoff
	if backoff == 0 {
		backoff = defaultPerasSegmentWitnessRetryBackoff
	}
	if backoff < 0 {
		return nil, ErrRuntimeInvalid
	}
	batchSize := cfg.SegmentBatchSize
	if batchSize == 0 {
		batchSize = defaultPerasSegmentBatchSize
	}
	if batchSize < 0 {
		return nil, ErrRuntimeInvalid
	}
	admitLimit := cfg.AdmissionPendingLimit
	if admitLimit == 0 {
		admitLimit = defaultPerasAdmissionPendingLimit(batchSize, cfg.SegmentMaxReplayOperations, cfg.SegmentInstallParallelism)
	}
	if admitLimit < 0 {
		return nil, ErrRuntimeInvalid
	}
	maxOps := cfg.SegmentMaxReplayOperations
	if maxOps == 0 {
		maxOps = defaultPerasSegmentMaxReplayOperations
	}
	if maxOps < 0 {
		return nil, ErrRuntimeInvalid
	}
	maxReplay := cfg.SegmentMaxReplayMutations
	if maxReplay == 0 {
		maxReplay = defaultPerasSegmentMaxReplayMutations
	}
	if maxReplay < 0 {
		return nil, ErrRuntimeInvalid
	}
	materializeMaxReplay := cfg.MaterializeMaxReplayMutations
	if materializeMaxReplay == 0 {
		materializeMaxReplay = defaultPerasMaterializeMaxReplayMutations
	}
	if materializeMaxReplay < 0 {
		return nil, ErrRuntimeInvalid
	}
	maxPayload := cfg.SegmentMaxPayloadBytes
	if maxPayload == 0 {
		maxPayload = defaultPerasSegmentMaxPayloadBytes
	}
	routeBudget := cfg.SegmentCatalogRouteBudget
	if routeBudget == 0 {
		routeBudget = defaultPerasSegmentCatalogRouteBudget
	}
	if routeBudget < 0 {
		return nil, ErrRuntimeInvalid
	}
	installN := cfg.SegmentInstallParallelism
	if installN == 0 {
		installN = defaultPerasSegmentInstallParallelism()
	}
	if installN < 0 {
		return nil, ErrRuntimeInvalid
	}
	flushN := cfg.SegmentFlushParallelism
	if flushN == 0 {
		flushN = installN
	}
	if flushN < 0 {
		return nil, ErrRuntimeInvalid
	}
	if flushN == 0 {
		flushN = 1
	}
	flushEvery := cfg.SegmentFlushEvery
	if flushEvery == 0 {
		flushEvery = defaultPerasSegmentFlushEvery
	}
	if flushEvery < 0 {
		return nil, ErrRuntimeInvalid
	}
	bgTimeout := cfg.BackgroundFlushTimeout
	if bgTimeout == 0 {
		bgTimeout = defaultPerasBackgroundFlushTimeout
	}
	if bgTimeout < 0 {
		return nil, ErrRuntimeInvalid
	}
	bgBackoff := cfg.BackgroundErrorBackoff
	if bgBackoff == 0 {
		bgBackoff = defaultPerasBackgroundErrorBackoff
	}
	if bgBackoff < 0 {
		return nil, ErrRuntimeInvalid
	}
	materialize := segmentInstallerMaterializes(cfg.Installer)
	if materialize && cfg.CatalogScanner == nil && cfg.VisibleLog != nil {
		if _, ok := cfg.VisibleLog.(visibleLogStateReplayer); !ok {
			return nil, ErrRuntimeInvalid
		}
		if _, ok := cfg.VisibleLog.(visibleReplayPlanApplier); !ok {
			return nil, ErrRuntimeInvalid
		}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	seals, _ := cfg.Authority.(SealProvider)
	// cfg.Installer holds the caller-supplied durability layer(s). The
	// completion-index layer is appended below once c.read exists; the
	// rest of the chain composition (sealed tracking, catalog, witness)
	// is the responsibility of future phases.
	c := &Runtime{
		authority:              cfg.Authority,
		seals:                  seals,
		witnesses:              witnesses,
		installer:              cfg.Installer,
		catalog:                cfg.CatalogScanner,
		watch:                  cfg.WatchPublisher,
		visibleLog:             cfg.VisibleLog,
		quorum:                 quorum,
		retries:                retries,
		backoff:                backoff,
		batchSize:              batchSize,
		admitLimit:             admitLimit,
		maxOps:                 maxOps,
		maxReplay:              maxReplay,
		materializeMaxReplay:   materializeMaxReplay,
		maxPayload:             maxPayload,
		routeBudget:            routeBudget,
		installN:               installN,
		flushN:                 flushN,
		materialize:            materialize,
		visibleSnapshots:       cfg.VisibleSnapshotCapture,
		quorumVisibleSnapshots: cfg.QuorumVisibleSnapshotCapture,
		flushEvery:             flushEvery,
		bgTimeout:              bgTimeout,
		bgBackoff:              bgBackoff,
		now:                    now,
		closer:                 utils.NewCloser(),
		epochs:                 newEpochTable(),
		latches:                fsperas.NewAdmissionLatches(),
		read:                   newReadState(),
	}
	c.witness = newWitnessSignLayer(c)
	c.drainCond = sync.NewCond(&c.drainMu)
	c.admissionCond = sync.NewCond(&c.admissionMu)
	// Phase 3 composition: the caller-supplied installer owns durable
	// segment evidence. Read-view work runs later through finalizer after
	// publish/seal or the local materialized write is safe to observe.
	if c.installer != nil {
		c.finalizer = NewFinalizeChain(newSealedTrackingLayer(c), newCompletionIndexLayer(c.read))
		c.installQ = newPerasInstallLane(c, c.installN)
	}
	if _, ok := c.authority.(SealPublisher); ok {
		c.sealQ = newVisibleSealLane(c, c.installN)
	}
	recoveredVisible, err := c.recoverVisibleLog(context.Background())
	if err != nil {
		return nil, err
	}
	if c.watch != nil {
		c.watchQueue = utils.NewMPSCQueue[observe.WatchEvent](defaultPerasVisibleWatchQueue)
		c.closer.Add(1)
		go c.visibleWatchLoop()
	}
	if c.flushEvery > 0 {
		var skipFirst atomic.Bool
		skipFirst.Store(true)
		c.flushTask = utils.NewPeriodicTask(utils.PeriodicTaskConfig{
			Name:     "peras-segment-flush",
			Interval: c.flushEvery,
			Run: func(context.Context) error {
				if skipFirst.Swap(false) {
					return nil
				}
				c.triggerBackgroundFlush()
				return nil
			},
		})
		c.flushTask.Start()
	}
	if recoveredVisible > 0 && c.installer != nil {
		c.triggerBackgroundFlush()
	}
	return c, nil
}

func (c *Runtime) SubmitVisible(ctx context.Context, id fsperas.OperationID, op compile.MaterializedOp, admission fsperas.AdmissionFunc) (fsperas.VisibleAck, error) {
	if c == nil || c.authority == nil {
		return fsperas.VisibleAck{}, ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c.closed.Load() {
		return fsperas.VisibleAck{}, ErrRuntimeClosed
	}
	if !op.Placement.CanSegment {
		return fsperas.VisibleAck{}, fsperas.ErrIneligibleOperation
	}
	delta := op.Delta
	leaveAuthority := c.enterAuthority(delta.Authority)
	defer leaveAuthority()
	if err := c.waitForAdmissionCapacity(ctx); err != nil {
		return fsperas.VisibleAck{}, err
	}
	c.commitMu.RLock()
	defer c.commitMu.RUnlock()
	if c.closed.Load() {
		return fsperas.VisibleAck{}, ErrRuntimeClosed
	}
	if completion, ok := c.completionForOperation(id); ok {
		if !completionMatchesOperation(completion.completion, op) {
			return fsperas.VisibleAck{}, fsperas.ErrDuplicateOperation
		}
		return fsperas.VisibleAck{EpochID: completion.epochID, OpID: id, HolderID: c.authority.HolderID()}, nil
	}
	grant, owned, err := c.authority.Acquire(ctx, delta.Authority)
	if err != nil {
		return fsperas.VisibleAck{}, c.recordError(err)
	}
	if !owned {
		return fsperas.VisibleAck{}, c.recordError(ErrNotHeld)
	}
	holder, err := c.holderForGrant(ctx, grant, delta.Authority)
	if err != nil {
		return fsperas.VisibleAck{}, c.recordError(err)
	}
	if ack, ok, err := holder.PendingAck(id, op); ok || err != nil {
		if err != nil {
			return ack, c.recordError(err)
		}
		return ack, nil
	}
	unlockAdmission := c.latches.Lock(op)
	defer unlockAdmission()
	admissionCtx := fsperas.AdmissionContext{
		ProofFrontier: proof.ProofFrontier{EpochID: holder.EpochID(), Sequence: id.Seq},
	}
	admitted, err := fsperas.AdmitAndSeal(ctx, op, admission, admissionCtx)
	if err != nil {
		if !errors.Is(err, fsperas.ErrAdmissionRejected) && !isAdmissionTerminalError(err) {
			return fsperas.VisibleAck{}, c.recordError(err)
		}
		return fsperas.VisibleAck{}, err
	}
	op = admitted
	ack, replay, err := holder.Submit(ctx, id, op)
	if err != nil {
		return fsperas.VisibleAck{}, c.recordError(err)
	}
	if err := c.appendVisibleLog(ctx, grant, holder, op, replay); err != nil {
		holder.MarkAppliedIDs(id)
		c.signalAdmissionCapacity()
		return fsperas.VisibleAck{}, c.recordError(err)
	}
	if err := c.addOverlay(id, op); err != nil {
		holder.MarkAppliedIDs(id)
		c.signalAdmissionCapacity()
		return fsperas.VisibleAck{}, c.recordError(err)
	}
	c.publishVisibleWatch(op, ack)
	c.metrics.commitTotal.Add(1)
	if c.batchSize > 0 && holder.Pending() >= c.batchSize {
		c.triggerBackgroundFlush()
	}
	return ack, nil
}

func (c *Runtime) appendVisibleLog(ctx context.Context, grant rootproto.VisibleAuthorityGrant, holder *fsperas.Holder, op compile.MaterializedOp, replay fsperas.ReplayOperation) error {
	if c == nil || c.visibleLog == nil {
		return fsperas.ErrVisibleLogRequired
	}
	record := fsperas.VisibleOperationRecord{
		EpochID:           holder.EpochID(),
		HolderID:          holder.HolderID(),
		GrantID:           grant.GrantID,
		GrantExpiresNanos: grant.ExpiresUnixNano,
		PredecessorDigest: grant.PredecessorDigest,
		RootLineage:       visibleRootLineageFromGrant(grant),
		Scope:             op.Delta.Authority,
		Operation:         replay,
		TimestampUnixNano: c.now().UnixNano(),
	}
	if err := c.visibleLog.AppendVisible(ctx, record); err != nil {
		return fmt.Errorf("append peras visible record kind=%s op=%s/%d epoch=%d holder=%s lineage_valid=%t mutations=%d predicates=%d guards=%d: %w",
			replay.Kind, replay.OpID.ClientID, replay.OpID.Seq, record.EpochID, record.HolderID, record.RootLineage.Valid(), len(replay.Mutations), len(replay.PredicateProofs), len(replay.GuardProofs), err)
	}
	return nil
}

func (c *Runtime) holderForGrant(ctx context.Context, grant rootproto.VisibleAuthorityGrant, scope compile.AuthorityScope) (*fsperas.Holder, error) {
	if !grant.Valid() || grant.HolderID != c.authority.HolderID() {
		return nil, ErrRuntimeInvalid
	}
	if holder, ok := c.epochs.holder(grant); ok {
		return holder, nil
	}

	recoveryScope := scope
	if GrantHasPredecessor(grant) {
		if grantScope := ScopeFromGrant(grant); !ScopeEmpty(grantScope) {
			recoveryScope = grantScope
		}
	}
	if err := c.LoadRootSealedSegments(ctx, recoveryScope); err != nil {
		return nil, err
	}
	if GrantHasPredecessor(grant) && c.installer != nil {
		if err := c.RecoverWitnessSegments(ctx, recoveryScope, grant.EpochID-1); err != nil {
			return nil, err
		}
	}

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{
		EpochID:  grant.EpochID,
		HolderID: grant.HolderID,
	})
	if err != nil {
		return nil, err
	}
	return c.epochs.installHolder(grant, holder), nil
}

func (c *Runtime) FlushDurable(ctx context.Context) error {
	return c.FlushTo(ctx, fsperas.SegmentPersistenceDurable)
}

func (c *Runtime) FlushPublished(ctx context.Context) error {
	return c.FlushTo(ctx, fsperas.SegmentPersistencePublished)
}

func (c *Runtime) FlushTo(ctx context.Context, level fsperas.SegmentPersistenceLevel) error {
	return c.flush(ctx, nil, level)
}

func (c *Runtime) FlushAuthority(ctx context.Context, scope compile.AuthorityScope) error {
	return c.FlushAuthorityTo(ctx, scope, fsperas.SegmentPersistenceDurable)
}

func (c *Runtime) FlushAuthorityPublished(ctx context.Context, scope compile.AuthorityScope) error {
	return c.FlushAuthorityTo(ctx, scope, fsperas.SegmentPersistencePublished)
}

func (c *Runtime) FlushAuthorityTo(ctx context.Context, scope compile.AuthorityScope, level fsperas.SegmentPersistenceLevel) error {
	if scope.Mount == "" || scope.MountKeyID == 0 {
		return c.FlushTo(ctx, level)
	}
	return c.flush(ctx, &scope, level)
}

func (c *Runtime) flush(ctx context.Context, scope *compile.AuthorityScope, level fsperas.SegmentPersistenceLevel) error {
	if c == nil {
		return ErrRuntimeInvalid
	}
	level = fsperas.NormalizeSegmentPersistence(level)
	if !level.Valid() {
		return ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	return c.flushLocked(ctx, scope, level)
}

func defaultPerasSegmentInstallParallelism() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		return 1
	}
	return n
}

func defaultPerasAdmissionPendingLimit(batchSize, maxOps, installN int) int {
	if batchSize <= 0 {
		batchSize = defaultPerasSegmentBatchSize
	}
	if maxOps <= 0 {
		maxOps = defaultPerasSegmentMaxReplayOperations
	}
	if installN <= 0 {
		installN = defaultPerasSegmentInstallParallelism()
	}
	window := max(multiplyIntSaturated(maxOps, installN), batchSize)
	limit := multiplyIntSaturated(window, 4)
	if limit <= 0 {
		return window
	}
	return limit
}

func (c *Runtime) nextWitnessUnixNano() int64 {
	if c == nil {
		return time.Now().UnixNano()
	}
	now := c.now().UnixNano()
	for {
		old := c.witnessSeq.Load()
		next := now
		if next <= old {
			next = old + 1
		}
		if c.witnessSeq.CompareAndSwap(old, next) {
			return next
		}
	}
}

func (c *Runtime) flushBackground() {
	if c == nil {
		return
	}
	reschedule := false
	defer func() {
		c.bgRunning.Store(false)
		if reschedule {
			c.triggerBackgroundFlush()
		}
	}()
	now := c.now().UnixNano()
	if until := c.bgNext.Load(); until > now {
		c.metrics.bgSkipTotal.Add(1)
		return
	}
	if !c.flushMu.TryLock() {
		c.metrics.bgSkipTotal.Add(1)
		return
	}
	defer c.flushMu.Unlock()
	ctx := context.Background()
	var cancel context.CancelFunc
	if c.bgTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.bgTimeout)
		defer cancel()
	}
	c.commitMu.Lock()
	plans, err := c.freezeReplayPlansLocked(nil, c.backgroundFlushMaxOpsPerHolder())
	c.commitMu.Unlock()
	var batches []perasFlushBatch
	if err == nil {
		batches, err = c.buildFlushBatches(plans, c.materialize)
	}
	if err == nil {
		err = (flushPipeline{
			runtime:                 c,
			level:                   fsperas.SegmentPersistencePublished,
			materialize:             c.materialize,
			allowDurableOldEpochRun: true,
		}).run(ctx, batches)
	}
	if err != nil {
		c.metrics.bgErrorTotal.Add(1)
		if c.bgBackoff > 0 {
			c.bgNext.Store(c.now().Add(c.bgBackoff).UnixNano())
		}
		return
	}
	c.bgNext.Store(0)
	reschedule = c.pendingOperations() > 0
}

func (c *Runtime) pendingOperations() int {
	if c == nil {
		return 0
	}
	total := 0
	for _, holder := range c.holderSnapshot() {
		total += holder.Pending()
	}
	return total
}

func (c *Runtime) waitForAdmissionCapacity(ctx context.Context) error {
	if c == nil || c.admitLimit <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var stopContextWake chan struct{}
	if done := ctx.Done(); done != nil {
		stopContextWake = make(chan struct{})
		go func() {
			select {
			case <-done:
				c.signalAdmissionCapacity()
			case <-stopContextWake:
			}
		}()
		defer close(stopContextWake)
	}
	var waitStarted time.Time
	waiting := false
	defer func() {
		if !waiting {
			return
		}
		c.recordAdmissionWait(time.Since(waitStarted))
		c.metrics.admissionWaiting.Add(-1)
	}()
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	for {
		if c.closed.Load() {
			return ErrRuntimeClosed
		}
		if c.pendingOperations() < c.admitLimit {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !waiting {
			waiting = true
			waitStarted = time.Now()
			c.metrics.admissionWaitTotal.Add(1)
			c.metrics.admissionWaiting.Add(1)
		}
		c.triggerBackgroundFlush()
		c.admissionCond.Wait()
	}
}

func (c *Runtime) signalAdmissionCapacity() {
	if c == nil || c.admissionCond == nil {
		return
	}
	c.admissionMu.Lock()
	c.admissionCond.Broadcast()
	c.admissionMu.Unlock()
}

func (c *Runtime) backgroundFlushMaxOpsPerHolder() int {
	if c == nil {
		return 0
	}
	maxOps := max(c.batchSize, 0)
	workers := max(c.installN, 1)
	segmentOps := c.maxOps
	if segmentOps < 1 {
		segmentOps = maxOps
	}
	parallelOps := multiplyIntSaturated(segmentOps, workers)
	if parallelOps > maxOps {
		maxOps = parallelOps
	}
	return maxOps
}

func multiplyIntSaturated(left, right int) int {
	if left <= 0 || right <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if left > maxInt/right {
		return maxInt
	}
	return left * right
}

func (c *Runtime) Close() {
	if c == nil {
		return
	}
	c.closed.Store(true)
	c.signalAdmissionCapacity()
	if c.flushTask != nil {
		c.flushTask.Close()
	}
	if c.watchQueue != nil {
		c.watchQueue.Close()
	}
	if c.installQ != nil {
		c.installQ.close()
	}
	if c.sealQ != nil {
		c.sealQ.close()
	}
	if visibleLog, ok := c.visibleLog.(interface{ Close() }); ok {
		visibleLog.Close()
	}
	c.bgLaunchMu.Lock()
	closer := c.closer
	c.bgLaunchMu.Unlock()
	if closer != nil {
		closer.Close()
	}
}

func (c *Runtime) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := c.FlushPublished(ctx)
	c.Close()
	return err
}

func (c *Runtime) triggerBackgroundFlush() {
	if c == nil {
		return
	}
	c.bgLaunchMu.Lock()
	defer c.bgLaunchMu.Unlock()
	if c.closed.Load() {
		return
	}
	if !c.bgRunning.CompareAndSwap(false, true) {
		c.metrics.bgSkipTotal.Add(1)
		return
	}
	if c.closer != nil {
		c.closer.Add(1)
	}
	go func() {
		defer func() {
			if c.closer != nil {
				c.closer.Done()
			}
		}()
		c.flushBackground()
	}()
}
