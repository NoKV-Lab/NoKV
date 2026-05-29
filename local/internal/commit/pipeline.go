// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package commit implements the DB write commit pipeline: queue, batch
// coalescing, optional sync worker, and the associated ack lifecycle. The Host
// interface is the intentionally narrow set of DB hooks the pipeline calls into
// for storage operations (commit-store batch apply and Sync) and read-only
// state signals (block/slow-write throttle, hot-key tracking, write metrics).
//
// The hot path is intentionally single-lane at this layer. The selected
// storage/kv backend owns physical atomicity and any internal write
// parallelism; the embedded DB facade only coalesces small requests and
// preserves per-request ack/error boundaries.
package commit

import (
	"sync"
	"time"

	"github.com/feichai0017/NoKV/metrics"
	kv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
	pkgerrors "github.com/pkg/errors"
)

// Config carries the commit-pipeline tuning knobs the host snapshots at
// New() time. Updates after Start() do not propagate.
type Config struct {
	SyncWrites         bool
	SyncPipeline       bool
	MaxBatchCount      int64
	MaxBatchSize       int64
	WriteBatchMaxCount int
	WriteBatchMaxSize  int64
	WriteBatchWait     time.Duration
}

// CommitStore is the narrow commit-store surface the pipeline writes through.
type CommitStore interface {
	ApplyEntryGroups(groups [][]*kv.Entry) (int, error)
	Sync() error
	ThrottleRateBytesPerSec() uint64
}

// Host wires the Pipeline back into its DB. The interface stays read-only so
// write admission and CommitStore apply remain owned by the pipeline.
type Host interface {
	CommitStore() CommitStore
	WriteMetrics() *metrics.WriteMetrics

	// Throttle/lifecycle indicators consulted by Send before queueing.
	BlockWritesActive() bool
	SlowWritesActive() bool
	IsClosed() bool
	// ThrottleSignal returns the chan a Send-blocked caller should
	// wait on for clearance. Updated under the host's throttle lock;
	// the pipeline reads it once per spin.
	ThrottleSignal() <-chan struct{}
}

// Pipeline owns the commit queue, commit worker, and optional sync worker.
// Construct with New, drive with Start, drain with Close.
type Pipeline struct {
	cfg  Config
	host Host

	queue     CommitQueue
	wg        sync.WaitGroup
	batchPool sync.Pool
	syncQueue chan *SyncBatch
	syncWG    sync.WaitGroup
}

// New constructs a Pipeline wired to host. Call Start before submitting
// requests through Send.
func New(cfg Config, host Host) *Pipeline {
	p := &Pipeline{
		cfg:  cfg,
		host: host,
	}
	queueCap := max(cfg.WriteBatchMaxCount*8, 1024)
	p.queue.Init(queueCap)
	p.batchPool.New = func() any {
		batch := make([]*CommitRequest, 0, cfg.WriteBatchMaxCount)
		return &batch
	}
	return p
}

// Start launches the commit worker and (when SyncPipeline is set) the sync
// worker. Idempotent — but New + Start is the canonical pattern and Start
// should run exactly once per Pipeline instance.
func (p *Pipeline) Start() {
	if p.cfg.SyncWrites && p.cfg.SyncPipeline {
		p.syncQueue = make(chan *SyncBatch, 128)
		p.syncWG.Add(1)
		go p.syncWorker()
	}
	p.wg.Add(1)
	go p.worker()
}

// Close stops every pipeline goroutine and drains in-flight batches.
// Caller must guarantee no further Send calls.
func (p *Pipeline) Close() {
	p.queue.Close()
	p.wg.Wait()
	if p.syncQueue != nil {
		close(p.syncQueue)
		p.syncWG.Wait()
	}
}

// Send pushes a batch of entries onto the commit queue and returns the
// in-flight Request the caller can block on with Wait. waitOnThrottle
// controls whether the call blocks while writes are stopped (true) or
// returns ErrBlockedWrites immediately (false).
func (p *Pipeline) Send(entries []*kv.Entry, waitOnThrottle bool) (*Request, error) {
	var size int64
	count := int64(len(entries))
	for _, e := range entries {
		size += int64(e.EstimateSize())
	}
	limitCount, limitSize := p.cfg.MaxBatchCount, p.cfg.MaxBatchSize
	if count >= limitCount || size >= limitSize {
		return nil, utils.ErrTxnTooBig
	}
	if p.host.SlowWritesActive() {
		if commitStore := p.host.CommitStore(); commitStore != nil {
			if d := SlowdownDelay(size, commitStore.ThrottleRateBytesPerSec()); d > 0 {
				time.Sleep(d)
			}
		}
	}
	for p.host.BlockWritesActive() {
		if !waitOnThrottle {
			return nil, utils.ErrBlockedWrites
		}
		if p.host.IsClosed() || p.queue.Closed() {
			return nil, utils.ErrBlockedWrites
		}
		ch := p.host.ThrottleSignal()
		if !p.host.BlockWritesActive() {
			break
		}
		select {
		case <-ch:
		case <-p.queue.CloseCh():
			return nil, utils.ErrBlockedWrites
		}
	}

	req := RequestPool.Get().(*Request)
	req.Reset()
	req.Entries = entries
	if p.host.WriteMetrics() != nil {
		req.EnqueueAt = time.Now()
	}
	req.WG.Add(1)
	req.IncrRef()

	cr := CommitRequestPool.Get().(*CommitRequest)
	cr.Req = req
	cr.EntryCount = int(count)
	cr.Size = size

	if err := p.enqueue(cr); err != nil {
		req.WG.Done()
		req.Entries = nil
		req.DecrRef()
		CommitRequestPool.Put(cr)
		return nil, err
	}

	return req, nil
}

func (p *Pipeline) enqueue(cr *CommitRequest) error {
	if cr == nil {
		return nil
	}
	cq := &p.queue
	if cq.Closed() && cq.CloseCh() == nil {
		return utils.ErrBlockedWrites
	}
	if cq.Closed() {
		return utils.ErrBlockedWrites
	}
	cq.AddPending(int64(cr.EntryCount), cr.Size)
	queued := false
	defer func() {
		if !queued {
			cq.AddPending(-int64(cr.EntryCount), -cr.Size)
		}
	}()
	if !cq.Push(cr) {
		return utils.ErrBlockedWrites
	}
	queued = true
	if wm := p.host.WriteMetrics(); wm != nil {
		wm.UpdateQueue(cq.Len(), int(cq.PendingEntries()), cq.PendingBytes())
	}
	return nil
}

func (p *Pipeline) nextBatch(consumer *utils.MPSCConsumer[*CommitRequest]) *CommitBatch {
	cq := &p.queue
	first := cq.Pop(consumer)
	if first == nil {
		return nil
	}

	batchPtr := p.batchPool.Get().(*[]*CommitRequest)
	batch := (*batchPtr)[:0]
	pendingEntries := int64(0)
	pendingBytes := int64(0)
	coalesceWait := p.cfg.WriteBatchWait

	limitCount, limitSize := p.cfg.WriteBatchMaxCount, p.cfg.WriteBatchMaxSize
	backlog := cq.Len()
	if backlog > limitCount && limitCount > 0 {
		factor := min(max(backlog/limitCount, 1), 4)
		if scaled := limitCount * factor; scaled > 0 {
			limitCount = min(scaled, backlog)
		}
		if scaled := limitSize * int64(factor); scaled > 0 {
			limitSize = scaled
		}
	}

	addToBatch := func(cr *CommitRequest) {
		batch = append(batch, cr)
		pendingEntries += int64(cr.EntryCount)
		pendingBytes += cr.Size
	}

	addToBatch(first)
	if coalesceWait > 0 && cq.Len() == 0 && len(batch) < limitCount && pendingBytes < limitSize {
		time.Sleep(coalesceWait)
	}
	remaining := limitCount - len(batch)
	if remaining > 0 && pendingBytes < limitSize {
		cq.DrainReady(consumer, remaining, func(cr *CommitRequest) bool {
			addToBatch(cr)
			return pendingBytes < limitSize
		})
	}

	cq.AddPending(-pendingEntries, -pendingBytes)
	if wm := p.host.WriteMetrics(); wm != nil {
		wm.UpdateQueue(cq.Len(), int(cq.PendingEntries()), cq.PendingBytes())
	}
	return &CommitBatch{Reqs: batch, Pool: batchPtr}
}

// worker owns the MPSC queue's single consumer slot. It drains write requests
// into coalesced commit batches and applies them to the selected storage
// backend.
func (p *Pipeline) worker() {
	defer p.wg.Done()
	consumer := p.queue.Consumer()
	if consumer == nil {
		return
	}
	defer consumer.Close()
	for {
		batch := p.nextBatch(consumer)
		if batch == nil {
			return
		}
		p.runBatch(batch)
	}
}

func (p *Pipeline) runBatch(batch *CommitBatch) {
	wm := p.host.WriteMetrics()
	batch.BatchStart = time.Now()
	requests, totalEntries, totalSize, waitSum := collectCommitRequests(batch.Reqs, batch.BatchStart)
	if len(requests) == 0 {
		p.ackBatch(batch.Reqs, batch.Pool, nil, -1, nil)
		return
	}
	batch.Requests = requests
	if wm != nil {
		wm.RecordBatch(len(requests), totalEntries, totalSize, waitSum)
	}

	failedAt, err := p.applyRequests(batch.Requests)
	if err == nil && p.syncQueue != nil {
		sb := &SyncBatch{
			Reqs:      batch.Reqs,
			Pool:      batch.Pool,
			Requests:  batch.Requests,
			FailedAt:  failedAt,
			ApplyDone: time.Now(),
		}
		batch.Reqs = nil
		batch.Pool = nil
		p.releaseBatch(batch)
		p.syncQueue <- sb
		return
	}

	if err == nil && p.cfg.SyncWrites {
		syncStart := time.Now()
		err = p.host.CommitStore().Sync()
		if wm != nil {
			wm.RecordSync(time.Since(syncStart), 1)
		}
	}

	if wm != nil {
		totalDur := max(time.Since(batch.BatchStart), 0)
		if totalDur > 0 {
			wm.RecordApply(totalDur)
		}
	}

	p.ackBatch(batch.Reqs, batch.Pool, batch.Requests, failedAt, err)
}

func (p *Pipeline) syncWorker() {
	defer p.syncWG.Done()
	wm := p.host.WriteMetrics()
	pending := make([]*SyncBatch, 0, 64)
	for first := range p.syncQueue {
		pending = append(pending, first)
	drain:
		for {
			select {
			case sb, ok := <-p.syncQueue:
				if !ok {
					break drain
				}
				pending = append(pending, sb)
			default:
				break drain
			}
		}

		syncStart := time.Now()
		syncErr := p.host.CommitStore().Sync()
		if wm != nil {
			wm.RecordSync(time.Since(syncStart), len(pending))
		}
		for _, sb := range pending {
			p.ackBatch(sb.Reqs, sb.Pool, sb.Requests, sb.FailedAt, syncErr)
		}
		pending = pending[:0]
	}
}

func (p *Pipeline) ackBatch(reqs []*CommitRequest, pool *[]*CommitRequest, requests []*Request, failedAt int, defaultErr error) {
	if defaultErr != nil && failedAt >= 0 && failedAt < len(requests) {
		perReqErr := make(map[*Request]error, len(requests)-failedAt)
		for i := failedAt; i < len(requests); i++ {
			if requests[i] != nil {
				perReqErr[requests[i]] = defaultErr
			}
		}
		finishCommitRequests(reqs, nil, perReqErr)
	} else {
		finishCommitRequests(reqs, defaultErr, nil)
	}
	if pool != nil {
		for i := range reqs {
			reqs[i] = nil
		}
		*pool = reqs[:0]
		p.batchPool.Put(pool)
	}
}

func collectCommitRequests(reqs []*CommitRequest, now time.Time) ([]*Request, int, int64, int64) {
	requests := make([]*Request, 0, len(reqs))
	var (
		totalEntries int
		totalSize    int64
		waitSum      int64
	)
	for _, cr := range reqs {
		if cr == nil || cr.Req == nil {
			continue
		}
		r := cr.Req
		requests = append(requests, r)
		totalEntries += len(r.Entries)
		totalSize += cr.Size
		if !r.EnqueueAt.IsZero() {
			waitSum += now.Sub(r.EnqueueAt).Nanoseconds()
			r.EnqueueAt = time.Time{}
		}
	}
	return requests, totalEntries, totalSize, waitSum
}

func (p *Pipeline) releaseBatch(batch *CommitBatch) {
	if batch == nil || batch.Pool == nil {
		return
	}
	batch.Requests = nil
	batch.BatchStart = time.Time{}
	reqs := batch.Reqs
	for i := range reqs {
		reqs[i] = nil
	}
	*batch.Pool = reqs[:0]
	p.batchPool.Put(batch.Pool)
}

// ApplyRequests writes reqs into the CommitStore. Exported so root-package
// integration tests can drive the apply path directly without going through the
// queue.
func (p *Pipeline) ApplyRequests(reqs []*Request) (int, error) {
	return p.applyRequests(reqs)
}

func (p *Pipeline) applyRequests(reqs []*Request) (int, error) {
	failedAt, err := p.writeRequestsToStore(reqs)
	if err != nil {
		return failedAt, pkgerrors.Wrap(err, "writeRequests")
	}
	return -1, nil
}

// FinishCommitRequests acks each pending request with either defaultErr
// or perReqErr[req] (when present). Exported for root-package tests.
func FinishCommitRequests(reqs []*CommitRequest, defaultErr error, perReqErr map[*Request]error) {
	finishCommitRequests(reqs, defaultErr, perReqErr)
}

func finishCommitRequests(reqs []*CommitRequest, defaultErr error, perReqErr map[*Request]error) {
	for _, cr := range reqs {
		if cr == nil || cr.Req == nil {
			continue
		}
		if perReqErr != nil {
			if reqErr, ok := perReqErr[cr.Req]; ok {
				cr.Req.Err = reqErr
			} else {
				cr.Req.Err = defaultErr
			}
		} else {
			cr.Req.Err = defaultErr
		}
		cr.Req.WG.Done()
		cr.Req = nil
		cr.EntryCount = 0
		cr.Size = 0
		CommitRequestPool.Put(cr)
	}
}

func (p *Pipeline) writeRequestsToStore(reqs []*Request) (int, error) {
	groups := make([][]*kv.Entry, 0, len(reqs))
	groupToReq := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if r == nil || len(r.Entries) == 0 {
			continue
		}
		if err := prepareStoreRequest(r); err != nil {
			// Nothing has reached the CommitStore yet, so the whole commit batch must fail.
			return 0, err
		}
		groups = append(groups, r.Entries)
		groupToReq = append(groupToReq, i)
	}
	if len(groups) == 0 {
		return -1, nil
	}
	failedGroup, err := p.host.CommitStore().ApplyEntryGroups(groups)
	if err != nil {
		if failedGroup >= 0 && failedGroup < len(groupToReq) {
			return groupToReq[failedGroup], err
		}
		return 0, err
	}
	return -1, nil
}

func prepareStoreRequest(b *Request) error {
	return nil
}
