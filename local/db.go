// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package local provides the embedded DB facade and local runtime wiring.
package local

import (
	"bytes"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/experimental/thermos"
	"github.com/feichai0017/NoKV/local/internal/commit"
	iterpkg "github.com/feichai0017/NoKV/local/internal/iterator"
	"github.com/feichai0017/NoKV/local/stats"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	"github.com/feichai0017/NoKV/metrics"
	storekv "github.com/feichai0017/NoKV/storage/kv"
	pebblestore "github.com/feichai0017/NoKV/storage/pebble"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/feichai0017/NoKV/storage/wal"
	kv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
)

// nonTxnMaxVersion is the read upper-bound sentinel used by non-transactional APIs.
// Non-transactional writes use monotonic versions <= this sentinel.
const nonTxnMaxVersion = kv.MaxVersion

// defaultControlWALShards controls the number of WAL Manager instances that
// back replicated control-log fan-out. Each shard is one fd + one fsync
// worker + one bufio.Writer, so the count is a tradeoff between fd cost
// and per-Manager.mu contention. Must be a power of two — controlWALShard
// uses `& (N-1)` for placement.
//
// The storage backend owns its physical WAL. These managers are only for
// replicated control-log consumers such as raftlog and experimental witnesses.
const defaultControlWALShards = 4

type (
	// BatchSetItem represents one non-transactional write in the default CF.
	//
	// Ownership note: key is copied into the internal-key encoding; value is
	// referenced directly until the write path finishes.
	BatchSetItem struct {
		Key   []byte
		Value []byte
	}

	mvccGCStatsSnapshotSource func() stats.MVCCGCStatsSnapshot
	transportMetricsSource    func() metrics.GRPCTransportMetrics

	// DB is the embedded database facade and owns shared local runtime resources.
	DB struct {
		sync.RWMutex
		opt              *Options
		fs               vfs.FS
		dirLock          io.Closer
		store            storekv.Store
		controlWALMu     sync.Mutex
		controlWALs      [defaultControlWALShards]*wal.Manager
		controlWatchdogs [defaultControlWALShards]*wal.Watchdog
		nonTxnVersion    atomic.Uint64
		blockWrites      atomic.Int32
		slowWrites       atomic.Int32
		isClosed         atomic.Uint32
		closeOnce        sync.Once
		closeErr         error
		throttleMu       sync.Mutex
		throttleCh       chan struct{}
		hotWrite         *thermos.RotatingThermos
		writeMetrics     *metrics.WriteMetrics
		// pipeline owns the commit queue, per-shard dispatch channels,
		// processors, and the optional sync worker.
		pipeline        *commit.Pipeline
		iterPool        *iterpkg.IteratorPool
		workdirMode     workdirmode.Mode
		mvccGCStats     atomic.Value
		transportStats  atomic.Value
		hotWriteLimited atomic.Uint64
		background      backgroundServices
	}
)

func newDB(opt *Options) *DB {
	cfg := opt
	if cfg == nil {
		cfg = &Options{}
	}
	db := &DB{opt: cfg, writeMetrics: metrics.NewWriteMetrics()}
	db.fs = vfs.Ensure(cfg.FS)
	db.throttleCh = make(chan struct{})
	db.hotWrite = commit.NewHotWriteRing(commit.HotWriteConfig{
		Enabled:          cfg.ThermosEnabled && cfg.WriteHotKeyLimit > 0,
		Bits:             cfg.ThermosBits,
		WindowSlots:      cfg.ThermosWindowSlots,
		WindowSlotPeriod: cfg.ThermosWindowSlotDuration,
		DecayInterval:    cfg.ThermosDecayInterval,
		DecayShift:       cfg.ThermosDecayShift,
		NodeCap:          cfg.ThermosNodeCap,
		NodeSampleBits:   cfg.ThermosNodeSampleBits,
		RotationInterval: cfg.ThermosRotationInterval,
	})
	return db
}

func (db *DB) openDurability() error {
	if err := db.fs.MkdirAll(db.opt.WorkDir, os.ModePerm); err != nil {
		return fmt.Errorf("open db: create workdir %q: %w", db.opt.WorkDir, err)
	}
	lock, err := db.fs.Lock(filepath.Join(db.opt.WorkDir, "LOCK"))
	if err != nil {
		return fmt.Errorf("open db: acquire workdir lock %q: %w", db.opt.WorkDir, err)
	}
	db.dirLock = lock

	if err := db.runRecoveryChecks(); err != nil {
		return fmt.Errorf("open db: recovery checks: %w", err)
	}
	return nil
}

func controlRetentionMark(ptrs map[uint64]stats.ControlLogPointer) wal.RetentionMark {
	var first uint32
	for _, ptr := range ptrs {
		if ptr.Segment > 0 && (first == 0 || ptr.Segment < first) {
			first = ptr.Segment
		}
		if ptr.SegmentIndex > 0 {
			seg := uint32(ptr.SegmentIndex)
			if first == 0 || seg < first {
				first = seg
			}
		}
	}
	return wal.RetentionMark{FirstSegment: first}
}

func (db *DB) checkWorkDirMode() error {
	if db == nil || db.opt == nil {
		return fmt.Errorf("open db: options not initialized")
	}
	mode, err := workdirmode.ReadOnlyMode(db.opt.WorkDir)
	if err != nil {
		return fmt.Errorf("open db: read workdir mode: %w", err)
	}
	if workdirmode.Allowed(db.opt.AllowedModes, mode) {
		db.workdirMode = mode
		return nil
	}
	if len(db.opt.AllowedModes) == 0 {
		return fmt.Errorf("open db: workdir mode %q requires explicit distributed open intent", mode)
	}
	return fmt.Errorf("open db: workdir mode %q is not allowed for this open intent", mode)
}

func (db *DB) openStorageBackend() error {
	factory := db.opt.StorageBackendFactory
	if factory == nil {
		factory = openPebbleStorageBackend
	}
	store, err := factory(StorageBackendConfig{
		Dir:              db.storageBackendDir(),
		SyncWrites:       db.opt.SyncWrites,
		CacheBytes:       db.opt.BlockCacheBytes,
		WriteBufferBytes: uint64(max(db.opt.StorageWriteBufferBytes, 0)),
	})
	if err != nil {
		return fmt.Errorf("open db: storage backend init: %w", err)
	}
	db.store = store
	maxVersion, err := db.scanMaxDefaultVersion()
	if err != nil {
		return fmt.Errorf("open db: scan max version: %w", err)
	}
	db.nonTxnVersion.Store(maxVersion)
	db.iterPool = iterpkg.NewIteratorPool()
	db.background.Init(stats.New(db, 0))
	return nil
}

func openPebbleStorageBackend(cfg StorageBackendConfig) (storekv.Store, error) {
	return pebblestore.Open(pebblestore.Options{
		Dir:           cfg.Dir,
		SyncWrites:    cfg.SyncWrites,
		CacheBytes:    cfg.CacheBytes,
		MemTableBytes: cfg.WriteBufferBytes,
	})
}

func (db *DB) storageBackendDir() string {
	return filepath.Join(db.opt.WorkDir, "storage")
}

func (db *DB) scanMaxDefaultVersion() (uint64, error) {
	if db == nil || db.store == nil {
		return 0, nil
	}
	iter, err := db.newStorageIterator(storekv.IteratorOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()
	var maxVersion uint64
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		if entry == nil {
			continue
		}
		cf, _, ts, ok := kv.SplitInternalKey(entry.Key)
		if !ok || cf != kv.CFDefault || ts == kv.MaxVersion {
			continue
		}
		if ts > maxVersion {
			maxVersion = ts
		}
	}
	return maxVersion, nil
}

func (db *DB) startWriteRuntime() {
	// Commit processors are local CPU/admission lanes. The selected storage
	// backend owns physical batch atomicity; shard placement only spreads
	// commit-pipeline work.
	workers := db.opt.WriteShardCount
	if workers <= 0 {
		workers = 1
	}
	db.pipeline = commit.New(commit.Config{
		ShardCount:         workers,
		SyncWrites:         db.opt.SyncWrites,
		SyncPipeline:       db.opt.SyncPipeline,
		MaxBatchCount:      db.opt.MaxBatchCount,
		MaxBatchSize:       db.opt.MaxBatchSize,
		WriteBatchMaxCount: db.opt.WriteBatchMaxCount,
		WriteBatchMaxSize:  db.opt.WriteBatchMaxSize,
		WriteBatchWait:     db.opt.WriteBatchWait,
	}, db)
	db.pipeline.Start()
}

func (db *DB) controlWALFor(groupID uint64) (*wal.Manager, error) {
	if db == nil || db.opt == nil {
		return nil, fmt.Errorf("db control wal: nil db")
	}
	if db.IsClosed() {
		return nil, fmt.Errorf("db control wal: closed db")
	}
	shard := controlWALShard(groupID)
	db.controlWALMu.Lock()
	defer db.controlWALMu.Unlock()
	if mgr := db.controlWALs[shard]; mgr != nil {
		return mgr, nil
	}
	mgr, err := wal.Open(wal.Config{
		Dir:        db.controlWALDir(shard),
		BufferSize: db.opt.ControlWALBufferSize,
		FS:         db.fs,
	})
	if err != nil {
		return nil, err
	}
	if db.opt.ControlLogPointerSnapshot != nil {
		if err := mgr.RegisterRetention("control-log", func() wal.RetentionMark {
			return controlRetentionMarkForShard(db.opt.ControlLogPointerSnapshot(), shard)
		}); err != nil {
			_ = mgr.Close()
			return nil, err
		}
	}
	if db.opt.EnableControlWALWatchdog {
		wd := wal.NewWatchdog(wal.WatchdogConfig{
			Manager:      mgr,
			Interval:     db.opt.ControlWALAutoGCInterval,
			MinRemovable: db.opt.ControlWALAutoGCMinRemovable,
			MaxBatch:     db.opt.ControlWALAutoGCMaxBatch,
			WarnRatio:    db.opt.ControlWALTypedRecordWarnRatio,
			WarnSegments: db.opt.ControlWALTypedRecordWarnSegments,
		})
		if wd != nil {
			wd.Start()
			db.controlWatchdogs[shard] = wd
		}
	}
	db.controlWALs[shard] = mgr
	return mgr, nil
}

// OpenControlWAL returns one sharded WAL manager for replicated control-log
// adapters. The DB owns durability and retention; callers define the log
// semantics above this storage boundary.
func (db *DB) OpenControlWAL(groupID uint64) (*wal.Manager, error) {
	return db.controlWALFor(groupID)
}

func (db *DB) controlWALDir(shard int) string {
	return filepath.Join(db.opt.WorkDir, fmt.Sprintf("control-wal-%02d", shard))
}

func controlWALShard(groupID uint64) int {
	const mix = 11400714819323198485
	return int((groupID * mix) & (defaultControlWALShards - 1))
}

func controlRetentionMarkForShard(ptrs map[uint64]stats.ControlLogPointer, shard int) wal.RetentionMark {
	filtered := make(map[uint64]stats.ControlLogPointer)
	for groupID, ptr := range ptrs {
		if controlWALShard(groupID) == shard {
			filtered[groupID] = ptr
		}
	}
	return controlRetentionMark(filtered)
}

// WorkdirMode returns the persisted lifecycle mode observed when the DB was opened.
func (db *DB) WorkdirMode() workdirmode.Mode {
	if db == nil {
		return ""
	}
	return db.workdirMode
}

// Open constructs the database and returns initialization errors instead of panicking.
func Open(opt *Options) (_ *DB, err error) {
	if opt == nil {
		return nil, stderrors.New("open db: nil options")
	}
	frozen := *opt
	frozen.resolveOpenDefaults()
	db := newDB(&frozen)
	defer func() {
		if err != nil {
			err = stderrors.Join(err, db.closeInternal())
		}
	}()
	if err := db.checkWorkDirMode(); err != nil {
		return nil, err
	}
	if err := db.openDurability(); err != nil {
		return nil, err
	}
	if err := db.openStorageBackend(); err != nil {
		return nil, err
	}
	db.startWriteRuntime()
	db.background.Start(backgroundConfig{})
	return db, nil
}

func (db *DB) runRecoveryChecks() error {
	if db == nil || db.opt == nil {
		return fmt.Errorf("recovery checks: options not initialized")
	}
	for shard := range defaultControlWALShards {
		if err := wal.VerifyDir(db.controlWALDir(shard), db.fs); err != nil {
			return err
		}
	}
	return nil
}

// Close stops background workers and flushes in-memory state before releasing all resources.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.closeOnce.Do(func() {
		db.closeErr = db.closeInternal()
	})
	return db.closeErr
}

// closeInternal executes DB shutdown exactly once and aggregates non-fatal
// close failures so callers can observe every resource teardown error.
func (db *DB) closeInternal() error {
	if db == nil {
		return nil
	}

	if db.IsClosed() {
		return nil
	}

	if db.pipeline != nil {
		db.pipeline.Close()
	}

	var errs []error
	if err := db.background.Close(); err != nil {
		errs = append(errs, err)
	}

	if db.hotWrite != nil {
		db.hotWrite.Close()
	}

	if db.store != nil {
		if err := db.store.Close(); err != nil {
			errs = append(errs, fmt.Errorf("storage close: %w", err))
		}
		db.store = nil
	}

	if err := db.closeControlWALs(); err != nil {
		errs = append(errs, err)
	}

	if db.dirLock != nil {
		if err := db.dirLock.Close(); err != nil {
			errs = append(errs, fmt.Errorf("dir lock release: %w", err))
		}
		db.dirLock = nil
	}

	db.isClosed.Store(1)

	if len(errs) > 0 {
		return stderrors.Join(errs...)
	}
	return nil
}

func (db *DB) closeControlWALs() error {
	db.controlWALMu.Lock()
	defer db.controlWALMu.Unlock()
	var errs []error
	for shard, wd := range db.controlWatchdogs {
		if wd != nil {
			wd.Stop()
			db.controlWatchdogs[shard] = nil
		}
	}
	for shard, mgr := range db.controlWALs {
		if mgr == nil {
			continue
		}
		if err := mgr.Close(); err != nil {
			errs = append(errs, fmt.Errorf("control wal shard %d close: %w", shard, err))
		}
		db.controlWALs[shard] = nil
	}
	return stderrors.Join(errs...)
}

// Del removes a key from the default column family by writing a tombstone.
func (db *DB) Del(key []byte) error {
	if len(key) == 0 {
		return utils.ErrEmptyKey
	}
	entry := kv.NewInternalEntry(kv.CFDefault, key, db.nextNonTxnVersion(), nil, kv.BitDelete, 0)
	defer entry.DecrRef()
	return db.ApplyInternalEntries([]*kv.Entry{entry})
}

// DeleteRange removes all keys in [start, end) from the default column family.
func (db *DB) DeleteRange(start, end []byte) error {
	if len(start) == 0 || len(end) == 0 {
		return utils.ErrEmptyKey
	}
	if bytes.Compare(start, end) >= 0 {
		return utils.ErrInvalidRequest
	}
	entry := kv.NewInternalEntry(kv.CFDefault, start, db.nextNonTxnVersion(), end, kv.BitRangeDelete, 0)
	defer entry.DecrRef()
	return db.ApplyInternalEntries([]*kv.Entry{entry})
}

// Set writes a key/value pair into the default column family.
// Use Del for explicit deletion; nil values are rejected.
func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return utils.ErrEmptyKey
	}
	if value == nil {
		return utils.ErrNilValue
	}
	entry := kv.NewInternalEntry(kv.CFDefault, key, db.nextNonTxnVersion(), value, 0, 0)
	defer entry.DecrRef()
	return db.ApplyInternalEntries([]*kv.Entry{entry})
}

// SetBatch writes multiple key/value pairs into the default column family.
//
// Semantics:
//   - Non-transactional API: each entry receives a monotonically increasing
//     internal version.
//   - The batch is submitted through the regular write pipeline and commit queue.
//
// Validation:
//   - Empty batch is a no-op.
//   - Every item must have a non-empty key and non-nil value.
//
// Ownership:
//   - key bytes are encoded into internal keys.
//   - value slices are referenced directly until this call returns; callers must
//     keep them immutable for the duration of this call.
func (db *DB) SetBatch(items []BatchSetItem) error {
	if len(items) == 0 {
		return nil
	}
	entries := make([]*kv.Entry, 0, len(items))
	release := func() {
		for _, entry := range entries {
			if entry != nil {
				entry.DecrRef()
			}
		}
	}
	for _, item := range items {
		if len(item.Key) == 0 {
			release()
			return utils.ErrEmptyKey
		}
		if item.Value == nil {
			release()
			return utils.ErrNilValue
		}
		entries = append(entries, kv.NewInternalEntry(kv.CFDefault, item.Key, db.nextNonTxnVersion(), item.Value, 0, 0))
	}
	defer release()
	return db.ApplyInternalEntries(entries)
}

// SetWithTTL writes a key/value pair into the default column family with TTL.
// Use Del for explicit deletion; nil values are rejected.
//
// Ownership note: key is encoded into a new internal-key buffer, while value is
// referenced directly (no deep copy). Callers must keep value immutable until
// this method returns.
func (db *DB) SetWithTTL(key, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return utils.ErrEmptyKey
	}
	if value == nil {
		return utils.ErrNilValue
	}
	entry := kv.NewInternalEntry(kv.CFDefault, key, db.nextNonTxnVersion(), value, 0, 0)
	entry.WithTTL(ttl)
	defer entry.DecrRef()
	return db.ApplyInternalEntries([]*kv.Entry{entry})
}

// nextNonTxnVersion allocates the next monotonic version for non-transactional writes.
func (db *DB) nextNonTxnVersion() uint64 {
	next := db.nonTxnVersion.Add(1)
	if next == 0 {
		panic("NoKV: non-transactional version overflow")
	}
	return next
}

// ApplyInternalEntries writes pre-built internal-key entries through the
// regular write pipeline.
//
// The caller must provide entries with internal keys. The entry slices must not
// be mutated until this call returns.
//
// Multi-key internal batches are regrouped by the same key-affinity router used
// by the commit pipeline before they reach the sharded CommitStore. That keeps every
// write/delete for one user key on one shard, which is required for
// same-version MVCC tombstones such as Percolator lock removal.
func (db *DB) ApplyInternalEntries(entries []*kv.Entry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if len(entries) == 0 {
		return nil
	}
	for _, entry := range entries {
		if entry == nil || len(entry.Key) == 0 {
			return utils.ErrEmptyKey
		}
		// ApplyInternalEntries is for pre-built internal keys only.
		parsedCF, userKey, parsedVersion, ok := kv.SplitInternalKey(entry.Key)
		if !ok || len(userKey) == 0 {
			return utils.ErrInvalidRequest
		}
		entry.CF = parsedCF
		entry.Version = parsedVersion
		if err := db.maybeThrottleWrite(parsedCF, userKey); err != nil {
			return err
		}
	}
	for _, entry := range entries {
		entry.IncrRef()
	}
	return db.batchSetInternalEntries(entries)
}

// GetInternalEntry retrieves one internal-key record for the provided version.
//
// The returned entry is borrowed from internal pools and returned as-is
// (no clone/no copy). entry.Key remains in internal encoding
// (cf+user_key+ts). Callers MUST call DecrRef exactly once when finished.
func (db *DB) GetInternalEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	entry, err := db.findVisibleInternalEntry(cf, key, version)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// MaterializeInternalEntry converts a borrowed internal entry into a detached
// internal entry suitable for export or replay. The returned entry preserves
// canonical internal-key layout and inline value payloads.
func (db *DB) MaterializeInternalEntry(src *kv.Entry) (*kv.Entry, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if src == nil {
		return nil, utils.ErrKeyNotFound
	}
	buf := make([]byte, len(src.Key)+len(src.Value))
	keyCopy := buf[:len(src.Key)]
	copy(keyCopy, src.Key)
	valueCopy := buf[len(src.Key):]
	copy(valueCopy, src.Value)
	entry := kv.NewEntry(keyCopy, valueCopy)
	entry.ExpiresAt = src.ExpiresAt
	entry.CF = src.CF
	entry.Meta = src.Meta
	entry.Version = src.Version
	entry.Offset = src.Offset
	entry.Hlen = src.Hlen
	entry.ValThreshold = src.ValThreshold
	return entry, nil
}

// Get reads the latest visible value for key from the default column family.
//
// The returned Entry is DETACHED: caller owns Entry.Value bytes and must
// NOT call DecrRef. This differs from GetInternalEntry, which returns
// a BORROWED entry that must be released with DecrRef exactly once.
// Mixing the two contracts typically surfaces as a crash far from the
// bug site.
func (db *DB) Get(key []byte) (*kv.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	entry, err := db.findVisibleInternalEntry(kv.CFDefault, key, kv.MaxVersion)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, utils.ErrKeyNotFound
	}
	defer entry.DecrRef()
	if entry.IsRangeDelete() {
		return nil, utils.ErrKeyNotFound
	}
	if entry.IsDeletedOrExpired() {
		return nil, utils.ErrKeyNotFound
	}
	cf, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
	if !ok {
		return nil, utils.ErrInvalidRequest
	}
	version := entry.Version
	if ts != 0 {
		version = ts
	}
	buf := make([]byte, len(userKey)+len(entry.Value))
	keyCopy := buf[:len(userKey)]
	copy(keyCopy, userKey)
	valueCopy := buf[len(userKey):]
	copy(valueCopy, entry.Value)
	return &kv.Entry{
		Key:          keyCopy,
		Value:        valueCopy,
		ExpiresAt:    entry.ExpiresAt,
		CF:           cf,
		Meta:         entry.Meta,
		Version:      version,
		Offset:       entry.Offset,
		Hlen:         entry.Hlen,
		ValThreshold: entry.ValThreshold,
	}, nil
}

func (db *DB) findVisibleInternalEntry(cf kv.ColumnFamily, key []byte, readVersion uint64) (*kv.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	seekKey := kv.InternalKey(cf, key, readVersion)
	iter, err := db.newStorageIterator(storekv.IteratorOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	iter.Seek(seekKey)
	for iter.Valid() {
		entry := iter.Item().Entry()
		if entry == nil {
			iter.Next()
			continue
		}
		entryCF, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
		if !ok || entryCF != cf {
			iter.Next()
			continue
		}
		if !bytes.Equal(userKey, key) {
			return nil, utils.ErrKeyNotFound
		}
		if entry.IsRangeDelete() {
			iter.Next()
			continue
		}
		if db.IsKeyCoveredByRangeTombstone(cf, userKey, ts) {
			return nil, utils.ErrKeyNotFound
		}
		entry.IncrRef()
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}

// NewIterator creates a DB-level iterator over user keys in the default
// column family. The state machine + Item materialization live in
// local/internal/iterator; this method wires DB internals (store, iterPool)
// into iterpkg.New as a thin facade.
func (db *DB) NewIterator(opt *kv.Options) kv.Iterator {
	return iterpkg.New(iterpkg.Deps{
		Storage: db,
		Pool:    db.iterPool,
	}, opt)
}

// NewInternalIterator returns an iterator over internal keys (CF marker +
// user key + timestamp). Callers should decode kv.Entry.Key via
// kv.SplitInternalKey and handle ok=false.
func (db *DB) NewInternalIterator(opt *kv.Options) kv.Iterator {
	if opt == nil {
		opt = &kv.Options{}
	}
	iter, err := db.newStorageIterator(storekv.IteratorOptions{
		LowerBound: internalIteratorBound(opt.LowerBound),
		UpperBound: internalIteratorBound(opt.UpperBound),
		Reverse:    !opt.IsAsc,
	})
	if err != nil {
		return &errorIterator{err: err}
	}
	return iter
}

func internalIteratorBound(bound []byte) []byte {
	if len(bound) == 0 {
		return nil
	}
	if _, _, _, ok := kv.SplitInternalKey(bound); ok {
		return bound
	}
	return kv.InternalKey(kv.CFDefault, bound, kv.MaxVersion)
}

// Info returns the live stats collector for snapshot/diagnostic access.
func (db *DB) Info() *stats.Stats {
	if db == nil {
		return nil
	}
	s, _ := db.background.StatsCollector().(*stats.Stats)
	return s
}

// stats.Host implementation: read-only accessors the stats subsystem
// uses to assemble a StatsSnapshot. They are intentionally a thin lift
// over DB struct fields so the snapshot logic can live in local/stats without
// importing the local DB implementation.

func (db *DB) ControlWALWatchdogs() []*wal.Watchdog {
	if db == nil {
		return nil
	}
	watchdogs := db.background.walWatchdogs
	if len(watchdogs) == 0 {
		return nil
	}
	out := make([]*wal.Watchdog, len(watchdogs))
	copy(out, watchdogs)
	return out
}
func (db *DB) StorageStats() storekv.Stats {
	if db == nil || db.store == nil {
		return storekv.Stats{}
	}
	return db.store.Stats()
}
func (db *DB) SetMVCCGCStatsSnapshotSource(source func() stats.MVCCGCStatsSnapshot) {
	if db == nil || source == nil {
		return
	}
	db.mvccGCStats.Store(mvccGCStatsSnapshotSource(source))
}

func (db *DB) MVCCGCStatsSnapshot() stats.MVCCGCStatsSnapshot {
	if db == nil {
		return stats.MVCCGCStatsSnapshot{}
	}
	v := db.mvccGCStats.Load()
	if v == nil {
		return stats.MVCCGCStatsSnapshot{}
	}
	source, ok := v.(mvccGCStatsSnapshotSource)
	if !ok || source == nil {
		return stats.MVCCGCStatsSnapshot{}
	}
	return source()
}

func (db *DB) HotWrite() *thermos.RotatingThermos      { return db.hotWrite }
func (db *DB) IteratorReused() uint64                  { return db.iterPool.Reused() }
func (db *DB) WriteMetrics() *metrics.WriteMetrics     { return db.writeMetrics }
func (db *DB) BlockWritesActive() bool                 { return db.blockWrites.Load() == 1 }
func (db *DB) SlowWritesActive() bool                  { return db.slowWrites.Load() == 1 }
func (db *DB) HotWriteLimited() uint64                 { return db.hotWriteLimited.Load() }
func (db *DB) ControlLogLagWarnSegments() int64        { return db.opt.ControlLogLagWarnSegments }
func (db *DB) ControlWALTypedRecordWarnRatio() float64 { return db.opt.ControlWALTypedRecordWarnRatio }
func (db *DB) ControlWALTypedRecordWarnSegments() int64 {
	return db.opt.ControlWALTypedRecordWarnSegments
}
func (db *DB) ThermosTopK() int { return db.opt.ThermosTopK }

// WriteBatchLimits returns the per-batch entry-count and byte-size caps the
// commit pipeline enforces. Callers that build batched-install paths use
// these to chunk their writes so the pipeline's Send() never rejects a
// group as too large.
func (db *DB) WriteBatchLimits() (count int64, size int64) {
	if db == nil || db.opt == nil {
		return 0, 0
	}
	return db.opt.MaxBatchCount, db.opt.MaxBatchSize
}

func (db *DB) ControlLogPointerSnapshot() func() map[uint64]stats.ControlLogPointer {
	if db == nil || db.opt == nil {
		return nil
	}
	return db.opt.ControlLogPointerSnapshot
}

func (db *DB) SetTransportMetricsSource(source func() metrics.GRPCTransportMetrics) {
	if db == nil || source == nil {
		return
	}
	db.transportStats.Store(transportMetricsSource(source))
}

func (db *DB) TransportMetrics() metrics.GRPCTransportMetrics {
	if db == nil {
		return metrics.GRPCTransportMetrics{}
	}
	v := db.transportStats.Load()
	if v == nil {
		return metrics.GRPCTransportMetrics{}
	}
	source, ok := v.(transportMetricsSource)
	if !ok || source == nil {
		return metrics.GRPCTransportMetrics{}
	}
	return source()
}

func (db *DB) ControlWALsLocked(fn func(wals []*wal.Manager)) {
	db.controlWALMu.Lock()
	defer db.controlWALMu.Unlock()
	fn(db.controlWALs[:])
}

// commit.Host implementation: read-only accessors the commit Pipeline uses
// without importing the local DB implementation.

func (db *DB) ThrottleSignal() <-chan struct{} {
	db.throttleMu.Lock()
	ch := db.throttleCh
	db.throttleMu.Unlock()
	return ch
}

func (db *DB) CommitStore() commit.CommitStore { return db }

// SetRegionMetrics attaches region metrics recorder so Stats snapshot and expvar
// include region state counts.
func (db *DB) SetRegionMetrics(rm *metrics.RegionMetrics) {
	if db == nil {
		return
	}
	db.background.SetRegionMetrics(rm)
}

// SyncWAL preserves the administrative durability hook used by transaction
// tests. The Pebble-backed local store owns its own WAL, so this delegates to
// the storage backend sync boundary instead of exposing data-plane WAL managers.
func (db *DB) SyncWAL() error {
	if db == nil || db.store == nil {
		return fmt.Errorf("db: wal is unavailable")
	}
	return db.store.Sync()
}

// ReplayWAL is intentionally empty for the Pebble-backed data path. Pebble owns
// physical WAL recovery internally; NoKV no longer exposes data WAL replay as a
// product API.
func (db *DB) ReplayWAL(fn func(info wal.EntryInfo, payload []byte) error) error {
	if db == nil || db.store == nil {
		return fmt.Errorf("db: wal is unavailable")
	}
	return nil
}

// IsClosed reports whether Close has finished and the DB no longer accepts work.
func (db *DB) IsClosed() bool {
	return db.isClosed.Load() == 1
}

func (db *DB) ApplyThrottle(state commit.WriteThrottleState) {
	state = commit.NormalizeWriteThrottleState(state)
	stop := int32(0)
	slow := int32(0)
	switch state {
	case commit.WriteThrottleStop:
		stop = 1
	case commit.WriteThrottleSlowdown:
		slow = 1
	}
	prevStop := db.blockWrites.Swap(stop)
	prevSlow := db.slowWrites.Swap(slow)
	if prevStop == stop && prevSlow == slow {
		return
	}
	db.throttleMu.Lock()
	ch := db.throttleCh
	db.throttleCh = make(chan struct{})
	db.throttleMu.Unlock()
	close(ch)
	switch state {
	case commit.WriteThrottleStop:
		slog.Default().Warn("write stop enabled")
	case commit.WriteThrottleSlowdown:
		slog.Default().Info("write slowdown enabled")
	default:
		slog.Default().Info("write throttling cleared")
	}
}

// sendToWriteCh delegates to the commit Pipeline. Kept as a thin facade
// so internal batch helpers (batchSet) keep their current names without
// spreading commit-pipeline details across the DB facade.
func (db *DB) sendToWriteCh(entries []*kv.Entry, waitOnThrottle bool) (*commit.Request, error) {
	return db.pipeline.Send(entries, waitOnThrottle)
}

func (db *DB) maybeThrottleWrite(cf kv.ColumnFamily, key []byte) error {
	if db == nil || db.opt == nil {
		return nil
	}
	if !commit.ShouldThrottleHotWrite(db.hotWrite, db.opt.WriteHotKeyLimit, cf, key) {
		return nil
	}
	db.hotWriteLimited.Add(1)
	return utils.ErrHotKeyWriteThrottle
}

func (db *DB) batchSet(entries []*kv.Entry) error {
	req, err := db.sendToWriteCh(entries, true)
	if err != nil {
		for _, entry := range entries {
			if entry != nil {
				entry.DecrRef()
			}
		}
		return err
	}
	return req.Wait()
}

func (db *DB) batchSetInternalEntries(entries []*kv.Entry) error {
	return db.batchSet(entries)
}

func (db *DB) WorkDir() string {
	if db == nil || db.opt == nil {
		return ""
	}
	return db.opt.WorkDir
}

func (db *DB) SetBatchGroup(_ int, groups [][]*kv.Entry) (int, error) {
	for i, group := range groups {
		if len(group) == 0 {
			continue
		}
		batch := storekv.Batch{Ops: make([]storekv.Mutation, 0, len(group))}
		for _, entry := range group {
			if entry == nil || len(entry.Key) == 0 {
				return i, utils.ErrEmptyKey
			}
			physicalKey, err := encodePhysicalKey(entry.Key)
			if err != nil {
				return i, err
			}
			batch.Ops = append(batch.Ops, storekv.Mutation{
				Op:    storekv.PutOp,
				Key:   physicalKey,
				Value: encodeStoredEntry(entry),
			})
		}
		if err := db.store.ApplyBatch(batch); err != nil {
			return i, err
		}
	}
	return -1, nil
}

func (db *DB) Sync() error {
	if db == nil || db.store == nil {
		return fmt.Errorf("db: storage is unavailable")
	}
	return db.store.Sync()
}

func (db *DB) ThrottleRateBytesPerSec() uint64 { return 0 }

func (db *DB) IsKeyCoveredByRangeTombstone(cf kv.ColumnFamily, userKey []byte, version uint64) bool {
	if db == nil || db.store == nil {
		return false
	}
	iter, err := db.newStorageIterator(storekv.IteratorOptions{})
	if err != nil {
		return false
	}
	defer func() { _ = iter.Close() }()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		if entry == nil || !entry.IsRangeDelete() {
			continue
		}
		tombstoneCF, start, tombstoneVersion, ok := kv.SplitInternalKey(entry.Key)
		if !ok || tombstoneCF != cf {
			continue
		}
		if tombstoneVersion < version {
			continue
		}
		if bytes.Compare(userKey, start) >= 0 && bytes.Compare(userKey, entry.RangeEnd()) < 0 {
			return true
		}
	}
	return false
}

func encodeStoredEntry(entry *kv.Entry) []byte {
	vs := kv.ValueStruct{
		Meta:      entry.Meta,
		Value:     entry.Value,
		ExpiresAt: entry.ExpiresAt,
	}
	out := make([]byte, vs.EncodedSize())
	vs.EncodeValue(out)
	return out
}

func decodeStoredEntry(internalKey []byte, encoded []byte) (*kv.Entry, error) {
	if len(encoded) == 0 {
		return nil, utils.ErrInvalidRequest
	}
	var vs kv.ValueStruct
	vs.DecodeValue(encoded)
	keyCopy := append([]byte(nil), internalKey...)
	valueCopy := make([]byte, len(vs.Value))
	copy(valueCopy, vs.Value)
	entry := kv.NewValueStructEntry(keyCopy, kv.ValueStruct{
		Meta:      vs.Meta,
		Value:     valueCopy,
		ExpiresAt: vs.ExpiresAt,
	})
	if !entry.PopulateInternalMeta() {
		entry.DecrRef()
		return nil, utils.ErrInvalidRequest
	}
	return entry, nil
}

func (db *DB) newStorageIterator(opts storekv.IteratorOptions) (kv.Iterator, error) {
	if db == nil || db.store == nil {
		return nil, fmt.Errorf("db: storage is unavailable")
	}
	var err error
	if len(opts.LowerBound) > 0 {
		opts.LowerBound, err = encodePhysicalKey(opts.LowerBound)
		if err != nil {
			return nil, err
		}
	}
	if len(opts.UpperBound) > 0 {
		opts.UpperBound, err = encodePhysicalKey(opts.UpperBound)
		if err != nil {
			return nil, err
		}
	}
	iter, err := db.store.NewIterator(opts)
	if err != nil {
		return nil, err
	}
	return &internalIterator{iter: iter, reverse: opts.Reverse}, nil
}

type internalIterator struct {
	iter    storekv.Iterator
	reverse bool
	entry   *kv.Entry
	item    internalIteratorItem
}

type internalIteratorItem struct {
	entry *kv.Entry
}

type errorIterator struct {
	err error
}

func (it *errorIterator) Next()       {}
func (it *errorIterator) Rewind()     {}
func (it *errorIterator) Seek([]byte) {}
func (it *errorIterator) Valid() bool { return false }
func (it *errorIterator) Item() kv.Item {
	return nil
}
func (it *errorIterator) Close() error {
	if it == nil {
		return nil
	}
	return it.err
}

func (item *internalIteratorItem) Entry() *kv.Entry {
	if item == nil {
		return nil
	}
	return item.entry
}

func (it *internalIterator) Next() {
	it.releaseItem()
	if it.reverse {
		it.iter.Prev()
		return
	}
	it.iter.Next()
}

func (it *internalIterator) Valid() bool {
	return it != nil && it.iter != nil && it.iter.Valid()
}

func (it *internalIterator) Rewind() {
	it.releaseItem()
	if it.reverse {
		it.iter.Last()
		return
	}
	it.iter.First()
}

func (it *internalIterator) Item() kv.Item {
	if !it.Valid() {
		return nil
	}
	it.releaseItem()
	logicalKey, err := decodePhysicalKey(it.iter.Key())
	if err != nil {
		return nil
	}
	value, err := it.iter.Value()
	if err != nil {
		return nil
	}
	entry, err := decodeStoredEntry(logicalKey, value)
	if err != nil {
		return nil
	}
	it.entry = entry
	it.item.entry = entry
	return &it.item
}

func (it *internalIterator) Close() error {
	if it == nil || it.iter == nil {
		return nil
	}
	it.releaseItem()
	return it.iter.Close()
}

func (it *internalIterator) Seek(key []byte) {
	if it == nil || it.iter == nil {
		return
	}
	it.releaseItem()
	physicalKey, err := encodePhysicalKey(key)
	if err != nil {
		return
	}
	if !it.reverse {
		it.iter.Seek(physicalKey)
		return
	}
	if !it.iter.Seek(physicalKey) {
		it.iter.Last()
		return
	}
	if bytes.Compare(it.iter.Key(), physicalKey) > 0 {
		it.iter.Prev()
	}
}

func (it *internalIterator) releaseItem() {
	if it == nil || it.entry == nil {
		return
	}
	it.entry.DecrRef()
	it.entry = nil
	it.item.entry = nil
}

func encodePhysicalKey(internalKey []byte) ([]byte, error) {
	cf, userKey, _, ok := kv.SplitInternalKey(internalKey)
	if !ok {
		return nil, utils.ErrInvalidRequest
	}
	out := make([]byte, 0, 4+len(userKey)*2+2+8)
	out = append(out, 0xff, 'C', 'F', byte(cf))
	for _, b := range userKey {
		if b == 0 {
			out = append(out, 0, 0xff)
			continue
		}
		out = append(out, b)
	}
	out = append(out, 0, 0)
	out = append(out, internalKey[len(internalKey)-8:]...)
	return out, nil
}

func decodePhysicalKey(physicalKey []byte) ([]byte, error) {
	if len(physicalKey) < 4+2+8 ||
		physicalKey[0] != 0xff ||
		physicalKey[1] != 'C' ||
		physicalKey[2] != 'F' {
		return nil, utils.ErrInvalidRequest
	}
	cf := kv.ColumnFamily(physicalKey[3])
	if !cf.Valid() {
		return nil, utils.ErrInvalidRequest
	}
	body := physicalKey[4 : len(physicalKey)-8]
	userKey := make([]byte, 0, len(body))
	for i := 0; i < len(body); i++ {
		b := body[i]
		if b != 0 {
			userKey = append(userKey, b)
			continue
		}
		if i+1 >= len(body) {
			return nil, utils.ErrInvalidRequest
		}
		next := body[i+1]
		i++
		if next == 0 {
			logical := kv.InternalKey(cf, userKey, kv.Timestamp(physicalKey[len(physicalKey)-8:]))
			copy(logical[len(logical)-8:], physicalKey[len(physicalKey)-8:])
			return logical, nil
		}
		if next != 0xff {
			return nil, utils.ErrInvalidRequest
		}
		userKey = append(userKey, 0)
	}
	return nil, utils.ErrInvalidRequest
}
