// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/local"
	index "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
)

func newNoKVEngine(opts ycsbEngineOptions) ycsbEngine {
	return &nokvEngine{
		opts:     opts,
		engineID: "nokv",
		name:     "NoKV",
	}
}

type nokvEngine struct {
	opts      ycsbEngineOptions
	db        *local.DB
	valuePool sync.Pool
	valueSize int
	valueCap  int
	statsStop chan struct{}
	statsWG   sync.WaitGroup
	engineID  string
	name      string
}

func (e *nokvEngine) Name() string {
	if e.name != "" {
		return e.name
	}
	if e.engineID != "" {
		return e.engineID
	}
	return "NoKV"
}

func (e *nokvEngine) Open(clean bool) error {
	engineID := e.engineID
	if engineID == "" {
		engineID = "nokv"
	}
	dir := e.opts.engineDir(engineID)
	if clean {
		if err := ensureCleanDir(dir); err != nil {
			return fmt.Errorf("nokv: ensure dir: %w", err)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("nokv: mkdir: %w", err)
	}
	opt := buildNoKVBenchmarkOptions(dir, e.opts)
	db, err := local.Open(opt)
	if err != nil {
		return fmt.Errorf("nokv: open db: %w", err)
	}
	e.db = db
	e.valueSize = e.opts.ValueSize
	if e.valueSize <= 0 {
		e.valueSize = 1
	}
	e.valueCap = e.valueSize
	e.valuePool.New = func() any { return make([]byte, e.valueCap) }
	e.startStatsTicker()
	return nil
}

func (e *nokvEngine) Close() error {
	if e.statsStop != nil {
		close(e.statsStop)
		e.statsWG.Wait()
		e.statsStop = nil
	}
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

func (e *nokvEngine) Read(key []byte, dst []byte) ([]byte, error) {
	entry, err := e.db.Get(key)
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	out := append(dst[:0], entry.Value...)
	return out, nil
}

func (e *nokvEngine) Insert(key, value []byte) error {
	return e.db.Set(key, value)
}

func (e *nokvEngine) Update(key, value []byte) error {
	return e.db.Set(key, value)
}

func (e *nokvEngine) Scan(startKey []byte, count int) (int, error) {
	var read int
	var lastKey []byte
	it := e.db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = it.Close() }()
	it.Seek(startKey)
	for ; it.Valid() && read < count; it.Next() {
		item := it.Item()
		if item == nil || item.Entry() == nil {
			return 0, fmt.Errorf("nokv: iterator returned nil item during scan")
		}
		// YCSB scan counts records (logical keys), not MVCC/internal versions.
		// Skip duplicate user keys that may appear in iterator streams.
		key := item.Entry().Key
		if len(lastKey) > 0 && bytes.Equal(lastKey, key) {
			continue
		}
		lastKey = append(lastKey[:0], key...)
		// Values are already materialized for non-key-only iterators; touching
		// the entry avoids an extra per-item copy/allocation in benchmark scans.
		_ = len(item.Entry().Value)
		read++
	}
	return read, nil
}

func (e *nokvEngine) startStatsTicker() {
	interval := os.Getenv("NOKV_BENCH_STATS_INTERVAL")
	if interval == "" || e.db == nil {
		return
	}
	d, err := time.ParseDuration(interval)
	if err != nil || d <= 0 {
		return
	}
	e.statsStop = make(chan struct{})
	e.statsWG.Add(1)
	go func() {
		defer e.statsWG.Done()
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.printStats()
			case <-e.statsStop:
				return
			}
		}
	}()
}

func (e *nokvEngine) printStats() {
	if e.db == nil {
		return
	}
	snap := e.db.Info().Snapshot()
	var (
		l0Tables  int
		l0Bytes   int64
		l0Landing int
	)
	for _, lvl := range snap.LSM.Levels {
		if lvl.Level == 0 {
			l0Tables = lvl.TableCount
			l0Bytes = lvl.SizeBytes
			l0Landing = lvl.LandingTables
			break
		}
	}
	fmt.Printf("[NoKV Stats] entries=%d l0_tables=%d l0_bytes=%d l0_landing=%d flush_pending=%d compaction_backlog=%d compaction_max=%.2f write_q=%d write_entries=%d write_bytes=%d throttle=%v\n",
		snap.Entries,
		l0Tables,
		l0Bytes,
		l0Landing,
		snap.Flush.Pending,
		snap.Compaction.Backlog,
		snap.Compaction.MaxScore,
		snap.Write.QueueDepth,
		snap.Write.QueueEntries,
		snap.Write.QueueBytes,
		snap.Write.ThrottleActive,
	)
}
