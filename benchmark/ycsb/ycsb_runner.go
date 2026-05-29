// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"encoding/csv"
	"expvar"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ycsbWorkload struct {
	Name            string
	ReadRatio       float64
	UpdateRatio     float64
	InsertRatio     float64
	ScanRatio       float64
	ReadModifyWrite float64
	Distribution    string
	Description     string
}

var defaultYCSBWorkloads = map[string]ycsbWorkload{
	"A": {Name: "A", ReadRatio: 0.5, UpdateRatio: 0.5, Distribution: "zipfian", Description: "50/50 read/update"},
	"B": {Name: "B", ReadRatio: 0.95, UpdateRatio: 0.05, Distribution: "zipfian", Description: "95/5 read/update"},
	"C": {Name: "C", ReadRatio: 1.0, Distribution: "zipfian", Description: "100% read"},
	"D": {Name: "D", ReadRatio: 0.95, InsertRatio: 0.05, Distribution: "latest", Description: "95% read, 5% insert (latest)"},
	"E": {Name: "E", ScanRatio: 0.95, InsertRatio: 0.05, Distribution: "zipfian", Description: "95% scan, 5% insert"},
	"F": {Name: "F", ReadRatio: 0.5, ReadModifyWrite: 0.5, Distribution: "zipfian", Description: "read-modify-write"},
}

type ycsbConfig struct {
	BaseDir     string
	Seed        int64
	RecordCount int
	Operations  int
	WarmUpOps   int
	ValueSize   int
	ValueDist   string
	ValueMin    int
	ValueMax    int
	ValueStd    int
	ValuePct    string
	Concurrency int
	ScanLength  int
	TargetOps   int // overall ops/sec target; 0 = unlimited
	StatusEvery time.Duration
	Workloads   []ycsbWorkload
	Engines     []string
}

type ycsbKeyspace struct {
	baseRecords int64
	inserted    atomic.Int64
}

type valueSizer struct {
	dist        string
	fixed       int
	min         int
	max         int
	std         float64
	percentiles []pctSize
	maxSize     int
}

type pctSize struct {
	pct  float64
	size int
}

func newValueSizer(cfg ycsbConfig) valueSizer {
	dist := strings.ToLower(strings.TrimSpace(cfg.ValueDist))
	if dist == "" {
		dist = "fixed"
	}
	min := cfg.ValueMin
	max := cfg.ValueMax
	if min <= 0 {
		min = cfg.ValueSize
	}
	if max <= 0 {
		max = cfg.ValueSize
	}
	if max < min {
		max = min
	}
	std := cfg.ValueStd
	if std <= 0 {
		std = cfg.ValueSize / 4
	}
	percentiles, pctMax := parsePercentiles(cfg.ValuePct, cfg.ValueSize)
	maxSize := max
	if pctMax > maxSize {
		maxSize = pctMax
	}
	if cfg.ValueSize > maxSize {
		maxSize = cfg.ValueSize
	}
	return valueSizer{
		dist:        dist,
		fixed:       cfg.ValueSize,
		min:         min,
		max:         max,
		std:         float64(std),
		percentiles: percentiles,
		maxSize:     maxSize,
	}
}

func (vs valueSizer) Next(rng *rand.Rand) int {
	if rng == nil {
		return vs.fixed
	}
	switch vs.dist {
	case "uniform":
		if vs.max <= vs.min {
			return vs.min
		}
		return rng.Intn(vs.max-vs.min+1) + vs.min
	case "normal":
		mean := float64(vs.min+vs.max) / 2
		val := int(math.Round(rng.NormFloat64()*vs.std + mean))
		if val < vs.min {
			val = vs.min
		}
		if val > vs.max {
			val = vs.max
		}
		return val
	case "percentile":
		p := rng.Float64() * 100
		for _, ps := range vs.percentiles {
			if p <= ps.pct {
				if ps.size > 0 {
					return ps.size
				}
				break
			}
		}
		// fallback
		if len(vs.percentiles) > 0 {
			return vs.percentiles[len(vs.percentiles)-1].size
		}
		return vs.fixed
	default:
		return vs.fixed
	}
}

func parsePercentiles(spec string, def int) ([]pctSize, int) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, def
	}
	parts := strings.Split(spec, ",")
	out := make([]pctSize, 0, len(parts))
	maxSize := def
	for _, part := range parts {
		if part == "" {
			continue
		}
		kv := strings.Split(part, ":")
		if len(kv) != 2 {
			continue
		}
		p, err1 := strconv.ParseFloat(strings.TrimSpace(kv[0]), 64)
		sz, err2 := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err1 != nil || err2 != nil || p < 0 {
			continue
		}
		if p > 100 {
			p = 100
		}
		if sz <= 0 {
			continue
		}
		out = append(out, pctSize{pct: p, size: sz})
		if sz > maxSize {
			maxSize = sz
		}
	}
	if len(out) == 0 {
		return nil, def
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].pct == out[j].pct {
			return out[i].size < out[j].size
		}
		return out[i].pct < out[j].pct
	})
	return out, maxSize
}

func (ks *ycsbKeyspace) totalRecords() int64 {
	return ks.baseRecords + ks.inserted.Load()
}

func (ks *ycsbKeyspace) nextInsertID() int64 {
	n := ks.inserted.Add(1)
	return ks.baseRecords + n - 1
}

func chooseScanLength(rng *rand.Rand, maxLen int) int {
	if maxLen <= 0 {
		return 1
	}
	if maxLen == 1 || rng == nil {
		return maxLen
	}
	// Official workload E uses maxscanlength + uniform scan length distribution.
	return rng.Intn(maxLen) + 1
}

func ycsbKeyNumWidth(recordCount, operations int) int {
	maxKeyNum := int64(recordCount) + int64(operations)
	if maxKeyNum <= 0 {
		return 1
	}
	return len(strconv.FormatInt(maxKeyNum-1, 10))
}

func ycsbHashKeyNum(keyNum int64) int64 {
	if keyNum < 0 {
		keyNum = 0
	}
	val := uint64(keyNum)
	var hash uint64 = 0xcbf29ce484222325
	const prime uint64 = 1099511628211
	for i := 0; i < 8; i++ {
		octet := val & 0x00ff
		val >>= 8
		hash ^= octet
		hash *= prime
	}
	signed := int64(hash)
	if signed < 0 {
		signed = -signed
		// Preserve non-negative behavior even for MinInt64.
		if signed < 0 {
			return 0
		}
	}
	return signed
}

type keyGenerator interface {
	Next(max int64) int64
}

type zipfGenerator struct {
	r    *rand.Rand
	zipf *rand.Zipf
}

func newZipfGenerator(r *rand.Rand, span uint64) *zipfGenerator {
	if span == 0 {
		span = 1
	}
	return &zipfGenerator{
		r:    r,
		zipf: rand.NewZipf(r, 1.01, 1, span),
	}
}

func (g *zipfGenerator) Next(max int64) int64 {
	if max <= 0 {
		return 0
	}
	for {
		v := int64(g.zipf.Uint64())
		if v < max {
			return v
		}
	}
}

type uniformGenerator struct {
	r *rand.Rand
}

func (g *uniformGenerator) Next(max int64) int64 {
	if max <= 0 {
		return 0
	}
	return g.r.Int63n(max)
}

type latestGenerator struct {
	state *ycsbKeyspace
	zipf  *rand.Zipf
}

func newLatestGenerator(r *rand.Rand, span uint64, state *ycsbKeyspace) *latestGenerator {
	if span == 0 {
		span = 1
	}
	return &latestGenerator{
		state: state,
		zipf:  rand.NewZipf(r, 1.01, 1, span),
	}
}

func (g *latestGenerator) Next(max int64) int64 {
	total := g.state.totalRecords()
	if total <= 0 {
		return 0
	}
	latest := total - 1
	offset := int64(g.zipf.Uint64())
	id := latest - offset
	if id < 0 {
		id = 0
	}
	if id >= max {
		return latest % max
	}
	return id
}

func runYCSBBenchmarks(cfg ycsbConfig, opts ycsbEngineOptions) ([]BenchmarkResult, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = runtime.GOMAXPROCS(0)
		if cfg.Concurrency <= 0 {
			cfg.Concurrency = 1
		}
	}
	results := make([]BenchmarkResult, 0)
	for _, engineName := range cfg.Engines {
		engineName = strings.ToLower(strings.TrimSpace(engineName))
		if engineName == "" {
			continue
		}
		engine, err := newYCSBEngine(engineName, opts)
		if err != nil {
			return nil, err
		}
		engineResults, err := runYCSBWorkloadsOnEngine(engine, cfg)
		if err != nil {
			return nil, err
		}
		results = append(results, engineResults...)
	}
	return results, nil
}

func newYCSBEngine(engineName string, opts ycsbEngineOptions) (ycsbEngine, error) {
	switch engineName {
	case "nokv":
		return newNoKVEngine(opts), nil
	case "badger":
		return newBadgerEngine(opts), nil
	case "pebble":
		return newPebbleEngine(opts), nil
	case "rocksdb":
		return newRocksDBEngine(opts), nil
	default:
		return nil, fmt.Errorf("unknown engine %q", engineName)
	}
}

func runYCSBWorkloadsOnEngine(engine ycsbEngine, cfg ycsbConfig) ([]BenchmarkResult, error) {
	results := make([]BenchmarkResult, 0, len(cfg.Workloads))
	for _, workload := range cfg.Workloads {
		res, err := runIsolatedWorkload(engine, cfg, workload)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}

func runIsolatedWorkload(engine ycsbEngine, cfg ycsbConfig, workload ycsbWorkload) (res BenchmarkResult, rerr error) {
	if err := engine.Open(true); err != nil {
		return BenchmarkResult{}, fmt.Errorf("%s open: %w", engine.Name(), err)
	}
	defer func() {
		if err := engine.Close(); rerr == nil && err != nil {
			rerr = err
		}
	}()

	state := &ycsbKeyspace{baseRecords: int64(cfg.RecordCount)}
	if err := ycsbLoad(engine, cfg, state); err != nil {
		return BenchmarkResult{}, fmt.Errorf("%s load: %w", engine.Name(), err)
	}

	if cfg.WarmUpOps > 0 {
		warmCfg := cfg
		warmCfg.Operations = cfg.WarmUpOps
		warmCfg.WarmUpOps = 0
		if _, err := ycsbRunWorkload(engine, warmCfg, state, workload); err != nil {
			return BenchmarkResult{}, fmt.Errorf("%s warmup %s: %w", engine.Name(), workload.Name, err)
		}
	}

	res, err := ycsbRunWorkload(engine, cfg, state, workload)
	if err != nil {
		return BenchmarkResult{}, err
	}
	return res, nil
}

func ycsbLoad(engine ycsbEngine, cfg ycsbConfig, state *ycsbKeyspace) error {
	records := cfg.RecordCount
	keyWidth := ycsbKeyNumWidth(cfg.RecordCount, cfg.Operations)
	keyFor := func(id int64) []byte {
		return formatYCSBKey(id, keyWidth)
	}
	perWorker := (records + cfg.Concurrency - 1) / cfg.Concurrency
	errCh := make(chan error, cfg.Concurrency)
	var wg sync.WaitGroup
	sizer := newValueSizer(cfg)
	for worker := 0; worker < cfg.Concurrency; worker++ {
		start := worker * perWorker
		end := min((worker+1)*perWorker, records)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(s)))
			for i := s; i < e; i++ {
				key := keyFor(int64(i))
				val := randomValue(rng, sizer.Next(rng))
				if err := engine.Insert(key, val); err != nil {
					errCh <- err
					return
				}
				valueBufPool.Put(val)
			}
		}(start, end)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	state.inserted.Store(0)
	return nil
}

func ycsbRunWorkload(engine ycsbEngine, cfg ycsbConfig, state *ycsbKeyspace, wl ycsbWorkload) (BenchmarkResult, error) {
	totalOps := cfg.Operations
	opsPerWorker := (totalOps + cfg.Concurrency - 1) / cfg.Concurrency
	if cfg.ScanLength <= 0 {
		cfg.ScanLength = 1
	}
	keyWidth := ycsbKeyNumWidth(cfg.RecordCount, cfg.Operations)
	keyFor := func(id int64) []byte {
		return formatYCSBKey(id, keyWidth)
	}
	latRec := newLatencyRecorder(totalOps)
	valRec := newIntRecorder(totalOps)
	var loadedOps atomic.Int64
	var (
		readOps   atomic.Int64
		updateOps atomic.Int64
		insertOps atomic.Int64
		scanOps   atomic.Int64
		rmwOps    atomic.Int64
		scanItems atomic.Int64
		dataBytes atomic.Int64
	)
	start := time.Now()
	errCh := make(chan error, cfg.Concurrency)
	var wg sync.WaitGroup

	// Optional overall throughput throttle.
	var targetInterval time.Duration
	if cfg.TargetOps > 0 {
		// Spread tokens evenly over a second; clamp to avoid zero duration.
		perOp := time.Second / time.Duration(cfg.TargetOps)
		if perOp < time.Microsecond {
			perOp = time.Microsecond
		}
		targetInterval = perOp
	}

	// Optional status ticker.
	var statusStop chan struct{}
	if cfg.StatusEvery > 0 {
		statusStop = make(chan struct{})
		go func() {
			ticker := time.NewTicker(cfg.StatusEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					done := loadedOps.Load()
					elapsed := time.Since(start).Seconds()
					opsRate := float64(done) / elapsed
					fmt.Printf("[YCSB %s %s] ops=%d rate=%.0f ops/s elapsed=%.1fs\n",
						engine.Name(), wl.Name, done, opsRate, elapsed)
				case <-statusStop:
					return
				}
			}
		}()
	}

	keySpan := uint64(cfg.RecordCount + cfg.Operations*2)
	sizer := newValueSizer(cfg)
	maxBuf := sizer.maxSize
	if maxBuf <= 0 {
		maxBuf = cfg.ValueSize
	}

	for worker := 0; worker < cfg.Concurrency; worker++ {
		localStart := worker * opsPerWorker
		localEnd := min((worker+1)*opsPerWorker, totalOps)
		if localStart >= localEnd {
			continue
		}
		wg.Add(1)
		go func(idx int, s, e int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(idx) + 1))
			readGen := newKeyGenerator(wl.Distribution, rng, keySpan, state)

			for i := s; i < e; i++ {
				opType := chooseOperation(rng, wl)
				startOp := time.Now()
				switch opType {
				case "read":
					id := selectExistingKeyID(readGen, state)
					key := keyFor(id)
					valBuf := valueBufPool.Get(maxBuf)
					out, err := engine.Read(key, valBuf)
					if err != nil {
						errCh <- fmt.Errorf("%s read: %w", engine.Name(), err)
						return
					}
					valRec.Record(len(out))
					valueBufPool.Put(valBuf)
					readOps.Add(1)
					dataBytes.Add(int64(len(out)))
				case "update":
					id := selectExistingKeyID(readGen, state)
					key := keyFor(id)
					sz := sizer.Next(rng)
					val := randomValue(rng, sz)
					if err := engine.Update(key, val); err != nil {
						errCh <- fmt.Errorf("%s update: %w", engine.Name(), err)
						return
					}
					valueBufPool.Put(val)
					updateOps.Add(1)
					valRec.Record(sz)
					dataBytes.Add(int64(sz))
				case "insert":
					id := state.nextInsertID()
					key := keyFor(id)
					sz := sizer.Next(rng)
					val := randomValue(rng, sz)
					if err := engine.Insert(key, val); err != nil {
						errCh <- fmt.Errorf("%s insert: %w", engine.Name(), err)
						return
					}
					valueBufPool.Put(val)
					insertOps.Add(1)
					valRec.Record(sz)
					dataBytes.Add(int64(sz))
				case "scan":
					id := selectExistingKeyID(readGen, state)
					key := keyFor(id)
					scanLen := chooseScanLength(rng, cfg.ScanLength)
					items, err := engine.Scan(key, scanLen)
					if err != nil {
						errCh <- fmt.Errorf("%s scan: %w", engine.Name(), err)
						return
					}
					scanOps.Add(1)
					if items > 0 {
						scanItems.Add(int64(items))
						dataBytes.Add(int64(items) * int64(cfg.ValueSize))
					}
				case "readmodifywrite":
					id := selectExistingKeyID(readGen, state)
					key := keyFor(id)
					valBuf := valueBufPool.Get(maxBuf)
					out, err := engine.Read(key, valBuf)
					if err != nil {
						errCh <- fmt.Errorf("%s read: %w", engine.Name(), err)
						return
					}
					readSize := len(out)
					val := randomValue(rng, sizer.Next(rng))
					if err := engine.Update(key, val); err != nil {
						errCh <- fmt.Errorf("%s update: %w", engine.Name(), err)
						return
					}
					valueBufPool.Put(valBuf)
					valueBufPool.Put(val)
					rmwOps.Add(1)
					valRec.Record(readSize)
					valRec.Record(len(val))
					dataBytes.Add(int64(readSize + len(val)))
				default:
					errCh <- fmt.Errorf("unknown op %s", opType)
					return
				}
				latRec.Record(time.Since(startOp).Nanoseconds())
				loadedOps.Add(1)
				if targetInterval > 0 {
					time.Sleep(targetInterval)
				}
			}
		}(worker, localStart, localEnd)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return BenchmarkResult{}, err
		}
	}
	duration := time.Since(start)
	if statusStop != nil {
		close(statusStop)
	}

	result := BenchmarkResult{
		Name:               fmt.Sprintf("%s-YCSB-%s", engine.Name(), wl.Name),
		Engine:             engine.Name(),
		Operation:          fmt.Sprintf("YCSB-%s", wl.Name),
		Mode:               wl.Description,
		StartTime:          start,
		EndTime:            start.Add(duration),
		TotalDuration:      duration,
		TotalOperations:    loadedOps.Load(),
		DataBytes:          dataBytes.Load(),
		ReadOps:            readOps.Load(),
		UpdateOps:          updateOps.Load(),
		InsertOps:          insertOps.Load(),
		ScanOps:            scanOps.Load(),
		ReadModifyWriteOps: rmwOps.Load(),
		ScanItems:          scanItems.Load(),
		ValueAvg:           valRec.Average(),
		ValueP50:           valRec.Percentile(50),
		ValueP95:           valRec.Percentile(95),
		ValueP99:           valRec.Percentile(99),
	}
	result.Finalize()
	result.P50LatencyNS = latRec.Percentile(50)
	result.P95LatencyNS = latRec.Percentile(95)
	result.P99LatencyNS = latRec.Percentile(99)
	return result, nil
}

func newKeyGenerator(dist string, rng *rand.Rand, span uint64, state *ycsbKeyspace) keyGenerator {
	switch strings.ToLower(dist) {
	case "latest":
		return newLatestGenerator(rng, span, state)
	case "uniform":
		return &uniformGenerator{r: rng}
	default:
		return newZipfGenerator(rng, span)
	}
}

func chooseOperation(r *rand.Rand, wl ycsbWorkload) string {
	p := r.Float64()
	switch {
	case p < wl.ReadRatio:
		return "read"
	case p < wl.ReadRatio+wl.UpdateRatio:
		return "update"
	case p < wl.ReadRatio+wl.UpdateRatio+wl.InsertRatio:
		return "insert"
	case p < wl.ReadRatio+wl.UpdateRatio+wl.InsertRatio+wl.ScanRatio:
		return "scan"
	case p < wl.ReadRatio+wl.UpdateRatio+wl.InsertRatio+wl.ScanRatio+wl.ReadModifyWrite:
		return "readmodifywrite"
	default:
		return "read"
	}
}

func selectExistingKeyID(gen keyGenerator, state *ycsbKeyspace) int64 {
	count := state.totalRecords()
	if count <= 0 {
		return 0
	}
	id := gen.Next(count)
	if id < 0 {
		id = 0
	}
	if id >= count {
		id = count - 1
	}
	return id
}

func selectExistingKey(gen keyGenerator, state *ycsbKeyspace) []byte {
	return formatYCSBKey(selectExistingKeyID(gen, state), 12)
}

func formatYCSBKey(id int64, width int) []byte {
	if id < 0 {
		id = 0
	}
	id = ycsbHashKeyNum(id)
	if width < 1 {
		width = 1
	}
	decimal := strconv.FormatInt(id, 10)
	if len(decimal) < width {
		padded := make([]byte, 0, width)
		for i := 0; i < width-len(decimal); i++ {
			padded = append(padded, '0')
		}
		padded = append(padded, decimal...)
		decimal = string(padded)
	}
	buf := make([]byte, 0, 4+len(decimal))
	buf = append(buf, 'u', 's', 'e', 'r')
	buf = append(buf, decimal...)
	return buf
}

var (
	valueBufPool = &valuePool{
		pool:     sync.Pool{},
		gets:     expvar.NewInt("local.Benchmark.ValuePool.Gets"),
		releases: expvar.NewInt("local.Benchmark.ValuePool.Releases"),
	}
)

type valuePool struct {
	pool     sync.Pool
	gets     *expvar.Int
	releases *expvar.Int
}

func (vp *valuePool) Get(size int) []byte {
	vp.gets.Add(1)
	if size <= 0 {
		return nil
	}
	if v := vp.pool.Get(); v != nil {
		if bufPtr, ok := v.(*[]byte); ok {
			buf := *bufPtr
			if cap(buf) >= size {
				return buf[:size]
			}
		}
	}
	return make([]byte, size)
}

func (vp *valuePool) Put(buf []byte) {
	if buf == nil {
		return
	}
	vp.releases.Add(1)
	buf = buf[:0]
	vp.pool.Put(&buf)
}

func randomValue(rng *rand.Rand, size int) []byte {
	buf := valueBufPool.Get(size)
	if size <= 0 {
		return buf
	}
	if _, err := rng.Read(buf); err != nil {
		for i := range buf {
			buf[i] = byte(rng.Intn(256))
		}
	}
	return buf
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func writeYCSBSummary(results []BenchmarkResult, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	file := filepath.Join(dir, fmt.Sprintf("ycsb_results_%s.csv", time.Now().Format("20060102_150405")))
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{"name", "engine", "workload", "ops", "ops_per_sec", "avg_ns", "p50_ns", "p95_ns", "p99_ns", "duration_ns", "data_bytes", "data_mb", "reads", "updates", "inserts", "scans", "scan_items", "rmw", "val_avg", "val_p50", "val_p95", "val_p99"}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, r := range results {
		row := []string{
			r.Name,
			r.Engine,
			r.Operation,
			fmt.Sprintf("%d", r.TotalOperations),
			fmt.Sprintf("%.0f", r.Throughput),
			fmt.Sprintf("%.0f", r.AvgLatencyNS),
			fmt.Sprintf("%.0f", r.P50LatencyNS),
			fmt.Sprintf("%.0f", r.P95LatencyNS),
			fmt.Sprintf("%.0f", r.P99LatencyNS),
			fmt.Sprintf("%d", r.TotalDuration.Nanoseconds()),
			fmt.Sprintf("%d", r.DataBytes),
			fmt.Sprintf("%.2f", r.DataSize),
			fmt.Sprintf("%d", r.ReadOps),
			fmt.Sprintf("%d", r.UpdateOps),
			fmt.Sprintf("%d", r.InsertOps),
			fmt.Sprintf("%d", r.ScanOps),
			fmt.Sprintf("%d", r.ScanItems),
			fmt.Sprintf("%d", r.ReadModifyWriteOps),
			fmt.Sprintf("%.0f", r.ValueAvg),
			fmt.Sprintf("%.0f", r.ValueP50),
			fmt.Sprintf("%.0f", r.ValueP95),
			fmt.Sprintf("%.0f", r.ValueP99),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func parseYCSBWorkloads(spec string) ([]ycsbWorkload, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, fmt.Errorf("empty workload list")
	}
	parts := strings.Split(spec, ",")
	workloads := make([]ycsbWorkload, 0, len(parts))
	for _, part := range parts {
		name := strings.ToUpper(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		wl, ok := defaultYCSBWorkloads[name]
		if !ok {
			return nil, fmt.Errorf("unknown workload %q", name)
		}
		workloads = append(workloads, wl)
	}
	if len(workloads) == 0 {
		return nil, fmt.Errorf("no workloads selected")
	}
	return workloads, nil
}

func parseYCSBEngines(spec string) []string {
	parts := strings.Split(spec, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
