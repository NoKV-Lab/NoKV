// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func SummaryRows(result Result) []SummaryRow {
	byOp := make(map[string][]Sample)
	for _, sample := range result.Samples {
		byOp[sample.Operation] = append(byOp[sample.Operation], sample)
	}
	ops := make([]string, 0, len(byOp))
	for op := range byOp {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	rows := make([]SummaryRow, 0, len(ops))
	for _, op := range ops {
		samples := byOp[op]
		profile := ProfileFor(result.Name)
		var total time.Duration
		var sampleErrors int
		latencies := make([]time.Duration, 0, len(samples))
		for _, sample := range samples {
			total += sample.Duration
			latencies = append(latencies, sample.Duration)
			if sample.Error != "" {
				sampleErrors++
			}
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		count := len(samples)
		avgUS := 0.0
		if count > 0 {
			avgUS = float64(total.Microseconds()) / float64(count)
		}
		rows = append(rows, SummaryRow{
			Workload:             result.Name,
			Driver:               result.Driver,
			RunID:                result.RunID,
			Source:               profile.Source,
			SourceURL:            profile.SourceURL,
			Projection:           profile.Projection,
			Operation:            op,
			Count:                count,
			Errors:               sampleErrors,
			Throughput:           throughput(count, result.Duration),
			ActiveThroughput:     throughput(count, total),
			ActiveDurationSecs:   total.Seconds(),
			AverageUS:            avgUS,
			P50US:                percentileUS(latencies, 0.50),
			P95US:                percentileUS(latencies, 0.95),
			P99US:                percentileUS(latencies, 0.99),
			WorkloadDurationSecs: result.Duration.Seconds(),
		})
	}
	return rows
}

func WriteSummaryCSV(w io.Writer, rows []SummaryRow) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{
		"workload",
		"driver",
		"run_id",
		"source",
		"source_url",
		"projection",
		"operation",
		"count",
		"errors",
		"throughput_ops_sec",
		"active_ops_per_sec",
		"active_duration_sec",
		"avg_latency_us",
		"p50_latency_us",
		"p95_latency_us",
		"p99_latency_us",
		"workload_duration_sec",
	}); err != nil {
		return err
	}
	for _, row := range rows {
		if err := cw.Write([]string{
			row.Workload,
			row.Driver,
			row.RunID,
			row.Source,
			row.SourceURL,
			row.Projection,
			row.Operation,
			strconv.Itoa(row.Count),
			strconv.Itoa(row.Errors),
			formatFloat(row.Throughput),
			formatFloat(row.ActiveThroughput),
			formatFloat(row.ActiveDurationSecs),
			formatFloat(row.AverageUS),
			formatFloat(row.P50US),
			formatFloat(row.P95US),
			formatFloat(row.P99US),
			formatFloat(row.WorkloadDurationSecs),
		}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func NewRunID() string {
	return time.Now().UTC().Format("20060102T150405.000000000")
}

type recorder struct {
	mu      sync.Mutex
	samples []Sample
}

func newRecorder() *recorder {
	return &recorder{}
}

func (r *recorder) record(operation string, duration time.Duration, err error) {
	sample := Sample{Operation: operation, Duration: duration}
	if err != nil {
		sample.Error = err.Error()
	}
	r.mu.Lock()
	r.samples = append(r.samples, sample)
	r.mu.Unlock()
}

func (r *recorder) recordCall(operation string, fn func() error) {
	duration, err := timeCall(fn)
	r.record(operation, duration, err)
}

func (r *recorder) snapshot() []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Sample, len(r.samples))
	copy(out, r.samples)
	return out
}

func timeCall(fn func() error) (time.Duration, error) {
	start := time.Now()
	var err error
	for attempt := 0; attempt < maxOperationAttempts; attempt++ {
		err = fn()
		if !shouldRetryOperation(err) || attempt+1 == maxOperationAttempts {
			return time.Since(start), err
		}
		time.Sleep(operationRetryBackoff(attempt))
	}
	return time.Since(start), err
}

func shouldRetryOperation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return false
	}
	return nokverrors.Retryable(err)
}

func operationRetryBackoff(attempt int) time.Duration {
	backoff := operationRetryBase
	for i := 0; i < attempt; i++ {
		backoff *= 2
		if backoff >= operationRetryMax {
			return operationRetryMax
		}
	}
	return backoff
}

func finishResult(name, runID string, started time.Time, samples []Sample) (Result, error) {
	result := Result{
		Name:      name,
		RunID:     runID,
		StartedAt: started,
		Duration:  time.Since(started),
		Ops:       len(samples),
		Samples:   samples,
	}
	for _, sample := range samples {
		if sample.Error != "" {
			result.Errors++
		}
	}
	if result.Errors > 0 {
		return result, fmt.Errorf("%w: %d/%d operations failed; samples: %s", ErrWorkloadFailed, result.Errors, result.Ops, firstErrorSummary(samples, 3))
	}
	return result, nil
}

func firstErrorSummary(samples []Sample, limit int) string {
	if limit <= 0 {
		limit = 1
	}
	out := make([]string, 0, limit)
	for _, sample := range samples {
		if sample.Error == "" {
			continue
		}
		out = append(out, fmt.Sprintf("%s: %s", sample.Operation, sample.Error))
		if len(out) >= limit {
			break
		}
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, "; ")
}

type watchStarts struct {
	mu     sync.Mutex
	values map[string]time.Time
}

func newWatchStarts() *watchStarts {
	return &watchStarts{values: make(map[string]time.Time)}
}

func (s *watchStarts) put(name string, started time.Time) {
	s.mu.Lock()
	s.values[name] = started
	s.mu.Unlock()
}

func (s *watchStarts) pop(name string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	started, ok := s.values[name]
	if ok {
		delete(s.values, name)
	}
	return started, ok
}

func (s *watchStarts) delete(name string) {
	s.mu.Lock()
	delete(s.values, name)
	s.mu.Unlock()
}

func closeWatchAfterDelivery(stream fsmetaclient.WatchSubscription, expected, delivered *atomic.Int64) {
	deadline := time.NewTimer(watchTailTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(watchDeliveryPoll)
	defer ticker.Stop()
	for {
		if delivered.Load() >= expected.Load() {
			_ = stream.Close()
			return
		}
		select {
		case <-deadline.C:
			_ = stream.Close()
			return
		case <-ticker.C:
		}
	}
}

func collectWatchEvents(ctx context.Context, stream fsmetaclient.WatchSubscription, starts *watchStarts, expected, delivered *atomic.Int64, writesDone <-chan struct{}, operation string, rec *recorder, done chan<- error) {
	received := 0
	for {
		select {
		case <-writesDone:
			if received >= int(expected.Load()) {
				done <- nil
				return
			}
			writesDone = nil
		default:
		}
		evt, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) && received >= int(expected.Load()) {
				done <- nil
				return
			}
			if errors.Is(err, io.EOF) {
				done <- fmt.Errorf("%s received %d/%d notifications before stream closed", operation, received, expected.Load())
				return
			}
			done <- err
			return
		}
		_ = stream.Ack(evt.Cursor)
		name, ok := fsmeta.DentryNameOfKey(evt.Key)
		if !ok {
			continue
		}
		if started, ok := starts.pop(name); ok {
			rec.record(operation, time.Since(started), nil)
			received++
			delivered.Add(1)
		}
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		default:
		}
	}
}

func waitForWatchName(ctx context.Context, stream fsmetaclient.WatchSubscription, want string) error {
	for {
		evt, err := stream.Recv()
		if err != nil {
			return err
		}
		_ = stream.Ack(evt.Cursor)
		if name, ok := fsmeta.DentryNameOfKey(evt.Key); ok && name == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func runParallel(workers, total int, fn func(int)) {
	if workers <= 0 {
		workers = 1
	}
	if total <= 0 {
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := int(next.Add(1)) - 1
				if idx >= total {
					return
				}
				fn(idx)
			}
		}()
	}
	wg.Wait()
}

func recordCall(rec *recorder, operation string, fn func() error) error {
	duration, err := timeCall(fn)
	rec.record(operation, duration, err)
	return err
}

func hasRecordedErrors(samples []Sample) bool {
	for _, sample := range samples {
		if sample.Error != "" {
			return true
		}
	}
	return false
}

func defaultMount(mount model.MountID) model.MountID {
	if mount == "" {
		return "fsmeta-workload"
	}
	return mount
}

func defaultRunID(runID string) string {
	if runID == "" {
		return NewRunID()
	}
	return runID
}

func defaultInt(value, configured, fallback int) int {
	if value > 0 {
		return value
	}
	if configured > 0 {
		return configured
	}
	return fallback
}

func defaultUint32(value, configured, fallback uint32) uint32 {
	if value > 0 {
		return value
	}
	if configured > 0 {
		return configured
	}
	return fallback
}

func clampReadDirLimit(limit uint32, fallback int) uint32 {
	if limit == 0 && fallback > 0 {
		limit = uint32(fallback)
	}
	if limit == 0 {
		limit = model.DefaultReadDirLimit
	}
	if limit > model.MaxReadDirLimit {
		return model.MaxReadDirLimit
	}
	return limit
}

func percentileUS(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return float64(sorted[0].Microseconds())
	}
	if p >= 1 {
		return float64(sorted[len(sorted)-1].Microseconds())
	}
	idx := int(float64(len(sorted)-1) * p)
	return float64(sorted[idx].Microseconds())
}

func throughput(count int, duration time.Duration) float64 {
	if count == 0 || duration <= 0 {
		return 0
	}
	return float64(count) / duration.Seconds()
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 3, 64)
}
