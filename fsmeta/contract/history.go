// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

const (
	maxConcurrentHistoryBatch   = 5
	defaultMaxHistoryCandidates = 128
)

type scheduledOperation struct {
	index int
	op    Operation
}

type observedOperation struct {
	index    int
	op       Operation
	result   Result
	started  int64
	finished int64
}

type HistoryOptions struct {
	// AllowIndeterminateErrors treats retryable transport/availability errors as
	// operations whose commit outcome is unknown. Docker chaos uses this mode
	// because a restarted store can return Unavailable after the request crossed
	// the server boundary; strict in-process tests keep the default false value.
	AllowIndeterminateErrors bool

	// MaxCandidates bounds the number of ambiguous model states retained after a
	// concurrent batch. A small cap keeps nightly checks deterministic while still
	// preserving alternative legal write orders for later reads to disambiguate.
	MaxCandidates int
}

type historyCandidate struct {
	model *Model
	order []int
}

// RunConcurrentBatches executes generated fsmeta operations in small
// overlapping batches and checks every completed batch against a bounded
// linearization oracle. It is deliberately factorial and bounded: the point is
// to catch non-serializable metadata histories in nightly runs without turning
// the regular PR path into a large model checker.
func RunConcurrentBatches(ctx context.Context, exec Executor, state *Model, ops []Operation, batchSize int, opts HistoryOptions) error {
	if exec == nil {
		return errExecutorRequired
	}
	if state == nil {
		return errModelRequired
	}
	if batchSize <= 1 && !opts.AllowIndeterminateErrors {
		return Run(ctx, exec, state, ops)
	}
	if batchSize > maxConcurrentHistoryBatch {
		return fmt.Errorf("fsmeta/contract: concurrent batch size %d exceeds max %d", batchSize, maxConcurrentHistoryBatch)
	}
	maxCandidates := opts.MaxCandidates
	if maxCandidates <= 0 {
		maxCandidates = defaultMaxHistoryCandidates
	}

	history := make([]string, 0, len(ops))
	candidates := []*Model{cloneModel(state)}
	batch := make([]scheduledOperation, 0, batchSize)
	batchID := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		observed := executeConcurrentBatch(ctx, exec, candidates[0], batch)
		next, orders, err := linearizeCandidateBatch(candidates, observed, opts, maxCandidates)
		for _, event := range observed {
			history = append(history, fmt.Sprintf("%03d batch=%03d start=%d finish=%d %s -> got=%s",
				event.index, batchID, event.started, event.finished, event.op, summarize(event.result)))
		}
		if err != nil {
			return fmt.Errorf("batch %d failed: %w\nbatch:\n%s\nhistory:\n%s",
				batchID, err, describeObserved(observed), strings.Join(history, "\n"))
		}
		candidates = next
		history = append(history, fmt.Sprintf("batch %03d candidates=%d first_linearized_as=%s",
			batchID, len(candidates), describeOrder(observed, orders[0])))
		replaceModel(state, candidates[0])
		if err := state.CheckInvariants(); err != nil {
			return fmt.Errorf("batch %d corrupted model invariants: %w\nhistory:\n%s", batchID, err, strings.Join(history, "\n"))
		}
		batch = batch[:0]
		batchID++
		return nil
	}

	for i, op := range ops {
		if historyBarrier(op) {
			if err := flush(); err != nil {
				return err
			}
			next, err := runSequentialObservedCandidates(ctx, exec, candidates, i, op, opts, maxCandidates, &history)
			if err != nil {
				return err
			}
			candidates = next
			replaceModel(state, candidates[0])
			continue
		}
		batch = append(batch, scheduledOperation{index: i, op: op})
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func historyBarrier(op Operation) bool {
	switch op.Kind {
	case OpAdvanceTime, OpSnapshotSubtree:
		return true
	default:
		return false
	}
}

func runSequentialObservedCandidates(ctx context.Context, exec Executor, candidates []*Model, index int, op Operation, opts HistoryOptions, maxCandidates int, history *[]string) ([]*Model, error) {
	got := execute(ctx, exec, candidates[0], op)
	next, firstMismatch := advanceCandidates(candidates, op, got, opts, maxCandidates)
	want := Result{}
	if len(candidates) != 0 {
		want = applyObserved(cloneModel(candidates[0]), op, got)
	}
	*history = append(*history, fmt.Sprintf("%03d sequential candidates=%d %s -> got=%s want=%s", index, len(next), op, summarize(got), summarize(want)))
	if len(next) == 0 {
		if firstMismatch == nil {
			firstMismatch = fmt.Errorf("no candidate model accepted observed result")
		}
		return nil, fmt.Errorf("step %d failed: %w\nhistory:\n%s", index, firstMismatch, strings.Join(*history, "\n"))
	}
	if err := next[0].CheckInvariants(); err != nil {
		return nil, fmt.Errorf("step %d corrupted model invariants: %w\nhistory:\n%s", index, err, strings.Join(*history, "\n"))
	}
	return next, nil
}

func executeConcurrentBatch(ctx context.Context, exec Executor, model *Model, batch []scheduledOperation) []observedOperation {
	observed := make([]observedOperation, len(batch))
	var seq atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, entry := range batch {
		observed[i].index = entry.index
		observed[i].op = entry.op
		wg.Go(func() {
			<-start
			observed[i].started = seq.Add(1)
			observed[i].result = execute(ctx, exec, model, entry.op)
			observed[i].finished = seq.Add(1)
		})
	}
	close(start)
	wg.Wait()
	return observed
}

func linearizeCandidateBatch(base []*Model, observed []observedOperation, opts HistoryOptions, maxCandidates int) ([]*Model, [][]int, error) {
	next := make([]historyCandidate, 0, len(base))
	seen := make(map[string]struct{}, maxCandidates)
	var firstMismatch error
	for _, candidate := range base {
		matches, err := linearizeBatchCandidates(candidate, observed, opts, maxCandidates)
		if err != nil && firstMismatch == nil {
			firstMismatch = err
		}
		for _, match := range matches {
			next = appendUniqueHistoryCandidate(next, seen, match, maxCandidates)
			if len(next) >= maxCandidates {
				break
			}
		}
		if len(next) >= maxCandidates {
			break
		}
	}
	if len(next) == 0 {
		if firstMismatch == nil {
			firstMismatch = fmt.Errorf("no candidate operation respects real-time constraints")
		}
		return nil, nil, fmt.Errorf("no serial order matched observed concurrent results: %w", firstMismatch)
	}
	models := make([]*Model, 0, len(next))
	orders := make([][]int, 0, len(next))
	for _, candidate := range next {
		models = append(models, candidate.model)
		orders = append(orders, candidate.order)
	}
	return models, orders, nil
}

func linearizeBatchCandidates(base *Model, observed []observedOperation, opts HistoryOptions, limit int) ([]historyCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	used := make([]bool, len(observed))
	order := make([]int, 0, len(observed))
	var firstMismatch error
	out := make([]historyCandidate, 0, limit)

	var search func(*Model)
	search = func(current *Model) {
		if len(out) >= limit {
			return
		}
		if len(order) == len(observed) {
			out = append(out, historyCandidate{
				model: current,
				order: append([]int(nil), order...),
			})
			return
		}
		for i := range observed {
			if used[i] || !respectsRealTime(i, used, observed) {
				continue
			}
			nextModels, err := advanceOneCandidate(current, observed[i].op, observed[i].result, opts)
			if err != nil {
				if firstMismatch == nil {
					firstMismatch = fmt.Errorf("op %03d %s: %w", observed[i].index, observed[i].op, err)
				}
				continue
			}
			used[i] = true
			order = append(order, i)
			for _, next := range nextModels {
				search(next)
				if len(out) >= limit {
					break
				}
			}
			order = order[:len(order)-1]
			used[i] = false
		}
	}

	search(cloneModel(base))
	out = dedupeHistoryCandidates(out, limit)
	if len(out) != 0 {
		return out, nil
	}
	if firstMismatch == nil {
		firstMismatch = fmt.Errorf("no candidate operation respects real-time constraints")
	}
	return nil, fmt.Errorf("no serial order matched observed concurrent results: %w", firstMismatch)
}

func respectsRealTime(candidate int, used []bool, observed []observedOperation) bool {
	for i := range observed {
		if i == candidate || used[i] {
			continue
		}
		if observed[i].finished < observed[candidate].started {
			return false
		}
	}
	return true
}

func applyObserved(state *Model, op Operation, got Result) Result {
	if op.Kind == OpCreate && got.Err == nil {
		op.Inode = got.Inode.Inode
	}
	if op.Kind == OpSnapshotSubtree {
		if got.Err == nil {
			return state.ApplySnapshot(op, got.Token)
		}
		return state.Apply(op)
	}
	return state.Apply(op)
}

func advanceCandidates(base []*Model, op Operation, got Result, opts HistoryOptions, maxCandidates int) ([]*Model, error) {
	out := make([]historyCandidate, 0, len(base))
	seen := make(map[string]struct{}, maxCandidates)
	var firstMismatch error
	for _, candidate := range base {
		next, err := advanceOneCandidate(candidate, op, got, opts)
		if err != nil {
			if firstMismatch == nil {
				firstMismatch = err
			}
			continue
		}
		for _, model := range next {
			out = appendUniqueHistoryCandidate(out, seen, historyCandidate{model: model}, maxCandidates)
			if len(out) >= maxCandidates {
				break
			}
		}
		if len(out) >= maxCandidates {
			break
		}
	}
	models := make([]*Model, 0, len(out))
	for _, candidate := range out {
		models = append(models, candidate.model)
	}
	return models, firstMismatch
}

func advanceOneCandidate(current *Model, op Operation, got Result, opts HistoryOptions) ([]*Model, error) {
	if opts.AllowIndeterminateErrors && isIndeterminateHistoryError(got.Err) {
		return advanceIndeterminateCandidate(current, op)
	}
	next := cloneModel(current)
	want := applyObserved(next, op, got)
	if err := compareResult(got, want); err != nil {
		return nil, fmt.Errorf("%w; got=%s want=%s", err, summarize(got), summarize(want))
	}
	if err := next.CheckInvariants(); err != nil {
		return nil, fmt.Errorf("candidate invariants: %w", err)
	}
	return []*Model{next}, nil
}

func advanceIndeterminateCandidate(current *Model, op Operation) ([]*Model, error) {
	out := []*Model{cloneModel(current)}
	if !operationMayMutate(op.Kind) {
		return out, nil
	}
	applied := cloneModel(current)
	want := applied.Apply(op)
	if want.Err == nil {
		if err := applied.CheckInvariants(); err != nil {
			return nil, fmt.Errorf("candidate invariants after indeterminate apply: %w", err)
		}
		out = append(out, applied)
	}
	return out, nil
}

func isIndeterminateHistoryError(err error) bool {
	if err == nil {
		return false
	}
	if nokverrors.IsTxnContention(err) {
		return true
	}
	switch nokverrors.KindOf(err) {
	case nokverrors.KindUnavailable, nokverrors.KindRouteUnavailable, nokverrors.KindRegionRouting, nokverrors.KindNotLeader, nokverrors.KindRetryExhausted:
		return true
	default:
		return false
	}
}

func operationMayMutate(kind OperationKind) bool {
	switch kind {
	case OpCreate, OpUpdateInode, OpRename, OpRenameReplace, OpRenameSubtree, OpLink, OpUnlink, OpOpenWriteSession, OpHeartbeatSession, OpCloseSession, OpExpireSessions:
		return true
	default:
		return false
	}
}

func cloneModel(in *Model) *Model {
	if in == nil {
		return nil
	}
	out := &Model{
		Mount:       in.Mount,
		Root:        in.Root,
		NowUnixNs:   in.NowUnixNs,
		dentries:    cloneDentries(in.dentries),
		inodes:      cloneInodes(in.inodes),
		sessions:    make(map[sessionKey]model.SessionRecord, len(in.sessions)),
		owners:      make(map[model.InodeID]model.SessionRecord, len(in.owners)),
		snapshots:   make(map[uint64]snapshotState, len(in.snapshots)),
		snapshotRef: make(map[int]uint64, len(in.snapshotRef)),
	}
	maps.Copy(out.sessions, in.sessions)
	maps.Copy(out.owners, in.owners)
	maps.Copy(out.snapshotRef, in.snapshotRef)
	for version, snapshot := range in.snapshots {
		out.snapshots[version] = snapshotState{
			dentries: cloneDentries(snapshot.dentries),
			inodes:   cloneInodes(snapshot.inodes),
		}
	}
	return out
}

func replaceModel(dst *Model, src *Model) {
	*dst = *src
}

func dedupeHistoryCandidates(in []historyCandidate, limit int) []historyCandidate {
	if limit <= 0 || len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]historyCandidate, 0, len(in))
	for _, candidate := range in {
		key := modelFingerprint(candidate.model)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, candidate)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func appendUniqueHistoryCandidate(out []historyCandidate, seen map[string]struct{}, candidate historyCandidate, limit int) []historyCandidate {
	if len(out) >= limit {
		return out
	}
	key := modelFingerprint(candidate.model)
	if _, ok := seen[key]; ok {
		return out
	}
	seen[key] = struct{}{}
	return append(out, candidate)
}

func modelFingerprint(m *Model) string {
	var b strings.Builder
	fmt.Fprintf(&b, "mount=%s root=%d now=%d|", m.Mount, m.Root, m.NowUnixNs)
	dentryKeys := make([]dentryKey, 0, len(m.dentries))
	for key := range m.dentries {
		dentryKeys = append(dentryKeys, key)
	}
	sort.Slice(dentryKeys, func(i, j int) bool {
		if dentryKeys[i].parent != dentryKeys[j].parent {
			return dentryKeys[i].parent < dentryKeys[j].parent
		}
		return dentryKeys[i].name < dentryKeys[j].name
	})
	for _, key := range dentryKeys {
		dentry := m.dentries[key]
		fmt.Fprintf(&b, "d:%d/%s=%d/%s;", key.parent, key.name, dentry.Inode, dentry.Type)
	}
	inodeIDs := make([]model.InodeID, 0, len(m.inodes))
	for inode := range m.inodes {
		inodeIDs = append(inodeIDs, inode)
	}
	slices.Sort(inodeIDs)
	for _, id := range inodeIDs {
		inode := m.inodes[id]
		fmt.Fprintf(&b, "i:%d=%s/%d/%d/%d/%x;", id, inode.Type, inode.Size, inode.Mode, inode.LinkCount, inode.OpaqueAttrs)
	}
	sessionKeys := make([]sessionKey, 0, len(m.sessions))
	for key := range m.sessions {
		sessionKeys = append(sessionKeys, key)
	}
	sort.Slice(sessionKeys, func(i, j int) bool {
		if sessionKeys[i].inode != sessionKeys[j].inode {
			return sessionKeys[i].inode < sessionKeys[j].inode
		}
		return sessionKeys[i].session < sessionKeys[j].session
	})
	for _, key := range sessionKeys {
		session := m.sessions[key]
		fmt.Fprintf(&b, "s:%d/%s=%d;", key.inode, key.session, session.ExpiresUnixNs)
	}
	ownerIDs := make([]model.InodeID, 0, len(m.owners))
	for id := range m.owners {
		ownerIDs = append(ownerIDs, id)
	}
	slices.Sort(ownerIDs)
	for _, id := range ownerIDs {
		owner := m.owners[id]
		fmt.Fprintf(&b, "o:%d=%s/%d;", id, owner.Session, owner.ExpiresUnixNs)
	}
	refs := make([]int, 0, len(m.snapshotRef))
	for ref := range m.snapshotRef {
		refs = append(refs, ref)
	}
	sort.Ints(refs)
	for _, ref := range refs {
		fmt.Fprintf(&b, "r:%d=%d;", ref, m.snapshotRef[ref])
	}
	versions := make([]uint64, 0, len(m.snapshots))
	for version := range m.snapshots {
		versions = append(versions, version)
	}
	slices.Sort(versions)
	for _, version := range versions {
		snapshot := m.snapshots[version]
		fmt.Fprintf(&b, "snap:%d:", version)
		snapshotDentryKeys := make([]dentryKey, 0, len(snapshot.dentries))
		for key := range snapshot.dentries {
			snapshotDentryKeys = append(snapshotDentryKeys, key)
		}
		sort.Slice(snapshotDentryKeys, func(i, j int) bool {
			if snapshotDentryKeys[i].parent != snapshotDentryKeys[j].parent {
				return snapshotDentryKeys[i].parent < snapshotDentryKeys[j].parent
			}
			return snapshotDentryKeys[i].name < snapshotDentryKeys[j].name
		})
		for _, key := range snapshotDentryKeys {
			dentry := snapshot.dentries[key]
			fmt.Fprintf(&b, "d:%d/%s=%d/%s;", key.parent, key.name, dentry.Inode, dentry.Type)
		}
		snapshotInodeIDs := make([]model.InodeID, 0, len(snapshot.inodes))
		for inode := range snapshot.inodes {
			snapshotInodeIDs = append(snapshotInodeIDs, inode)
		}
		slices.Sort(snapshotInodeIDs)
		for _, id := range snapshotInodeIDs {
			inode := snapshot.inodes[id]
			fmt.Fprintf(&b, "i:%d=%s/%d/%d/%d/%x;", id, inode.Type, inode.Size, inode.Mode, inode.LinkCount, inode.OpaqueAttrs)
		}
	}
	return b.String()
}

func describeObserved(observed []observedOperation) string {
	lines := make([]string, 0, len(observed))
	for _, event := range observed {
		lines = append(lines, fmt.Sprintf("%03d start=%d finish=%d %s -> %s",
			event.index, event.started, event.finished, event.op, summarize(event.result)))
	}
	return strings.Join(lines, "\n")
}

func describeOrder(observed []observedOperation, order []int) string {
	parts := make([]string, 0, len(order))
	for _, index := range order {
		parts = append(parts, fmt.Sprintf("%03d", observed[index].index))
	}
	return "[" + strings.Join(parts, ",") + "]"
}
