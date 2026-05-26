// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"sort"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

func (e *Executor) visibleOverlay() VisibleOverlayReader {
	if e == nil || e.visibleCommitter == nil {
		return nil
	}
	overlay, ok := e.visibleCommitter.(VisibleOverlayReader)
	if !ok {
		return nil
	}
	return overlay
}

func (e *Executor) flushVisible(ctx context.Context) error {
	if e == nil || e.visibleCommitter == nil {
		return nil
	}
	flusher, ok := e.visibleCommitter.(VisibleFlusher)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return flusher.FlushDurable(context.WithoutCancel(ctx))
}

func (e *Executor) flushVisibleAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if e == nil || e.visibleCommitter == nil {
		return nil
	}
	if len(scopes) == 0 {
		return e.flushVisible(ctx)
	}
	if scoped, ok := e.visibleCommitter.(VisibleAuthorityFlusher); ok {
		for _, scope := range scopes {
			if authorityScopeEmpty(scope) {
				return e.flushVisible(ctx)
			}
			if err := scoped.FlushAuthority(context.WithoutCancel(ctx), scope); err != nil {
				return err
			}
		}
		return nil
	}
	return e.flushVisible(ctx)
}

func (e *Executor) retireVisibleAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if e == nil || e.visibleAuthority == nil {
		return nil
	}
	retirer, ok := e.visibleAuthority.(VisibleAuthorityRetirer)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return retirer.RetireVisibleAuthority(context.WithoutCancel(ctx), scopes...)
}

func (e *Executor) drainVisibleAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if e == nil {
		return nil
	}
	if e.visibleCommitter != nil && e.visibleAuthority != nil {
		drainer, drainOK := e.visibleCommitter.(VisibleAuthorityDrainer)
		retirer, retireOK := e.visibleAuthority.(VisibleAuthorityRetirer)
		if drainOK && retireOK {
			if ctx == nil {
				ctx = context.Background()
			}
			return drainer.DrainVisibleAuthority(context.WithoutCancel(ctx), retirer, scopes...)
		}
	}
	if err := e.flushVisibleAuthority(ctx, scopes...); err != nil {
		return err
	}
	return e.retireVisibleAuthority(ctx, scopes...)
}

func authorityScopeEmpty(scope compile.AuthorityScope) bool {
	return scope.Mount == "" || scope.MountKeyID == 0
}

func (e *Executor) visibleOverlayGet(key []byte) ([]byte, bool, bool) {
	overlay := e.visibleOverlay()
	if overlay == nil {
		return nil, false, false
	}
	return overlay.GetVisibleOverlayView(key)
}

func (e *Executor) visibleSnapshotOverlay() VisibleSnapshotOverlayReader {
	if e == nil || e.visibleCommitter == nil {
		return nil
	}
	reader, ok := e.visibleCommitter.(VisibleSnapshotOverlayReader)
	if !ok {
		return nil
	}
	return reader
}

func (e *Executor) visibleOverlayReadSnapshot() VisibleOverlayReadSnapshotReader {
	if e == nil || e.visibleCommitter == nil {
		return nil
	}
	reader, ok := e.visibleCommitter.(VisibleOverlayReadSnapshotReader)
	if !ok {
		return nil
	}
	return reader
}

func (e *Executor) readVisibleProgram(program compile.ReadProgram) ([]byte, bool, bool) {
	if len(program.Key) == 0 {
		return nil, false, false
	}
	return e.visibleOverlayGet(program.Key)
}

func (e *Executor) getMergedValue(ctx context.Context, key []byte, version uint64) ([]byte, bool, error) {
	if value, deleted, ok := e.visibleOverlayGet(key); ok {
		if deleted {
			return nil, false, nil
		}
		return value, true, nil
	}
	return e.runner.Get(ctx, key, version)
}

func (e *Executor) getMergedProgramValue(ctx context.Context, program compile.ReadProgram, version uint64) ([]byte, bool, error) {
	if value, deleted, ok := e.readVisibleProgram(program); ok {
		if deleted {
			return nil, false, nil
		}
		return value, true, nil
	}
	return e.runner.Get(ctx, program.Key, version)
}

type visibleReadView struct {
	executor        *Executor
	ctx             context.Context
	version         uint64
	haveVersion     bool
	observedVisible bool
	observed        map[string]int
	proofs          []proof.PredicateProof
}

func (e *Executor) newVisibleReadView(ctx context.Context) *visibleReadView {
	if ctx == nil {
		ctx = context.Background()
	}
	return &visibleReadView{
		executor: e,
		ctx:      ctx,
		observed: make(map[string]int),
	}
}

func (v *visibleReadView) get(key []byte) ([]byte, bool, error) {
	if v == nil || v.executor == nil {
		return nil, false, model.ErrInvalidRequest
	}
	if value, deleted, ok := v.executor.visibleOverlayGet(key); ok {
		v.observedVisible = true
		if deleted {
			v.remember(key, nil, false, proof.ReadSourceOverlay, 0)
			return nil, false, nil
		}
		v.remember(key, value, true, proof.ReadSourceOverlay, 0)
		return value, true, nil
	}
	if index := v.executor.visiblePredicateIndex(); index != nil {
		present, known := index.KeyState(key)
		if known && !present {
			v.remember(key, nil, false, proof.ReadSourceUnknown, 0)
			return nil, false, nil
		}
	}
	if !v.haveVersion {
		version, err := v.executor.reserveReadVersion(v.ctx)
		if err != nil {
			return nil, false, err
		}
		v.version = version
		v.haveVersion = true
	}
	value, ok, err := v.executor.runner.Get(v.ctx, key, v.version)
	if err != nil {
		return nil, false, err
	}
	v.remember(key, value, ok, proof.ReadSourceBase, v.version)
	return value, ok, nil
}

func (v *visibleReadView) observedVisibleOverlay() bool {
	return v != nil && v.observedVisible
}

func (v *visibleReadView) observedKeyFromVisibleOverlay(key []byte) bool {
	if v == nil {
		return false
	}
	index, ok := v.observed[string(key)]
	if !ok || index < 0 || index >= len(v.proofs) {
		return false
	}
	return v.proofs[index].Source == proof.ReadSourceOverlay
}

func (v *visibleReadView) remember(key, value []byte, present bool, source proof.ReadSource, version uint64) {
	if v == nil {
		return
	}
	predicateProof := proof.NewPredicateProof(key, value, present, version, source, proof.ProofFrontier{})
	keyString := string(key)
	if index, ok := v.observed[keyString]; ok {
		v.proofs[index] = predicateProof
		return
	}
	v.observed[keyString] = len(v.proofs)
	v.proofs = append(v.proofs, predicateProof)
}

func (v *visibleReadView) materializeVisibleCompiledOp(compiled compile.CompiledOp, effects []compile.WriteEffect) (compile.MaterializedOp, error) {
	if v == nil || v.executor == nil {
		return compile.MaterializeCompiledOpWithEvidence(compiled, effects, compile.PredicateEvidence{}, nil)
	}
	return compile.MaterializeCompiledOpWithEvidence(compiled, effects, v.predicateEvidenceForDelta(compiled.Delta), nil)
}

func (v *visibleReadView) predicateProofs() []proof.PredicateProof {
	if v == nil || len(v.proofs) == 0 {
		return nil
	}
	proofs := make([]proof.PredicateProof, len(v.proofs))
	copy(proofs, v.proofs)
	sort.Slice(proofs, func(i, j int) bool {
		return bytes.Compare(proofs[i].Key, proofs[j].Key) < 0
	})
	for i := range proofs {
		proofs[i] = clonePredicateProof(proofs[i])
	}
	return proofs
}

func clonePredicateProof(predicateProof proof.PredicateProof) proof.PredicateProof {
	predicateProof.Key = cloneBytes(predicateProof.Key)
	predicateProof.Value = cloneBytes(predicateProof.Value)
	return predicateProof
}

func (v *visibleReadView) predicateEvidenceForDelta(delta compile.SemanticDelta) compile.PredicateEvidence {
	if v == nil || v.executor == nil {
		return compile.PredicateEvidence{}
	}
	index := v.executor.visiblePredicateIndex()
	allowAbsentDowngrade := visibleDeltaAllowsAbsentObservedValue(delta)
	proofs := v.predicateProofs()
	for _, predicate := range delta.ReadPredicates {
		if predicate.Kind == compile.PredicatePrefixScan {
			continue
		}
		if _, ok := v.observed[string(predicate.Key)]; ok {
			continue
		}
		if allowAbsentDowngrade &&
			predicate.Kind == compile.PredicateObservedValue &&
			v.executor.visibleNotExistsKnown(delta.Authority, predicate.Key, index) {
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, false, 0, proof.ReadSourceOverlay, proof.ProofFrontier{}))
		}
	}
	return compile.PredicateEvidence{
		Proofs: proofs,
	}
}

func (v *visibleReadView) readDentry(key []byte) (model.DentryRecord, error) {
	value, ok, err := v.get(key)
	if err != nil {
		return model.DentryRecord{}, err
	}
	if !ok {
		return model.DentryRecord{}, model.ErrNotFound
	}
	return layout.DecodeDentryValue(value)
}

func (v *visibleReadView) readInode(mount model.MountIdentity, inodeID model.InodeID) (model.InodeRecord, bool, error) {
	program, err := compile.CompileGetAttrReadProgram(mount, inodeID)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	value, ok, err := v.get(program.Key)
	if err != nil || !ok {
		return model.InodeRecord{}, ok, err
	}
	inode, err := layout.DecodeInodeValue(value)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func (v *visibleReadView) readSession(mount model.MountIdentity, key []byte) (model.SessionRecord, bool, error) {
	parts, ok := layout.InspectKey(key)
	if !ok || parts.Kind != layout.KeyKindSession {
		return model.SessionRecord{}, false, layout.ErrInvalidKey
	}
	if parts.MountKeyID != mount.MountKeyID {
		return model.SessionRecord{}, false, model.ErrInvalidRequest
	}
	program, err := compile.CompileReadSessionKeyProgram(mount, key)
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	value, ok, err := v.get(program.Key)
	if err != nil || !ok {
		return model.SessionRecord{}, ok, err
	}
	session, err := layout.DecodeSessionValue(value)
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	return session, true, nil
}

func (e *Executor) batchGetMergedValuesOrderedAt(ctx context.Context, keys [][]byte, version uint64, includeOverlay, snapshotRead bool, overlayGeneration, sealedGeneration uint64) ([][]byte, []bool, error) {
	values := make([][]byte, len(keys))
	present := make([]bool, len(keys))

	overlay := e.visibleOverlay()
	snapshot := e.visibleSnapshotOverlay()
	readSnapshot := e.visibleOverlayReadSnapshot()
	useReadSnapshot := !snapshotRead && readSnapshot != nil && (overlayGeneration != 0 || sealedGeneration != 0)
	if !includeOverlay || (!snapshotRead && overlay == nil && !useReadSnapshot) || (snapshotRead && snapshot == nil) {
		base, err := e.runner.BatchGet(ctx, keys, version)
		if err != nil {
			return nil, nil, err
		}
		for i, key := range keys {
			value, ok := base[string(key)]
			if ok {
				values[i] = value
				present[i] = true
			}
		}
		return values, present, nil
	}

	missing := make([][]byte, 0, len(keys))
	missingIndexes := make([]int, 0, len(keys))
	for i, key := range keys {
		var value []byte
		var deleted, ok bool
		if snapshotRead {
			value, deleted, ok = snapshot.GetVisibleSnapshotOverlayView(version, key)
		} else if useReadSnapshot {
			value, deleted, ok = readSnapshot.GetVisibleOverlayViewAt(overlayGeneration, sealedGeneration, key)
		} else {
			value, deleted, ok = overlay.GetVisibleOverlayView(key)
		}
		switch {
		case ok && !deleted:
			values[i] = value
			present[i] = true
		case ok && deleted:
		default:
			missing = append(missing, key)
			missingIndexes = append(missingIndexes, i)
		}
	}
	if len(missing) == 0 {
		return values, present, nil
	}
	base, err := e.runner.BatchGet(ctx, missing, version)
	if err != nil {
		return nil, nil, err
	}
	for i, key := range missing {
		value, ok := base[string(key)]
		if ok {
			index := missingIndexes[i]
			values[index] = value
			present[index] = true
		}
	}
	return values, present, nil
}

func (e *Executor) mergeVisibleOverlayScan(kvs []KV, start []byte, limit uint32) []KV {
	overlay := e.visibleOverlay()
	if overlay == nil || limit == 0 {
		return kvs
	}
	overlayKVs := overlay.ScanVisibleOverlay(start, limit)
	if len(overlayKVs) == 0 {
		return kvs
	}
	return mergeOverlayScanRows(kvs, overlayKVs, limit)
}

func (e *Executor) mergeVisibleDirectoryOverlayScan(kvs []KV, prefix, start []byte, limit uint32) ([]KV, uint32, bool) {
	return e.mergeVisibleDirectoryOverlayScanAt(kvs, prefix, start, limit, 0, 0)
}

func (e *Executor) mergeVisibleDirectoryOverlayScanAt(kvs []KV, prefix, start []byte, limit uint32, overlayGeneration, sealedGeneration uint64) ([]KV, uint32, bool) {
	overlayKVs, usedIndex := e.scanVisibleDirectoryOverlayRowsAt(overlayGeneration, sealedGeneration, prefix, start, limit)
	if len(overlayKVs) == 0 {
		return kvs, 0, usedIndex
	}
	out := mergeOverlayScanRows(kvs, overlayKVs, limit)
	return out, uint32(len(overlayKVs)), usedIndex
}

func (e *Executor) scanVisibleDirectoryOverlayRowsAt(overlayGeneration, sealedGeneration uint64, prefix, start []byte, limit uint32) ([]VisibleOverlayKV, bool) {
	var (
		out       []VisibleOverlayKV
		startKey  = cloneBytes(start)
		usedIndex bool
	)
	for {
		batch, batchUsedIndex := e.scanVisibleDirectoryOverlayBatchAt(overlayGeneration, sealedGeneration, prefix, startKey, limit)
		usedIndex = usedIndex || batchUsedIndex
		if len(batch) == 0 {
			return out, usedIndex
		}
		out = append(out, batch...)
		visible := mergeOverlayScanRows(nil, out, limit)
		if directoryMergeComplete(visible, prefix, limit) || !directoryOverlayScanMayContinue(batch, prefix, limit) {
			return out, usedIndex
		}
		startKey = keyAfter(batch[len(batch)-1].Key)
	}
}

func (e *Executor) scanVisibleSnapshotDirectoryOverlayRows(version uint64, prefix, start []byte, limit uint32) ([]VisibleOverlayKV, bool) {
	reader := e.visibleSnapshotOverlay()
	if reader == nil || version == 0 || limit == 0 {
		return nil, false
	}
	var (
		out      []VisibleOverlayKV
		startKey = cloneBytes(start)
	)
	for {
		overlayKVs := reader.ScanVisibleSnapshotDirectory(version, prefix, startKey, limit)
		if len(overlayKVs) == 0 {
			return out, true
		}
		batch := overlayKVs[:0]
		for _, row := range overlayKVs {
			if !bytes.HasPrefix(row.Key, prefix) {
				break
			}
			batch = append(batch, row)
		}
		if len(batch) == 0 {
			return out, true
		}
		out = append(out, batch...)
		visible := mergeOverlayScanRows(nil, out, limit)
		if directoryMergeComplete(visible, prefix, limit) || !directoryOverlayScanMayContinue(batch, prefix, limit) {
			return out, true
		}
		startKey = keyAfter(batch[len(batch)-1].Key)
	}
}

func (e *Executor) scanVisibleDirectoryOverlayBatchAt(overlayGeneration, sealedGeneration uint64, prefix, start []byte, limit uint32) ([]VisibleOverlayKV, bool) {
	if readSnapshot := e.visibleOverlayReadSnapshot(); readSnapshot != nil && (overlayGeneration != 0 || sealedGeneration != 0) {
		overlayKVs := readSnapshot.ScanVisibleDirectoryAt(overlayGeneration, sealedGeneration, prefix, start, limit)
		return trimVisibleDirectoryOverlayBatch(prefix, overlayKVs), true
	}
	overlay := e.visibleOverlay()
	if overlay == nil || limit == 0 {
		return nil, false
	}
	var (
		overlayKVs []VisibleOverlayKV
		usedIndex  bool
	)
	if directoryReader, ok := overlay.(VisibleDirectoryOverlayReader); ok {
		overlayKVs = directoryReader.ScanVisibleDirectory(prefix, start, limit)
		usedIndex = true
	} else {
		overlayKVs = overlay.ScanVisibleOverlay(start, limit)
	}
	return trimVisibleDirectoryOverlayBatch(prefix, overlayKVs), usedIndex
}

func trimVisibleDirectoryOverlayBatch(prefix []byte, overlayKVs []VisibleOverlayKV) []VisibleOverlayKV {
	if len(overlayKVs) == 0 {
		return nil
	}
	out := overlayKVs[:0]
	for _, row := range overlayKVs {
		if !bytes.HasPrefix(row.Key, prefix) {
			break
		}
		out = append(out, row)
	}
	return out
}

func (e *Executor) scanMergedDirectoryRowsAt(ctx context.Context, plan compile.DirectoryReadPlan, version uint64, snapshotRead bool, overlayGeneration, sealedGeneration uint64) ([]KV, uint32, uint32, bool, error) {
	var overlayKVs []VisibleOverlayKV
	var usedIndex bool
	if snapshotRead {
		overlayKVs, usedIndex = e.scanVisibleSnapshotDirectoryOverlayRows(version, plan.Prefix, plan.StartKey, plan.Limit)
	} else {
		overlayKVs, usedIndex = e.scanVisibleDirectoryOverlayRowsAt(overlayGeneration, sealedGeneration, plan.Prefix, plan.StartKey, plan.Limit)
	}
	start := cloneBytes(plan.StartKey)
	baseRows := make([]KV, 0, plan.Limit)
	var baseTotal uint32
	for {
		batch, err := e.runner.Scan(ctx, start, plan.Limit, version)
		if err != nil {
			return nil, baseTotal, uint32(len(overlayKVs)), usedIndex, err
		}
		baseTotal += uint32(len(batch))
		baseRows = append(baseRows, batch...)
		merged := mergeOverlayScanRows(baseRows, overlayKVs, plan.Limit)
		if directoryMergeComplete(merged, plan.Prefix, plan.Limit) || !directoryBaseScanMayContinue(batch, plan.Prefix, plan.Limit) {
			return merged, baseTotal, uint32(len(overlayKVs)), usedIndex, nil
		}
		start = keyAfter(batch[len(batch)-1].Key)
	}
}

func mergeOverlayScanRows(kvs []KV, overlayKVs []VisibleOverlayKV, limit uint32) []KV {
	out := make([]KV, 0, int(limit))
	base, overlay := 0, 0
	for len(out) < int(limit) && (base < len(kvs) || overlay < len(overlayKVs)) {
		switch {
		case base >= len(kvs):
			out = appendOverlayScanKV(out, overlayKVs[overlay])
			overlay++
		case overlay >= len(overlayKVs):
			out = append(out, kvs[base])
			base++
		default:
			cmp := bytes.Compare(kvs[base].Key, overlayKVs[overlay].Key)
			switch {
			case cmp < 0:
				out = append(out, kvs[base])
				base++
			case cmp > 0:
				out = appendOverlayScanKV(out, overlayKVs[overlay])
				overlay++
			default:
				out = appendOverlayScanKV(out, overlayKVs[overlay])
				base++
				overlay++
			}
		}
	}
	return out
}

func appendOverlayScanKV(out []KV, kv VisibleOverlayKV) []KV {
	if kv.Delete {
		return out
	}
	return append(out, KV{Key: kv.Key, Value: kv.Value})
}

func directoryMergeComplete(kvs []KV, prefix []byte, limit uint32) bool {
	if limit == 0 {
		return true
	}
	var n uint32
	for _, kv := range kvs {
		if !bytes.HasPrefix(kv.Key, prefix) {
			return true
		}
		n++
		if n >= limit {
			return true
		}
	}
	return false
}

func directoryBaseScanMayContinue(batch []KV, prefix []byte, limit uint32) bool {
	if limit == 0 || uint32(len(batch)) < limit {
		return false
	}
	return bytes.HasPrefix(batch[len(batch)-1].Key, prefix)
}

func directoryOverlayScanMayContinue(batch []VisibleOverlayKV, prefix []byte, limit uint32) bool {
	if limit == 0 || uint32(len(batch)) < limit {
		return false
	}
	return bytes.HasPrefix(batch[len(batch)-1].Key, prefix)
}

func keyAfter(key []byte) []byte {
	out := make([]byte, len(key)+1)
	copy(out, key)
	return out
}
