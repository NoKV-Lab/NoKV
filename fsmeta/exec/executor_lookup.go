// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"fmt"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/cache/slab/dirpage"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// Lookup returns the dentry record for parent/name. visible overlay is consulted
// before the negative cache so a visible or recovered record cannot be hidden
// by a stale miss memo. Misses observed by the runner are recorded so the next
// Lookup can skip the authoritative probe; mutating operations invalidate the
// affected dentry keys after a successful commit.
func (e *Executor) Lookup(ctx context.Context, req model.LookupRequest) (model.DentryRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.DentryRecord{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileLookupReadProgram(req, mount)
	if err != nil {
		return model.DentryRecord{}, err
	}
	plan := program.Plan
	if e.visibleDirectoryHasOverlay(mount, req.Parent) {
		record, found, err := e.lookupMergedDentry(ctx, mount, req.Parent, plan.PrimaryKey)
		if err != nil {
			return model.DentryRecord{}, err
		}
		if !found {
			if e.negCache != nil {
				e.negCache.Remember(plan.PrimaryKey)
			}
			return model.DentryRecord{}, model.ErrNotFound
		}
		e.invalidateNegative(plan.PrimaryKey)
		return record, nil
	}
	if value, deleted, ok := e.readVisibleProgram(program); ok {
		e.invalidateNegative(plan.PrimaryKey)
		if deleted {
			return model.DentryRecord{}, model.ErrNotFound
		}
		return layout.DecodeDentryValue(value)
	}
	if e.negCache != nil && e.negCache.Has(plan.PrimaryKey) {
		return model.DentryRecord{}, model.ErrNotFound
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return model.DentryRecord{}, err
	}
	value, ok, err := e.runner.Get(ctx, plan.PrimaryKey, version)
	if err != nil {
		return model.DentryRecord{}, err
	}
	if !ok {
		if e.negCache != nil {
			e.negCache.Remember(plan.PrimaryKey)
		}
		return model.DentryRecord{}, model.ErrNotFound
	}
	return layout.DecodeDentryValue(value)
}

func (e *Executor) lookupMergedDentry(ctx context.Context, mount model.MountIdentity, parent model.InodeID, key []byte) (model.DentryRecord, bool, error) {
	prefix, err := layout.EncodeDentryPrefix(mount, parent)
	if err != nil {
		return model.DentryRecord{}, false, err
	}
	plan := compile.DirectoryReadPlan{
		Prefix:         prefix,
		StartKey:       cloneBytes(key),
		Limit:          1,
		IncludeOverlay: true,
	}
	var record model.DentryRecord
	var found bool
	err = e.withReadRetry(ctx, 0, func(version uint64) error {
		overlayGeneration, sealedGeneration := e.captureVisibleOverlayRead(true)
		kvs, _, _, _, err := e.scanMergedDirectoryRowsAt(ctx, plan, version, false, overlayGeneration, sealedGeneration)
		if err != nil {
			return err
		}
		if len(kvs) == 0 || !bytes.Equal(kvs[0].Key, key) {
			found = false
			return nil
		}
		decoded, err := layout.DecodeDentryValue(kvs[0].Value)
		if err != nil {
			return err
		}
		record = decoded
		found = true
		return nil
	})
	if err != nil {
		return model.DentryRecord{}, false, err
	}
	return record, found, nil
}

// LookupPlus returns one dentry and its inode attributes at the same read
// version, merged with visible visible overlay rows.
func (e *Executor) LookupPlus(ctx context.Context, req model.LookupRequest) (model.DentryAttrPair, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileLookupReadProgram(req, mount)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	plan := program.Plan
	if value, deleted, ok := e.readVisibleProgram(program); ok {
		e.invalidateNegative(plan.PrimaryKey)
		if deleted {
			return model.DentryAttrPair{}, model.ErrNotFound
		}
		dentry, err := layout.DecodeDentryValue(value)
		if err != nil {
			return model.DentryAttrPair{}, err
		}
		return e.readLookupPlusInode(ctx, mount, dentry, 0)
	}
	if e.negCache != nil && e.negCache.Has(plan.PrimaryKey) {
		return model.DentryAttrPair{}, model.ErrNotFound
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	value, ok, err := e.runner.Get(ctx, plan.PrimaryKey, version)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if !ok {
		if e.negCache != nil {
			e.negCache.Remember(plan.PrimaryKey)
		}
		return model.DentryAttrPair{}, model.ErrNotFound
	}
	dentry, err := layout.DecodeDentryValue(value)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	return e.readLookupPlusInode(ctx, mount, dentry, version)
}

func (e *Executor) readLookupPlusInode(ctx context.Context, mount model.MountIdentity, dentry model.DentryRecord, version uint64) (model.DentryAttrPair, error) {
	program, err := compile.CompileGetAttrReadProgram(mount, dentry.Inode)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if value, deleted, ok := e.readVisibleProgram(program); ok {
		if deleted {
			return model.DentryAttrPair{}, fmt.Errorf("%w: inode %d", model.ErrNotFound, dentry.Inode)
		}
		inode, err := layout.DecodeInodeValue(value)
		if err != nil {
			return model.DentryAttrPair{}, err
		}
		if inode.Inode != dentry.Inode {
			return model.DentryAttrPair{}, fmt.Errorf("%w: dentry inode=%d value inode=%d", model.ErrInvalidValue, dentry.Inode, inode.Inode)
		}
		return model.DentryAttrPair{Dentry: dentry, Inode: inode}, nil
	}
	if version == 0 {
		version, err = e.reserveReadVersion(ctx)
		if err != nil {
			return model.DentryAttrPair{}, err
		}
	}
	value, ok, err := e.runner.Get(ctx, program.Key, version)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if !ok {
		return model.DentryAttrPair{}, fmt.Errorf("%w: inode %d", model.ErrNotFound, dentry.Inode)
	}
	inode, err := layout.DecodeInodeValue(value)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if inode.Inode != dentry.Inode {
		return model.DentryAttrPair{}, fmt.Errorf("%w: dentry inode=%d value inode=%d", model.ErrInvalidValue, dentry.Inode, inode.Inode)
	}
	return model.DentryAttrPair{Dentry: dentry, Inode: inode}, nil
}

// invalidateNegative drops cached "missing" memos for every dentry key that
// was just mutated, so the next Lookup re-issues against the runner instead
// of returning a stale ErrNotFound. Safe with a nil cache.
func (e *Executor) invalidateNegative(keys ...[]byte) {
	if e == nil || e.negCache == nil {
		return
	}
	for _, k := range keys {
		if len(k) > 0 {
			e.negCache.Invalidate(k)
		}
	}
}

func (e *Executor) clearNegativeCache() {
	if e == nil || e.negCache == nil {
		return
	}
	clearer, ok := e.negCache.(negativeCacheClearer)
	if ok {
		clearer.Clear()
	}
}

// invalidateDirPages bumps the dirpage cache's epoch for every parent
// directory the just-committed mutation touched. Safe with a nil cache.
// Caller passes (mount, parent) tuples — the helper folds duplicates so
// rename across a single parent doesn't double-bump.
func (e *Executor) invalidateDirPages(mount model.MountID, parents ...model.InodeID) {
	if e == nil || e.dirPages == nil {
		return
	}
	seen := make(map[model.InodeID]struct{}, len(parents))
	for _, p := range parents {
		if p == 0 {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		e.dirPages.Invalidate(dirPageDirectoryKey(mount, p))
	}
}

// ReadDir returns one directory page from a dentry prefix scan.
func (e *Executor) ReadDir(ctx context.Context, req model.ReadDirRequest) ([]model.DentryRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return nil, err
	}
	mount := mountRecord.Identity()
	snapshotHasOverlay := req.SnapshotVersion != 0 && e.visibleSnapshotDirectoryHasOverlay(req.SnapshotVersion, mount, req.Parent)
	overlayOnly := req.SnapshotVersion == 0 && e.visibleDirectoryBaseEmpty(mount, req.Parent)
	hasVisibleOverlay := req.SnapshotVersion == 0 && e.visibleDirectoryHasVisibleOverlay(mount, req.Parent)
	includeOverlay := snapshotHasOverlay || overlayOnly || hasVisibleOverlay || (req.SnapshotVersion == 0 && e.visibleDirectoryHasOverlay(mount, req.Parent))
	plan, err := compile.CompileDirectoryReadPlan(req, mountRecord.Identity(), includeOverlay, overlayOnly)
	if err != nil {
		return nil, err
	}
	if overlayOnly {
		return e.scanDentries(ctx, plan, 0, false)
	}
	var out []model.DentryRecord
	snapshotRead := req.SnapshotVersion != 0
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		var err error
		out, err = e.scanDentries(ctx, plan, version, snapshotRead)
		return err
	})
	return out, err
}

// ReadDirPlus returns one directory page fused with inode attributes at the
// same snapshot version. This is the first native fsmeta operation that avoids
// client-side dentry scan plus N point reads.
//
// When a dirpage cache is wired and the request omits an explicit
// SnapshotVersion (i.e. the caller is asking for "latest"), ReadDirPlus checks
// the cache first against the parent's current invalidation epoch. visible-backed
// reads bypass the persistent cache because visible overlay rows are not durable
// until they have flushed and installed.
//
// Snapshot-versioned reads bypass the cache: pages are tagged with the
// "latest" frontier and a stale snapshot-versioned read might disagree
// with the live cache, so we keep that path on the authoritative LSM
// route.
func (e *Executor) ReadDirPlus(ctx context.Context, req model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return nil, err
	}
	mount := mountRecord.Identity()
	snapshotHasOverlay := req.SnapshotVersion != 0 && e.visibleSnapshotDirectoryHasOverlay(req.SnapshotVersion, mount, req.Parent)
	overlayOnly := req.SnapshotVersion == 0 && e.visibleDirectoryBaseEmpty(mount, req.Parent)
	hasVisibleOverlay := req.SnapshotVersion == 0 && e.visibleDirectoryHasVisibleOverlay(mount, req.Parent)
	includeOverlay := snapshotHasOverlay || overlayOnly || hasVisibleOverlay || (req.SnapshotVersion == 0 && e.visibleDirectoryHasOverlay(mount, req.Parent))
	plan, err := compile.CompileDirectoryReadPlan(req, mount, includeOverlay, overlayOnly)
	if err != nil {
		return nil, err
	}

	useDirPage := e.dirPages != nil && req.SnapshotVersion == 0 && !hasVisibleOverlay
	var pageKey dirpage.PageKey
	var frontier uint64
	if useDirPage {
		pageKey = dirPageKey(req.Mount, req.Parent, req.StartAfter, plan.Limit)
		frontier = e.dirPageReadFrontier(pageKey.Directory(), mount, req.Parent)
		if entries, ok := e.dirPages.Lookup(pageKey, frontier); ok {
			if cached, err := decodeDirPageEntries(pageKey, entries); err == nil {
				return cached, nil
			}
		}
	}
	if overlayOnly {
		overlayGeneration, sealedGeneration := e.captureVisibleOverlayRead(includeOverlay)
		dentries, err := e.scanDentriesAt(ctx, plan, 0, false, overlayGeneration, sealedGeneration)
		if err != nil {
			return nil, err
		}
		if pairs, ok, err := e.readDirPlusFromVisibleViewAt(mount, dentries, overlayGeneration, sealedGeneration); err != nil {
			return nil, err
		} else if ok {
			if useDirPage {
				e.materializeDirPage(pageKey, frontier, pairs)
			}
			return pairs, nil
		}
	}

	var out []model.DentryAttrPair
	snapshotRead := req.SnapshotVersion != 0
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		overlayGeneration, sealedGeneration := e.captureVisibleOverlayRead(includeOverlay && !snapshotRead)
		dentries, err := e.scanDentriesAt(ctx, plan, version, snapshotRead, overlayGeneration, sealedGeneration)
		if err != nil {
			return err
		}
		if len(dentries) == 0 {
			out = []model.DentryAttrPair{}
			return nil
		}
		inodeKeys, err := compile.CompileReadDirPlusInodeKeys(mount, dentries)
		if err != nil {
			return err
		}
		inodeValues, inodePresent, err := e.batchGetMergedValuesOrderedAt(ctx, inodeKeys, version, includeOverlay, snapshotRead, overlayGeneration, sealedGeneration)
		if err != nil {
			return err
		}
		pairs := make([]model.DentryAttrPair, 0, len(dentries))
		for i, dentry := range dentries {
			if !inodePresent[i] {
				return fmt.Errorf("%w: inode %d", model.ErrNotFound, dentry.Inode)
			}
			inode, err := layout.DecodeInodeValue(inodeValues[i])
			if err != nil {
				return err
			}
			if inode.Inode != dentry.Inode {
				return fmt.Errorf("%w: dentry inode=%d value inode=%d", model.ErrInvalidValue, dentry.Inode, inode.Inode)
			}
			pairs = append(pairs, model.DentryAttrPair{
				Dentry: dentry,
				Inode:  inode,
			})
		}
		out = pairs
		return nil
	})
	if err != nil {
		return nil, err
	}
	if useDirPage {
		// Materialize is best-effort: if Invalidate fired since we read,
		// the cache drops the write and the next call re-fetches. Encoding must
		// be all-or-none: a partial cached page would be worse than a miss.
		e.materializeDirPage(pageKey, frontier, out)
	}
	return out, nil
}

func (e *Executor) captureVisibleOverlayRead(includeOverlay bool) (uint64, uint64) {
	if !includeOverlay {
		return 0, 0
	}
	reader := e.visibleOverlayReadSnapshot()
	if reader == nil {
		return 0, 0
	}
	return reader.CaptureVisibleOverlayRead()
}

func (e *Executor) materializeDirPage(pageKey dirpage.PageKey, frontier uint64, pairs []model.DentryAttrPair) {
	if e == nil || e.dirPages == nil {
		return
	}
	entries, err := encodeDirPageEntries(pairs)
	if err != nil {
		return
	}
	_ = e.dirPages.MaterializeAsync(pageKey, frontier, entries)
}

func (e *Executor) readDirPlusFromVisibleView(mount model.MountIdentity, dentries []model.DentryRecord) ([]model.DentryAttrPair, bool, error) {
	return e.readDirPlusFromVisibleViewAt(mount, dentries, 0, 0)
}

func (e *Executor) readDirPlusFromVisibleViewAt(mount model.MountIdentity, dentries []model.DentryRecord, overlayGeneration, sealedGeneration uint64) ([]model.DentryAttrPair, bool, error) {
	if len(dentries) == 0 {
		return []model.DentryAttrPair{}, true, nil
	}
	inodeKeys, err := compile.CompileReadDirPlusInodeKeys(mount, dentries)
	if err != nil {
		return nil, false, err
	}
	overlay := e.visibleOverlay()
	readSnapshot := e.visibleOverlayReadSnapshot()
	useReadSnapshot := readSnapshot != nil && (overlayGeneration != 0 || sealedGeneration != 0)
	if overlay == nil && !useReadSnapshot {
		return nil, false, nil
	}
	pairs := make([]model.DentryAttrPair, 0, len(dentries))
	for i, dentry := range dentries {
		var value []byte
		var deleted, ok bool
		if useReadSnapshot {
			value, deleted, ok = readSnapshot.GetVisibleOverlayViewAt(overlayGeneration, sealedGeneration, inodeKeys[i])
		} else {
			value, deleted, ok = overlay.GetVisibleOverlayView(inodeKeys[i])
		}
		if !ok || deleted {
			return nil, false, nil
		}
		inode, err := layout.DecodeInodeValue(value)
		if err != nil {
			return nil, false, err
		}
		if inode.Inode != dentry.Inode {
			return nil, false, fmt.Errorf("%w: dentry inode=%d value inode=%d", model.ErrInvalidValue, dentry.Inode, inode.Inode)
		}
		pairs = append(pairs, model.DentryAttrPair{Dentry: dentry, Inode: inode})
	}
	return pairs, true, nil
}

func (e *Executor) scanDentries(ctx context.Context, plan compile.DirectoryReadPlan, version uint64, snapshotRead bool) ([]model.DentryRecord, error) {
	overlayGeneration, sealedGeneration := e.captureVisibleOverlayRead(plan.IncludeOverlay && !snapshotRead)
	return e.scanDentriesAt(ctx, plan, version, snapshotRead, overlayGeneration, sealedGeneration)
}

func (e *Executor) scanDentriesAt(ctx context.Context, plan compile.DirectoryReadPlan, version uint64, snapshotRead bool, overlayGeneration, sealedGeneration uint64) ([]model.DentryRecord, error) {
	var kvs []backend.KV
	stats := compile.DirectoryReadStats{UsedOverlayOnly: plan.OverlayOnly}
	if !plan.OverlayOnly {
		if plan.IncludeOverlay {
			var overlayRows uint32
			var err error
			kvs, stats.BaseRows, overlayRows, stats.UsedDirIndex, err = e.scanMergedDirectoryRowsAt(ctx, plan, version, snapshotRead, overlayGeneration, sealedGeneration)
			if err != nil {
				return nil, err
			}
			stats.OverlayRows = overlayRows
		} else {
			var err error
			kvs, err = e.runner.Scan(ctx, plan.StartKey, plan.Limit, version)
			if err != nil {
				return nil, err
			}
			stats.BaseRows = uint32(len(kvs))
		}
	} else if plan.IncludeOverlay {
		var overlayRows uint32
		kvs, overlayRows, stats.UsedDirIndex = e.mergeVisibleDirectoryOverlayScanAt(kvs, plan.Prefix, plan.StartKey, plan.Limit, overlayGeneration, sealedGeneration)
		stats.OverlayRows = overlayRows
	}
	out := make([]model.DentryRecord, 0, len(kvs))
	for _, kv := range kvs {
		if !bytes.HasPrefix(kv.Key, plan.Prefix) {
			break
		}
		record, err := layout.DecodeDentryValue(kv.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	stats.OutputRows = uint32(len(out))
	e.visibleDirectoryRead.record(stats)
	return out, nil
}

func (e *Executor) visibleDirectoryBaseEmpty(mount model.MountIdentity, parent model.InodeID) bool {
	index := e.visiblePredicateIndex()
	if index == nil {
		return false
	}
	return index.DirectoryBaseEmpty(mount, parent)
}

func (e *Executor) visibleDirectoryHasOverlay(mount model.MountIdentity, parent model.InodeID) bool {
	overlay := e.visibleOverlay()
	if overlay == nil {
		return false
	}
	presence, ok := overlay.(VisibleDirectoryOverlayPresence)
	if !ok {
		return true
	}
	prefix, err := layout.EncodeDentryPrefix(mount, parent)
	if err != nil {
		return true
	}
	return presence.HasVisibleDirectoryOverlay(prefix)
}

func (e *Executor) visibleDirectoryHasVisibleOverlay(mount model.MountIdentity, parent model.InodeID) bool {
	overlay := e.visibleOverlay()
	if overlay == nil {
		return false
	}
	presence, ok := overlay.(PendingVisibleDirectoryPresence)
	if !ok {
		return e.visibleDirectoryHasOverlay(mount, parent)
	}
	prefix, err := layout.EncodeDentryPrefix(mount, parent)
	if err != nil {
		return true
	}
	return presence.HasPendingVisibleDirectory(prefix)
}

func (e *Executor) visibleSnapshotDirectoryHasOverlay(version uint64, mount model.MountIdentity, parent model.InodeID) bool {
	reader := e.visibleSnapshotOverlay()
	if reader == nil {
		return false
	}
	prefix, err := layout.EncodeDentryPrefix(mount, parent)
	if err != nil {
		return true
	}
	return reader.HasVisibleSnapshotDirectory(version, prefix)
}

func (e *Executor) dirPageReadFrontier(directory dirpage.DirectoryKey, mount model.MountIdentity, parent model.InodeID) uint64 {
	e.syncDirPageVisibleFrontier(directory, e.visibleDirectoryCacheFrontier(mount, parent))
	return e.dirPages.CurrentEpoch(directory)
}

func (e *Executor) visibleDirectoryCacheFrontier(mount model.MountIdentity, parent model.InodeID) uint64 {
	overlay := e.visibleOverlay()
	if overlay == nil {
		return 0
	}
	reporter, ok := overlay.(VisibleDirectoryCacheFrontier)
	if !ok {
		return 0
	}
	prefix, err := layout.EncodeDentryPrefix(mount, parent)
	if err != nil {
		return 0
	}
	return reporter.VisibleDirectoryCacheFrontier(prefix)
}

func (e *Executor) syncDirPageVisibleFrontier(directory dirpage.DirectoryKey, frontier uint64) {
	if e == nil || e.dirPages == nil {
		return
	}
	e.dirPageVisibleMu.Lock()
	if e.dirPageVisibleFrontier == nil {
		e.dirPageVisibleFrontier = make(map[dirpage.DirectoryKey]uint64)
	}
	previous, known := e.dirPageVisibleFrontier[directory]
	if (!known && frontier == 0) || (known && previous == frontier) {
		e.dirPageVisibleMu.Unlock()
		return
	}
	if frontier == 0 {
		delete(e.dirPageVisibleFrontier, directory)
	} else {
		e.dirPageVisibleFrontier[directory] = frontier
	}
	e.dirPageVisibleMu.Unlock()
	e.dirPages.Invalidate(directory)
}

func (e *Executor) readDentry(ctx context.Context, key []byte, version uint64) (model.DentryRecord, error) {
	value, ok, err := e.getMergedValue(ctx, key, version)
	if err != nil {
		return model.DentryRecord{}, err
	}
	if !ok {
		return model.DentryRecord{}, model.ErrNotFound
	}
	return layout.DecodeDentryValue(value)
}

func (e *Executor) readInode(ctx context.Context, mount model.MountIdentity, inodeID model.InodeID, version uint64) (model.InodeRecord, bool, error) {
	program, err := compile.CompileGetAttrReadProgram(mount, inodeID)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	value, ok, err := e.getMergedProgramValue(ctx, program, version)
	if err != nil || !ok {
		return model.InodeRecord{}, ok, err
	}
	inode, err := layout.DecodeInodeValue(value)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func (e *Executor) readSessionByKey(ctx context.Context, mount model.MountIdentity, key []byte, version uint64) (model.SessionRecord, bool, error) {
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
	value, ok, err := e.getMergedProgramValue(ctx, program, version)
	if err != nil || !ok {
		return model.SessionRecord{}, ok, err
	}
	record, err := layout.DecodeSessionValue(value)
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	return record, true, nil
}

// dirPageDirectoryKey hashes (mount, parent) into the dirpage cache's
// directory invalidation key. model.MountID is a string; we use xxhash.Sum64
// to fold it into a uint64 mount slot. Collision probability across reasonable
// mount counts (<= 10K) is ~5e-12, well below "fallback re-warm" tolerance.
func dirPageDirectoryKey(mount model.MountID, parent model.InodeID) dirpage.DirectoryKey {
	return dirpage.DirectoryKey{
		Mount:  xxhash.Sum64String(string(mount)),
		Parent: uint64(parent),
	}
}

// dirPageKey includes the caller-visible page cursor. ReadDirPlus cache hits
// are only valid for the exact StartAfter/Limit shape that produced them.
func dirPageKey(mount model.MountID, parent model.InodeID, startAfter string, limit uint32) dirpage.PageKey {
	return dirpage.PageKey{
		Mount:      xxhash.Sum64String(string(mount)),
		Parent:     uint64(parent),
		StartAfter: startAfter,
		Limit:      limit,
	}
}

// encodeDirPageEntries converts assembled DentryAttrPairs into the
// generic dirpage Entry shape. AttrBlob is the encoded InodeRecord; if any
// entry cannot be encoded, the whole materialization is skipped so the cache
// never serves a truncated page as complete.
func encodeDirPageEntries(pairs []model.DentryAttrPair) ([]dirpage.Entry, error) {
	out := make([]dirpage.Entry, 0, len(pairs))
	for _, p := range pairs {
		blob, err := layout.EncodeInodeValue(p.Inode)
		if err != nil {
			return nil, err
		}
		out = append(out, dirpage.Entry{
			Name:     []byte(p.Dentry.Name),
			Inode:    uint64(p.Dentry.Inode),
			AttrBlob: blob,
		})
	}
	return out, nil
}

// decodeDirPageEntries reverses encodeDirPageEntries. Decode failure on
// any entry treats the whole page set as corrupt and forces a fallback
// to the runner.
func decodeDirPageEntries(key dirpage.PageKey, entries []dirpage.Entry) ([]model.DentryAttrPair, error) {
	out := make([]model.DentryAttrPair, 0, len(entries))
	for _, e := range entries {
		inode, err := layout.DecodeInodeValue(e.AttrBlob)
		if err != nil {
			return nil, err
		}
		out = append(out, model.DentryAttrPair{
			Dentry: model.DentryRecord{
				Parent: model.InodeID(key.Parent),
				Name:   string(e.Name),
				Inode:  model.InodeID(e.Inode),
				Type:   inode.Type,
			},
			Inode: inode,
		})
	}
	return out, nil
}
