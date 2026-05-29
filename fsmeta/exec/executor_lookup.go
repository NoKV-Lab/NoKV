// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// Lookup returns the dentry record for parent/name. Visible overlay is consulted
// before the authoritative backend so visible or recovered records are observed
// without waiting for the durable install path.
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
			return model.DentryRecord{}, model.ErrNotFound
		}
		return record, nil
	}
	if value, deleted, ok := e.readVisibleProgram(program); ok {
		if deleted {
			return model.DentryRecord{}, model.ErrNotFound
		}
		return layout.DecodeDentryValue(value)
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
		if deleted {
			return model.DentryAttrPair{}, model.ErrNotFound
		}
		dentry, err := layout.DecodeDentryValue(value)
		if err != nil {
			return model.DentryAttrPair{}, err
		}
		return e.readLookupPlusInode(ctx, mount, dentry, 0)
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

	if overlayOnly {
		overlayGeneration, sealedGeneration := e.captureVisibleOverlayRead(includeOverlay)
		dentries, err := e.scanDentriesAt(ctx, plan, 0, false, overlayGeneration, sealedGeneration)
		if err != nil {
			return nil, err
		}
		if pairs, ok, err := e.readDirPlusFromVisibleViewAt(mount, dentries, overlayGeneration, sealedGeneration); err != nil {
			return nil, err
		} else if ok {
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
