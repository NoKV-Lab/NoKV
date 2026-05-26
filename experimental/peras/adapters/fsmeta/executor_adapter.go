// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"context"
	"errors"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

type executorAdapter struct {
	authority *runtimeperas.AuthorityManager
	runtime   *runtimeperas.Runtime
}

func newExecutorAdapter(authority *runtimeperas.AuthorityManager, runtime *runtimeperas.Runtime) executorAdapter {
	return executorAdapter{authority: authority, runtime: runtime}
}

// NewExecutorCommitter adapts a Peras runtime to fsmeta's stable visible commit
// interfaces. The returned dynamic type also implements the optional visible
// overlay, snapshot, flush, predicate-index, and stats interfaces.
func NewExecutorCommitter(runtime *runtimeperas.Runtime) fsmetaexec.VisibleCommitter {
	return executorAdapter{runtime: runtime}
}

func (a executorAdapter) AcquireVisibleAuthority(ctx context.Context, scope compile.AuthorityScope) (bool, error) {
	if a.authority == nil {
		return false, runtimeperas.ErrClientRequired
	}
	return a.authority.AcquireVisibleAuthority(ctx, scope)
}

func (a executorAdapter) RetireVisibleAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if a.authority == nil {
		return runtimeperas.ErrClientRequired
	}
	return a.authority.RetireVisibleAuthority(ctx, scopes...)
}

func (a executorAdapter) SubmitVisible(ctx context.Context, id fsmetaexec.VisibleOperationID, op compile.MaterializedOp, admission fsmetaexec.VisibleAdmissionFunc) (fsmetaexec.VisibleAck, error) {
	if a.runtime == nil {
		return fsmetaexec.VisibleAck{}, runtimeperas.ErrRuntimeInvalid
	}
	ack, err := a.runtime.SubmitVisible(ctx, fromVisibleOperationID(id), op, fromVisibleAdmissionFunc(admission))
	if err != nil {
		return fsmetaexec.VisibleAck{}, mapPerasAdmissionError(err)
	}
	return toVisibleAck(ack), nil
}

func (a executorAdapter) FlushDurable(ctx context.Context) error {
	if a.runtime == nil {
		return runtimeperas.ErrRuntimeInvalid
	}
	return a.runtime.FlushDurable(ctx)
}

func (a executorAdapter) FlushAuthority(ctx context.Context, scope compile.AuthorityScope) error {
	if a.runtime == nil {
		return runtimeperas.ErrRuntimeInvalid
	}
	return a.runtime.FlushAuthority(ctx, scope)
}

func (a executorAdapter) DrainVisibleAuthority(ctx context.Context, retirer fsmetaexec.VisibleAuthorityRetirer, scopes ...compile.AuthorityScope) error {
	if a.runtime == nil || retirer == nil {
		return runtimeperas.ErrRuntimeInvalid
	}
	return a.runtime.DrainAuthority(ctx, retirer, scopes...)
}

func (a executorAdapter) GetVisibleOverlay(key []byte) ([]byte, bool, bool) {
	if a.runtime == nil {
		return nil, false, false
	}
	return a.runtime.GetPerasOverlay(key)
}

func (a executorAdapter) GetVisibleOverlayView(key []byte) ([]byte, bool, bool) {
	if a.runtime == nil {
		return nil, false, false
	}
	return a.runtime.GetPerasOverlayView(key)
}

func (a executorAdapter) ScanVisibleOverlay(start []byte, limit uint32) []fsmetaexec.VisibleOverlayKV {
	if a.runtime == nil {
		return nil
	}
	return toVisibleOverlayKVs(a.runtime.ScanPerasOverlay(start, limit))
}

func (a executorAdapter) CaptureVisibleOverlayRead() (uint64, uint64) {
	if a.runtime == nil {
		return 0, 0
	}
	return a.runtime.CapturePerasOverlayRead()
}

func (a executorAdapter) GetVisibleOverlayViewAt(overlayGeneration, sealedGeneration uint64, key []byte) ([]byte, bool, bool) {
	if a.runtime == nil {
		return nil, false, false
	}
	return a.runtime.GetPerasOverlayViewAt(overlayGeneration, sealedGeneration, key)
}

func (a executorAdapter) ScanVisibleDirectoryAt(overlayGeneration, sealedGeneration uint64, prefix, start []byte, limit uint32) []fsmetaexec.VisibleOverlayKV {
	if a.runtime == nil {
		return nil
	}
	return toVisibleOverlayKVs(a.runtime.ScanPerasDirectoryAt(overlayGeneration, sealedGeneration, prefix, start, limit))
}

func (a executorAdapter) ScanVisibleDirectory(prefix, start []byte, limit uint32) []fsmetaexec.VisibleOverlayKV {
	if a.runtime == nil {
		return nil
	}
	return toVisibleOverlayKVs(a.runtime.ScanPerasDirectory(prefix, start, limit))
}

func (a executorAdapter) HasVisibleDirectoryOverlay(prefix []byte) bool {
	return a.runtime != nil && a.runtime.HasPerasDirectory(prefix)
}

func (a executorAdapter) HasPendingVisibleDirectory(prefix []byte) bool {
	return a.runtime != nil && a.runtime.HasPerasVisibleDirectory(prefix)
}

func (a executorAdapter) VisibleDirectoryCacheFrontier(prefix []byte) uint64 {
	if a.runtime == nil {
		return 0
	}
	return a.runtime.PerasDirectoryCacheFrontier(prefix)
}

func (a executorAdapter) CaptureInstalledVisibleSnapshot(version uint64) error {
	if a.runtime == nil {
		return runtimeperas.ErrRuntimeInvalid
	}
	return a.runtime.CapturePerasSnapshot(version)
}

func (a executorAdapter) CaptureVisibleSnapshot(ctx context.Context, version uint64, scope compile.AuthorityScope) (model.VisibleSnapshotCapture, bool, error) {
	if a.runtime == nil {
		return model.VisibleSnapshotCapture{}, false, runtimeperas.ErrRuntimeInvalid
	}
	return a.runtime.CapturePerasVisibleSnapshot(ctx, version, scope)
}

func (a executorAdapter) GetVisibleSnapshotOverlayView(version uint64, key []byte) ([]byte, bool, bool) {
	if a.runtime == nil {
		return nil, false, false
	}
	return a.runtime.GetPerasSnapshotOverlayView(version, key)
}

func (a executorAdapter) ScanVisibleSnapshotDirectory(version uint64, prefix, start []byte, limit uint32) []fsmetaexec.VisibleOverlayKV {
	if a.runtime == nil {
		return nil
	}
	return toVisibleOverlayKVs(a.runtime.ScanPerasSnapshotDirectory(version, prefix, start, limit))
}

func (a executorAdapter) HasVisibleSnapshotDirectory(version uint64, prefix []byte) bool {
	return a.runtime != nil && a.runtime.HasPerasSnapshotDirectory(version, prefix)
}

func (a executorAdapter) RetireVisibleSnapshot(version uint64) {
	if a.runtime != nil {
		a.runtime.RetirePerasSnapshot(version)
	}
}

func (a executorAdapter) KeyState(key []byte) (bool, bool) {
	if a.runtime == nil {
		return false, false
	}
	return a.runtime.KeyState(key)
}

func (a executorAdapter) DirectoryEmpty(mount model.MountIdentity, inode model.InodeID) bool {
	return a.runtime != nil && a.runtime.DirectoryEmpty(mount, inode)
}

func (a executorAdapter) DirectoryBaseEmpty(mount model.MountIdentity, inode model.InodeID) bool {
	return a.runtime != nil && a.runtime.DirectoryBaseEmpty(mount, inode)
}

func (a executorAdapter) SessionNamespaceEmpty(mount model.MountIdentity, inode model.InodeID) bool {
	return a.runtime != nil && a.runtime.SessionNamespaceEmpty(mount, inode)
}

func (a executorAdapter) RememberKey(key []byte, present bool) {
	if a.runtime != nil {
		a.runtime.RememberKey(key, present)
	}
}

func (a executorAdapter) RememberEmptyDirectory(mount model.MountIdentity, inode model.InodeID) {
	if a.runtime != nil {
		a.runtime.RememberEmptyDirectory(mount, inode)
	}
}

func (a executorAdapter) RememberEmptySessionNamespace(mount model.MountIdentity, inode model.InodeID) {
	if a.runtime != nil {
		a.runtime.RememberEmptySessionNamespace(mount, inode)
	}
}

func (a executorAdapter) ForgetEmptyDirectory(mount model.MountIdentity, inode model.InodeID) {
	if a.runtime != nil {
		a.runtime.ForgetEmptyDirectory(mount, inode)
	}
}

func (a executorAdapter) Stats() map[string]any {
	if a.runtime == nil {
		return nil
	}
	return a.runtime.Stats()
}

func fromVisibleOperationID(id fsmetaexec.VisibleOperationID) fsperas.OperationID {
	return fsperas.OperationID{ClientID: id.ClientID, Seq: id.Seq}
}

func toVisibleOperationID(id fsperas.OperationID) fsmetaexec.VisibleOperationID {
	return fsmetaexec.VisibleOperationID{ClientID: id.ClientID, Seq: id.Seq}
}

func toVisibleAck(ack fsperas.VisibleAck) fsmetaexec.VisibleAck {
	return fsmetaexec.VisibleAck{
		EpochID:  ack.EpochID,
		OpID:     toVisibleOperationID(ack.OpID),
		HolderID: ack.HolderID,
	}
}

func fromVisibleAdmissionFunc(fn fsmetaexec.VisibleAdmissionFunc) fsperas.AdmissionFunc {
	if fn == nil {
		return nil
	}
	return func(ctx context.Context, op compile.MaterializedOp, admissionCtx fsperas.AdmissionContext) (fsperas.AdmissionResult, bool, error) {
		result, ok, err := fn(ctx, op, fsmetaexec.VisibleAdmissionContext{ProofFrontier: admissionCtx.ProofFrontier})
		if err != nil || !ok {
			return fsperas.AdmissionResult{}, ok, err
		}
		return fsperas.AdmissionResult{
			PredicateProofs: result.PredicateProofs,
			GuardProofs:     result.GuardProofs,
		}, true, nil
	}
}

func toVisibleOverlayKVs(rows []fsperas.OverlayKV) []fsmetaexec.VisibleOverlayKV {
	if len(rows) == 0 {
		return nil
	}
	out := make([]fsmetaexec.VisibleOverlayKV, len(rows))
	for i, row := range rows {
		out[i] = fsmetaexec.VisibleOverlayKV{
			Key:    row.Key,
			Value:  row.Value,
			Delete: row.Delete,
		}
	}
	return out
}

func mapPerasAdmissionError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fsperas.ErrAdmissionRejected):
		return errors.Join(fsmetaexec.ErrVisibleAdmissionRejected, err)
	case errors.Is(err, fsperas.ErrIneligibleOperation):
		return errors.Join(fsmetaexec.ErrVisibleIneligibleOperation, err)
	default:
		return err
	}
}
