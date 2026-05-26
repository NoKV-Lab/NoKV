// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// SnapshotSubtree reserves a durable MVCC read version for one direct subtree
// root. The service boundary publishes the returned token into rooted truth so
// GC can treat it as a retained snapshot until RetireSnapshotSubtree.
func (e *Executor) SnapshotSubtree(ctx context.Context, req model.SnapshotSubtreeRequest) (model.SnapshotSubtreeToken, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.SnapshotSubtreeToken{}, err
	}
	program, err := compile.CompileSnapshotSubtreeProgram(req, mountRecord.Identity())
	if err != nil {
		return model.SnapshotSubtreeToken{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return model.SnapshotSubtreeToken{}, err
	}
	if capturer, ok := e.visibleCommitter.(VisibleSnapshotCapturer); ok {
		version, err := e.reserveReadVersion(ctx)
		if err != nil {
			return model.SnapshotSubtreeToken{}, err
		}
		capture, captured, err := capturer.CaptureVisibleSnapshot(ctx, version, delta.Authority)
		if err != nil {
			return model.SnapshotSubtreeToken{}, err
		}
		if captured {
			return model.SnapshotSubtreeToken{
				Mount:           req.Mount,
				MountKeyID:      mountRecord.MountKeyID,
				RootInode:       req.RootInode,
				ReadVersion:     version,
				RuntimeEvidence: append([]model.SnapshotEvidenceRef(nil), capture.Evidence...),
			}, nil
		}
	}
	if err := e.flushVisibleAuthority(ctx, delta.Authority); err != nil {
		return model.SnapshotSubtreeToken{}, err
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return model.SnapshotSubtreeToken{}, err
	}
	if capturer, ok := e.visibleCommitter.(InstalledVisibleSnapshotCapturer); ok {
		if err := capturer.CaptureInstalledVisibleSnapshot(version); err != nil {
			return model.SnapshotSubtreeToken{}, err
		}
	}
	return model.SnapshotSubtreeToken{
		Mount:       req.Mount,
		MountKeyID:  mountRecord.MountKeyID,
		RootInode:   req.RootInode,
		ReadVersion: version,
	}, nil
}

func (e *Executor) ResolveSnapshotSubtreeToken(ctx context.Context, token model.SnapshotSubtreeToken) (model.SnapshotSubtreeToken, error) {
	record, err := e.resolveKnownMount(ctx, token.Mount)
	if err != nil {
		return model.SnapshotSubtreeToken{}, err
	}
	if token.RootInode == 0 || token.ReadVersion == 0 {
		return model.SnapshotSubtreeToken{}, model.ErrInvalidRequest
	}
	for _, ref := range token.RuntimeEvidence {
		if !ref.Valid() {
			return model.SnapshotSubtreeToken{}, model.ErrInvalidRequest
		}
	}
	token.MountKeyID = record.MountKeyID
	return token.Clone(), nil
}

func (e *Executor) RetireVisibleSnapshot(version uint64) {
	if e == nil || version == 0 {
		return
	}
	retirer, ok := e.visibleCommitter.(visibleSnapshotRetirer)
	if !ok {
		return
	}
	retirer.RetireVisibleSnapshot(version)
}
