// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package layout

import "github.com/feichai0017/NoKV/fsmeta/model"

// fsmeta operation plans define semantic key boundaries only. The executor
// owns value interpretation, conflict handling, and operation-specific checks;
// the transaction runner owns timestamps, retries, and MVCC mutation encoding.
//
// RenameSubtree moves only the subtree-root dentry. Descendants reference
// parent inode IDs, so they remain reachable without descendant key rewrites.

// OperationPlan describes the key set one metadata operation will touch.
//
// It is intentionally value-only. The transaction runner decides timestamps,
// retries, and mutation encoding; fsmeta only defines the semantic key boundary.
type OperationPlan struct {
	Kind         model.OperationKind
	Mount        model.MountID
	PrimaryKey   []byte
	StartKey     []byte
	Limit        uint32
	ReadKeys     [][]byte
	ReadPrefixes [][]byte
	MutateKeys   [][]byte
}

func PlanCreate(req model.CreateRequest, mount model.MountIdentity, inodeID model.InodeID) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if err := model.ValidateInodeID(req.Parent); err != nil {
		return OperationPlan{}, err
	}
	if err := model.ValidateInodeID(inodeID); err != nil {
		return OperationPlan{}, err
	}
	dentry, err := EncodeDentryKey(mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	parentInode, err := EncodeInodeKey(mount, req.Parent)
	if err != nil {
		return OperationPlan{}, err
	}
	inode, err := EncodeInodeKey(mount, inodeID)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationCreate,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(dentry),
		ReadKeys:   cloneKeySet(parentInode, dentry, inode),
		MutateKeys: cloneKeySet(parentInode, dentry, inode),
	}, nil
}

func PlanUpdateInode(req model.UpdateInodeRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if err := model.ValidateInodeID(req.Parent); err != nil {
		return OperationPlan{}, err
	}
	dentry, err := EncodeDentryKey(mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	inode, err := EncodeInodeKey(mount, req.Inode)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationUpdateInode,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(inode),
		ReadKeys:   cloneKeySet(dentry, inode),
		MutateKeys: cloneKeySet(inode),
	}, nil
}

func PlanLookup(req model.LookupRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	dentry, err := EncodeDentryKey(mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationLookup,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(dentry),
		ReadKeys:   cloneKeySet(dentry),
	}, nil
}

func PlanReadDir(req model.ReadDirRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	limit, err := model.NormalizeReadDirLimit(req.Limit)
	if err != nil {
		return OperationPlan{}, err
	}
	var startKey []byte
	if req.StartAfter == "" {
		prefix, err := EncodeDentryPrefix(mount, req.Parent)
		if err != nil {
			return OperationPlan{}, err
		}
		startKey = prefix
	} else {
		if err := model.ValidateName(req.StartAfter); err != nil {
			return OperationPlan{}, err
		}
		cursor, err := EncodeDentryKey(mount, req.Parent, req.StartAfter)
		if err != nil {
			return OperationPlan{}, err
		}
		// Names cannot contain NUL, so cursor+"\x00" is the first inclusive
		// seek key after the cursor while still staying inside the dentry range.
		startKey = append(cursor, 0)
	}
	prefix, err := EncodeDentryPrefix(mount, req.Parent)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:         model.OperationReadDir,
		Mount:        req.Mount,
		PrimaryKey:   cloneBytes(prefix),
		StartKey:     cloneBytes(startKey),
		Limit:        limit,
		ReadPrefixes: cloneKeySet(prefix),
	}, nil
}

func PlanSnapshotSubtree(req model.SnapshotSubtreeRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if err := model.ValidateInodeID(req.RootInode); err != nil {
		return OperationPlan{}, err
	}
	prefix, err := EncodeDentryPrefix(mount, req.RootInode)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:         model.OperationSnapshotSubtree,
		Mount:        req.Mount,
		PrimaryKey:   cloneBytes(prefix),
		ReadPrefixes: cloneKeySet(prefix),
	}, nil
}

func PlanRename(req model.RenameRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if req.FromParent == req.ToParent && req.FromName == req.ToName {
		return OperationPlan{}, model.ErrInvalidRequest
	}
	source, err := EncodeDentryKey(mount, req.FromParent, req.FromName)
	if err != nil {
		return OperationPlan{}, err
	}
	dest, err := EncodeDentryKey(mount, req.ToParent, req.ToName)
	if err != nil {
		return OperationPlan{}, err
	}
	fromParent, err := EncodeInodeKey(mount, req.FromParent)
	if err != nil {
		return OperationPlan{}, err
	}
	toParent, err := EncodeInodeKey(mount, req.ToParent)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationRename,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(source),
		ReadKeys:   cloneKeySet(source, dest, fromParent, toParent),
		MutateKeys: cloneKeySet(source, dest, fromParent, toParent),
	}, nil
}

func PlanRenameReplace(req model.RenameReplaceRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if req.FromParent == req.ToParent && req.FromName == req.ToName {
		return OperationPlan{}, model.ErrInvalidRequest
	}
	source, err := EncodeDentryKey(mount, req.FromParent, req.FromName)
	if err != nil {
		return OperationPlan{}, err
	}
	dest, err := EncodeDentryKey(mount, req.ToParent, req.ToName)
	if err != nil {
		return OperationPlan{}, err
	}
	fromParent, err := EncodeInodeKey(mount, req.FromParent)
	if err != nil {
		return OperationPlan{}, err
	}
	toParent, err := EncodeInodeKey(mount, req.ToParent)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationRenameReplace,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(source),
		ReadKeys:   cloneKeySet(source, dest, fromParent, toParent),
		MutateKeys: cloneKeySet(source, dest, fromParent, toParent),
	}, nil
}

func PlanRenameSubtree(req model.RenameSubtreeRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if req.FromParent == req.ToParent && req.FromName == req.ToName {
		return OperationPlan{}, model.ErrInvalidRequest
	}
	from, err := EncodeDentryKey(mount, req.FromParent, req.FromName)
	if err != nil {
		return OperationPlan{}, err
	}
	to, err := EncodeDentryKey(mount, req.ToParent, req.ToName)
	if err != nil {
		return OperationPlan{}, err
	}
	fromParent, err := EncodeInodeKey(mount, req.FromParent)
	if err != nil {
		return OperationPlan{}, err
	}
	toParent, err := EncodeInodeKey(mount, req.ToParent)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationRenameSubtree,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(from),
		ReadKeys:   cloneKeySet(from, to, fromParent, toParent),
		MutateKeys: cloneKeySet(from, to, fromParent, toParent),
	}, nil
}

func PlanLink(req model.LinkRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if req.FromParent == req.ToParent && req.FromName == req.ToName {
		return OperationPlan{}, model.ErrInvalidRequest
	}
	from, err := EncodeDentryKey(mount, req.FromParent, req.FromName)
	if err != nil {
		return OperationPlan{}, err
	}
	to, err := EncodeDentryKey(mount, req.ToParent, req.ToName)
	if err != nil {
		return OperationPlan{}, err
	}
	toParent, err := EncodeInodeKey(mount, req.ToParent)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationLink,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(to),
		ReadKeys:   cloneKeySet(from, to, toParent),
		MutateKeys: cloneKeySet(to, toParent),
	}, nil
}

func PlanUnlink(req model.UnlinkRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	dentry, err := EncodeDentryKey(mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	parentInode, err := EncodeInodeKey(mount, req.Parent)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationUnlink,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(dentry),
		ReadKeys:   cloneKeySet(dentry, parentInode),
		MutateKeys: cloneKeySet(dentry, parentInode),
	}, nil
}

func PlanRemove(req model.RemoveRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	dentry, err := EncodeDentryKey(mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	parentInode, err := EncodeInodeKey(mount, req.Parent)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationRemove,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(dentry),
		ReadKeys:   cloneKeySet(dentry, parentInode),
		MutateKeys: cloneKeySet(dentry, parentInode),
	}, nil
}

func PlanRemoveDirectory(req model.RemoveDirectoryRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	parentInode, err := EncodeInodeKey(mount, req.Parent)
	if err != nil {
		return OperationPlan{}, err
	}
	dentry, err := EncodeDentryKey(mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationRemoveDirectory,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(dentry),
		ReadKeys:   cloneKeySet(parentInode, dentry),
		MutateKeys: cloneKeySet(parentInode, dentry),
	}, nil
}

func PlanOpenWriteSession(req model.OpenWriteSessionRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	inode, err := EncodeInodeKey(mount, req.Inode)
	if err != nil {
		return OperationPlan{}, err
	}
	session, err := EncodeSessionKey(mount, req.Inode, req.Session)
	if err != nil {
		return OperationPlan{}, err
	}
	owner, err := EncodeInodeSessionKey(mount, req.Inode)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationOpenWriteSession,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(session),
		ReadKeys:   cloneKeySet(inode, session, owner),
		MutateKeys: cloneKeySet(session, owner),
	}, nil
}

func PlanHeartbeatWriteSession(req model.HeartbeatWriteSessionRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	session, err := EncodeSessionKey(mount, req.Inode, req.Session)
	if err != nil {
		return OperationPlan{}, err
	}
	owner, err := EncodeInodeSessionKey(mount, req.Inode)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationHeartbeatSession,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(session),
		ReadKeys:   cloneKeySet(session, owner),
		MutateKeys: cloneKeySet(session, owner),
	}, nil
}

func PlanCloseWriteSession(req model.CloseWriteSessionRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	session, err := EncodeSessionKey(mount, req.Inode, req.Session)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       model.OperationCloseSession,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(session),
		ReadKeys:   cloneKeySet(session),
		MutateKeys: cloneKeySet(session),
	}, nil
}

func PlanExpireWriteSessions(req model.ExpireWriteSessionsRequest, mount model.MountIdentity) (OperationPlan, error) {
	if err := model.ValidateMountIdentityForRequest(mount, req.Mount); err != nil {
		return OperationPlan{}, err
	}
	limit, err := model.NormalizeSessionExpireLimit(req.Limit)
	if err != nil {
		return OperationPlan{}, err
	}
	prefixes := make([][]byte, 0, DefaultAffinityBucketCount)
	for bucket := range DefaultAffinityBucketCount {
		prefix, err := EncodeSessionBucketPrefix(mount, AffinityBucket(bucket))
		if err != nil {
			return OperationPlan{}, err
		}
		prefixes = append(prefixes, prefix)
	}
	return OperationPlan{
		Kind:         model.OperationExpireSessions,
		Mount:        req.Mount,
		PrimaryKey:   cloneBytes(prefixes[0]),
		StartKey:     cloneBytes(prefixes[0]),
		Limit:        limit,
		ReadPrefixes: cloneKeySet(prefixes...),
	}, nil
}

func cloneKeySet(keys ...[]byte) [][]byte {
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		out = append(out, cloneBytes(key))
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
