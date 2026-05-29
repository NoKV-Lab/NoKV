// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"slices"
	"sort"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func (e *Executor) tryVisibleOpenWriteSession(ctx context.Context, program compile.OpenWriteSessionProgram, mount model.MountIdentity, req model.OpenWriteSessionRequest) (model.SessionRecord, bool, error) {
	delta := program.Compiled.Delta
	if e == nil || e.visibleCommitter == nil || e.visibleAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return model.SessionRecord{}, false, nil
	}
	plan := delta.Plan
	view := e.newVisibleReadView(ctx)
	inode, ok, err := view.readInode(mount, req.Inode)
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	if !ok {
		return model.SessionRecord{}, false, nil
	}
	if inode.Type != model.InodeTypeFile {
		return model.SessionRecord{}, false, nil
	}
	nowTime := e.clock()
	expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
	if !ok {
		return model.SessionRecord{}, false, nil
	}
	now := nowTime.UnixNano()
	if existing, ok, err := view.readSession(mount, plan.ReadKeys[1]); err != nil {
		return model.SessionRecord{}, false, err
	} else if ok {
		if sessionLive(existing, now) {
			return model.SessionRecord{}, false, nil
		}
		// Stale cleanup is value-sensitive and may touch an old session-id key
		// outside this request's concrete write-set. Keep it on the transaction
		// runner.
		return model.SessionRecord{}, false, nil
	}
	if index := e.visiblePredicateIndex(); !e.visibleNotExistsKnown(delta.Authority, plan.ReadKeys[2], index) {
		if owner, ok, err := view.readSession(mount, plan.ReadKeys[2]); err != nil {
			return model.SessionRecord{}, false, err
		} else if ok {
			if sessionLive(owner, now) {
				return model.SessionRecord{}, false, nil
			}
			return model.SessionRecord{}, false, nil
		}
	}
	record := model.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
	value, err := layout.EncodeSessionValue(record)
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	evidence := view.predicateEvidenceForDelta(delta)
	concrete, err := compile.MaterializeOpenWriteSession(program, compile.OpenWriteSessionValues{
		SessionValue:    value,
		PredicateProofs: evidence.Proofs,
	})
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	committed, err := e.tryVisibleCommitAfterRead(ctx, view, concrete)
	if err != nil {
		return model.SessionRecord{}, committed, err
	}
	if !committed {
		return model.SessionRecord{}, false, nil
	}
	return record, true, nil
}

func (e *Executor) tryVisibleHeartbeatWriteSession(ctx context.Context, program compile.HeartbeatWriteSessionProgram, mount model.MountIdentity, req model.HeartbeatWriteSessionRequest) (model.SessionRecord, bool, error) {
	delta := program.Compiled.Delta
	if e == nil || e.visibleCommitter == nil || e.visibleAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return model.SessionRecord{}, false, nil
	}
	plan := delta.Plan
	view := e.newVisibleReadView(ctx)
	nowTime := e.clock()
	expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
	if !ok {
		return model.SessionRecord{}, false, nil
	}
	now := nowTime.UnixNano()
	session, ok, err := view.readSession(mount, plan.ReadKeys[0])
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	if !ok || !sessionLive(session, now) || session.Inode != req.Inode {
		return model.SessionRecord{}, false, nil
	}
	owner, ok, err := view.readSession(mount, plan.ReadKeys[1])
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	if !ok || !sessionLive(owner, now) || owner.Session != req.Session || owner.Inode != req.Inode {
		return model.SessionRecord{}, false, nil
	}
	record := model.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
	value, err := layout.EncodeSessionValue(record)
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	evidence := view.predicateEvidenceForDelta(delta)
	concrete, err := compile.MaterializeHeartbeatWriteSession(program, compile.HeartbeatWriteSessionValues{
		SessionValue:    value,
		PredicateProofs: evidence.Proofs,
	})
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	committed, err := e.tryVisibleCommitAfterRead(ctx, view, concrete)
	if err != nil {
		return model.SessionRecord{}, committed, err
	}
	if !committed {
		return model.SessionRecord{}, false, nil
	}
	return record, true, nil
}

func (e *Executor) tryVisibleCloseWriteSession(ctx context.Context, program compile.CloseWriteSessionProgram, mount model.MountIdentity, req model.CloseWriteSessionRequest) (bool, error) {
	delta := program.Compiled.Delta
	if e == nil || e.visibleCommitter == nil || e.visibleAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	plan := delta.Plan
	view := e.newVisibleReadView(ctx)
	session, ok, err := view.readSession(mount, plan.ReadKeys[0])
	if err != nil {
		return false, err
	}
	if !ok || session.Inode != req.Inode {
		return false, nil
	}
	deleteOwner := false
	ownerKey, err := layout.EncodeInodeSessionKey(mount, session.Inode)
	if err != nil {
		return false, err
	}
	if owner, ok, err := view.readSession(mount, ownerKey); err != nil {
		return false, err
	} else if ok && owner.Session == req.Session && owner.Inode == session.Inode {
		deleteOwner = true
	}
	evidence := view.predicateEvidenceForDelta(delta)
	concrete, err := compile.MaterializeCloseWriteSession(program, compile.CloseWriteSessionValues{
		DeleteOwner:     deleteOwner,
		PredicateProofs: evidence.Proofs,
	})
	if err != nil {
		return false, err
	}
	return e.tryVisibleCommitAfterRead(ctx, view, concrete)
}

func (e *Executor) tryVisibleExpireWriteSession(ctx context.Context, mount model.MountIdentity, record model.SessionRecord) (bool, error) {
	program, err := compile.CompileCloseWriteSessionProgram(model.CloseWriteSessionRequest{
		Mount:   mount.MountID,
		Inode:   record.Inode,
		Session: record.Session,
	}, mount)
	if err != nil {
		return false, err
	}
	return e.tryVisibleCloseWriteSession(ctx, program, mount, model.CloseWriteSessionRequest{
		Mount:   mount.MountID,
		Inode:   record.Inode,
		Session: record.Session,
	})
}

func sessionDrainScopeForInodes(mount model.MountIdentity, inodes map[model.InodeID]struct{}) compile.AuthorityScope {
	if mount.MountKeyID == 0 || len(inodes) == 0 {
		return compile.AuthorityScope{}
	}
	out := compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Inodes:     make([]model.InodeID, 0, len(inodes)),
	}
	seenBuckets := make(map[layout.AffinityBucket]struct{}, len(inodes))
	for inode := range inodes {
		out.Inodes = append(out.Inodes, inode)
		bucket := layout.BucketForInodeID(inode)
		if _, ok := seenBuckets[bucket]; !ok {
			seenBuckets[bucket] = struct{}{}
			out.Buckets = append(out.Buckets, bucket)
		}
	}
	slices.Sort(out.Inodes)
	slices.Sort(out.Buckets)
	return out
}

// OpenWriteSession records one exclusive writer lease for an inode. It writes
// both a session-id key and an inode-owner key so concurrent opens for the same
// inode conflict on one Percolator key.
func (e *Executor) OpenWriteSession(ctx context.Context, req model.OpenWriteSessionRequest) (model.SessionRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.SessionRecord{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileOpenWriteSessionProgram(req, mount)
	if err != nil {
		return model.SessionRecord{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return model.SessionRecord{}, err
	}
	plan := delta.Plan
	if req.TTL <= 0 {
		return model.SessionRecord{}, model.ErrInvalidRequest
	}
	if record, committed, err := e.tryVisibleOpenWriteSession(ctx, program, mount, req); committed || err != nil {
		if err != nil {
			return model.SessionRecord{}, err
		}
		return record, nil
	}
	var record model.SessionRecord
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		inode, ok, err := e.readInode(ctx, mount, req.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return model.ErrNotFound
		}
		if inode.Type != model.InodeTypeFile {
			return model.ErrInvalidRequest
		}
		inodeKey, err := layout.EncodeInodeKey(mount, inode.Inode)
		if err != nil {
			return err
		}
		inodeValue, err := layout.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		nowTime := e.clock()
		expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
		if !ok {
			return model.ErrInvalidRequest
		}
		candidate := model.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
		now := nowTime.UnixNano()
		predicates := make([]*backend.Predicate, 0, 4)
		if existing, ok, err := e.readSessionByKey(ctx, mount, plan.ReadKeys[1], startVersion); err != nil {
			return err
		} else if ok && sessionLive(existing, now) {
			return model.ErrExists
		} else if ok {
			existingValue, err := layout.EncodeSessionValue(existing)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(plan.ReadKeys[1], existingValue))
		} else {
			predicates = append(predicates, atomicNotExists(plan.ReadKeys[1]))
		}
		mutations := make([]*backend.Mutation, 0, 3)
		if owner, ok, err := e.readSessionByKey(ctx, mount, plan.ReadKeys[2], startVersion); err != nil {
			return err
		} else if ok {
			if sessionLive(owner, now) {
				return model.ErrExists
			}
			ownerValue, err := layout.EncodeSessionValue(owner)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(plan.ReadKeys[2], ownerValue))
			staleSessionKey, err := layout.EncodeSessionKey(mount, owner.Inode, owner.Session)
			if err != nil {
				return err
			}
			if string(staleSessionKey) != string(plan.ReadKeys[1]) {
				if value, ok, err := e.runner.Get(ctx, staleSessionKey, startVersion); err != nil {
					return err
				} else if ok && bytes.Equal(value, ownerValue) {
					predicates = append(predicates, atomicValueEquals(staleSessionKey, ownerValue))
					mutations = append(mutations, &backend.Mutation{Op: backend.MutationDelete, Key: staleSessionKey})
				}
			}
		} else {
			predicates = append(predicates, atomicNotExists(plan.ReadKeys[2]))
		}
		value, err := layout.EncodeSessionValue(candidate)
		if err != nil {
			return err
		}
		mutations = append(mutations,
			&backend.Mutation{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[0]), Value: value},
			&backend.Mutation{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[1]), Value: value},
		)
		predicates = append(predicates, atomicValueEquals(inodeKey, inodeValue))
		// Open is a value-sensitive admission path: the session-id key, owner
		// key, inode key, and any stale cleanup key must still match the values
		// read above. Value predicates make the 1PC attempt a real CAS instead
		// of an existence-only overwrite.
		if err := e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion); err != nil {
			return err
		}
		record = candidate
		return nil
	}, delta.Authority); err != nil {
		return model.SessionRecord{}, err
	}
	return record, nil
}

// HeartbeatWriteSession extends a live writer lease. Both session records must
// agree, otherwise the session is considered lost and the caller must reopen.
func (e *Executor) HeartbeatWriteSession(ctx context.Context, req model.HeartbeatWriteSessionRequest) (model.SessionRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.SessionRecord{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileHeartbeatWriteSessionProgram(req, mount)
	if err != nil {
		return model.SessionRecord{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return model.SessionRecord{}, err
	}
	plan := delta.Plan
	if req.TTL <= 0 {
		return model.SessionRecord{}, model.ErrInvalidRequest
	}
	if record, committed, err := e.tryVisibleHeartbeatWriteSession(ctx, program, mount, req); committed || err != nil {
		if err != nil {
			return model.SessionRecord{}, err
		}
		return record, nil
	}
	var record model.SessionRecord
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		nowTime := e.clock()
		expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
		if !ok {
			return model.ErrInvalidRequest
		}
		candidate := model.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
		now := nowTime.UnixNano()
		session, ok, err := e.readSessionByKey(ctx, mount, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		if !ok || !sessionLive(session, now) || session.Inode != req.Inode {
			return model.ErrNotFound
		}
		sessionValue, err := layout.EncodeSessionValue(session)
		if err != nil {
			return err
		}
		owner, ok, err := e.readSessionByKey(ctx, mount, plan.ReadKeys[1], startVersion)
		if err != nil {
			return err
		}
		if !ok || !sessionLive(owner, now) || owner.Session != req.Session || owner.Inode != req.Inode {
			return model.ErrNotFound
		}
		ownerValue, err := layout.EncodeSessionValue(owner)
		if err != nil {
			return err
		}
		value, err := layout.EncodeSessionValue(candidate)
		if err != nil {
			return err
		}
		mutations := []*backend.Mutation{
			{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[0]), Value: value},
			{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[1]), Value: value},
		}
		predicates := []*backend.Predicate{
			atomicValueEquals(plan.ReadKeys[0], sessionValue),
			atomicValueEquals(plan.ReadKeys[1], ownerValue),
		}
		if err := e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion); err != nil {
			return err
		}
		record = candidate
		return nil
	}, delta.Authority); err != nil {
		return model.SessionRecord{}, err
	}
	return record, nil
}

// CloseWriteSession releases one writer lease. It deletes the owner key only
// when it still points at the closing session.
func (e *Executor) CloseWriteSession(ctx context.Context, req model.CloseWriteSessionRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileCloseWriteSessionProgram(req, mount)
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	if committed, err := e.tryVisibleCloseWriteSession(ctx, program, mount, req); committed || err != nil {
		return err
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		session, ok, err := e.readSessionByKey(ctx, mount, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return model.ErrNotFound
		}
		if session.Inode != req.Inode {
			return model.ErrNotFound
		}
		sessionValue, err := layout.EncodeSessionValue(session)
		if err != nil {
			return err
		}
		mutations := []*backend.Mutation{{Op: backend.MutationDelete, Key: cloneBytes(plan.MutateKeys[0])}}
		predicates := []*backend.Predicate{atomicValueEquals(plan.ReadKeys[0], sessionValue)}
		ownerKey, err := layout.EncodeInodeSessionKey(mount, session.Inode)
		if err != nil {
			return err
		}
		if owner, ok, err := e.readSessionByKey(ctx, mount, ownerKey, startVersion); err != nil {
			return err
		} else if ok && owner.Session == req.Session && owner.Inode == session.Inode {
			ownerValue, err := layout.EncodeSessionValue(owner)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(ownerKey, ownerValue))
			mutations = append(mutations, &backend.Mutation{Op: backend.MutationDelete, Key: ownerKey})
		}
		return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	return nil
}

// ExpireWriteSessions removes stale session-id and inode-owner records for one
// mount. It is a bounded maintenance primitive; callers should repeat until
// Expired is zero when draining a large backlog.
func (e *Executor) ExpireWriteSessions(ctx context.Context, req model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.ExpireWriteSessionsResult{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileExpireWriteSessionsProgram(req, mount)
	if err != nil {
		return model.ExpireWriteSessionsResult{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return model.ExpireWriteSessionsResult{}, err
	}
	plan := delta.Plan
	now := e.clock().UnixNano()
	var expired uint64
	scanPrefixes := plan.ReadPrefixes
	if err := e.withTxnRetryNoVisibleFlush(ctx, func(startVersion, commitVersion uint64) error {
		deletes := make(map[string][]byte)
		type expiredSessionKey struct {
			inode   model.InodeID
			session model.SessionID
		}
		expiredSessions := make(map[expiredSessionKey]struct{})
		fallbackInodes := make(map[model.InodeID]struct{})
		remaining := plan.Limit
		for _, scanPrefix := range scanPrefixes {
			if remaining == 0 {
				break
			}
			kvs, err := e.runner.Scan(ctx, scanPrefix, remaining, startVersion)
			if err != nil {
				return err
			}
			kvs = e.mergeVisibleOverlayScan(kvs, scanPrefix, remaining)
			var matched uint32
			for _, kv := range kvs {
				if !bytes.HasPrefix(kv.Key, scanPrefix) {
					break
				}
				matched++
				kind, err := layout.KeyKindOf(kv.Key)
				if err != nil {
					return err
				}
				if kind != layout.KeyKindSession {
					continue
				}
				record, err := layout.DecodeSessionValue(kv.Value)
				if err != nil {
					return err
				}
				if sessionLive(record, now) {
					continue
				}
				expiredKey := expiredSessionKey{inode: record.Inode, session: record.Session}
				if _, seen := expiredSessions[expiredKey]; seen {
					continue
				}
				if committed, err := e.tryVisibleExpireWriteSession(ctx, mount, record); err != nil {
					return err
				} else if committed {
					expiredSessions[expiredKey] = struct{}{}
					continue
				}
				deletes[string(kv.Key)] = cloneBytes(kv.Key)
				sessionKey, err := layout.EncodeSessionKey(mount, record.Inode, record.Session)
				if err != nil {
					return err
				}
				ownerKey, err := layout.EncodeInodeSessionKey(mount, record.Inode)
				if err != nil {
					return err
				}
				if value, ok, err := e.runner.Get(ctx, sessionKey, startVersion); err != nil {
					return err
				} else if ok && bytes.Equal(value, kv.Value) {
					deletes[string(sessionKey)] = sessionKey
					expiredSessions[expiredSessionKey{inode: record.Inode, session: record.Session}] = struct{}{}
				}
				if value, ok, err := e.runner.Get(ctx, ownerKey, startVersion); err != nil {
					return err
				} else if ok && bytes.Equal(value, kv.Value) {
					deletes[string(ownerKey)] = ownerKey
				}
				fallbackInodes[record.Inode] = struct{}{}
			}
			remaining -= matched
		}
		if len(deletes) == 0 {
			expired = uint64(len(expiredSessions))
			return nil
		}
		drainScope := sessionDrainScopeForInodes(mount, fallbackInodes)
		if authorityScopeEmpty(drainScope) {
			drainScope = delta.Authority
		}
		// Fallback expiration mutates base LSM session keys. Drain only after
		// concrete expired keys are known so ordinary visible session updates do
		// not wait behind speculative maintenance scans.
		if err := e.drainVisibleAuthority(ctx, drainScope); err != nil {
			return err
		}
		keys := make([]string, 0, len(deletes))
		for key := range deletes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		mutations := make([]*backend.Mutation, 0, len(deletes))
		for _, key := range keys {
			mutations = append(mutations, &backend.Mutation{Op: backend.MutationDelete, Key: deletes[key]})
		}
		primary := deletes[keys[0]]
		if _, err := e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL); err != nil {
			return err
		}
		expired = uint64(len(expiredSessions))
		return nil
	}); err != nil {
		return model.ExpireWriteSessionsResult{}, err
	}
	return model.ExpireWriteSessionsResult{Expired: expired}, nil
}

func sessionExpiryUnixNs(now time.Time, ttl time.Duration) (int64, bool) {
	if ttl <= 0 {
		return 0, false
	}
	const maxInt64 = int64(1<<63 - 1)
	nowUnixNs := now.UnixNano()
	ttlUnixNs := int64(ttl)
	if nowUnixNs > maxInt64-ttlUnixNs {
		return 0, false
	}
	return nowUnixNs + ttlUnixNs, true
}

func (e *Executor) clock() time.Time {
	if e != nil && e.now != nil {
		return e.now()
	}
	return time.Now()
}

func sessionLive(record model.SessionRecord, nowUnixNs int64) bool {
	return record.ExpiresUnixNs > nowUnixNs
}
