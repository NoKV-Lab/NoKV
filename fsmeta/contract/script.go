// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"math/rand"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

// GenerateScript returns a deterministic fsmeta operation script for one seed.
// The generator plans against the same reference model used by Run so invalid
// operations are intentional API-edge coverage rather than random noise.
func GenerateScript(seed int64, steps int) []Operation {
	rng := rand.New(rand.NewSource(seed))
	state := NewModel("vol")
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	sessions := []model.SessionID{"writer-a", "writer-b", "writer-c"}
	nextInode := model.InodeID(10)
	nextSnapshotRef := 0
	syntheticSnapshotVersion := uint64(10_000)
	ops := make([]Operation, 0, steps)
	for range steps {
		op := chooseOperation(rng, state, names, sessions, &nextInode, &nextSnapshotRef)
		ops = append(ops, op)
		if op.Kind == OpSnapshotSubtree {
			_ = state.ApplySnapshot(op, model.SnapshotSubtreeToken{
				Mount:       op.Mount,
				RootInode:   op.Parent,
				ReadVersion: syntheticSnapshotVersion,
			})
			syntheticSnapshotVersion++
			continue
		}
		_ = state.Apply(op)
	}
	return ops
}

func chooseOperation(rng *rand.Rand, state *Model, names []string, sessions []model.SessionID, nextInode *model.InodeID, nextSnapshotRef *int) Operation {
	existing := state.ExistingDentries()
	files := state.ExistingFileDentries()
	liveSessions := state.ExistingSessions()
	if len(existing) < 2 && rng.Intn(100) < 60 {
		return createOperation(rng, state, names, nextInode)
	}
	switch rng.Intn(100) {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9:
		return createOperation(rng, state, names, nextInode)
	case 10, 11, 12, 13, 14, 15, 16, 17:
		return lookupOperation(rng, state, names)
	case 18, 19, 20, 21, 22, 23, 24, 25:
		return readDirPlusOperation(rng, state, names)
	case 26, 27, 28, 29, 30, 31, 32, 33:
		return renameOperation(rng, state, names)
	case 34, 35, 36, 37, 38, 39, 40:
		return renameReplaceOperation(rng, state, names)
	case 41, 42, 43, 44:
		return linkOperation(rng, state, names)
	case 45, 46, 47:
		return unlinkOperation(rng, state, names)
	case 48, 49, 50:
		return removeOperation(rng, state, names)
	case 51, 52, 53:
		return updateOperation(rng, state, names)
	case 55, 56, 57, 58, 59:
		if len(files) == 0 {
			return createOperation(rng, state, names, nextInode)
		}
		record := files[rng.Intn(len(files))]
		return Operation{
			Kind:      OpOpenWriteSession,
			Mount:     state.Mount,
			Inode:     record.Inode,
			Session:   sessions[rng.Intn(len(sessions))],
			ExpiresNs: state.NowUnixNs + int64(1+rng.Intn(10))*int64(time.Second),
		}
	case 60, 61, 62, 63:
		if len(liveSessions) == 0 {
			return readDirPlusOperation(rng, state, names)
		}
		session := liveSessions[rng.Intn(len(liveSessions))]
		return Operation{
			Kind:      OpHeartbeatSession,
			Mount:     state.Mount,
			Inode:     session.Inode,
			Session:   session.Session,
			ExpiresNs: state.NowUnixNs + int64(10+rng.Intn(10))*int64(time.Second),
		}
	case 64, 65, 66:
		if len(liveSessions) == 0 {
			return lookupOperation(rng, state, names)
		}
		session := liveSessions[rng.Intn(len(liveSessions))]
		return Operation{Kind: OpCloseSession, Mount: state.Mount, Inode: session.Inode, Session: session.Session}
	case 67, 68:
		return Operation{
			Kind:      OpAdvanceTime,
			Mount:     state.Mount,
			AdvanceNs: int64(1+rng.Intn(6)) * int64(time.Second),
		}
	case 69, 70:
		return Operation{Kind: OpExpireSessions, Mount: state.Mount, Limit: 16}
	case 71, 72, 73:
		ref := *nextSnapshotRef
		*nextSnapshotRef++
		return Operation{Kind: OpSnapshotSubtree, Mount: state.Mount, Parent: state.Root, SnapshotRef: ref}
	default:
		return readDirPlusOperation(rng, state, names)
	}
}

func createOperation(rng *rand.Rand, state *Model, names []string, nextInode *model.InodeID) Operation {
	name := names[rng.Intn(len(names))]
	if rng.Intn(100) < 70 {
		if free := freeNames(state, names); len(free) > 0 {
			name = free[rng.Intn(len(free))]
		}
	}
	inode := *nextInode
	*nextInode++
	typ := model.InodeTypeFile
	if rng.Intn(100) < 20 {
		typ = model.InodeTypeDirectory
	}
	return Operation{
		Kind:   OpCreate,
		Mount:  state.Mount,
		Parent: state.Root,
		Name:   name,
		Inode:  inode,
		Type:   typ,
		Size:   uint64(1 + rng.Intn(4096)),
		Mode:   0o600 + uint32(rng.Intn(0o177)),
	}
}

func lookupOperation(rng *rand.Rand, state *Model, names []string) Operation {
	name := names[rng.Intn(len(names))]
	if existing := state.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 70 {
		name = existing[rng.Intn(len(existing))].Name
	}
	return Operation{Kind: OpLookup, Mount: state.Mount, Parent: state.Root, Name: name}
}

func readDirPlusOperation(rng *rand.Rand, state *Model, names []string) Operation {
	startAfter := ""
	if rng.Intn(100) < 25 {
		startAfter = names[rng.Intn(len(names))]
	}
	snapshotRef := -1
	refs := state.KnownSnapshotRefs()
	if len(refs) > 0 && rng.Intn(100) < 35 {
		snapshotRef = refs[rng.Intn(len(refs))]
	}
	return Operation{
		Kind:        OpReadDirPlus,
		Mount:       state.Mount,
		Parent:      state.Root,
		StartAfter:  startAfter,
		Limit:       uint32(1 + rng.Intn(6)),
		SnapshotRef: snapshotRef,
	}
}

func renameOperation(rng *rand.Rand, state *Model, names []string) Operation {
	fromName := names[rng.Intn(len(names))]
	if existing := state.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 80 {
		fromName = existing[rng.Intn(len(existing))].Name
	}
	toName := names[rng.Intn(len(names))]
	if rng.Intn(100) < 70 {
		if free := freeNames(state, names); len(free) > 0 {
			toName = free[rng.Intn(len(free))]
		}
	}
	return Operation{
		Kind:       OpRename,
		Mount:      state.Mount,
		FromParent: state.Root,
		FromName:   fromName,
		ToParent:   state.Root,
		ToName:     toName,
	}
}

func renameReplaceOperation(rng *rand.Rand, state *Model, names []string) Operation {
	fromName := names[rng.Intn(len(names))]
	if files := state.ExistingFileDentries(); len(files) > 0 && rng.Intn(100) < 85 {
		fromName = files[rng.Intn(len(files))].Name
	} else if existing := state.ExistingDentries(); len(existing) > 0 {
		fromName = existing[rng.Intn(len(existing))].Name
	}
	toName := names[rng.Intn(len(names))]
	if existing := state.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 70 {
		toName = existing[rng.Intn(len(existing))].Name
	} else if free := freeNames(state, names); len(free) > 0 {
		toName = free[rng.Intn(len(free))]
	}
	return Operation{
		Kind:       OpRenameReplace,
		Mount:      state.Mount,
		FromParent: state.Root,
		FromName:   fromName,
		ToParent:   state.Root,
		ToName:     toName,
	}
}

func linkOperation(rng *rand.Rand, state *Model, names []string) Operation {
	fromName := names[rng.Intn(len(names))]
	if files := state.ExistingFileDentries(); len(files) > 0 && rng.Intn(100) < 85 {
		fromName = files[rng.Intn(len(files))].Name
	} else if existing := state.ExistingDentries(); len(existing) > 0 {
		fromName = existing[rng.Intn(len(existing))].Name
	}
	toName := names[rng.Intn(len(names))]
	if rng.Intn(100) < 75 {
		if free := freeNames(state, names); len(free) > 0 {
			toName = free[rng.Intn(len(free))]
		}
	}
	return Operation{
		Kind:       OpLink,
		Mount:      state.Mount,
		FromParent: state.Root,
		FromName:   fromName,
		ToParent:   state.Root,
		ToName:     toName,
	}
}

func unlinkOperation(rng *rand.Rand, state *Model, names []string) Operation {
	name := names[rng.Intn(len(names))]
	if existing := state.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 80 {
		name = existing[rng.Intn(len(existing))].Name
	}
	return Operation{Kind: OpUnlink, Mount: state.Mount, Parent: state.Root, Name: name}
}

func removeOperation(rng *rand.Rand, state *Model, names []string) Operation {
	op := unlinkOperation(rng, state, names)
	op.Kind = OpRemove
	return op
}

func updateOperation(rng *rand.Rand, state *Model, names []string) Operation {
	name := names[rng.Intn(len(names))]
	inode := model.InodeID(9999)
	if existing := state.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 85 {
		record := existing[rng.Intn(len(existing))]
		name = record.Name
		inode = record.Inode
	}
	return Operation{
		Kind:   OpUpdateInode,
		Mount:  state.Mount,
		Parent: state.Root,
		Name:   name,
		Inode:  inode,
		Size:   uint64(1 + rng.Intn(8192)),
		Mode:   0o600 + uint32(rng.Intn(0o177)),
	}
}

func freeNames(state *Model, names []string) []string {
	out := make([]string, 0, len(names))
	used := make(map[string]struct{})
	for _, record := range state.ExistingDentries() {
		used[record.Name] = struct{}{}
	}
	for _, name := range names {
		if _, ok := used[name]; !ok {
			out = append(out, name)
		}
	}
	return out
}
