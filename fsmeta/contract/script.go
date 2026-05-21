// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"math/rand"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
)

// GenerateScript returns a deterministic fsmeta operation script for one seed.
// The generator plans against the same reference model used by Run so invalid
// operations are intentional API-edge coverage rather than random noise.
func GenerateScript(seed int64, steps int) []Operation {
	rng := rand.New(rand.NewSource(seed))
	model := NewModel("vol")
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	sessions := []fsmeta.SessionID{"writer-a", "writer-b", "writer-c"}
	nextInode := fsmeta.InodeID(10)
	nextSnapshotRef := 0
	syntheticSnapshotVersion := uint64(10_000)
	ops := make([]Operation, 0, steps)
	for range steps {
		op := chooseOperation(rng, model, names, sessions, &nextInode, &nextSnapshotRef)
		ops = append(ops, op)
		if op.Kind == OpSnapshotSubtree {
			_ = model.ApplySnapshot(op, fsmeta.SnapshotSubtreeToken{
				Mount:       op.Mount,
				RootInode:   op.Parent,
				ReadVersion: syntheticSnapshotVersion,
			})
			syntheticSnapshotVersion++
			continue
		}
		_ = model.Apply(op)
	}
	return ops
}

func chooseOperation(rng *rand.Rand, model *Model, names []string, sessions []fsmeta.SessionID, nextInode *fsmeta.InodeID, nextSnapshotRef *int) Operation {
	existing := model.ExistingDentries()
	files := model.ExistingFileDentries()
	liveSessions := model.ExistingSessions()
	if len(existing) < 2 && rng.Intn(100) < 60 {
		return createOperation(rng, model, names, nextInode)
	}
	switch rng.Intn(100) {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9:
		return createOperation(rng, model, names, nextInode)
	case 10, 11, 12, 13, 14, 15, 16, 17:
		return lookupOperation(rng, model, names)
	case 18, 19, 20, 21, 22, 23, 24, 25:
		return readDirPlusOperation(rng, model, names)
	case 26, 27, 28, 29, 30, 31, 32, 33:
		return renameOperation(rng, model, names)
	case 34, 35, 36, 37, 38, 39, 40:
		return linkOperation(rng, model, names)
	case 41, 42, 43, 44, 45, 46, 47:
		return unlinkOperation(rng, model, names)
	case 48, 49, 50:
		return removeOperation(rng, model, names)
	case 51, 52, 53:
		return updateOperation(rng, model, names)
	case 54, 55, 56, 57, 58:
		if len(files) == 0 {
			return createOperation(rng, model, names, nextInode)
		}
		record := files[rng.Intn(len(files))]
		return Operation{
			Kind:      OpOpenWriteSession,
			Mount:     model.Mount,
			Inode:     record.Inode,
			Session:   sessions[rng.Intn(len(sessions))],
			ExpiresNs: model.NowUnixNs + int64(1+rng.Intn(10))*int64(time.Second),
		}
	case 59, 60, 61, 62:
		if len(liveSessions) == 0 {
			return readDirPlusOperation(rng, model, names)
		}
		session := liveSessions[rng.Intn(len(liveSessions))]
		return Operation{
			Kind:      OpHeartbeatSession,
			Mount:     model.Mount,
			Inode:     session.Inode,
			Session:   session.Session,
			ExpiresNs: model.NowUnixNs + int64(10+rng.Intn(10))*int64(time.Second),
		}
	case 63, 64, 65:
		if len(liveSessions) == 0 {
			return lookupOperation(rng, model, names)
		}
		session := liveSessions[rng.Intn(len(liveSessions))]
		return Operation{Kind: OpCloseSession, Mount: model.Mount, Inode: session.Inode, Session: session.Session}
	case 66, 67:
		return Operation{
			Kind:      OpAdvanceTime,
			Mount:     model.Mount,
			AdvanceNs: int64(1+rng.Intn(6)) * int64(time.Second),
		}
	case 68, 69:
		return Operation{Kind: OpExpireSessions, Mount: model.Mount, Limit: 16}
	case 70, 71, 72:
		ref := *nextSnapshotRef
		*nextSnapshotRef++
		return Operation{Kind: OpSnapshotSubtree, Mount: model.Mount, Parent: model.Root, SnapshotRef: ref}
	default:
		return readDirPlusOperation(rng, model, names)
	}
}

func createOperation(rng *rand.Rand, model *Model, names []string, nextInode *fsmeta.InodeID) Operation {
	name := names[rng.Intn(len(names))]
	if rng.Intn(100) < 70 {
		if free := freeNames(model, names); len(free) > 0 {
			name = free[rng.Intn(len(free))]
		}
	}
	inode := *nextInode
	*nextInode++
	typ := fsmeta.InodeTypeFile
	if rng.Intn(100) < 20 {
		typ = fsmeta.InodeTypeDirectory
	}
	return Operation{
		Kind:   OpCreate,
		Mount:  model.Mount,
		Parent: model.Root,
		Name:   name,
		Inode:  inode,
		Type:   typ,
		Size:   uint64(1 + rng.Intn(4096)),
		Mode:   0o600 + uint32(rng.Intn(0o177)),
	}
}

func lookupOperation(rng *rand.Rand, model *Model, names []string) Operation {
	name := names[rng.Intn(len(names))]
	if existing := model.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 70 {
		name = existing[rng.Intn(len(existing))].Name
	}
	return Operation{Kind: OpLookup, Mount: model.Mount, Parent: model.Root, Name: name}
}

func readDirPlusOperation(rng *rand.Rand, model *Model, names []string) Operation {
	startAfter := ""
	if rng.Intn(100) < 25 {
		startAfter = names[rng.Intn(len(names))]
	}
	snapshotRef := -1
	refs := model.KnownSnapshotRefs()
	if len(refs) > 0 && rng.Intn(100) < 35 {
		snapshotRef = refs[rng.Intn(len(refs))]
	}
	return Operation{
		Kind:        OpReadDirPlus,
		Mount:       model.Mount,
		Parent:      model.Root,
		StartAfter:  startAfter,
		Limit:       uint32(1 + rng.Intn(6)),
		SnapshotRef: snapshotRef,
	}
}

func renameOperation(rng *rand.Rand, model *Model, names []string) Operation {
	fromName := names[rng.Intn(len(names))]
	if existing := model.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 80 {
		fromName = existing[rng.Intn(len(existing))].Name
	}
	toName := names[rng.Intn(len(names))]
	if rng.Intn(100) < 70 {
		if free := freeNames(model, names); len(free) > 0 {
			toName = free[rng.Intn(len(free))]
		}
	}
	return Operation{
		Kind:       OpRename,
		Mount:      model.Mount,
		FromParent: model.Root,
		FromName:   fromName,
		ToParent:   model.Root,
		ToName:     toName,
	}
}

func linkOperation(rng *rand.Rand, model *Model, names []string) Operation {
	fromName := names[rng.Intn(len(names))]
	if files := model.ExistingFileDentries(); len(files) > 0 && rng.Intn(100) < 85 {
		fromName = files[rng.Intn(len(files))].Name
	} else if existing := model.ExistingDentries(); len(existing) > 0 {
		fromName = existing[rng.Intn(len(existing))].Name
	}
	toName := names[rng.Intn(len(names))]
	if rng.Intn(100) < 75 {
		if free := freeNames(model, names); len(free) > 0 {
			toName = free[rng.Intn(len(free))]
		}
	}
	return Operation{
		Kind:       OpLink,
		Mount:      model.Mount,
		FromParent: model.Root,
		FromName:   fromName,
		ToParent:   model.Root,
		ToName:     toName,
	}
}

func unlinkOperation(rng *rand.Rand, model *Model, names []string) Operation {
	name := names[rng.Intn(len(names))]
	if existing := model.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 80 {
		name = existing[rng.Intn(len(existing))].Name
	}
	return Operation{Kind: OpUnlink, Mount: model.Mount, Parent: model.Root, Name: name}
}

func removeOperation(rng *rand.Rand, model *Model, names []string) Operation {
	op := unlinkOperation(rng, model, names)
	op.Kind = OpRemove
	return op
}

func updateOperation(rng *rand.Rand, model *Model, names []string) Operation {
	name := names[rng.Intn(len(names))]
	inode := fsmeta.InodeID(9999)
	if existing := model.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 85 {
		record := existing[rng.Intn(len(existing))]
		name = record.Name
		inode = record.Inode
	}
	return Operation{
		Kind:   OpUpdateInode,
		Mount:  model.Mount,
		Parent: model.Root,
		Name:   name,
		Inode:  inode,
		Size:   uint64(1 + rng.Intn(8192)),
		Mode:   0o600 + uint32(rng.Intn(0o177)),
	}
}

func freeNames(model *Model, names []string) []string {
	out := make([]string, 0, len(names))
	used := make(map[string]struct{})
	for _, record := range model.ExistingDentries() {
		used[record.Name] = struct{}{}
	}
	for _, name := range names {
		if _, ok := used[name]; !ok {
			out = append(out, name)
		}
	}
	return out
}
