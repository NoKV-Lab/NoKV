// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"errors"
	"fmt"
	"maps"
	"sort"

	"github.com/feichai0017/NoKV/fsmeta"
)

// OperationKind identifies one fsmeta contract operation used by the seeded
// model harness. It intentionally mirrors the public fsmeta primitive names.
type OperationKind string

const (
	OpCreate           OperationKind = "create"
	OpUpdateInode      OperationKind = "update_inode"
	OpLookup           OperationKind = "lookup"
	OpReadDirPlus      OperationKind = "readdir_plus"
	OpSnapshotSubtree  OperationKind = "snapshot_subtree"
	OpRename           OperationKind = "rename"
	OpRenameSubtree    OperationKind = "rename_subtree"
	OpLink             OperationKind = "link"
	OpUnlink           OperationKind = "unlink"
	OpRemove           OperationKind = "remove"
	OpOpenWriteSession OperationKind = "open_write_session"
	OpHeartbeatSession OperationKind = "heartbeat_write_session"
	OpCloseSession     OperationKind = "close_write_session"
	OpExpireSessions   OperationKind = "expire_write_sessions"
	OpAdvanceTime      OperationKind = "advance_time"
)

const contractMountKeyID fsmeta.MountKeyID = 1

// Operation is one generated fsmeta request. Unused fields are ignored by the
// selected Kind. For OpCreate, Inode is the model-assigned server-side inode
// used to keep later generated operations meaningful; it is not a public
// CreateRequest field.
type Operation struct {
	Kind        OperationKind
	Mount       fsmeta.MountID
	Parent      fsmeta.InodeID
	Name        string
	Inode       fsmeta.InodeID
	Type        fsmeta.InodeType
	Size        uint64
	Mode        uint32
	FromParent  fsmeta.InodeID
	FromName    string
	ToParent    fsmeta.InodeID
	ToName      string
	Session     fsmeta.SessionID
	ExpiresNs   int64
	AdvanceNs   int64
	StartAfter  string
	Limit       uint32
	SnapshotRef int
}

func (op Operation) String() string {
	return fmt.Sprintf("%s mount=%s parent=%d name=%q inode=%d from=(%d,%q) to=(%d,%q) session=%q start_after=%q limit=%d advance_ns=%d snapshot=%d",
		op.Kind, op.Mount, op.Parent, op.Name, op.Inode, op.FromParent, op.FromName, op.ToParent, op.ToName, op.Session, op.StartAfter, op.Limit, op.AdvanceNs, op.SnapshotRef)
}

// Result is the comparable response shape returned by both the reference model
// and the system under test.
type Result struct {
	Err     error
	Dentry  fsmeta.DentryRecord
	Pairs   []fsmeta.DentryAttrPair
	Token   fsmeta.SnapshotSubtreeToken
	Inode   fsmeta.InodeRecord
	Session fsmeta.SessionRecord
	Expired uint64
}

type dentryKey struct {
	parent fsmeta.InodeID
	name   string
}

type snapshotState struct {
	dentries map[dentryKey]fsmeta.DentryRecord
	inodes   map[fsmeta.InodeID]fsmeta.InodeRecord
}

type sessionIndexEntry struct {
	key     string
	record  fsmeta.SessionRecord
	session bool
}

type sessionKey struct {
	inode   fsmeta.InodeID
	session fsmeta.SessionID
}

// Model is a sequential oracle for fsmeta namespace semantics. It does not
// model raftstore, routing, or Percolator internals; it models the user-visible
// metadata contract those layers must preserve.
type Model struct {
	Mount       fsmeta.MountID
	Root        fsmeta.InodeID
	NowUnixNs   int64
	dentries    map[dentryKey]fsmeta.DentryRecord
	inodes      map[fsmeta.InodeID]fsmeta.InodeRecord
	sessions    map[sessionKey]fsmeta.SessionRecord
	owners      map[fsmeta.InodeID]fsmeta.SessionRecord
	snapshots   map[uint64]snapshotState
	snapshotRef map[int]uint64
}

// NewModel returns a model with one mounted namespace and a root directory.
func NewModel(mount fsmeta.MountID) *Model {
	m := &Model{
		Mount:       mount,
		Root:        fsmeta.RootInode,
		NowUnixNs:   1_000_000_000,
		dentries:    make(map[dentryKey]fsmeta.DentryRecord),
		inodes:      make(map[fsmeta.InodeID]fsmeta.InodeRecord),
		sessions:    make(map[sessionKey]fsmeta.SessionRecord),
		owners:      make(map[fsmeta.InodeID]fsmeta.SessionRecord),
		snapshots:   make(map[uint64]snapshotState),
		snapshotRef: make(map[int]uint64),
	}
	m.inodes[m.Root] = fsmeta.InodeRecord{
		Inode:     m.Root,
		Type:      fsmeta.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	}
	return m
}

// Apply applies one operation to the reference model. Snapshot operations must
// use ApplySnapshot with the actual token returned by the system under test.
func (m *Model) Apply(op Operation) Result {
	switch op.Kind {
	case OpCreate:
		return m.create(op)
	case OpUpdateInode:
		return m.updateInode(op)
	case OpLookup:
		return m.lookup(op)
	case OpReadDirPlus:
		return m.readDirPlus(op)
	case OpRename, OpRenameSubtree:
		return m.renameSubtree(op)
	case OpLink:
		return m.link(op)
	case OpUnlink:
		return m.unlink(op)
	case OpRemove:
		return m.unlink(op)
	case OpOpenWriteSession:
		return m.openWriteSession(op)
	case OpHeartbeatSession:
		return m.heartbeatSession(op)
	case OpCloseSession:
		return m.closeSession(op)
	case OpExpireSessions:
		return m.expireSessions(op)
	case OpAdvanceTime:
		return m.advanceTime(op)
	default:
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
}

// ApplySnapshot records the model view protected by an externally allocated
// snapshot token.
func (m *Model) ApplySnapshot(op Operation, token fsmeta.SnapshotSubtreeToken) Result {
	if op.Mount == "" || op.Mount != m.Mount || op.Parent == 0 || token.ReadVersion == 0 {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	if token.Mount != op.Mount || token.RootInode != op.Parent {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	m.snapshots[token.ReadVersion] = snapshotState{
		dentries: cloneDentries(m.dentries),
		inodes:   cloneInodes(m.inodes),
	}
	if op.SnapshotRef >= 0 {
		m.snapshotRef[op.SnapshotRef] = token.ReadVersion
	}
	return Result{Token: token}
}

func (m *Model) SnapshotVersion(ref int) uint64 {
	if ref < 0 {
		return 0
	}
	return m.snapshotRef[ref]
}

func (m *Model) ExistingDentries() []fsmeta.DentryRecord {
	out := make([]fsmeta.DentryRecord, 0, len(m.dentries))
	for _, record := range m.dentries {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Parent != out[j].Parent {
			return out[i].Parent < out[j].Parent
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (m *Model) ExistingFileDentries() []fsmeta.DentryRecord {
	all := m.ExistingDentries()
	out := all[:0]
	for _, record := range all {
		if record.Type == fsmeta.InodeTypeFile {
			out = append(out, record)
		}
	}
	return out
}

func (m *Model) ExistingSessions() []fsmeta.SessionRecord {
	out := make([]fsmeta.SessionRecord, 0, len(m.sessions))
	for _, record := range m.sessions {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Session < out[j].Session })
	return out
}

func (m *Model) KnownSnapshotRefs() []int {
	out := make([]int, 0, len(m.snapshotRef))
	for ref := range m.snapshotRef {
		out = append(out, ref)
	}
	sort.Ints(out)
	return out
}

func (m *Model) create(op Operation) Result {
	key := dentryKey{parent: op.Parent, name: op.Name}
	if _, ok := m.dentries[key]; ok {
		return Result{Err: fsmeta.ErrExists}
	}
	if _, ok := m.inodes[op.Inode]; ok {
		return Result{Err: fsmeta.ErrExists}
	}
	linkCount := uint32(1)
	inode := fsmeta.InodeRecord{
		Inode:     op.Inode,
		Type:      op.Type,
		Size:      op.Size,
		Mode:      op.Mode,
		LinkCount: linkCount,
	}
	dentry := fsmeta.DentryRecord{Parent: op.Parent, Name: op.Name, Inode: op.Inode, Type: op.Type}
	m.inodes[op.Inode] = inode
	m.dentries[key] = dentry
	return Result{Dentry: dentry, Inode: inode}
}

func (m *Model) updateInode(op Operation) Result {
	key := dentryKey{parent: op.Parent, name: op.Name}
	dentry, ok := m.dentries[key]
	if !ok {
		return Result{Err: fsmeta.ErrNotFound}
	}
	if dentry.Inode != op.Inode {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	inode, ok := m.inodes[op.Inode]
	if !ok {
		return Result{Err: fsmeta.ErrNotFound}
	}
	if dentry.Type != inode.Type {
		return Result{Err: fsmeta.ErrInvalidValue}
	}
	if inode.LinkCount != 1 {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	inode.Size = op.Size
	inode.Mode = op.Mode
	m.inodes[op.Inode] = inode
	return Result{Inode: inode}
}

func (m *Model) lookup(op Operation) Result {
	record, ok := m.dentries[dentryKey{parent: op.Parent, name: op.Name}]
	if !ok {
		return Result{Err: fsmeta.ErrNotFound}
	}
	return Result{Dentry: record}
}

func (m *Model) readDirPlus(op Operation) Result {
	state := snapshotState{dentries: m.dentries, inodes: m.inodes}
	if version := m.SnapshotVersion(op.SnapshotRef); version != 0 {
		snapshot, ok := m.snapshots[version]
		if !ok {
			return Result{Err: fsmeta.ErrInvalidRequest}
		}
		state = snapshot
	}
	limit := op.Limit
	if limit == 0 {
		limit = fsmeta.DefaultReadDirLimit
	}
	names := make([]string, 0)
	for key := range state.dentries {
		if key.parent == op.Parent && key.name > op.StartAfter {
			names = append(names, key.name)
		}
	}
	sort.Strings(names)
	if uint32(len(names)) > limit {
		names = names[:limit]
	}
	pairs := make([]fsmeta.DentryAttrPair, 0, len(names))
	for _, name := range names {
		dentry := state.dentries[dentryKey{parent: op.Parent, name: name}]
		inode, ok := state.inodes[dentry.Inode]
		if !ok {
			return Result{Err: fsmeta.ErrNotFound}
		}
		pairs = append(pairs, fsmeta.DentryAttrPair{Dentry: dentry, Inode: inode})
	}
	return Result{Pairs: pairs}
}

func (m *Model) renameSubtree(op Operation) Result {
	if op.FromParent == op.ToParent && op.FromName == op.ToName {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	from := dentryKey{parent: op.FromParent, name: op.FromName}
	to := dentryKey{parent: op.ToParent, name: op.ToName}
	record, ok := m.dentries[from]
	if !ok {
		return Result{Err: fsmeta.ErrNotFound}
	}
	if _, ok := m.dentries[to]; ok {
		return Result{Err: fsmeta.ErrExists}
	}
	delete(m.dentries, from)
	record.Parent = op.ToParent
	record.Name = op.ToName
	m.dentries[to] = record
	return Result{}
}

func (m *Model) link(op Operation) Result {
	if op.FromParent == op.ToParent && op.FromName == op.ToName {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	from := dentryKey{parent: op.FromParent, name: op.FromName}
	to := dentryKey{parent: op.ToParent, name: op.ToName}
	record, ok := m.dentries[from]
	if !ok {
		return Result{Err: fsmeta.ErrNotFound}
	}
	if record.Type == fsmeta.InodeTypeDirectory {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	if _, ok := m.dentries[to]; ok {
		return Result{Err: fsmeta.ErrExists}
	}
	inode, ok := m.inodes[record.Inode]
	if !ok {
		return Result{Err: fsmeta.ErrNotFound}
	}
	if inode.Type == fsmeta.InodeTypeDirectory || inode.LinkCount == ^uint32(0) {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	if inode.LinkCount == 0 {
		inode.LinkCount = 1
	}
	inode.LinkCount++
	m.inodes[inode.Inode] = inode
	m.dentries[to] = fsmeta.DentryRecord{Parent: op.ToParent, Name: op.ToName, Inode: record.Inode, Type: record.Type}
	return Result{}
}

func (m *Model) unlink(op Operation) Result {
	key := dentryKey{parent: op.Parent, name: op.Name}
	record, ok := m.dentries[key]
	if !ok {
		return Result{Err: fsmeta.ErrNotFound}
	}
	if record.Type == fsmeta.InodeTypeDirectory {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	if inode, ok := m.inodes[record.Inode]; ok {
		if inode.Type == fsmeta.InodeTypeDirectory {
			return Result{Err: fsmeta.ErrInvalidRequest}
		}
		delete(m.dentries, key)
		if inode.LinkCount <= 1 {
			delete(m.inodes, inode.Inode)
		} else {
			inode.LinkCount--
			m.inodes[inode.Inode] = inode
		}
	} else {
		delete(m.dentries, key)
	}
	return Result{}
}

func (m *Model) openWriteSession(op Operation) Result {
	if op.ExpiresNs <= m.NowUnixNs {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	inode, ok := m.inodes[op.Inode]
	if !ok {
		return Result{Err: fsmeta.ErrNotFound}
	}
	if inode.Type != fsmeta.InodeTypeFile {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	key := sessionKey{inode: op.Inode, session: op.Session}
	if existing, ok := m.sessions[key]; ok && sessionLive(existing, m.NowUnixNs) {
		return Result{Err: fsmeta.ErrExists}
	}
	if owner, ok := m.owners[op.Inode]; ok {
		if sessionLive(owner, m.NowUnixNs) {
			return Result{Err: fsmeta.ErrExists}
		}
		ownerKey := sessionKey{inode: owner.Inode, session: owner.Session}
		if current, ok := m.sessions[ownerKey]; ok && current == owner {
			delete(m.sessions, ownerKey)
		}
	}
	record := fsmeta.SessionRecord{Session: op.Session, Inode: op.Inode, ExpiresUnixNs: op.ExpiresNs}
	m.sessions[key] = record
	m.owners[op.Inode] = record
	return Result{Session: record}
}

func (m *Model) heartbeatSession(op Operation) Result {
	if op.ExpiresNs <= m.NowUnixNs {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	key := sessionKey{inode: op.Inode, session: op.Session}
	session, ok := m.sessions[key]
	if !ok || !sessionLive(session, m.NowUnixNs) || session.Inode != op.Inode {
		return Result{Err: fsmeta.ErrNotFound}
	}
	owner, ok := m.owners[op.Inode]
	if !ok || !sessionLive(owner, m.NowUnixNs) || owner.Session != op.Session || owner.Inode != op.Inode {
		return Result{Err: fsmeta.ErrNotFound}
	}
	record := fsmeta.SessionRecord{Session: op.Session, Inode: op.Inode, ExpiresUnixNs: op.ExpiresNs}
	m.sessions[key] = record
	m.owners[op.Inode] = record
	return Result{Session: record}
}

func (m *Model) closeSession(op Operation) Result {
	key := sessionKey{inode: op.Inode, session: op.Session}
	session, ok := m.sessions[key]
	if !ok {
		return Result{Err: fsmeta.ErrNotFound}
	}
	if session.Inode != op.Inode {
		return Result{Err: fsmeta.ErrNotFound}
	}
	delete(m.sessions, key)
	if owner, ok := m.owners[session.Inode]; ok && owner.Session == op.Session && owner.Inode == session.Inode {
		delete(m.owners, session.Inode)
	}
	return Result{}
}

func (m *Model) expireSessions(op Operation) Result {
	limit := op.Limit
	if limit == 0 {
		limit = fsmeta.DefaultSessionExpireLimit
	}
	identity := fsmeta.MountIdentity{MountID: m.Mount, MountKeyID: contractMountKeyID}
	entries := make([]sessionIndexEntry, 0, len(m.sessions)+len(m.owners))
	for _, record := range m.sessions {
		key, err := fsmeta.EncodeSessionKey(identity, record.Inode, record.Session)
		if err != nil {
			return Result{Err: err}
		}
		entries = append(entries, sessionIndexEntry{key: string(key), record: record, session: true})
	}
	for _, record := range m.owners {
		key, err := fsmeta.EncodeInodeSessionKey(identity, record.Inode)
		if err != nil {
			return Result{Err: err}
		}
		entries = append(entries, sessionIndexEntry{key: string(key), record: record})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	deleteSessions := make(map[sessionKey]struct{})
	deleteOwners := make(map[fsmeta.InodeID]struct{})
	expiredSessions := make(map[sessionKey]struct{})
	visited := uint32(0)
	for _, entry := range entries {
		if visited >= limit {
			break
		}
		visited++
		if sessionLive(entry.record, m.NowUnixNs) {
			continue
		}
		key := sessionKey{inode: entry.record.Inode, session: entry.record.Session}
		if entry.session {
			deleteSessions[key] = struct{}{}
		} else {
			deleteOwners[entry.record.Inode] = struct{}{}
		}
		if current, ok := m.sessions[key]; ok && current == entry.record {
			deleteSessions[key] = struct{}{}
			expiredSessions[key] = struct{}{}
		}
		if owner, ok := m.owners[entry.record.Inode]; ok && owner == entry.record {
			deleteOwners[entry.record.Inode] = struct{}{}
		}
	}
	for key := range deleteSessions {
		delete(m.sessions, key)
	}
	for inode := range deleteOwners {
		delete(m.owners, inode)
	}
	return Result{Expired: uint64(len(expiredSessions))}
}

func (m *Model) advanceTime(op Operation) Result {
	if op.AdvanceNs <= 0 {
		return Result{Err: fsmeta.ErrInvalidRequest}
	}
	m.NowUnixNs += op.AdvanceNs
	return Result{}
}

func sessionLive(record fsmeta.SessionRecord, nowUnixNs int64) bool {
	return record.ExpiresUnixNs > nowUnixNs
}

// CheckInvariants validates global namespace consistency after every generated
// operation. It catches model bugs and expected-state corruption before the
// model is used as an oracle for later steps.
func (m *Model) CheckInvariants() error {
	refs := make(map[fsmeta.InodeID]uint32)
	for key, dentry := range m.dentries {
		if key.parent == 0 || key.name == "" {
			return fmt.Errorf("invalid dentry key: %+v", key)
		}
		if dentry.Parent != key.parent || dentry.Name != key.name {
			return fmt.Errorf("dentry/key mismatch key=%+v record=%+v", key, dentry)
		}
		inode, ok := m.inodes[dentry.Inode]
		if !ok {
			return fmt.Errorf("%w: dentry %s points to inode %d", fsmeta.ErrNotFound, dentry.Name, dentry.Inode)
		}
		if inode.Inode != dentry.Inode {
			return fmt.Errorf("inode key/value mismatch key=%d record=%+v", dentry.Inode, inode)
		}
		if inode.Type != dentry.Type {
			return fmt.Errorf("%w: dentry type=%s inode type=%s", fsmeta.ErrInvalidValue, dentry.Type, inode.Type)
		}
		refs[dentry.Inode]++
	}
	for inodeID, inode := range m.inodes {
		if inodeID == m.Root {
			continue
		}
		if refs[inodeID] == 0 {
			return fmt.Errorf("inode %d has no dentry references", inodeID)
		}
		if refs[inodeID] != inode.LinkCount {
			return fmt.Errorf("inode %d link_count=%d refs=%d", inodeID, inode.LinkCount, refs[inodeID])
		}
	}
	for key, session := range m.sessions {
		if session.Session != key.session || session.Inode != key.inode {
			return fmt.Errorf("session key/value mismatch key=%+v record=%+v", key, session)
		}
		if !sessionLive(session, m.NowUnixNs) {
			continue
		}
		owner, ok := m.owners[session.Inode]
		if !ok || owner.Session != session.Session {
			return fmt.Errorf("session %s missing owner for inode %d", session.Session, session.Inode)
		}
	}
	for inodeID, owner := range m.owners {
		if owner.Inode != inodeID {
			return fmt.Errorf("owner key/value mismatch key=%d record=%+v", inodeID, owner)
		}
		if !sessionLive(owner, m.NowUnixNs) {
			continue
		}
		session, ok := m.sessions[sessionKey{inode: owner.Inode, session: owner.Session}]
		if !ok || session.Inode != inodeID {
			return fmt.Errorf("owner for inode %d missing session %s", inodeID, owner.Session)
		}
	}
	return nil
}

func EquivalentError(got, want error) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	for _, sentinel := range []error{
		fsmeta.ErrInvalidMountID,
		fsmeta.ErrInvalidInodeID,
		fsmeta.ErrInvalidName,
		fsmeta.ErrInvalidSession,
		fsmeta.ErrInvalidRequest,
		fsmeta.ErrInvalidKey,
		fsmeta.ErrInvalidKeyKind,
		fsmeta.ErrInvalidValue,
		fsmeta.ErrInvalidValueKind,
		fsmeta.ErrInvalidPageSize,
		fsmeta.ErrExists,
		fsmeta.ErrNotFound,
		fsmeta.ErrMountNotRegistered,
		fsmeta.ErrMountRetired,
		fsmeta.ErrQuotaExceeded,
	} {
		if errors.Is(got, sentinel) || errors.Is(want, sentinel) {
			return errors.Is(got, sentinel) && errors.Is(want, sentinel)
		}
	}
	// Neither side is a registered NoKV sentinel: fall back to comparing
	// the rendered messages. We extract to locals so it is obvious the
	// comparison is the opaque-error fallback, not a sentinel shortcut.
	gotMessage := got.Error()
	wantMessage := want.Error()
	return gotMessage == wantMessage
}

func cloneDentries(in map[dentryKey]fsmeta.DentryRecord) map[dentryKey]fsmeta.DentryRecord {
	out := make(map[dentryKey]fsmeta.DentryRecord, len(in))
	maps.Copy(out, in)
	return out
}

func cloneInodes(in map[fsmeta.InodeID]fsmeta.InodeRecord) map[fsmeta.InodeID]fsmeta.InodeRecord {
	out := make(map[fsmeta.InodeID]fsmeta.InodeRecord, len(in))
	for key, value := range in {
		value.OpaqueAttrs = append([]byte(nil), value.OpaqueAttrs...)
		out[key] = value
	}
	return out
}
