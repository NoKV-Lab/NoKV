// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package specs

import "github.com/feichai0017/NoKV/fsmeta/exec/compile/specdsl"

var Create = specdsl.OpSpec{
	Name:           "Create",
	FileName:       "create.program.go",
	ProgramType:    "CreateProgram",
	ValuesType:     "CreateValues",
	RequestType:    "fsmeta.CreateRequest",
	CompileName:    "CompileCreateProgram",
	PlanName:       "fsmeta.PlanCreate",
	Materialize:    "MaterializeCreate",
	OperationKind:  "fsmeta.OperationCreate",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 3,
	EffectCount:    3,
	Emitter:        "create",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}, Inodes: []string{"inode"}},
	SlowFallbacks:  []string{"SlowReasonSharedQuota"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "parent_inode_observed", Kind: "PredicateObservedValue", Key: "read[0]"},
		{Name: "dentry_absent", Kind: "PredicateNotExists", Key: "mutate[1]"},
		{Name: "inode_absent", Kind: "PredicateNotExists", Key: "mutate[2]"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "parent_inode", Kind: "EffectDerivedPut", Key: "mutate[0]", ValueName: "ParentInodeValue"},
		{Name: "dentry", Kind: "EffectPut", Key: "mutate[1]", ValueName: "DentryValue"},
		{Name: "inode", Kind: "EffectPut", Key: "mutate[2]", ValueName: "InodeValue"},
	},
	OptionalGuards: []specdsl.GuardSpec{
		{Name: "quota_credit", Guard: "GuardQuotaCredit", Condition: "quota_escrow"},
	},
}

var UpdateInode = specdsl.OpSpec{
	Name:           "UpdateInode",
	FileName:       "inode.program.go",
	ProgramType:    "UpdateInodeProgram",
	RequestType:    "fsmeta.UpdateInodeRequest",
	CompileName:    "CompileUpdateInodeProgram",
	PlanName:       "fsmeta.PlanUpdateInode",
	OperationKind:  "fsmeta.OperationUpdateInode",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 2,
	EffectCount:    1,
	HasOptions:     true,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}, Inodes: []string{"inode"}},
	SlowFallbacks:  []string{"SlowReasonSharedQuota"},
	RequestChecks:  []string{"inode_update_has_mutation"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_observed", Kind: "PredicateObservedValue", Key: "read[0]"},
		{Name: "inode_observed", Kind: "PredicateObservedValue", Key: "read[1]"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "inode", Kind: "EffectDerivedPut", Key: "mutate[0]", ValueSource: "runtime"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "single_link_inode", Guard: "GuardSingleLinkInode"},
	},
	OptionalGuards: []specdsl.GuardSpec{
		{Name: "quota_credit", Guard: "GuardQuotaCredit", Condition: "quota_escrow"},
	},
}

var Lookup = specdsl.OpSpec{
	Name:           "Lookup",
	FileName:       "lookup.program.go",
	ProgramType:    "LookupProgram",
	RequestType:    "fsmeta.LookupRequest",
	CompileName:    "CompileLookupProgram",
	PlanName:       "fsmeta.PlanLookup",
	OperationKind:  "fsmeta.OperationLookup",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilitySlowPath",
	SlowReason:     "SlowReasonReadOnly",
	PredicateCount: 1,
	EffectCount:    0,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_exists", Kind: "PredicateExists", Key: "primary"},
	},
}

var ReadDir = specdsl.OpSpec{
	Name:           "ReadDir",
	FileName:       "readdir.program.go",
	ProgramType:    "ReadDirProgram",
	RequestType:    "fsmeta.ReadDirRequest",
	CompileName:    "CompileReadDirProgram",
	PlanName:       "fsmeta.PlanReadDir",
	OperationKind:  "fsmeta.OperationReadDir",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilitySlowPath",
	SlowReason:     "SlowReasonRangeRead",
	PredicateCount: 1,
	EffectCount:    0,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_prefix", Kind: "PredicatePrefixScan", Key: "read_prefix[0]"},
	},
}

var SnapshotSubtree = specdsl.OpSpec{
	Name:              "SnapshotSubtree",
	FileName:          "snapshot.program.go",
	ProgramType:       "SnapshotSubtreeProgram",
	RequestType:       "fsmeta.SnapshotSubtreeRequest",
	CompileName:       "CompileSnapshotSubtreeProgram",
	PlanName:          "fsmeta.PlanSnapshotSubtree",
	OperationKind:     "fsmeta.OperationSnapshotSubtree",
	Durability:        "DurabilityNeedsPublishCheckpoint",
	Eligibility:       "EligibilitySlowPath",
	SlowReason:        "SlowReasonDurabilityBarrier",
	DurabilityBarrier: true,
	PredicateCount:    1,
	EffectCount:       0,
	Emitter:           "operation",
	Authority:         specdsl.AuthoritySpec{Parents: []string{"root"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "subtree_prefix", Kind: "PredicatePrefixScan", Key: "read_prefix[0]"},
	},
}

var Rename = specdsl.OpSpec{
	Name:           "Rename",
	FileName:       "rename.program.go",
	ProgramType:    "RenameProgram",
	RequestType:    "fsmeta.RenameRequest",
	CompileName:    "CompileRenameProgram",
	PlanName:       "fsmeta.PlanRename",
	OperationKind:  "fsmeta.OperationRename",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: -1,
	EffectCount:    4,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"from_parent", "to_parent"}},
	SlowFallbacks:  []string{"SlowReasonCrossBucket"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "from_dentry_exists", Kind: "PredicateExists", Key: "read[0]"},
		{Name: "to_dentry_absent", Kind: "PredicateNotExists", Key: "read[1]"},
		{Name: "from_parent_inode_observed", Kind: "PredicateObservedValue", Key: "read[2]"},
		{Name: "to_parent_inode_observed", Kind: "PredicateObservedValue", Key: "read[3]"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "from_dentry", Kind: "EffectDelete", Key: "mutate[0]"},
		{Name: "to_dentry", Kind: "EffectDerivedPut", Key: "mutate[1]", ValueSource: "runtime"},
		{Name: "from_parent_inode", Kind: "EffectDerivedPut", Key: "mutate[2]", ValueSource: "runtime"},
		{Name: "to_parent_inode", Kind: "EffectDerivedPut", Key: "mutate[3]", ValueSource: "runtime"},
	},
}

var RenameSubtree = specdsl.OpSpec{
	Name:              "RenameSubtree",
	FileName:          "rename.program.go",
	ProgramType:       "RenameSubtreeProgram",
	RequestType:       "fsmeta.RenameSubtreeRequest",
	CompileName:       "CompileRenameSubtreeProgram",
	PlanName:          "fsmeta.PlanRenameSubtree",
	OperationKind:     "fsmeta.OperationRenameSubtree",
	Durability:        "DurabilityNeedsPublishCheckpoint",
	Eligibility:       "EligibilitySlowPath",
	SlowReason:        "SlowReasonDurabilityBarrier",
	DurabilityBarrier: true,
	WatchAtSeal:       true,
	PredicateCount:    4,
	EffectCount:       4,
	Emitter:           "operation",
	Authority:         specdsl.AuthoritySpec{Parents: []string{"from_parent", "to_parent"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "from_dentry_exists", Kind: "PredicateExists", Key: "read[0]"},
		{Name: "to_dentry_absent", Kind: "PredicateNotExists", Key: "read[1]"},
		{Name: "from_parent_inode_observed", Kind: "PredicateObservedValue", Key: "read[2]"},
		{Name: "to_parent_inode_observed", Kind: "PredicateObservedValue", Key: "read[3]"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "from_dentry", Kind: "EffectDelete", Key: "mutate[0]"},
		{Name: "to_dentry", Kind: "EffectDerivedPut", Key: "mutate[1]", ValueSource: "runtime"},
		{Name: "from_parent_inode", Kind: "EffectDerivedPut", Key: "mutate[2]", ValueSource: "runtime"},
		{Name: "to_parent_inode", Kind: "EffectDerivedPut", Key: "mutate[3]", ValueSource: "runtime"},
	},
}

var Link = specdsl.OpSpec{
	Name:           "Link",
	FileName:       "link.program.go",
	ProgramType:    "LinkProgram",
	RequestType:    "fsmeta.LinkRequest",
	CompileName:    "CompileLinkProgram",
	PlanName:       "fsmeta.PlanLink",
	OperationKind:  "fsmeta.OperationLink",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: -1,
	EffectCount:    3,
	HasOptions:     true,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"from_parent", "to_parent"}},
	SlowFallbacks:  []string{"SlowReasonSharedQuota"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "from_dentry_observed", Kind: "PredicateObservedValue", Key: "read[0]"},
		{Name: "to_dentry_absent", Kind: "PredicateNotExists", Key: "read[1]"},
		{Name: "to_parent_inode_observed", Kind: "PredicateObservedValue", Key: "read[2]"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "to_dentry", Kind: "EffectDerivedPut", Key: "mutate[0]", ValueSource: "runtime"},
		{Name: "from_inode", Kind: "EffectDerivedPut", Key: "runtime", ValueSource: "runtime"},
		{Name: "to_parent_inode", Kind: "EffectDerivedPut", Key: "mutate[1]", ValueSource: "runtime"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "non_directory_inode", Guard: "GuardNonDirectoryInode"},
		{Name: "same_authority", Guard: "GuardSameAuthority"},
	},
	OptionalGuards: []specdsl.GuardSpec{
		{Name: "quota_credit", Guard: "GuardQuotaCredit", Condition: "quota_escrow"},
	},
}

var Unlink = specdsl.OpSpec{
	Name:           "Unlink",
	FileName:       "unlink.program.go",
	ProgramType:    "UnlinkProgram",
	RequestType:    "fsmeta.UnlinkRequest",
	CompileName:    "CompileUnlinkProgram",
	PlanName:       "fsmeta.PlanUnlink",
	OperationKind:  "fsmeta.OperationUnlink",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: -1,
	EffectCount:    3,
	HasOptions:     true,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}},
	SlowFallbacks:  []string{"SlowReasonSharedQuota"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_observed", Kind: "PredicateObservedValue", Key: "primary"},
		{Name: "parent_inode_observed", Kind: "PredicateObservedValue", Key: "read[1]"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "dentry", Kind: "EffectDelete", Key: "mutate[0]"},
		{Name: "inode", Kind: "EffectDerivedPut", Key: "runtime", ValueSource: "runtime"},
		{Name: "parent_inode", Kind: "EffectDerivedPut", Key: "mutate[1]", ValueSource: "runtime"},
	},
	OptionalGuards: []specdsl.GuardSpec{
		{Name: "quota_credit", Guard: "GuardQuotaCredit", Condition: "quota_escrow"},
	},
}

var Remove = specdsl.OpSpec{
	Name:           "Remove",
	FileName:       "remove.program.go",
	ProgramType:    "RemoveProgram",
	RequestType:    "fsmeta.RemoveRequest",
	CompileName:    "CompileRemoveProgram",
	PlanName:       "fsmeta.PlanRemove",
	OperationKind:  "fsmeta.OperationRemove",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: -1,
	EffectCount:    3,
	HasOptions:     true,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}},
	SlowFallbacks:  []string{"SlowReasonSharedQuota"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_observed", Kind: "PredicateObservedValue", Key: "primary"},
		{Name: "parent_inode_observed", Kind: "PredicateObservedValue", Key: "read[1]"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "dentry", Kind: "EffectDelete", Key: "mutate[0]"},
		{Name: "inode", Kind: "EffectDerivedPut", Key: "runtime", ValueSource: "runtime"},
		{Name: "parent_inode", Kind: "EffectDerivedPut", Key: "mutate[1]", ValueSource: "runtime"},
	},
	OptionalGuards: []specdsl.GuardSpec{
		{Name: "quota_credit", Guard: "GuardQuotaCredit", Condition: "quota_escrow"},
	},
}

var RemoveDirectory = specdsl.OpSpec{
	Name:           "RemoveDirectory",
	FileName:       "remove_directory.program.go",
	ProgramType:    "RemoveDirectoryProgram",
	RequestType:    "fsmeta.RemoveDirectoryRequest",
	CompileName:    "CompileRemoveDirectoryProgram",
	PlanName:       "fsmeta.PlanRemoveDirectory",
	OperationKind:  "fsmeta.OperationRemoveDirectory",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: -1,
	EffectCount:    3,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "parent_inode_observed", Kind: "PredicateObservedValue", Key: "read[0]"},
		{Name: "dentry_observed", Kind: "PredicateObservedValue", Key: "primary"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "parent_inode", Kind: "EffectDerivedPut", Key: "mutate[0]", ValueSource: "runtime"},
		{Name: "dentry", Kind: "EffectDelete", Key: "mutate[1]"},
		{Name: "inode", Kind: "EffectDerivedDelete", Key: "runtime", ValueSource: "runtime"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "empty_directory_inode", Guard: "GuardEmptyDirectory"},
	},
}

var OpenWriteSession = specdsl.OpSpec{
	Name:           "OpenWriteSession",
	FileName:       "session.program.go",
	ProgramType:    "OpenWriteSessionProgram",
	ValuesType:     "OpenWriteSessionValues",
	RequestType:    "fsmeta.OpenWriteSessionRequest",
	CompileName:    "CompileOpenWriteSessionProgram",
	PlanName:       "fsmeta.PlanOpenWriteSession",
	Materialize:    "MaterializeOpenWriteSession",
	OperationKind:  "fsmeta.OperationOpenWriteSession",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 3,
	EffectCount:    2,
	Emitter:        "operation",
	Materializer:   "session_put",
	Authority:      specdsl.AuthoritySpec{Inodes: []string{"inode"}},
	RequestChecks:  []string{"positive_ttl"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "inode_observed", Kind: "PredicateObservedValue", Key: "read[0]"},
		{Name: "session_observed", Kind: "PredicateObservedValue", Key: "read[1]"},
		{Name: "owner_observed", Kind: "PredicateObservedValue", Key: "read[2]"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "session", Kind: "EffectDerivedPut", Key: "mutate[0]", ValueSource: "runtime"},
		{Name: "owner", Kind: "EffectDerivedPut", Key: "mutate[1]", ValueSource: "runtime"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "non_directory_inode", Guard: "GuardNonDirectoryInode"},
		{Name: "expired_session_owner", Guard: "GuardExpiredSessionOwner"},
	},
}

var HeartbeatWriteSession = specdsl.OpSpec{
	Name:           "HeartbeatWriteSession",
	FileName:       "session.program.go",
	ProgramType:    "HeartbeatWriteSessionProgram",
	ValuesType:     "HeartbeatWriteSessionValues",
	RequestType:    "fsmeta.HeartbeatWriteSessionRequest",
	CompileName:    "CompileHeartbeatWriteSessionProgram",
	PlanName:       "fsmeta.PlanHeartbeatWriteSession",
	Materialize:    "MaterializeHeartbeatWriteSession",
	OperationKind:  "fsmeta.OperationHeartbeatSession",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 2,
	EffectCount:    2,
	Emitter:        "operation",
	Materializer:   "session_put",
	Authority:      specdsl.AuthoritySpec{Inodes: []string{"inode"}},
	RequestChecks:  []string{"positive_ttl"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "session_observed", Kind: "PredicateObservedValue", Key: "read[0]"},
		{Name: "owner_observed", Kind: "PredicateObservedValue", Key: "read[1]"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "session", Kind: "EffectDerivedPut", Key: "mutate[0]", ValueSource: "runtime"},
		{Name: "owner", Kind: "EffectDerivedPut", Key: "mutate[1]", ValueSource: "runtime"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "live_session", Guard: "GuardLiveSession"},
	},
}

var CloseWriteSession = specdsl.OpSpec{
	Name:           "CloseWriteSession",
	FileName:       "session.program.go",
	ProgramType:    "CloseWriteSessionProgram",
	ValuesType:     "CloseWriteSessionValues",
	RequestType:    "fsmeta.CloseWriteSessionRequest",
	CompileName:    "CompileCloseWriteSessionProgram",
	PlanName:       "fsmeta.PlanCloseWriteSession",
	Materialize:    "MaterializeCloseWriteSession",
	OperationKind:  "fsmeta.OperationCloseSession",
	Durability:     "DurabilityNeedsCloseSession",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 2,
	EffectCount:    -1,
	Emitter:        "operation",
	Materializer:   "session_close",
	Authority:      specdsl.AuthoritySpec{Inodes: []string{"inode"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "session_observed", Kind: "PredicateObservedValue", Key: "read[0]"},
		{Name: "owner_observed", Kind: "PredicateObservedValue", Key: "owner"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "session", Kind: "EffectDelete", Key: "mutate[0]"},
		{Name: "owner", Kind: "EffectDerivedDelete", Key: "owner", ValueSource: "runtime"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "live_session", Guard: "GuardLiveSession"},
	},
}

var ExpireWriteSessions = specdsl.OpSpec{
	Name:           "ExpireWriteSessions",
	FileName:       "session.program.go",
	ProgramType:    "ExpireWriteSessionsProgram",
	RequestType:    "fsmeta.ExpireWriteSessionsRequest",
	CompileName:    "CompileExpireWriteSessionsProgram",
	PlanName:       "fsmeta.PlanExpireWriteSessions",
	OperationKind:  "fsmeta.OperationExpireSessions",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilitySlowPath",
	SlowReason:     "SlowReasonMaintenanceScan",
	PredicateCount: -1,
	EffectCount:    0,
	Emitter:        "operation",
	Predicates: []specdsl.PredicateSpec{
		{Name: "session_prefix", Kind: "PredicatePrefixScan", Key: "read_prefix[0]", Repeatable: true},
	},
}

func All() []specdsl.OpSpec {
	return []specdsl.OpSpec{
		Create,
		UpdateInode,
		Lookup,
		ReadDir,
		SnapshotSubtree,
		Rename,
		RenameSubtree,
		Link,
		Unlink,
		Remove,
		RemoveDirectory,
		OpenWriteSession,
		HeartbeatWriteSession,
		CloseWriteSession,
		ExpireWriteSessions,
	}
}

func ReadAll() []specdsl.ReadSpec {
	return []specdsl.ReadSpec{
		{Name: "Lookup", OperationKind: "fsmeta.OperationLookup", KeyShape: "dentry", Authority: "parent", Source: "point"},
		{Name: "GetAttr", OperationKind: "fsmeta.OperationGetAttr", KeyShape: "inode", Authority: "inode", Source: "point"},
		{Name: "ReadSession", OperationKind: "fsmeta.OperationReadSession", KeyShape: "session", Authority: "inode", Source: "point"},
		{Name: "ReadSessionOwner", OperationKind: "fsmeta.OperationReadSession", KeyShape: "session_owner", Authority: "inode", Source: "point"},
		{Name: "ReadSessionKey", OperationKind: "fsmeta.OperationReadSession", KeyShape: "session_key", Authority: "session_key", Source: "point"},
		{Name: "ReadDir", OperationKind: "fsmeta.OperationReadDir", KeyShape: "dentry_prefix", Authority: "parent", Source: "directory"},
	}
}
