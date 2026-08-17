/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Storage-neutral keyspace catalog for the workspace metadata schema.

use nokv_meta_store::Keyspace;

/// One schema-owned keyspace and its stable logical name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyspaceDef {
    pub id: Keyspace,
    pub name: &'static str,
}

impl KeyspaceDef {
    const fn new(id: u16, name: &'static str) -> Self {
        Self {
            id: Keyspace::new(id),
            name,
        }
    }
}

pub(super) const SYSTEM: KeyspaceDef = KeyspaceDef::new(0x0101, "system");
pub(super) const ROOT_FENCE: KeyspaceDef = KeyspaceDef::new(0x0102, "root_fence");
pub(super) const COMMAND_DEDUPE: KeyspaceDef = KeyspaceDef::new(0x0103, "command_dedupe");
pub(super) const CHANGE_EVENT: KeyspaceDef = KeyspaceDef::new(0x0104, "change_event");
pub(super) const HISTORY: KeyspaceDef = KeyspaceDef::new(0x0105, "history");
pub(super) const RECOVERY_OUTBOX: KeyspaceDef = KeyspaceDef::new(0x0106, "recovery_outbox");

/// Durable metadata families that domain commands may mutate.
///
/// MetaShard-owned system, root-fence, dedupe, change-event, history, and
/// recovery-outbox keyspaces are deliberately absent. The `u8` discriminants
/// are durable recovery and history format tags. They are not physical
/// keyspace identifiers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum MetadataFamily {
    WorkspaceCurrent = 0x02,
    PathCurrent = 0x03,
    ArtifactRevision = 0x04,
    ArtifactManifest = 0x05,
    RevisionRef = 0x06,
    Commit = 0x07,
    CommitMember = 0x08,
    WorkbenchCommitHead = 0x09,
    Tag = 0x0a,
    SnapshotRef = 0x0b,
    SnapshotAlias = 0x0c,
    HistoryHold = 0x0d,
    CommitConsumer = 0x0e,
    SecondaryIndex = 0x0f,
    Operation = 0x11,
    RestoreMember = 0x12,
    StagedObject = 0x13,
    GcCandidate = 0x15,
    GcBarrier = 0x16,
    WorkspaceIncarnationClaim = 0x17,
    GenericIndexCurrent = 0x19,
    GenericIndexGeneration = 0x1a,
    CommitGenericIndexMember = 0x1b,
}

impl MetadataFamily {
    pub const ALL: [Self; 23] = [
        Self::WorkspaceCurrent,
        Self::PathCurrent,
        Self::ArtifactRevision,
        Self::ArtifactManifest,
        Self::RevisionRef,
        Self::Commit,
        Self::CommitMember,
        Self::WorkbenchCommitHead,
        Self::Tag,
        Self::SnapshotRef,
        Self::SnapshotAlias,
        Self::HistoryHold,
        Self::CommitConsumer,
        Self::SecondaryIndex,
        Self::Operation,
        Self::RestoreMember,
        Self::StagedObject,
        Self::GcCandidate,
        Self::GcBarrier,
        Self::WorkspaceIncarnationClaim,
        Self::GenericIndexCurrent,
        Self::GenericIndexGeneration,
        Self::CommitGenericIndexMember,
    ];

    /// Storage-neutral keyspace used by transaction-store requests.
    pub const fn keyspace(self) -> Keyspace {
        Keyspace::new(0x0200 | self.format_tag() as u16)
    }

    /// Durable `u8` tag used by recovery and history encodings.
    pub const fn format_tag(self) -> u8 {
        self as u8
    }

    /// Decode one durable recovery or history format tag.
    pub const fn from_format_tag(value: u8) -> Option<Self> {
        match value {
            0x02 => Some(Self::WorkspaceCurrent),
            0x03 => Some(Self::PathCurrent),
            0x04 => Some(Self::ArtifactRevision),
            0x05 => Some(Self::ArtifactManifest),
            0x06 => Some(Self::RevisionRef),
            0x07 => Some(Self::Commit),
            0x08 => Some(Self::CommitMember),
            0x09 => Some(Self::WorkbenchCommitHead),
            0x0a => Some(Self::Tag),
            0x0b => Some(Self::SnapshotRef),
            0x0c => Some(Self::SnapshotAlias),
            0x0d => Some(Self::HistoryHold),
            0x0e => Some(Self::CommitConsumer),
            0x0f => Some(Self::SecondaryIndex),
            0x11 => Some(Self::Operation),
            0x12 => Some(Self::RestoreMember),
            0x13 => Some(Self::StagedObject),
            0x15 => Some(Self::GcCandidate),
            0x16 => Some(Self::GcBarrier),
            0x17 => Some(Self::WorkspaceIncarnationClaim),
            0x19 => Some(Self::GenericIndexCurrent),
            0x1a => Some(Self::GenericIndexGeneration),
            0x1b => Some(Self::CommitGenericIndexMember),
            _ => None,
        }
    }

    /// Stable physical name used by store adapters.
    pub const fn name(self) -> &'static str {
        match self {
            Self::WorkspaceCurrent => "workspace_current",
            Self::PathCurrent => "path_current",
            Self::ArtifactRevision => "artifact_revision",
            Self::ArtifactManifest => "artifact_manifest",
            Self::RevisionRef => "revision_ref",
            Self::Commit => "commit",
            Self::CommitMember => "commit_member",
            Self::WorkbenchCommitHead => "workbench_commit_head",
            Self::Tag => "tag",
            Self::SnapshotRef => "snapshot_ref",
            Self::SnapshotAlias => "snapshot_alias",
            Self::HistoryHold => "history_hold",
            Self::CommitConsumer => "commit_consumer",
            Self::SecondaryIndex => "secondary_index",
            Self::Operation => "operation",
            Self::RestoreMember => "restore_member",
            Self::StagedObject => "staged_object",
            Self::GcCandidate => "gc_candidate",
            Self::GcBarrier => "gc_barrier",
            Self::WorkspaceIncarnationClaim => "workspace_incarnation_claim",
            Self::GenericIndexCurrent => "generic_index_current",
            Self::GenericIndexGeneration => "generic_index_generation",
            Self::CommitGenericIndexMember => "commit_generic_index_member",
        }
    }
}

const KEYSPACES: [KeyspaceDef; 29] = [
    SYSTEM,
    ROOT_FENCE,
    COMMAND_DEDUPE,
    CHANGE_EVENT,
    HISTORY,
    RECOVERY_OUTBOX,
    KeyspaceDef::new(0x0202, "workspace_current"),
    KeyspaceDef::new(0x0203, "path_current"),
    KeyspaceDef::new(0x0204, "artifact_revision"),
    KeyspaceDef::new(0x0205, "artifact_manifest"),
    KeyspaceDef::new(0x0206, "revision_ref"),
    KeyspaceDef::new(0x0207, "commit"),
    KeyspaceDef::new(0x0208, "commit_member"),
    KeyspaceDef::new(0x0209, "workbench_commit_head"),
    KeyspaceDef::new(0x020a, "tag"),
    KeyspaceDef::new(0x020b, "snapshot_ref"),
    KeyspaceDef::new(0x020c, "snapshot_alias"),
    KeyspaceDef::new(0x020d, "history_hold"),
    KeyspaceDef::new(0x020e, "commit_consumer"),
    KeyspaceDef::new(0x020f, "secondary_index"),
    KeyspaceDef::new(0x0211, "operation"),
    KeyspaceDef::new(0x0212, "restore_member"),
    KeyspaceDef::new(0x0213, "staged_object"),
    KeyspaceDef::new(0x0215, "gc_candidate"),
    KeyspaceDef::new(0x0216, "gc_barrier"),
    KeyspaceDef::new(0x0217, "workspace_incarnation_claim"),
    KeyspaceDef::new(0x0219, "generic_index_current"),
    KeyspaceDef::new(0x021a, "generic_index_generation"),
    KeyspaceDef::new(0x021b, "commit_generic_index_member"),
];

/// Exact logical keyspace catalog for the current workspace schema.
pub const fn keyspaces() -> &'static [KeyspaceDef] {
    &KEYSPACES
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn catalog_has_unique_ids_and_names() {
        assert_eq!(keyspaces().len(), 29);
        let ids = keyspaces()
            .iter()
            .map(|definition| definition.id)
            .collect::<BTreeSet<_>>();
        let names = keyspaces()
            .iter()
            .map(|definition| definition.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), keyspaces().len());
        assert_eq!(names.len(), keyspaces().len());
    }

    #[test]
    fn catalog_ids_and_names_are_frozen() {
        let actual = keyspaces()
            .iter()
            .map(|definition| (definition.id.get(), definition.name))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                (0x0101, "system"),
                (0x0102, "root_fence"),
                (0x0103, "command_dedupe"),
                (0x0104, "change_event"),
                (0x0105, "history"),
                (0x0106, "recovery_outbox"),
                (0x0202, "workspace_current"),
                (0x0203, "path_current"),
                (0x0204, "artifact_revision"),
                (0x0205, "artifact_manifest"),
                (0x0206, "revision_ref"),
                (0x0207, "commit"),
                (0x0208, "commit_member"),
                (0x0209, "workbench_commit_head"),
                (0x020a, "tag"),
                (0x020b, "snapshot_ref"),
                (0x020c, "snapshot_alias"),
                (0x020d, "history_hold"),
                (0x020e, "commit_consumer"),
                (0x020f, "secondary_index"),
                (0x0211, "operation"),
                (0x0212, "restore_member"),
                (0x0213, "staged_object"),
                (0x0215, "gc_candidate"),
                (0x0216, "gc_barrier"),
                (0x0217, "workspace_incarnation_claim"),
                (0x0219, "generic_index_current"),
                (0x021a, "generic_index_generation"),
                (0x021b, "commit_generic_index_member"),
            ]
        );
    }

    #[test]
    fn family_tags_round_trip_without_becoming_keyspace_ids() {
        for family in MetadataFamily::ALL {
            assert_eq!(
                MetadataFamily::from_format_tag(family.format_tag()),
                Some(family)
            );
            assert_eq!(
                family.keyspace().get(),
                0x0200 | u16::from(family.format_tag())
            );
            assert!(keyspaces().iter().any(|definition| {
                definition.id == family.keyspace() && definition.name == family.name()
            }));
        }
        for reserved in [0x00, 0x01, 0x10, 0x14, 0x18, 0xff] {
            assert_eq!(MetadataFamily::from_format_tag(reserved), None);
        }
    }
}
