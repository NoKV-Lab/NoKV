use nokv_types::{
    ArtifactRevisionId, CommitConsumerKind, CommitId, CommitVersion, HistoryHoldKind,
    LogicalShardId, NormalizedRelativePath, OperationId, OperationKind, ReferenceEpoch,
    ReferenceKind, RootId, SnapshotAliasName, SnapshotId, TagName, WorkbenchId,
    WorkspaceIncarnationId,
};

pub const SCHEMA_ID: &str = "nokv_workspace";
pub const VALUE_FORMAT_VERSION: u8 = 1;
pub const PATH_COMPONENT_DELIMITER: u8 = 0;
/// Terminates every exact path key immediately after its descendant range.
///
/// Every UTF-8 component byte is shifted by one before storage, reserving both
/// `0x00` and `0x01`. This keeps one child's delimiter rollup and exact row
/// adjacent while sorting the exact row before every longer sibling, without
/// making an exact key a strict prefix of any valid path key.
pub const PATH_EXACT_TERMINATOR: u8 = 0x01;
const SNAPSHOT_ID_CLAIM_DISCRIMINATOR: u8 = 0xff;

pub const SYSTEM_TREE: &str = "system";
pub const ROOT_FENCE_TREE: &str = "root_fence";
pub const WORKSPACE_CURRENT_TREE: &str = "workspace_current";
pub const WORKSPACE_INCARNATION_CLAIM_TREE: &str = "workspace_incarnation_claim";
pub const PATH_CURRENT_TREE: &str = "path_current";
pub const ARTIFACT_REVISION_TREE: &str = "artifact_revision";
pub const ARTIFACT_MANIFEST_TREE: &str = "artifact_manifest";
pub const REVISION_REF_TREE: &str = "revision_ref";
pub const COMMIT_TREE: &str = "commit";
pub const COMMIT_MEMBER_TREE: &str = "commit_member";
pub const WORKBENCH_COMMIT_HEAD_TREE: &str = "workbench_commit_head";
pub const TAG_TREE: &str = "tag";
pub const SNAPSHOT_REF_TREE: &str = "snapshot_ref";
pub const SNAPSHOT_ALIAS_TREE: &str = "snapshot_alias";
pub const HISTORY_HOLD_TREE: &str = "history_hold";
pub const COMMIT_CONSUMER_TREE: &str = "commit_consumer";
pub const SECONDARY_INDEX_TREE: &str = "secondary_index";
pub const CHANGE_EVENT_TREE: &str = "change_event";
pub const OPERATION_TREE: &str = "operation";
pub const RESTORE_MEMBER_TREE: &str = "restore_member";
pub const STAGED_OBJECT_TREE: &str = "staged_object";
pub const COMMAND_DEDUPE_TREE: &str = "command_dedupe";
pub const GC_CANDIDATE_TREE: &str = "gc_candidate";
pub const GC_BARRIER_TREE: &str = "gc_barrier";
pub const HISTORY_TREE: &str = "history";
pub const RECOVERY_OUTBOX_TREE: &str = "recovery_outbox";

pub(crate) const SCHEMA_TREES: &[&str] = &[
    SYSTEM_TREE,
    ROOT_FENCE_TREE,
    WORKSPACE_CURRENT_TREE,
    WORKSPACE_INCARNATION_CLAIM_TREE,
    PATH_CURRENT_TREE,
    ARTIFACT_REVISION_TREE,
    ARTIFACT_MANIFEST_TREE,
    REVISION_REF_TREE,
    COMMIT_TREE,
    COMMIT_MEMBER_TREE,
    WORKBENCH_COMMIT_HEAD_TREE,
    TAG_TREE,
    SNAPSHOT_REF_TREE,
    SNAPSHOT_ALIAS_TREE,
    HISTORY_HOLD_TREE,
    COMMIT_CONSUMER_TREE,
    SECONDARY_INDEX_TREE,
    CHANGE_EVENT_TREE,
    OPERATION_TREE,
    RESTORE_MEMBER_TREE,
    STAGED_OBJECT_TREE,
    COMMAND_DEDUPE_TREE,
    GC_CANDIDATE_TREE,
    GC_BARRIER_TREE,
    HISTORY_TREE,
    RECOVERY_OUTBOX_TREE,
];

const FIXED_ID_BYTES: usize = 16;
pub(crate) const SYSTEM_SCHEMA_KEY: &[u8] = b"schema";
const SYSTEM_FORMAT_VERSION: u32 = 11;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SchemaMarkerError;

impl std::fmt::Display for SchemaMarkerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("missing, unknown, or malformed workspace schema marker")
    }
}

impl std::error::Error for SchemaMarkerError {}

pub fn workspace_current_key(root: RootId, workbench: &WorkbenchId) -> Vec<u8> {
    let name = workbench.as_bytes();
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + 4 + name.len());
    key.extend_from_slice(root.as_bytes());
    push_len_prefixed(&mut key, name);
    key
}

/// Root-scoped prefix for every current workbench marker.
pub fn workspace_current_prefix(root: RootId) -> Vec<u8> {
    root.as_bytes().to_vec()
}

/// Permanent root-scoped claim for one never-reused workspace incarnation.
pub fn workspace_incarnation_claim_key(
    root: RootId,
    incarnation: WorkspaceIncarnationId,
) -> Vec<u8> {
    [root.as_bytes().as_slice(), incarnation.as_bytes()].concat()
}

/// Decode the workbench identity from one exact current-marker key.
pub fn decode_workspace_current_key(root: RootId, key: &[u8]) -> Option<WorkbenchId> {
    let prefix = workspace_current_prefix(root);
    if !key.starts_with(&prefix) || key.len() < prefix.len() + 4 {
        return None;
    }
    let length = u32::from_be_bytes(key[prefix.len()..prefix.len() + 4].try_into().ok()?) as usize;
    let value = key.get(prefix.len() + 4..)?;
    if value.len() != length {
        return None;
    }
    WorkbenchId::new(std::str::from_utf8(value).ok()?).ok()
}

pub fn path_current_key(
    root: RootId,
    workspace: WorkspaceIncarnationId,
    path: &NormalizedRelativePath,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + path.byte_len().saturating_add(1));
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(workspace.as_bytes());
    push_ordered_path_components(&mut key, path);
    key.push(PATH_EXACT_TERMINATOR);
    key
}

pub fn path_child_prefix(
    root: RootId,
    workspace: WorkspaceIncarnationId,
    parent: Option<&NormalizedRelativePath>,
) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(
        FIXED_ID_BYTES * 2
            + parent
                .map(NormalizedRelativePath::byte_len)
                .unwrap_or_default()
            + usize::from(parent.is_some()),
    );
    prefix.extend_from_slice(root.as_bytes());
    prefix.extend_from_slice(workspace.as_bytes());
    if let Some(parent) = parent {
        push_ordered_path_components(&mut prefix, parent);
        prefix.push(PATH_COMPONENT_DELIMITER);
    }
    prefix
}

pub(crate) fn change_event_key(root: RootId, version: CommitVersion, sequence: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + 8 + 4);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(&version.get().to_be_bytes());
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

pub(crate) fn decode_change_event_key(root: RootId, key: &[u8]) -> Option<(CommitVersion, u32)> {
    const EXPECTED: usize = FIXED_ID_BYTES + 8 + 4;
    if key.len() != EXPECTED || !key.starts_with(root.as_bytes()) {
        return None;
    }
    let version = CommitVersion::new(u64::from_be_bytes(
        key[FIXED_ID_BYTES..FIXED_ID_BYTES + 8].try_into().ok()?,
    ))
    .ok()?;
    let sequence = u32::from_be_bytes(key[FIXED_ID_BYTES + 8..].try_into().ok()?);
    Some((version, sequence))
}

pub fn decode_path_current_key(
    root: RootId,
    workspace: WorkspaceIncarnationId,
    key: &[u8],
) -> Option<NormalizedRelativePath> {
    let prefix_length = FIXED_ID_BYTES * 2;
    if key.len() < prefix_length + 2
        || &key[..FIXED_ID_BYTES] != root.as_bytes()
        || &key[FIXED_ID_BYTES..prefix_length] != workspace.as_bytes()
        || key.last() != Some(&PATH_EXACT_TERMINATOR)
    {
        return None;
    }
    decode_path_components(&key[prefix_length..key.len() - 1])
}

pub fn decode_path_common_prefix(
    root: RootId,
    workspace: WorkspaceIncarnationId,
    key: &[u8],
) -> Option<NormalizedRelativePath> {
    let prefix_length = FIXED_ID_BYTES * 2;
    if key.len() < prefix_length + 2
        || &key[..FIXED_ID_BYTES] != root.as_bytes()
        || &key[FIXED_ID_BYTES..prefix_length] != workspace.as_bytes()
        || key.last() != Some(&PATH_COMPONENT_DELIMITER)
    {
        return None;
    }
    decode_path_components(&key[prefix_length..key.len() - 1])
}

pub fn artifact_revision_key(root: RootId, revision: ArtifactRevisionId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(revision.as_bytes());
    key
}

pub fn history_hold_prefix(root: RootId) -> Vec<u8> {
    root.as_bytes().to_vec()
}

pub fn commit_key(root: RootId, commit: CommitId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + CommitId::BYTE_WIDTH);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(commit.as_bytes());
    key
}

/// Root-scoped prefix for every immutable commit record.
pub fn commit_prefix(root: RootId) -> Vec<u8> {
    root.as_bytes().to_vec()
}

/// Decode one exact immutable-commit key.
pub fn decode_commit_key(root: RootId, key: &[u8]) -> Option<CommitId> {
    let prefix = commit_prefix(root);
    if key.len() != prefix.len() + CommitId::BYTE_WIDTH || !key.starts_with(&prefix) {
        return None;
    }
    Some(CommitId::from_bytes(key[prefix.len()..].try_into().ok()?))
}

pub fn commit_member_key(root: RootId, commit: CommitId, path: &NormalizedRelativePath) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + CommitId::BYTE_WIDTH + path.byte_len() + 1);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(commit.as_bytes());
    push_ordered_path_components(&mut key, path);
    key.push(PATH_EXACT_TERMINATOR);
    key
}

pub fn commit_member_prefix(root: RootId, commit: CommitId) -> Vec<u8> {
    commit_key(root, commit)
}

pub fn decode_commit_member_key(
    root: RootId,
    commit: CommitId,
    key: &[u8],
) -> Option<NormalizedRelativePath> {
    let prefix = commit_member_prefix(root, commit);
    if key.len() < prefix.len() + 2
        || !key.starts_with(&prefix)
        || key.last() != Some(&PATH_EXACT_TERMINATOR)
    {
        return None;
    }
    decode_path_components(&key[prefix.len()..key.len() - 1])
}

pub fn workbench_commit_head_key(root: RootId, workspace: WorkspaceIncarnationId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(workspace.as_bytes());
    key
}

pub fn tag_key(root: RootId, workspace: WorkspaceIncarnationId, tag: &TagName) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + 2 + tag.as_bytes().len());
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(workspace.as_bytes());
    push_short_len_prefixed(&mut key, tag.as_bytes());
    key
}

pub fn snapshot_ref_key(
    root: RootId,
    workspace: WorkspaceIncarnationId,
    snapshot: SnapshotId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + 8);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(workspace.as_bytes());
    key.extend_from_slice(&snapshot.get().to_be_bytes());
    key
}

/// Exact snapshot-row prefix for one workspace incarnation.
pub fn snapshot_ref_prefix(root: RootId, workspace: WorkspaceIncarnationId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(workspace.as_bytes());
    key
}

/// Decode one exact snapshot row, excluding the reserved root-global claim key.
pub fn decode_snapshot_ref_key(
    root: RootId,
    key: &[u8],
) -> Option<(WorkspaceIncarnationId, SnapshotId)> {
    if key.len() != FIXED_ID_BYTES * 2 + 8 || !key.starts_with(root.as_bytes()) {
        return None;
    }
    let workspace = WorkspaceIncarnationId::from_bytes(
        key[FIXED_ID_BYTES..FIXED_ID_BYTES * 2].try_into().ok()?,
    );
    let snapshot = SnapshotId::new(u64::from_be_bytes(
        key[FIXED_ID_BYTES * 2..].try_into().ok()?,
    ));
    Some((workspace, snapshot))
}

/// Root-global never-reused claim for one numeric snapshot id.
///
/// Snapshot rows otherwise include a workspace incarnation in their key. This
/// reserved short key makes root-global identity enforceable without a remote
/// allocator or a second authoritative family.
pub fn snapshot_id_claim_key(root: RootId, snapshot: SnapshotId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + 1 + 8);
    key.extend_from_slice(root.as_bytes());
    key.push(SNAPSHOT_ID_CLAIM_DISCRIMINATOR);
    key.extend_from_slice(&snapshot.get().to_be_bytes());
    key
}

pub fn snapshot_alias_key(
    root: RootId,
    workspace: WorkspaceIncarnationId,
    alias: &SnapshotAliasName,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + 2 + alias.as_bytes().len());
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(workspace.as_bytes());
    push_short_len_prefixed(&mut key, alias.as_bytes());
    key
}

pub fn snapshot_history_hold_key(root: RootId, snapshot: SnapshotId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + 1 + 8);
    key.extend_from_slice(root.as_bytes());
    key.push(HistoryHoldKind::Snapshot.into());
    key.extend_from_slice(&snapshot.get().to_be_bytes());
    key
}

pub fn build_commit_history_hold_key(root: RootId, operation: OperationId) -> Vec<u8> {
    operation_history_hold_key(root, HistoryHoldKind::BuildCommit, operation)
}

pub fn restore_history_hold_key(root: RootId, operation: OperationId) -> Vec<u8> {
    operation_history_hold_key(root, HistoryHoldKind::Restore, operation)
}

fn operation_history_hold_key(
    root: RootId,
    kind: HistoryHoldKind,
    operation: OperationId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + 1);
    key.extend_from_slice(root.as_bytes());
    key.push(kind.into());
    key.extend_from_slice(operation.as_bytes());
    key
}

pub fn artifact_manifest_key(
    root: RootId,
    revision: ArtifactRevisionId,
    object_index: u64,
) -> Vec<u8> {
    let mut key = artifact_manifest_prefix(root, revision);
    key.extend_from_slice(&object_index.to_be_bytes());
    key
}

pub fn artifact_manifest_prefix(root: RootId, revision: ArtifactRevisionId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(revision.as_bytes());
    key
}

pub fn decode_artifact_manifest_key(
    root: RootId,
    revision: ArtifactRevisionId,
    key: &[u8],
) -> Option<u64> {
    let prefix = artifact_manifest_prefix(root, revision);
    if key.len() != prefix.len() + 8 || !key.starts_with(&prefix) {
        return None;
    }
    Some(u64::from_be_bytes(key[prefix.len()..].try_into().ok()?))
}

pub fn path_revision_ref_key(
    root: RootId,
    workspace: WorkspaceIncarnationId,
    path: &NormalizedRelativePath,
    revision: ArtifactRevisionId,
) -> Vec<u8> {
    let encoded_path = encoded_path(path);
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 3 + 1 + 4 + encoded_path.len());
    key.extend_from_slice(root.as_bytes());
    key.push(ReferenceKind::Path.into());
    key.extend_from_slice(workspace.as_bytes());
    push_len_prefixed(&mut key, &encoded_path);
    key.extend_from_slice(revision.as_bytes());
    key
}

pub fn commit_revision_ref_key(
    root: RootId,
    commit_id: CommitId,
    revision: ArtifactRevisionId,
) -> Vec<u8> {
    let mut key = commit_revision_ref_prefix(root, commit_id);
    key.extend_from_slice(revision.as_bytes());
    key
}

pub fn commit_revision_ref_prefix(root: RootId, commit_id: CommitId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + 1 + CommitId::BYTE_WIDTH);
    key.extend_from_slice(root.as_bytes());
    key.push(ReferenceKind::Commit.into());
    key.extend_from_slice(commit_id.as_bytes());
    key
}

pub fn revision_dependency_ref_key(
    root: RootId,
    child: ArtifactRevisionId,
    owner: ArtifactRevisionId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 3 + 1);
    key.extend_from_slice(root.as_bytes());
    key.push(ReferenceKind::RevisionDependency.into());
    key.extend_from_slice(child.as_bytes());
    key.extend_from_slice(owner.as_bytes());
    key
}

pub fn revision_dependency_ref_prefix(root: RootId, child: ArtifactRevisionId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + 1);
    key.extend_from_slice(root.as_bytes());
    key.push(ReferenceKind::RevisionDependency.into());
    key.extend_from_slice(child.as_bytes());
    key
}

pub fn decode_revision_dependency_ref_key(
    root: RootId,
    child: ArtifactRevisionId,
    key: &[u8],
) -> Option<ArtifactRevisionId> {
    let prefix = revision_dependency_ref_prefix(root, child);
    if key.len() != prefix.len() + FIXED_ID_BYTES || !key.starts_with(&prefix) {
        return None;
    }
    let owner = key[prefix.len()..].try_into().ok()?;
    Some(ArtifactRevisionId::from_bytes(owner))
}

pub fn workbench_head_commit_consumer_key(
    root: RootId,
    commit: CommitId,
    workspace: WorkspaceIncarnationId,
) -> Vec<u8> {
    let mut key = commit_consumer_prefix(root, commit, CommitConsumerKind::WorkbenchHead);
    key.extend_from_slice(workspace.as_bytes());
    key
}

pub fn tag_commit_consumer_key(
    root: RootId,
    commit: CommitId,
    workspace: WorkspaceIncarnationId,
    tag: &TagName,
) -> Vec<u8> {
    let mut key = commit_consumer_prefix(root, commit, CommitConsumerKind::Tag);
    key.extend_from_slice(workspace.as_bytes());
    push_short_len_prefixed(&mut key, tag.as_bytes());
    key
}

pub fn lease_commit_consumer_key(
    root: RootId,
    commit: CommitId,
    operation: OperationId,
) -> Vec<u8> {
    let mut key = commit_consumer_prefix(root, commit, CommitConsumerKind::Lease);
    key.extend_from_slice(operation.as_bytes());
    key
}

pub fn child_commit_consumer_key(root: RootId, parent: CommitId, child: CommitId) -> Vec<u8> {
    let mut key = commit_consumer_prefix(root, parent, CommitConsumerKind::ChildCommit);
    key.extend_from_slice(child.as_bytes());
    key
}

fn commit_consumer_prefix(root: RootId, commit: CommitId, kind: CommitConsumerKind) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + CommitId::BYTE_WIDTH + 1);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(commit.as_bytes());
    key.push(kind.into());
    key
}

pub fn operation_key(root: RootId, kind: OperationKind, operation: OperationId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + 1);
    key.extend_from_slice(root.as_bytes());
    key.push(kind.into());
    key.extend_from_slice(operation.as_bytes());
    key
}

/// Root-and-kind-scoped prefix for durable lifecycle operations.
pub fn operation_prefix(root: RootId, kind: OperationKind) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + 1);
    key.extend_from_slice(root.as_bytes());
    key.push(kind.into());
    key
}

/// Decode one exact lifecycle-operation key of the requested kind.
pub fn decode_operation_key(root: RootId, kind: OperationKind, key: &[u8]) -> Option<OperationId> {
    let prefix = operation_prefix(root, kind);
    if key.len() != prefix.len() + FIXED_ID_BYTES || !key.starts_with(&prefix) {
        return None;
    }
    Some(OperationId::from_bytes(
        key[prefix.len()..].try_into().ok()?,
    ))
}

pub fn staged_object_key(root: RootId, operation: OperationId, sequence: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + 8);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(operation.as_bytes());
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

pub fn staged_object_prefix(root: RootId, operation: OperationId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(operation.as_bytes());
    key
}

pub fn restore_member_key(root: RootId, operation: OperationId, sequence: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + 8);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(operation.as_bytes());
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

pub fn restore_member_prefix(root: RootId, operation: OperationId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(operation.as_bytes());
    key
}

pub fn gc_candidate_key(
    root: RootId,
    revision: ArtifactRevisionId,
    reference_epoch: ReferenceEpoch,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES * 2 + 8);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(revision.as_bytes());
    key.extend_from_slice(&reference_epoch.get().to_be_bytes());
    key
}

pub fn gc_candidate_prefix(root: RootId) -> Vec<u8> {
    root.as_bytes().to_vec()
}

/// Sole root-scoped durable row used to advance a quiescent GC history floor.
pub fn gc_history_barrier_key(root: RootId) -> Vec<u8> {
    root.as_bytes().to_vec()
}

pub fn decode_gc_candidate_key(
    root: RootId,
    key: &[u8],
) -> Option<(ArtifactRevisionId, ReferenceEpoch)> {
    let prefix = gc_candidate_prefix(root);
    if key.len() != prefix.len() + FIXED_ID_BYTES + 8 || !key.starts_with(&prefix) {
        return None;
    }
    let revision = ArtifactRevisionId::from_bytes(
        key[prefix.len()..prefix.len() + FIXED_ID_BYTES]
            .try_into()
            .ok()?,
    );
    let epoch = ReferenceEpoch::new(u64::from_be_bytes(
        key[prefix.len() + FIXED_ID_BYTES..].try_into().ok()?,
    ));
    Some((revision, epoch))
}

pub fn object_block_key(
    shard: LogicalShardId,
    root: RootId,
    revision: ArtifactRevisionId,
    object_index: u64,
) -> String {
    format!(
        "nokv/artifacts/{}/{}/{}/blocks/{object_index:016x}",
        lower_hex(shard.as_bytes()),
        lower_hex(root.as_bytes()),
        lower_hex(revision.as_bytes()),
    )
}

pub(crate) fn encode_schema_marker() -> Vec<u8> {
    let mut value = Vec::with_capacity(1 + 4 + SCHEMA_ID.len() + 4);
    value.push(VALUE_FORMAT_VERSION);
    push_len_prefixed(&mut value, SCHEMA_ID.as_bytes());
    value.extend_from_slice(&SYSTEM_FORMAT_VERSION.to_be_bytes());
    value
}

pub(crate) fn validate_schema_marker(value: &[u8]) -> Result<(), SchemaMarkerError> {
    if value == encode_schema_marker() {
        Ok(())
    } else {
        Err(SchemaMarkerError)
    }
}

fn encoded_path(path: &NormalizedRelativePath) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(path.byte_len());
    push_ordered_path_components(&mut encoded, path);
    encoded
}

/// Append the format-v8 order-preserving physical encoding of a logical path.
pub(crate) fn push_ordered_path_components(target: &mut Vec<u8>, path: &NormalizedRelativePath) {
    for (index, component) in path.components().enumerate() {
        if index != 0 {
            target.push(PATH_COMPONENT_DELIMITER);
        }
        target.extend(component.as_bytes().iter().map(|byte| byte + 1));
    }
}

fn decode_path_components(encoded: &[u8]) -> Option<NormalizedRelativePath> {
    let mut decoded = Vec::with_capacity(encoded.len());
    for byte in encoded {
        match *byte {
            PATH_COMPONENT_DELIMITER => decoded.push(b'/'),
            PATH_EXACT_TERMINATOR => return None,
            byte => decoded.push(byte - 1),
        }
    }
    let path = NormalizedRelativePath::new(std::str::from_utf8(&decoded).ok()?).ok()?;
    (encoded_path(&path) == encoded).then_some(path)
}

fn push_len_prefixed(target: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("validated component length fits u32");
    target.extend_from_slice(&len.to_be_bytes());
    target.extend_from_slice(bytes);
}

fn push_short_len_prefixed(target: &mut Vec<u8>, bytes: &[u8]) {
    let len = u16::try_from(bytes.len()).expect("validated durable name length fits u16");
    target.extend_from_slice(&len.to_be_bytes());
    target.extend_from_slice(bytes);
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(fill: u8) -> RootId {
        RootId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn workspace(fill: u8) -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn revision(fill: u8) -> ArtifactRevisionId {
        ArtifactRevisionId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn path(value: &str) -> NormalizedRelativePath {
        NormalizedRelativePath::new(value).unwrap()
    }

    #[test]
    fn workspace_key_uses_fixed_root_and_big_endian_name_length() {
        let key = workspace_current_key(root(0x11), &WorkbenchId::new("wb-1").unwrap());
        assert_eq!(&key[..16], &[0x11; 16]);
        assert_eq!(&key[16..20], &4_u32.to_be_bytes());
        assert_eq!(&key[20..], b"wb-1");

        let claim = workspace_incarnation_claim_key(root(0x11), workspace(0x22));
        assert_eq!(&claim[..16], &[0x11; 16]);
        assert_eq!(&claim[16..], &[0x22; 16]);
    }

    #[test]
    fn exact_path_uses_ordered_terminator() {
        let expected_path = path("outputs/run/file.bin");
        let key = path_current_key(root(1), workspace(2), &expected_path);
        assert_eq!(&key[..16], &[1; 16]);
        assert_eq!(&key[16..32], &[2; 16]);
        assert_eq!(&key[32..key.len() - 1], b"pvuqvut\0svo\0gjmf/cjo");
        assert_eq!(key.last(), Some(&PATH_EXACT_TERMINATOR));
        assert_eq!(
            decode_path_current_key(root(1), workspace(2), &key),
            Some(expected_path)
        );
    }

    #[test]
    fn child_prefix_separates_a_from_ab() {
        let exact_a = path_current_key(root(1), workspace(2), &path("a"));
        let exact_ab = path_current_key(root(1), workspace(2), &path("ab"));
        let exact_a_control = path_current_key(root(1), workspace(2), &path("a\u{1}"));
        let child_a = path_child_prefix(root(1), workspace(2), Some(&path("a")));
        let key_a_child = path_current_key(root(1), workspace(2), &path("a/file"));

        assert_eq!(&exact_a[32..], b"b\x01");
        assert_eq!(&child_a[32..], b"b\0");
        assert_eq!(&exact_ab[32..], b"bc\x01");
        assert_eq!(&exact_a_control[32..], b"b\x02\x01");
        assert!(!exact_a.starts_with(&child_a));
        assert!(key_a_child.starts_with(&child_a));
        assert!(!exact_ab.starts_with(&child_a));
        assert!(child_a < exact_a);
        assert!(exact_a < exact_a_control);
        assert!(exact_a < exact_ab);
        assert_eq!(
            decode_path_current_key(root(1), workspace(2), &exact_a_control),
            Some(path("a\u{1}"))
        );
        assert_eq!(
            decode_path_common_prefix(root(1), workspace(2), &child_a),
            Some(path("a"))
        );
    }

    #[test]
    fn lifecycle_root_prefix_decoders_reject_adjacent_key_shapes() {
        let root = root(9);
        let other_root = RootId::from_bytes([1; FIXED_ID_BYTES]);
        let workbench = WorkbenchId::new("run-9").unwrap();
        let workspace = workspace(8);
        let snapshot = SnapshotId::new(7);
        let commit = CommitId::from_bytes([6; CommitId::BYTE_WIDTH]);
        let operation = OperationId::from_bytes([5; FIXED_ID_BYTES]);

        let workspace_key = workspace_current_key(root, &workbench);
        assert_eq!(workspace_current_prefix(root), root.as_bytes());
        assert_eq!(
            decode_workspace_current_key(root, &workspace_key),
            Some(workbench)
        );
        assert_eq!(decode_workspace_current_key(root, root.as_bytes()), None);

        let snapshot_key = snapshot_ref_key(root, workspace, snapshot);
        assert_eq!(
            decode_snapshot_ref_key(root, &snapshot_key),
            Some((workspace, snapshot))
        );
        assert_eq!(
            decode_snapshot_ref_key(root, &snapshot_id_claim_key(root, snapshot)),
            None
        );

        let commit_key = commit_key(root, commit);
        assert_eq!(decode_commit_key(root, &commit_key), Some(commit));
        assert_eq!(decode_commit_key(other_root, &commit_key), None);

        let operation_key = operation_key(root, OperationKind::Gc, operation);
        assert_eq!(
            decode_operation_key(root, OperationKind::Gc, &operation_key),
            Some(operation)
        );
        assert_eq!(
            decode_operation_key(root, OperationKind::Restore, &operation_key),
            None
        );
    }

    #[test]
    fn root_listing_prefix_is_exact_incarnation_prefix() {
        let prefix = path_child_prefix(root(3), workspace(4), None);
        assert_eq!(prefix, [[3; 16].as_slice(), [4; 16].as_slice()].concat());
    }

    #[test]
    fn path_codec_round_trips_unicode() {
        let expected_path = path("输出/Å/Å");
        let key = path_current_key(root(0), workspace(0), &expected_path);
        assert!(key[32..key.len() - 1]
            .iter()
            .all(|byte| *byte != PATH_EXACT_TERMINATOR));
        assert_eq!(key.last(), Some(&PATH_EXACT_TERMINATOR));
        assert_eq!(
            decode_path_current_key(root(0), workspace(0), &key),
            Some(expected_path)
        );
    }

    #[test]
    fn reference_kinds_have_distinct_owner_encodings() {
        let path_ref =
            path_revision_ref_key(root(1), workspace(2), &path("logs/run.log"), revision(3));
        let commit_ref =
            commit_revision_ref_key(root(1), CommitId::from_bytes([4; 32]), revision(3));
        let dependency_ref = revision_dependency_ref_key(root(1), revision(5), revision(3));

        assert_eq!(path_ref[16], u8::from(ReferenceKind::Path));
        assert_eq!(commit_ref[16], u8::from(ReferenceKind::Commit));
        assert_eq!(
            dependency_ref[16],
            u8::from(ReferenceKind::RevisionDependency)
        );
        assert_ne!(path_ref, commit_ref);
        assert_ne!(commit_ref, dependency_ref);
    }

    #[test]
    fn object_key_is_owner_scoped_and_fixed_width() {
        assert_eq!(
            object_block_key(
                LogicalShardId::from_bytes([0xab; 16]),
                root(0xcd),
                revision(0xef),
                15,
            ),
            concat!(
                "nokv/artifacts/abababababababababababababababab/",
                "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd/",
                "efefefefefefefefefefefefefefefef/",
                "blocks/000000000000000f"
            )
        );
    }

    #[test]
    fn publication_family_keys_are_root_scoped_and_ordered() {
        let root = root(0x11);
        let revision = revision(0x22);
        let operation = OperationId::from_bytes([0x33; 16]);

        let manifest = artifact_manifest_key(root, revision, 9);
        assert_eq!(
            &manifest[..32],
            artifact_manifest_prefix(root, revision).as_slice()
        );
        assert_eq!(&manifest[32..], &9_u64.to_be_bytes());
        assert_eq!(
            decode_artifact_manifest_key(root, revision, &manifest),
            Some(9)
        );

        let operation_key = operation_key(root, OperationKind::Publish, operation);
        assert_eq!(&operation_key[..16], root.as_bytes());
        assert_eq!(operation_key[16], u8::from(OperationKind::Publish));
        assert_eq!(&operation_key[17..], operation.as_bytes());

        let staged = staged_object_key(root, operation, 12);
        let staged_prefix = staged_object_prefix(root, operation);
        assert!(staged.starts_with(&staged_prefix));
        assert_eq!(staged.len(), staged_prefix.len() + u64::BITS as usize / 8);
        assert_eq!(&staged[..32], staged_prefix.as_slice());
        assert_eq!(&staged[32..], &12_u64.to_be_bytes());

        let candidate = gc_candidate_key(root, revision, ReferenceEpoch::new(4));
        assert_eq!(&candidate[..16], root.as_bytes());
        assert_eq!(&candidate[16..32], revision.as_bytes());
        assert_eq!(&candidate[32..], &4_u64.to_be_bytes());
        assert_eq!(
            decode_gc_candidate_key(root, &candidate),
            Some((revision, ReferenceEpoch::new(4)))
        );

        let child = ArtifactRevisionId::from_bytes([0x44; FIXED_ID_BYTES]);
        let dependency = revision_dependency_ref_key(root, child, revision);
        assert_eq!(
            decode_revision_dependency_ref_key(root, child, &dependency),
            Some(revision)
        );
        assert_eq!(
            revision_dependency_ref_prefix(root, child),
            dependency[..33]
        );
    }

    #[test]
    fn commit_snapshot_and_hold_keys_freeze_owner_shapes() {
        let root = root(0x11);
        let workspace = workspace(0x22);
        let commit = CommitId::from_bytes([0x33; 32]);
        let operation = OperationId::from_bytes([0x44; 16]);
        let member_path = path("outputs/result.json");
        let member = commit_member_key(root, commit, &member_path);

        assert_eq!(&member[..48], commit_member_prefix(root, commit).as_slice());
        assert_eq!(&member[48..], b"pvuqvut\0sftvmu/ktpo\x01");
        assert_eq!(
            decode_commit_member_key(root, commit, &member),
            Some(member_path)
        );
        assert_eq!(
            workbench_commit_head_key(root, workspace),
            [root.as_bytes().as_slice(), workspace.as_bytes().as_slice()].concat()
        );

        let tag = TagName::new("latest").unwrap();
        let tag = tag_key(root, workspace, &tag);
        assert_eq!(
            &tag[..32],
            [root.as_bytes().as_slice(), workspace.as_bytes().as_slice(),].concat()
        );
        assert_eq!(&tag[32..34], &6_u16.to_be_bytes());
        assert_eq!(&tag[34..], b"latest");

        let alias = SnapshotAliasName::new("handoff").unwrap();
        let alias = snapshot_alias_key(root, workspace, &alias);
        assert_eq!(&alias[32..34], &7_u16.to_be_bytes());
        assert_eq!(&alias[34..], b"handoff");
        let snapshot = snapshot_ref_key(root, workspace, SnapshotId::new(9));
        assert_eq!(&snapshot[32..], &9_u64.to_be_bytes());

        let snapshot_hold = snapshot_history_hold_key(root, SnapshotId::new(9));
        assert_eq!(snapshot_hold[16], u8::from(HistoryHoldKind::Snapshot));
        assert_eq!(&snapshot_hold[17..], &9_u64.to_be_bytes());
        let build_hold = build_commit_history_hold_key(root, operation);
        assert_eq!(build_hold[16], u8::from(HistoryHoldKind::BuildCommit));
        assert_eq!(&build_hold[17..], operation.as_bytes());
        let restore_hold = restore_history_hold_key(root, operation);
        assert_eq!(restore_hold[16], u8::from(HistoryHoldKind::Restore));
    }

    #[test]
    fn commit_consumer_keys_encode_the_discriminated_owner() {
        let root = root(1);
        let workspace = workspace(2);
        let parent = CommitId::from_bytes([3; 32]);
        let child = CommitId::from_bytes([4; 32]);
        let operation = OperationId::from_bytes([5; 16]);
        let tag = TagName::new("prod").unwrap();

        let head = workbench_head_commit_consumer_key(root, parent, workspace);
        let tag_owner = tag_commit_consumer_key(root, parent, workspace, &tag);
        let lease = lease_commit_consumer_key(root, parent, operation);
        let child_owner = child_commit_consumer_key(root, parent, child);
        assert_eq!(head[48], u8::from(CommitConsumerKind::WorkbenchHead));
        assert_eq!(tag_owner[48], u8::from(CommitConsumerKind::Tag));
        assert_eq!(lease[48], u8::from(CommitConsumerKind::Lease));
        assert_eq!(child_owner[48], u8::from(CommitConsumerKind::ChildCommit));
        assert_eq!(&head[49..], workspace.as_bytes());
        assert_eq!(&lease[49..], operation.as_bytes());
        assert_eq!(&child_owner[49..], child.as_bytes());
        assert_eq!(&tag_owner[49..65], workspace.as_bytes());
        assert_eq!(&tag_owner[65..67], &4_u16.to_be_bytes());
        assert_eq!(&tag_owner[67..], b"prod");
    }

    #[test]
    fn schema_marker_is_exact_and_versioned() {
        let marker = encode_schema_marker();
        assert_eq!(marker[0], VALUE_FORMAT_VERSION);
        assert_eq!(
            &marker[marker.len() - std::mem::size_of::<u32>()..],
            &SYSTEM_FORMAT_VERSION.to_be_bytes()
        );
        validate_schema_marker(&marker).unwrap();

        let mut previous_layout = marker.clone();
        let version_start = previous_layout.len() - std::mem::size_of::<u32>();
        previous_layout[version_start..].copy_from_slice(&8_u32.to_be_bytes());
        assert_eq!(
            validate_schema_marker(&previous_layout),
            Err(SchemaMarkerError)
        );

        let mut unknown = marker;
        unknown[0] = VALUE_FORMAT_VERSION + 1;
        assert_eq!(validate_schema_marker(&unknown), Err(SchemaMarkerError));
    }
}
