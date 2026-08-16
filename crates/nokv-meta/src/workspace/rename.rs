/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Atomic, storage-neutral moves of authoritative workspace paths.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommandDigest, CommitVersion, Generation, NormalizedRelativePath,
    RevisionState, WorkbenchId, WorkspaceRevision, WorkspaceState, FIXED_ID_BYTES, SHA256_BYTES,
};
use sha2::{Digest as _, Sha256};

use super::codec::{
    artifact_revision_key, path_current_key, path_revision_ref_key, workspace_current_key,
    SCHEMA_ID,
};
use super::commit::RUN_MANIFEST_PATH;
use super::engine::{
    CommandMutation, CommandPredicate, HistoryProjection, MetaError, MetaShard, MetadataCommand,
    RootFenceAction,
};
use super::event_projection::change_event_projection;
use super::keyspace::MetadataFamily;
use super::namespace::RootWriteContext;
use super::publication_records::{
    ArtifactRevisionRecord, PathEntry, PublicationRecordCodecError, RevisionRefRecord,
    WorkspaceRecord,
};
use super::query_records::{
    secondary_index_key, ChangeEventKind, ChangeEventRecord, QueryRecordError,
    SecondaryIndexRecord, TypedProjection,
};
use super::restore::RESTORE_MANIFEST_PATH;

const RENAME_RESULT_VERSION: u8 = 1;

/// One generation-fenced, create-only path move inside a visible Workbench.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenamePathRequest {
    pub context: RootWriteContext,
    pub workbench_id: WorkbenchId,
    pub source: NormalizedRelativePath,
    pub destination: NormalizedRelativePath,
    pub expected_generation: Generation,
}

/// Durable result returned for both the first path move and an exact replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenamePathOutcome {
    pub workspace_revision: WorkspaceRevision,
    pub generation: Generation,
    pub artifact_revision_id: ArtifactRevisionId,
    pub commit_version: CommitVersion,
    pub replayed: bool,
}

/// Typed failure for one atomic path move.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RenamePathError {
    Meta(MetaError),
    RecordCodec(PublicationRecordCodecError),
    QueryRecord(QueryRecordError),
    WorkspaceNotFound,
    WorkspaceUnavailable,
    ReservedManifest,
    SamePath,
    WorkspaceRevisionOverflow,
    SourceNotFound,
    DestinationAlreadyExists,
    GenerationMismatch { expected: u64, actual: u64 },
    RevisionNotFound { revision: ArtifactRevisionId },
    RevisionUnavailable { revision: ArtifactRevisionId },
    RevisionReferenceMissing,
    RevisionReferenceEpochAhead,
    ConcurrentMutation,
    RequestInputMismatch,
    DeterministicResultMismatch { reason: String },
}

impl fmt::Display for RenamePathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Meta(error) => error.fmt(formatter),
            Self::RecordCodec(error) => write!(formatter, "metadata record failed: {error}"),
            Self::QueryRecord(error) => write!(formatter, "query record failed: {error}"),
            Self::WorkspaceNotFound => formatter.write_str("workbench does not exist"),
            Self::WorkspaceUnavailable => formatter.write_str("workbench is not visible"),
            Self::ReservedManifest => formatter.write_str(
                "canonical Workbench manifests cannot be renamed outside their lifecycle",
            ),
            Self::SamePath => formatter.write_str("source and destination paths must differ"),
            Self::WorkspaceRevisionOverflow => formatter.write_str("workspace revision overflowed"),
            Self::SourceNotFound => formatter.write_str("source path does not exist"),
            Self::DestinationAlreadyExists => {
                formatter.write_str("destination path already exists")
            }
            Self::GenerationMismatch { expected, actual } => write!(
                formatter,
                "source path generation mismatch: expected {expected}, actual {actual}"
            ),
            Self::RevisionNotFound { revision } => write!(
                formatter,
                "artifact revision {:?} does not exist",
                revision.as_bytes()
            ),
            Self::RevisionUnavailable { revision } => write!(
                formatter,
                "artifact revision {:?} is unavailable",
                revision.as_bytes()
            ),
            Self::RevisionReferenceMissing => {
                formatter.write_str("source path revision reference is missing")
            }
            Self::RevisionReferenceEpochAhead => {
                formatter.write_str("source path revision reference epoch is ahead of its owner")
            }
            Self::ConcurrentMutation => {
                formatter.write_str("path rename lost a concurrent metadata mutation")
            }
            Self::RequestInputMismatch => {
                formatter.write_str("request id was reused with different path-rename inputs")
            }
            Self::DeterministicResultMismatch { reason } => {
                write!(formatter, "invalid replayed path-rename result: {reason}")
            }
        }
    }
}

impl std::error::Error for RenamePathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Meta(error) => Some(error),
            Self::RecordCodec(error) => Some(error),
            Self::QueryRecord(error) => Some(error),
            _ => None,
        }
    }
}

impl From<MetaError> for RenamePathError {
    fn from(error: MetaError) -> Self {
        Self::Meta(error)
    }
}

impl From<PublicationRecordCodecError> for RenamePathError {
    fn from(error: PublicationRecordCodecError) -> Self {
        Self::RecordCodec(error)
    }
}

impl From<QueryRecordError> for RenamePathError {
    fn from(error: QueryRecordError) -> Self {
        Self::QueryRecord(error)
    }
}

/// Move one authoritative path without changing its immutable revision lifetime.
///
/// Source and destination namespace rows, the path-owned revision reference,
/// typed secondary indexes, the Workbench revision, and both change events
/// advance through one metadata command. The artifact revision row is an exact
/// predicate only: its reference count, epoch, and last-zero version never
/// change during a move.
pub fn rename_path(
    store: &MetaShard,
    request: RenamePathRequest,
) -> Result<RenamePathOutcome, RenamePathError> {
    if request.source == request.destination {
        return Err(RenamePathError::SamePath);
    }
    if [request.source.as_str(), request.destination.as_str()]
        .into_iter()
        .any(|path| matches!(path, RUN_MANIFEST_PATH | RESTORE_MANIFEST_PATH))
    {
        return Err(RenamePathError::ReservedManifest);
    }

    let input_digest = rename_input_digest(&request);
    if let Some(replay) = store.lookup_request(
        request.context.root_id,
        request.context.placement_generation,
        request.context.owner_epoch,
        request.context.request_id,
    )? {
        let result = decode_rename_result(&replay.deterministic_result, input_digest)?;
        return Ok(RenamePathOutcome {
            workspace_revision: result.workspace_revision,
            generation: result.generation,
            artifact_revision_id: result.artifact_revision_id,
            commit_version: replay.commit_version,
            replayed: true,
        });
    }

    let workspace_key = workspace_current_key(request.context.root_id, &request.workbench_id);
    let workspace_payload = read_current(
        store,
        request.context,
        MetadataFamily::WorkspaceCurrent,
        &workspace_key,
    )?
    .ok_or(RenamePathError::WorkspaceNotFound)?;
    let workspace = WorkspaceRecord::decode(&workspace_payload)?;
    if workspace.state != WorkspaceState::Visible || workspace.owning_operation_id.is_some() {
        return Err(RenamePathError::WorkspaceUnavailable);
    }
    let workspace_revision = WorkspaceRevision::new(
        workspace
            .workspace_revision
            .get()
            .checked_add(1)
            .ok_or(RenamePathError::WorkspaceRevisionOverflow)?,
    );
    let next_workspace = WorkspaceRecord {
        workspace_revision,
        ..workspace
    };

    let source_key = path_current_key(
        request.context.root_id,
        workspace.incarnation_id,
        &request.source,
    );
    let source_payload = read_current(
        store,
        request.context,
        MetadataFamily::PathCurrent,
        &source_key,
    )?
    .ok_or(RenamePathError::SourceNotFound)?;
    let source = PathEntry::decode(&source_payload)?;
    if source.generation != request.expected_generation {
        return Err(RenamePathError::GenerationMismatch {
            expected: request.expected_generation.get(),
            actual: source.generation.get(),
        });
    }

    let destination_key = path_current_key(
        request.context.root_id,
        workspace.incarnation_id,
        &request.destination,
    );
    if read_current(
        store,
        request.context,
        MetadataFamily::PathCurrent,
        &destination_key,
    )?
    .is_some()
    {
        return Err(RenamePathError::DestinationAlreadyExists);
    }

    let revision_key = artifact_revision_key(request.context.root_id, source.artifact_revision_id);
    let revision_payload = read_current(
        store,
        request.context,
        MetadataFamily::ArtifactRevision,
        &revision_key,
    )?
    .ok_or(RenamePathError::RevisionNotFound {
        revision: source.artifact_revision_id,
    })?;
    let revision = ArtifactRevisionRecord::decode(&revision_payload)?;
    if revision.state != RevisionState::Available {
        return Err(RenamePathError::RevisionUnavailable {
            revision: source.artifact_revision_id,
        });
    }

    let source_reference_key = path_revision_ref_key(
        request.context.root_id,
        workspace.incarnation_id,
        &request.source,
        source.artifact_revision_id,
    );
    let source_reference_payload = read_current(
        store,
        request.context,
        MetadataFamily::RevisionRef,
        &source_reference_key,
    )?
    .ok_or(RenamePathError::RevisionReferenceMissing)?;
    let reference = RevisionRefRecord::decode(&source_reference_payload)?;
    if reference.reference_epoch_at_add > revision.reference_epoch {
        return Err(RenamePathError::RevisionReferenceEpochAhead);
    }
    let destination_reference_key = path_revision_ref_key(
        request.context.root_id,
        workspace.incarnation_id,
        &request.destination,
        source.artifact_revision_id,
    );

    let projection = TypedProjection::decode(&source.typed_index_projection)?;
    let mut predicates = Vec::new();
    let mut mutations = Vec::new();
    let mut history_projection = Vec::new();
    replace(
        &mut predicates,
        &mut mutations,
        &mut history_projection,
        MetadataFamily::WorkspaceCurrent,
        workspace_key,
        workspace_payload,
        next_workspace.encode()?,
    );
    delete(
        &mut predicates,
        &mut mutations,
        &mut history_projection,
        MetadataFamily::PathCurrent,
        source_key,
        source_payload.clone(),
    );
    put_absent(
        &mut predicates,
        &mut mutations,
        MetadataFamily::PathCurrent,
        destination_key,
        source_payload,
    );
    delete(
        &mut predicates,
        &mut mutations,
        &mut history_projection,
        MetadataFamily::RevisionRef,
        source_reference_key,
        source_reference_payload.clone(),
    );
    put_absent(
        &mut predicates,
        &mut mutations,
        MetadataFamily::RevisionRef,
        destination_reference_key,
        source_reference_payload,
    );
    predicates.push(CommandPredicate::Value {
        family: MetadataFamily::ArtifactRevision,
        key: revision_key,
        expected: Some(revision_payload),
    });

    let index_payload = SecondaryIndexRecord {
        path_generation: source.generation,
        compact_projection: projection.clone(),
    }
    .encode()?;
    for (field, scalar) in projection.fields() {
        delete(
            &mut predicates,
            &mut mutations,
            &mut history_projection,
            MetadataFamily::SecondaryIndex,
            secondary_index_key(
                request.context.root_id,
                field,
                scalar,
                workspace.incarnation_id,
                &request.source,
            ),
            index_payload.clone(),
        );
        put_absent(
            &mut predicates,
            &mut mutations,
            MetadataFamily::SecondaryIndex,
            secondary_index_key(
                request.context.root_id,
                field,
                scalar,
                workspace.incarnation_id,
                &request.destination,
            ),
            index_payload.clone(),
        );
    }

    let removed = change_event_projection(&ChangeEventRecord {
        workbench_id: request.workbench_id.clone(),
        workspace_incarnation_id: workspace.incarnation_id,
        kind: ChangeEventKind::PathRemoved,
        artifact_revision_id: Some(source.artifact_revision_id),
        commit_id: None,
        operation_id: None,
        path: Some(request.source.clone()),
        before: projection.clone(),
        after: TypedProjection::empty(),
    })?;
    let published = change_event_projection(&ChangeEventRecord {
        workbench_id: request.workbench_id,
        workspace_incarnation_id: workspace.incarnation_id,
        kind: ChangeEventKind::ArtifactPublished,
        artifact_revision_id: Some(source.artifact_revision_id),
        commit_id: None,
        operation_id: None,
        path: Some(request.destination),
        before: TypedProjection::empty(),
        after: projection,
    })?;
    let deterministic_result = encode_rename_result(
        input_digest,
        workspace_revision,
        source.generation,
        source.artifact_revision_id,
    );
    let command = MetadataCommand {
        schema_id: SCHEMA_ID.to_owned(),
        root_id: request.context.root_id,
        logical_shard_id: request.context.logical_shard_id,
        object_namespace_id: Some(request.context.object_namespace_id),
        placement_generation: request.context.placement_generation,
        owner_epoch: request.context.owner_epoch,
        request_id: request.context.request_id,
        command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
        read_version: request.context.read_version,
        root_fence_action: RootFenceAction::RequireActive,
        predicates,
        mutations,
        history_projection,
        event_projection: vec![removed, published],
        deterministic_result,
    }
    .seal();

    let executed = match store.execute(&command) {
        Ok(executed) => executed,
        Err(
            MetaError::PredicateFailed
            | MetaError::WriteConflict
            | MetaError::WriteReadVersionMismatch { .. },
        ) => return Err(RenamePathError::ConcurrentMutation),
        Err(error) => return Err(RenamePathError::Meta(error)),
    };
    let result = decode_rename_result(&executed.deterministic_result, input_digest)?;
    Ok(RenamePathOutcome {
        workspace_revision: result.workspace_revision,
        generation: result.generation,
        artifact_revision_id: result.artifact_revision_id,
        commit_version: executed.commit_version,
        replayed: executed.replayed,
    })
}

#[derive(Clone, Copy)]
struct DecodedRenameResult {
    workspace_revision: WorkspaceRevision,
    generation: Generation,
    artifact_revision_id: ArtifactRevisionId,
}

fn read_current(
    store: &MetaShard,
    context: RootWriteContext,
    family: MetadataFamily,
    key: &[u8],
) -> Result<Option<Vec<u8>>, RenamePathError> {
    store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            family,
            key,
            context.read_version,
        )
        .map_err(Into::into)
}

fn replace(
    predicates: &mut Vec<CommandPredicate>,
    mutations: &mut Vec<CommandMutation>,
    history: &mut Vec<HistoryProjection>,
    family: MetadataFamily,
    key: Vec<u8>,
    expected: Vec<u8>,
    value: Vec<u8>,
) {
    predicates.push(CommandPredicate::Value {
        family,
        key: key.clone(),
        expected: Some(expected),
    });
    mutations.push(CommandMutation::Put {
        family,
        key: key.clone(),
        value,
    });
    history.push(HistoryProjection { family, key });
}

fn delete(
    predicates: &mut Vec<CommandPredicate>,
    mutations: &mut Vec<CommandMutation>,
    history: &mut Vec<HistoryProjection>,
    family: MetadataFamily,
    key: Vec<u8>,
    expected: Vec<u8>,
) {
    predicates.push(CommandPredicate::Value {
        family,
        key: key.clone(),
        expected: Some(expected),
    });
    mutations.push(CommandMutation::Delete {
        family,
        key: key.clone(),
    });
    history.push(HistoryProjection { family, key });
}

fn put_absent(
    predicates: &mut Vec<CommandPredicate>,
    mutations: &mut Vec<CommandMutation>,
    family: MetadataFamily,
    key: Vec<u8>,
    value: Vec<u8>,
) {
    predicates.push(CommandPredicate::Value {
        family,
        key: key.clone(),
        expected: None,
    });
    mutations.push(CommandMutation::Put {
        family,
        key: key.clone(),
        value,
    });
}

fn rename_input_digest(request: &RenamePathRequest) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.rename-path.input.v1\0");
    hasher.update(request.context.root_id.as_bytes());
    hash_bytes(&mut hasher, request.workbench_id.as_str().as_bytes());
    hash_bytes(&mut hasher, request.source.as_str().as_bytes());
    hash_bytes(&mut hasher, request.destination.as_str().as_bytes());
    hasher.update(request.expected_generation.get().to_be_bytes());
    hasher.finalize().into()
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn encode_rename_result(
    input_digest: [u8; SHA256_BYTES],
    workspace_revision: WorkspaceRevision,
    generation: Generation,
    artifact_revision_id: ArtifactRevisionId,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + SHA256_BYTES + 8 + 8 + FIXED_ID_BYTES);
    encoded.push(RENAME_RESULT_VERSION);
    encoded.extend_from_slice(&input_digest);
    encoded.extend_from_slice(&workspace_revision.get().to_be_bytes());
    encoded.extend_from_slice(&generation.get().to_be_bytes());
    encoded.extend_from_slice(artifact_revision_id.as_bytes());
    encoded
}

fn decode_rename_result(
    encoded: &[u8],
    expected_input_digest: [u8; SHA256_BYTES],
) -> Result<DecodedRenameResult, RenamePathError> {
    let expected_length = 1 + SHA256_BYTES + 8 + 8 + FIXED_ID_BYTES;
    if encoded.len() != expected_length {
        return Err(RenamePathError::DeterministicResultMismatch {
            reason: format!(
                "expected {expected_length} result bytes, found {}",
                encoded.len()
            ),
        });
    }
    if encoded[0] != RENAME_RESULT_VERSION {
        return Err(RenamePathError::DeterministicResultMismatch {
            reason: format!(
                "unsupported result version {}, expected {RENAME_RESULT_VERSION}",
                encoded[0]
            ),
        });
    }
    if encoded[1..1 + SHA256_BYTES] != expected_input_digest {
        return Err(RenamePathError::RequestInputMismatch);
    }
    let workspace_offset = 1 + SHA256_BYTES;
    let generation_offset = workspace_offset + 8;
    let revision_offset = generation_offset + 8;
    let workspace_revision = WorkspaceRevision::new(u64::from_be_bytes(
        encoded[workspace_offset..generation_offset]
            .try_into()
            .expect("checked result length"),
    ));
    let generation = Generation::new(u64::from_be_bytes(
        encoded[generation_offset..revision_offset]
            .try_into()
            .expect("checked result length"),
    ))
    .map_err(|error| RenamePathError::DeterministicResultMismatch {
        reason: error.to_string(),
    })?;
    let artifact_revision_id = ArtifactRevisionId::from_bytes(
        encoded[revision_offset..]
            .try_into()
            .expect("checked result length"),
    );
    Ok(DecodedRenameResult {
        workspace_revision,
        generation,
        artifact_revision_id,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nokv_types::{
        CommandDigest, LogicalShardId, ObjectNamespaceId, OwnerEpoch, PlacementGeneration,
        ReferenceEpoch, RequestId, RevisionState, RootActivationState, RootId,
        WorkspaceIncarnationId, WorkspaceState, FIXED_ID_BYTES, SHA256_BYTES,
    };

    use super::*;
    use crate::workspace::{
        artifact_revision_key, create_visible_workspace, get_visible_path_at,
        get_visible_workspace_at, path_current_key, path_revision_ref_key, read_changes_at,
        secondary_index_key, workspace_current_key, ArtifactRevisionRecord, ChangeEventKind,
        ChangePageRequest, CommandMutation, CommandPredicate, HistoryProjection, MetadataCommand,
        MetadataFamily, PathEntry, QueryFieldId, QueryScalar, QueryScope, RevisionRefRecord,
        RootFenceAction, RootReadContext, SecondaryIndexRecord, TypedProjection, WorkspaceRecord,
        SCHEMA_ID,
    };

    fn root() -> RootId {
        RootId::from_bytes([1; FIXED_ID_BYTES])
    }

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([2; FIXED_ID_BYTES])
    }

    fn owner() -> OwnerEpoch {
        OwnerEpoch::new(3).unwrap()
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(4).unwrap()
    }

    fn request(fill: u8) -> RequestId {
        RequestId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn workbench() -> WorkbenchId {
        WorkbenchId::new("run-42").unwrap()
    }

    fn incarnation() -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([5; FIXED_ID_BYTES])
    }

    fn source() -> NormalizedRelativePath {
        NormalizedRelativePath::new("outputs/source.json").unwrap()
    }

    fn destination() -> NormalizedRelativePath {
        NormalizedRelativePath::new("outputs/destination.json").unwrap()
    }

    fn revision() -> ArtifactRevisionId {
        ArtifactRevisionId::from_bytes([6; FIXED_ID_BYTES])
    }

    fn write_context(store: &MetaShard, request_id: RequestId) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement(),
            owner(),
            request_id,
        )
        .unwrap()
    }

    fn read_context(store: &MetaShard) -> RootReadContext {
        RootReadContext::current(store, root(), placement(), owner()).unwrap()
    }

    fn command(
        store: &MetaShard,
        request_id: RequestId,
        action: RootFenceAction,
        predicates: Vec<CommandPredicate>,
        mutations: Vec<CommandMutation>,
    ) -> MetadataCommand {
        let history_projection = predicates
            .iter()
            .filter_map(|predicate| match predicate {
                CommandPredicate::Value {
                    family,
                    key,
                    expected: Some(_),
                } => Some(HistoryProjection {
                    family: *family,
                    key: key.clone(),
                }),
                _ => None,
            })
            .collect();
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES])),
            placement_generation: placement(),
            owner_epoch: owner(),
            request_id,
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: action,
            predicates,
            mutations,
            history_projection,
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal()
    }

    fn ready_store() -> (
        MetaShard,
        TypedProjection,
        PathEntry,
        ArtifactRevisionRecord,
    ) {
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&command(
                &store,
                request(1),
                RootFenceAction::Install,
                Vec::new(),
                Vec::new(),
            ))
            .unwrap();
        store
            .execute(&command(
                &store,
                request(2),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
                Vec::new(),
                Vec::new(),
            ))
            .unwrap();
        create_visible_workspace(
            &store,
            write_context(&store, request(3)),
            &workbench(),
            incarnation(),
        )
        .unwrap();

        let projection = TypedProjection::new(BTreeMap::from([
            (
                QueryFieldId::new("artifact.stage").unwrap(),
                QueryScalar::String("complete".to_owned()),
            ),
            (
                QueryFieldId::new("artifact.score").unwrap(),
                QueryScalar::Unsigned(9),
            ),
        ]))
        .unwrap();
        let path = PathEntry {
            generation: Generation::new(7).unwrap(),
            artifact_revision_id: revision(),
            body_digest_uri: format!("sha256:{}", "11".repeat(32)),
            manifest_digest_uri: format!("sha256:{}", "22".repeat(32)),
            logical_size: 5,
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/json".to_owned(),
            producer: Some("test".to_owned()),
            manifest_id: None,
            typed_index_projection: projection.encode().unwrap(),
        };
        let revision_record = ArtifactRevisionRecord {
            logical_size: 5,
            body_digest_uri: path.body_digest_uri.clone(),
            manifest_digest_uri: path.manifest_digest_uri.clone(),
            block_count: 1,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: [0; SHA256_BYTES],
            content_type: path.content_type.clone(),
            state: RevisionState::Available,
            reference_epoch: ReferenceEpoch::new(5),
            strong_reference_count: 3,
            last_zero_ref_version: Some(CommitVersion::new(2).unwrap()),
        };
        let reference = RevisionRefRecord {
            reference_epoch_at_add: ReferenceEpoch::new(4),
        };
        let index = SecondaryIndexRecord {
            path_generation: path.generation,
            compact_projection: projection.clone(),
        }
        .encode()
        .unwrap();
        let mut rows = vec![
            (
                MetadataFamily::PathCurrent,
                path_current_key(root(), incarnation(), &source()),
                path.encode().unwrap(),
            ),
            (
                MetadataFamily::ArtifactRevision,
                artifact_revision_key(root(), revision()),
                revision_record.encode().unwrap(),
            ),
            (
                MetadataFamily::RevisionRef,
                path_revision_ref_key(root(), incarnation(), &source(), revision()),
                reference.encode().unwrap(),
            ),
        ];
        for (field, scalar) in projection.fields() {
            rows.push((
                MetadataFamily::SecondaryIndex,
                secondary_index_key(root(), field, scalar, incarnation(), &source()),
                index.clone(),
            ));
        }
        let predicates = rows
            .iter()
            .map(|(family, key, _)| CommandPredicate::Value {
                family: *family,
                key: key.clone(),
                expected: None,
            })
            .collect();
        let mutations = rows
            .into_iter()
            .map(|(family, key, value)| CommandMutation::Put { family, key, value })
            .collect();
        store
            .execute(&command(
                &store,
                request(4),
                RootFenceAction::RequireActive,
                predicates,
                mutations,
            ))
            .unwrap();
        (store, projection, path, revision_record)
    }

    fn rename_request(store: &MetaShard, request_id: RequestId) -> RenamePathRequest {
        RenamePathRequest {
            context: write_context(store, request_id),
            workbench_id: workbench(),
            source: source(),
            destination: destination(),
            expected_generation: Generation::new(7).unwrap(),
        }
    }

    fn read(store: &MetaShard, family: MetadataFamily, key: &[u8]) -> Option<Vec<u8>> {
        let context = read_context(store);
        store
            .read_at(
                root(),
                placement(),
                owner(),
                family,
                key,
                context.read_version,
            )
            .unwrap()
    }

    #[test]
    fn rename_atomically_moves_namespace_reference_indexes_and_events_without_revision_churn() {
        let (store, projection, path, revision_record) = ready_store();
        let request = rename_request(&store, request(5));
        let outcome = rename_path(&store, request.clone()).unwrap();
        assert!(!outcome.replayed);
        assert_eq!(outcome.workspace_revision, WorkspaceRevision::new(1));
        assert_eq!(outcome.generation, path.generation);
        assert_eq!(outcome.artifact_revision_id, revision());

        let replay = rename_path(&store, request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, outcome.commit_version);
        assert_eq!(replay.workspace_revision, outcome.workspace_revision);

        let context = read_context(&store);
        assert_eq!(
            get_visible_path_at(&store, context, &workbench(), &source()).unwrap(),
            None
        );
        assert_eq!(
            get_visible_path_at(&store, context, &workbench(), &destination()).unwrap(),
            Some(path.clone())
        );
        assert_eq!(
            get_visible_workspace_at(&store, context, &workbench())
                .unwrap()
                .unwrap()
                .workspace_revision,
            WorkspaceRevision::new(1)
        );

        let source_ref = path_revision_ref_key(root(), incarnation(), &source(), revision());
        let destination_ref =
            path_revision_ref_key(root(), incarnation(), &destination(), revision());
        assert_eq!(read(&store, MetadataFamily::RevisionRef, &source_ref), None);
        assert_eq!(
            RevisionRefRecord::decode(
                &read(&store, MetadataFamily::RevisionRef, &destination_ref).unwrap()
            )
            .unwrap(),
            RevisionRefRecord {
                reference_epoch_at_add: ReferenceEpoch::new(4),
            }
        );
        assert_eq!(
            ArtifactRevisionRecord::decode(
                &read(
                    &store,
                    MetadataFamily::ArtifactRevision,
                    &artifact_revision_key(root(), revision()),
                )
                .unwrap()
            )
            .unwrap(),
            revision_record
        );

        for (field, scalar) in projection.fields() {
            assert_eq!(
                read(
                    &store,
                    MetadataFamily::SecondaryIndex,
                    &secondary_index_key(root(), field, scalar, incarnation(), &source()),
                ),
                None
            );
            let moved = read(
                &store,
                MetadataFamily::SecondaryIndex,
                &secondary_index_key(root(), field, scalar, incarnation(), &destination()),
            )
            .unwrap();
            assert_eq!(
                SecondaryIndexRecord::decode(&moved).unwrap(),
                SecondaryIndexRecord {
                    path_generation: path.generation,
                    compact_projection: projection.clone(),
                }
            );
        }

        let changes = read_changes_at(
            &store,
            context,
            &ChangePageRequest {
                scope: QueryScope::Workspace(workbench()),
                after_commit_version: Some(
                    CommitVersion::new(outcome.commit_version.get() - 1).unwrap(),
                ),
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(changes.events.len(), 2);
        assert_eq!(changes.events[0].commit_version, outcome.commit_version);
        assert_eq!(changes.events[1].commit_version, outcome.commit_version);
        assert_eq!(changes.events[0].event.kind, ChangeEventKind::PathRemoved);
        assert_eq!(changes.events[0].event.path.as_ref(), Some(&source()));
        assert_eq!(
            changes.events[1].event.kind,
            ChangeEventKind::ArtifactPublished
        );
        assert_eq!(changes.events[1].event.path.as_ref(), Some(&destination()));
    }

    fn put_destination(store: &MetaShard, path: &PathEntry) {
        let key = path_current_key(root(), incarnation(), &destination());
        store
            .execute(&command(
                store,
                request(20),
                RootFenceAction::RequireActive,
                vec![CommandPredicate::Value {
                    family: MetadataFamily::PathCurrent,
                    key: key.clone(),
                    expected: None,
                }],
                vec![CommandMutation::Put {
                    family: MetadataFamily::PathCurrent,
                    key,
                    value: path.encode().unwrap(),
                }],
            ))
            .unwrap();
    }

    #[test]
    fn rename_conflicts_are_typed_and_do_not_partially_mutate() {
        let (store, _, path, _) = ready_store();
        let mut missing = rename_request(&store, request(5));
        missing.source = NormalizedRelativePath::new("outputs/missing.bin").unwrap();
        assert_eq!(
            rename_path(&store, missing),
            Err(RenamePathError::SourceNotFound)
        );

        let mut wrong_generation = rename_request(&store, request(6));
        wrong_generation.expected_generation = Generation::new(8).unwrap();
        assert_eq!(
            rename_path(&store, wrong_generation),
            Err(RenamePathError::GenerationMismatch {
                expected: 8,
                actual: 7,
            })
        );

        put_destination(&store, &path);
        assert_eq!(
            rename_path(&store, rename_request(&store, request(7))),
            Err(RenamePathError::DestinationAlreadyExists)
        );
        assert!(
            get_visible_path_at(&store, read_context(&store), &workbench(), &source())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn same_and_reserved_paths_fail_before_metadata_access() {
        let (store, _, _, _) = ready_store();
        let mut same = rename_request(&store, request(5));
        same.destination = same.source.clone();
        assert_eq!(rename_path(&store, same), Err(RenamePathError::SamePath));

        for (source_path, destination_path) in [
            ("metadata/run_manifest.json", "outputs/a.bin"),
            ("outputs/a.bin", "metadata/restore_manifest.json"),
        ] {
            let mut reserved = rename_request(&store, request(6));
            reserved.source = NormalizedRelativePath::new(source_path).unwrap();
            reserved.destination = NormalizedRelativePath::new(destination_path).unwrap();
            assert_eq!(
                rename_path(&store, reserved),
                Err(RenamePathError::ReservedManifest)
            );
        }
    }

    fn set_workspace_state(store: &MetaShard, state: WorkspaceState) {
        let key = workspace_current_key(root(), &workbench());
        let current = read(store, MetadataFamily::WorkspaceCurrent, &key).unwrap();
        let mut workspace = WorkspaceRecord::decode(&current).unwrap();
        workspace.state = state;
        store
            .execute(&command(
                store,
                request(30 + u8::from(state)),
                RootFenceAction::RequireActive,
                vec![CommandPredicate::Value {
                    family: MetadataFamily::WorkspaceCurrent,
                    key: key.clone(),
                    expected: Some(current),
                }],
                vec![CommandMutation::Put {
                    family: MetadataFamily::WorkspaceCurrent,
                    key,
                    value: workspace.encode().unwrap(),
                }],
            ))
            .unwrap();
    }

    #[test]
    fn staging_and_retired_workspaces_reject_path_mutation() {
        for state in [WorkspaceState::Staging, WorkspaceState::Retired] {
            let (store, _, _, _) = ready_store();
            set_workspace_state(&store, state);
            assert_eq!(
                rename_path(&store, rename_request(&store, request(40))),
                Err(RenamePathError::WorkspaceUnavailable)
            );
        }
    }

    #[test]
    fn request_id_reuse_with_different_inputs_fails_exactly() {
        let (store, _, _, _) = ready_store();
        let request = rename_request(&store, request(5));
        rename_path(&store, request.clone()).unwrap();
        let mut mismatch = request;
        mismatch.destination = NormalizedRelativePath::new("outputs/other.json").unwrap();
        assert_eq!(
            rename_path(&store, mismatch),
            Err(RenamePathError::RequestInputMismatch)
        );
    }
}
