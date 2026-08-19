/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Atomic removal of one authoritative workspace path.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommandDigest, CommitVersion, GcClaimState, Generation,
    NormalizedRelativePath, ReferenceEpoch, RevisionState, WorkbenchId, WorkspaceRevision,
    WorkspaceState, FIXED_ID_BYTES, SHA256_BYTES,
};
use sha2::{Digest as _, Sha256};

use super::codec::{
    artifact_revision_key, gc_candidate_key, path_current_key, path_revision_ref_key,
    workspace_current_key, SCHEMA_ID,
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
    ArtifactRevisionRecord, GcCandidateRecord, PathEntry, PublicationRecordCodecError,
    RevisionRefRecord, WorkspaceRecord,
};
use super::query_records::{
    secondary_index_key, ChangeEventKind, ChangeEventRecord, QueryRecordError,
    SecondaryIndexRecord, TypedProjection,
};
use super::restore::RESTORE_MANIFEST_PATH;

const REMOVE_RESULT_VERSION: u8 = 1;

/// One generation-fenced path removal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovePathRequest {
    pub context: RootWriteContext,
    pub workbench_id: WorkbenchId,
    pub path: NormalizedRelativePath,
    pub expected_generation: Generation,
}

/// Durable result returned for both the first removal and an exact replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemovePathOutcome {
    pub workspace_revision: WorkspaceRevision,
    pub removed_artifact_revision_id: ArtifactRevisionId,
    pub commit_version: CommitVersion,
    pub replayed: bool,
}

/// Typed failure for atomic path removal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemovePathError {
    Meta(MetaError),
    RecordCodec(PublicationRecordCodecError),
    QueryRecord(QueryRecordError),
    WorkspaceNotFound,
    WorkspaceUnavailable,
    ReservedManifest,
    WorkspaceRevisionOverflow,
    PathNotFound,
    GenerationMismatch { expected: u64, actual: u64 },
    RevisionNotFound { revision: ArtifactRevisionId },
    RevisionUnavailable { revision: ArtifactRevisionId },
    RevisionReferenceMissing,
    RevisionReferenceEpochAhead,
    ReferenceCountUnderflow,
    ReferenceEpochOverflow,
    CommitVersionOverflow,
    ConcurrentMutation,
    RequestInputMismatch,
    DeterministicResultMismatch { reason: String },
}

impl fmt::Display for RemovePathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Meta(error) => error.fmt(formatter),
            Self::RecordCodec(error) => write!(formatter, "metadata record failed: {error}"),
            Self::QueryRecord(error) => write!(formatter, "query record failed: {error}"),
            Self::WorkspaceNotFound => formatter.write_str("workbench does not exist"),
            Self::WorkspaceUnavailable => formatter.write_str("workbench is not visible"),
            Self::ReservedManifest => formatter.write_str(
                "canonical Workbench manifests cannot be removed outside their lifecycle",
            ),
            Self::WorkspaceRevisionOverflow => formatter.write_str("workspace revision overflowed"),
            Self::PathNotFound => formatter.write_str("path does not exist"),
            Self::GenerationMismatch { expected, actual } => write!(
                formatter,
                "path generation mismatch: expected {expected}, actual {actual}"
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
                formatter.write_str("path revision reference is missing")
            }
            Self::RevisionReferenceEpochAhead => {
                formatter.write_str("path revision reference epoch is ahead of its owner")
            }
            Self::ReferenceCountUnderflow => {
                formatter.write_str("artifact revision reference count underflowed")
            }
            Self::ReferenceEpochOverflow => {
                formatter.write_str("artifact revision reference epoch overflowed")
            }
            Self::CommitVersionOverflow => {
                formatter.write_str("metadata commit version overflowed")
            }
            Self::ConcurrentMutation => {
                formatter.write_str("path removal lost a concurrent metadata mutation")
            }
            Self::RequestInputMismatch => {
                formatter.write_str("request id was reused with different path-removal inputs")
            }
            Self::DeterministicResultMismatch { reason } => {
                write!(formatter, "invalid replayed path-removal result: {reason}")
            }
        }
    }
}

impl std::error::Error for RemovePathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Meta(error) => Some(error),
            Self::RecordCodec(error) => Some(error),
            Self::QueryRecord(error) => Some(error),
            _ => None,
        }
    }
}

impl From<MetaError> for RemovePathError {
    fn from(error: MetaError) -> Self {
        Self::Meta(error)
    }
}

impl From<PublicationRecordCodecError> for RemovePathError {
    fn from(error: PublicationRecordCodecError) -> Self {
        Self::RecordCodec(error)
    }
}

impl From<QueryRecordError> for RemovePathError {
    fn from(error: QueryRecordError) -> Self {
        Self::QueryRecord(error)
    }
}

/// Delete one current path and release its strong artifact-revision reference.
///
/// Path visibility, the path reference, the owner revision, a possible GC
/// candidate, all secondary-index rows, the workspace revision, and the change
/// event advance in one bounded metadata command.
pub fn remove_path(
    store: &MetaShard,
    request: RemovePathRequest,
) -> Result<RemovePathOutcome, RemovePathError> {
    if matches!(
        request.path.as_str(),
        RUN_MANIFEST_PATH | RESTORE_MANIFEST_PATH
    ) {
        return Err(RemovePathError::ReservedManifest);
    }
    let input_digest = remove_input_digest(&request);
    if let Some(replay) = store.lookup_request_result(
        request.context.root_id,
        request.context.placement_generation,
        request.context.owner_epoch,
        request.context.request_id,
    )? {
        let result = decode_remove_result(&replay.deterministic_result, input_digest)?;
        return Ok(RemovePathOutcome {
            workspace_revision: result.workspace_revision,
            removed_artifact_revision_id: result.removed_artifact_revision_id,
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
    .ok_or(RemovePathError::WorkspaceNotFound)?;
    let workspace = WorkspaceRecord::decode(&workspace_payload)?;
    if workspace.state != WorkspaceState::Visible || workspace.owning_operation_id.is_some() {
        return Err(RemovePathError::WorkspaceUnavailable);
    }
    let workspace_revision = WorkspaceRevision::new(
        workspace
            .workspace_revision
            .get()
            .checked_add(1)
            .ok_or(RemovePathError::WorkspaceRevisionOverflow)?,
    );
    let next_workspace = WorkspaceRecord {
        workspace_revision,
        ..workspace
    };

    let path_key = path_current_key(
        request.context.root_id,
        workspace.incarnation_id,
        &request.path,
    );
    let path_payload = read_current(
        store,
        request.context,
        MetadataFamily::PathCurrent,
        &path_key,
    )?
    .ok_or(RemovePathError::PathNotFound)?;
    let path = PathEntry::decode(&path_payload)?;
    if path.generation != request.expected_generation {
        return Err(RemovePathError::GenerationMismatch {
            expected: request.expected_generation.get(),
            actual: path.generation.get(),
        });
    }
    let projection = TypedProjection::decode_stored(&path.typed_index_projection)?;

    let revision_key = artifact_revision_key(request.context.root_id, path.artifact_revision_id);
    let revision_payload = read_current(
        store,
        request.context,
        MetadataFamily::ArtifactRevision,
        &revision_key,
    )?
    .ok_or(RemovePathError::RevisionNotFound {
        revision: path.artifact_revision_id,
    })?;
    let revision = ArtifactRevisionRecord::decode(&revision_payload)?;
    if revision.state != RevisionState::Available {
        return Err(RemovePathError::RevisionUnavailable {
            revision: path.artifact_revision_id,
        });
    }

    let reference_key = path_revision_ref_key(
        request.context.root_id,
        workspace.incarnation_id,
        &request.path,
        path.artifact_revision_id,
    );
    let reference_payload = read_current(
        store,
        request.context,
        MetadataFamily::RevisionRef,
        &reference_key,
    )?
    .ok_or(RemovePathError::RevisionReferenceMissing)?;
    let reference = RevisionRefRecord::decode(&reference_payload)?;
    if reference.reference_epoch_at_add > revision.reference_epoch {
        return Err(RemovePathError::RevisionReferenceEpochAhead);
    }

    let next_epoch = ReferenceEpoch::new(
        revision
            .reference_epoch
            .get()
            .checked_add(1)
            .ok_or(RemovePathError::ReferenceEpochOverflow)?,
    );
    let next_reference_count = revision
        .strong_reference_count
        .checked_sub(1)
        .ok_or(RemovePathError::ReferenceCountUnderflow)?;
    let expected_commit_version = CommitVersion::new(
        request
            .context
            .read_version
            .get()
            .checked_add(1)
            .ok_or(RemovePathError::CommitVersionOverflow)?,
    )
    .map_err(|_| RemovePathError::CommitVersionOverflow)?;
    let mut next_revision = revision;
    next_revision.reference_epoch = next_epoch;
    next_revision.strong_reference_count = next_reference_count;
    next_revision.last_zero_ref_version =
        (next_reference_count == 0).then_some(expected_commit_version);

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
        path_key,
        path_payload,
    );
    delete(
        &mut predicates,
        &mut mutations,
        &mut history_projection,
        MetadataFamily::RevisionRef,
        reference_key,
        reference_payload,
    );
    replace(
        &mut predicates,
        &mut mutations,
        &mut history_projection,
        MetadataFamily::ArtifactRevision,
        revision_key,
        revision_payload,
        next_revision.encode()?,
    );

    for (field, scalar) in projection.fields() {
        let key = secondary_index_key(
            request.context.root_id,
            field,
            scalar,
            workspace.incarnation_id,
            &request.path,
        );
        let value = SecondaryIndexRecord {
            path_generation: path.generation,
            compact_projection: projection.clone(),
        }
        .encode()?;
        delete(
            &mut predicates,
            &mut mutations,
            &mut history_projection,
            MetadataFamily::SecondaryIndex,
            key,
            value,
        );
    }

    if next_reference_count == 0 {
        let key = gc_candidate_key(
            request.context.root_id,
            path.artifact_revision_id,
            next_epoch,
        );
        predicates.push(CommandPredicate::Value {
            family: MetadataFamily::GcCandidate,
            key: key.clone(),
            expected: None,
        });
        mutations.push(CommandMutation::Put {
            family: MetadataFamily::GcCandidate,
            key,
            value: GcCandidateRecord {
                last_zero_ref_version: expected_commit_version,
                claim_state: GcClaimState::Candidate,
                retry_count: 0,
                quarantine_evidence: None,
            }
            .encode()?,
        });
    }

    let event = change_event_projection(&ChangeEventRecord {
        workbench_id: request.workbench_id.clone(),
        workspace_incarnation_id: workspace.incarnation_id,
        kind: ChangeEventKind::PathRemoved,
        artifact_revision_id: Some(path.artifact_revision_id),
        commit_id: None,
        operation_id: None,
        path: Some(request.path.clone()),
        before: projection,
        after: TypedProjection::empty(),
    })?;
    let deterministic_result =
        encode_remove_result(input_digest, workspace_revision, path.artifact_revision_id);
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
        event_projection: vec![event],
        deterministic_result,
    }
    .seal();

    let executed = match store.execute(&command) {
        Ok(executed) => executed,
        Err(
            MetaError::PredicateFailed
            | MetaError::WriteConflict
            | MetaError::WriteReadVersionMismatch { .. },
        ) => return Err(RemovePathError::ConcurrentMutation),
        Err(error) => return Err(RemovePathError::Meta(error)),
    };
    let result = decode_remove_result(&executed.deterministic_result, input_digest)?;
    Ok(RemovePathOutcome {
        workspace_revision: result.workspace_revision,
        removed_artifact_revision_id: result.removed_artifact_revision_id,
        commit_version: executed.commit_version,
        replayed: executed.replayed,
    })
}

#[derive(Clone, Copy)]
struct DecodedRemoveResult {
    workspace_revision: WorkspaceRevision,
    removed_artifact_revision_id: ArtifactRevisionId,
}

fn read_current(
    store: &MetaShard,
    context: RootWriteContext,
    family: MetadataFamily,
    key: &[u8],
) -> Result<Option<Vec<u8>>, RemovePathError> {
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

fn remove_input_digest(request: &RemovePathRequest) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.remove-path.input\0");
    hasher.update(request.context.root_id.as_bytes());
    hash_bytes(&mut hasher, request.workbench_id.as_str().as_bytes());
    hash_bytes(&mut hasher, request.path.as_str().as_bytes());
    hasher.update(request.expected_generation.get().to_be_bytes());
    hasher.finalize().into()
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn encode_remove_result(
    input_digest: [u8; SHA256_BYTES],
    workspace_revision: WorkspaceRevision,
    artifact_revision_id: ArtifactRevisionId,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + SHA256_BYTES + 8 + FIXED_ID_BYTES);
    encoded.push(REMOVE_RESULT_VERSION);
    encoded.extend_from_slice(&input_digest);
    encoded.extend_from_slice(&workspace_revision.get().to_be_bytes());
    encoded.extend_from_slice(artifact_revision_id.as_bytes());
    encoded
}

fn decode_remove_result(
    encoded: &[u8],
    expected_input_digest: [u8; SHA256_BYTES],
) -> Result<DecodedRemoveResult, RemovePathError> {
    let expected_length = 1 + SHA256_BYTES + 8 + FIXED_ID_BYTES;
    if encoded.len() != expected_length {
        return Err(RemovePathError::DeterministicResultMismatch {
            reason: format!(
                "expected {expected_length} result bytes, found {}",
                encoded.len()
            ),
        });
    }
    if encoded[0] != REMOVE_RESULT_VERSION {
        return Err(RemovePathError::DeterministicResultMismatch {
            reason: format!(
                "unsupported result version {}, expected {REMOVE_RESULT_VERSION}",
                encoded[0]
            ),
        });
    }
    if encoded[1..1 + SHA256_BYTES] != expected_input_digest {
        return Err(RemovePathError::RequestInputMismatch);
    }
    let revision_offset = 1 + SHA256_BYTES + 8;
    let workspace_revision = WorkspaceRevision::new(u64::from_be_bytes(
        encoded[1 + SHA256_BYTES..revision_offset]
            .try_into()
            .expect("checked result length"),
    ));
    let removed_artifact_revision_id = ArtifactRevisionId::from_bytes(
        encoded[revision_offset..]
            .try_into()
            .expect("checked result length"),
    );
    Ok(DecodedRemoveResult {
        workspace_revision,
        removed_artifact_revision_id,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nokv_types::{
        LogicalShardId, OwnerEpoch, PlacementGeneration, RequestId, RootActivationState, RootId,
        WorkspaceIncarnationId,
    };

    use super::*;
    use crate::workspace::{
        create_visible_workspace, get_visible_path_at, get_visible_workspace_at, read_changes_at,
        ChangePageRequest, QueryFieldId, QueryScalar, QueryScope, RootReadContext,
        SecondaryIndexRecord,
    };

    fn root() -> RootId {
        RootId::from_bytes([1; 16])
    }

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([2; 16])
    }

    fn owner() -> OwnerEpoch {
        OwnerEpoch::new(3).unwrap()
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(4).unwrap()
    }

    fn request(fill: u8) -> RequestId {
        RequestId::from_bytes([fill; 16])
    }

    fn workbench() -> WorkbenchId {
        WorkbenchId::new("run-42").unwrap()
    }

    fn incarnation() -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([5; 16])
    }

    fn path() -> NormalizedRelativePath {
        NormalizedRelativePath::new("outputs/result.json").unwrap()
    }

    fn revision() -> ArtifactRevisionId {
        ArtifactRevisionId::from_bytes([6; 16])
    }

    fn write_context(store: &MetaShard, request_id: RequestId) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            nokv_types::ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement(),
            owner(),
            request_id,
        )
        .unwrap()
    }

    fn fence_command(
        store: &MetaShard,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                [10; FIXED_ID_BYTES],
            )),
            placement_generation: placement(),
            owner_epoch: owner(),
            request_id,
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: action,
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal()
    }

    fn ready_store(reference_count: u64) -> (MetaShard, TypedProjection) {
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&fence_command(&store, request(1), RootFenceAction::Install))
            .unwrap();
        store
            .execute(&fence_command(
                &store,
                request(2),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
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
        let path_record = PathEntry {
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
            body_digest_uri: path_record.body_digest_uri.clone(),
            manifest_digest_uri: format!("sha256:{}", "22".repeat(32)),
            block_count: 1,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: [0; SHA256_BYTES],
            content_type: "application/json".to_owned(),
            state: RevisionState::Available,
            reference_epoch: ReferenceEpoch::new(1),
            strong_reference_count: reference_count,
            last_zero_ref_version: None,
        };
        let index_value = SecondaryIndexRecord {
            path_generation: path_record.generation,
            compact_projection: projection.clone(),
        }
        .encode()
        .unwrap();
        let mut records = vec![
            (
                MetadataFamily::PathCurrent,
                path_current_key(root(), incarnation(), &path()),
                path_record.encode().unwrap(),
            ),
            (
                MetadataFamily::ArtifactRevision,
                artifact_revision_key(root(), revision()),
                revision_record.encode().unwrap(),
            ),
            (
                MetadataFamily::RevisionRef,
                path_revision_ref_key(root(), incarnation(), &path(), revision()),
                RevisionRefRecord {
                    reference_epoch_at_add: ReferenceEpoch::new(1),
                }
                .encode()
                .unwrap(),
            ),
        ];
        for (field, scalar) in projection.fields() {
            records.push((
                MetadataFamily::SecondaryIndex,
                secondary_index_key(root(), field, scalar, incarnation(), &path()),
                index_value.clone(),
            ));
        }
        let context = write_context(&store, request(4));
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: context.root_id,
                    logical_shard_id: context.logical_shard_id,
                    object_namespace_id: Some(context.object_namespace_id),
                    placement_generation: context.placement_generation,
                    owner_epoch: context.owner_epoch,
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: records
                        .iter()
                        .map(|(family, key, _)| CommandPredicate::Value {
                            family: *family,
                            key: key.clone(),
                            expected: None,
                        })
                        .collect(),
                    mutations: records
                        .into_iter()
                        .map(|(family, key, value)| CommandMutation::Put { family, key, value })
                        .collect(),
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: b"seed-path".to_vec(),
                }
                .seal(),
            )
            .unwrap();
        (store, projection)
    }

    fn removal_request(store: &MetaShard, request_id: RequestId) -> RemovePathRequest {
        RemovePathRequest {
            context: write_context(store, request_id),
            workbench_id: workbench(),
            path: path(),
            expected_generation: Generation::new(7).unwrap(),
        }
    }

    fn read_context(store: &MetaShard) -> RootReadContext {
        RootReadContext::current(store, root(), placement(), owner()).unwrap()
    }

    #[test]
    fn removal_is_atomic_and_exact_replay_survives_absence() {
        let (store, projection) = ready_store(1);
        let request = removal_request(&store, request(5));
        let outcome = remove_path(&store, request.clone()).unwrap();
        assert!(!outcome.replayed);
        assert_eq!(outcome.workspace_revision, WorkspaceRevision::new(1));
        assert_eq!(outcome.removed_artifact_revision_id, revision());

        let replay = remove_path(&store, request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, outcome.commit_version);
        assert_eq!(replay.workspace_revision, outcome.workspace_revision);

        let context = read_context(&store);
        assert_eq!(
            get_visible_path_at(&store, context, &workbench(), &path()).unwrap(),
            None
        );
        assert_eq!(
            get_visible_workspace_at(&store, context, &workbench())
                .unwrap()
                .unwrap()
                .workspace_revision,
            WorkspaceRevision::new(1)
        );
        assert_eq!(
            store
                .read_at(
                    root(),
                    placement(),
                    owner(),
                    MetadataFamily::RevisionRef,
                    &path_revision_ref_key(root(), incarnation(), &path(), revision()),
                    context.read_version,
                )
                .unwrap(),
            None
        );
        let revision_record = ArtifactRevisionRecord::decode(
            &store
                .read_at(
                    root(),
                    placement(),
                    owner(),
                    MetadataFamily::ArtifactRevision,
                    &artifact_revision_key(root(), revision()),
                    context.read_version,
                )
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(revision_record.reference_epoch, ReferenceEpoch::new(2));
        assert_eq!(revision_record.strong_reference_count, 0);
        assert_eq!(
            revision_record.last_zero_ref_version,
            Some(outcome.commit_version)
        );
        assert!(store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::GcCandidate,
                &gc_candidate_key(root(), revision(), ReferenceEpoch::new(2)),
                context.read_version,
            )
            .unwrap()
            .is_some());
        for (field, scalar) in projection.fields() {
            assert_eq!(
                store
                    .read_at(
                        root(),
                        placement(),
                        owner(),
                        MetadataFamily::SecondaryIndex,
                        &secondary_index_key(root(), field, scalar, incarnation(), &path()),
                        context.read_version,
                    )
                    .unwrap(),
                None
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
        assert_eq!(changes.events.len(), 1);
        assert_eq!(changes.events[0].event.kind, ChangeEventKind::PathRemoved);
        assert_eq!(
            changes.events[0].event.artifact_revision_id,
            Some(revision())
        );
    }

    #[test]
    fn remove_exact_replay_rejects_a_missing_recovery_binding() {
        let (store, _) = ready_store(1);
        let request_value = removal_request(&store, request(5));
        remove_path(&store, request_value.clone()).unwrap();
        let dedupe = store
            .lookup_request(root(), placement(), owner(), request(5))
            .unwrap()
            .unwrap();
        store
            .replace_recovery_header_for_test(dedupe.recovery_lsn, None)
            .unwrap();

        assert!(matches!(
            remove_path(&store, request_value),
            Err(RemovePathError::Meta(MetaError::CorruptRecord { .. }))
        ));
    }

    #[test]
    fn removal_preserves_a_still_referenced_revision() {
        let (store, _) = ready_store(2);
        let outcome = remove_path(&store, removal_request(&store, request(5))).unwrap();
        let context = read_context(&store);
        let revision_record = ArtifactRevisionRecord::decode(
            &store
                .read_at(
                    root(),
                    placement(),
                    owner(),
                    MetadataFamily::ArtifactRevision,
                    &artifact_revision_key(root(), revision()),
                    context.read_version,
                )
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(revision_record.reference_epoch, ReferenceEpoch::new(2));
        assert_eq!(revision_record.strong_reference_count, 1);
        assert_eq!(revision_record.last_zero_ref_version, None);
        assert_eq!(outcome.workspace_revision, WorkspaceRevision::new(1));
        assert_eq!(
            store
                .read_at(
                    root(),
                    placement(),
                    owner(),
                    MetadataFamily::GcCandidate,
                    &gc_candidate_key(root(), revision(), ReferenceEpoch::new(2)),
                    context.read_version,
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn generation_and_request_identity_are_strict() {
        let (store, _) = ready_store(1);
        let mut wrong_generation = removal_request(&store, request(5));
        wrong_generation.expected_generation = Generation::new(8).unwrap();
        assert_eq!(
            remove_path(&store, wrong_generation),
            Err(RemovePathError::GenerationMismatch {
                expected: 8,
                actual: 7,
            })
        );

        let request = removal_request(&store, request(6));
        remove_path(&store, request.clone()).unwrap();
        let mut mismatched_replay = request;
        mismatched_replay.expected_generation = Generation::new(6).unwrap();
        assert_eq!(
            remove_path(&store, mismatched_replay),
            Err(RemovePathError::RequestInputMismatch)
        );
    }
}
