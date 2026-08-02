/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use nokv_meta::workspace::{
    artifact_revision_key, create_visible_workspace, dependency_owner_digest, path_current_key,
    path_revision_ref_key, workspace_current_key, AgentMetadataStore, ArtifactRevisionRecord,
    CommandMutation, CommandPredicate, HistoryProjection, MetadataCommand, MetadataFamily,
    PathEntry, RevisionRefRecord, RootFenceAction, RootWriteContext, TypedProjection,
    WorkspaceRecord, SCHEMA_ID,
};
use nokv_protocol::{LogicalShardIdentity, RootIdentity, RootRoute, WorkbenchName};
use nokv_server::MetadataWorkspaceRequestExecutor;
use nokv_types::{
    ArtifactRevisionId, CommandDigest, Generation, LogicalShardId, NormalizedRelativePath,
    OwnerEpoch, PlacementGeneration, ReferenceEpoch, RequestId, RevisionState, RootActivationState,
    RootId, WorkbenchId, WorkspaceIncarnationId, WorkspaceRevision, FIXED_ID_BYTES, SHA256_BYTES,
};

use super::options::MetadataOptions;

pub(super) const EMPTY_SHA256_URI: &str =
    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
// Each path installs PathCurrent, ArtifactRevision, and its strong RevisionRef.
// Predicates and mutations both count against the engine's 256-item bound.
pub(super) const SEED_BATCH_SIZE: usize = 40;
pub(super) const MAX_FIXTURE_PATHS: usize = 1_000_000;

pub(super) struct Harness {
    pub(super) executor: MetadataWorkspaceRequestExecutor,
    pub(super) route: RootRoute,
    pub(super) workbench: WorkbenchName,
    pub(super) direct_children: usize,
    pub(super) seed: u64,
    pub(super) fixture_paths: Vec<String>,
    pub(super) workspace_revision: u64,
}

impl Harness {
    pub(super) fn new(options: &MetadataOptions) -> Result<Self, String> {
        let fixture_paths = fixture_paths(options)?;
        let workspace_revision = u64::try_from(fixture_paths.len())
            .map_err(|_| "fixture path count does not fit u64".to_owned())?;
        let identity = Identities::new(options.seed)?;
        let store = match &options.metadata_dir {
            Some(path) => AgentMetadataStore::create_file(path, identity.shard),
            None => AgentMetadataStore::open_memory(identity.shard),
        }
        .map_err(|error| error.to_string())?;
        store
            .advance_owner_epoch(None, identity.owner)
            .map_err(|error| error.to_string())?;
        execute_fence(
            &store,
            &identity,
            request_id(options.seed, 1),
            RootFenceAction::Install,
        )?;
        execute_fence(
            &store,
            &identity,
            request_id(options.seed, 2),
            RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
        )?;

        let workbench_id = WorkbenchId::new("benchmark").map_err(|error| error.to_string())?;
        let workspace_incarnation = fixed_id(options.seed, 3);
        let created = create_visible_workspace(
            &store,
            RootWriteContext::current(
                &store,
                identity.root,
                identity.shard,
                identity.placement,
                identity.owner,
                request_id(options.seed, 3),
            )
            .map_err(|error| error.to_string())?,
            &workbench_id,
            WorkspaceIncarnationId::from_bytes(workspace_incarnation),
        )
        .map_err(|error| error.to_string())?;

        let next_mutation_sequence = seed_paths(
            &store,
            &identity,
            WorkspaceIncarnationId::from_bytes(workspace_incarnation),
            &fixture_paths,
            options.seed,
        )?;
        finish_fixture_workspace(
            &store,
            &identity,
            &workbench_id,
            created.workspace,
            WorkspaceRevision::new(workspace_revision),
            request_id(options.seed, next_mutation_sequence),
        )?;

        let route = RootRoute {
            root_id: RootIdentity(*identity.root.as_bytes()),
            logical_shard_id: LogicalShardIdentity(*identity.shard.as_bytes()),
            placement_generation: identity.placement.get(),
            owner_epoch: identity.owner.get(),
        };
        Ok(Self {
            executor: MetadataWorkspaceRequestExecutor::new(Arc::new(store)),
            route,
            workbench: WorkbenchName::new(workbench_id.as_str())
                .map_err(|error| error.to_string())?,
            direct_children: options.direct_children,
            seed: options.seed,
            fixture_paths,
            workspace_revision,
        })
    }
}

#[derive(Clone, Copy)]
struct Identities {
    shard: LogicalShardId,
    root: RootId,
    placement: PlacementGeneration,
    owner: OwnerEpoch,
}

impl Identities {
    fn new(seed: u64) -> Result<Self, String> {
        Ok(Self {
            shard: LogicalShardId::from_bytes(fixed_id(seed, 1)),
            root: RootId::from_bytes(fixed_id(seed, 2)),
            placement: PlacementGeneration::new(1).map_err(|error| error.to_string())?,
            owner: OwnerEpoch::new(1).map_err(|error| error.to_string())?,
        })
    }
}

fn execute_fence(
    store: &AgentMetadataStore,
    identity: &Identities,
    request_id: RequestId,
    action: RootFenceAction,
) -> Result<(), String> {
    store
        .execute(
            &MetadataCommand {
                schema_id: SCHEMA_ID.to_owned(),
                root_id: identity.root,
                logical_shard_id: identity.shard,
                placement_generation: identity.placement,
                owner_epoch: identity.owner,
                request_id,
                command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                read_version: store
                    .current_read_version()
                    .map_err(|error| error.to_string())?,
                root_fence_action: action,
                predicates: Vec::new(),
                mutations: Vec::new(),
                history_projection: Vec::new(),
                event_projection: Vec::new(),
                deterministic_result: Vec::new(),
            }
            .seal(),
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn finish_fixture_workspace(
    store: &AgentMetadataStore,
    identity: &Identities,
    workbench_id: &WorkbenchId,
    previous: WorkspaceRecord,
    workspace_revision: WorkspaceRevision,
    request_id: RequestId,
) -> Result<(), String> {
    let next = WorkspaceRecord {
        workspace_revision,
        ..previous
    };
    let key = workspace_current_key(identity.root, workbench_id);
    let previous_payload = previous.encode().map_err(|error| error.to_string())?;
    let next_payload = next.encode().map_err(|error| error.to_string())?;
    let context = RootWriteContext::current(
        store,
        identity.root,
        identity.shard,
        identity.placement,
        identity.owner,
        request_id,
    )
    .map_err(|error| error.to_string())?;
    store
        .execute(
            &MetadataCommand {
                schema_id: SCHEMA_ID.to_owned(),
                root_id: context.root_id,
                logical_shard_id: context.logical_shard_id,
                placement_generation: context.placement_generation,
                owner_epoch: context.owner_epoch,
                request_id: context.request_id,
                command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                read_version: context.read_version,
                root_fence_action: RootFenceAction::RequireActive,
                predicates: vec![CommandPredicate::Value {
                    family: MetadataFamily::WorkspaceCurrent,
                    key: key.clone(),
                    expected: Some(previous_payload),
                }],
                mutations: vec![CommandMutation::Put {
                    family: MetadataFamily::WorkspaceCurrent,
                    key: key.clone(),
                    value: next_payload.clone(),
                }],
                history_projection: vec![HistoryProjection {
                    family: MetadataFamily::WorkspaceCurrent,
                    key,
                }],
                event_projection: Vec::new(),
                deterministic_result: next_payload,
            }
            .seal(),
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn seed_paths(
    store: &AgentMetadataStore,
    identity: &Identities,
    workspace_incarnation: WorkspaceIncarnationId,
    paths: &[String],
    seed: u64,
) -> Result<u64, String> {
    let projection = TypedProjection::empty()
        .encode()
        .map_err(|error| error.to_string())?;
    let dependency_digest = dependency_owner_digest(&[]).map_err(|error| error.to_string())?;
    let mut request_sequence = 4_u64;
    for (batch_index, chunk) in paths.chunks(SEED_BATCH_SIZE).enumerate() {
        let context = RootWriteContext::current(
            store,
            identity.root,
            identity.shard,
            identity.placement,
            identity.owner,
            request_id(seed, request_sequence),
        )
        .map_err(|error| error.to_string())?;
        request_sequence = request_sequence
            .checked_add(1)
            .ok_or_else(|| "fixture request sequence overflowed u64".to_owned())?;
        let row_capacity = chunk
            .len()
            .checked_mul(3)
            .ok_or_else(|| "fixture batch row count overflowed usize".to_owned())?;
        let mut predicates = Vec::with_capacity(row_capacity);
        let mut mutations = Vec::with_capacity(row_capacity);
        for (offset, value) in chunk.iter().enumerate() {
            let index = batch_index
                .saturating_mul(SEED_BATCH_SIZE)
                .saturating_add(offset)
                .saturating_add(1);
            let path =
                NormalizedRelativePath::new(value.clone()).map_err(|error| error.to_string())?;
            let index = u64::try_from(index)
                .map_err(|_| "fixture artifact index does not fit u64".to_owned())?;
            let revision = ArtifactRevisionId::from_bytes(fixed_id(seed, index));
            let body_digest_uri = EMPTY_SHA256_URI.to_owned();
            let manifest_digest_uri = EMPTY_SHA256_URI.to_owned();
            let entry = PathEntry {
                generation: Generation::new(index).map_err(|error| error.to_string())?,
                artifact_revision_id: revision,
                body_digest_uri: body_digest_uri.clone(),
                manifest_digest_uri: manifest_digest_uri.clone(),
                logical_size: 0,
                dependency_count: 0,
                dependency_depth: 0,
                content_type: "application/octet-stream".to_owned(),
                producer: Some("nokv-bench".to_owned()),
                manifest_id: Some(format!("bench-{index}")),
                typed_index_projection: projection.clone(),
            };
            let revision_record = ArtifactRevisionRecord {
                logical_size: 0,
                body_digest_uri,
                manifest_digest_uri,
                block_count: 0,
                dependency_count: 0,
                dependency_depth: 0,
                dependency_digest,
                content_type: "application/octet-stream".to_owned(),
                state: RevisionState::Available,
                reference_epoch: ReferenceEpoch::new(1),
                strong_reference_count: 1,
                last_zero_ref_version: None,
            };
            let revision_ref = RevisionRefRecord {
                reference_epoch_at_add: ReferenceEpoch::new(1),
            };
            for (family, key, value) in [
                (
                    MetadataFamily::PathCurrent,
                    path_current_key(identity.root, workspace_incarnation, &path),
                    entry.encode().map_err(|error| error.to_string())?,
                ),
                (
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(identity.root, revision),
                    revision_record
                        .encode()
                        .map_err(|error| error.to_string())?,
                ),
                (
                    MetadataFamily::RevisionRef,
                    path_revision_ref_key(identity.root, workspace_incarnation, &path, revision),
                    revision_ref.encode().map_err(|error| error.to_string())?,
                ),
            ] {
                predicates.push(CommandPredicate::Value {
                    family,
                    key: key.clone(),
                    expected: None,
                });
                mutations.push(CommandMutation::Put { family, key, value });
            }
        }
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: context.root_id,
                    logical_shard_id: context.logical_shard_id,
                    placement_generation: context.placement_generation,
                    owner_epoch: context.owner_epoch,
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates,
                    mutations,
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: b"nokv-bench-seed".to_vec(),
                }
                .seal(),
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(request_sequence)
}

pub(super) fn fixture_path_count(options: &MetadataOptions) -> Result<usize, String> {
    let per_child = options
        .leaves_per_child
        .checked_add(1)
        .ok_or_else(|| "fixture path count overflows usize".to_owned())?;
    let descendants = options
        .direct_children
        .checked_mul(per_child)
        .ok_or_else(|| "fixture path count overflows usize".to_owned())?;
    let path_count = descendants
        .checked_add(1)
        .ok_or_else(|| "fixture path count overflows usize".to_owned())?;
    if path_count > MAX_FIXTURE_PATHS {
        return Err(format!(
            "fixture contains {path_count} paths; maximum is {MAX_FIXTURE_PATHS}"
        ));
    }
    Ok(path_count)
}

fn fixture_paths(options: &MetadataOptions) -> Result<Vec<String>, String> {
    let path_count = fixture_path_count(options)?;
    let mut paths = Vec::with_capacity(path_count);
    paths.push("outputs/hot".to_owned());
    for child in 0..options.direct_children {
        paths.push(format!("outputs/hot/child-{child:04}"));
        for leaf in 0..options.leaves_per_child {
            paths.push(format!("outputs/hot/child-{child:04}/deep/{leaf:04}.bin"));
        }
    }
    Ok(paths)
}

pub(super) fn is_direct_fixture_path(path: &str) -> bool {
    path == "outputs/hot"
        || path
            .strip_prefix("outputs/hot/")
            .is_some_and(|suffix| !suffix.contains('/'))
}

fn request_id(seed: u64, sequence: u64) -> RequestId {
    RequestId::from_bytes(fixed_id(seed, sequence))
}

pub(super) fn fixed_id(seed: u64, sequence: u64) -> [u8; FIXED_ID_BYTES] {
    let mut bytes = [0_u8; FIXED_ID_BYTES];
    bytes[..8].copy_from_slice(&seed.to_be_bytes());
    bytes[8..].copy_from_slice(&sequence.to_be_bytes());
    bytes
}
