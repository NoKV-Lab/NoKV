/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Authoritative workspace visibility and path-native namespace reads.
//!
//! Every path operation is gated by a visible `WorkspaceCurrent` marker. The
//! resolved-workspace primitives accept only the exact marker record obtained
//! at the same read context, so callers can reuse one visibility point read
//! across exact lookup or a bounded list page.

use std::collections::BTreeMap;
use std::fmt;

use nokv_types::{
    CommandDigest, CommitVersion, LogicalShardId, NormalizedRelativePath, OwnerEpoch,
    PlacementGeneration, ReadVersion, RequestId, RootId, WorkbenchId, WorkspaceIncarnationId,
    WorkspaceRevision, WorkspaceState, SHA256_BYTES,
};

use super::codec::{
    decode_path_current_key, path_child_prefix, path_current_key, workspace_current_key, SCHEMA_ID,
};
use super::engine::{
    AgentMetadataError, AgentMetadataStore, CommandMutation, CommandPredicate, MetadataCommand,
    MetadataFamily, RootFenceAction,
};
use super::publication_records::{PathEntry, PublicationRecordCodecError, WorkspaceRecord};
use super::query::change_event_projection;
use super::query_records::{
    ChangeEventKind, ChangeEventRecord, QueryFieldId, QueryRecordError, QueryScalar,
    TypedProjection,
};

/// Maximum number of visible paths returned by one namespace page.
///
/// One additional engine item is reserved to determine whether another page
/// exists while staying within the engine's 256-item scan bound.
pub const MAX_VISIBLE_PATH_PAGE_SIZE: usize = 255;

/// Fenced root context for one exact MVCC namespace read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootReadContext {
    pub root_id: RootId,
    pub placement_generation: PlacementGeneration,
    pub owner_epoch: OwnerEpoch,
    pub read_version: ReadVersion,
}

impl RootReadContext {
    /// Capture the store's current durable read version.
    pub fn current(
        store: &AgentMetadataStore,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
    ) -> Result<Self, NamespaceError> {
        Ok(Self {
            root_id,
            placement_generation,
            owner_epoch,
            read_version: store.current_read_version()?,
        })
    }
}

/// Fenced root context for one exact, replayable metadata command.
///
/// Callers retain this value across retries. Rebuilding it at a later read
/// version changes the canonical command and correctly conflicts with reuse of
/// the same request id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootWriteContext {
    pub root_id: RootId,
    pub logical_shard_id: LogicalShardId,
    pub placement_generation: PlacementGeneration,
    pub owner_epoch: OwnerEpoch,
    pub request_id: RequestId,
    pub read_version: ReadVersion,
}

impl RootWriteContext {
    /// Capture the store's current durable read version for a new request.
    pub fn current(
        store: &AgentMetadataStore,
        root_id: RootId,
        logical_shard_id: LogicalShardId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        request_id: RequestId,
    ) -> Result<Self, NamespaceError> {
        Ok(Self {
            root_id,
            logical_shard_id,
            placement_generation,
            owner_epoch,
            request_id,
            read_version: store.current_read_version()?,
        })
    }
}

/// Outcome of creating one immediately-visible workspace marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreateVisibleWorkspaceResult {
    pub workspace: WorkspaceRecord,
    pub commit_version: CommitVersion,
    pub replayed: bool,
}

/// One normalized full path and its authoritative current entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisiblePath {
    pub path: NormalizedRelativePath,
    pub entry: PathEntry,
}

/// One stable ordered page of visible paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisiblePathPage {
    pub entries: Vec<VisiblePath>,
    /// Pass this full path as `start_after` to obtain the next page.
    pub next_marker: Option<NormalizedRelativePath>,
}

impl VisiblePathPage {
    fn empty() -> Self {
        Self {
            entries: Vec::new(),
            next_marker: None,
        }
    }
}

/// Path-native namespace domain failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceError {
    AlreadyExists {
        workbench_id: WorkbenchId,
    },
    InvalidPageLimit {
        requested: usize,
        max: usize,
    },
    CorruptKey {
        family: &'static str,
        reason: String,
    },
    Codec {
        record: &'static str,
        source: PublicationRecordCodecError,
    },
    DeterministicResultMismatch {
        reason: String,
    },
    QueryRecord(QueryRecordError),
    Engine(AgentMetadataError),
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists { workbench_id } => {
                write!(formatter, "workbench {workbench_id} already exists")
            }
            Self::InvalidPageLimit { requested, max } => write!(
                formatter,
                "visible path page limit {requested} is outside 1..={max}"
            ),
            Self::CorruptKey { family, reason } => {
                write!(formatter, "corrupt {family} key: {reason}")
            }
            Self::Codec { record, source } => {
                write!(formatter, "invalid {record} payload: {source}")
            }
            Self::DeterministicResultMismatch { reason } => {
                write!(formatter, "invalid workspace creation result: {reason}")
            }
            Self::QueryRecord(source) => source.fmt(formatter),
            Self::Engine(source) => source.fmt(formatter),
        }
    }
}

impl std::error::Error for NamespaceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec { source, .. } => Some(source),
            Self::QueryRecord(source) => Some(source),
            Self::Engine(source) => Some(source),
            _ => None,
        }
    }
}

impl From<AgentMetadataError> for NamespaceError {
    fn from(source: AgentMetadataError) -> Self {
        Self::Engine(source)
    }
}

impl From<QueryRecordError> for NamespaceError {
    fn from(source: QueryRecordError) -> Self {
        Self::QueryRecord(source)
    }
}

/// Create one workbench name as an immediately-visible, empty workspace.
///
/// The name is guarded by an exact absence predicate. An exact request replay
/// returns and validates the originally persisted typed result.
pub fn create_visible_workspace(
    store: &AgentMetadataStore,
    context: RootWriteContext,
    workbench_id: &WorkbenchId,
    incarnation_id: WorkspaceIncarnationId,
) -> Result<CreateVisibleWorkspaceResult, NamespaceError> {
    let workspace = WorkspaceRecord {
        incarnation_id,
        workspace_revision: WorkspaceRevision::ZERO,
        state: WorkspaceState::Visible,
        owning_operation_id: None,
    };
    let payload = workspace
        .encode()
        .map_err(|source| codec_error("WorkspaceCurrent", source))?;
    let key = workspace_current_key(context.root_id, workbench_id);
    let event = change_event_projection(&ChangeEventRecord {
        workspace_incarnation_id: incarnation_id,
        kind: ChangeEventKind::WorkspaceCreated,
        artifact_revision_id: None,
        commit_id: None,
        operation_id: None,
        path: None,
        before: TypedProjection::empty(),
        after: TypedProjection::new(BTreeMap::from([(
            QueryFieldId::new("workspace.revision")?,
            QueryScalar::Unsigned(WorkspaceRevision::ZERO.get()),
        )]))?,
    })?;
    let command = MetadataCommand {
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
            expected: None,
        }],
        mutations: vec![CommandMutation::Put {
            family: MetadataFamily::WorkspaceCurrent,
            key,
            value: payload.clone(),
        }],
        history_projection: Vec::new(),
        event_projection: vec![event],
        deterministic_result: payload,
    }
    .seal();

    let result = match store.execute(&command) {
        Ok(result) => result,
        Err(AgentMetadataError::PredicateFailed) => {
            return Err(NamespaceError::AlreadyExists {
                workbench_id: workbench_id.clone(),
            })
        }
        Err(source) => return Err(NamespaceError::Engine(source)),
    };
    let decoded = WorkspaceRecord::decode(&result.deterministic_result)
        .map_err(|source| codec_error("CreateVisibleWorkspaceResult", source))?;
    if result.deterministic_result != command.deterministic_result {
        return Err(NamespaceError::DeterministicResultMismatch {
            reason: "persisted result bytes do not match the exact request".to_owned(),
        });
    }
    if decoded != workspace {
        return Err(NamespaceError::DeterministicResultMismatch {
            reason: "persisted workspace record does not match the exact request".to_owned(),
        });
    }
    Ok(CreateVisibleWorkspaceResult {
        workspace: decoded,
        commit_version: result.commit_version,
        replayed: result.replayed,
    })
}

/// Resolve a workbench marker at one exact read version.
///
/// Staging and retired incarnations are deliberately indistinguishable from an
/// absent workbench on every namespace read surface.
pub fn get_visible_workspace_at(
    store: &AgentMetadataStore,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
) -> Result<Option<WorkspaceRecord>, NamespaceError> {
    let key = workspace_current_key(context.root_id, workbench_id);
    let Some(payload) = store.read_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::WorkspaceCurrent,
        &key,
        context.read_version,
    )?
    else {
        return Ok(None);
    };
    let workspace = WorkspaceRecord::decode(&payload)
        .map_err(|source| codec_error("WorkspaceCurrent", source))?;
    Ok((workspace.state == WorkspaceState::Visible).then_some(workspace))
}

/// Read one exact normalized path after resolving its visible workspace marker.
pub fn get_visible_path_at(
    store: &AgentMetadataStore,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
    path: &NormalizedRelativePath,
) -> Result<Option<PathEntry>, NamespaceError> {
    let Some(workspace) = get_visible_workspace_at(store, context, workbench_id)? else {
        return Ok(None);
    };
    get_path_at_visible_workspace(store, context, &workspace, path)
}

/// Read one exact path after the caller resolved `WorkspaceCurrent` at the
/// same read context.
///
/// Keeping this as a separate primitive makes the cold read shape explicit:
/// one marker point read followed by one authoritative path point read. The
/// state check fails closed if a staging or retired record is passed by an
/// internal caller.
pub fn get_path_at_visible_workspace(
    store: &AgentMetadataStore,
    context: RootReadContext,
    workspace: &WorkspaceRecord,
    path: &NormalizedRelativePath,
) -> Result<Option<PathEntry>, NamespaceError> {
    if workspace.state != WorkspaceState::Visible {
        return Ok(None);
    }
    let key = path_current_key(context.root_id, workspace.incarnation_id, path);
    let Some(payload) = store.read_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::PathCurrent,
        &key,
        context.read_version,
    )?
    else {
        return Ok(None);
    };
    PathEntry::decode(&payload)
        .map(Some)
        .map_err(|source| codec_error("PathCurrent", source))
}

/// Scan all normalized full paths in one visible workspace incarnation.
///
/// `start_after` is exclusive. A returned `next_marker` is the last entry in
/// the current page and is safe to pass to the next call at the same read
/// version.
pub fn scan_visible_paths_at(
    store: &AgentMetadataStore,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
    start_after: Option<&NormalizedRelativePath>,
    limit: usize,
) -> Result<VisiblePathPage, NamespaceError> {
    if !(1..=MAX_VISIBLE_PATH_PAGE_SIZE).contains(&limit) {
        return Err(NamespaceError::InvalidPageLimit {
            requested: limit,
            max: MAX_VISIBLE_PATH_PAGE_SIZE,
        });
    }
    let Some(workspace) = get_visible_workspace_at(store, context, workbench_id)? else {
        return Ok(VisiblePathPage::empty());
    };
    scan_paths_at_visible_workspace(store, context, &workspace, None, start_after, limit)
}

/// Scan normalized paths below an optional prefix after `WorkspaceCurrent`
/// has already been resolved at the same read context.
///
/// `path_prefix = Some(path)` selects descendants of `path` using the encoded
/// component delimiter. It deliberately does not include an exact file stored
/// at `path`; callers that expose "exact prefix or descendants" semantics must
/// merge the exact point read after the descendant scan because the exact key
/// sorts after its descendants.
pub fn scan_paths_at_visible_workspace(
    store: &AgentMetadataStore,
    context: RootReadContext,
    workspace: &WorkspaceRecord,
    path_prefix: Option<&NormalizedRelativePath>,
    start_after: Option<&NormalizedRelativePath>,
    limit: usize,
) -> Result<VisiblePathPage, NamespaceError> {
    if workspace.state != WorkspaceState::Visible {
        return Ok(VisiblePathPage::empty());
    }
    if !(1..=MAX_VISIBLE_PATH_PAGE_SIZE).contains(&limit) {
        return Err(NamespaceError::InvalidPageLimit {
            requested: limit,
            max: MAX_VISIBLE_PATH_PAGE_SIZE,
        });
    }
    let prefix = path_child_prefix(context.root_id, workspace.incarnation_id, path_prefix);
    let marker_key =
        start_after.map(|path| path_current_key(context.root_id, workspace.incarnation_id, path));
    let scanned = store.scan_prefix_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::PathCurrent,
        &prefix,
        context.read_version,
        marker_key.as_deref(),
        limit + 1,
    )?;

    let mut entries = Vec::with_capacity(scanned.len().min(limit));
    for item in scanned {
        let path = decode_path_current_key(context.root_id, workspace.incarnation_id, &item.key)
            .ok_or_else(|| NamespaceError::CorruptKey {
                family: "PathCurrent",
                reason: "key is not the canonical root/workspace/path encoding".to_owned(),
            })?;
        let entry =
            PathEntry::decode(&item.value).map_err(|source| codec_error("PathCurrent", source))?;
        entries.push(VisiblePath { path, entry });
    }
    let has_more = entries.len() > limit;
    if has_more {
        entries.truncate(limit);
    }
    let next_marker = has_more.then(|| {
        entries
            .last()
            .expect("a page with a lookahead item has one returned entry")
            .path
            .clone()
    });
    Ok(VisiblePathPage {
        entries,
        next_marker,
    })
}

fn codec_error(record: &'static str, source: PublicationRecordCodecError) -> NamespaceError {
    NamespaceError::Codec { record, source }
}

#[cfg(test)]
mod tests {
    use nokv_types::{
        ArtifactRevisionId, Generation, OperationId, RootActivationState, FIXED_ID_BYTES,
    };

    use super::super::codec::PATH_EXACT_TERMINATOR;

    use super::*;

    fn shard(fill: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn root(fill: u8) -> RootId {
        RootId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn incarnation(fill: u8) -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn request(fill: u8) -> RequestId {
        RequestId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(7).unwrap()
    }

    fn owner() -> OwnerEpoch {
        OwnerEpoch::new(1).unwrap()
    }

    fn workbench(value: &str) -> WorkbenchId {
        WorkbenchId::new(value).unwrap()
    }

    fn path(value: &str) -> NormalizedRelativePath {
        NormalizedRelativePath::new(value).unwrap()
    }

    fn path_entry(fill: u8, generation_value: u64) -> PathEntry {
        PathEntry {
            generation: Generation::new(generation_value).unwrap(),
            artifact_revision_id: ArtifactRevisionId::from_bytes([fill; FIXED_ID_BYTES]),
            body_digest_uri: format!("sha256:body-{fill}"),
            manifest_digest_uri: format!("sha256:manifest-{fill}"),
            logical_size: u64::from(fill),
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/octet-stream".to_owned(),
            producer: Some("namespace-test".to_owned()),
            manifest_id: Some(format!("manifest-{fill}")),
            typed_index_projection: TypedProjection::empty().encode().unwrap(),
        }
    }

    fn fence_command(
        store: &AgentMetadataStore,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(2),
            logical_shard_id: shard(1),
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

    fn ready_store() -> AgentMetadataStore {
        let store = AgentMetadataStore::open_memory(shard(1)).unwrap();
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
        store
    }

    fn write_context(store: &AgentMetadataStore, request_fill: u8) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(2),
            shard(1),
            placement(),
            owner(),
            request(request_fill),
        )
        .unwrap()
    }

    fn read_context(store: &AgentMetadataStore) -> RootReadContext {
        RootReadContext::current(store, root(2), placement(), owner()).unwrap()
    }

    fn read_context_at(version: CommitVersion) -> RootReadContext {
        RootReadContext {
            root_id: root(2),
            placement_generation: placement(),
            owner_epoch: owner(),
            read_version: ReadVersion::new(version.get()).unwrap(),
        }
    }

    fn put_absent(
        store: &AgentMetadataStore,
        request_fill: u8,
        records: Vec<(MetadataFamily, Vec<u8>, Vec<u8>)>,
    ) -> CommitVersion {
        let context = write_context(store, request_fill);
        let predicates = records
            .iter()
            .map(|(family, key, _)| CommandPredicate::Value {
                family: *family,
                key: key.clone(),
                expected: None,
            })
            .collect();
        let mutations = records
            .into_iter()
            .map(|(family, key, value)| CommandMutation::Put { family, key, value })
            .collect();
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
                    deterministic_result: b"namespace-test-write".to_vec(),
                }
                .seal(),
            )
            .unwrap()
            .commit_version
    }

    fn create_workspace(
        store: &AgentMetadataStore,
        request_fill: u8,
        name: &WorkbenchId,
        incarnation_fill: u8,
    ) -> CreateVisibleWorkspaceResult {
        create_visible_workspace(
            store,
            write_context(store, request_fill),
            name,
            incarnation(incarnation_fill),
        )
        .unwrap()
    }

    #[test]
    fn visible_workspace_create_replays_and_request_mismatch_fails() {
        let store = ready_store();
        let name = workbench("wb-create");
        let context = write_context(&store, 3);
        let created = create_visible_workspace(&store, context, &name, incarnation(3)).unwrap();
        assert!(!created.replayed);
        assert_eq!(created.workspace.incarnation_id, incarnation(3));
        assert_eq!(
            created.workspace.workspace_revision,
            WorkspaceRevision::ZERO
        );
        assert_eq!(created.workspace.state, WorkspaceState::Visible);
        assert_eq!(created.workspace.owning_operation_id, None);

        let replay = create_visible_workspace(&store, context, &name, incarnation(3)).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, created.commit_version);
        assert_eq!(replay.workspace, created.workspace);

        assert!(matches!(
            create_visible_workspace(&store, context, &name, incarnation(4)),
            Err(NamespaceError::Engine(AgentMetadataError::RequestIdReused))
        ));
        assert_eq!(
            create_visible_workspace(&store, write_context(&store, 4), &name, incarnation(4)),
            Err(NamespaceError::AlreadyExists {
                workbench_id: name.clone(),
            })
        );

        let visible =
            get_visible_workspace_at(&store, read_context_at(created.commit_version), &name)
                .unwrap();
        assert_eq!(visible, Some(created.workspace));
    }

    #[test]
    fn staging_and_retired_workspaces_hide_all_paths() {
        let store = ready_store();
        let staging_name = workbench("wb-staging");
        let retired_name = workbench("wb-retired");
        let staging_incarnation = incarnation(10);
        let retired_incarnation = incarnation(11);
        let staging_path = path("outputs/staging.bin");
        let retired_path = path("outputs/retired.bin");
        let staging_record = WorkspaceRecord {
            incarnation_id: staging_incarnation,
            workspace_revision: WorkspaceRevision::new(2),
            state: WorkspaceState::Staging,
            owning_operation_id: Some(OperationId::from_bytes([9; FIXED_ID_BYTES])),
        };
        let retired_record = WorkspaceRecord {
            incarnation_id: retired_incarnation,
            workspace_revision: WorkspaceRevision::new(3),
            state: WorkspaceState::Retired,
            owning_operation_id: None,
        };
        put_absent(
            &store,
            3,
            vec![
                (
                    MetadataFamily::WorkspaceCurrent,
                    workspace_current_key(root(2), &staging_name),
                    staging_record.encode().unwrap(),
                ),
                (
                    MetadataFamily::PathCurrent,
                    path_current_key(root(2), staging_incarnation, &staging_path),
                    path_entry(10, 1).encode().unwrap(),
                ),
                (
                    MetadataFamily::WorkspaceCurrent,
                    workspace_current_key(root(2), &retired_name),
                    retired_record.encode().unwrap(),
                ),
                (
                    MetadataFamily::PathCurrent,
                    path_current_key(root(2), retired_incarnation, &retired_path),
                    path_entry(11, 1).encode().unwrap(),
                ),
            ],
        );
        let context = read_context(&store);

        for (name, hidden_path) in [
            (&staging_name, &staging_path),
            (&retired_name, &retired_path),
        ] {
            assert_eq!(
                get_visible_workspace_at(&store, context, name).unwrap(),
                None
            );
            assert_eq!(
                get_visible_path_at(&store, context, name, hidden_path).unwrap(),
                None
            );
            assert_eq!(
                scan_visible_paths_at(&store, context, name, None, 10).unwrap(),
                VisiblePathPage::empty()
            );
        }
    }

    #[test]
    fn exact_path_read_uses_visible_marker_and_read_version() {
        let store = ready_store();
        let name = workbench("wb-exact");
        let created = create_workspace(&store, 3, &name, 12);
        let exact = path("outputs/a");
        let neighbor = path("outputs/ab");
        let exact_entry = path_entry(12, 1);
        let neighbor_entry = path_entry(13, 2);
        put_absent(
            &store,
            4,
            vec![
                (
                    MetadataFamily::PathCurrent,
                    path_current_key(root(2), incarnation(12), &neighbor),
                    neighbor_entry.encode().unwrap(),
                ),
                (
                    MetadataFamily::PathCurrent,
                    path_current_key(root(2), incarnation(12), &exact),
                    exact_entry.encode().unwrap(),
                ),
            ],
        );

        assert_eq!(
            get_visible_path_at(
                &store,
                read_context_at(created.commit_version),
                &name,
                &exact
            )
            .unwrap(),
            None
        );
        assert_eq!(
            get_visible_path_at(&store, read_context(&store), &name, &exact).unwrap(),
            Some(exact_entry)
        );
        assert_eq!(
            get_visible_path_at(
                &store,
                read_context(&store),
                &name,
                &path("outputs/missing")
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn resolved_workspace_reads_one_exact_path_and_pushes_down_child_prefix() {
        let store = ready_store();
        let name = workbench("wb-resolved-fast-path");
        let created = create_workspace(&store, 3, &name, 13);
        let paths = [
            "outputs",
            "outputs/a/deep.bin",
            "outputs/a",
            "outputs/b",
            "outputs2/outside.bin",
        ];
        put_absent(
            &store,
            4,
            paths
                .iter()
                .enumerate()
                .map(|(index, raw)| {
                    let path = path(raw);
                    (
                        MetadataFamily::PathCurrent,
                        path_current_key(root(2), incarnation(13), &path),
                        path_entry(index as u8 + 1, index as u64 + 1)
                            .encode()
                            .unwrap(),
                    )
                })
                .collect(),
        );
        let context = read_context(&store);
        let prefix = path("outputs");

        assert!(
            get_path_at_visible_workspace(&store, context, &created.workspace, &prefix,)
                .unwrap()
                .is_some()
        );

        let first = scan_paths_at_visible_workspace(
            &store,
            context,
            &created.workspace,
            Some(&prefix),
            None,
            2,
        )
        .unwrap();
        assert_eq!(
            first
                .entries
                .iter()
                .map(|visible| visible.path.as_str())
                .collect::<Vec<_>>(),
            ["outputs/a/deep.bin", "outputs/a"]
        );
        assert_eq!(first.next_marker, Some(path("outputs/a")));

        let second = scan_paths_at_visible_workspace(
            &store,
            context,
            &created.workspace,
            Some(&prefix),
            first.next_marker.as_ref(),
            2,
        )
        .unwrap();
        assert_eq!(
            second
                .entries
                .iter()
                .map(|visible| visible.path.as_str())
                .collect::<Vec<_>>(),
            ["outputs/b"]
        );
        assert!(second.next_marker.is_none());

        let retired = WorkspaceRecord {
            state: WorkspaceState::Retired,
            ..created.workspace
        };
        assert_eq!(
            get_path_at_visible_workspace(&store, context, &retired, &prefix).unwrap(),
            None
        );
        assert_eq!(
            scan_paths_at_visible_workspace(&store, context, &retired, Some(&prefix), None, 2,)
                .unwrap(),
            VisiblePathPage::empty()
        );
    }

    #[test]
    fn visible_path_scan_is_ordered_and_paginated_by_full_path() {
        let store = ready_store();
        let name = workbench("wb-pages");
        create_workspace(&store, 3, &name, 14);
        let paths = ["outputs/d", "outputs/a", "logs/z", "outputs/c", "outputs/b"];
        put_absent(
            &store,
            4,
            paths
                .iter()
                .enumerate()
                .map(|(index, raw)| {
                    let path = path(raw);
                    (
                        MetadataFamily::PathCurrent,
                        path_current_key(root(2), incarnation(14), &path),
                        path_entry(index as u8 + 1, index as u64 + 1)
                            .encode()
                            .unwrap(),
                    )
                })
                .collect(),
        );
        let context = read_context(&store);

        let first = scan_visible_paths_at(&store, context, &name, None, 2).unwrap();
        assert_eq!(
            first
                .entries
                .iter()
                .map(|item| item.path.as_str())
                .collect::<Vec<_>>(),
            ["logs/z", "outputs/a"]
        );
        assert_eq!(first.next_marker, Some(path("outputs/a")));

        let second =
            scan_visible_paths_at(&store, context, &name, first.next_marker.as_ref(), 2).unwrap();
        assert_eq!(
            second
                .entries
                .iter()
                .map(|item| item.path.as_str())
                .collect::<Vec<_>>(),
            ["outputs/b", "outputs/c"]
        );
        assert_eq!(second.next_marker, Some(path("outputs/c")));

        let third =
            scan_visible_paths_at(&store, context, &name, second.next_marker.as_ref(), 2).unwrap();
        assert_eq!(
            third
                .entries
                .iter()
                .map(|item| item.path.as_str())
                .collect::<Vec<_>>(),
            ["outputs/d"]
        );
        assert_eq!(third.next_marker, None);

        assert_eq!(
            scan_visible_paths_at(&store, context, &name, None, 0),
            Err(NamespaceError::InvalidPageLimit {
                requested: 0,
                max: MAX_VISIBLE_PATH_PAGE_SIZE,
            })
        );
        assert_eq!(
            scan_visible_paths_at(&store, context, &name, None, MAX_VISIBLE_PATH_PAGE_SIZE + 1),
            Err(NamespaceError::InvalidPageLimit {
                requested: MAX_VISIBLE_PATH_PAGE_SIZE + 1,
                max: MAX_VISIBLE_PATH_PAGE_SIZE,
            })
        );
    }

    #[test]
    fn malformed_workspace_path_record_and_path_key_fail_closed() {
        let store = ready_store();
        let corrupt_workspace = workbench("wb-corrupt-workspace");
        put_absent(
            &store,
            3,
            vec![(
                MetadataFamily::WorkspaceCurrent,
                workspace_current_key(root(2), &corrupt_workspace),
                vec![0xff],
            )],
        );
        assert!(matches!(
            get_visible_workspace_at(&store, read_context(&store), &corrupt_workspace),
            Err(NamespaceError::Codec {
                record: "WorkspaceCurrent",
                ..
            })
        ));

        let corrupt_record = workbench("wb-corrupt-record");
        create_workspace(&store, 4, &corrupt_record, 20);
        let corrupt_record_path = path("outputs/corrupt.bin");
        put_absent(
            &store,
            5,
            vec![(
                MetadataFamily::PathCurrent,
                path_current_key(root(2), incarnation(20), &corrupt_record_path),
                vec![0xff],
            )],
        );
        assert!(matches!(
            get_visible_path_at(
                &store,
                read_context(&store),
                &corrupt_record,
                &corrupt_record_path
            ),
            Err(NamespaceError::Codec {
                record: "PathCurrent",
                ..
            })
        ));

        let corrupt_key = workbench("wb-corrupt-key");
        create_workspace(&store, 6, &corrupt_key, 21);
        let mut malformed_key = path_child_prefix(root(2), incarnation(21), None);
        malformed_key.extend_from_slice(b"outputs/bad/name");
        malformed_key.push(PATH_EXACT_TERMINATOR);
        put_absent(
            &store,
            7,
            vec![(
                MetadataFamily::PathCurrent,
                malformed_key,
                path_entry(21, 1).encode().unwrap(),
            )],
        );
        assert!(matches!(
            scan_visible_paths_at(&store, read_context(&store), &corrupt_key, None, 10),
            Err(NamespaceError::CorruptKey {
                family: "PathCurrent",
                ..
            })
        ));
    }
}
