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
    CommandDigest, CommitVersion, LogicalShardId, NormalizedRelativePath, ObjectNamespaceId,
    OwnerEpoch, PlacementGeneration, ReadVersion, RequestId, RootId, WorkbenchId,
    WorkspaceIncarnationId, WorkspaceRevision, WorkspaceState, SHA256_BYTES,
};

use super::codec::{
    decode_path_common_prefix, decode_path_current_key, path_child_prefix, path_current_key,
    workspace_current_key, workspace_incarnation_claim_key, PATH_COMPONENT_DELIMITER, SCHEMA_ID,
};
use super::engine::{
    CommandMutation, CommandPredicate, DelimitedMetadataScanItem, MetaError, MetaShard,
    MetadataCommand, RootFenceAction,
};
use super::event_projection::change_event_projection;
use super::keyspace::MetadataFamily;
use super::publication_records::{
    PathEntry, PublicationRecordCodecError, WorkspaceIncarnationClaimRecord, WorkspaceRecord,
};
use super::query_records::{
    ChangeEventKind, ChangeEventRecord, QueryFieldId, QueryRecordError, QueryScalar,
    TypedProjection,
};

/// Maximum number of visible paths returned by one namespace page.
///
/// One additional logical item is reserved to determine whether another page
/// exists. A direct-child scan may consume two adjacent raw items per logical
/// child: one common prefix and one exact artifact row.
pub(super) const MAX_VISIBLE_PATH_PAGE_SIZE: usize = 255;

/// Maximum number of logical entries returned by one namespace listing.
///
/// Larger logical pages are assembled from bounded transaction-store scans so callers do
/// not need to know the metadata layer's raw scan bound.
pub const MAX_VISIBLE_PATH_LIST_PAGE_SIZE: usize = 1_000;

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
        store: &MetaShard,
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
    pub object_namespace_id: ObjectNamespaceId,
    pub placement_generation: PlacementGeneration,
    pub owner_epoch: OwnerEpoch,
    pub request_id: RequestId,
    pub read_version: ReadVersion,
}

impl RootWriteContext {
    /// Capture the store's current durable read version for a new request.
    pub fn current(
        store: &MetaShard,
        root_id: RootId,
        logical_shard_id: LogicalShardId,
        object_namespace_id: ObjectNamespaceId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        request_id: RequestId,
    ) -> Result<Self, NamespaceError> {
        Ok(Self {
            root_id,
            logical_shard_id,
            object_namespace_id,
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
pub(super) struct VisiblePath {
    pub path: NormalizedRelativePath,
    pub entry: PathEntry,
}

/// One direct child, represented either by an exact artifact or an implicit
/// descendant prefix. An artifact wins when both exist at the same path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisiblePathChild {
    pub path: NormalizedRelativePath,
    pub entry: Option<PathEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct VisiblePathChildPage {
    pub entries: Vec<VisiblePathChild>,
    pub next_marker: Option<NormalizedRelativePath>,
}

impl VisiblePathChildPage {
    fn empty() -> Self {
        Self {
            entries: Vec::new(),
            next_marker: None,
        }
    }
}

/// One stable logical listing page for a visible workspace.
///
/// Entries may be exact artifacts or implicit direct-child prefixes. The
/// marker is always the last returned full path and is exclusive on resume.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisiblePathListPage {
    pub entries: Vec<VisiblePathChild>,
    pub next_marker: Option<NormalizedRelativePath>,
}

impl VisiblePathListPage {
    fn empty() -> Self {
        Self {
            entries: Vec::new(),
            next_marker: None,
        }
    }
}

/// One visible workspace marker and the optional exact path resolved under the
/// same fenced point-read session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisibleWorkspacePathRead {
    pub context: RootReadContext,
    pub workspace: WorkspaceRecord,
    pub entry: Option<PathEntry>,
}

/// One stable ordered page of visible paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct VisiblePathPage {
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
    IncarnationAlreadyClaimed {
        incarnation_id: WorkspaceIncarnationId,
        workbench_id: WorkbenchId,
    },
    InvalidPageLimit {
        requested: usize,
        max: usize,
    },
    InvalidListMarker,
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
    Meta(MetaError),
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists { workbench_id } => {
                write!(formatter, "workbench {workbench_id} already exists")
            }
            Self::IncarnationAlreadyClaimed {
                incarnation_id,
                workbench_id,
            } => write!(
                formatter,
                "workspace incarnation {:02x?} is permanently claimed by workbench {workbench_id}",
                incarnation_id.as_bytes()
            ),
            Self::InvalidPageLimit { requested, max } => write!(
                formatter,
                "visible path page limit {requested} is outside 1..={max}"
            ),
            Self::InvalidListMarker => write!(
                formatter,
                "list_paths cursor does not belong to the requested listing level"
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
            Self::Meta(source) => source.fmt(formatter),
        }
    }
}

impl std::error::Error for NamespaceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec { source, .. } => Some(source),
            Self::QueryRecord(source) => Some(source),
            Self::Meta(source) => Some(source),
            _ => None,
        }
    }
}

impl From<MetaError> for NamespaceError {
    fn from(source: MetaError) -> Self {
        Self::Meta(source)
    }
}

impl From<QueryRecordError> for NamespaceError {
    fn from(source: QueryRecordError) -> Self {
        Self::QueryRecord(source)
    }
}

/// Create one workbench name as an immediately-visible, empty workspace.
///
/// Both the name and never-reused incarnation are guarded by exact absence
/// predicates. An exact request replay returns and validates the originally
/// persisted typed result.
pub fn create_visible_workspace(
    store: &MetaShard,
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
    let claim_key = workspace_incarnation_claim_key(context.root_id, incarnation_id);
    let claim_payload = WorkspaceIncarnationClaimRecord {
        workbench_id: workbench_id.clone(),
    }
    .encode()
    .map_err(|source| codec_error("WorkspaceIncarnationClaim", source))?;
    let event = change_event_projection(&ChangeEventRecord {
        workbench_id: workbench_id.clone(),
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
        object_namespace_id: Some(context.object_namespace_id),
        placement_generation: context.placement_generation,
        owner_epoch: context.owner_epoch,
        request_id: context.request_id,
        command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
        read_version: context.read_version,
        root_fence_action: RootFenceAction::RequireActive,
        predicates: vec![
            CommandPredicate::Value {
                family: MetadataFamily::WorkspaceCurrent,
                key: key.clone(),
                expected: None,
            },
            CommandPredicate::Value {
                family: MetadataFamily::WorkspaceIncarnationClaim,
                key: claim_key.clone(),
                expected: None,
            },
        ],
        mutations: vec![
            CommandMutation::Put {
                family: MetadataFamily::WorkspaceCurrent,
                key: key.clone(),
                value: payload.clone(),
            },
            CommandMutation::Put {
                family: MetadataFamily::WorkspaceIncarnationClaim,
                key: claim_key.clone(),
                value: claim_payload,
            },
        ],
        history_projection: Vec::new(),
        event_projection: vec![event],
        deterministic_result: payload,
    }
    .seal();

    let result = match store.execute(&command) {
        Ok(result) => result,
        Err(MetaError::PredicateFailed) => {
            if store
                .read_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    MetadataFamily::WorkspaceCurrent,
                    &key,
                    context.read_version,
                )?
                .is_some()
            {
                return Err(NamespaceError::AlreadyExists {
                    workbench_id: workbench_id.clone(),
                });
            }

            let Some(claim_payload) = store.read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                MetadataFamily::WorkspaceIncarnationClaim,
                &claim_key,
                context.read_version,
            )?
            else {
                // The command has only the two exact absence predicates above.
                // If neither key existed at its exact MVCC read version, keep
                // the metadata failure instead of inventing a domain conflict.
                return Err(NamespaceError::Meta(MetaError::PredicateFailed));
            };
            let claim = WorkspaceIncarnationClaimRecord::decode(&claim_payload)
                .map_err(|source| codec_error("WorkspaceIncarnationClaim", source))?;
            return Err(NamespaceError::IncarnationAlreadyClaimed {
                incarnation_id,
                workbench_id: claim.workbench_id,
            });
        }
        Err(source) => return Err(NamespaceError::Meta(source)),
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
    store: &MetaShard,
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
    store: &MetaShard,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
    path: &NormalizedRelativePath,
) -> Result<Option<PathEntry>, NamespaceError> {
    Ok(
        get_visible_workspace_path_at(store, context, workbench_id, path)?
            .and_then(|read| read.entry),
    )
}

/// Resolve a live workspace marker and one dependent path while capturing the
/// current read version inside the same fenced point-read session.
pub fn get_current_visible_workspace_path(
    store: &MetaShard,
    root_id: RootId,
    placement_generation: PlacementGeneration,
    owner_epoch: OwnerEpoch,
    workbench_id: &WorkbenchId,
    path: &NormalizedRelativePath,
) -> Result<Option<VisibleWorkspacePathRead>, NamespaceError> {
    get_visible_workspace_path(
        store,
        root_id,
        placement_generation,
        owner_epoch,
        None,
        workbench_id,
        path,
    )
}

/// Resolve a workspace marker and one dependent path at an exact read version
/// under one ownership/fence validation.
pub fn get_visible_workspace_path_at(
    store: &MetaShard,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
    path: &NormalizedRelativePath,
) -> Result<Option<VisibleWorkspacePathRead>, NamespaceError> {
    get_visible_workspace_path(
        store,
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        Some(context.read_version),
        workbench_id,
        path,
    )
}

fn get_visible_workspace_path(
    store: &MetaShard,
    root_id: RootId,
    placement_generation: PlacementGeneration,
    owner_epoch: OwnerEpoch,
    requested_version: Option<ReadVersion>,
    workbench_id: &WorkbenchId,
    path: &NormalizedRelativePath,
) -> Result<Option<VisibleWorkspacePathRead>, NamespaceError> {
    store.with_fenced_point_reads(
        root_id,
        placement_generation,
        owner_epoch,
        requested_version,
        |read_version, reader| {
            let context = RootReadContext {
                root_id,
                placement_generation,
                owner_epoch,
                read_version,
            };
            let workspace_key = workspace_current_key(root_id, workbench_id);
            let Some(payload) = reader.get(MetadataFamily::WorkspaceCurrent, &workspace_key)?
            else {
                return Ok(None);
            };
            let workspace = WorkspaceRecord::decode(&payload)
                .map_err(|source| codec_error("WorkspaceCurrent", source))?;
            if workspace.state != WorkspaceState::Visible {
                return Ok(None);
            }

            let path_key = path_current_key(root_id, workspace.incarnation_id, path);
            let entry = reader
                .get(MetadataFamily::PathCurrent, &path_key)?
                .map(|payload| {
                    PathEntry::decode(&payload).map_err(|source| codec_error("PathCurrent", source))
                })
                .transpose()?;
            Ok(Some(VisibleWorkspacePathRead {
                context,
                workspace,
                entry,
            }))
        },
    )
}

/// Read one exact path after the caller resolved `WorkspaceCurrent` at the
/// same read context.
///
/// Keeping this as a separate primitive makes the cold read shape explicit:
/// one marker point read followed by one authoritative path point read. The
/// state check fails closed if a staging or retired record is passed by an
/// internal caller.
pub fn get_path_at_visible_workspace(
    store: &MetaShard,
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

/// List one logical page below an optional normalized prefix after the caller
/// has resolved `WorkspaceCurrent` at the same read context.
///
/// Recursive listings return authoritative artifact rows. Direct listings
/// coalesce deeper descendants into implicit prefix entries. When `prefix`
/// itself names an artifact it follows all descendants, matching the encoded
/// full-path key order. `start_after` must be the exact full-path marker
/// returned by a preceding page at the same listing level.
pub fn list_paths_at_visible_workspace(
    store: &MetaShard,
    context: RootReadContext,
    workspace: &WorkspaceRecord,
    prefix: Option<&NormalizedRelativePath>,
    recursive: bool,
    start_after: Option<&NormalizedRelativePath>,
    limit: usize,
) -> Result<VisiblePathListPage, NamespaceError> {
    if workspace.state != WorkspaceState::Visible {
        return Ok(VisiblePathListPage::empty());
    }
    if !(1..=MAX_VISIBLE_PATH_LIST_PAGE_SIZE).contains(&limit) {
        return Err(NamespaceError::InvalidPageLimit {
            requested: limit,
            max: MAX_VISIBLE_PATH_LIST_PAGE_SIZE,
        });
    }
    if start_after.is_some_and(|marker| !list_marker_is_valid(prefix, recursive, marker)) {
        return Err(NamespaceError::InvalidListMarker);
    }

    let mut marker = start_after.cloned();
    let mut selected = Vec::with_capacity(limit.saturating_add(1));
    // A marker equal to an explicit prefix means the exact-prefix row was the
    // final entry on the preceding page. A root listing has no exact-prefix
    // row, so `(None, None)` must still enter the descendant scan.
    if prefix.is_none() || prefix != marker.as_ref() {
        'scan: loop {
            let remaining = limit.saturating_add(1).saturating_sub(selected.len());
            if remaining == 0 {
                break;
            }
            let scan_limit = remaining.min(MAX_VISIBLE_PATH_PAGE_SIZE);
            let page = if recursive {
                let page = scan_paths_at_visible_workspace(
                    store,
                    context,
                    workspace,
                    prefix,
                    marker.as_ref(),
                    scan_limit,
                )?;
                VisiblePathChildPage {
                    entries: page
                        .entries
                        .into_iter()
                        .map(|visible| VisiblePathChild {
                            path: visible.path,
                            entry: Some(visible.entry),
                        })
                        .collect(),
                    next_marker: page.next_marker,
                }
            } else {
                scan_direct_path_children_at_visible_workspace(
                    store,
                    context,
                    workspace,
                    prefix,
                    marker.as_ref(),
                    scan_limit,
                )?
            };
            for visible in page.entries {
                selected.push(visible);
                if selected.len() > limit {
                    break 'scan;
                }
            }
            let Some(next) = page.next_marker else {
                break;
            };
            marker = Some(next);
        }
    }

    if selected.len() <= limit {
        let exact_prefix = match prefix {
            Some(prefix) if marker.as_ref() != Some(prefix) => {
                get_path_at_visible_workspace(store, context, workspace, prefix)?.map(|entry| {
                    VisiblePathChild {
                        path: prefix.clone(),
                        entry: Some(entry),
                    }
                })
            }
            _ => None,
        };
        selected.extend(exact_prefix);
    }

    let has_more = selected.len() > limit;
    if has_more {
        selected.truncate(limit);
    }
    let next_marker = has_more.then(|| {
        selected
            .last()
            .expect("a listing page with lookahead has one returned entry")
            .path
            .clone()
    });
    Ok(VisiblePathListPage {
        entries: selected,
        next_marker,
    })
}

/// Scan all normalized full paths in one visible workspace incarnation.
///
/// `start_after` is exclusive. A returned `next_marker` is the last entry in
/// the current page and is safe to pass to the next call at the same read
/// version.
pub(super) fn scan_visible_paths_at(
    store: &MetaShard,
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
pub(super) fn scan_paths_at_visible_workspace(
    store: &MetaShard,
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

/// Scan direct logical children below an optional normalized parent.
///
/// Transaction-store common-prefix rollups are retained as implicit children. If an exact
/// artifact exists at the same path, it replaces the implicit prefix in the
/// returned logical item. The exclusive marker skips both representations.
pub(super) fn scan_direct_path_children_at_visible_workspace(
    store: &MetaShard,
    context: RootReadContext,
    workspace: &WorkspaceRecord,
    path_prefix: Option<&NormalizedRelativePath>,
    start_after: Option<&NormalizedRelativePath>,
    limit: usize,
) -> Result<VisiblePathChildPage, NamespaceError> {
    if workspace.state != WorkspaceState::Visible {
        return Ok(VisiblePathChildPage::empty());
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
    let logical_with_lookahead = limit + 1;
    let raw_limit = logical_with_lookahead * 2;
    let scanned = store.scan_delimited_prefix_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::PathCurrent,
        &prefix,
        PATH_COMPONENT_DELIMITER,
        context.read_version,
        marker_key.as_deref(),
        raw_limit,
    )?;

    let mut children = BTreeMap::<NormalizedRelativePath, Option<PathEntry>>::new();
    for item in scanned {
        match item {
            DelimitedMetadataScanItem::Record(item) => {
                let path =
                    decode_path_current_key(context.root_id, workspace.incarnation_id, &item.key)
                        .ok_or_else(|| NamespaceError::CorruptKey {
                        family: "PathCurrent",
                        reason: "key is not the canonical root/workspace/path encoding".to_owned(),
                    })?;
                let entry = PathEntry::decode(&item.value)
                    .map_err(|source| codec_error("PathCurrent", source))?;
                children.insert(path, Some(entry));
            }
            DelimitedMetadataScanItem::CommonPrefix(key) => {
                let path =
                    decode_path_common_prefix(context.root_id, workspace.incarnation_id, &key)
                        .ok_or_else(|| NamespaceError::CorruptKey {
                            family: "PathCurrent",
                            reason: "common prefix is not a canonical root/workspace/path prefix"
                                .to_owned(),
                        })?;
                children.entry(path).or_insert(None);
            }
        }
    }

    let has_more = children.len() > limit;
    let entries = children
        .into_iter()
        .take(limit)
        .map(|(path, entry)| VisiblePathChild { path, entry })
        .collect::<Vec<_>>();
    let next_marker = has_more.then(|| {
        entries
            .last()
            .expect("a page with a lookahead item has one returned child")
            .path
            .clone()
    });
    Ok(VisiblePathChildPage {
        entries,
        next_marker,
    })
}

fn list_marker_is_valid(
    prefix: Option<&NormalizedRelativePath>,
    recursive: bool,
    marker: &NormalizedRelativePath,
) -> bool {
    match (prefix, recursive) {
        (None, true) => true,
        (None, false) => marker.component_count() == 1,
        (Some(prefix), true) => marker == prefix || descendant_suffix(marker, prefix).is_some(),
        (Some(prefix), false) if marker == prefix => true,
        (Some(prefix), false) => descendant_suffix(marker, prefix)
            .is_some_and(|suffix| !suffix.is_empty() && !suffix.contains('/')),
    }
}

fn descendant_suffix<'a>(
    path: &'a NormalizedRelativePath,
    prefix: &NormalizedRelativePath,
) -> Option<&'a str> {
    path.as_str()
        .strip_prefix(prefix.as_str())
        .and_then(|suffix| suffix.strip_prefix('/'))
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
    use super::super::engine::HistoryProjection;

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
            index_generation: nokv_types::PathIndexGenerationId::from_bytes([fill; FIXED_ID_BYTES]),
            path_digest: [fill; SHA256_BYTES],
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
        store: &MetaShard,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(2),
            logical_shard_id: shard(1),
            object_namespace_id: Some(ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES])),
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

    fn ready_store() -> MetaShard {
        let store = crate::workspace::test_support::memory(shard(1)).unwrap();
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

    fn write_context(store: &MetaShard, request_fill: u8) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(2),
            shard(1),
            ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement(),
            owner(),
            request(request_fill),
        )
        .unwrap()
    }

    fn read_context(store: &MetaShard) -> RootReadContext {
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
        store: &MetaShard,
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
                    object_namespace_id: Some(context.object_namespace_id),
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
        store: &MetaShard,
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
            Err(NamespaceError::Meta(MetaError::RequestIdReused))
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
    fn incarnation_claim_conflict_does_not_report_an_absent_workbench_as_existing() {
        let store = ready_store();
        let existing_name = workbench("wb-create");
        let absent_name = workbench("wb-create-other");
        let claimed_incarnation = incarnation(3);
        create_visible_workspace(
            &store,
            write_context(&store, 3),
            &existing_name,
            claimed_incarnation,
        )
        .unwrap();

        assert_eq!(
            create_visible_workspace(
                &store,
                write_context(&store, 4),
                &absent_name,
                claimed_incarnation,
            ),
            Err(NamespaceError::IncarnationAlreadyClaimed {
                incarnation_id: claimed_incarnation,
                workbench_id: existing_name.clone(),
            })
        );
        assert_eq!(
            get_visible_workspace_at(&store, read_context(&store), &absent_name).unwrap(),
            None
        );

        let claim_key = workspace_incarnation_claim_key(root(2), claimed_incarnation);
        let claim = store
            .read_at(
                root(2),
                placement(),
                owner(),
                MetadataFamily::WorkspaceIncarnationClaim,
                &claim_key,
                read_context(&store).read_version,
            )
            .unwrap()
            .expect("created incarnation has a permanent claim");
        assert_eq!(
            WorkspaceIncarnationClaimRecord::decode(&claim).unwrap(),
            WorkspaceIncarnationClaimRecord {
                workbench_id: existing_name,
            }
        );
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
        let path_created_version = put_absent(
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
            Some(exact_entry.clone())
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

        let current = get_current_visible_workspace_path(
            &store,
            root(2),
            placement(),
            owner(),
            &name,
            &exact,
        )
        .unwrap()
        .expect("visible workspace resolves in the fused point-read session");
        assert_eq!(current.workspace, created.workspace);
        assert_eq!(current.entry, Some(exact_entry.clone()));
        assert_eq!(
            current.context.read_version,
            read_context(&store).read_version
        );

        let missing = get_current_visible_workspace_path(
            &store,
            root(2),
            placement(),
            owner(),
            &name,
            &path("outputs/missing"),
        )
        .unwrap()
        .expect("visible workspace remains distinguishable from a missing path");
        assert!(missing.entry.is_none());
        assert!(get_current_visible_workspace_path(
            &store,
            root(2),
            placement(),
            owner(),
            &workbench("missing-workbench"),
            &exact,
        )
        .unwrap()
        .is_none());

        let context = write_context(&store, 5);
        let exact_key = path_current_key(root(2), incarnation(12), &exact);
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
                    predicates: vec![CommandPredicate::Value {
                        family: MetadataFamily::PathCurrent,
                        key: exact_key.clone(),
                        expected: Some(exact_entry.encode().unwrap()),
                    }],
                    mutations: vec![CommandMutation::Delete {
                        family: MetadataFamily::PathCurrent,
                        key: exact_key.clone(),
                    }],
                    history_projection: vec![HistoryProjection {
                        family: MetadataFamily::PathCurrent,
                        key: exact_key,
                    }],
                    event_projection: Vec::new(),
                    deterministic_result: b"delete-exact-path".to_vec(),
                }
                .seal(),
            )
            .unwrap();
        let live_after_delete = get_current_visible_workspace_path(
            &store,
            root(2),
            placement(),
            owner(),
            &name,
            &exact,
        )
        .unwrap()
        .expect("workspace remains visible after deleting one path");
        assert_eq!(live_after_delete.entry, None);
        assert_eq!(
            get_visible_path_at(&store, read_context_at(path_created_version), &name, &exact,)
                .unwrap(),
            Some(exact_entry)
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
    fn logical_listing_composes_prefix_order_pagination_and_marker_validation() {
        let store = ready_store();
        let name = workbench("wb-logical-list");
        let created = create_workspace(&store, 3, &name, 16);
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
                        path_current_key(root(2), incarnation(16), &path),
                        path_entry(index as u8 + 1, index as u64 + 1)
                            .encode()
                            .unwrap(),
                    )
                })
                .collect(),
        );
        let context = read_context(&store);
        let prefix = path("outputs");

        let first = list_paths_at_visible_workspace(
            &store,
            context,
            &created.workspace,
            Some(&prefix),
            true,
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

        let second = list_paths_at_visible_workspace(
            &store,
            context,
            &created.workspace,
            Some(&prefix),
            true,
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
            ["outputs/b", "outputs"]
        );
        assert!(second.entries.iter().all(|visible| visible.entry.is_some()));
        assert!(second.next_marker.is_none());

        let direct = list_paths_at_visible_workspace(
            &store,
            context,
            &created.workspace,
            Some(&prefix),
            false,
            None,
            10,
        )
        .unwrap();
        assert_eq!(
            direct
                .entries
                .iter()
                .map(|visible| visible.path.as_str())
                .collect::<Vec<_>>(),
            ["outputs/a", "outputs/b", "outputs"]
        );
        assert!(direct.entries.iter().all(|visible| visible.entry.is_some()));

        assert_eq!(
            list_paths_at_visible_workspace(
                &store,
                context,
                &created.workspace,
                Some(&prefix),
                true,
                Some(&path("outputs2/outside.bin")),
                2,
            ),
            Err(NamespaceError::InvalidListMarker)
        );
        assert_eq!(
            list_paths_at_visible_workspace(
                &store,
                context,
                &created.workspace,
                Some(&prefix),
                false,
                Some(&path("outputs/a/deep.bin")),
                2,
            ),
            Err(NamespaceError::InvalidListMarker)
        );
        assert_eq!(
            list_paths_at_visible_workspace(
                &store,
                context,
                &created.workspace,
                None,
                false,
                Some(&path("outputs/a")),
                2,
            ),
            Err(NamespaceError::InvalidListMarker)
        );
    }

    #[test]
    fn logical_listing_assembles_pages_larger_than_one_meta_scan() {
        let store = ready_store();
        let name = workbench("wb-wide-list");
        let created = create_workspace(&store, 3, &name, 17);
        let records = (0..260)
            .map(|index| {
                let path = path(&format!("items/{index:04}"));
                (
                    MetadataFamily::PathCurrent,
                    path_current_key(root(2), incarnation(17), &path),
                    path_entry((index % 250 + 1) as u8, index as u64 + 1)
                        .encode()
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        put_absent(&store, 4, records[..255].to_vec());
        put_absent(&store, 5, records[255..].to_vec());
        let context = read_context(&store);
        let prefix = path("items");

        let page = list_paths_at_visible_workspace(
            &store,
            context,
            &created.workspace,
            Some(&prefix),
            true,
            None,
            300,
        )
        .unwrap();
        assert_eq!(page.entries.len(), 260);
        assert_eq!(page.entries.first().unwrap().path, path("items/0000"));
        assert_eq!(page.entries.last().unwrap().path, path("items/0259"));
        assert!(page.next_marker.is_none());

        assert_eq!(
            list_paths_at_visible_workspace(
                &store,
                context,
                &created.workspace,
                Some(&prefix),
                true,
                None,
                MAX_VISIBLE_PATH_LIST_PAGE_SIZE + 1,
            ),
            Err(NamespaceError::InvalidPageLimit {
                requested: MAX_VISIBLE_PATH_LIST_PAGE_SIZE + 1,
                max: MAX_VISIBLE_PATH_LIST_PAGE_SIZE,
            })
        );
    }

    #[test]
    fn direct_child_scan_coalesces_prefix_and_artifact_without_skipping_a_sibling() {
        let store = ready_store();
        let name = workbench("wb-direct-pages");
        let created = create_workspace(&store, 3, &name, 15);
        let paths = ["parent/a/deep", "parent/a", "parent/ab", "parent/b/deep"];
        let historical_version = put_absent(
            &store,
            4,
            paths
                .iter()
                .enumerate()
                .map(|(index, raw)| {
                    let path = path(raw);
                    (
                        MetadataFamily::PathCurrent,
                        path_current_key(root(2), incarnation(15), &path),
                        path_entry(index as u8 + 1, index as u64 + 1)
                            .encode()
                            .unwrap(),
                    )
                })
                .collect(),
        );
        let context = read_context(&store);
        let parent = path("parent");

        let first = scan_direct_path_children_at_visible_workspace(
            &store,
            context,
            &created.workspace,
            Some(&parent),
            None,
            1,
        )
        .unwrap();
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].path, path("parent/a"));
        assert!(first.entries[0].entry.is_some());
        assert_eq!(first.next_marker, Some(path("parent/a")));

        let second = scan_direct_path_children_at_visible_workspace(
            &store,
            context,
            &created.workspace,
            Some(&parent),
            first.next_marker.as_ref(),
            1,
        )
        .unwrap();
        assert_eq!(second.entries[0].path, path("parent/ab"));
        assert!(second.entries[0].entry.is_some());
        assert_eq!(second.next_marker, Some(path("parent/ab")));

        let third = scan_direct_path_children_at_visible_workspace(
            &store,
            context,
            &created.workspace,
            Some(&parent),
            second.next_marker.as_ref(),
            1,
        )
        .unwrap();
        assert_eq!(third.entries[0].path, path("parent/b"));
        assert!(third.entries[0].entry.is_none());
        assert!(third.next_marker.is_none());

        let deleted = [
            ("parent/a", path_entry(2, 2)),
            ("parent/ab", path_entry(3, 3)),
            ("parent/b/deep", path_entry(4, 4)),
        ];
        let added_path = path("parent/aa");
        let added_payload = path_entry(5, 5).encode().unwrap();
        let context = write_context(&store, 5);
        let mut command = MetadataCommand {
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
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: b"mutated-direct-children".to_vec(),
        };
        for (raw, entry) in deleted {
            let key = path_current_key(root(2), incarnation(15), &path(raw));
            command.predicates.push(CommandPredicate::Value {
                family: MetadataFamily::PathCurrent,
                key: key.clone(),
                expected: Some(entry.encode().unwrap()),
            });
            command.mutations.push(CommandMutation::Delete {
                family: MetadataFamily::PathCurrent,
                key: key.clone(),
            });
            command.history_projection.push(HistoryProjection {
                family: MetadataFamily::PathCurrent,
                key,
            });
        }
        let added_key = path_current_key(root(2), incarnation(15), &added_path);
        command.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::PathCurrent,
            key: added_key.clone(),
            expected: None,
        });
        command.mutations.push(CommandMutation::Put {
            family: MetadataFamily::PathCurrent,
            key: added_key,
            value: added_payload,
        });
        store.execute(&command.seal()).unwrap();

        let historical_context = read_context_at(historical_version);
        let mut marker = None;
        let mut historical_paths = Vec::new();
        let mut historical_kinds = Vec::new();
        loop {
            let page = scan_direct_path_children_at_visible_workspace(
                &store,
                historical_context,
                &created.workspace,
                Some(&parent),
                marker.as_ref(),
                1,
            )
            .unwrap();
            historical_paths.extend(page.entries.iter().map(|entry| entry.path.clone()));
            historical_kinds.extend(page.entries.iter().map(|entry| entry.entry.is_some()));
            let Some(next) = page.next_marker else {
                break;
            };
            marker = Some(next);
        }
        assert_eq!(
            historical_paths,
            [path("parent/a"), path("parent/ab"), path("parent/b")]
        );
        assert_eq!(historical_kinds, [true, true, false]);
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
        malformed_key.extend_from_slice(b"b\x01c");
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
