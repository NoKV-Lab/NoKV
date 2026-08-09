/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Stable visible-workspace snapshot listing at one fenced read version.

use std::fmt;

use nokv_types::{
    ReadVersion, RootId, SnapshotId, WorkbenchId, WorkspaceIncarnationId, FIXED_ID_BYTES,
};

use super::engine::{AgentMetadataError, AgentMetadataStore, MetadataFamily};
use super::namespace::{get_visible_workspace_at, NamespaceError, RootReadContext};
use super::snapshot::ResolvedSnapshot;
use super::snapshot_records::{SnapshotRecordError, SnapshotRefRecord};

/// Maximum snapshots returned by one page.
pub const MAX_SNAPSHOT_PAGE_SIZE: usize = 1_000;

const ENGINE_SCAN_BATCH: usize = 256;
const CURSOR_FORMAT_VERSION: u8 = 1;
const CURSOR_BYTES: usize = 1 + FIXED_ID_BYTES * 2 + 8 * 2;

/// One stable page of snapshots owned by the current visible incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotListPage {
    pub workspace_incarnation_id: WorkspaceIncarnationId,
    pub snapshots: Vec<ResolvedSnapshot>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: ReadVersion,
}

/// Snapshot-list query failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotListError {
    InvalidLimit { requested: usize, max: usize },
    WorkspaceNotFound { workbench_id: WorkbenchId },
    InvalidCursor { reason: &'static str },
    CursorScopeMismatch,
    CursorReadVersionMismatch { cursor: u64, request: u64 },
    CorruptSnapshotKey,
    SnapshotCodec(SnapshotRecordError),
    Namespace(NamespaceError),
    Engine(AgentMetadataError),
}

impl fmt::Display for SnapshotListError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit { requested, max } => {
                write!(formatter, "snapshot page limit {requested} is outside 1..={max}")
            }
            Self::WorkspaceNotFound { workbench_id } => {
                write!(formatter, "visible workbench {workbench_id} does not exist")
            }
            Self::InvalidCursor { reason } => {
                write!(formatter, "invalid snapshot-list cursor: {reason}")
            }
            Self::CursorScopeMismatch => {
                formatter.write_str("snapshot-list cursor belongs to a different workspace")
            }
            Self::CursorReadVersionMismatch { cursor, request } => write!(
                formatter,
                "snapshot-list cursor read version {cursor} does not match request version {request}"
            ),
            Self::CorruptSnapshotKey => formatter.write_str("corrupt SnapshotRef key"),
            Self::SnapshotCodec(source) => write!(formatter, "invalid SnapshotRef payload: {source}"),
            Self::Namespace(source) => source.fmt(formatter),
            Self::Engine(source) => source.fmt(formatter),
        }
    }
}

impl std::error::Error for SnapshotListError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SnapshotCodec(source) => Some(source),
            Self::Namespace(source) => Some(source),
            Self::Engine(source) => Some(source),
            _ => None,
        }
    }
}

impl From<SnapshotRecordError> for SnapshotListError {
    fn from(source: SnapshotRecordError) -> Self {
        Self::SnapshotCodec(source)
    }
}

impl From<NamespaceError> for SnapshotListError {
    fn from(source: NamespaceError) -> Self {
        Self::Namespace(source)
    }
}

impl From<AgentMetadataError> for SnapshotListError {
    fn from(source: AgentMetadataError) -> Self {
        Self::Engine(source)
    }
}

/// List every durable snapshot state for the current visible workspace.
pub fn list_snapshots_at(
    store: &AgentMetadataStore,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
    cursor: Option<&[u8]>,
    limit: usize,
) -> Result<SnapshotListPage, SnapshotListError> {
    if limit == 0 || limit > MAX_SNAPSHOT_PAGE_SIZE {
        return Err(SnapshotListError::InvalidLimit {
            requested: limit,
            max: MAX_SNAPSHOT_PAGE_SIZE,
        });
    }
    let workspace = get_visible_workspace_at(store, context, workbench_id)?.ok_or_else(|| {
        SnapshotListError::WorkspaceNotFound {
            workbench_id: workbench_id.clone(),
        }
    })?;
    let prefix = snapshot_prefix(context.root_id, workspace.incarnation_id);
    let mut marker = cursor
        .map(|encoded| {
            decode_cursor(
                encoded,
                context.root_id,
                workspace.incarnation_id,
                context.read_version,
            )
            .map(|snapshot_id| snapshot_key(&prefix, snapshot_id))
        })
        .transpose()?;
    let mut snapshots = Vec::with_capacity(limit.saturating_add(1));

    loop {
        let remaining = limit.saturating_add(1).saturating_sub(snapshots.len());
        if remaining == 0 {
            break;
        }
        let batch_limit = remaining.min(ENGINE_SCAN_BATCH);
        let rows = store.scan_prefix_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::SnapshotRef,
            &prefix,
            context.read_version,
            marker.as_deref(),
            batch_limit,
        )?;
        if rows.is_empty() {
            break;
        }
        let short = rows.len() < batch_limit;
        marker = rows.last().map(|row| row.key.clone());
        for row in rows {
            let snapshot_id = decode_snapshot_key(&prefix, &row.key)
                .ok_or(SnapshotListError::CorruptSnapshotKey)?;
            snapshots.push(ResolvedSnapshot {
                snapshot_id,
                record: SnapshotRefRecord::decode(&row.value)?,
                alias_record: None,
            });
        }
        if short {
            break;
        }
    }

    let has_more = snapshots.len() > limit;
    if has_more {
        snapshots.truncate(limit);
    }
    let next_cursor = has_more.then(|| {
        encode_cursor(
            context.root_id,
            workspace.incarnation_id,
            context.read_version,
            snapshots
                .last()
                .expect("lookahead page returns at least one snapshot")
                .snapshot_id,
        )
    });
    Ok(SnapshotListPage {
        workspace_incarnation_id: workspace.incarnation_id,
        snapshots,
        next_cursor,
        read_version: context.read_version,
    })
}

fn snapshot_prefix(root_id: RootId, workspace_incarnation_id: WorkspaceIncarnationId) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(FIXED_ID_BYTES * 2);
    prefix.extend_from_slice(root_id.as_bytes());
    prefix.extend_from_slice(workspace_incarnation_id.as_bytes());
    prefix
}

fn snapshot_key(prefix: &[u8], snapshot_id: SnapshotId) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&snapshot_id.get().to_be_bytes());
    key
}

fn decode_snapshot_key(prefix: &[u8], key: &[u8]) -> Option<SnapshotId> {
    if key.len() != prefix.len() + 8 || !key.starts_with(prefix) {
        return None;
    }
    Some(SnapshotId::new(u64::from_be_bytes(
        key[prefix.len()..].try_into().ok()?,
    )))
}

fn encode_cursor(
    root_id: RootId,
    workspace_incarnation_id: WorkspaceIncarnationId,
    read_version: ReadVersion,
    snapshot_id: SnapshotId,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(CURSOR_BYTES);
    encoded.push(CURSOR_FORMAT_VERSION);
    encoded.extend_from_slice(root_id.as_bytes());
    encoded.extend_from_slice(workspace_incarnation_id.as_bytes());
    encoded.extend_from_slice(&read_version.get().to_be_bytes());
    encoded.extend_from_slice(&snapshot_id.get().to_be_bytes());
    encoded
}

fn decode_cursor(
    encoded: &[u8],
    expected_root_id: RootId,
    expected_workspace_incarnation_id: WorkspaceIncarnationId,
    expected_read_version: ReadVersion,
) -> Result<SnapshotId, SnapshotListError> {
    if encoded.len() != CURSOR_BYTES || encoded.first().copied() != Some(CURSOR_FORMAT_VERSION) {
        return Err(SnapshotListError::InvalidCursor {
            reason: "unsupported envelope",
        });
    }
    let root_end = 1 + FIXED_ID_BYTES;
    let workspace_end = root_end + FIXED_ID_BYTES;
    if &encoded[1..root_end] != expected_root_id.as_bytes()
        || &encoded[root_end..workspace_end] != expected_workspace_incarnation_id.as_bytes()
    {
        return Err(SnapshotListError::CursorScopeMismatch);
    }
    let version_end = workspace_end + 8;
    let snapshot_end = version_end + 8;
    let cursor_read_version = u64::from_be_bytes(
        encoded[workspace_end..version_end]
            .try_into()
            .expect("cursor read version has fixed width"),
    );
    if cursor_read_version != expected_read_version.get() {
        return Err(SnapshotListError::CursorReadVersionMismatch {
            cursor: cursor_read_version,
            request: expected_read_version.get(),
        });
    }
    Ok(SnapshotId::new(u64::from_be_bytes(
        encoded[version_end..snapshot_end]
            .try_into()
            .expect("cursor snapshot id has fixed width"),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_types::{
        CommandDigest, LogicalShardId, OwnerEpoch, PlacementGeneration, RequestId,
        RootActivationState, WorkspaceIncarnationId, SHA256_BYTES,
    };

    use crate::workspace::{
        create_visible_workspace, mint_snapshot, retire_snapshot, MetadataCommand,
        RetireSnapshotRequest, RootFenceAction, RootWriteContext, SnapshotSelector, SCHEMA_ID,
    };

    fn root() -> RootId {
        RootId::from_bytes([1; FIXED_ID_BYTES])
    }

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([2; FIXED_ID_BYTES])
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(3).unwrap()
    }

    fn owner() -> OwnerEpoch {
        OwnerEpoch::new(1).unwrap()
    }

    fn request_id(value: u8) -> RequestId {
        RequestId::from_bytes([value; FIXED_ID_BYTES])
    }

    fn command(
        store: &AgentMetadataStore,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
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

    fn write_context(store: &AgentMetadataStore, request: u8) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            placement(),
            owner(),
            request_id(request),
        )
        .unwrap()
    }

    fn ready_store() -> (AgentMetadataStore, WorkbenchId, WorkspaceIncarnationId) {
        let store = AgentMetadataStore::open_memory(shard()).unwrap();
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&command(
                &store,
                request_id(1),
                RootFenceAction::Install {
                    layout_profile: nokv_types::RootLayoutProfile::SingleShardRoot,
                    layout_generation: nokv_types::RootLayoutGeneration::new(1).unwrap(),
                    partition_id: nokv_types::RootPartitionId::SINGLE_SHARD,
                },
            ))
            .unwrap();
        store
            .execute(&command(
                &store,
                request_id(2),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();
        let workbench = WorkbenchId::new("snapshot-list").unwrap();
        let incarnation = WorkspaceIncarnationId::from_bytes([4; FIXED_ID_BYTES]);
        create_visible_workspace(&store, write_context(&store, 3), &workbench, incarnation)
            .unwrap();
        (store, workbench, incarnation)
    }

    #[test]
    fn pages_all_states_and_binds_cursor_scope_and_read_version() {
        let (store, workbench, incarnation) = ready_store();
        for snapshot_id in 1..=3 {
            mint_snapshot(
                &store,
                write_context(&store, 3 + snapshot_id as u8),
                &crate::workspace::MintSnapshotRequest {
                    workbench_id: workbench.clone(),
                    snapshot_id: SnapshotId::new(snapshot_id),
                    alias: None,
                    lease_deadline_ms: 1_000 + snapshot_id,
                    annotation: vec![snapshot_id as u8],
                },
            )
            .unwrap();
        }
        retire_snapshot(
            &store,
            write_context(&store, 7),
            &RetireSnapshotRequest {
                workbench_id: workbench.clone(),
                selector: SnapshotSelector::Id(SnapshotId::new(2)),
                retire_annotation: None,
            },
        )
        .unwrap();

        let context = RootReadContext::current(&store, root(), placement(), owner()).unwrap();
        let first = list_snapshots_at(&store, context, &workbench, None, 2).unwrap();
        assert_eq!(first.workspace_incarnation_id, incarnation);
        assert_eq!(
            first
                .snapshots
                .iter()
                .map(|snapshot| snapshot.snapshot_id.get())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            first.snapshots[1].record.state,
            nokv_types::SnapshotState::Retired
        );
        let cursor = first
            .next_cursor
            .expect("three snapshots need a second page");

        let second = list_snapshots_at(&store, context, &workbench, Some(&cursor), 2).unwrap();
        assert_eq!(second.snapshots.len(), 1);
        assert_eq!(second.snapshots[0].snapshot_id.get(), 3);
        assert!(second.next_cursor.is_none());

        let other_incarnation = WorkspaceIncarnationId::from_bytes([9; FIXED_ID_BYTES]);
        assert_eq!(
            decode_cursor(&cursor, root(), other_incarnation, context.read_version),
            Err(SnapshotListError::CursorScopeMismatch)
        );
        let newer = ReadVersion::new(context.read_version.get() + 1).unwrap();
        assert!(matches!(
            decode_cursor(&cursor, root(), incarnation, newer),
            Err(SnapshotListError::CursorReadVersionMismatch { .. })
        ));
    }
}
