/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Bounded, generation-fenced maintenance for derived path indexes.

use std::collections::BTreeMap;
use std::fmt;

use nokv_types::{CommandDigest, CommitVersion, RootId, WorkspaceIncarnationId, SHA256_BYTES};
use sha2::{Digest as _, Sha256};

use super::codec::{path_current_key, SCHEMA_ID};
use super::engine::{
    CommandFit, CommandMutation, CommandPredicate, HistoryProjection, MetaError, MetaShard,
    MetadataCommand, MetadataScanItem, RootFenceAction,
};
use super::keyspace::MetadataFamily;
use super::namespace::RootWriteContext;
use super::publication_records::{PathEntry, PublicationRecordCodecError};
use super::query_records::{
    decode_path_index_locator_key, decode_secondary_index_row_key, path_index_digest,
    path_index_locator_key, PathIndexLocatorRecord, PathIndexLocatorState, QueryRecordError,
    SecondaryIndexRecord,
};

const CLEANUP_RESULT_VERSION: u8 = 1;
/// Maximum physical derived rows inspected by one maintenance command.
pub const MAX_SECONDARY_INDEX_CLEANUP_PAGE_SIZE: usize = 64;

/// Ordered subspace currently scanned by one cleanup cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SecondaryIndexCleanupPhase {
    SecondaryIndexRows = 1,
    PathIndexLocators = 2,
}

impl TryFrom<u8> for SecondaryIndexCleanupPhase {
    type Error = SecondaryIndexCleanupError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::SecondaryIndexRows),
            2 => Ok(Self::PathIndexLocators),
            value => Err(SecondaryIndexCleanupError::DeterministicResultMismatch {
                reason: format!("unknown cleanup phase {value}"),
            }),
        }
    }
}

/// Exclusive physical cursor returned by one cleanup page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexCleanupCursor {
    pub phase: SecondaryIndexCleanupPhase,
    pub start_after: Option<Vec<u8>>,
}

/// One bounded asynchronous cleanup request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupSecondaryIndexPageRequest {
    pub context: RootWriteContext,
    /// `None` starts a new full sweep at SecondaryIndexV2.
    pub cursor: Option<SecondaryIndexCleanupCursor>,
    pub limit: usize,
}

/// Durable page result returned both initially and after response-loss replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupSecondaryIndexPageOutcome {
    pub next_cursor: Option<SecondaryIndexCleanupCursor>,
    pub scanned_rows: usize,
    pub deleted_rows: usize,
    pub commit_version: CommitVersion,
    pub replayed: bool,
}

/// Typed failure from derived-index maintenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecondaryIndexCleanupError {
    Meta(MetaError),
    PathRecord(PublicationRecordCodecError),
    QueryRecord(QueryRecordError),
    InvalidLimit {
        requested: usize,
        maximum: usize,
    },
    InvalidCursor {
        reason: String,
    },
    CorruptKey {
        family: &'static str,
        reason: String,
    },
    ConcurrentMutation,
    TransactionTargetTooSmall {
        family: &'static str,
        key_bytes: usize,
    },
    RequestInputMismatch,
    DeterministicResultMismatch {
        reason: String,
    },
}

impl fmt::Display for SecondaryIndexCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Meta(error) => error.fmt(formatter),
            Self::PathRecord(error) => write!(formatter, "path record failed: {error}"),
            Self::QueryRecord(error) => write!(formatter, "query record failed: {error}"),
            Self::InvalidLimit { requested, maximum } => write!(
                formatter,
                "secondary-index cleanup limit {requested} is outside 1..={maximum}"
            ),
            Self::InvalidCursor { reason } => {
                write!(formatter, "invalid secondary-index cleanup cursor: {reason}")
            }
            Self::CorruptKey { family, reason } => {
                write!(formatter, "corrupt {family} key: {reason}")
            }
            Self::ConcurrentMutation => {
                formatter.write_str("secondary-index cleanup lost a concurrent mutation")
            }
            Self::TransactionTargetTooSmall { family, key_bytes } => write!(
                formatter,
                "one {family} cleanup row with a {key_bytes}-byte key exceeds the transaction target"
            ),
            Self::RequestInputMismatch => formatter.write_str(
                "request id was reused with different secondary-index cleanup inputs",
            ),
            Self::DeterministicResultMismatch { reason } => write!(
                formatter,
                "invalid replayed secondary-index cleanup result: {reason}"
            ),
        }
    }
}

impl std::error::Error for SecondaryIndexCleanupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Meta(error) => Some(error),
            Self::PathRecord(error) => Some(error),
            Self::QueryRecord(error) => Some(error),
            _ => None,
        }
    }
}

impl From<MetaError> for SecondaryIndexCleanupError {
    fn from(error: MetaError) -> Self {
        Self::Meta(error)
    }
}

impl From<PublicationRecordCodecError> for SecondaryIndexCleanupError {
    fn from(error: PublicationRecordCodecError) -> Self {
        Self::PathRecord(error)
    }
}

impl From<QueryRecordError> for SecondaryIndexCleanupError {
    fn from(error: QueryRecordError) -> Self {
        Self::QueryRecord(error)
    }
}

#[derive(Clone)]
struct ExactGuard {
    family: MetadataFamily,
    key: Vec<u8>,
    expected: Option<Vec<u8>>,
}

struct LoadedLocatorPath {
    key: Vec<u8>,
    payload: Option<Vec<u8>>,
    record: Option<PathEntry>,
}

#[derive(Clone)]
struct CleanupObservation {
    family: MetadataFamily,
    key: Vec<u8>,
    value: Vec<u8>,
    stale_guards: Option<Vec<ExactGuard>>,
}

struct PlannedCleanupPage {
    command: MetadataCommand,
    next_cursor: Option<SecondaryIndexCleanupCursor>,
    scanned_rows: usize,
    deleted_rows: usize,
}

/// Delete one bounded page of stale SecondaryIndexV2 rows or locators.
///
/// The first half of a sweep removes stale value rows. The second removes
/// their published locators. Every deletion asserts the exact derived row,
/// locator state, and current PathCurrent value (or absence). Staged locators
/// are never deleted by this generic worker.
pub fn cleanup_secondary_index_page(
    store: &MetaShard,
    request: CleanupSecondaryIndexPageRequest,
) -> Result<CleanupSecondaryIndexPageOutcome, SecondaryIndexCleanupError> {
    if !(1..=MAX_SECONDARY_INDEX_CLEANUP_PAGE_SIZE).contains(&request.limit) {
        return Err(SecondaryIndexCleanupError::InvalidLimit {
            requested: request.limit,
            maximum: MAX_SECONDARY_INDEX_CLEANUP_PAGE_SIZE,
        });
    }
    validate_cursor(request.context.root_id, request.cursor.as_ref())?;
    let input_digest = cleanup_input_digest(
        request.context.root_id,
        request.cursor.as_ref(),
        request.limit,
    );
    if let Some(replay) = store.lookup_request_result(
        request.context.root_id,
        request.context.placement_generation,
        request.context.owner_epoch,
        request.context.request_id,
    )? {
        let decoded = decode_cleanup_result(
            &replay.deterministic_result,
            request.context.root_id,
            input_digest,
        )?;
        return Ok(CleanupSecondaryIndexPageOutcome {
            next_cursor: decoded.next_cursor,
            scanned_rows: decoded.scanned_rows,
            deleted_rows: decoded.deleted_rows,
            commit_version: replay.commit_version,
            replayed: true,
        });
    }

    let phase = request
        .cursor
        .as_ref()
        .map_or(SecondaryIndexCleanupPhase::SecondaryIndexRows, |cursor| {
            cursor.phase
        });
    let start_after = request
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.start_after.as_deref());
    let family = family_for_phase(phase);
    let rows = store.scan_prefix_at(
        request.context.root_id,
        request.context.placement_generation,
        request.context.owner_epoch,
        family,
        request.context.root_id.as_bytes(),
        request.context.read_version,
        start_after,
        request.limit + 1,
    )?;
    let observations = rows
        .iter()
        .map(|row| observe_row(store, request.context, phase, row))
        .collect::<Result<Vec<_>, _>>()?;

    let planned = if observations.is_empty() {
        plan_cleanup_page(request.context, input_digest, phase, &observations, 0)?
    } else {
        choose_fitting_page(
            store,
            request.context,
            input_digest,
            phase,
            &observations,
            request.limit.min(observations.len()),
        )?
    };
    let executed = match store.execute(&planned.command) {
        Ok(executed) => executed,
        Err(
            MetaError::PredicateFailed
            | MetaError::WriteConflict
            | MetaError::WriteReadVersionMismatch { .. },
        ) => return Err(SecondaryIndexCleanupError::ConcurrentMutation),
        Err(error) => return Err(error.into()),
    };
    let decoded = decode_cleanup_result(
        &executed.deterministic_result,
        request.context.root_id,
        input_digest,
    )?;
    if decoded.next_cursor != planned.next_cursor
        || decoded.scanned_rows != planned.scanned_rows
        || decoded.deleted_rows != planned.deleted_rows
    {
        return Err(SecondaryIndexCleanupError::DeterministicResultMismatch {
            reason: "executed result disagrees with the selected cleanup page".to_owned(),
        });
    }
    Ok(CleanupSecondaryIndexPageOutcome {
        next_cursor: decoded.next_cursor,
        scanned_rows: decoded.scanned_rows,
        deleted_rows: decoded.deleted_rows,
        commit_version: executed.commit_version,
        replayed: executed.replayed,
    })
}

fn choose_fitting_page(
    store: &MetaShard,
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
    phase: SecondaryIndexCleanupPhase,
    observations: &[CleanupObservation],
    maximum: usize,
) -> Result<PlannedCleanupPage, SecondaryIndexCleanupError> {
    let mut low = 1;
    let mut high = maximum;
    let mut selected = None;
    while low <= high {
        let count = low + (high - low) / 2;
        let planned = plan_cleanup_page(context, input_digest, phase, observations, count)?;
        match store.command_fit(&planned.command, None)? {
            CommandFit::Fits => {
                selected = Some(planned);
                low = count + 1;
            }
            CommandFit::Exceeds { .. } => high = count - 1,
        }
    }
    selected.ok_or_else(|| SecondaryIndexCleanupError::TransactionTargetTooSmall {
        family: family_name(phase),
        key_bytes: observations[0].key.len(),
    })
}

fn plan_cleanup_page(
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
    phase: SecondaryIndexCleanupPhase,
    observations: &[CleanupObservation],
    count: usize,
) -> Result<PlannedCleanupPage, SecondaryIndexCleanupError> {
    let selected = &observations[..count];
    let next_cursor = if count < observations.len() {
        Some(SecondaryIndexCleanupCursor {
            phase,
            start_after: Some(
                selected
                    .last()
                    .expect("a partial nonempty page has a final key")
                    .key
                    .clone(),
            ),
        })
    } else {
        match phase {
            SecondaryIndexCleanupPhase::SecondaryIndexRows => Some(SecondaryIndexCleanupCursor {
                phase: SecondaryIndexCleanupPhase::PathIndexLocators,
                start_after: None,
            }),
            SecondaryIndexCleanupPhase::PathIndexLocators => None,
        }
    };
    let mut exact = BTreeMap::<(MetadataFamily, Vec<u8>), Option<Vec<u8>>>::new();
    let mut mutations = Vec::new();
    let mut history_projection = Vec::new();
    let mut deleted_rows = 0;
    for observation in selected {
        let Some(guards) = observation.stale_guards.as_ref() else {
            continue;
        };
        insert_guard(
            &mut exact,
            ExactGuard {
                family: observation.family,
                key: observation.key.clone(),
                expected: Some(observation.value.clone()),
            },
        )?;
        for guard in guards {
            insert_guard(&mut exact, guard.clone())?;
        }
        mutations.push(CommandMutation::Delete {
            family: observation.family,
            key: observation.key.clone(),
        });
        history_projection.push(HistoryProjection {
            family: observation.family,
            key: observation.key.clone(),
        });
        deleted_rows += 1;
    }
    let predicates = exact
        .into_iter()
        .map(|((family, key), expected)| CommandPredicate::Value {
            family,
            key,
            expected,
        })
        .collect();
    let deterministic_result =
        encode_cleanup_result(input_digest, next_cursor.as_ref(), count, deleted_rows)?;
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
        predicates,
        mutations,
        history_projection,
        event_projection: Vec::new(),
        deterministic_result,
    }
    .seal();
    Ok(PlannedCleanupPage {
        command,
        next_cursor,
        scanned_rows: count,
        deleted_rows,
    })
}

fn insert_guard(
    exact: &mut BTreeMap<(MetadataFamily, Vec<u8>), Option<Vec<u8>>>,
    guard: ExactGuard,
) -> Result<(), SecondaryIndexCleanupError> {
    let key = (guard.family, guard.key);
    if let Some(existing) = exact.get(&key) {
        if existing != &guard.expected {
            return Err(SecondaryIndexCleanupError::CorruptKey {
                family: guard.family.name(),
                reason: "one cleanup page observed two values for the same exact key".to_owned(),
            });
        }
        return Ok(());
    }
    exact.insert(key, guard.expected);
    Ok(())
}

fn observe_row(
    store: &MetaShard,
    context: RootWriteContext,
    phase: SecondaryIndexCleanupPhase,
    row: &MetadataScanItem,
) -> Result<CleanupObservation, SecondaryIndexCleanupError> {
    match phase {
        SecondaryIndexCleanupPhase::SecondaryIndexRows => {
            observe_secondary_index_row(store, context, row)
        }
        SecondaryIndexCleanupPhase::PathIndexLocators => {
            observe_path_index_locator(store, context, row)
        }
    }
}

fn observe_secondary_index_row(
    store: &MetaShard,
    context: RootWriteContext,
    row: &MetadataScanItem,
) -> Result<CleanupObservation, SecondaryIndexCleanupError> {
    let (_, _, workspace, digest, generation) =
        decode_secondary_index_row_key(context.root_id, &row.key).ok_or_else(|| {
            SecondaryIndexCleanupError::CorruptKey {
                family: "SecondaryIndex",
                reason: "key is not canonical SecondaryIndexV2".to_owned(),
            }
        })?;
    let value = SecondaryIndexRecord::decode(&row.value)?;
    if value.path_digest != digest || value.index_generation != generation {
        return Err(SecondaryIndexCleanupError::CorruptKey {
            family: "SecondaryIndex",
            reason: "value identity disagrees with its key suffix".to_owned(),
        });
    }
    let locator_key = path_index_locator_key(context.root_id, workspace, digest, generation);
    let locator_payload = read_current(
        store,
        context,
        MetadataFamily::PathIndexLocator,
        &locator_key,
    )?;
    let stale_guards = match locator_payload {
        None => {
            return Err(SecondaryIndexCleanupError::CorruptKey {
                family: "SecondaryIndex",
                reason: "row has no path-index locator".to_owned(),
            })
        }
        Some(locator_payload) => {
            let locator = PathIndexLocatorRecord::decode(&locator_payload)?;
            validate_locator_identity(digest, &locator)?;
            let current = load_locator_path(store, context, workspace, &locator)?;
            if locator.state == PathIndexLocatorState::Staged {
                if current.record.as_ref().is_some_and(|current| {
                    current.path_digest == digest && current.index_generation == generation
                }) {
                    return Err(SecondaryIndexCleanupError::CorruptKey {
                        family: "PathIndexLocator",
                        reason: "a staged locator is already authoritative in PathCurrent"
                            .to_owned(),
                    });
                }
                None
            } else if current.record.as_ref().is_some_and(|current| {
                current.path_digest == digest && current.index_generation == generation
            }) {
                None
            } else {
                Some(vec![
                    ExactGuard {
                        family: MetadataFamily::PathIndexLocator,
                        key: locator_key,
                        expected: Some(locator_payload),
                    },
                    ExactGuard {
                        family: MetadataFamily::PathCurrent,
                        key: current.key,
                        expected: current.payload,
                    },
                ])
            }
        }
    };
    Ok(CleanupObservation {
        family: MetadataFamily::SecondaryIndex,
        key: row.key.clone(),
        value: row.value.clone(),
        stale_guards,
    })
}

fn observe_path_index_locator(
    store: &MetaShard,
    context: RootWriteContext,
    row: &MetadataScanItem,
) -> Result<CleanupObservation, SecondaryIndexCleanupError> {
    let (workspace, digest, generation) = decode_path_index_locator_key(context.root_id, &row.key)
        .ok_or_else(|| SecondaryIndexCleanupError::CorruptKey {
            family: "PathIndexLocator",
            reason: "key is not canonical".to_owned(),
        })?;
    let locator = PathIndexLocatorRecord::decode(&row.value)?;
    validate_locator_identity(digest, &locator)?;
    let current = load_locator_path(store, context, workspace, &locator)?;
    let current_matches = current.record.as_ref().is_some_and(|current| {
        current.path_digest == digest && current.index_generation == generation
    });
    if locator.state == PathIndexLocatorState::Staged && current_matches {
        return Err(SecondaryIndexCleanupError::CorruptKey {
            family: "PathIndexLocator",
            reason: "a staged locator is already authoritative in PathCurrent".to_owned(),
        });
    }
    let stale_guards = (locator.state == PathIndexLocatorState::Published && !current_matches)
        .then(|| {
            vec![ExactGuard {
                family: MetadataFamily::PathCurrent,
                key: current.key,
                expected: current.payload,
            }]
        });
    Ok(CleanupObservation {
        family: MetadataFamily::PathIndexLocator,
        key: row.key.clone(),
        value: row.value.clone(),
        stale_guards,
    })
}

fn validate_locator_identity(
    expected_digest: [u8; SHA256_BYTES],
    locator: &PathIndexLocatorRecord,
) -> Result<(), SecondaryIndexCleanupError> {
    if path_index_digest(&locator.path) != expected_digest {
        return Err(SecondaryIndexCleanupError::CorruptKey {
            family: "PathIndexLocator",
            reason: "locator path does not match its digest key".to_owned(),
        });
    }
    Ok(())
}

fn load_locator_path(
    store: &MetaShard,
    context: RootWriteContext,
    workspace: WorkspaceIncarnationId,
    locator: &PathIndexLocatorRecord,
) -> Result<LoadedLocatorPath, SecondaryIndexCleanupError> {
    let key = path_current_key(context.root_id, workspace, &locator.path);
    let payload = read_current(store, context, MetadataFamily::PathCurrent, &key)?;
    let record = payload.as_deref().map(PathEntry::decode).transpose()?;
    if record
        .as_ref()
        .is_some_and(|record| record.path_digest != path_index_digest(&locator.path))
    {
        return Err(SecondaryIndexCleanupError::CorruptKey {
            family: "PathCurrent",
            reason: "path digest disagrees with its canonical path key".to_owned(),
        });
    }
    Ok(LoadedLocatorPath {
        key,
        payload,
        record,
    })
}

fn read_current(
    store: &MetaShard,
    context: RootWriteContext,
    family: MetadataFamily,
    key: &[u8],
) -> Result<Option<Vec<u8>>, SecondaryIndexCleanupError> {
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

fn validate_cursor(
    root: RootId,
    cursor: Option<&SecondaryIndexCleanupCursor>,
) -> Result<(), SecondaryIndexCleanupError> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.phase == SecondaryIndexCleanupPhase::SecondaryIndexRows
        && cursor.start_after.is_none()
    {
        return Err(SecondaryIndexCleanupError::InvalidCursor {
            reason: "the initial secondary-index position is represented by no cursor".to_owned(),
        });
    }
    let Some(key) = cursor.start_after.as_deref() else {
        return Ok(());
    };
    let canonical = match cursor.phase {
        SecondaryIndexCleanupPhase::SecondaryIndexRows => {
            decode_secondary_index_row_key(root, key).is_some()
        }
        SecondaryIndexCleanupPhase::PathIndexLocators => {
            decode_path_index_locator_key(root, key).is_some()
        }
    };
    if !canonical {
        return Err(SecondaryIndexCleanupError::InvalidCursor {
            reason: format!(
                "cursor is not a canonical {} key",
                family_name(cursor.phase)
            ),
        });
    }
    Ok(())
}

fn family_for_phase(phase: SecondaryIndexCleanupPhase) -> MetadataFamily {
    match phase {
        SecondaryIndexCleanupPhase::SecondaryIndexRows => MetadataFamily::SecondaryIndex,
        SecondaryIndexCleanupPhase::PathIndexLocators => MetadataFamily::PathIndexLocator,
    }
}

fn family_name(phase: SecondaryIndexCleanupPhase) -> &'static str {
    match phase {
        SecondaryIndexCleanupPhase::SecondaryIndexRows => "SecondaryIndex",
        SecondaryIndexCleanupPhase::PathIndexLocators => "PathIndexLocator",
    }
}

fn cleanup_input_digest(
    root: RootId,
    cursor: Option<&SecondaryIndexCleanupCursor>,
    limit: usize,
) -> [u8; SHA256_BYTES] {
    let mut digest = Sha256::new();
    digest.update(b"nokv.metadata.secondary-index-cleanup.v1\0");
    digest.update(root.as_bytes());
    match cursor {
        None => digest.update([0]),
        Some(cursor) => {
            digest.update([1, cursor.phase as u8]);
            match cursor.start_after.as_deref() {
                None => digest.update(0_u32.to_be_bytes()),
                Some(key) => {
                    digest.update(
                        u32::try_from(key.len())
                            .expect("metadata key length fits u32")
                            .to_be_bytes(),
                    );
                    digest.update(key);
                }
            }
        }
    }
    digest.update(
        u32::try_from(limit)
            .expect("cleanup page limit fits u32")
            .to_be_bytes(),
    );
    digest.finalize().into()
}

struct DecodedCleanupResult {
    next_cursor: Option<SecondaryIndexCleanupCursor>,
    scanned_rows: usize,
    deleted_rows: usize,
}

fn encode_cleanup_result(
    input_digest: [u8; SHA256_BYTES],
    next_cursor: Option<&SecondaryIndexCleanupCursor>,
    scanned_rows: usize,
    deleted_rows: usize,
) -> Result<Vec<u8>, SecondaryIndexCleanupError> {
    let mut encoded = Vec::new();
    encoded.push(CLEANUP_RESULT_VERSION);
    encoded.extend_from_slice(&input_digest);
    match next_cursor {
        None => encoded.push(0),
        Some(cursor) => {
            encoded.push(1);
            encoded.push(cursor.phase as u8);
            let key = cursor.start_after.as_deref().unwrap_or_default();
            encoded.extend_from_slice(
                &u32::try_from(key.len())
                    .map_err(
                        |_| SecondaryIndexCleanupError::DeterministicResultMismatch {
                            reason: "cleanup cursor key exceeds u32".to_owned(),
                        },
                    )?
                    .to_be_bytes(),
            );
            encoded.extend_from_slice(key);
        }
    }
    encoded.extend_from_slice(
        &u32::try_from(scanned_rows)
            .expect("bounded cleanup row count fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(
        &u32::try_from(deleted_rows)
            .expect("bounded cleanup row count fits u32")
            .to_be_bytes(),
    );
    Ok(encoded)
}

fn decode_cleanup_result(
    encoded: &[u8],
    root: RootId,
    expected_input_digest: [u8; SHA256_BYTES],
) -> Result<DecodedCleanupResult, SecondaryIndexCleanupError> {
    let mut offset = 0;
    let version = take_u8(encoded, &mut offset, "result version")?;
    if version != CLEANUP_RESULT_VERSION {
        return Err(SecondaryIndexCleanupError::DeterministicResultMismatch {
            reason: format!(
                "unsupported result version {version}, expected {CLEANUP_RESULT_VERSION}"
            ),
        });
    }
    let digest = take(encoded, &mut offset, SHA256_BYTES, "input digest")?;
    if digest != expected_input_digest {
        return Err(SecondaryIndexCleanupError::RequestInputMismatch);
    }
    let next_cursor = match take_u8(encoded, &mut offset, "cursor tag")? {
        0 => None,
        1 => {
            let phase = SecondaryIndexCleanupPhase::try_from(take_u8(
                encoded,
                &mut offset,
                "cursor phase",
            )?)?;
            let length = usize::try_from(u32::from_be_bytes(
                take(encoded, &mut offset, 4, "cursor length")?
                    .try_into()
                    .expect("exact cursor length field"),
            ))
            .expect("u32 fits usize");
            let key = take(encoded, &mut offset, length, "cursor key")?.to_vec();
            Some(SecondaryIndexCleanupCursor {
                phase,
                start_after: (!key.is_empty()).then_some(key),
            })
        }
        value => {
            return Err(SecondaryIndexCleanupError::DeterministicResultMismatch {
                reason: format!("invalid cursor tag {value}"),
            })
        }
    };
    let scanned_rows = usize::try_from(u32::from_be_bytes(
        take(encoded, &mut offset, 4, "scanned rows")?
            .try_into()
            .expect("exact scanned count"),
    ))
    .expect("u32 fits usize");
    let deleted_rows = usize::try_from(u32::from_be_bytes(
        take(encoded, &mut offset, 4, "deleted rows")?
            .try_into()
            .expect("exact deleted count"),
    ))
    .expect("u32 fits usize");
    if offset != encoded.len() {
        return Err(SecondaryIndexCleanupError::DeterministicResultMismatch {
            reason: format!("{} trailing result bytes", encoded.len() - offset),
        });
    }
    if deleted_rows > scanned_rows || scanned_rows > MAX_SECONDARY_INDEX_CLEANUP_PAGE_SIZE {
        return Err(SecondaryIndexCleanupError::DeterministicResultMismatch {
            reason: "cleanup counts exceed their canonical bounds".to_owned(),
        });
    }
    validate_cursor(root, next_cursor.as_ref())?;
    Ok(DecodedCleanupResult {
        next_cursor,
        scanned_rows,
        deleted_rows,
    })
}

fn take<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    length: usize,
    field: &'static str,
) -> Result<&'a [u8], SecondaryIndexCleanupError> {
    let end = offset.checked_add(length).ok_or_else(|| {
        SecondaryIndexCleanupError::DeterministicResultMismatch {
            reason: format!("{field} length overflowed"),
        }
    })?;
    let value = encoded.get(*offset..end).ok_or_else(|| {
        SecondaryIndexCleanupError::DeterministicResultMismatch {
            reason: format!("result is truncated at {field}"),
        }
    })?;
    *offset = end;
    Ok(value)
}

fn take_u8(
    encoded: &[u8],
    offset: &mut usize,
    field: &'static str,
) -> Result<u8, SecondaryIndexCleanupError> {
    Ok(take(encoded, offset, 1, field)?[0])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nokv_types::{
        ArtifactRevisionId, Generation, LogicalShardId, ObjectNamespaceId, OwnerEpoch,
        PathIndexGenerationId, PlacementGeneration, RequestId, RootActivationState, FIXED_ID_BYTES,
    };

    use super::*;
    use crate::workspace::{
        create_visible_workspace, path_index_locator_key, search_paths_at, secondary_index_key,
        QueryFieldId, QueryOperand, QueryOperator, QueryPredicate, QueryProfile, QueryScalar,
        QueryScope, RootReadContext, SearchRequest, TypedProjection,
    };

    fn root() -> RootId {
        RootId::from_bytes([1; FIXED_ID_BYTES])
    }

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([2; FIXED_ID_BYTES])
    }

    fn namespace() -> ObjectNamespaceId {
        ObjectNamespaceId::from_bytes([3; FIXED_ID_BYTES])
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(1).unwrap()
    }

    fn owner() -> OwnerEpoch {
        OwnerEpoch::new(1).unwrap()
    }

    fn request(value: u128) -> RequestId {
        RequestId::from_bytes(value.to_be_bytes())
    }

    fn workspace() -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([4; FIXED_ID_BYTES])
    }

    fn workbench() -> nokv_types::WorkbenchId {
        nokv_types::WorkbenchId::new("index-maintenance").unwrap()
    }

    fn context(store: &MetaShard, value: &mut u128) -> RootWriteContext {
        *value += 1;
        RootWriteContext::current(
            store,
            root(),
            shard(),
            namespace(),
            placement(),
            owner(),
            request(*value),
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
            object_namespace_id: Some(namespace()),
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

    fn activate(store: &MetaShard) {
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&fence_command(store, request(1), RootFenceAction::Install))
            .unwrap();
        store
            .execute(&fence_command(
                store,
                request(2),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();
    }

    fn seed_rows(
        store: &MetaShard,
        request_id: RequestId,
        rows: Vec<(MetadataFamily, Vec<u8>, Vec<u8>)>,
    ) {
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
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(namespace()),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates,
                    mutations,
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn path_entry(
        path: &nokv_types::NormalizedRelativePath,
        index_generation: PathIndexGenerationId,
        projection: &TypedProjection,
        revision_fill: u8,
    ) -> PathEntry {
        PathEntry {
            generation: Generation::new(1).unwrap(),
            index_generation,
            path_digest: path_index_digest(path),
            artifact_revision_id: ArtifactRevisionId::from_bytes([revision_fill; FIXED_ID_BYTES]),
            body_digest_uri: format!("sha256:{}", "11".repeat(32)),
            manifest_digest_uri: format!("sha256:{}", "22".repeat(32)),
            logical_size: 1,
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/octet-stream".to_owned(),
            producer: None,
            manifest_id: None,
            typed_index_projection: projection.encode().unwrap(),
        }
    }

    fn read(store: &MetaShard, family: MetadataFamily, key: &[u8]) -> Option<Vec<u8>> {
        store
            .read_at(
                root(),
                placement(),
                owner(),
                family,
                key,
                store.current_read_version().unwrap(),
            )
            .unwrap()
    }

    #[test]
    fn cleanup_fails_closed_on_a_locator_digest_collision() {
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate(&store);
        let key_path = nokv_types::NormalizedRelativePath::new("outputs/key.json").unwrap();
        let conflicting_path =
            nokv_types::NormalizedRelativePath::new("outputs/conflict.json").unwrap();
        let digest = path_index_digest(&key_path);
        let generation = PathIndexGenerationId::from_bytes([9; FIXED_ID_BYTES]);
        let locator_key = path_index_locator_key(root(), workspace(), digest, generation);
        seed_rows(
            &store,
            request(3),
            vec![(
                MetadataFamily::PathIndexLocator,
                locator_key.clone(),
                PathIndexLocatorRecord {
                    state: PathIndexLocatorState::Published,
                    path: conflicting_path,
                }
                .encode()
                .unwrap(),
            )],
        );
        let version_before = store.current_read_version().unwrap();
        let error = cleanup_secondary_index_page(
            &store,
            CleanupSecondaryIndexPageRequest {
                context: RootWriteContext::current(
                    &store,
                    root(),
                    shard(),
                    ObjectNamespaceId::from_bytes([4; FIXED_ID_BYTES]),
                    placement(),
                    owner(),
                    request(4),
                )
                .unwrap(),
                cursor: Some(SecondaryIndexCleanupCursor {
                    phase: SecondaryIndexCleanupPhase::PathIndexLocators,
                    start_after: None,
                }),
                limit: 1,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SecondaryIndexCleanupError::CorruptKey {
                family: "PathIndexLocator",
                ..
            }
        ));
        assert_eq!(store.current_read_version().unwrap(), version_before);
        assert!(read(&store, MetadataFamily::PathIndexLocator, &locator_key).is_some());
    }

    #[test]
    fn cleanup_fails_closed_when_a_secondary_row_has_no_locator() {
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate(&store);
        let field = QueryFieldId::new("artifact.stage").unwrap();
        let scalar = QueryScalar::String("ready".to_owned());
        let path = nokv_types::NormalizedRelativePath::new("outputs/orphan.json").unwrap();
        let digest = path_index_digest(&path);
        let generation = PathIndexGenerationId::from_bytes([9; FIXED_ID_BYTES]);
        let index_key =
            secondary_index_key(root(), &field, &scalar, workspace(), digest, generation);
        seed_rows(
            &store,
            request(3),
            vec![(
                MetadataFamily::SecondaryIndex,
                index_key.clone(),
                SecondaryIndexRecord {
                    path_digest: digest,
                    index_generation: generation,
                }
                .encode()
                .unwrap(),
            )],
        );
        let version_before = store.current_read_version().unwrap();
        let mut counter = 4;
        let error = cleanup_secondary_index_page(
            &store,
            CleanupSecondaryIndexPageRequest {
                context: context(&store, &mut counter),
                cursor: None,
                limit: 1,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SecondaryIndexCleanupError::CorruptKey {
                family: "SecondaryIndex",
                ..
            }
        ));
        assert_eq!(store.current_read_version().unwrap(), version_before);
        assert!(read(&store, MetadataFamily::SecondaryIndex, &index_key).is_some());
    }

    #[test]
    fn cleanup_replays_and_deletes_only_published_stale_generations() {
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate(&store);
        let path = nokv_types::NormalizedRelativePath::new("outputs/result.json").unwrap();
        let digest = path_index_digest(&path);
        let old_generation = PathIndexGenerationId::from_bytes([10; FIXED_ID_BYTES]);
        let current_generation = PathIndexGenerationId::from_bytes([20; FIXED_ID_BYTES]);
        let staged_generation = PathIndexGenerationId::from_bytes([30; FIXED_ID_BYTES]);
        let field = QueryFieldId::new("artifact.stage").unwrap();
        let scalar = QueryScalar::String("ready".to_owned());
        let projection =
            TypedProjection::new(BTreeMap::from([(field.clone(), scalar.clone())])).unwrap();
        let current = path_entry(&path, current_generation, &projection, 8);
        let index_value = |generation| {
            SecondaryIndexRecord {
                path_digest: digest,
                index_generation: generation,
            }
            .encode()
            .unwrap()
        };
        let locator = |state, generation| {
            (
                path_index_locator_key(root(), workspace(), digest, generation),
                PathIndexLocatorRecord {
                    state,
                    path: path.clone(),
                }
                .encode()
                .unwrap(),
            )
        };
        let old_index_key =
            secondary_index_key(root(), &field, &scalar, workspace(), digest, old_generation);
        let current_index_key = secondary_index_key(
            root(),
            &field,
            &scalar,
            workspace(),
            digest,
            current_generation,
        );
        let staged_index_key = secondary_index_key(
            root(),
            &field,
            &scalar,
            workspace(),
            digest,
            staged_generation,
        );
        let (old_locator_key, old_locator_value) =
            locator(PathIndexLocatorState::Published, old_generation);
        let (current_locator_key, current_locator_value) =
            locator(PathIndexLocatorState::Published, current_generation);
        let (staged_locator_key, staged_locator_value) =
            locator(PathIndexLocatorState::Staged, staged_generation);
        seed_rows(
            &store,
            request(3),
            vec![
                (
                    MetadataFamily::PathCurrent,
                    path_current_key(root(), workspace(), &path),
                    current.encode().unwrap(),
                ),
                (
                    MetadataFamily::SecondaryIndex,
                    old_index_key.clone(),
                    index_value(old_generation),
                ),
                (
                    MetadataFamily::SecondaryIndex,
                    current_index_key.clone(),
                    index_value(current_generation),
                ),
                (
                    MetadataFamily::SecondaryIndex,
                    staged_index_key.clone(),
                    index_value(staged_generation),
                ),
                (
                    MetadataFamily::PathIndexLocator,
                    old_locator_key.clone(),
                    old_locator_value,
                ),
                (
                    MetadataFamily::PathIndexLocator,
                    current_locator_key.clone(),
                    current_locator_value,
                ),
                (
                    MetadataFamily::PathIndexLocator,
                    staged_locator_key.clone(),
                    staged_locator_value,
                ),
            ],
        );

        let mut counter = 10_u128;
        let first_request = CleanupSecondaryIndexPageRequest {
            context: context(&store, &mut counter),
            cursor: None,
            limit: 1,
        };
        let first = cleanup_secondary_index_page(&store, first_request.clone()).unwrap();
        assert_eq!(first.scanned_rows, 1);
        assert_eq!(first.deleted_rows, 1);
        let replay = cleanup_secondary_index_page(&store, first_request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, first.commit_version);
        assert_eq!(replay.next_cursor, first.next_cursor);

        let mut cursor = first.next_cursor;
        let mut deleted = first.deleted_rows;
        while let Some(next) = cursor {
            let outcome = cleanup_secondary_index_page(
                &store,
                CleanupSecondaryIndexPageRequest {
                    context: context(&store, &mut counter),
                    cursor: Some(next),
                    limit: 1,
                },
            )
            .unwrap();
            deleted += outcome.deleted_rows;
            cursor = outcome.next_cursor;
        }
        assert_eq!(deleted, 2);
        assert!(read(&store, MetadataFamily::SecondaryIndex, &old_index_key).is_none());
        assert!(read(&store, MetadataFamily::PathIndexLocator, &old_locator_key).is_none());
        assert!(read(&store, MetadataFamily::SecondaryIndex, &current_index_key).is_some());
        assert!(read(
            &store,
            MetadataFamily::PathIndexLocator,
            &current_locator_key
        )
        .is_some());
        assert!(read(&store, MetadataFamily::SecondaryIndex, &staged_index_key).is_some());
        assert!(read(
            &store,
            MetadataFamily::PathIndexLocator,
            &staged_locator_key
        )
        .is_some());
    }

    #[test]
    fn indexed_query_joins_candidates_in_bounded_batches() {
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate(&store);
        let mut counter = 20_u128;
        create_visible_workspace(
            &store,
            context(&store, &mut counter),
            &workbench(),
            workspace(),
        )
        .unwrap();
        let field = QueryFieldId::new("run.stage").unwrap();
        let scalar = QueryScalar::String("ready".to_owned());
        let projection =
            TypedProjection::new(BTreeMap::from([(field.clone(), scalar.clone())])).unwrap();
        let mut rows = Vec::new();
        for index in 0..65_u8 {
            let path =
                nokv_types::NormalizedRelativePath::new(format!("outputs/item-{index:02}.json"))
                    .unwrap();
            let digest = path_index_digest(&path);
            let generation = PathIndexGenerationId::from_bytes([index + 1; FIXED_ID_BYTES]);
            rows.push((
                MetadataFamily::PathCurrent,
                path_current_key(root(), workspace(), &path),
                path_entry(&path, generation, &projection, index + 1)
                    .encode()
                    .unwrap(),
            ));
            rows.push((
                MetadataFamily::PathIndexLocator,
                path_index_locator_key(root(), workspace(), digest, generation),
                PathIndexLocatorRecord {
                    state: PathIndexLocatorState::Published,
                    path: path.clone(),
                }
                .encode()
                .unwrap(),
            ));
            rows.push((
                MetadataFamily::SecondaryIndex,
                secondary_index_key(root(), &field, &scalar, workspace(), digest, generation),
                SecondaryIndexRecord {
                    path_digest: digest,
                    index_generation: generation,
                }
                .encode()
                .unwrap(),
            ));
        }
        let staged_path = nokv_types::NormalizedRelativePath::new("outputs/staged.json").unwrap();
        let staged_digest = path_index_digest(&staged_path);
        let staged_generation = PathIndexGenerationId::from_bytes([90; FIXED_ID_BYTES]);
        rows.push((
            MetadataFamily::PathIndexLocator,
            path_index_locator_key(root(), workspace(), staged_digest, staged_generation),
            PathIndexLocatorRecord {
                state: PathIndexLocatorState::Staged,
                path: staged_path,
            }
            .encode()
            .unwrap(),
        ));
        rows.push((
            MetadataFamily::SecondaryIndex,
            secondary_index_key(
                root(),
                &field,
                &scalar,
                workspace(),
                staged_digest,
                staged_generation,
            ),
            SecondaryIndexRecord {
                path_digest: staged_digest,
                index_generation: staged_generation,
            }
            .encode()
            .unwrap(),
        ));
        seed_rows(&store, request(30), rows);

        let page = search_paths_at(
            &store,
            RootReadContext::current(&store, root(), placement(), owner()).unwrap(),
            &SearchRequest {
                profile: QueryProfile::ArtifactV1,
                scope: QueryScope::Workspace(workbench()),
                path_prefix: None,
                predicates: vec![QueryPredicate {
                    field_id: field,
                    operator: QueryOperator::Equal,
                    operand: QueryOperand::Scalar(scalar),
                }],
                projection: Vec::new(),
                sort: Vec::new(),
                facets: Vec::new(),
                cursor: None,
                limit: 65,
            },
        )
        .unwrap();
        assert_eq!(page.hits.len(), 65);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn large_cleanup_pages_shrink_to_the_fdb_planning_target() {
        let inner = crate::workspace::test_support::memory_txn_store().unwrap();
        let (capturing, capture) = crate::workspace::test_support::capture_txn_store(inner);
        let targeted = crate::workspace::test_support::with_transaction_target(capturing, 900_000);
        let store = MetaShard::initialize(targeted, shard()).unwrap();
        activate(&store);
        let projection = TypedProjection::new(
            (0..super::super::query_records::MAX_TYPED_PROJECTION_FIELDS)
                .map(|index| {
                    (
                        QueryFieldId::new(format!("cleanup.capacity_{index:02}")).unwrap(),
                        QueryScalar::String("x".repeat(928)),
                    )
                })
                .collect(),
        )
        .unwrap();
        assert_eq!(projection.encode().unwrap().len(), 57_243);
        let index_field = QueryFieldId::new("cleanup.marker").unwrap();
        let index_scalar = QueryScalar::String("stale".to_owned());
        let mut rows = Vec::new();
        let path_count = 12;
        for index in 0..path_count {
            let mut path_bytes = "x".repeat(nokv_types::NormalizedRelativePath::MAX_BYTES - 2);
            path_bytes.push_str(&format!("{index:02}"));
            let path = nokv_types::NormalizedRelativePath::new(path_bytes).unwrap();
            let digest = path_index_digest(&path);
            let old_generation = PathIndexGenerationId::from_bytes(
                [u8::try_from(index + 1).unwrap(); FIXED_ID_BYTES],
            );
            let current_generation = PathIndexGenerationId::from_bytes(
                [u8::try_from(index + 101).unwrap(); FIXED_ID_BYTES],
            );
            rows.push((
                MetadataFamily::PathCurrent,
                path_current_key(root(), workspace(), &path),
                path_entry(
                    &path,
                    current_generation,
                    &projection,
                    u8::try_from(index + 1).unwrap(),
                )
                .encode()
                .unwrap(),
            ));
            rows.push((
                MetadataFamily::PathIndexLocator,
                path_index_locator_key(root(), workspace(), digest, old_generation),
                PathIndexLocatorRecord {
                    state: PathIndexLocatorState::Published,
                    path: path.clone(),
                }
                .encode()
                .unwrap(),
            ));
            rows.push((
                MetadataFamily::SecondaryIndex,
                secondary_index_key(
                    root(),
                    &index_field,
                    &index_scalar,
                    workspace(),
                    digest,
                    old_generation,
                ),
                SecondaryIndexRecord {
                    path_digest: digest,
                    index_generation: old_generation,
                }
                .encode()
                .unwrap(),
            ));
        }
        seed_rows(&store, request(3), rows);

        let mut counter = 100_u128;
        let mut cursor = None;
        let mut deleted = 0;
        let mut first_scanned = None;
        loop {
            let outcome = cleanup_secondary_index_page(
                &store,
                CleanupSecondaryIndexPageRequest {
                    context: context(&store, &mut counter),
                    cursor,
                    limit: MAX_SECONDARY_INDEX_CLEANUP_PAGE_SIZE,
                },
            )
            .unwrap();
            first_scanned.get_or_insert(outcome.scanned_rows);
            deleted += outcome.deleted_rows;
            let transaction_bytes =
                capture.with_last_commit(crate::workspace::test_support::transaction_bytes);
            assert!(transaction_bytes <= 900_000);
            let Some(next) = outcome.next_cursor else {
                break;
            };
            cursor = Some(next);
        }
        assert!(first_scanned.unwrap() < path_count);
        assert_eq!(deleted, path_count * 2);
    }
}
