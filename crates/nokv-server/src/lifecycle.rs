/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Owner-fenced background recovery for workspace lifecycle state machines.
//!
//! Every discovery scan is rooted in authoritative metadata. Object-store
//! listing is intentionally absent. Provider calls happen only after the exact
//! local route, owner-loss signal, persisted owner epoch, and active root fence
//! have all been checked.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nokv_meta::workspace as meta;
use nokv_object::{ArtifactObjectStore, ObjectDeleteOutcome, ObjectKey};
use nokv_protocol::RootRoute;
use nokv_types::{
    ArtifactRevisionId, BuildCommitPhase, CommitRetirePhase, CommitState, GcClaimState, GcPhase,
    OperationId, OperationKind, OwnerEpoch, PlacementGeneration, PublishPhase, RequestId,
    RestorePhase, RootActivationState, RootId, SnapshotState, StagedCleanupState,
    StagedProviderState, WorkspaceState, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use crate::{OwnerLossSignal, RootOwnerRegistry};

const OWNER_LOSS_POLL_SLICE: Duration = Duration::from_millis(10);

/// Why an authoritative metadata row requires provider deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleDeletePurpose {
    AbortedPublication,
    RevisionGarbageCollection,
}

/// Provider-neutral deletion request. For an aborted multipart publication the
/// provider implementation must abort the named upload and prove the final key
/// absent before returning success.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleDeleteRequest {
    pub purpose: LifecycleDeletePurpose,
    pub object_key: String,
    pub multipart_upload_id: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleDeleteDisposition {
    Deleted,
    AlreadyAbsent,
}

/// Stable provider evidence consumed by the durable metadata state machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifecycleAbsenceProof {
    pub disposition: LifecycleDeleteDisposition,
    pub digest: [u8; SHA256_BYTES],
}

impl LifecycleAbsenceProof {
    /// Build stable provider evidence for one authoritative delete request.
    /// The caller must pass the canonical object key received from the
    /// lifecycle worker; the digest binds the metadata schema, proof domain,
    /// delete purpose, exact key, optional multipart id, and disposition.
    pub fn from_delete_request(
        request: &LifecycleDeleteRequest,
        disposition: LifecycleDeleteDisposition,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.lifecycle.object-absence-proof.v1\0");
        hash_part(&mut hasher, meta::SCHEMA_ID.as_bytes());
        hasher.update([match request.purpose {
            LifecycleDeletePurpose::AbortedPublication => 1,
            LifecycleDeletePurpose::RevisionGarbageCollection => 2,
        }]);
        hash_part(&mut hasher, request.object_key.as_bytes());
        match request.multipart_upload_id.as_deref() {
            None => hasher.update([0]),
            Some(multipart_upload_id) => {
                hasher.update([1]);
                hash_part(&mut hasher, multipart_upload_id);
            }
        }
        hasher.update([match disposition {
            LifecycleDeleteDisposition::Deleted => 1,
            LifecycleDeleteDisposition::AlreadyAbsent => 2,
        }]);
        Self {
            disposition,
            digest: hasher.finalize().into(),
        }
    }
}

/// Provider failure classification. `Retryable` is legal only when the
/// implementation knows deletion was not dispatched. Every uncertain outcome
/// must be `Ambiguous` so the owning metadata operation is quarantined.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleDeleteError {
    Retryable { detail: String },
    Ambiguous { evidence: Vec<u8> },
}

/// Narrow provider boundary used by lifecycle workers. It deliberately has no
/// list operation.
pub trait LifecycleObjectDeleter: Send + Sync {
    fn delete(
        &self,
        request: &LifecycleDeleteRequest,
    ) -> Result<LifecycleAbsenceProof, LifecycleDeleteError>;
}

/// Server-owned adapter from immutable object-store primitives to the durable
/// lifecycle deletion contract.
///
/// The object boundary cannot prove that a failed destructive call was not
/// dispatched, and it currently exposes no multipart-abort operation. Both
/// cases therefore fail closed as ambiguous so the metadata operation is
/// quarantined instead of retrying an uncertain deletion.
#[derive(Clone)]
pub struct ArtifactLifecycleDeleter<Store: ?Sized> {
    store: Arc<Store>,
}

impl<Store: ?Sized> ArtifactLifecycleDeleter<Store> {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

impl<Store> LifecycleObjectDeleter for ArtifactLifecycleDeleter<Store>
where
    Store: ArtifactObjectStore + Send + Sync + ?Sized,
{
    fn delete(
        &self,
        request: &LifecycleDeleteRequest,
    ) -> Result<LifecycleAbsenceProof, LifecycleDeleteError> {
        if request.multipart_upload_id.is_some() {
            return Err(LifecycleDeleteError::Ambiguous {
                evidence: b"configured object provider has no multipart-abort boundary".to_vec(),
            });
        }
        let key = ObjectKey::new(request.object_key.clone()).map_err(|error| {
            LifecycleDeleteError::Ambiguous {
                evidence: format!("authoritative lifecycle object key is invalid: {error}")
                    .into_bytes(),
            }
        })?;
        let disposition = match self.store.delete(&key) {
            Ok(ObjectDeleteOutcome::Deleted) => LifecycleDeleteDisposition::Deleted,
            Ok(ObjectDeleteOutcome::Absent) => LifecycleDeleteDisposition::AlreadyAbsent,
            Err(error) => {
                return Err(LifecycleDeleteError::Ambiguous {
                    evidence: format!("object delete outcome is uncertain: {error}").into_bytes(),
                });
            }
        };
        Ok(LifecycleAbsenceProof::from_delete_request(
            request,
            disposition,
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifecycleRunnerOptions {
    pub poll_interval: Duration,
    pub scan_page_size: usize,
    pub mutation_batch_size: usize,
    pub maximum_publish_clock_skew_ms: u64,
    pub maximum_snapshot_clock_skew_ms: u64,
}

impl Default for LifecycleRunnerOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(250),
            scan_page_size: 64,
            mutation_batch_size: 32,
            maximum_publish_clock_skew_ms: 30_000,
            maximum_snapshot_clock_skew_ms: 30_000,
        }
    }
}

impl LifecycleRunnerOptions {
    fn validate(self) -> Result<Self, LifecycleError> {
        if self.poll_interval.is_zero() {
            return Err(LifecycleError::InvalidOptions(
                "poll interval must be greater than zero".to_owned(),
            ));
        }
        if !(1..=meta::MAX_GC_CANDIDATE_PAGE_SIZE).contains(&self.scan_page_size) {
            return Err(LifecycleError::InvalidOptions(format!(
                "scan page size must be within 1..={}",
                meta::MAX_GC_CANDIDATE_PAGE_SIZE
            )));
        }
        let maximum_batch = meta::MAX_PUBLICATION_BATCH_ROWS
            .min(meta::MAX_RESTORE_BATCH_MEMBERS)
            .min(meta::MAX_COMMIT_RETIRE_MEMBER_BATCH_ROWS)
            .min(meta::MAX_GC_BATCH_ROWS);
        if !(1..=maximum_batch).contains(&self.mutation_batch_size) {
            return Err(LifecycleError::InvalidOptions(format!(
                "mutation batch size must be within 1..={maximum_batch}"
            )));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LifecycleCycleReport {
    pub metadata_transitions: u64,
    pub provider_deletions: u64,
    pub quarantined_operations: u64,
    pub deferred_operations: u64,
}

#[derive(Debug)]
pub enum LifecycleError {
    InvalidOptions(String),
    OwnerLost(String),
    Meta(meta::MetaError),
    CorruptMetadata {
        record: &'static str,
        detail: String,
    },
    StateMachine {
        action: &'static str,
        detail: String,
    },
    Clock(String),
    WorkerLockPoisoned,
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions(detail) => {
                write!(formatter, "invalid lifecycle options: {detail}")
            }
            Self::OwnerLost(detail) => write!(formatter, "lifecycle owner fence lost: {detail}"),
            Self::Meta(error) => write!(formatter, "lifecycle metadata failed: {error}"),
            Self::CorruptMetadata { record, detail } => {
                write!(formatter, "corrupt lifecycle {record}: {detail}")
            }
            Self::StateMachine { action, detail } => {
                write!(formatter, "lifecycle {action} failed: {detail}")
            }
            Self::Clock(detail) => write!(formatter, "lifecycle clock failed: {detail}"),
            Self::WorkerLockPoisoned => formatter.write_str("lifecycle worker lock is poisoned"),
        }
    }
}

impl std::error::Error for LifecycleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Meta(error) => Some(error),
            _ => None,
        }
    }
}

impl From<meta::MetaError> for LifecycleError {
    fn from(error: meta::MetaError) -> Self {
        Self::Meta(error)
    }
}

#[derive(Default)]
struct LifecycleCursors {
    publish_operation: Option<Vec<u8>>,
    restore_operation: Option<Vec<u8>>,
    build_operation: Option<Vec<u8>>,
    retire_operation: Option<Vec<u8>>,
    commit: Option<Vec<u8>>,
    snapshot_workspace: Option<Vec<u8>>,
    snapshot: Option<Vec<u8>>,
    gc_candidate: Option<meta::GcCandidateCursor>,
}

/// One root-affine lifecycle runner. Calls are serialized because its bounded
/// discovery cursors are in-memory soft state; metadata records hold all
/// destructive progress.
pub struct LifecycleRunner {
    meta: Arc<meta::MetaShard>,
    registry: Arc<RootOwnerRegistry>,
    route: RootRoute,
    owner_loss: OwnerLossSignal,
    objects: Arc<dyn LifecycleObjectDeleter>,
    options: LifecycleRunnerOptions,
    cursors: Mutex<LifecycleCursors>,
}

impl LifecycleRunner {
    pub fn new(
        meta: Arc<meta::MetaShard>,
        registry: Arc<RootOwnerRegistry>,
        route: RootRoute,
        owner_loss: OwnerLossSignal,
        objects: Arc<dyn LifecycleObjectDeleter>,
        options: LifecycleRunnerOptions,
    ) -> Result<Self, LifecycleError> {
        route
            .validate()
            .map_err(|error| LifecycleError::InvalidOptions(error.to_string()))?;
        let runner = Self {
            meta,
            registry,
            route,
            owner_loss,
            objects,
            options: options.validate()?,
            cursors: Mutex::new(LifecycleCursors::default()),
        };
        runner.require_current_owner()?;
        Ok(runner)
    }

    /// Execute one bounded recovery pass over every lifecycle family.
    pub fn run_once(&self, observed_now_ms: u64) -> Result<LifecycleCycleReport, LifecycleError> {
        self.require_current_owner()?;
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| LifecycleError::WorkerLockPoisoned)?;
        let mut report = LifecycleCycleReport::default();
        self.recover_publications(&mut cursors, observed_now_ms, &mut report)?;
        self.reap_snapshots(&mut cursors, observed_now_ms, &mut report)?;
        self.recover_restores(&mut cursors, &mut report)?;
        self.recover_commit_builds(&mut cursors, &mut report)?;
        self.retire_commits(&mut cursors, &mut report)?;
        self.collect_revisions(&mut cursors, &mut report)?;
        Ok(report)
    }

    /// Run continuously until ownership is lost or a non-retryable invariant
    /// failure requires operator attention.
    pub fn run_until_owner_loss(&self) -> Result<(), LifecycleError> {
        loop {
            let observed_now_ms = match unix_time_ms() {
                Ok(observed_now_ms) => observed_now_ms,
                Err(error) => return Err(self.fail_closed_error(error)),
            };
            match self.run_once(observed_now_ms) {
                Ok(_) => {}
                Err(error) if retryable_lifecycle(&error) => {}
                Err(error) => return Err(self.fail_closed_error(error)),
            }
            if let Err(error) = self.wait_poll_interval_or_owner_loss() {
                return Err(self.fail_closed_error(error));
            }
        }
    }

    fn fail_closed_error(&self, primary: LifecycleError) -> LifecycleError {
        match self.registry.fail_closed_shard(self.route.logical_shard_id) {
            Ok(()) => primary,
            Err(fence) => LifecycleError::OwnerLost(format!(
                "{primary}; logical-shard route/response fail-close failed: {fence}"
            )),
        }
    }

    fn wait_poll_interval_or_owner_loss(&self) -> Result<(), LifecycleError> {
        let deadline = Instant::now()
            .checked_add(self.options.poll_interval)
            .ok_or_else(|| {
                LifecycleError::InvalidOptions("poll interval overflows monotonic time".to_owned())
            })?;
        loop {
            self.require_current_owner()?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(());
            }
            thread::sleep(remaining.min(OWNER_LOSS_POLL_SLICE));
        }
    }

    fn recover_publications(
        &self,
        cursors: &mut LifecycleCursors,
        observed_now_ms: u64,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let prefix = meta::operation_prefix(self.root_id(), OperationKind::Publish);
        let rows = self.scan_page(
            meta::MetadataFamily::Operation,
            &prefix,
            cursors.publish_operation.as_deref(),
        )?;
        advance_key_cursor(
            &mut cursors.publish_operation,
            &rows,
            self.options.scan_page_size,
        );
        for item in rows {
            let operation_id =
                meta::decode_operation_key(self.root_id(), OperationKind::Publish, &item.key)
                    .ok_or_else(|| corrupt("publish operation key", "malformed root/kind key"))?;
            let operation = meta::PublishOperationRecord::decode(&item.value)
                .map_err(|error| corrupt("publish operation", error.to_string()))?;
            if operation.operation_id != operation_id {
                return Err(corrupt(
                    "publish operation",
                    "payload identity differs from key",
                ));
            }
            match operation.phase {
                PublishPhase::Uploading | PublishPhase::Finalizing => {
                    let current_owner_epoch = self.owner_epoch();
                    let lease_expiry_threshold = operation
                        .activity_deadline_ms
                        .saturating_add(self.options.maximum_publish_clock_skew_ms);
                    if current_owner_epoch.get() > operation.initiating_owner_epoch.get()
                        || (current_owner_epoch == operation.initiating_owner_epoch
                            && observed_now_ms >= lease_expiry_threshold)
                    {
                        self.take_over_orphaned_publication(operation, observed_now_ms, report)?;
                    } else if current_owner_epoch.get() < operation.initiating_owner_epoch.get() {
                        return Err(corrupt(
                            "publish operation",
                            format!(
                                "initiating owner epoch {} is newer than current epoch {}",
                                operation.initiating_owner_epoch.get(),
                                current_owner_epoch.get()
                            ),
                        ));
                    }
                }
                PublishPhase::Aborting => {
                    let context = self.publication_context(
                        b"publish-begin-cleaning",
                        &operation
                            .encode()
                            .map_err(|error| corrupt("publish operation", error.to_string()))?,
                    )?;
                    match meta::PublicationService::new(&self.meta).transition_publish(
                        meta::TransitionPublishRequest {
                            context,
                            expected_operation: operation,
                            transition: meta::PublishTransition::BeginCleaning,
                        },
                    ) {
                        Ok(_) => report.metadata_transitions += 1,
                        Err(error) if publication_concurrent(&error) => {
                            report.deferred_operations += 1;
                        }
                        Err(error) => return Err(state("begin publish cleanup", error)),
                    }
                }
                PublishPhase::Cleaning => self.clean_publication(operation, report)?,
                _ => {}
            }
        }
        Ok(())
    }

    fn take_over_orphaned_publication(
        &self,
        operation: meta::PublishOperationRecord,
        observed_now_ms: u64,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let encoded = operation
            .encode()
            .map_err(|error| corrupt("publish operation", error.to_string()))?;
        let context = self.publication_context(b"publish-owner-takeover", &encoded)?;
        let initiating_owner_epoch = operation.initiating_owner_epoch;
        let (terminal_kind, terminal_message) =
            if context.owner_epoch.get() > initiating_owner_epoch.get() {
                (
                    meta::PublishTerminalErrorKind::OwnerEpochSuperseded,
                    format!(
                        "publish owner epoch {} was superseded by epoch {}",
                        initiating_owner_epoch.get(),
                        context.owner_epoch.get()
                    ),
                )
            } else {
                (
                    meta::PublishTerminalErrorKind::ActivityLeaseExpired,
                    format!(
                        "publish activity deadline {} expired in owner epoch {}",
                        operation.activity_deadline_ms,
                        initiating_owner_epoch.get()
                    ),
                )
            };
        match meta::PublicationService::new(&self.meta).take_over_orphaned_publish(
            meta::TakeOverOrphanedPublishRequest {
                context,
                expected_operation: operation,
                observed_now_ms,
                maximum_clock_skew_ms: self.options.maximum_publish_clock_skew_ms,
                terminal_error: meta::PublishTerminalError {
                    kind: terminal_kind,
                    message: terminal_message,
                    evidence_digest: None,
                },
            },
        ) {
            Ok(_) => report.metadata_transitions += 1,
            Err(error) if publication_concurrent(&error) => report.deferred_operations += 1,
            Err(error) => return Err(state("take over orphaned publish", error)),
        }
        Ok(())
    }

    fn clean_publication(
        &self,
        operation: meta::PublishOperationRecord,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let service = meta::PublicationService::new(&self.meta);
        // A cleaning operation's staged object keys and manifest rows are
        // revision-scoped, so once that revision is published (or claimed by
        // another in-flight operation) they belong to that operation. The
        // begin-time revision claim prevents two live owners, but a claimless
        // pre-claim operation can still share its revision with a claimed one
        // during a rolling upgrade: quarantine when the revision is already
        // published, defer while another operation holds the claim so its
        // outcome decides. The checks and the provider deletes below are not
        // one atomic unit, so a claimless-vs-claimless upgrade-window race
        // remains between one check and one delete batch; the claim makes
        // that window impossible once both sides took one.
        let destructive = operation.cleanup_staged_object_cursor < operation.staged_object_cursor
            || operation.cleanup_manifest_cursor < operation.manifest_cursor;
        if destructive {
            if self.revision_published(operation.artifact_revision_id)? {
                return self.quarantine_publication(
                    operation,
                    b"aborted publish cleanup found its artifact revision published".to_vec(),
                    report,
                );
            }
            if let Some(claim_owner) = self.revision_claim_owner(operation.artifact_revision_id)? {
                if claim_owner != operation.operation_id {
                    report.deferred_operations += 1;
                    return Ok(());
                }
            }
        }
        if operation.cleanup_staged_object_cursor < operation.staged_object_cursor {
            let remaining = operation
                .staged_object_cursor
                .saturating_sub(operation.cleanup_staged_object_cursor)
                as usize;
            let count = remaining.min(self.options.mutation_batch_size);
            let mut updates = Vec::with_capacity(count);
            for offset in 0..count {
                let sequence = operation.cleanup_staged_object_cursor
                    + u32::try_from(offset).expect("bounded batch offset fits u32");
                let expected = self.read_staged_object(operation.operation_id, sequence)?;
                if matches!(expected.provider_state, StagedProviderState::Ambiguous)
                    || matches!(expected.cleanup_state, StagedCleanupState::Quarantined)
                {
                    return self.quarantine_publication(
                        operation,
                        b"durable staged-object state is ambiguous".to_vec(),
                        report,
                    );
                }
                if !matches!(
                    (expected.provider_state, expected.cleanup_state),
                    (StagedProviderState::Aborted, StagedCleanupState::Deleted)
                ) {
                    self.require_current_owner()?;
                    let request = LifecycleDeleteRequest {
                        purpose: LifecycleDeletePurpose::AbortedPublication,
                        object_key: expected.object_key.clone(),
                        multipart_upload_id: expected.multipart_upload_id.clone(),
                    };
                    match self.objects.delete(&request) {
                        Ok(_) => report.provider_deletions += 1,
                        Err(LifecycleDeleteError::Retryable { .. }) => {
                            report.deferred_operations += 1;
                            return Ok(());
                        }
                        Err(LifecycleDeleteError::Ambiguous { evidence }) => {
                            return self.quarantine_publication(operation, evidence, report);
                        }
                    }
                }
                let mut next = expected.clone();
                next.provider_state = StagedProviderState::Aborted;
                next.cleanup_state = StagedCleanupState::Deleted;
                updates.push(meta::StagedObjectUpdate { expected, next });
            }
            let encoded = operation
                .encode()
                .map_err(|error| corrupt("publish operation", error.to_string()))?;
            let context = self.publication_context(b"publish-clean-staged", &encoded)?;
            match service.cleanup_publish_batch(meta::CleanupPublishBatchRequest {
                context,
                expected_operation: operation,
                staged_object_updates: updates,
            }) {
                Ok(_) => report.metadata_transitions += 1,
                Err(error) if publication_concurrent(&error) => report.deferred_operations += 1,
                Err(error) => return Err(state("clean publish staged objects", error)),
            }
            return Ok(());
        }
        if operation.cleanup_manifest_cursor < operation.manifest_cursor {
            let encoded = operation
                .encode()
                .map_err(|error| corrupt("publish operation", error.to_string()))?;
            let context = self.publication_context(b"publish-clean-manifest", &encoded)?;
            match service.cleanup_publish_batch(meta::CleanupPublishBatchRequest {
                context,
                expected_operation: operation,
                staged_object_updates: Vec::new(),
            }) {
                Ok(_) => report.metadata_transitions += 1,
                Err(error) if publication_concurrent(&error) => report.deferred_operations += 1,
                Err(error) => return Err(state("clean publish manifest", error)),
            }
            return Ok(());
        }
        let encoded = operation
            .encode()
            .map_err(|error| corrupt("publish operation", error.to_string()))?;
        let context = self.publication_context(b"publish-finish-cleanup", &encoded)?;
        match service.transition_publish(meta::TransitionPublishRequest {
            context,
            expected_operation: operation,
            transition: meta::PublishTransition::FinishCleanup,
        }) {
            Ok(_) => report.metadata_transitions += 1,
            Err(error) if publication_concurrent(&error) => report.deferred_operations += 1,
            Err(error) => return Err(state("finish publish cleanup", error)),
        }
        Ok(())
    }

    fn quarantine_publication(
        &self,
        operation: meta::PublishOperationRecord,
        evidence: Vec<u8>,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let evidence = nonempty_evidence(evidence, b"provider returned empty ambiguity evidence");
        let evidence_digest = Sha256::digest(&evidence).into();
        let encoded = operation
            .encode()
            .map_err(|error| corrupt("publish operation", error.to_string()))?;
        let context = self.publication_context(b"publish-quarantine", &encoded)?;
        match meta::PublicationService::new(&self.meta).transition_publish(
            meta::TransitionPublishRequest {
                context,
                expected_operation: operation,
                transition: meta::PublishTransition::Quarantine {
                    terminal_error: meta::PublishTerminalError {
                        kind: meta::PublishTerminalErrorKind::CleanupFailed,
                        message: "provider cleanup outcome is ambiguous".to_owned(),
                        evidence_digest: Some(evidence_digest),
                    },
                },
            },
        ) {
            Ok(_) => {
                report.metadata_transitions += 1;
                report.quarantined_operations += 1;
            }
            Err(error) if publication_concurrent(&error) => report.deferred_operations += 1,
            Err(error) => return Err(state("quarantine publish cleanup", error)),
        }
        Ok(())
    }

    fn revision_published(&self, revision: ArtifactRevisionId) -> Result<bool, LifecycleError> {
        let context = self.read_context()?;
        let key = meta::artifact_revision_key(self.root_id(), revision);
        Ok(self
            .meta
            .read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                meta::MetadataFamily::ArtifactRevision,
                &key,
                context.read_version,
            )?
            .is_some())
    }

    fn revision_claim_owner(
        &self,
        revision: ArtifactRevisionId,
    ) -> Result<Option<OperationId>, LifecycleError> {
        let context = self.read_context()?;
        let key = meta::artifact_revision_claim_key(self.root_id(), revision);
        self.meta
            .read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                meta::MetadataFamily::ArtifactRevision,
                &key,
                context.read_version,
            )?
            .map(|payload| {
                meta::ArtifactRevisionClaimRecord::decode(&payload)
                    .map(|claim| claim.operation_id)
                    .map_err(|error| corrupt("artifact revision claim", error.to_string()))
            })
            .transpose()
    }

    fn read_staged_object(
        &self,
        operation_id: OperationId,
        sequence: u32,
    ) -> Result<meta::StagedObjectRecord, LifecycleError> {
        let context = self.read_context()?;
        let key = meta::staged_object_key(self.root_id(), operation_id, u64::from(sequence));
        let payload = self
            .meta
            .read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                meta::MetadataFamily::StagedObject,
                &key,
                context.read_version,
            )?
            .ok_or_else(|| corrupt("staged object", format!("sequence {sequence} is missing")))?;
        meta::StagedObjectRecord::decode(&payload)
            .map_err(|error| corrupt("staged object", error.to_string()))
    }

    fn reap_snapshots(
        &self,
        cursors: &mut LifecycleCursors,
        observed_now_ms: u64,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let prefix = meta::workspace_current_prefix(self.root_id());
        let workspaces = self.scan_page_with_limit(
            meta::MetadataFamily::WorkspaceCurrent,
            &prefix,
            cursors.snapshot_workspace.as_deref(),
            1,
        )?;
        let Some(workspace_item) = workspaces.first() else {
            cursors.snapshot_workspace = None;
            cursors.snapshot = None;
            return Ok(());
        };
        let workbench = meta::decode_workspace_current_key(self.root_id(), &workspace_item.key)
            .ok_or_else(|| corrupt("workspace marker key", "malformed workbench identity"))?;
        let workspace = meta::WorkspaceRecord::decode(&workspace_item.value)
            .map_err(|error| corrupt("workspace marker", error.to_string()))?;
        if workspace.state != WorkspaceState::Visible {
            cursors.snapshot_workspace = Some(workspace_item.key.clone());
            cursors.snapshot = None;
            return Ok(());
        }

        let snapshot_prefix = meta::snapshot_ref_prefix(self.root_id(), workspace.incarnation_id);
        let rows = self.scan_page(
            meta::MetadataFamily::SnapshotRef,
            &snapshot_prefix,
            cursors.snapshot.as_deref(),
        )?;
        for item in &rows {
            let (incarnation, snapshot_id) =
                meta::decode_snapshot_ref_key(self.root_id(), &item.key)
                    .ok_or_else(|| corrupt("snapshot key", "malformed snapshot identity"))?;
            if incarnation != workspace.incarnation_id {
                return Err(corrupt(
                    "snapshot key",
                    "workspace incarnation differs from scan prefix",
                ));
            }
            let snapshot = meta::SnapshotRefRecord::decode(&item.value)
                .map_err(|error| corrupt("snapshot", error.to_string()))?;
            match snapshot.state {
                SnapshotState::Active
                    if snapshot.consumer_count == 0
                        && observed_now_ms
                            >= snapshot
                                .lease_deadline_ms
                                .saturating_add(self.options.maximum_snapshot_clock_skew_ms) =>
                {
                    let context = self.write_context(
                        b"snapshot-claim-expired",
                        &[&item.key, &item.value, &observed_now_ms.to_be_bytes()],
                    )?;
                    match meta::claim_expired_snapshot(
                        &self.meta,
                        context,
                        &meta::ClaimExpiredSnapshotRequest {
                            workbench_id: workbench.clone(),
                            snapshot_id,
                            observed_now_ms,
                            maximum_clock_skew_ms: self.options.maximum_snapshot_clock_skew_ms,
                        },
                    ) {
                        Ok(_) => report.metadata_transitions += 1,
                        Err(error) if snapshot_concurrent(&error) => {
                            report.deferred_operations += 1;
                        }
                        Err(error) => return Err(state("claim expired snapshot", error)),
                    }
                }
                SnapshotState::ReapClaimed => {
                    let context =
                        self.write_context(b"snapshot-finish-reap", &[&item.key, &item.value])?;
                    match meta::finish_snapshot_reap(
                        &self.meta,
                        context,
                        &meta::FinishSnapshotReapRequest {
                            workbench_id: workbench.clone(),
                            snapshot_id,
                        },
                    ) {
                        Ok(_) => report.metadata_transitions += 1,
                        Err(error) if snapshot_concurrent(&error) => {
                            report.deferred_operations += 1;
                        }
                        Err(error) => return Err(state("finish snapshot reap", error)),
                    }
                }
                _ => {}
            }
        }
        if rows.len() < self.options.scan_page_size {
            cursors.snapshot_workspace = Some(workspace_item.key.clone());
            cursors.snapshot = None;
        } else {
            cursors.snapshot = rows.last().map(|item| item.key.clone());
        }
        Ok(())
    }

    fn recover_restores(
        &self,
        cursors: &mut LifecycleCursors,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let prefix = meta::operation_prefix(self.root_id(), OperationKind::Restore);
        let rows = self.scan_page(
            meta::MetadataFamily::Operation,
            &prefix,
            cursors.restore_operation.as_deref(),
        )?;
        advance_key_cursor(
            &mut cursors.restore_operation,
            &rows,
            self.options.scan_page_size,
        );
        for item in rows {
            let operation_id =
                meta::decode_operation_key(self.root_id(), OperationKind::Restore, &item.key)
                    .ok_or_else(|| corrupt("restore operation key", "malformed root/kind key"))?;
            let operation = meta::RestoreOperationRecord::decode(&item.value)
                .map_err(|error| corrupt("restore operation", error.to_string()))?;
            if operation.operation_id != operation_id {
                return Err(corrupt(
                    "restore operation",
                    "payload identity differs from key",
                ));
            }
            let request = meta::RestoreOperationRequest { operation_id };
            match operation.phase {
                RestorePhase::Aborting => {
                    let context =
                        self.write_context(b"restore-begin-cleaning", &[&item.key, &item.value])?;
                    match meta::start_restore_cleanup(&self.meta, context, request) {
                        Ok(_) => report.metadata_transitions += 1,
                        Err(error) if restore_concurrent(&error) => {
                            report.deferred_operations += 1;
                        }
                        Err(error) => return Err(state("begin restore cleanup", error)),
                    }
                }
                RestorePhase::Cleaning
                    if operation.cleanup_member_cursor < operation.next_member_sequence =>
                {
                    let context =
                        self.write_context(b"restore-clean-members", &[&item.key, &item.value])?;
                    match meta::cleanup_restore_batch(
                        &self.meta,
                        context,
                        meta::CopyRestoreBatchRequest {
                            operation_id,
                            limit: self.options.mutation_batch_size,
                        },
                    ) {
                        Ok(_) => report.metadata_transitions += 1,
                        Err(error) if restore_concurrent(&error) => {
                            report.deferred_operations += 1;
                        }
                        Err(error) => return Err(state("clean restore members", error)),
                    }
                }
                RestorePhase::Cleaning => {
                    let context =
                        self.write_context(b"restore-finish-cleaning", &[&item.key, &item.value])?;
                    match meta::finish_restore_cleanup(&self.meta, context, request) {
                        Ok(_) => report.metadata_transitions += 1,
                        Err(error) if restore_concurrent(&error) => {
                            report.deferred_operations += 1;
                        }
                        Err(error) => return Err(state("finish restore cleanup", error)),
                    }
                }
                RestorePhase::Quarantined => {
                    report.quarantined_operations += 1;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn recover_commit_builds(
        &self,
        cursors: &mut LifecycleCursors,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let prefix = meta::operation_prefix(self.root_id(), OperationKind::BuildCommit);
        let rows = self.scan_page(
            meta::MetadataFamily::Operation,
            &prefix,
            cursors.build_operation.as_deref(),
        )?;
        advance_key_cursor(
            &mut cursors.build_operation,
            &rows,
            self.options.scan_page_size,
        );
        let service = meta::CommitService::new(&self.meta);
        for item in rows {
            let operation_id = meta::decode_operation_key(
                self.root_id(),
                OperationKind::BuildCommit,
                &item.key,
            )
            .ok_or_else(|| corrupt("commit-build operation key", "malformed root/kind key"))?;
            let operation = meta::BuildCommitOperationRecord::decode(&item.value)
                .map_err(|error| corrupt("commit-build operation", error.to_string()))?;
            if operation.operation_id != operation_id {
                return Err(corrupt(
                    "commit-build operation",
                    "payload identity differs from key",
                ));
            }
            match operation.phase {
                BuildCommitPhase::Aborting | BuildCommitPhase::Cleaning => {
                    let context =
                        self.write_context(b"commit-build-cleanup", &[&item.key, &item.value])?;
                    match service.cleanup_build(meta::BuildCommitStepRequest {
                        context,
                        operation_id,
                        limit: self.options.mutation_batch_size,
                    }) {
                        Ok(_) => report.metadata_transitions += 1,
                        Err(error) if commit_concurrent(&error) => {
                            report.deferred_operations += 1;
                        }
                        Err(error) => return Err(state("clean commit build", error)),
                    }
                }
                BuildCommitPhase::Quarantined => {
                    report.quarantined_operations += 1;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn retire_commits(
        &self,
        cursors: &mut LifecycleCursors,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let operation_prefix = meta::operation_prefix(self.root_id(), OperationKind::CommitRetire);
        let operations = self.scan_page(
            meta::MetadataFamily::Operation,
            &operation_prefix,
            cursors.retire_operation.as_deref(),
        )?;
        advance_key_cursor(
            &mut cursors.retire_operation,
            &operations,
            self.options.scan_page_size,
        );
        let service = meta::CommitService::new(&self.meta);
        for item in operations {
            let operation_id =
                meta::decode_operation_key(self.root_id(), OperationKind::CommitRetire, &item.key)
                    .ok_or_else(|| {
                        corrupt("commit-retire operation key", "malformed root/kind key")
                    })?;
            let operation = meta::CommitRetireOperationRecord::decode(&item.value)
                .map_err(|error| corrupt("commit-retire operation", error.to_string()))?;
            if operation.operation_id != operation_id {
                return Err(corrupt(
                    "commit-retire operation",
                    "payload identity differs from key",
                ));
            }
            match operation.phase {
                CommitRetirePhase::Claiming | CommitRetirePhase::Releasing => {
                    let context =
                        self.write_context(b"commit-retire-release", &[&item.key, &item.value])?;
                    match service.release_retired_commit(meta::BuildCommitStepRequest {
                        context,
                        operation_id,
                        limit: self.options.mutation_batch_size,
                    }) {
                        Ok(_) => report.metadata_transitions += 1,
                        Err(error) if commit_concurrent(&error) => {
                            report.deferred_operations += 1;
                        }
                        Err(error) => return Err(state("release retired commit", error)),
                    }
                }
                CommitRetirePhase::Quarantined => {
                    report.quarantined_operations += 1;
                }
                CommitRetirePhase::Complete => {}
            }
        }

        let commit_prefix = meta::commit_prefix(self.root_id());
        let commits = self.scan_page(
            meta::MetadataFamily::Commit,
            &commit_prefix,
            cursors.commit.as_deref(),
        )?;
        advance_key_cursor(&mut cursors.commit, &commits, self.options.scan_page_size);
        for item in commits {
            let commit_id = meta::decode_commit_key(self.root_id(), &item.key)
                .ok_or_else(|| corrupt("commit key", "malformed commit identity"))?;
            let commit = meta::CommitRecord::decode(&item.value)
                .map_err(|error| corrupt("commit", error.to_string()))?;
            if commit.state != CommitState::Sealed || commit.consumer_count != 0 {
                continue;
            }
            let operation_id = derived_operation_id(
                b"nokv.lifecycle.commit-retire-operation.v1",
                &[
                    self.root_id().as_bytes(),
                    commit_id.as_bytes(),
                    &commit.consumer_epoch.get().to_be_bytes(),
                ],
            );
            let context = self.write_context(
                b"commit-retire-claim",
                &[&item.key, &item.value, operation_id.as_bytes()],
            )?;
            match service.begin_retirement(meta::BeginCommitRetirementRequest {
                context,
                operation_id,
                commit_id,
                expected_consumer_epoch: commit.consumer_epoch,
            }) {
                Ok(_) => report.metadata_transitions += 1,
                Err(error) if commit_concurrent(&error) => report.deferred_operations += 1,
                Err(error) => return Err(state("claim commit retirement", error)),
            }
        }
        Ok(())
    }

    fn collect_revisions(
        &self,
        cursors: &mut LifecycleCursors,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let read_context = self.read_context()?;
        let service = meta::GcService::new(&self.meta);
        let page = service
            .list_candidates(
                read_context,
                cursors.gc_candidate,
                self.options.scan_page_size,
            )
            .map_err(|error| state("list GC candidates", error))?;
        cursors.gc_candidate = page.next_cursor;
        for entry in page.entries {
            match entry.candidate.claim_state {
                GcClaimState::Candidate => {
                    let context = self.write_context(
                        b"gc-claim",
                        &[
                            entry.cursor.artifact_revision_id.as_bytes(),
                            &entry.cursor.reference_epoch.get().to_be_bytes(),
                            &entry
                                .candidate
                                .encode()
                                .map_err(|error| corrupt("GC candidate", error.to_string()))?,
                        ],
                    )?;
                    let claim_read_version = context.read_version.get();
                    match service.claim(meta::ClaimGcRequest {
                        context,
                        artifact_revision_id: entry.cursor.artifact_revision_id,
                        reference_epoch: entry.cursor.reference_epoch,
                    }) {
                        Ok(_) => report.metadata_transitions += 1,
                        Err(meta::GcError::UnsafeHistoryFloor { last_zero, floor }) => {
                            if last_zero == floor && floor == claim_read_version {
                                let context = self.write_context(
                                    b"gc-history-barrier",
                                    &[
                                        entry.cursor.artifact_revision_id.as_bytes(),
                                        &entry.cursor.reference_epoch.get().to_be_bytes(),
                                        &floor.to_be_bytes(),
                                    ],
                                )?;
                                match service.advance_history_barrier(context) {
                                    Ok(_) => report.metadata_transitions += 1,
                                    Err(meta::GcError::HistoryHoldActive) => {
                                        report.deferred_operations += 1;
                                    }
                                    Err(error) if gc_concurrent(&error) => {
                                        report.deferred_operations += 1;
                                    }
                                    Err(error) => {
                                        return Err(state("advance GC history barrier", error));
                                    }
                                }
                            } else {
                                report.deferred_operations += 1;
                            }
                        }
                        Err(meta::GcError::ReferenceEpochMismatch { .. }) => {
                            let context = self.write_context(
                                b"gc-clear-stale",
                                &[
                                    entry.cursor.artifact_revision_id.as_bytes(),
                                    &entry.cursor.reference_epoch.get().to_be_bytes(),
                                ],
                            )?;
                            match service.clear_stale_candidate(
                                meta::ClearStaleGcCandidateRequest {
                                    context,
                                    artifact_revision_id: entry.cursor.artifact_revision_id,
                                    reference_epoch: entry.cursor.reference_epoch,
                                },
                            ) {
                                Ok(_) => report.metadata_transitions += 1,
                                Err(error) if gc_concurrent(&error) => {
                                    report.deferred_operations += 1;
                                }
                                Err(error) => return Err(state("clear stale GC candidate", error)),
                            }
                        }
                        Err(error) if gc_concurrent(&error) => report.deferred_operations += 1,
                        Err(error) => return Err(state("claim GC candidate", error)),
                    }
                }
                GcClaimState::Claimed => {
                    self.resume_gc(entry.cursor, report)?;
                }
                GcClaimState::Complete | GcClaimState::Quarantined => {}
            }
        }
        Ok(())
    }

    fn resume_gc(
        &self,
        cursor: meta::GcCandidateCursor,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let operation_id = meta::gc_operation_id(
            self.root_id(),
            cursor.artifact_revision_id,
            cursor.reference_epoch,
        );
        let operation_key = meta::operation_key(self.root_id(), OperationKind::Gc, operation_id);
        let read = self.read_context()?;
        let payload = self
            .meta
            .read_at(
                read.root_id,
                read.placement_generation,
                read.owner_epoch,
                meta::MetadataFamily::Operation,
                &operation_key,
                read.read_version,
            )?
            .ok_or_else(|| corrupt("GC operation", "claimed candidate has no operation"))?;
        let operation = meta::GcOperationRecord::decode(&payload)
            .map_err(|error| corrupt("GC operation", error.to_string()))?;
        if operation.operation_id != operation_id {
            return Err(corrupt("GC operation", "payload identity differs from key"));
        }
        let service = meta::GcService::new(&self.meta);
        match operation.phase {
            GcPhase::Claimed => {
                let context =
                    self.write_context(b"gc-begin-delete", &[&operation_key, &payload])?;
                match service.begin_deletion(meta::BeginGcDeletionRequest {
                    context,
                    expected_operation: operation,
                }) {
                    Ok(_) => report.metadata_transitions += 1,
                    Err(error) if gc_concurrent(&error) => report.deferred_operations += 1,
                    Err(error) => return Err(state("begin GC deletion", error)),
                }
            }
            GcPhase::Deleting => self.delete_gc_batch(operation, report)?,
            GcPhase::Deleted | GcPhase::Quarantined | GcPhase::Queued => {}
        }
        Ok(())
    }

    fn delete_gc_batch(
        &self,
        operation: meta::GcOperationRecord,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let service = meta::GcService::new(&self.meta);
        let scan_context = self.write_context(
            b"gc-scan-manifest",
            &[&operation
                .encode()
                .map_err(|error| corrupt("GC operation", error.to_string()))?],
        )?;
        let batch = match service.scan_manifest_batch(
            scan_context,
            &operation,
            self.options.mutation_batch_size,
        ) {
            Ok(batch) => batch,
            Err(error) if gc_concurrent(&error) => {
                report.deferred_operations += 1;
                return Ok(());
            }
            Err(error) => return Err(state("scan GC manifest", error)),
        };
        if batch.entries.is_empty() {
            if !batch.end_of_manifest {
                return Err(corrupt(
                    "GC manifest",
                    "empty non-terminal authoritative page",
                ));
            }
            let context = self.write_context(
                b"gc-complete",
                &[&operation
                    .encode()
                    .map_err(|error| corrupt("GC operation", error.to_string()))?],
            )?;
            match service.complete(meta::CompleteGcRequest {
                context,
                expected_operation: operation,
            }) {
                Ok(_) => report.metadata_transitions += 1,
                Err(error) if gc_concurrent(&error) => report.deferred_operations += 1,
                Err(error) => return Err(state("complete GC", error)),
            }
            return Ok(());
        }

        let mut confirmations = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            let absence_digest = if entry.delete_required {
                self.require_current_owner()?;
                let request = LifecycleDeleteRequest {
                    purpose: LifecycleDeletePurpose::RevisionGarbageCollection,
                    object_key: entry.row.object_key.clone(),
                    multipart_upload_id: None,
                };
                match self.objects.delete(&request) {
                    Ok(proof) => {
                        report.provider_deletions += 1;
                        Some(proof.digest)
                    }
                    Err(LifecycleDeleteError::Retryable { .. }) => {
                        report.deferred_operations += 1;
                        return Ok(());
                    }
                    Err(LifecycleDeleteError::Ambiguous { evidence }) => {
                        return self.quarantine_gc(operation, evidence, report);
                    }
                }
            } else {
                None
            };
            confirmations.push(meta::GcObjectAbsence {
                position: entry.position,
                object_key: entry.row.object_key,
                absence_digest,
            });
        }
        let context = self.write_context(
            b"gc-advance-delete",
            &[&operation
                .encode()
                .map_err(|error| corrupt("GC operation", error.to_string()))?],
        )?;
        match service.advance_deletion_batch(meta::AdvanceGcDeletionBatchRequest {
            context,
            expected_operation: operation,
            confirmations,
        }) {
            Ok(_) => report.metadata_transitions += 1,
            Err(error) if gc_concurrent(&error) => report.deferred_operations += 1,
            Err(error) => return Err(state("advance GC deletion", error)),
        }
        Ok(())
    }

    fn quarantine_gc(
        &self,
        operation: meta::GcOperationRecord,
        evidence: Vec<u8>,
        report: &mut LifecycleCycleReport,
    ) -> Result<(), LifecycleError> {
        let mut evidence =
            nonempty_evidence(evidence, b"provider returned empty ambiguity evidence");
        evidence.truncate(meta::MAX_GC_EVIDENCE_BYTES);
        let context = self.write_context(
            b"gc-quarantine",
            &[
                &operation
                    .encode()
                    .map_err(|error| corrupt("GC operation", error.to_string()))?,
                &evidence,
            ],
        )?;
        match meta::GcService::new(&self.meta).quarantine(meta::QuarantineGcRequest {
            context,
            expected_operation: operation,
            evidence,
        }) {
            Ok(_) => {
                report.metadata_transitions += 1;
                report.quarantined_operations += 1;
            }
            Err(error) if gc_concurrent(&error) => report.deferred_operations += 1,
            Err(error) => return Err(state("quarantine GC", error)),
        }
        Ok(())
    }

    fn scan_page(
        &self,
        family: meta::MetadataFamily,
        prefix: &[u8],
        start_after: Option<&[u8]>,
    ) -> Result<Vec<meta::MetadataScanItem>, LifecycleError> {
        self.scan_page_with_limit(family, prefix, start_after, self.options.scan_page_size)
    }

    fn scan_page_with_limit(
        &self,
        family: meta::MetadataFamily,
        prefix: &[u8],
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<meta::MetadataScanItem>, LifecycleError> {
        let context = self.read_context()?;
        self.meta
            .scan_prefix_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                family,
                prefix,
                context.read_version,
                start_after,
                limit,
            )
            .map_err(Into::into)
    }

    fn read_context(&self) -> Result<meta::RootReadContext, LifecycleError> {
        self.require_current_owner()?;
        meta::RootReadContext::current(
            &self.meta,
            self.root_id(),
            self.placement_generation(),
            self.owner_epoch(),
        )
        .map_err(|error| LifecycleError::StateMachine {
            action: "capture read context",
            detail: error.to_string(),
        })
    }

    fn write_context(
        &self,
        domain: &'static [u8],
        inputs: &[&[u8]],
    ) -> Result<meta::RootWriteContext, LifecycleError> {
        self.require_current_owner()?;
        let read_version = self.meta.current_read_version()?;
        let request_id = derived_request_id(domain, read_version.get(), inputs);
        Ok(meta::RootWriteContext {
            root_id: self.root_id(),
            logical_shard_id: self.logical_shard_id(),
            placement_generation: self.placement_generation(),
            owner_epoch: self.owner_epoch(),
            request_id,
            read_version,
        })
    }

    fn publication_context(
        &self,
        domain: &'static [u8],
        input: &[u8],
    ) -> Result<meta::PublicationContext, LifecycleError> {
        let context = self.write_context(domain, &[input])?;
        Ok(meta::PublicationContext {
            root_id: context.root_id,
            logical_shard_id: context.logical_shard_id,
            placement_generation: context.placement_generation,
            owner_epoch: context.owner_epoch,
            request_id: context.request_id,
            read_version: context.read_version,
        })
    }

    fn require_current_owner(&self) -> Result<(), LifecycleError> {
        if self.owner_loss.is_lost() {
            return Err(LifecycleError::OwnerLost(
                "owner-loss signal is set".to_owned(),
            ));
        }
        if !self
            .registry
            .contains_exact(self.route)
            .map_err(|error| LifecycleError::OwnerLost(error.to_string()))?
        {
            return Err(LifecycleError::OwnerLost(
                "exact root route is no longer installed".to_owned(),
            ));
        }
        if self.meta.logical_shard_id() != self.logical_shard_id() {
            return Err(LifecycleError::OwnerLost(
                "metadata shard differs from the installed route".to_owned(),
            ));
        }
        if self.meta.current_owner_epoch()? != Some(self.owner_epoch()) {
            return Err(LifecycleError::OwnerLost(
                "persisted owner epoch differs from the installed route".to_owned(),
            ));
        }
        let fence = self
            .meta
            .root_fence(self.root_id())?
            .ok_or_else(|| LifecycleError::OwnerLost("root fence is missing".to_owned()))?;
        if fence.logical_shard_id != self.logical_shard_id()
            || fence.placement_generation != self.placement_generation()
            || fence.activation_state != RootActivationState::Active
        {
            return Err(LifecycleError::OwnerLost(
                "root fence is not the exact active installed route".to_owned(),
            ));
        }
        Ok(())
    }

    fn root_id(&self) -> RootId {
        self.route.root_id.into()
    }

    fn logical_shard_id(&self) -> nokv_types::LogicalShardId {
        self.route.logical_shard_id.into()
    }

    fn placement_generation(&self) -> PlacementGeneration {
        PlacementGeneration::new(self.route.placement_generation)
            .expect("validated route generation is non-zero")
    }

    fn owner_epoch(&self) -> OwnerEpoch {
        OwnerEpoch::new(self.route.owner_epoch).expect("validated route epoch is non-zero")
    }
}

fn advance_key_cursor(
    cursor: &mut Option<Vec<u8>>,
    rows: &[meta::MetadataScanItem],
    page_size: usize,
) {
    *cursor = (rows.len() == page_size)
        .then(|| rows.last().map(|item| item.key.clone()))
        .flatten();
}

fn derived_request_id(domain: &[u8], read_version: u64, inputs: &[&[u8]]) -> RequestId {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.lifecycle.request.v1\0");
    hash_part(&mut hasher, domain);
    hasher.update(read_version.to_be_bytes());
    for input in inputs {
        hash_part(&mut hasher, input);
    }
    let digest = hasher.finalize();
    RequestId::from_bytes(digest[..16].try_into().expect("SHA-256 has sixteen bytes"))
}

fn derived_operation_id(domain: &[u8], inputs: &[&[u8]]) -> OperationId {
    let mut hasher = Sha256::new();
    hash_part(&mut hasher, domain);
    for input in inputs {
        hash_part(&mut hasher, input);
    }
    let digest = hasher.finalize();
    OperationId::from_bytes(digest[..16].try_into().expect("SHA-256 has sixteen bytes"))
}

fn hash_part(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn nonempty_evidence(mut evidence: Vec<u8>, fallback: &[u8]) -> Vec<u8> {
    if evidence.is_empty() {
        evidence.extend_from_slice(fallback);
    }
    evidence
}

fn unix_time_ms() -> Result<u64, LifecycleError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LifecycleError::Clock(error.to_string()))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| LifecycleError::Clock("wall clock milliseconds exceed u64".to_owned()))
}

fn corrupt(record: &'static str, detail: impl Into<String>) -> LifecycleError {
    LifecycleError::CorruptMetadata {
        record,
        detail: detail.into(),
    }
}

fn state(action: &'static str, error: impl fmt::Display) -> LifecycleError {
    LifecycleError::StateMachine {
        action,
        detail: error.to_string(),
    }
}

fn concurrent_meta(error: &meta::MetaError) -> bool {
    matches!(
        error,
        meta::MetaError::WriteReadVersionMismatch { .. }
            | meta::MetaError::PredicateFailed
            | meta::MetaError::WriteConflict
            | meta::MetaError::ReadStabilityExhausted { .. }
    )
}

fn retryable_lifecycle(error: &LifecycleError) -> bool {
    matches!(
        error,
        LifecycleError::Meta(meta::MetaError::ReadStabilityExhausted { .. })
    )
}

fn publication_concurrent(error: &meta::PublicationError) -> bool {
    matches!(error, meta::PublicationError::Meta(source) if concurrent_meta(source))
}

fn snapshot_concurrent(error: &meta::SnapshotError) -> bool {
    matches!(error, meta::SnapshotError::ConcurrentMutation)
        || matches!(error, meta::SnapshotError::Meta(source) if concurrent_meta(source))
}

fn restore_concurrent(error: &meta::RestoreError) -> bool {
    matches!(error, meta::RestoreError::ConcurrentMutation)
        || matches!(error, meta::RestoreError::Meta(source) if concurrent_meta(source))
}

fn commit_concurrent(error: &meta::CommitError) -> bool {
    matches!(error, meta::CommitError::Meta(source) if concurrent_meta(source))
}

fn gc_concurrent(error: &meta::GcError) -> bool {
    matches!(error, meta::GcError::ConcurrentMutation)
        || matches!(error, meta::GcError::Meta(source) if concurrent_meta(source))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use nokv_object::{
        ArtifactStoreCapabilities, ImmutableCreateOutcome, ObjectError, ObjectInfo, ObjectRange,
    };
    use nokv_protocol::{RpcFailure, WorkspaceRpcRequest};
    use nokv_types::{
        ArtifactRevisionId, CommandDigest, CommitVersion, GcClaimState, HistoryHoldState,
        LogicalShardId, NormalizedRelativePath, ReferenceEpoch, RevisionState, RootActivationState,
        WorkbenchId, WorkspaceIncarnationId, FIXED_ID_BYTES,
    };

    use super::*;
    use crate::{ExecutedRequest, WorkspaceRequestExecutor};

    struct UnusedExecutor;

    impl WorkspaceRequestExecutor for UnusedExecutor {
        fn execute(&self, _request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure> {
            panic!("lifecycle test never dispatches RPC")
        }
    }

    #[test]
    fn unstable_metadata_reads_are_retryable_lifecycle_work() {
        let error = meta::MetaError::ReadStabilityExhausted { attempts: 4 };
        assert!(concurrent_meta(&error));
        assert!(retryable_lifecycle(&LifecycleError::Meta(error)));
    }

    struct FakeArtifactStore {
        delete_result: Result<ObjectDeleteOutcome, ObjectError>,
        delete_calls: AtomicUsize,
    }

    impl ArtifactObjectStore for FakeArtifactStore {
        fn capabilities(&self) -> ArtifactStoreCapabilities {
            ArtifactStoreCapabilities::default()
        }

        fn create_immutable(
            &self,
            _key: &ObjectKey,
            _bytes: &[u8],
        ) -> Result<ImmutableCreateOutcome, ObjectError> {
            panic!("lifecycle adapter test never creates objects")
        }

        fn read(
            &self,
            _key: &ObjectKey,
            _range: Option<ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            panic!("lifecycle adapter test never reads objects")
        }

        fn head(&self, _key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            panic!("lifecycle adapter test never heads objects")
        }

        fn delete(&self, _key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
            self.delete_calls.fetch_add(1, Ordering::SeqCst);
            self.delete_result.clone()
        }
    }

    fn lifecycle_delete_request() -> LifecycleDeleteRequest {
        LifecycleDeleteRequest {
            purpose: LifecycleDeletePurpose::RevisionGarbageCollection,
            object_key: "nokv/artifacts/object".to_owned(),
            multipart_upload_id: None,
        }
    }

    #[test]
    fn artifact_lifecycle_deleter_proves_deleted_object_absent() {
        let store = Arc::new(FakeArtifactStore {
            delete_result: Ok(ObjectDeleteOutcome::Deleted),
            delete_calls: AtomicUsize::new(0),
        });
        let deleter = ArtifactLifecycleDeleter::new(Arc::clone(&store));
        let request = lifecycle_delete_request();

        let proof = deleter.delete(&request).unwrap();

        assert_eq!(proof.disposition, LifecycleDeleteDisposition::Deleted);
        assert_eq!(
            proof,
            LifecycleAbsenceProof::from_delete_request(
                &request,
                LifecycleDeleteDisposition::Deleted,
            )
        );
        assert_eq!(store.delete_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn artifact_lifecycle_deleter_proves_already_absent_object() {
        let store = Arc::new(FakeArtifactStore {
            delete_result: Ok(ObjectDeleteOutcome::Absent),
            delete_calls: AtomicUsize::new(0),
        });
        let deleter = ArtifactLifecycleDeleter::new(Arc::clone(&store));
        let request = lifecycle_delete_request();

        let proof = deleter.delete(&request).unwrap();

        assert_eq!(proof.disposition, LifecycleDeleteDisposition::AlreadyAbsent);
        assert_eq!(store.delete_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn artifact_lifecycle_deleter_quarantines_multipart_before_dispatch() {
        let store = Arc::new(FakeArtifactStore {
            delete_result: Ok(ObjectDeleteOutcome::Deleted),
            delete_calls: AtomicUsize::new(0),
        });
        let deleter = ArtifactLifecycleDeleter::new(Arc::clone(&store));
        let mut request = lifecycle_delete_request();
        request.multipart_upload_id = Some(b"upload-id".to_vec());

        let error = deleter.delete(&request).unwrap_err();

        assert!(matches!(error, LifecycleDeleteError::Ambiguous { .. }));
        assert_eq!(store.delete_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn artifact_lifecycle_deleter_quarantines_provider_error() {
        let key = ObjectKey::new("nokv/artifacts/object").unwrap();
        let store = Arc::new(FakeArtifactStore {
            delete_result: Err(ObjectError::DeleteAmbiguous {
                key,
                detail: "timed out after dispatch".to_owned(),
            }),
            delete_calls: AtomicUsize::new(0),
        });
        let deleter = ArtifactLifecycleDeleter::new(Arc::clone(&store));

        let error = deleter.delete(&lifecycle_delete_request()).unwrap_err();

        assert!(matches!(error, LifecycleDeleteError::Ambiguous { .. }));
        assert_eq!(store.delete_calls.load(Ordering::SeqCst), 1);
    }

    struct FakeDeleter {
        ambiguous: bool,
        calls: AtomicUsize,
        object_keys: Mutex<Vec<String>>,
    }

    impl LifecycleObjectDeleter for FakeDeleter {
        fn delete(
            &self,
            request: &LifecycleDeleteRequest,
        ) -> Result<LifecycleAbsenceProof, LifecycleDeleteError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.object_keys
                .lock()
                .unwrap()
                .push(request.object_key.clone());
            assert_eq!(
                request.purpose,
                LifecycleDeletePurpose::RevisionGarbageCollection
            );
            if self.ambiguous {
                Err(LifecycleDeleteError::Ambiguous {
                    evidence: b"provider timed out after delete dispatch".to_vec(),
                })
            } else {
                Ok(LifecycleAbsenceProof::from_delete_request(
                    request,
                    LifecycleDeleteDisposition::Deleted,
                ))
            }
        }
    }

    struct Fixture {
        store: Arc<meta::MetaShard>,
        registry: Arc<RootOwnerRegistry>,
        route: RootRoute,
        target: ArtifactRevisionId,
        epoch: ReferenceEpoch,
        owned_object_keys: Vec<String>,
        borrowed_object_keys: Vec<String>,
    }

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
        OwnerEpoch::new(4).unwrap()
    }

    fn successor_owner() -> OwnerEpoch {
        OwnerEpoch::new(5).unwrap()
    }

    fn route() -> RootRoute {
        RootRoute {
            root_id: root().into(),
            logical_shard_id: shard().into(),
            placement_generation: placement().get(),
            owner_epoch: owner().get(),
        }
    }

    fn command(
        store: &meta::MetaShard,
        request: u8,
        action: meta::RootFenceAction,
        mutations: Vec<meta::CommandMutation>,
    ) -> meta::MetadataCommand {
        let predicates = mutations
            .iter()
            .map(|mutation| match mutation {
                meta::CommandMutation::Put { family, key, .. } => meta::CommandPredicate::Value {
                    family: *family,
                    key: key.clone(),
                    expected: None,
                },
                meta::CommandMutation::Delete { .. } => {
                    panic!("lifecycle test seeding never deletes metadata")
                }
            })
            .collect();
        meta::MetadataCommand {
            schema_id: meta::SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            placement_generation: placement(),
            owner_epoch: owner(),
            request_id: RequestId::from_bytes([request; FIXED_ID_BYTES]),
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: action,
            predicates,
            mutations,
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal()
    }

    fn sha256_uri(digest: [u8; SHA256_BYTES]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut value = String::from("sha256:");
        for byte in digest {
            value.push(HEX[usize::from(byte >> 4)] as char);
            value.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        value
    }

    fn fixture() -> Fixture {
        fixture_with_last_zero(2)
    }

    fn fixture_with_last_zero(last_zero: u64) -> Fixture {
        let store = crate::test_support::meta_shard(shard());
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&command(
                &store,
                1,
                meta::RootFenceAction::Install,
                Vec::new(),
            ))
            .unwrap();
        store
            .execute(&command(
                &store,
                2,
                meta::RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
                Vec::new(),
            ))
            .unwrap();

        let target = ArtifactRevisionId::from_bytes([5; FIXED_ID_BYTES]);
        let dependency_owner = ArtifactRevisionId::from_bytes([6; FIXED_ID_BYTES]);
        let epoch = ReferenceEpoch::new(1);
        let base_rows = [
            meta::ArtifactManifestRow {
                physical_owner_revision_id: dependency_owner,
                physical_object_index: 0,
                object_key: meta::object_block_key(shard(), root(), dependency_owner, 0),
                logical_offset: 0,
                offset: 0,
                length: 8,
                digest_uri: sha256_uri([0x31; SHA256_BYTES]),
                append_segment: None,
            },
            meta::ArtifactManifestRow {
                physical_owner_revision_id: dependency_owner,
                physical_object_index: 1,
                object_key: meta::object_block_key(shard(), root(), dependency_owner, 1),
                logical_offset: 8,
                offset: 0,
                length: 8,
                digest_uri: sha256_uri([0x32; SHA256_BYTES]),
                append_segment: None,
            },
        ];
        let delta_rows = vec![
            meta::ArtifactManifestRow {
                physical_owner_revision_id: target,
                physical_object_index: 0,
                object_key: meta::object_block_key(shard(), root(), target, 0),
                logical_offset: 16,
                offset: 0,
                length: 8,
                digest_uri: sha256_uri([0x33; SHA256_BYTES]),
                append_segment: Some(meta::AppendSegment {
                    segment_sequence: 0,
                    segment_offset: 0,
                }),
            },
            meta::ArtifactManifestRow {
                physical_owner_revision_id: target,
                physical_object_index: 1,
                object_key: meta::object_block_key(shard(), root(), target, 1),
                logical_offset: 24,
                offset: 0,
                length: 8,
                digest_uri: sha256_uri([0x34; SHA256_BYTES]),
                append_segment: Some(meta::AppendSegment {
                    segment_sequence: 0,
                    segment_offset: 8,
                }),
            },
        ];
        let child_rows = base_rows
            .iter()
            .chain(&delta_rows)
            .cloned()
            .collect::<Vec<_>>();
        let manifest_digest =
            child_rows
                .iter()
                .enumerate()
                .fold([0; SHA256_BYTES], |digest, (object_index, row)| {
                    meta::advance_manifest_rolling_digest(
                        digest,
                        &meta::ManifestRowInput {
                            object_index: object_index as u64,
                            row: row.clone(),
                        },
                    )
                    .unwrap()
                });
        let base_manifest_digest =
            base_rows
                .iter()
                .enumerate()
                .fold([0; SHA256_BYTES], |digest, (object_index, row)| {
                    meta::advance_manifest_rolling_digest(
                        digest,
                        &meta::ManifestRowInput {
                            object_index: object_index as u64,
                            row: row.clone(),
                        },
                    )
                    .unwrap()
                });
        let revision = meta::ArtifactRevisionRecord {
            logical_size: 32,
            body_digest_uri: sha256_uri([0x41; SHA256_BYTES]),
            manifest_digest_uri: sha256_uri(manifest_digest),
            block_count: 4,
            dependency_count: 1,
            dependency_depth: 1,
            dependency_digest: meta::dependency_owner_digest(&[dependency_owner]).unwrap(),
            content_type: "application/octet-stream".to_owned(),
            state: RevisionState::Available,
            reference_epoch: epoch,
            strong_reference_count: 0,
            last_zero_ref_version: Some(CommitVersion::new(last_zero).unwrap()),
        };
        let dependency_revision = meta::ArtifactRevisionRecord {
            logical_size: 16,
            body_digest_uri: sha256_uri([0x42; SHA256_BYTES]),
            manifest_digest_uri: sha256_uri(base_manifest_digest),
            block_count: 2,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: meta::dependency_owner_digest(&[]).unwrap(),
            content_type: "application/octet-stream".to_owned(),
            state: RevisionState::Available,
            reference_epoch: ReferenceEpoch::new(1),
            strong_reference_count: 1,
            last_zero_ref_version: None,
        };
        let candidate = meta::GcCandidateRecord {
            last_zero_ref_version: CommitVersion::new(last_zero).unwrap(),
            claim_state: GcClaimState::Candidate,
            retry_count: 0,
            quarantine_evidence: None,
        };
        let mut mutations = vec![
            meta::CommandMutation::Put {
                family: meta::MetadataFamily::ArtifactRevision,
                key: meta::artifact_revision_key(root(), target),
                value: revision.encode().unwrap(),
            },
            meta::CommandMutation::Put {
                family: meta::MetadataFamily::ArtifactRevision,
                key: meta::artifact_revision_key(root(), dependency_owner),
                value: dependency_revision.encode().unwrap(),
            },
            meta::CommandMutation::Put {
                family: meta::MetadataFamily::RevisionRef,
                key: meta::revision_dependency_ref_key(root(), target, dependency_owner),
                value: meta::RevisionRefRecord {
                    reference_epoch_at_add: ReferenceEpoch::new(1),
                }
                .encode()
                .unwrap(),
            },
            meta::CommandMutation::Put {
                family: meta::MetadataFamily::GcCandidate,
                key: meta::gc_candidate_key(root(), target, epoch),
                value: candidate.encode().unwrap(),
            },
        ];
        for (object_index, row) in child_rows.iter().enumerate() {
            mutations.push(meta::CommandMutation::Put {
                family: meta::MetadataFamily::ArtifactManifest,
                key: meta::artifact_manifest_key(root(), target, object_index as u64),
                value: row.encode().unwrap(),
            });
        }
        for (object_index, row) in base_rows.iter().enumerate() {
            mutations.push(meta::CommandMutation::Put {
                family: meta::MetadataFamily::ArtifactManifest,
                key: meta::artifact_manifest_key(root(), dependency_owner, object_index as u64),
                value: row.encode().unwrap(),
            });
        }
        store
            .execute(&command(
                &store,
                3,
                meta::RootFenceAction::RequireActive,
                mutations,
            ))
            .unwrap();

        let registry = Arc::new(RootOwnerRegistry::new());
        registry.install(route(), Arc::new(UnusedExecutor)).unwrap();
        Fixture {
            store,
            registry,
            route: route(),
            target,
            epoch,
            owned_object_keys: delta_rows
                .iter()
                .map(|row| row.object_key.clone())
                .collect(),
            borrowed_object_keys: base_rows.iter().map(|row| row.object_key.clone()).collect(),
        }
    }

    fn candidate(fixture: &Fixture) -> meta::GcCandidateRecord {
        let context =
            meta::RootReadContext::current(&fixture.store, root(), placement(), owner()).unwrap();
        let payload = fixture
            .store
            .read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                meta::MetadataFamily::GcCandidate,
                &meta::gc_candidate_key(root(), fixture.target, fixture.epoch),
                context.read_version,
            )
            .unwrap()
            .unwrap();
        meta::GcCandidateRecord::decode(&payload).unwrap()
    }

    fn operation(fixture: &Fixture) -> meta::GcOperationRecord {
        let operation_id = meta::gc_operation_id(root(), fixture.target, fixture.epoch);
        let context =
            meta::RootReadContext::current(&fixture.store, root(), placement(), owner()).unwrap();
        let payload = fixture
            .store
            .read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                meta::MetadataFamily::Operation,
                &meta::operation_key(root(), OperationKind::Gc, operation_id),
                context.read_version,
            )
            .unwrap()
            .unwrap();
        meta::GcOperationRecord::decode(&payload).unwrap()
    }

    #[test]
    fn absence_proof_constructor_is_deterministic_and_domain_separated() {
        let request = LifecycleDeleteRequest {
            purpose: LifecycleDeletePurpose::AbortedPublication,
            object_key: "nokv/artifacts/01010101010101010101010101010101/02020202020202020202020202020202/03030303030303030303030303030303/blocks/0000000000000007".to_owned(),
            multipart_upload_id: Some(b"multipart-7".to_vec()),
        };
        let proof = LifecycleAbsenceProof::from_delete_request(
            &request,
            LifecycleDeleteDisposition::Deleted,
        );
        assert_eq!(
            proof.digest,
            [
                0xc0, 0x04, 0xeb, 0x4f, 0x8c, 0x10, 0x64, 0x40, 0x85, 0xfd, 0xce, 0x1a, 0xc8, 0x95,
                0xd4, 0x94, 0xa2, 0xd0, 0x19, 0x4a, 0xd0, 0xa1, 0xcb, 0x64, 0xba, 0x03, 0x86, 0xa2,
                0x21, 0x88, 0xb9, 0x85,
            ]
        );
        assert_eq!(
            proof,
            LifecycleAbsenceProof::from_delete_request(
                &request,
                LifecycleDeleteDisposition::Deleted,
            )
        );
        let unbound_key_digest: [u8; SHA256_BYTES] =
            Sha256::digest(request.object_key.as_bytes()).into();
        assert_ne!(proof.digest, unbound_key_digest);

        let mut changed = request.clone();
        changed.purpose = LifecycleDeletePurpose::RevisionGarbageCollection;
        assert_ne!(
            proof.digest,
            LifecycleAbsenceProof::from_delete_request(
                &changed,
                LifecycleDeleteDisposition::Deleted,
            )
            .digest
        );
        changed = request.clone();
        changed.multipart_upload_id = None;
        assert_ne!(
            proof.digest,
            LifecycleAbsenceProof::from_delete_request(
                &changed,
                LifecycleDeleteDisposition::Deleted,
            )
            .digest
        );
        assert_ne!(
            proof.digest,
            LifecycleAbsenceProof::from_delete_request(
                &request,
                LifecycleDeleteDisposition::AlreadyAbsent,
            )
            .digest
        );
    }

    #[test]
    fn lifecycle_finishes_capacity_aborted_commit_and_releases_hold() {
        let store = crate::test_support::meta_shard(shard());
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&command(
                &store,
                1,
                meta::RootFenceAction::Install,
                Vec::new(),
            ))
            .unwrap();
        store
            .execute(&command(
                &store,
                2,
                meta::RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
                Vec::new(),
            ))
            .unwrap();

        let operation_id = OperationId::from_bytes([0x81; FIXED_ID_BYTES]);
        let commit_id = nokv_types::CommitId::from_bytes([0x82; SHA256_BYTES]);
        let tree_revision = ArtifactRevisionId::from_bytes([0x83; FIXED_ID_BYTES]);
        let source_read_version = store.current_read_version().unwrap();
        let mut operation = meta::BuildCommitOperationRecord {
            operation_id,
            identity_digest: [0; SHA256_BYTES],
            initialization_digest: [0; SHA256_BYTES],
            workbench_id: WorkbenchId::new("capacity-aborted-commit").unwrap(),
            source_workspace_incarnation_id: WorkspaceIncarnationId::from_bytes(
                [0x84; FIXED_ID_BYTES],
            ),
            source_read_version,
            commit_id,
            expected_head: None,
            content_digest_uri: sha256_uri([0x85; SHA256_BYTES]),
            manifest_digest_uri: sha256_uri([0x86; SHA256_BYTES]),
            projection_input_digest: [0x87; SHA256_BYTES],
            tree_manifest_revision_id: tree_revision,
            replace: false,
            run_manifest_condition: meta::CommitManifestCondition::CreateOnly,
            committed_at_unix_seconds: 1,
            commit_staged_run_manifest: None,
            producer: None,
            lineage_projection: Vec::new(),
            parent_commits: Vec::new(),
            phase: BuildCommitPhase::Aborting,
            member_cursor: None,
            member_count: 0,
            member_digest: [0; SHA256_BYTES],
            members_complete: false,
            revision_ref_count: 0,
            revision_cursor: None,
            revision_seal_count: 0,
            revision_digest: [0; SHA256_BYTES],
            revisions_complete: false,
            parent_cursor: 0,
            parent_digest: [0; SHA256_BYTES],
            parents_complete: false,
            cleanup_member_count: 0,
            cleanup_revision_count: 0,
            cleanup_parent_count: 0,
            history_hold_released: false,
            result: None,
            terminal_error: Some(meta::CommitOperationTerminalError {
                kind: meta::CommitOperationErrorKind::InvariantViolation,
                message: "serving transaction capacity cannot admit one commit member step"
                    .to_owned(),
            }),
        };
        operation.seal_digests();
        let operation_key = meta::operation_key(root(), OperationKind::BuildCommit, operation_id);
        let hold_key = meta::build_commit_history_hold_key(root(), operation_id);
        store
            .execute(&command(
                &store,
                3,
                meta::RootFenceAction::RequireActive,
                vec![
                    meta::CommandMutation::Put {
                        family: meta::MetadataFamily::Operation,
                        key: operation_key.clone(),
                        value: operation.encode().unwrap(),
                    },
                    meta::CommandMutation::Put {
                        family: meta::MetadataFamily::HistoryHold,
                        key: hold_key.clone(),
                        value: meta::HistoryHoldRecord {
                            read_version: source_read_version,
                            source_snapshot_id: None,
                            state: HistoryHoldState::Active,
                        }
                        .encode(),
                    },
                ],
            ))
            .unwrap();

        let registry = Arc::new(RootOwnerRegistry::new());
        registry.install(route(), Arc::new(UnusedExecutor)).unwrap();
        let runner = LifecycleRunner::new(
            Arc::clone(&store),
            registry,
            route(),
            OwnerLossSignal::default(),
            Arc::new(FakeDeleter {
                ambiguous: false,
                calls: AtomicUsize::new(0),
                object_keys: Mutex::new(Vec::new()),
            }),
            LifecycleRunnerOptions {
                scan_page_size: 8,
                mutation_batch_size: 8,
                ..LifecycleRunnerOptions::default()
            },
        )
        .unwrap();

        assert_eq!(runner.run_once(100).unwrap().metadata_transitions, 1);
        assert_eq!(runner.run_once(100).unwrap().metadata_transitions, 1);
        assert_eq!(runner.run_once(100).unwrap().metadata_transitions, 0);

        let read = meta::RootReadContext::current(&store, root(), placement(), owner()).unwrap();
        let payload = store
            .read_at(
                root(),
                placement(),
                owner(),
                meta::MetadataFamily::Operation,
                &operation_key,
                read.read_version,
            )
            .unwrap()
            .unwrap();
        let operation = meta::BuildCommitOperationRecord::decode(&payload).unwrap();
        assert_eq!(operation.phase, BuildCommitPhase::Cleaned);
        assert!(operation.history_hold_released);
        assert_eq!(
            store
                .read_at(
                    root(),
                    placement(),
                    owner(),
                    meta::MetadataFamily::HistoryHold,
                    &hold_key,
                    read.read_version,
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn gc_deletion_uses_owner_local_indexes_and_skips_borrowed_base_objects() {
        let fixture = fixture();
        let deleter = Arc::new(FakeDeleter {
            ambiguous: false,
            calls: AtomicUsize::new(0),
            object_keys: Mutex::new(Vec::new()),
        });
        let runner = LifecycleRunner::new(
            Arc::clone(&fixture.store),
            Arc::clone(&fixture.registry),
            fixture.route,
            OwnerLossSignal::default(),
            deleter.clone(),
            LifecycleRunnerOptions {
                scan_page_size: 8,
                mutation_batch_size: 8,
                ..LifecycleRunnerOptions::default()
            },
        )
        .unwrap();

        for _ in 0..4 {
            runner.run_once(100_000).unwrap();
        }
        assert_eq!(deleter.calls.load(Ordering::SeqCst), 2);
        let deleted_keys = deleter.object_keys.lock().unwrap().clone();
        assert_eq!(deleted_keys, fixture.owned_object_keys);
        assert!(fixture
            .borrowed_object_keys
            .iter()
            .all(|key| !deleted_keys.contains(key)));
        assert_eq!(candidate(&fixture).claim_state, GcClaimState::Complete);
        assert_eq!(operation(&fixture).phase, GcPhase::Deleted);
    }

    #[test]
    fn quiescent_last_zero_candidate_advances_real_history_barrier() {
        let fixture = fixture_with_last_zero(4);
        let deleter = Arc::new(FakeDeleter {
            ambiguous: false,
            calls: AtomicUsize::new(0),
            object_keys: Mutex::new(Vec::new()),
        });
        let runner = LifecycleRunner::new(
            Arc::clone(&fixture.store),
            Arc::clone(&fixture.registry),
            fixture.route,
            OwnerLossSignal::default(),
            deleter.clone(),
            LifecycleRunnerOptions {
                scan_page_size: 8,
                mutation_batch_size: 8,
                ..LifecycleRunnerOptions::default()
            },
        )
        .unwrap();

        for _ in 0..5 {
            runner.run_once(100_000).unwrap();
        }
        assert_eq!(deleter.calls.load(Ordering::SeqCst), 2);
        assert_eq!(candidate(&fixture).claim_state, GcClaimState::Complete);
        assert_eq!(operation(&fixture).phase, GcPhase::Deleted);
    }

    #[test]
    fn owner_loss_blocks_delete_and_ambiguous_delete_quarantines() {
        let fixture = fixture();
        let deleter = Arc::new(FakeDeleter {
            ambiguous: true,
            calls: AtomicUsize::new(0),
            object_keys: Mutex::new(Vec::new()),
        });
        let runner = LifecycleRunner::new(
            Arc::clone(&fixture.store),
            Arc::clone(&fixture.registry),
            fixture.route,
            OwnerLossSignal::default(),
            deleter.clone(),
            LifecycleRunnerOptions {
                scan_page_size: 8,
                mutation_batch_size: 8,
                ..LifecycleRunnerOptions::default()
            },
        )
        .unwrap();

        runner.run_once(100_000).unwrap();
        runner.run_once(100_000).unwrap();
        fixture.registry.remove(fixture.route).unwrap();
        assert!(matches!(
            runner.run_once(100_000),
            Err(LifecycleError::OwnerLost(_))
        ));
        assert_eq!(deleter.calls.load(Ordering::SeqCst), 0);

        fixture
            .registry
            .install(fixture.route, Arc::new(UnusedExecutor))
            .unwrap();
        runner.run_once(100_000).unwrap();
        assert_eq!(deleter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(candidate(&fixture).claim_state, GcClaimState::Quarantined);
        assert_eq!(operation(&fixture).phase, GcPhase::Quarantined);
        assert!(operation(&fixture).quarantine_evidence.is_some());
    }

    #[test]
    fn long_poll_interval_is_interrupted_by_owner_loss() {
        let fixture = fixture();
        let owner_loss = OwnerLossSignal::default();
        let runner = LifecycleRunner::new(
            Arc::clone(&fixture.store),
            Arc::clone(&fixture.registry),
            fixture.route,
            owner_loss.clone(),
            Arc::new(FakeDeleter {
                ambiguous: false,
                calls: AtomicUsize::new(0),
                object_keys: Mutex::new(Vec::new()),
            }),
            LifecycleRunnerOptions {
                poll_interval: Duration::from_secs(60),
                scan_page_size: 8,
                mutation_batch_size: 8,
                ..LifecycleRunnerOptions::default()
            },
        )
        .unwrap();
        let worker = thread::spawn(move || runner.run_until_owner_loss());
        thread::sleep(Duration::from_millis(40));

        let interrupted_at = Instant::now();
        owner_loss.fail_closed();
        let result = worker.join().unwrap();
        assert!(matches!(result, Err(LifecycleError::OwnerLost(_))));
        assert!(interrupted_at.elapsed() < Duration::from_secs(1));
        assert_eq!(fixture.registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn publish_uses_durable_activity_lease_and_new_owner_takeover() {
        let store = crate::test_support::meta_shard(shard());
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&command(
                &store,
                1,
                meta::RootFenceAction::Install,
                Vec::new(),
            ))
            .unwrap();
        store
            .execute(&command(
                &store,
                2,
                meta::RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
                Vec::new(),
            ))
            .unwrap();
        let workbench = WorkbenchId::new("publish-owner-takeover").unwrap();
        let incarnation = WorkspaceIncarnationId::from_bytes([0x33; FIXED_ID_BYTES]);
        meta::create_visible_workspace(
            &store,
            meta::RootWriteContext::current(
                &store,
                root(),
                shard(),
                placement(),
                owner(),
                RequestId::from_bytes([3; FIXED_ID_BYTES]),
            )
            .unwrap(),
            &workbench,
            incarnation,
        )
        .unwrap();

        let operation_id = OperationId::from_bytes([0x44; FIXED_ID_BYTES]);
        let mut operation = meta::PublishOperationRecord {
            operation_id,
            identity_digest: [0; SHA256_BYTES],
            initialization_digest: [0; SHA256_BYTES],
            initiating_owner_epoch: owner(),
            activity_deadline_ms: 100_000,
            authority: meta::PublishAuthority::Visible,
            workbench_id: workbench,
            workspace_incarnation_id: incarnation,
            path: NormalizedRelativePath::new("outputs/orphan.bin").unwrap(),
            artifact_revision_id: ArtifactRevisionId::from_bytes([0x45; FIXED_ID_BYTES]),
            claim: meta::PublishClaim::CreateOnly,
            phase: PublishPhase::Uploading,
            staged_object_count: 0,
            staged_object_seal: meta::staged_object_ledger_digest(&[]).unwrap(),
            staged_object_cursor: 0,
            staged_object_rolling_digest: [0; SHA256_BYTES],
            uploaded_object_cursor: 0,
            uploaded_object_rolling_digest: [0; SHA256_BYTES],
            manifest_row_count: 0,
            manifest_seal: meta::manifest_rows_digest(&[]).unwrap(),
            manifest_cursor: 0,
            manifest_rolling_digest: [0; SHA256_BYTES],
            manifest_last_position: None,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: meta::dependency_owner_digest(&[]).unwrap(),
            cleanup_staged_object_cursor: 0,
            cleanup_manifest_cursor: 0,
            publication_absence_proof: None,
            result: None,
            terminal_error: None,
        };
        meta::seal_publish_operation(&mut operation);
        let expired_operation_id = OperationId::from_bytes([0x46; FIXED_ID_BYTES]);
        let mut expired_operation = operation.clone();
        expired_operation.operation_id = expired_operation_id;
        expired_operation.path = NormalizedRelativePath::new("outputs/expired.bin").unwrap();
        expired_operation.artifact_revision_id =
            ArtifactRevisionId::from_bytes([0x47; FIXED_ID_BYTES]);
        meta::seal_publish_operation(&mut expired_operation);
        meta::PublicationService::new(&store)
            .begin_publish(meta::BeginPublishRequest {
                context: meta::PublicationContext {
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: RequestId::from_bytes([4; FIXED_ID_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                },
                operation: operation.clone(),
            })
            .unwrap();
        meta::PublicationService::new(&store)
            .begin_publish(meta::BeginPublishRequest {
                context: meta::PublicationContext {
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: RequestId::from_bytes([5; FIXED_ID_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                },
                operation: expired_operation,
            })
            .unwrap();

        let registry = Arc::new(RootOwnerRegistry::new());
        registry.install(route(), Arc::new(UnusedExecutor)).unwrap();
        let deleter = Arc::new(FakeDeleter {
            ambiguous: false,
            calls: AtomicUsize::new(0),
            object_keys: Mutex::new(Vec::new()),
        });
        let current_runner = LifecycleRunner::new(
            Arc::clone(&store),
            Arc::clone(&registry),
            route(),
            OwnerLossSignal::default(),
            deleter.clone(),
            LifecycleRunnerOptions {
                scan_page_size: 8,
                mutation_batch_size: 8,
                ..LifecycleRunnerOptions::default()
            },
        )
        .unwrap();
        let report = current_runner.run_once(129_999).unwrap();
        assert_eq!(report.metadata_transitions, 0);
        meta::PublicationService::new(&store)
            .heartbeat_publish(meta::HeartbeatPublishRequest {
                context: meta::PublicationContext {
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: RequestId::from_bytes([6; FIXED_ID_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                },
                expected_operation: operation,
                activity_deadline_ms: 1_000_000,
            })
            .unwrap();
        let report = current_runner.run_once(130_000).unwrap();
        assert_eq!(report.metadata_transitions, 1);
        let current_read =
            meta::RootReadContext::current(&store, root(), placement(), owner()).unwrap();
        let expired_payload = store
            .read_at(
                root(),
                placement(),
                owner(),
                meta::MetadataFamily::Operation,
                &meta::operation_key(root(), OperationKind::Publish, expired_operation_id),
                current_read.read_version,
            )
            .unwrap()
            .unwrap();
        let expired_operation = meta::PublishOperationRecord::decode(&expired_payload).unwrap();
        assert_eq!(expired_operation.phase, PublishPhase::Aborting);
        assert_eq!(
            expired_operation.terminal_error.unwrap().kind,
            meta::PublishTerminalErrorKind::ActivityLeaseExpired
        );

        store
            .advance_owner_epoch(Some(owner()), successor_owner())
            .unwrap();
        registry.remove(route()).unwrap();
        let successor_route = RootRoute {
            owner_epoch: successor_owner().get(),
            ..route()
        };
        registry
            .install(successor_route, Arc::new(UnusedExecutor))
            .unwrap();
        let successor_runner = LifecycleRunner::new(
            Arc::clone(&store),
            Arc::clone(&registry),
            successor_route,
            OwnerLossSignal::default(),
            deleter.clone(),
            LifecycleRunnerOptions {
                scan_page_size: 8,
                mutation_batch_size: 8,
                ..LifecycleRunnerOptions::default()
            },
        )
        .unwrap();
        let report = successor_runner.run_once(u64::MAX).unwrap();
        assert_eq!(report.metadata_transitions, 2);
        assert_eq!(deleter.calls.load(Ordering::SeqCst), 0);

        let read =
            meta::RootReadContext::current(&store, root(), placement(), successor_owner()).unwrap();
        let payload = store
            .read_at(
                root(),
                placement(),
                successor_owner(),
                meta::MetadataFamily::Operation,
                &meta::operation_key(root(), OperationKind::Publish, operation_id),
                read.read_version,
            )
            .unwrap()
            .unwrap();
        let operation = meta::PublishOperationRecord::decode(&payload).unwrap();
        assert_eq!(operation.phase, PublishPhase::Aborting);
        assert_eq!(operation.initiating_owner_epoch, owner());
        assert_eq!(
            operation.terminal_error.unwrap().kind,
            meta::PublishTerminalErrorKind::OwnerEpochSuperseded
        );
    }

    #[test]
    fn aborted_publish_cleanup_must_not_delete_winner_published_objects() {
        // TDD oracle for external risk report P1-A: operation A and operation
        // B (different operation ids) both begin publishing the SAME
        // artifact revision. Permanent object keys derive only from
        // (logical_shard, root, revision, object_index), so both operations'
        // staged rows name identical provider keys. A publishes successfully;
        // B aborts; the lifecycle cleanup cycle for B then runs. The oracle is
        // that A's already-published block object must still exist and be
        // readable afterwards.
        use nokv_object::MemoryArtifactStore;

        fn publication_context(
            store: &meta::MetaShard,
            counter: &mut u8,
        ) -> meta::PublicationContext {
            let request = *counter;
            *counter += 1;
            meta::PublicationContext {
                root_id: root(),
                logical_shard_id: shard(),
                placement_generation: placement(),
                owner_epoch: owner(),
                request_id: RequestId::from_bytes([request; FIXED_ID_BYTES]),
                read_version: store.current_read_version().unwrap(),
            }
        }

        let store = crate::test_support::meta_shard(shard());
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&command(
                &store,
                1,
                meta::RootFenceAction::Install,
                Vec::new(),
            ))
            .unwrap();
        store
            .execute(&command(
                &store,
                2,
                meta::RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
                Vec::new(),
            ))
            .unwrap();
        let workbench = WorkbenchId::new("cow-publish-oracle").unwrap();
        let incarnation = WorkspaceIncarnationId::from_bytes([0x51; FIXED_ID_BYTES]);
        meta::create_visible_workspace(
            &store,
            meta::RootWriteContext::current(
                &store,
                root(),
                shard(),
                placement(),
                owner(),
                RequestId::from_bytes([3; FIXED_ID_BYTES]),
            )
            .unwrap(),
            &workbench,
            incarnation,
        )
        .unwrap();

        let shared_revision = ArtifactRevisionId::from_bytes([0x77; FIXED_ID_BYTES]);
        let shared_object_key = meta::object_block_key(shard(), root(), shared_revision, 0);
        let block_bytes = b"winner!!".to_vec();
        let staged_planned = vec![meta::StagedObjectRecord {
            artifact_revision_id: shared_revision,
            object_sequence: 0,
            object_key: shared_object_key.clone(),
            multipart_upload_id: None,
            expected_length: block_bytes.len() as u64,
            expected_digest_uri: sha256_uri([0x61; SHA256_BYTES]),
            provider_state: StagedProviderState::Planned,
            cleanup_state: StagedCleanupState::Owned,
        }];
        let manifest = vec![meta::ManifestRowInput {
            object_index: 0,
            row: meta::ArtifactManifestRow {
                physical_owner_revision_id: shared_revision,
                physical_object_index: 0,
                object_key: shared_object_key.clone(),
                logical_offset: 0,
                offset: 0,
                length: block_bytes.len() as u64,
                digest_uri: sha256_uri([0x61; SHA256_BYTES]),
                append_segment: None,
            },
        }];
        let template = |operation_id_byte: u8| {
            let mut operation = meta::PublishOperationRecord {
                operation_id: OperationId::from_bytes([operation_id_byte; FIXED_ID_BYTES]),
                identity_digest: [0; SHA256_BYTES],
                initialization_digest: [0; SHA256_BYTES],
                initiating_owner_epoch: owner(),
                activity_deadline_ms: 1_000_000,
                authority: meta::PublishAuthority::Visible,
                workbench_id: workbench.clone(),
                workspace_incarnation_id: incarnation,
                path: NormalizedRelativePath::new("outputs/shared-revision.bin").unwrap(),
                artifact_revision_id: shared_revision,
                claim: meta::PublishClaim::CreateOnly,
                phase: PublishPhase::Uploading,
                staged_object_count: u32::try_from(staged_planned.len()).unwrap(),
                staged_object_seal: meta::staged_object_ledger_digest(&staged_planned).unwrap(),
                staged_object_cursor: 0,
                staged_object_rolling_digest: [0; SHA256_BYTES],
                uploaded_object_cursor: 0,
                uploaded_object_rolling_digest: [0; SHA256_BYTES],
                manifest_row_count: u32::try_from(manifest.len()).unwrap(),
                manifest_seal: meta::manifest_rows_digest(&manifest).unwrap(),
                manifest_cursor: 0,
                manifest_rolling_digest: [0; SHA256_BYTES],
                manifest_last_position: None,
                dependency_count: 0,
                dependency_depth: 0,
                dependency_digest: meta::dependency_owner_digest(&[]).unwrap(),
                cleanup_staged_object_cursor: 0,
                cleanup_manifest_cursor: 0,
                publication_absence_proof: None,
                result: None,
                terminal_error: None,
            };
            meta::seal_publish_operation(&mut operation);
            operation
        };

        let service = meta::PublicationService::new(&store);
        let mut request_counter = 10_u8;

        // Operation A begins publishing revision R.
        let operation_a = service
            .begin_publish(meta::BeginPublishRequest {
                context: publication_context(&store, &mut request_counter),
                operation: template(0xA1),
            })
            .expect("operation A begins for revision R")
            .operation;

        // Operation B targets the SAME revision with a different operation
        // id. The begin-time revision claim must reject it: both operations'
        // staged rows would otherwise derive identical permanent object keys,
        // and B's abort cleanup would delete A's published objects.
        assert_eq!(
            service.begin_publish(meta::BeginPublishRequest {
                context: publication_context(&store, &mut request_counter),
                operation: template(0xB1),
            }),
            Err(meta::PublicationError::RevisionClaimHeld {
                revision: shared_revision,
                operation_id: operation_a.operation_id,
            })
        );

        // A stages the same rows, uploads the real block bytes, confirms the
        // upload, seals the manifest, and publishes.
        let operation_a = service
            .stage_objects_batch(meta::StageObjectsBatchRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                staged_objects: staged_planned.clone(),
            })
            .expect("operation A stages its rows")
            .operation;
        let objects = Arc::new(MemoryArtifactStore::new());
        let created = objects
            .create_immutable(
                &ObjectKey::new(shared_object_key.clone()).unwrap(),
                &block_bytes,
            )
            .unwrap();
        assert_eq!(created, ImmutableCreateOutcome::Created);
        let uploaded_updates = staged_planned
            .iter()
            .cloned()
            .map(|expected| {
                let mut next = expected.clone();
                next.provider_state = StagedProviderState::Uploaded;
                meta::StagedObjectUpdate { expected, next }
            })
            .collect();
        let operation_a = service
            .mark_objects_uploaded_batch(meta::MarkObjectsUploadedBatchRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                staged_object_updates: uploaded_updates,
            })
            .expect("operation A confirms uploads")
            .operation;
        let operation_a = service
            .stage_manifest_batch(meta::StageManifestBatchRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                manifest_rows: manifest.clone(),
                dependency_owner_revision_ids: Vec::new(),
            })
            .expect("operation A stages its manifest")
            .operation;
        let operation_a = service
            .transition_publish(meta::TransitionPublishRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                transition: meta::PublishTransition::BeginFinalization,
            })
            .expect("operation A begins finalization")
            .operation;
        let manifest_digest_uri = sha256_uri(operation_a.manifest_seal);
        let finalized = service
            .finalize_publish(meta::FinalizePublishRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                artifact: meta::PublishedArtifact {
                    logical_size: block_bytes.len() as u64,
                    body_digest_uri: sha256_uri([0x62; SHA256_BYTES]),
                    manifest_digest_uri,
                    content_type: "application/octet-stream".to_owned(),
                    producer: Some("cow-oracle-test".to_owned()),
                    manifest_id: Some("cow-oracle-manifest".to_owned()),
                    typed_index_projection: meta::TypedProjection::empty().encode().unwrap(),
                },
                dependency_owner_revision_ids: Vec::new(),
            })
            .expect("operation A publishes revision R");
        assert_eq!(finalized.operation.phase, PublishPhase::Published);
        let read = meta::RootReadContext::current(&store, root(), placement(), owner()).unwrap();
        let revision_payload = store
            .read_at(
                root(),
                placement(),
                owner(),
                meta::MetadataFamily::ArtifactRevision,
                &meta::artifact_revision_key(root(), shared_revision),
                read.read_version,
            )
            .unwrap()
            .expect("published artifact revision record exists");
        let revision_record = meta::ArtifactRevisionRecord::decode(&revision_payload).unwrap();
        assert_eq!(revision_record.state, RevisionState::Available);
        assert_eq!(
            objects
                .read(&ObjectKey::new(shared_object_key.clone()).unwrap(), None)
                .expect("winner block is readable after publish"),
            block_bytes
        );

        // Publication released the claim, so only the exact revision row
        // remains under the revision identity.
        assert_eq!(
            store
                .read_at(
                    root(),
                    placement(),
                    owner(),
                    meta::MetadataFamily::ArtifactRevision,
                    &meta::artifact_revision_claim_key(root(), shared_revision),
                    read.read_version,
                )
                .unwrap(),
            None
        );

        // Lifecycle cycles find nothing to clean for the published revision.
        let registry = Arc::new(RootOwnerRegistry::new());
        registry.install(route(), Arc::new(UnusedExecutor)).unwrap();
        let runner = LifecycleRunner::new(
            Arc::clone(&store),
            registry,
            route(),
            OwnerLossSignal::default(),
            Arc::new(ArtifactLifecycleDeleter::new(Arc::clone(&objects))),
            LifecycleRunnerOptions {
                scan_page_size: 8,
                mutation_batch_size: 8,
                ..LifecycleRunnerOptions::default()
            },
        )
        .unwrap();
        for _ in 0..5 {
            runner.run_once(100).unwrap();
        }

        // ORACLE: operation A's published block object must still exist and be
        // readable from the object store.
        let survivor = objects.read(&ObjectKey::new(shared_object_key.clone()).unwrap(), None);
        assert!(
            survivor.is_ok(),
            "P1-A regressed: winner operation A's published object \
             {shared_object_key} was deleted: {survivor:?}"
        );
        assert_eq!(survivor.unwrap(), block_bytes);
    }

    #[test]
    fn legacy_claimless_abort_cleanup_quarantines_when_revision_published() {
        // Depth guard for operations begun before revision claims existed:
        // a cleaning operation whose revision is meanwhile Available must be
        // quarantined loudly instead of deleting the publisher's objects.
        use nokv_object::MemoryArtifactStore;

        fn publication_context(
            store: &meta::MetaShard,
            counter: &mut u8,
        ) -> meta::PublicationContext {
            let request = *counter;
            *counter += 1;
            meta::PublicationContext {
                root_id: root(),
                logical_shard_id: shard(),
                placement_generation: placement(),
                owner_epoch: owner(),
                request_id: RequestId::from_bytes([request; FIXED_ID_BYTES]),
                read_version: store.current_read_version().unwrap(),
            }
        }

        let store = crate::test_support::meta_shard(shard());
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&command(
                &store,
                1,
                meta::RootFenceAction::Install,
                Vec::new(),
            ))
            .unwrap();
        store
            .execute(&command(
                &store,
                2,
                meta::RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
                Vec::new(),
            ))
            .unwrap();
        let workbench = WorkbenchId::new("legacy-claimless-loser").unwrap();
        let incarnation = WorkspaceIncarnationId::from_bytes([0x52; FIXED_ID_BYTES]);
        meta::create_visible_workspace(
            &store,
            meta::RootWriteContext::current(
                &store,
                root(),
                shard(),
                placement(),
                owner(),
                RequestId::from_bytes([3; FIXED_ID_BYTES]),
            )
            .unwrap(),
            &workbench,
            incarnation,
        )
        .unwrap();

        let shared_revision = ArtifactRevisionId::from_bytes([0x78; FIXED_ID_BYTES]);
        let shared_object_key = meta::object_block_key(shard(), root(), shared_revision, 0);
        let block_bytes = b"winner!!".to_vec();
        let staged_planned = vec![meta::StagedObjectRecord {
            artifact_revision_id: shared_revision,
            object_sequence: 0,
            object_key: shared_object_key.clone(),
            multipart_upload_id: None,
            expected_length: block_bytes.len() as u64,
            expected_digest_uri: sha256_uri([0x61; SHA256_BYTES]),
            provider_state: StagedProviderState::Planned,
            cleanup_state: StagedCleanupState::Owned,
        }];
        let manifest = vec![meta::ManifestRowInput {
            object_index: 0,
            row: meta::ArtifactManifestRow {
                physical_owner_revision_id: shared_revision,
                physical_object_index: 0,
                object_key: shared_object_key.clone(),
                logical_offset: 0,
                offset: 0,
                length: block_bytes.len() as u64,
                digest_uri: sha256_uri([0x61; SHA256_BYTES]),
                append_segment: None,
            },
        }];
        let template = |operation_id_byte: u8| {
            let mut operation = meta::PublishOperationRecord {
                operation_id: OperationId::from_bytes([operation_id_byte; FIXED_ID_BYTES]),
                identity_digest: [0; SHA256_BYTES],
                initialization_digest: [0; SHA256_BYTES],
                initiating_owner_epoch: owner(),
                activity_deadline_ms: 1_000_000,
                authority: meta::PublishAuthority::Visible,
                workbench_id: workbench.clone(),
                workspace_incarnation_id: incarnation,
                path: NormalizedRelativePath::new("outputs/legacy-claimless.bin").unwrap(),
                artifact_revision_id: shared_revision,
                claim: meta::PublishClaim::CreateOnly,
                phase: PublishPhase::Uploading,
                staged_object_count: u32::try_from(staged_planned.len()).unwrap(),
                staged_object_seal: meta::staged_object_ledger_digest(&staged_planned).unwrap(),
                staged_object_cursor: 0,
                staged_object_rolling_digest: [0; SHA256_BYTES],
                uploaded_object_cursor: 0,
                uploaded_object_rolling_digest: [0; SHA256_BYTES],
                manifest_row_count: u32::try_from(manifest.len()).unwrap(),
                manifest_seal: meta::manifest_rows_digest(&manifest).unwrap(),
                manifest_cursor: 0,
                manifest_rolling_digest: [0; SHA256_BYTES],
                manifest_last_position: None,
                dependency_count: 0,
                dependency_depth: 0,
                dependency_digest: meta::dependency_owner_digest(&[]).unwrap(),
                cleanup_staged_object_cursor: 0,
                cleanup_manifest_cursor: 0,
                publication_absence_proof: None,
                result: None,
                terminal_error: None,
            };
            meta::seal_publish_operation(&mut operation);
            operation
        };

        let service = meta::PublicationService::new(&store);
        let mut request_counter = 10_u8;

        // Loser B begins (taking a claim), durably stages Planned/Owned rows
        // naming the shared permanent key, and aborts.
        let operation_b = service
            .begin_publish(meta::BeginPublishRequest {
                context: publication_context(&store, &mut request_counter),
                operation: template(0xB2),
            })
            .expect("operation B begins")
            .operation;
        let operation_b = service
            .stage_objects_batch(meta::StageObjectsBatchRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_b,
                staged_objects: staged_planned.clone(),
            })
            .expect("operation B stages its rows")
            .operation;
        let operation_b = service
            .transition_publish(meta::TransitionPublishRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_b,
                transition: meta::PublishTransition::BeginAbort {
                    terminal_error: meta::PublishTerminalError {
                        kind: meta::PublishTerminalErrorKind::AbortedByCaller,
                        message: "legacy duplicate publish cancelled".to_owned(),
                        evidence_digest: None,
                    },
                },
            })
            .expect("operation B aborts")
            .operation;
        assert_eq!(operation_b.phase, PublishPhase::Aborting);

        // Simulate a pre-claim legacy operation by deleting B's claim row the
        // way no current command can.
        let claim_key = meta::artifact_revision_claim_key(root(), shared_revision);
        let read = meta::RootReadContext::current(&store, root(), placement(), owner()).unwrap();
        let claim_payload = store
            .read_at(
                root(),
                placement(),
                owner(),
                meta::MetadataFamily::ArtifactRevision,
                &claim_key,
                read.read_version,
            )
            .unwrap()
            .expect("operation B's claim exists");
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: RequestId::from_bytes([0xEE; FIXED_ID_BYTES]),
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: meta::RootFenceAction::RequireActive,
                    predicates: vec![meta::CommandPredicate::Value {
                        family: meta::MetadataFamily::ArtifactRevision,
                        key: claim_key.clone(),
                        expected: Some(claim_payload),
                    }],
                    mutations: vec![meta::CommandMutation::Delete {
                        family: meta::MetadataFamily::ArtifactRevision,
                        key: claim_key.clone(),
                    }],
                    history_projection: vec![meta::HistoryProjection {
                        family: meta::MetadataFamily::ArtifactRevision,
                        key: claim_key,
                    }],
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .expect("legacy simulation removes operation B's claim");

        // Winner A begins the now-unclaimed revision, uploads the real bytes,
        // and publishes.
        let operation_a = service
            .begin_publish(meta::BeginPublishRequest {
                context: publication_context(&store, &mut request_counter),
                operation: template(0xA2),
            })
            .expect("operation A begins after the legacy claim removal")
            .operation;
        let operation_a = service
            .stage_objects_batch(meta::StageObjectsBatchRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                staged_objects: staged_planned.clone(),
            })
            .expect("operation A stages its rows")
            .operation;
        let objects = Arc::new(MemoryArtifactStore::new());
        objects
            .create_immutable(
                &ObjectKey::new(shared_object_key.clone()).unwrap(),
                &block_bytes,
            )
            .unwrap();
        let uploaded_updates = staged_planned
            .iter()
            .cloned()
            .map(|expected| {
                let mut next = expected.clone();
                next.provider_state = StagedProviderState::Uploaded;
                meta::StagedObjectUpdate { expected, next }
            })
            .collect();
        let operation_a = service
            .mark_objects_uploaded_batch(meta::MarkObjectsUploadedBatchRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                staged_object_updates: uploaded_updates,
            })
            .expect("operation A confirms uploads")
            .operation;
        let operation_a = service
            .stage_manifest_batch(meta::StageManifestBatchRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                manifest_rows: manifest.clone(),
                dependency_owner_revision_ids: Vec::new(),
            })
            .expect("operation A stages its manifest")
            .operation;
        let operation_a = service
            .transition_publish(meta::TransitionPublishRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                transition: meta::PublishTransition::BeginFinalization,
            })
            .expect("operation A begins finalization")
            .operation;

        let read_operation_b = |store: &Arc<meta::MetaShard>| {
            let read = meta::RootReadContext::current(store, root(), placement(), owner()).unwrap();
            let payload = store
                .read_at(
                    root(),
                    placement(),
                    owner(),
                    meta::MetadataFamily::Operation,
                    &meta::operation_key(root(), OperationKind::Publish, operation_b.operation_id),
                    read.read_version,
                )
                .unwrap()
                .expect("operation B record exists");
            meta::PublishOperationRecord::decode(&payload).unwrap()
        };

        let registry = Arc::new(RootOwnerRegistry::new());
        registry.install(route(), Arc::new(UnusedExecutor)).unwrap();
        let runner = LifecycleRunner::new(
            Arc::clone(&store),
            registry,
            route(),
            OwnerLossSignal::default(),
            Arc::new(ArtifactLifecycleDeleter::new(Arc::clone(&objects))),
            LifecycleRunnerOptions {
                scan_page_size: 8,
                mutation_batch_size: 8,
                ..LifecycleRunnerOptions::default()
            },
        )
        .unwrap();

        // While winner A is mid-flight (claim held, revision not yet
        // published), claimless B's destructive cleanup must defer: A's
        // uploaded object shares B's staged key.
        for _ in 0..2 {
            runner.run_once(100).unwrap();
        }
        let deferred = read_operation_b(&store);
        assert_eq!(
            deferred.phase,
            PublishPhase::Cleaning,
            "claimless cleanup must defer while another operation claims the revision"
        );
        assert_eq!(deferred.cleanup_staged_object_cursor, 0);
        assert_eq!(objects.stats().unwrap().deletes, 0);

        let manifest_digest_uri = sha256_uri(operation_a.manifest_seal);
        let finalized = service
            .finalize_publish(meta::FinalizePublishRequest {
                context: publication_context(&store, &mut request_counter),
                expected_operation: operation_a,
                artifact: meta::PublishedArtifact {
                    logical_size: block_bytes.len() as u64,
                    body_digest_uri: sha256_uri([0x62; SHA256_BYTES]),
                    manifest_digest_uri,
                    content_type: "application/octet-stream".to_owned(),
                    producer: None,
                    manifest_id: None,
                    typed_index_projection: meta::TypedProjection::empty().encode().unwrap(),
                },
                dependency_owner_revision_ids: Vec::new(),
            })
            .expect("operation A publishes the revision");
        assert_eq!(finalized.operation.phase, PublishPhase::Published);

        // With the revision now published, the depth guard must quarantine
        // claimless B before the first provider delete.
        for _ in 0..5 {
            runner.run_once(100).unwrap();
        }
        assert_eq!(
            read_operation_b(&store).phase,
            PublishPhase::Quarantined,
            "claimless cleanup over a published revision must quarantine"
        );
        assert_eq!(objects.stats().unwrap().deletes, 0);
        assert_eq!(
            objects
                .read(&ObjectKey::new(shared_object_key.clone()).unwrap(), None)
                .expect("winner object survives the quarantined cleanup"),
            block_bytes
        );
    }
}
