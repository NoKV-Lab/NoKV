/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Crash-safe prepared-create and exact metadata-commit receipt journal for one
//! Holt owner session. The journal is external to the Holt directory and is an
//! unpublished session-lifetime bridge, never control-plane or store-lifetime
//! authority.

use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nokv_control::{
    LogicalShardLease, LogicalShardRecord, LogicalShardState, MetadataAuthorityFence,
    MetadataAuthorityRecord, NodeId, OwnerEpoch, OwnerIncarnationId, RootId, RootPlacement,
};
use nokv_server::{
    AcknowledgedMetadataFrontier, HoltRuntimeGuard, HoltRuntimeGuardError, HoltStoreObjectIdentity,
    MetadataAuthorityCommitActionV1, MetadataCommandCommitClassV1, MetadataCommitPurposeV1,
    MetadataCommitReceiptDirtySourceV1, MetadataCommitReceiptErrorV1,
    MetadataCommitReceiptMutationBackendResultV1, MetadataCommitReceiptMutationNotDispatchedV1,
    MetadataCommitReceiptPersistBackendResultV1, MetadataCommitReceiptPersistCommandV1,
    MetadataCommitReceiptPersistNotDispatchedV1, MetadataCommitReceiptPersistOutcomeV1,
    MetadataCommitReceiptPoisonCommandV1, MetadataCommitReceiptPoisonOutcomeV1,
    MetadataCommitReceiptPoisonReasonV1, MetadataCommitReceiptQualificationV1,
    MetadataCommitReceiptResolveCommandV1, MetadataCommitReceiptResolveOutcomeV1,
    MetadataCommitReceiptStateV1, MetadataCommitReceiptStoreV1, MetadataCommitResolutionBasisV1,
    MetadataFrontierPointV1, MetadataStoreIdentity, PlannedMetadataCommitV1,
    RuntimeLifecycleValidationError, RuntimeLifecycleValidator,
};
use nokv_types::{
    CommandDigest, CommitVersion, ConsistencyDomainId, MetadataAuthorityGeneration,
    MetadataAuthorityId, MetadataContractDigest, OperationId, PlacementGeneration, RequestId,
    SHA256_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::owner_session::OwnerSessionToken;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::{ffi::CString, ffi::OsString};

const JOURNAL_VERSION: u8 = 4;
const MAX_JOURNAL_BYTES: u64 = 32 * 1024;
const PREPARATION_DOMAIN: &[u8] = b"nokv.holt-owner.prepared-create.v1\0";
const RUNTIME_BUNDLE_DOMAIN: &[u8] = b"nokv.holt-owner.runtime-bundle.v1\0";
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[cfg(all(unix, test))]
std::thread_local! {
    static STABLE_READ_TEST_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static REPLACE_AFTER_RENAME_TEST_FAILURE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedCreateDisposition {
    Created,
    Replayed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreparedControlOwner {
    First,
    ResumeOrSuccessor(LogicalShardLease),
    Successor(OwnerEpoch),
}

#[derive(Clone, PartialEq, Eq)]
pub struct OwnerSessionPreparation {
    digest: [u8; SHA256_BYTES],
    metadata_store_identity: MetadataStoreIdentity,
    frozen_runtime_bundle_digest: [u8; SHA256_BYTES],
    root_id: RootId,
    owner: NodeId,
    endpoint: String,
    logical_shard_id: nokv_types::LogicalShardId,
    authority: MetadataAuthorityFence,
    canonical_holt_locator: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct OwnerReleasePreparation {
    root_id: RootId,
    owner: NodeId,
    endpoint: String,
    canonical_holt_locator: Vec<u8>,
}

impl fmt::Debug for OwnerSessionPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionPreparation")
            .field("binding", &"<redacted>")
            .finish()
    }
}

impl fmt::Debug for OwnerReleasePreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerReleasePreparation")
            .field("binding", &"<redacted>")
            .finish()
    }
}

impl OwnerReleasePreparation {
    pub fn new(
        root_id: RootId,
        owner: NodeId,
        endpoint: String,
        metadata_path: &Path,
        journal_path: &Path,
    ) -> Result<Self, OwnerSessionJournalError> {
        #[cfg(not(unix))]
        {
            let _ = (root_id, owner, endpoint, metadata_path, journal_path);
            return Err(OwnerSessionJournalError::UnsupportedPlatform);
        }

        #[cfg(unix)]
        {
            OwnerSessionToken::validate_process_binding(&owner, &endpoint)
                .map_err(|_| OwnerSessionJournalError::InvalidJournal("owner or endpoint"))?;
            let canonical_holt_locator = canonical_locator(metadata_path)?;
            let (_, canonical_journal, _, _) = canonical_file_path(journal_path)?;
            if canonical_journal.starts_with(path_from_bytes(&canonical_holt_locator)) {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "owner-session journal must be outside the Holt metadata directory",
                ));
            }
            Ok(Self {
                root_id,
                owner,
                endpoint,
                canonical_holt_locator,
            })
        }
    }
}

impl OwnerSessionPreparation {
    pub fn new(
        placement: &RootPlacement,
        authority: &MetadataAuthorityRecord,
        owner: NodeId,
        endpoint: String,
        metadata_path: &Path,
        journal_path: &Path,
    ) -> Result<Self, OwnerSessionJournalError> {
        #[cfg(not(unix))]
        {
            let _ = (
                placement,
                authority,
                owner,
                endpoint,
                metadata_path,
                journal_path,
            );
            return Err(OwnerSessionJournalError::UnsupportedPlatform);
        }

        #[cfg(unix)]
        {
            if authority.logical_shard_id != placement.logical_shard_id {
                return Err(OwnerSessionJournalError::BindingMismatch(
                    "placement and metadata authority",
                ));
            }
            OwnerSessionToken::validate_process_binding(&owner, &endpoint)
                .map_err(|_| OwnerSessionJournalError::InvalidJournal("owner or endpoint"))?;
            let canonical_holt_locator = canonical_locator(metadata_path)?;
            let (_, canonical_journal, _, _) = canonical_file_path(journal_path)?;
            if canonical_journal.starts_with(path_from_bytes(&canonical_holt_locator)) {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "owner-session journal must be outside the Holt metadata directory",
                ));
            }
            let digest = preparation_digest(
                placement,
                authority,
                &owner,
                &endpoint,
                &canonical_holt_locator,
            );
            let metadata_store_identity = MetadataStoreIdentity {
                logical_shard_id: placement.logical_shard_id,
                authority_id: authority.active.authority_id,
                authority_generation: authority.authority_generation,
                consistency_domain_id: authority.active.consistency_domain_id,
                profile_fingerprint: authority.active.profile_fingerprint,
                contract_digest: authority.active.contract_digest,
            };
            validate_store_identity_binding(metadata_store_identity)?;
            let frozen_runtime_bundle_digest =
                runtime_bundle_digest(digest, metadata_store_identity, &canonical_holt_locator);
            if frozen_runtime_bundle_digest.iter().all(|byte| *byte == 0) {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "derived runtime bundle digest is zero",
                ));
            }
            Ok(Self {
                digest,
                metadata_store_identity,
                frozen_runtime_bundle_digest,
                root_id: placement.root_id,
                owner,
                endpoint,
                logical_shard_id: placement.logical_shard_id,
                authority: authority.fence(),
                canonical_holt_locator,
            })
        }
    }

    pub fn reconcile_control_owner(
        &self,
        shard: &LogicalShardRecord,
        authority: &MetadataAuthorityRecord,
    ) -> Result<PreparedControlOwner, OwnerSessionJournalError> {
        if shard.logical_shard_id != self.logical_shard_id
            || authority.logical_shard_id != self.logical_shard_id
            || authority.fence() != self.authority
        {
            return Err(OwnerSessionJournalError::BindingMismatch(
                "prepared create control identity",
            ));
        }
        if shard.owner.is_none()
            && shard.owner_epoch.is_none()
            && shard.owner_incarnation_id.is_none()
            && shard.lease_id == 0
            && shard.state == LogicalShardState::Unassigned
        {
            return Ok(PreparedControlOwner::First);
        }
        let owner_epoch = shard
            .owner_epoch
            .ok_or(OwnerSessionJournalError::BindingMismatch(
                "prepared create owner epoch",
            ))?;
        let owner_incarnation_id =
            shard
                .owner_incarnation_id
                .ok_or(OwnerSessionJournalError::BindingMismatch(
                    "prepared create owner incarnation",
                ))?;
        if shard.owner.is_none()
            && shard.endpoint.is_none()
            && shard.lease_id == 0
            && shard.state == LogicalShardState::Unassigned
        {
            return Ok(PreparedControlOwner::Successor(owner_epoch));
        }
        if shard.owner.as_ref() != Some(&self.owner)
            || shard.endpoint.as_deref() != Some(self.endpoint.as_str())
            || shard.lease_id == 0
            || !matches!(
                shard.state,
                LogicalShardState::Recovering | LogicalShardState::Serving
            )
        {
            return Err(OwnerSessionJournalError::BindingMismatch(
                "prepared create live owner session",
            ));
        }
        Ok(PreparedControlOwner::ResumeOrSuccessor(LogicalShardLease {
            logical_shard_id: self.logical_shard_id,
            owner: self.owner.clone(),
            owner_epoch,
            owner_incarnation_id,
            lease_id: shard.lease_id,
            authority: self.authority,
        }))
    }

    #[cfg(test)]
    fn release_preparation(&self) -> OwnerReleasePreparation {
        OwnerReleasePreparation {
            root_id: self.root_id,
            owner: self.owner.clone(),
            endpoint: self.endpoint.clone(),
            canonical_holt_locator: self.canonical_holt_locator.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum OwnerSessionJournalError {
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    UnsupportedPlatform,
    Io {
        operation: &'static str,
        kind: io::ErrorKind,
    },
    InvalidJournal(&'static str),
    BindingMismatch(&'static str),
    Changed,
    Poisoned,
}

impl fmt::Display for OwnerSessionJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            Self::UnsupportedPlatform => formatter.write_str(
                "Holt owner-session journals require Unix filesystem identity semantics",
            ),
            Self::Io { operation, kind } => {
                write!(
                    formatter,
                    "owner-session journal {operation} failed ({kind:?})"
                )
            }
            Self::InvalidJournal(reason) => {
                write!(formatter, "invalid owner-session journal: {reason}")
            }
            Self::BindingMismatch(field) => {
                write!(formatter, "owner-session journal binding mismatch: {field}")
            }
            Self::Changed => formatter.write_str("owner-session journal changed concurrently"),
            Self::Poisoned => formatter.write_str("owner-session journal is poisoned"),
        }
    }
}

impl std::error::Error for OwnerSessionJournalError {}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HoltObjectWire {
    directory_device: u64,
    directory_inode: u64,
    lock_device: u64,
    lock_inode: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FrontierWire {
    write_sequence: u64,
    commit_version: u64,
    recovery_lsn: u64,
    chain_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MetadataStoreIdentityWire {
    logical_shard_id: String,
    authority_id: String,
    authority_generation: u64,
    consistency_domain_id: String,
    profile_fingerprint: String,
    contract_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum FrontierPointWire {
    Absent,
    Exact { frontier: FrontierWire },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum MetadataCommandCommitClassWire {
    Domain,
    RootFence,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum MetadataAuthorityCommitActionWire {
    Quiesce {
        migration_id: String,
        owner_epoch: u64,
    },
    FenceQuiescedSource {
        migration_id: String,
        source_receipt_digest: String,
    },
    ActivateTarget {
        migration_id: String,
        activation_token_digest: String,
    },
    FenceTarget {
        migration_id: String,
        target_binding_digest: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum MetadataCommitPurposeWire {
    Genesis {
        authority_marker_digest: String,
    },
    AdvanceOwnerEpoch {
        expected: Option<u64>,
        next: u64,
    },
    ObserveLeaseClock {
        root_id: String,
        placement_generation: u64,
        owner_epoch: u64,
        observed_ms: u64,
    },
    MetadataCommand {
        class: MetadataCommandCommitClassWire,
        root_id: String,
        request_id: String,
        command_digest: String,
        lease_deadline_ms: Option<u64>,
    },
    Authority {
        action: MetadataAuthorityCommitActionWire,
        prior_marker_digest: String,
        next_marker_digest: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PlannedMetadataCommitWire {
    store_identity: MetadataStoreIdentityWire,
    frozen_bundle_digest: String,
    purpose: MetadataCommitPurposeWire,
    prior: FrontierPointWire,
    exact_next: FrontierWire,
    canonical_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
enum CommitReceiptWire {
    Clean { frontier: FrontierPointWire },
    Pending { planned: PlannedMetadataCommitWire },
    PoisonedSettled { planned: PlannedMetadataCommitWire },
    PoisonedUnsettled { planned: PlannedMetadataCommitWire },
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum JournalPhase {
    Prepared,
    Serving,
    Releasing,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct JournalWire {
    version: u8,
    generation: u64,
    phase: JournalPhase,
    preparation_digest: String,
    root_id: String,
    owner: String,
    endpoint: String,
    logical_shard_id: String,
    authority_id: String,
    authority_generation: u64,
    metadata_store_identity: MetadataStoreIdentityWire,
    frozen_runtime_bundle_digest: String,
    holt_locator: String,
    holt_object: Option<HoltObjectWire>,
    commit_receipt: CommitReceiptWire,
    owner_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    release_owner_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    release_owner_incarnation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    release_lease_id: Option<u64>,
}

struct JournalFileState {
    parent_directory: File,
    file_name: OsString,
    encoded: Vec<u8>,
    identity: FileIdentity,
    wire: JournalWire,
}

#[derive(Clone, PartialEq, Eq)]
pub struct OwnerReleaseReceiptBinding {
    preparation_digest: String,
    root_id: String,
    owner: String,
    endpoint: String,
    logical_shard_id: String,
    authority_id: String,
    authority_generation: u64,
    holt_locator: String,
}

pub struct OwnerSessionJournal {
    state: Mutex<JournalFileState>,
    frozen_runtime_bundle_digest: [u8; SHA256_BYTES],
    poisoned: AtomicBool,
}

impl fmt::Debug for OwnerSessionJournal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionJournal")
            .field("path", &"<redacted>")
            .field("contents", &"<redacted>")
            .finish()
    }
}

impl OwnerSessionJournal {
    pub fn prepare_create(
        path: &Path,
        preparation: &OwnerSessionPreparation,
    ) -> Result<(Arc<Self>, PreparedCreateDisposition), OwnerSessionJournalError> {
        #[cfg(not(unix))]
        {
            let _ = (path, preparation);
            return Err(OwnerSessionJournalError::UnsupportedPlatform);
        }

        #[cfg(unix)]
        {
            let (_, _, parent_directory, file_name) = canonical_file_path(path)?;
            match read_durable_stable_file_at(&parent_directory, &file_name) {
                Ok((encoded, identity)) => {
                    let journal = Self::from_prepared_file_state(
                        parent_directory,
                        file_name,
                        encoded,
                        identity,
                        preparation,
                    )?;
                    Ok((journal, PreparedCreateDisposition::Replayed))
                }
                Err(OwnerSessionJournalError::Io {
                    kind: io::ErrorKind::NotFound,
                    ..
                }) => {
                    let wire = prepared_wire(preparation);
                    let encoded = encode_wire(&wire)?;
                    match create_initial_file(&parent_directory, &file_name, &encoded)? {
                        Some(identity) => {
                            let journal = Arc::new(Self {
                                state: Mutex::new(JournalFileState {
                                    parent_directory,
                                    file_name,
                                    encoded,
                                    identity,
                                    wire,
                                }),
                                frozen_runtime_bundle_digest: preparation
                                    .frozen_runtime_bundle_digest,
                                poisoned: AtomicBool::new(false),
                            });
                            journal.ensure_prepared_store_object()?;
                            Ok((journal, PreparedCreateDisposition::Created))
                        }
                        None => {
                            let (encoded, identity) =
                                read_durable_stable_file_at(&parent_directory, &file_name)?;
                            let journal = Self::from_prepared_file_state(
                                parent_directory,
                                file_name,
                                encoded,
                                identity,
                                preparation,
                            )?;
                            Ok((journal, PreparedCreateDisposition::Replayed))
                        }
                    }
                }
                Err(error) => Err(error),
            }
        }
    }

    pub fn load_resume(
        path: &Path,
        metadata_path: &Path,
    ) -> Result<(OwnerSessionToken, Arc<Self>), OwnerSessionJournalError> {
        #[cfg(not(unix))]
        {
            let _ = (path, metadata_path);
            return Err(OwnerSessionJournalError::UnsupportedPlatform);
        }

        #[cfg(unix)]
        {
            let (_, _, parent_directory, file_name) = canonical_file_path(path)?;
            let (encoded, identity) = read_durable_stable_file_at(&parent_directory, &file_name)?;
            let wire = decode_wire(&encoded)?;
            if wire.phase != JournalPhase::Serving {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "resume requires a serving journal",
                ));
            }
            if wire.holt_locator != encode_hex(&canonical_locator(metadata_path)?) {
                return Err(OwnerSessionJournalError::BindingMismatch(
                    "canonical Holt locator",
                ));
            }
            let token_encoded = decode_hex_vec(wire.owner_token.as_deref().ok_or(
                OwnerSessionJournalError::InvalidJournal("missing owner token"),
            )?)?;
            let token = OwnerSessionToken::decode(&token_encoded)
                .map_err(|_| OwnerSessionJournalError::InvalidJournal("owner token"))?;
            validate_token_wire(&wire, &token)?;
            let frozen_runtime_bundle_digest = frozen_bundle_digest_from_wire(&wire)?;
            let journal = Arc::new(Self {
                state: Mutex::new(JournalFileState {
                    parent_directory,
                    file_name,
                    encoded,
                    identity,
                    wire,
                }),
                frozen_runtime_bundle_digest,
                poisoned: AtomicBool::new(false),
            });
            journal.validate_runtime_inner()?;
            Ok((token, journal))
        }
    }

    pub fn load_releasing(
        path: &Path,
        preparation: &OwnerReleasePreparation,
    ) -> Result<Option<(LogicalShardLease, Arc<Self>)>, OwnerSessionJournalError> {
        #[cfg(not(unix))]
        {
            let _ = (path, preparation);
            return Err(OwnerSessionJournalError::UnsupportedPlatform);
        }

        #[cfg(unix)]
        {
            let (_, _, parent_directory, file_name) = canonical_file_path(path)?;
            let (encoded, identity) =
                match read_durable_stable_file_at(&parent_directory, &file_name) {
                    Ok(file) => file,
                    Err(OwnerSessionJournalError::Io {
                        kind: io::ErrorKind::NotFound,
                        ..
                    }) => return Ok(None),
                    Err(error) => return Err(error),
                };
            let wire = decode_wire(&encoded)?;
            if wire.phase != JournalPhase::Releasing {
                return Ok(None);
            }
            validate_release_preparation_wire(&wire, preparation)?;
            let lease = release_lease_from_wire(&wire)?;
            let frozen_runtime_bundle_digest = frozen_bundle_digest_from_wire(&wire)?;
            let journal = Arc::new(Self {
                state: Mutex::new(JournalFileState {
                    parent_directory,
                    file_name,
                    encoded,
                    identity,
                    wire,
                }),
                frozen_runtime_bundle_digest,
                poisoned: AtomicBool::new(true),
            });
            Ok(Some((lease, journal)))
        }
    }

    #[cfg(unix)]
    fn from_prepared_file_state(
        parent_directory: File,
        file_name: OsString,
        encoded: Vec<u8>,
        identity: FileIdentity,
        preparation: &OwnerSessionPreparation,
    ) -> Result<Arc<Self>, OwnerSessionJournalError> {
        let wire = decode_wire(&encoded)?;
        validate_preparation_wire(&wire, preparation)?;
        if wire.phase != JournalPhase::Prepared || wire.owner_token.is_some() {
            return Err(OwnerSessionJournalError::InvalidJournal(
                "create replay requires a prepared journal",
            ));
        }
        let journal = Arc::new(Self {
            state: Mutex::new(JournalFileState {
                parent_directory,
                file_name,
                encoded,
                identity,
                wire,
            }),
            frozen_runtime_bundle_digest: preparation.frozen_runtime_bundle_digest,
            poisoned: AtomicBool::new(false),
        });
        journal.ensure_prepared_store_object()?;
        Ok(journal)
    }

    #[cfg(test)]
    fn preparation(&self) -> Result<OwnerSessionPreparation, OwnerSessionJournalError> {
        let state = self.lock_state()?;
        preparation_from_wire(&state.wire)
    }

    #[cfg(test)]
    fn is_store_prepared(&self) -> Result<bool, OwnerSessionJournalError> {
        Ok(self.lock_state()?.wire.holt_object.is_some())
    }

    #[cfg(test)]
    fn seed_clean_exact_fixture(
        &self,
        frontier: AcknowledgedMetadataFrontier,
    ) -> Result<(), OwnerSessionJournalError> {
        self.update(|wire| {
            if !matches!(&wire.commit_receipt, CommitReceiptWire::Clean { .. }) {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "test receipt seed requires a clean journal",
                ));
            }
            wire.commit_receipt = CommitReceiptWire::Clean {
                frontier: FrontierPointWire::Exact {
                    frontier: encode_frontier(frontier),
                },
            };
            Ok(())
        })
    }

    pub fn fail_closed(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    pub fn complete_serving(
        &self,
        token: &OwnerSessionToken,
    ) -> Result<(), OwnerSessionJournalError> {
        self.update(|wire| {
            if wire.holt_object.is_none()
                || !matches!(
                    &wire.commit_receipt,
                    CommitReceiptWire::Clean {
                        frontier: FrontierPointWire::Exact { .. }
                    }
                )
            {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "serving completion requires store identity and one exact clean frontier",
                ));
            }
            validate_token_wire(wire, token)?;
            let encoded_token =
                encode_hex(&token.encode().map_err(|_| {
                    OwnerSessionJournalError::InvalidJournal("owner token encoding")
                })?);
            match (&wire.phase, &wire.owner_token) {
                (JournalPhase::Prepared, None) => {
                    wire.phase = JournalPhase::Serving;
                    wire.owner_token = Some(encoded_token);
                    Ok(())
                }
                (JournalPhase::Serving, Some(existing)) if existing == &encoded_token => Ok(()),
                (JournalPhase::Serving, Some(_)) => Err(OwnerSessionJournalError::BindingMismatch(
                    "serving owner token",
                )),
                (JournalPhase::Prepared, Some(_)) => Err(OwnerSessionJournalError::InvalidJournal(
                    "prepared journal cannot contain an owner token",
                )),
                (JournalPhase::Serving, None) => Err(OwnerSessionJournalError::InvalidJournal(
                    "serving journal must contain an owner token",
                )),
                (JournalPhase::Releasing, _) => Err(OwnerSessionJournalError::InvalidJournal(
                    "releasing journal cannot return to serving",
                )),
            }
        })
    }

    #[cfg(test)]
    fn begin_releasing(&self, lease: &LogicalShardLease) -> Result<(), OwnerSessionJournalError> {
        let expected = {
            let state = self
                .state
                .lock()
                .map_err(|_| OwnerSessionJournalError::Poisoned)?;
            owner_release_receipt_binding(&state.wire)
        };
        self.begin_releasing_at_binding(&expected, lease)
    }

    fn begin_releasing_at_binding(
        &self,
        expected: &OwnerReleaseReceiptBinding,
        lease: &LogicalShardLease,
    ) -> Result<(), OwnerSessionJournalError> {
        self.poisoned.store(true, Ordering::Release);
        let mut state = self
            .state
            .lock()
            .map_err(|_| OwnerSessionJournalError::Poisoned)?;
        if owner_release_receipt_binding(&state.wire) != *expected {
            return Err(OwnerSessionJournalError::Changed);
        }
        let (current_encoded, current_identity) =
            read_durable_stable_file_at(&state.parent_directory, &state.file_name)?;
        let mut current = decode_wire(&current_encoded)?;
        if owner_release_receipt_binding(&current) != *expected {
            return Err(OwnerSessionJournalError::Changed);
        }
        validate_release_lease_wire(&current, lease)?;
        if current.phase == JournalPhase::Releasing {
            state.encoded = current_encoded;
            state.identity = current_identity;
            state.wire = current;
            return Ok(());
        }
        if !matches!(
            current.phase,
            JournalPhase::Prepared | JournalPhase::Serving
        ) {
            return Err(OwnerSessionJournalError::InvalidJournal(
                "release requires a prepared, serving, or releasing journal",
            ));
        }
        current.phase = JournalPhase::Releasing;
        current.release_owner_epoch = Some(lease.owner_epoch.get());
        current.release_owner_incarnation_id =
            Some(encode_hex(lease.owner_incarnation_id.as_bytes()));
        current.release_lease_id = Some(lease.lease_id);
        current.generation =
            current
                .generation
                .checked_add(1)
                .ok_or(OwnerSessionJournalError::InvalidJournal(
                    "journal generation overflow",
                ))?;
        let next_encoded = encode_wire(&current)?;
        state.encoded = current_encoded;
        state.identity = current_identity;
        state.wire = current.clone();
        let next_identity = replace_file(&state, &next_encoded)?;
        state.encoded = next_encoded;
        state.identity = next_identity;
        state.wire = current;
        Ok(())
    }

    fn ensure_prepared_store_object(&self) -> Result<(), OwnerSessionJournalError> {
        {
            let state = self.lock_state()?;
            if state.wire.holt_object.is_some() {
                drop(state);
                return self.validate_runtime_inner();
            }
        }
        let observed = {
            let state = self.lock_state()?;
            prepare_holt_object(&state.wire)?
        };
        self.update(|wire| {
            if wire.phase != JournalPhase::Prepared || wire.owner_token.is_some() {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "store preparation requires a prepared journal",
                ));
            }
            match &wire.holt_object {
                Some(expected) if expected != &observed => Err(
                    OwnerSessionJournalError::BindingMismatch("Holt filesystem object"),
                ),
                Some(_) => Ok(()),
                None => {
                    wire.holt_object = Some(observed);
                    Ok(())
                }
            }
        })?;
        self.validate_runtime_inner()
    }

    pub fn remove_if_exact(&self) -> Result<(), OwnerSessionJournalError> {
        let state = self
            .state
            .lock()
            .map_err(|_| OwnerSessionJournalError::Poisoned)?;
        if state.wire.phase != JournalPhase::Releasing {
            return Err(OwnerSessionJournalError::InvalidJournal(
                "journal removal requires a releasing receipt",
            ));
        }
        let _ = release_lease_from_wire(&state.wire)?;
        let (current, identity) =
            read_durable_stable_file_at(&state.parent_directory, &state.file_name)?;
        if current != state.encoded || identity != state.identity {
            return Err(OwnerSessionJournalError::Changed);
        }
        let current_wire = decode_wire(&current)?;
        if current_wire.phase != JournalPhase::Releasing {
            return Err(OwnerSessionJournalError::InvalidJournal(
                "journal removal requires a releasing receipt",
            ));
        }
        let _ = release_lease_from_wire(&current_wire)?;
        unlinkat_file(&state.parent_directory, &state.file_name)?;
        state
            .parent_directory
            .sync_all()
            .map_err(|error| io_error("parent directory sync", error))
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, JournalFileState>, OwnerSessionJournalError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(OwnerSessionJournalError::Poisoned);
        }
        self.state
            .lock()
            .map_err(|_| OwnerSessionJournalError::Poisoned)
    }

    fn update(
        &self,
        mutate: impl FnOnce(&mut JournalWire) -> Result<(), OwnerSessionJournalError>,
    ) -> Result<(), OwnerSessionJournalError> {
        let mut state = self.lock_state()?;
        let mut next = state.wire.clone();
        mutate(&mut next)?;
        if next == state.wire {
            return match validate_exact_file_state(&state) {
                Ok(()) => Ok(()),
                Err(error) => {
                    self.poisoned.store(true, Ordering::Release);
                    Err(error)
                }
            };
        }
        next.generation =
            next.generation
                .checked_add(1)
                .ok_or(OwnerSessionJournalError::InvalidJournal(
                    "journal generation overflow",
                ))?;
        let encoded = encode_wire(&next)?;
        match replace_file(&state, &encoded) {
            Ok(identity) => {
                state.identity = identity;
                state.encoded = encoded;
                state.wire = next;
                Ok(())
            }
            Err(error) => {
                self.poisoned.store(true, Ordering::Release);
                Err(error)
            }
        }
    }

    fn validate_runtime_inner(&self) -> Result<(), OwnerSessionJournalError> {
        let state = self.lock_state()?;
        if state.wire.phase == JournalPhase::Releasing {
            return Err(OwnerSessionJournalError::InvalidJournal(
                "releasing journal cannot validate a provider runtime",
            ));
        }
        let (current, identity) = read_stable_file_at(&state.parent_directory, &state.file_name)?;
        if current != state.encoded || identity != state.identity {
            return Err(OwnerSessionJournalError::Changed);
        }
        let expected =
            state
                .wire
                .holt_object
                .as_ref()
                .ok_or(OwnerSessionJournalError::InvalidJournal(
                    "missing Holt object identity",
                ))?;
        let locator = decode_path(&state.wire.holt_locator)?;
        let actual = capture_holt_object(&locator)?;
        if &actual != expected {
            return Err(OwnerSessionJournalError::BindingMismatch(
                "Holt directory or lock-file object",
            ));
        }
        Ok(())
    }
}

impl HoltRuntimeGuard for OwnerSessionJournal {
    fn bind_store(&self, identity: &HoltStoreObjectIdentity) -> Result<(), HoltRuntimeGuardError> {
        let result = self.update(|wire| {
            if wire.phase == JournalPhase::Releasing {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "releasing journal cannot bind a provider store",
                ));
            }
            if wire.holt_locator != encode_path(identity.canonical_locator())? {
                return Err(OwnerSessionJournalError::BindingMismatch(
                    "canonical Holt locator",
                ));
            }
            let observed = HoltObjectWire {
                directory_device: identity.directory_device(),
                directory_inode: identity.directory_inode(),
                lock_device: identity.lock_device(),
                lock_inode: identity.lock_inode(),
            };
            match &wire.holt_object {
                Some(expected) if expected != &observed => Err(
                    OwnerSessionJournalError::BindingMismatch("Holt filesystem object"),
                ),
                Some(_) => Ok(()),
                None => {
                    wire.holt_object = Some(observed);
                    Ok(())
                }
            }
        });
        result.map_err(|_| {
            self.poisoned.store(true, Ordering::Release);
            HoltRuntimeGuardError::Rejected
        })
    }

    fn validate_runtime(&self) -> Result<(), HoltRuntimeGuardError> {
        self.validate_runtime_inner().map_err(|_| {
            self.poisoned.store(true, Ordering::Release);
            HoltRuntimeGuardError::Rejected
        })
    }

    fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }
}

impl RuntimeLifecycleValidator for OwnerSessionJournal {
    fn validate(&self) -> Result<(), RuntimeLifecycleValidationError> {
        self.validate_runtime_inner().map_err(|_| {
            self.fail_closed();
            RuntimeLifecycleValidationError::Rejected
        })
    }

    fn poison(&self) {
        self.fail_closed();
    }
}

impl nokv_server::OwnerReleaseReceipt for OwnerSessionJournal {
    type Binding = OwnerReleaseReceiptBinding;

    fn owner_release_binding(
        &self,
    ) -> Result<Self::Binding, nokv_server::OwnerReleaseReceiptError> {
        self.state
            .lock()
            .map(|state| owner_release_receipt_binding(&state.wire))
            .map_err(|_| nokv_server::OwnerReleaseReceiptError::PersistenceRejectedV1)
    }

    fn preflight_owner_release_at_binding(
        &self,
        expected: &Self::Binding,
    ) -> Result<(), nokv_server::OwnerReleaseReceiptError> {
        self.validate_runtime_inner()
            .map_err(|_| nokv_server::OwnerReleaseReceiptError::PersistenceRejectedV1)?;
        let current = self.owner_release_binding()?;
        if &current != expected {
            return Err(nokv_server::OwnerReleaseReceiptError::BindingDriftV1);
        }
        Ok(())
    }

    fn persist_owner_releasing_at_binding(
        &self,
        expected: &Self::Binding,
        lease: &LogicalShardLease,
    ) -> Result<(), nokv_server::OwnerReleaseReceiptError> {
        self.begin_releasing_at_binding(expected, lease)
            .map_err(|error| {
                if error == OwnerSessionJournalError::Changed {
                    nokv_server::OwnerReleaseReceiptError::BindingDriftV1
                } else {
                    nokv_server::OwnerReleaseReceiptError::PersistenceRejectedV1
                }
            })
    }
}

fn resolve_frontier_transition(
    durable: &MetadataCommitReceiptStateV1,
    planned: &PlannedMetadataCommitV1,
    source: MetadataCommitReceiptDirtySourceV1,
    basis: MetadataCommitResolutionBasisV1,
    applied_exact_next: Option<AcknowledgedMetadataFrontier>,
    not_applied_exact_prior: Option<MetadataFrontierPointV1>,
    purpose_evidence_digest: [u8; SHA256_BYTES],
) -> Option<MetadataFrontierPointV1> {
    if !source.matches_state(durable, planned)
        || purpose_evidence_digest.iter().all(|byte| *byte == 0)
    {
        return None;
    }
    match basis {
        MetadataCommitResolutionBasisV1::ExactNextApplied
            if applied_exact_next == Some(planned.exact_next())
                && not_applied_exact_prior.is_none() =>
        {
            Some(MetadataFrontierPointV1::Exact(planned.exact_next()))
        }
        MetadataCommitResolutionBasisV1::ExactPriorNotAppliedSettled
            if source == MetadataCommitReceiptDirtySourceV1::PoisonedSettled
                && applied_exact_next.is_none()
                && not_applied_exact_prior == Some(planned.prior()) =>
        {
            Some(planned.prior())
        }
        _ => None,
    }
}

enum PoisonReceiptTransition {
    ReplaceSettled,
    ReplaceUnsettled,
    ExactNoChange,
}

fn poison_receipt_transition(
    durable: &MetadataCommitReceiptStateV1,
    planned: &PlannedMetadataCommitV1,
    reason: MetadataCommitReceiptPoisonReasonV1,
) -> Option<PoisonReceiptTransition> {
    match (durable, reason) {
        (
            MetadataCommitReceiptStateV1::Pending(durable),
            MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome,
        ) if durable == planned => Some(PoisonReceiptTransition::ReplaceSettled),
        (
            MetadataCommitReceiptStateV1::Pending(durable),
            MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome,
        ) if durable == planned => Some(PoisonReceiptTransition::ReplaceUnsettled),
        (
            MetadataCommitReceiptStateV1::PoisonedSettled(durable),
            MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome,
        ) if durable == planned => Some(PoisonReceiptTransition::ExactNoChange),
        (
            MetadataCommitReceiptStateV1::PoisonedUnsettled(durable),
            MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome,
        ) if durable == planned => Some(PoisonReceiptTransition::ExactNoChange),
        _ => None,
    }
}

const fn persist_outcome_after_local_uncertainty() -> MetadataCommitReceiptPersistBackendResultV1 {
    MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired
}

const fn mutation_outcome_after_local_uncertainty() -> MetadataCommitReceiptMutationBackendResultV1
{
    MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown
}

impl MetadataCommitReceiptStoreV1 for OwnerSessionJournal {
    fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
        MetadataCommitReceiptQualificationV1::Durable
    }

    fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
        self.frozen_runtime_bundle_digest
    }

    fn load_commit_receipt_v1(
        &self,
        store_identity: MetadataStoreIdentity,
    ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
        let state = self
            .state
            .lock()
            .map_err(|_| MetadataCommitReceiptErrorV1::Poisoned)?;
        if state.wire.phase == JournalPhase::Releasing {
            return Err(MetadataCommitReceiptErrorV1::Poisoned);
        }
        validate_exact_file_state(&state).map_err(receipt_load_error)?;
        let durable_identity = decode_store_identity(&state.wire.metadata_store_identity)
            .map_err(receipt_load_error)?;
        let durable_digest =
            frozen_bundle_digest_from_wire(&state.wire).map_err(receipt_load_error)?;
        if durable_identity != store_identity || durable_digest != self.frozen_runtime_bundle_digest
        {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        decode_commit_receipt(&state.wire).map_err(receipt_load_error)
    }

    fn persist_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptPersistCommandV1,
    ) -> MetadataCommitReceiptPersistOutcomeV1 {
        let command = command.claim_execution();
        let result = (|| {
            if self.poisoned.load(Ordering::Acquire) {
                // This bit can be set after an earlier atomic replacement had
                // an unknown outcome. Never let a caught unwind or a reused
                // allocation reinterpret that durable state as Clean.
                return persist_outcome_after_local_uncertainty();
            }
            let planned = command.planned();
            if planned.frozen_bundle_digest() != self.frozen_runtime_bundle_digest {
                return MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                    MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
                );
            }
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => {
                    // A poisoned mutex may have unwound after rename and
                    // before the cached state was advanced.
                    return persist_outcome_after_local_uncertainty();
                }
            };
            if state.wire.phase == JournalPhase::Releasing {
                return MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                    MetadataCommitReceiptPersistNotDispatchedV1::Unavailable,
                );
            }
            if let Err(error) = validate_exact_file_state(&state) {
                self.poisoned.store(true, Ordering::Release);
                return MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                    persist_not_dispatched(error),
                );
            }
            let current = match decode_commit_receipt(&state.wire) {
                Ok(current) => current,
                Err(error) => {
                    self.poisoned.store(true, Ordering::Release);
                    return MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                        persist_not_dispatched(error),
                    );
                }
            };
            match current {
                MetadataCommitReceiptStateV1::Clean {
                    store_identity,
                    frozen_bundle_digest,
                    frontier,
                } if store_identity == planned.store_identity()
                    && frozen_bundle_digest == self.frozen_runtime_bundle_digest
                    && frontier == planned.prior() => {}
                _ => {
                    return MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                        MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
                    )
                }
            }
            let mut next = state.wire.clone();
            next.commit_receipt = CommitReceiptWire::Pending {
                planned: encode_planned_commit(planned),
            };
            if install_next_wire(&mut state, next).is_err() {
                self.poisoned.store(true, Ordering::Release);
                MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired
            } else {
                MetadataCommitReceiptPersistBackendResultV1::Persisted
            }
        })();
        command.complete(result)
    }

    fn resolve_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptResolveCommandV1,
    ) -> MetadataCommitReceiptResolveOutcomeV1 {
        let command = command.claim_execution();
        let result = (|| {
            let planned = command.planned();
            let resolution = command.resolution();
            if planned.frozen_bundle_digest() != self.frozen_runtime_bundle_digest {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
                );
            }
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return mutation_outcome_after_local_uncertainty(),
            };
            if state.wire.phase == JournalPhase::Releasing {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::Poisoned,
                );
            }
            if let Err(error) = validate_exact_file_state(&state) {
                self.poisoned.store(true, Ordering::Release);
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    mutation_not_dispatched(error),
                );
            }
            let durable = match decode_commit_receipt(&state.wire) {
                Ok(durable) => durable,
                Err(error) => {
                    self.poisoned.store(true, Ordering::Release);
                    return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                        mutation_not_dispatched(error),
                    );
                }
            };
            let Some(frontier) = resolve_frontier_transition(
                &durable,
                planned,
                resolution.source(),
                resolution.basis(),
                resolution.applied_exact_next(),
                resolution.not_applied_exact_prior(),
                resolution.purpose_evidence_digest(),
            ) else {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
                );
            };
            let mut next = state.wire.clone();
            next.commit_receipt = CommitReceiptWire::Clean {
                frontier: encode_frontier_point(frontier),
            };
            if install_next_wire(&mut state, next).is_err() {
                self.poisoned.store(true, Ordering::Release);
                MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown
            } else {
                MetadataCommitReceiptMutationBackendResultV1::Completed
            }
        })();
        command.complete(result)
    }

    fn poison_commit_receipt_v1(
        &self,
        command: MetadataCommitReceiptPoisonCommandV1,
    ) -> MetadataCommitReceiptPoisonOutcomeV1 {
        let command = command.claim_execution();
        let result = (|| {
            let planned = command.planned();
            if planned.frozen_bundle_digest() != self.frozen_runtime_bundle_digest {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
                );
            }
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return mutation_outcome_after_local_uncertainty(),
            };
            if state.wire.phase == JournalPhase::Releasing {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::Poisoned,
                );
            }
            if let Err(error) = validate_exact_file_state(&state) {
                self.poisoned.store(true, Ordering::Release);
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    mutation_not_dispatched(error),
                );
            }
            let durable = match decode_commit_receipt(&state.wire) {
                Ok(durable) => durable,
                Err(error) => {
                    self.poisoned.store(true, Ordering::Release);
                    return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                        mutation_not_dispatched(error),
                    );
                }
            };
            let Some(transition) = poison_receipt_transition(&durable, planned, command.reason())
            else {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
                );
            };
            let next_receipt = match transition {
                PoisonReceiptTransition::ReplaceSettled => CommitReceiptWire::PoisonedSettled {
                    planned: encode_planned_commit(planned),
                },
                PoisonReceiptTransition::ReplaceUnsettled => CommitReceiptWire::PoisonedUnsettled {
                    planned: encode_planned_commit(planned),
                },
                PoisonReceiptTransition::ExactNoChange => {
                    return MetadataCommitReceiptMutationBackendResultV1::Completed
                }
            };
            let mut next = state.wire.clone();
            next.commit_receipt = next_receipt;
            if install_next_wire(&mut state, next).is_err() {
                self.poisoned.store(true, Ordering::Release);
                MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown
            } else {
                MetadataCommitReceiptMutationBackendResultV1::Completed
            }
        })();
        command.complete(result)
    }
}

fn receipt_load_error(error: OwnerSessionJournalError) -> MetadataCommitReceiptErrorV1 {
    match error {
        OwnerSessionJournalError::Io { .. } => MetadataCommitReceiptErrorV1::Unavailable,
        OwnerSessionJournalError::Poisoned => MetadataCommitReceiptErrorV1::Poisoned,
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        OwnerSessionJournalError::UnsupportedPlatform => MetadataCommitReceiptErrorV1::Unavailable,
        OwnerSessionJournalError::InvalidJournal(_)
        | OwnerSessionJournalError::BindingMismatch(_)
        | OwnerSessionJournalError::Changed => MetadataCommitReceiptErrorV1::InvalidBinding,
    }
}

fn persist_not_dispatched(
    error: OwnerSessionJournalError,
) -> MetadataCommitReceiptPersistNotDispatchedV1 {
    match receipt_load_error(error) {
        MetadataCommitReceiptErrorV1::Unavailable | MetadataCommitReceiptErrorV1::Poisoned => {
            MetadataCommitReceiptPersistNotDispatchedV1::Unavailable
        }
        MetadataCommitReceiptErrorV1::InvalidBinding => {
            MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding
        }
    }
}

fn mutation_not_dispatched(
    error: OwnerSessionJournalError,
) -> MetadataCommitReceiptMutationNotDispatchedV1 {
    match receipt_load_error(error) {
        MetadataCommitReceiptErrorV1::Poisoned => {
            MetadataCommitReceiptMutationNotDispatchedV1::Poisoned
        }
        MetadataCommitReceiptErrorV1::Unavailable => {
            MetadataCommitReceiptMutationNotDispatchedV1::Unavailable
        }
        MetadataCommitReceiptErrorV1::InvalidBinding => {
            MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding
        }
    }
}

fn owner_release_receipt_binding(wire: &JournalWire) -> OwnerReleaseReceiptBinding {
    OwnerReleaseReceiptBinding {
        preparation_digest: wire.preparation_digest.clone(),
        root_id: wire.root_id.clone(),
        owner: wire.owner.clone(),
        endpoint: wire.endpoint.clone(),
        logical_shard_id: wire.logical_shard_id.clone(),
        authority_id: wire.authority_id.clone(),
        authority_generation: wire.authority_generation,
        holt_locator: wire.holt_locator.clone(),
    }
}

fn prepared_wire(preparation: &OwnerSessionPreparation) -> JournalWire {
    JournalWire {
        version: JOURNAL_VERSION,
        generation: 1,
        phase: JournalPhase::Prepared,
        preparation_digest: encode_hex(&preparation.digest),
        root_id: encode_hex(preparation.root_id.as_bytes()),
        owner: preparation.owner.as_str().to_owned(),
        endpoint: preparation.endpoint.clone(),
        logical_shard_id: encode_hex(preparation.logical_shard_id.as_bytes()),
        authority_id: encode_hex(preparation.authority.authority_id.as_bytes()),
        authority_generation: preparation.authority.authority_generation.get(),
        metadata_store_identity: encode_store_identity(preparation.metadata_store_identity),
        frozen_runtime_bundle_digest: encode_hex(&preparation.frozen_runtime_bundle_digest),
        holt_locator: encode_hex(&preparation.canonical_holt_locator),
        holt_object: None,
        commit_receipt: CommitReceiptWire::Clean {
            frontier: FrontierPointWire::Absent,
        },
        owner_token: None,
        release_owner_epoch: None,
        release_owner_incarnation_id: None,
        release_lease_id: None,
    }
}

fn preparation_from_wire(
    wire: &JournalWire,
) -> Result<OwnerSessionPreparation, OwnerSessionJournalError> {
    let metadata_store_identity = decode_store_identity(&wire.metadata_store_identity)?;
    let frozen_runtime_bundle_digest = frozen_bundle_digest_from_wire(wire)?;
    Ok(OwnerSessionPreparation {
        digest: decode_fixed_hex(&wire.preparation_digest, "preparation digest")?,
        metadata_store_identity,
        frozen_runtime_bundle_digest,
        root_id: RootId::from_bytes(decode_fixed_hex(&wire.root_id, "root id")?),
        owner: NodeId::new(wire.owner.clone())
            .map_err(|_| OwnerSessionJournalError::InvalidJournal("owner"))?,
        endpoint: wire.endpoint.clone(),
        logical_shard_id: nokv_types::LogicalShardId::from_bytes(decode_fixed_hex(
            &wire.logical_shard_id,
            "logical shard id",
        )?),
        authority: MetadataAuthorityFence {
            logical_shard_id: nokv_types::LogicalShardId::from_bytes(decode_fixed_hex(
                &wire.logical_shard_id,
                "logical shard id",
            )?),
            authority_id: nokv_types::MetadataAuthorityId::from_bytes(decode_fixed_hex(
                &wire.authority_id,
                "authority id",
            )?),
            authority_generation: nokv_types::MetadataAuthorityGeneration::new(
                wire.authority_generation,
            )
            .map_err(|_| OwnerSessionJournalError::InvalidJournal("authority generation"))?,
        },
        canonical_holt_locator: decode_hex_vec(&wire.holt_locator)?,
    })
}

fn validate_release_preparation_wire(
    wire: &JournalWire,
    preparation: &OwnerReleasePreparation,
) -> Result<(), OwnerSessionJournalError> {
    let root_id = RootId::from_bytes(decode_fixed_hex(&wire.root_id, "root id")?);
    let owner = NodeId::new(wire.owner.clone())
        .map_err(|_| OwnerSessionJournalError::InvalidJournal("owner"))?;
    let canonical_holt_locator = decode_hex_vec(&wire.holt_locator)?;
    if root_id != preparation.root_id
        || owner != preparation.owner
        || wire.endpoint != preparation.endpoint
        || canonical_holt_locator != preparation.canonical_holt_locator
    {
        return Err(OwnerSessionJournalError::BindingMismatch(
            "release-only local process binding",
        ));
    }
    Ok(())
}

fn validate_preparation_wire(
    wire: &JournalWire,
    preparation: &OwnerSessionPreparation,
) -> Result<(), OwnerSessionJournalError> {
    if preparation_from_wire(wire)? != *preparation {
        return Err(OwnerSessionJournalError::BindingMismatch(
            "prepared create receipt",
        ));
    }
    Ok(())
}

fn validate_token_wire(
    wire: &JournalWire,
    token: &OwnerSessionToken,
) -> Result<(), OwnerSessionJournalError> {
    let preparation = preparation_from_wire(wire)?;
    let store_identity = decode_store_identity(&wire.metadata_store_identity)?;
    if token.logical_shard_id != preparation.logical_shard_id
        || token.lease.owner != preparation.owner
        || token.endpoint != preparation.endpoint
        || token.lease.authority != preparation.authority
        || store_identity.logical_shard_id != token.logical_shard_id
        || store_identity.authority_id != token.lease.authority.authority_id
        || store_identity.authority_generation != token.lease.authority.authority_generation
        || store_identity.consistency_domain_id != token.consistency_domain_id
        || store_identity.profile_fingerprint != token.profile_fingerprint
        || store_identity.contract_digest != token.contract_digest
        || encode_hex(&preparation_digest_from_token(
            token,
            &preparation.canonical_holt_locator,
        )) != wire.preparation_digest
    {
        return Err(OwnerSessionJournalError::BindingMismatch("owner token"));
    }
    Ok(())
}

fn validate_release_lease_wire(
    wire: &JournalWire,
    lease: &LogicalShardLease,
) -> Result<(), OwnerSessionJournalError> {
    let preparation = preparation_from_wire(wire)?;
    if lease.logical_shard_id != preparation.logical_shard_id
        || lease.owner != preparation.owner
        || lease.authority != preparation.authority
        || lease
            .owner_incarnation_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        || lease.lease_id == 0
    {
        return Err(OwnerSessionJournalError::BindingMismatch(
            "release owner lease",
        ));
    }
    if let Some(token) = wire.owner_token.as_deref() {
        let token = OwnerSessionToken::decode(&decode_hex_vec(token)?)
            .map_err(|_| OwnerSessionJournalError::InvalidJournal("owner token"))?;
        validate_token_wire(wire, &token)?;
        if token.lease() != lease {
            return Err(OwnerSessionJournalError::BindingMismatch(
                "release owner token",
            ));
        }
    }
    if wire.phase == JournalPhase::Releasing
        && (wire.release_owner_epoch != Some(lease.owner_epoch.get())
            || wire.release_owner_incarnation_id.as_deref()
                != Some(encode_hex(lease.owner_incarnation_id.as_bytes()).as_str())
            || wire.release_lease_id != Some(lease.lease_id))
    {
        return Err(OwnerSessionJournalError::BindingMismatch(
            "releasing owner lease",
        ));
    }
    Ok(())
}

fn release_lease_from_wire(
    wire: &JournalWire,
) -> Result<LogicalShardLease, OwnerSessionJournalError> {
    let preparation = preparation_from_wire(wire)?;
    let owner_epoch = OwnerEpoch::new(wire.release_owner_epoch.ok_or(
        OwnerSessionJournalError::InvalidJournal("missing release owner epoch"),
    )?)
    .map_err(|_| OwnerSessionJournalError::InvalidJournal("release owner epoch"))?;
    let owner_incarnation_id = OwnerIncarnationId::from_bytes(decode_fixed_hex(
        wire.release_owner_incarnation_id.as_deref().ok_or(
            OwnerSessionJournalError::InvalidJournal("missing release owner incarnation id"),
        )?,
        "release owner incarnation id",
    )?);
    let lease_id = wire
        .release_lease_id
        .filter(|lease_id| *lease_id != 0)
        .ok_or(OwnerSessionJournalError::InvalidJournal(
            "missing release lease id",
        ))?;
    let lease = LogicalShardLease {
        logical_shard_id: preparation.logical_shard_id,
        owner: preparation.owner,
        owner_epoch,
        owner_incarnation_id,
        lease_id,
        authority: preparation.authority,
    };
    validate_release_lease_wire(wire, &lease)?;
    Ok(lease)
}

fn preparation_digest_from_token(token: &OwnerSessionToken, locator: &[u8]) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(PREPARATION_DOMAIN);
    hasher.update(token.root_id.as_bytes());
    hasher.update([match token.layout_profile {
        nokv_types::RootLayoutProfile::SingleShardRoot => 1,
        nokv_types::RootLayoutProfile::PartitionedRoot => 2,
    }]);
    hasher.update(token.layout_generation.get().to_be_bytes());
    hasher.update(token.partition_id.as_bytes());
    hasher.update(token.logical_shard_id.as_bytes());
    hasher.update(token.placement_generation.get().to_be_bytes());
    hasher.update(token.lease.authority.authority_id.as_bytes());
    hasher.update(
        token
            .lease
            .authority
            .authority_generation
            .get()
            .to_be_bytes(),
    );
    hash_bytes(&mut hasher, token.provider_profile_id.as_str().as_bytes());
    hasher.update(token.profile_fingerprint);
    hasher.update(token.consistency_domain_id.as_bytes());
    hasher.update(token.contract_digest.as_bytes());
    hash_bytes(&mut hasher, token.lease.owner.as_str().as_bytes());
    hash_bytes(&mut hasher, token.endpoint.as_bytes());
    hash_bytes(&mut hasher, locator);
    hasher.finalize().into()
}

fn preparation_digest(
    placement: &RootPlacement,
    authority: &MetadataAuthorityRecord,
    owner: &NodeId,
    endpoint: &str,
    locator: &[u8],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(PREPARATION_DOMAIN);
    hasher.update(placement.root_id.as_bytes());
    hasher.update([match placement.layout_profile {
        nokv_types::RootLayoutProfile::SingleShardRoot => 1,
        nokv_types::RootLayoutProfile::PartitionedRoot => 2,
    }]);
    hasher.update(placement.layout_generation.get().to_be_bytes());
    hasher.update(placement.partition_id.as_bytes());
    hasher.update(placement.logical_shard_id.as_bytes());
    hasher.update(placement.placement_generation.get().to_be_bytes());
    hasher.update(authority.fence().authority_id.as_bytes());
    hasher.update(authority.fence().authority_generation.get().to_be_bytes());
    hash_bytes(
        &mut hasher,
        authority.active.provider_profile_id.as_str().as_bytes(),
    );
    hasher.update(authority.active.profile_fingerprint);
    hasher.update(authority.active.consistency_domain_id.as_bytes());
    hasher.update(authority.active.contract_digest.as_bytes());
    hash_bytes(&mut hasher, owner.as_str().as_bytes());
    hash_bytes(&mut hasher, endpoint.as_bytes());
    hash_bytes(&mut hasher, locator);
    hasher.finalize().into()
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn runtime_bundle_digest(
    preparation_digest: [u8; SHA256_BYTES],
    store_identity: MetadataStoreIdentity,
    canonical_holt_locator: &[u8],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(RUNTIME_BUNDLE_DOMAIN);
    hasher.update(preparation_digest);
    hash_store_identity(&mut hasher, store_identity);
    hash_bytes(&mut hasher, canonical_holt_locator);
    hasher.finalize().into()
}

fn hash_store_identity(hasher: &mut Sha256, identity: MetadataStoreIdentity) {
    hasher.update(identity.logical_shard_id.as_bytes());
    hasher.update(identity.authority_id.as_bytes());
    hasher.update(identity.authority_generation.get().to_be_bytes());
    hasher.update(identity.consistency_domain_id.as_bytes());
    hasher.update(identity.profile_fingerprint);
    hasher.update(identity.contract_digest.as_bytes());
}

fn encode_store_identity(identity: MetadataStoreIdentity) -> MetadataStoreIdentityWire {
    MetadataStoreIdentityWire {
        logical_shard_id: encode_hex(identity.logical_shard_id.as_bytes()),
        authority_id: encode_hex(identity.authority_id.as_bytes()),
        authority_generation: identity.authority_generation.get(),
        consistency_domain_id: encode_hex(identity.consistency_domain_id.as_bytes()),
        profile_fingerprint: encode_hex(&identity.profile_fingerprint),
        contract_digest: encode_hex(identity.contract_digest.as_bytes()),
    }
}

fn decode_store_identity(
    wire: &MetadataStoreIdentityWire,
) -> Result<MetadataStoreIdentity, OwnerSessionJournalError> {
    let identity = MetadataStoreIdentity {
        logical_shard_id: nokv_types::LogicalShardId::from_bytes(decode_fixed_hex(
            &wire.logical_shard_id,
            "metadata store logical shard id",
        )?),
        authority_id: MetadataAuthorityId::from_bytes(decode_fixed_hex(
            &wire.authority_id,
            "metadata store authority id",
        )?),
        authority_generation: MetadataAuthorityGeneration::new(wire.authority_generation).map_err(
            |_| OwnerSessionJournalError::InvalidJournal("metadata store authority generation"),
        )?,
        consistency_domain_id: ConsistencyDomainId::from_bytes(decode_fixed_hex(
            &wire.consistency_domain_id,
            "metadata store consistency domain id",
        )?),
        profile_fingerprint: decode_fixed_hex(
            &wire.profile_fingerprint,
            "metadata store profile fingerprint",
        )?,
        contract_digest: MetadataContractDigest::from_bytes(decode_fixed_hex(
            &wire.contract_digest,
            "metadata store contract digest",
        )?),
    };
    validate_store_identity_binding(identity)?;
    Ok(identity)
}

fn validate_store_identity_binding(
    identity: MetadataStoreIdentity,
) -> Result<(), OwnerSessionJournalError> {
    if identity
        .authority_id
        .as_bytes()
        .iter()
        .all(|byte| *byte == 0)
        || identity
            .consistency_domain_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        || identity.profile_fingerprint.iter().all(|byte| *byte == 0)
        || identity
            .contract_digest
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
    {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "metadata store identity contains a zero binding",
        ));
    }
    Ok(())
}

fn frozen_bundle_digest_from_wire(
    wire: &JournalWire,
) -> Result<[u8; SHA256_BYTES], OwnerSessionJournalError> {
    let persisted = decode_fixed_hex(
        &wire.frozen_runtime_bundle_digest,
        "frozen runtime bundle digest",
    )?;
    let expected = runtime_bundle_digest(
        decode_fixed_hex(&wire.preparation_digest, "preparation digest")?,
        decode_store_identity(&wire.metadata_store_identity)?,
        &decode_hex_vec(&wire.holt_locator)?,
    );
    if persisted.iter().all(|byte| *byte == 0) || persisted != expected {
        return Err(OwnerSessionJournalError::BindingMismatch(
            "frozen runtime bundle digest",
        ));
    }
    Ok(persisted)
}

fn encode_frontier_point(frontier: MetadataFrontierPointV1) -> FrontierPointWire {
    match frontier {
        MetadataFrontierPointV1::Absent => FrontierPointWire::Absent,
        MetadataFrontierPointV1::Exact(frontier) => FrontierPointWire::Exact {
            frontier: encode_frontier(frontier),
        },
    }
}

fn decode_frontier_point(
    wire: &FrontierPointWire,
) -> Result<MetadataFrontierPointV1, OwnerSessionJournalError> {
    match wire {
        FrontierPointWire::Absent => Ok(MetadataFrontierPointV1::Absent),
        FrontierPointWire::Exact { frontier } => {
            let frontier = decode_frontier(frontier)?;
            if frontier.chain_digest.iter().all(|byte| *byte == 0) {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "exact metadata frontier has a zero chain digest",
                ));
            }
            Ok(MetadataFrontierPointV1::Exact(frontier))
        }
    }
}

fn encode_commit_purpose(purpose: &MetadataCommitPurposeV1) -> MetadataCommitPurposeWire {
    match purpose {
        MetadataCommitPurposeV1::Genesis {
            authority_marker_digest,
        } => MetadataCommitPurposeWire::Genesis {
            authority_marker_digest: encode_hex(authority_marker_digest),
        },
        MetadataCommitPurposeV1::AdvanceOwnerEpoch { expected, next } => {
            MetadataCommitPurposeWire::AdvanceOwnerEpoch {
                expected: expected.map(OwnerEpoch::get),
                next: next.get(),
            }
        }
        MetadataCommitPurposeV1::ObserveLeaseClock {
            root_id,
            placement_generation,
            owner_epoch,
            observed_ms,
        } => MetadataCommitPurposeWire::ObserveLeaseClock {
            root_id: encode_hex(root_id.as_bytes()),
            placement_generation: placement_generation.get(),
            owner_epoch: owner_epoch.get(),
            observed_ms: *observed_ms,
        },
        MetadataCommitPurposeV1::MetadataCommand {
            class,
            root_id,
            request_id,
            command_digest,
            lease_deadline_ms,
        } => MetadataCommitPurposeWire::MetadataCommand {
            class: match class {
                MetadataCommandCommitClassV1::Domain => MetadataCommandCommitClassWire::Domain,
                MetadataCommandCommitClassV1::RootFence => {
                    MetadataCommandCommitClassWire::RootFence
                }
            },
            root_id: encode_hex(root_id.as_bytes()),
            request_id: encode_hex(request_id.as_bytes()),
            command_digest: encode_hex(command_digest.as_bytes()),
            lease_deadline_ms: *lease_deadline_ms,
        },
        MetadataCommitPurposeV1::Authority {
            action,
            prior_marker_digest,
            next_marker_digest,
        } => MetadataCommitPurposeWire::Authority {
            action: encode_authority_action(*action),
            prior_marker_digest: encode_hex(prior_marker_digest),
            next_marker_digest: encode_hex(next_marker_digest),
        },
    }
}

fn decode_commit_purpose(
    wire: &MetadataCommitPurposeWire,
) -> Result<MetadataCommitPurposeV1, OwnerSessionJournalError> {
    Ok(match wire {
        MetadataCommitPurposeWire::Genesis {
            authority_marker_digest,
        } => MetadataCommitPurposeV1::Genesis {
            authority_marker_digest: decode_fixed_hex(
                authority_marker_digest,
                "genesis authority marker digest",
            )?,
        },
        MetadataCommitPurposeWire::AdvanceOwnerEpoch { expected, next } => {
            MetadataCommitPurposeV1::AdvanceOwnerEpoch {
                expected: expected
                    .map(|value| {
                        OwnerEpoch::new(value).map_err(|_| {
                            OwnerSessionJournalError::InvalidJournal("expected owner epoch")
                        })
                    })
                    .transpose()?,
                next: OwnerEpoch::new(*next)
                    .map_err(|_| OwnerSessionJournalError::InvalidJournal("next owner epoch"))?,
            }
        }
        MetadataCommitPurposeWire::ObserveLeaseClock {
            root_id,
            placement_generation,
            owner_epoch,
            observed_ms,
        } => MetadataCommitPurposeV1::ObserveLeaseClock {
            root_id: RootId::from_bytes(decode_fixed_hex(root_id, "lease-clock root id")?),
            placement_generation: PlacementGeneration::new(*placement_generation).map_err(
                |_| OwnerSessionJournalError::InvalidJournal("lease-clock placement generation"),
            )?,
            owner_epoch: OwnerEpoch::new(*owner_epoch)
                .map_err(|_| OwnerSessionJournalError::InvalidJournal("lease-clock owner epoch"))?,
            observed_ms: *observed_ms,
        },
        MetadataCommitPurposeWire::MetadataCommand {
            class,
            root_id,
            request_id,
            command_digest,
            lease_deadline_ms,
        } => MetadataCommitPurposeV1::MetadataCommand {
            class: match class {
                MetadataCommandCommitClassWire::Domain => MetadataCommandCommitClassV1::Domain,
                MetadataCommandCommitClassWire::RootFence => {
                    MetadataCommandCommitClassV1::RootFence
                }
            },
            root_id: RootId::from_bytes(decode_fixed_hex(root_id, "metadata command root id")?),
            request_id: RequestId::from_bytes(decode_fixed_hex(
                request_id,
                "metadata command request id",
            )?),
            command_digest: CommandDigest::from_bytes(decode_fixed_hex(
                command_digest,
                "metadata command digest",
            )?),
            lease_deadline_ms: *lease_deadline_ms,
        },
        MetadataCommitPurposeWire::Authority {
            action,
            prior_marker_digest,
            next_marker_digest,
        } => MetadataCommitPurposeV1::Authority {
            action: decode_authority_action(action)?,
            prior_marker_digest: decode_fixed_hex(
                prior_marker_digest,
                "prior authority marker digest",
            )?,
            next_marker_digest: decode_fixed_hex(
                next_marker_digest,
                "next authority marker digest",
            )?,
        },
    })
}

fn encode_authority_action(
    action: MetadataAuthorityCommitActionV1,
) -> MetadataAuthorityCommitActionWire {
    match action {
        MetadataAuthorityCommitActionV1::Quiesce {
            migration_id,
            owner_epoch,
        } => MetadataAuthorityCommitActionWire::Quiesce {
            migration_id: encode_hex(migration_id.as_bytes()),
            owner_epoch: owner_epoch.get(),
        },
        MetadataAuthorityCommitActionV1::FenceQuiescedSource {
            migration_id,
            source_receipt_digest,
        } => MetadataAuthorityCommitActionWire::FenceQuiescedSource {
            migration_id: encode_hex(migration_id.as_bytes()),
            source_receipt_digest: encode_hex(&source_receipt_digest),
        },
        MetadataAuthorityCommitActionV1::ActivateTarget {
            migration_id,
            activation_token_digest,
        } => MetadataAuthorityCommitActionWire::ActivateTarget {
            migration_id: encode_hex(migration_id.as_bytes()),
            activation_token_digest: encode_hex(&activation_token_digest),
        },
        MetadataAuthorityCommitActionV1::FenceTarget {
            migration_id,
            target_binding_digest,
        } => MetadataAuthorityCommitActionWire::FenceTarget {
            migration_id: encode_hex(migration_id.as_bytes()),
            target_binding_digest: encode_hex(&target_binding_digest),
        },
    }
}

fn decode_authority_action(
    wire: &MetadataAuthorityCommitActionWire,
) -> Result<MetadataAuthorityCommitActionV1, OwnerSessionJournalError> {
    Ok(match wire {
        MetadataAuthorityCommitActionWire::Quiesce {
            migration_id,
            owner_epoch,
        } => MetadataAuthorityCommitActionV1::Quiesce {
            migration_id: OperationId::from_bytes(decode_fixed_hex(
                migration_id,
                "quiesce migration id",
            )?),
            owner_epoch: OwnerEpoch::new(*owner_epoch)
                .map_err(|_| OwnerSessionJournalError::InvalidJournal("quiesce owner epoch"))?,
        },
        MetadataAuthorityCommitActionWire::FenceQuiescedSource {
            migration_id,
            source_receipt_digest,
        } => MetadataAuthorityCommitActionV1::FenceQuiescedSource {
            migration_id: OperationId::from_bytes(decode_fixed_hex(
                migration_id,
                "source-fence migration id",
            )?),
            source_receipt_digest: decode_fixed_hex(
                source_receipt_digest,
                "source quiesce receipt digest",
            )?,
        },
        MetadataAuthorityCommitActionWire::ActivateTarget {
            migration_id,
            activation_token_digest,
        } => MetadataAuthorityCommitActionV1::ActivateTarget {
            migration_id: OperationId::from_bytes(decode_fixed_hex(
                migration_id,
                "target-activation migration id",
            )?),
            activation_token_digest: decode_fixed_hex(
                activation_token_digest,
                "target activation token digest",
            )?,
        },
        MetadataAuthorityCommitActionWire::FenceTarget {
            migration_id,
            target_binding_digest,
        } => MetadataAuthorityCommitActionV1::FenceTarget {
            migration_id: OperationId::from_bytes(decode_fixed_hex(
                migration_id,
                "target-fence migration id",
            )?),
            target_binding_digest: decode_fixed_hex(
                target_binding_digest,
                "target binding digest",
            )?,
        },
    })
}

fn encode_planned_commit(planned: &PlannedMetadataCommitV1) -> PlannedMetadataCommitWire {
    PlannedMetadataCommitWire {
        store_identity: encode_store_identity(planned.store_identity()),
        frozen_bundle_digest: encode_hex(&planned.frozen_bundle_digest()),
        purpose: encode_commit_purpose(planned.purpose()),
        prior: encode_frontier_point(planned.prior()),
        exact_next: encode_frontier(planned.exact_next()),
        canonical_digest: encode_hex(&planned.canonical_digest()),
    }
}

fn decode_planned_commit(
    wire: &PlannedMetadataCommitWire,
) -> Result<PlannedMetadataCommitV1, OwnerSessionJournalError> {
    PlannedMetadataCommitV1::from_durable_parts_v1(
        decode_store_identity(&wire.store_identity)?,
        decode_fixed_hex(&wire.frozen_bundle_digest, "planned frozen bundle digest")?,
        decode_commit_purpose(&wire.purpose)?,
        decode_frontier_point(&wire.prior)?,
        decode_frontier(&wire.exact_next)?,
        decode_fixed_hex(&wire.canonical_digest, "canonical commit plan digest")?,
    )
    .map_err(|_| OwnerSessionJournalError::InvalidJournal("invalid exact metadata commit plan"))
}

fn decode_commit_receipt(
    wire: &JournalWire,
) -> Result<MetadataCommitReceiptStateV1, OwnerSessionJournalError> {
    let store_identity = decode_store_identity(&wire.metadata_store_identity)?;
    let frozen_bundle_digest = frozen_bundle_digest_from_wire(wire)?;
    let state = match &wire.commit_receipt {
        CommitReceiptWire::Clean { frontier } => MetadataCommitReceiptStateV1::Clean {
            store_identity,
            frozen_bundle_digest,
            frontier: decode_frontier_point(frontier)?,
        },
        CommitReceiptWire::Pending { planned } => {
            MetadataCommitReceiptStateV1::Pending(decode_planned_commit(planned)?)
        }
        CommitReceiptWire::PoisonedSettled { planned } => {
            MetadataCommitReceiptStateV1::PoisonedSettled(decode_planned_commit(planned)?)
        }
        CommitReceiptWire::PoisonedUnsettled { planned } => {
            MetadataCommitReceiptStateV1::PoisonedUnsettled(decode_planned_commit(planned)?)
        }
    };
    let binding_matches = match &state {
        MetadataCommitReceiptStateV1::Clean { .. } => true,
        MetadataCommitReceiptStateV1::Pending(planned)
        | MetadataCommitReceiptStateV1::PoisonedSettled(planned)
        | MetadataCommitReceiptStateV1::PoisonedUnsettled(planned) => {
            planned.store_identity() == store_identity
                && planned.frozen_bundle_digest() == frozen_bundle_digest
        }
        MetadataCommitReceiptStateV1::UntrackedStandalone => false,
    };
    if !binding_matches {
        return Err(OwnerSessionJournalError::BindingMismatch(
            "exact metadata commit receipt",
        ));
    }
    Ok(state)
}

fn encode_frontier(frontier: AcknowledgedMetadataFrontier) -> FrontierWire {
    FrontierWire {
        write_sequence: frontier.write_sequence,
        commit_version: frontier.commit_version.get(),
        recovery_lsn: frontier.recovery_lsn,
        chain_digest: encode_hex(&frontier.chain_digest),
    }
}

fn decode_frontier(
    wire: &FrontierWire,
) -> Result<AcknowledgedMetadataFrontier, OwnerSessionJournalError> {
    Ok(AcknowledgedMetadataFrontier {
        write_sequence: wire.write_sequence,
        commit_version: CommitVersion::new(wire.commit_version)
            .map_err(|_| OwnerSessionJournalError::InvalidJournal("commit version"))?,
        recovery_lsn: wire.recovery_lsn,
        chain_digest: decode_fixed_hex(&wire.chain_digest, "recovery chain digest")?,
    })
}

fn install_next_wire(
    state: &mut JournalFileState,
    mut next: JournalWire,
) -> Result<(), OwnerSessionJournalError> {
    if next == state.wire {
        return validate_exact_file_state(state);
    }
    next.generation =
        next.generation
            .checked_add(1)
            .ok_or(OwnerSessionJournalError::InvalidJournal(
                "journal generation overflow",
            ))?;
    let encoded = encode_wire(&next)?;
    let identity = replace_file(state, &encoded)?;
    state.identity = identity;
    state.encoded = encoded;
    state.wire = next;
    Ok(())
}

fn encode_wire(wire: &JournalWire) -> Result<Vec<u8>, OwnerSessionJournalError> {
    let encoded = serde_json::to_vec(wire)
        .map_err(|_| OwnerSessionJournalError::InvalidJournal("cannot encode canonical JSON"))?;
    if encoded.is_empty() || encoded.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "encoded journal exceeds the supported bound",
        ));
    }
    Ok(encoded)
}

fn decode_wire(encoded: &[u8]) -> Result<JournalWire, OwnerSessionJournalError> {
    let wire: JournalWire = serde_json::from_slice(encoded)
        .map_err(|_| OwnerSessionJournalError::InvalidJournal("malformed canonical JSON"))?;
    if wire.version != JOURNAL_VERSION || wire.generation == 0 {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "unsupported version or generation",
        ));
    }
    let preparation = preparation_from_wire(&wire)?;
    validate_preparation_wire(&wire, &preparation)?;
    let store_identity = decode_store_identity(&wire.metadata_store_identity)?;
    if store_identity != preparation.metadata_store_identity
        || store_identity.logical_shard_id != preparation.logical_shard_id
        || store_identity.authority_id != preparation.authority.authority_id
        || store_identity.authority_generation != preparation.authority.authority_generation
    {
        return Err(OwnerSessionJournalError::BindingMismatch(
            "metadata store and owner preparation identity",
        ));
    }
    let receipt = decode_commit_receipt(&wire)?;
    let receipt_may_belong_to_serving = match &receipt {
        MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Exact(_),
            ..
        } => true,
        MetadataCommitReceiptStateV1::Pending(planned)
        | MetadataCommitReceiptStateV1::PoisonedSettled(planned)
        | MetadataCommitReceiptStateV1::PoisonedUnsettled(planned) => {
            matches!(planned.prior(), MetadataFrontierPointV1::Exact(_))
        }
        MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Absent,
            ..
        }
        | MetadataCommitReceiptStateV1::UntrackedStandalone => false,
    };
    match wire.phase {
        JournalPhase::Prepared
            if wire.owner_token.is_some()
                || wire.release_owner_epoch.is_some()
                || wire.release_owner_incarnation_id.is_some()
                || wire.release_lease_id.is_some() =>
        {
            return Err(OwnerSessionJournalError::InvalidJournal(
                "prepared journal contains an owner or release token",
            ));
        }
        JournalPhase::Serving
            if wire.owner_token.is_none()
                || wire.holt_object.is_none()
                || !receipt_may_belong_to_serving
                || wire.release_owner_epoch.is_some()
                || wire.release_owner_incarnation_id.is_some()
                || wire.release_lease_id.is_some() =>
        {
            return Err(OwnerSessionJournalError::InvalidJournal(
                "serving journal is incomplete",
            ));
        }
        JournalPhase::Releasing => {
            release_lease_from_wire(&wire)?;
        }
        _ => {}
    }
    if encode_wire(&wire)?.as_slice() != encoded {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "input is not canonical",
        ));
    }
    Ok(wire)
}

#[cfg(unix)]
fn canonical_locator(path: &Path) -> Result<Vec<u8>, OwnerSessionJournalError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "Holt locator must be a real directory",
                ));
            }
            Ok(fs::canonicalize(path)
                .map_err(|error| io_error("Holt locator resolution", error))?
                .as_os_str()
                .as_bytes()
                .to_vec())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let name = path
                .file_name()
                .ok_or(OwnerSessionJournalError::InvalidJournal(
                    "Holt locator has no file name",
                ))?;
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            Ok(fs::canonicalize(parent)
                .map_err(|error| io_error("Holt locator parent resolution", error))?
                .join(name)
                .as_os_str()
                .as_bytes()
                .to_vec())
        }
        Err(error) => Err(io_error("Holt locator inspection", error)),
    }
}

#[cfg(unix)]
fn canonical_file_path(
    path: &Path,
) -> Result<(PathBuf, PathBuf, File, OsString), OwnerSessionJournalError> {
    let name = path
        .file_name()
        .ok_or(OwnerSessionJournalError::InvalidJournal(
            "journal path has no file name",
        ))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent =
        fs::canonicalize(parent).map_err(|error| io_error("journal parent resolution", error))?;
    let metadata = fs::symlink_metadata(&parent)
        .map_err(|error| io_error("journal parent inspection", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "journal parent must be a real directory",
        ));
    }
    let full = parent.join(name);
    let parent_directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&parent)
        .map_err(|error| io_error("journal parent open", error))?;
    let opened = parent_directory
        .metadata()
        .map_err(|error| io_error("journal parent inspection", error))?;
    if !opened.is_dir() || opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
        return Err(OwnerSessionJournalError::Changed);
    }
    Ok((parent, full, parent_directory, name.to_os_string()))
}

#[cfg(unix)]
fn create_initial_file(
    parent_directory: &File,
    file_name: &std::ffi::OsStr,
    encoded: &[u8],
) -> Result<Option<FileIdentity>, OwnerSessionJournalError> {
    let (mut file, temp) =
        create_unique_temp_file(parent_directory, "initial temporary create", &TEMP_SEQUENCE)?;
    let result = (|| {
        file.write_all(encoded)
            .map_err(|error| io_error("initial temporary write", error))?;
        file.sync_all()
            .map_err(|error| io_error("initial temporary file sync", error))?;
        validate_file_metadata(
            &file
                .metadata()
                .map_err(|error| io_error("initial temporary inspection", error))?,
        )?;
        if !renameat_noreplace_file(parent_directory, &temp, file_name)? {
            return Ok(None);
        }
        let (linked, identity) = read_durable_stable_file_at(parent_directory, file_name)?;
        if linked != encoded {
            return Err(OwnerSessionJournalError::Changed);
        }
        Ok(Some(identity))
    })();
    if result.as_ref().is_err() || matches!(&result, Ok(None)) {
        let _ = unlinkat_file(parent_directory, &temp);
    }
    result
}

#[cfg(unix)]
fn replace_file(
    state: &JournalFileState,
    encoded: &[u8],
) -> Result<FileIdentity, OwnerSessionJournalError> {
    let (mut file, temp) = create_unique_temp_file(
        &state.parent_directory,
        "replacement temporary create",
        &TEMP_SEQUENCE,
    )?;
    let result = (|| {
        file.write_all(encoded)
            .map_err(|error| io_error("replacement write", error))?;
        file.sync_all()
            .map_err(|error| io_error("replacement file sync", error))?;
        validate_file_metadata(
            &file
                .metadata()
                .map_err(|error| io_error("replacement inspection", error))?,
        )?;
        let (current, identity) =
            read_durable_stable_file_at(&state.parent_directory, &state.file_name)?;
        if current != state.encoded || identity != state.identity {
            return Err(OwnerSessionJournalError::Changed);
        }
        renameat_file(&state.parent_directory, &temp, &state.file_name)?;
        replace_after_rename_test_failure()?;
        let (installed, installed_identity) =
            read_durable_stable_file_at(&state.parent_directory, &state.file_name)?;
        if installed != encoded {
            return Err(OwnerSessionJournalError::Changed);
        }
        Ok(installed_identity)
    })();
    if result.is_err() {
        let _ = unlinkat_file(&state.parent_directory, &temp);
    }
    result
}

#[cfg(unix)]
fn validate_exact_file_state(state: &JournalFileState) -> Result<(), OwnerSessionJournalError> {
    let (current, identity) =
        read_durable_stable_file_at(&state.parent_directory, &state.file_name)?;
    if current != state.encoded || identity != state.identity {
        return Err(OwnerSessionJournalError::Changed);
    }
    Ok(())
}

#[cfg(unix)]
fn read_stable_file_at(
    parent_directory: &File,
    file_name: &std::ffi::OsStr,
) -> Result<(Vec<u8>, FileIdentity), OwnerSessionJournalError> {
    read_stable_file_at_inner(parent_directory, file_name, false)
}

#[cfg(unix)]
fn read_durable_stable_file_at(
    parent_directory: &File,
    file_name: &std::ffi::OsStr,
) -> Result<(Vec<u8>, FileIdentity), OwnerSessionJournalError> {
    read_stable_file_at_inner(parent_directory, file_name, true)
}

#[cfg(unix)]
fn read_stable_file_at_inner(
    parent_directory: &File,
    file_name: &std::ffi::OsStr,
    durable_adoption: bool,
) -> Result<(Vec<u8>, FileIdentity), OwnerSessionJournalError> {
    let mut file = openat_read(parent_directory, file_name, "open")?;
    let opened = validate_file_metadata(
        &file
            .metadata()
            .map_err(|error| io_error("opened file inspection", error))?,
    )?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_JOURNAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io_error("read", error))?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "journal length is outside the supported bound",
        ));
    }
    if durable_adoption {
        file.sync_all()
            .map_err(|error| io_error("adopted file sync", error))?;
    }
    stable_read_test_hook();
    let mut linked = openat_read(parent_directory, file_name, "post-read open")?;
    let after = validate_file_metadata(
        &linked
            .metadata()
            .map_err(|error| io_error("post-read inspection", error))?,
    )?;
    let linked_bytes = read_bounded_journal(&mut linked, "post-read")?;
    let after_read = validate_file_metadata(
        &linked
            .metadata()
            .map_err(|error| io_error("post-read completion inspection", error))?,
    )?;
    if after != opened || after_read != opened || linked_bytes != bytes {
        return Err(OwnerSessionJournalError::Changed);
    }
    if durable_adoption {
        parent_directory
            .sync_all()
            .map_err(|error| io_error("adopted parent directory sync", error))?;
        let mut durable = openat_read(parent_directory, file_name, "post-adoption open")?;
        let durable_identity = validate_file_metadata(
            &durable
                .metadata()
                .map_err(|error| io_error("post-adoption inspection", error))?,
        )?;
        let durable_bytes = read_bounded_journal(&mut durable, "post-adoption read")?;
        let durable_after_read = validate_file_metadata(
            &durable
                .metadata()
                .map_err(|error| io_error("post-adoption completion inspection", error))?,
        )?;
        if durable_identity != opened || durable_after_read != opened || durable_bytes != bytes {
            return Err(OwnerSessionJournalError::Changed);
        }
    }
    Ok((bytes, opened))
}

#[cfg(unix)]
fn read_bounded_journal(
    file: &mut File,
    operation: &'static str,
) -> Result<Vec<u8>, OwnerSessionJournalError> {
    let mut bytes = Vec::new();
    Read::by_ref(file)
        .take(MAX_JOURNAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io_error(operation, error))?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "journal length is outside the supported bound",
        ));
    }
    Ok(bytes)
}

#[cfg(all(unix, test))]
fn stable_read_test_hook() {
    STABLE_READ_TEST_HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
}

#[cfg(all(unix, not(test)))]
fn stable_read_test_hook() {}

#[cfg(all(unix, test))]
fn replace_after_rename_test_failure() -> Result<(), OwnerSessionJournalError> {
    REPLACE_AFTER_RENAME_TEST_FAILURE.with(|failure| {
        if failure.replace(false) {
            Err(OwnerSessionJournalError::Io {
                operation: "replacement post-rename adoption",
                kind: io::ErrorKind::Other,
            })
        } else {
            Ok(())
        }
    })
}

#[cfg(all(unix, not(test)))]
fn replace_after_rename_test_failure() -> Result<(), OwnerSessionJournalError> {
    Ok(())
}

#[cfg(unix)]
fn create_unique_temp_file(
    parent_directory: &File,
    operation: &'static str,
    sequence: &AtomicU64,
) -> Result<(File, OsString), OwnerSessionJournalError> {
    const MAX_TEMP_ATTEMPTS: usize = 16;
    for _ in 0..MAX_TEMP_ATTEMPTS {
        let temp = owner_session_temp_name(sequence.fetch_add(1, Ordering::Relaxed));
        match openat_create(parent_directory, &temp, operation) {
            Ok(file) => return Ok((file, temp)),
            Err(OwnerSessionJournalError::Io {
                kind: io::ErrorKind::AlreadyExists,
                ..
            }) => {}
            Err(error) => return Err(error),
        }
    }
    Err(OwnerSessionJournalError::Io {
        operation: "temporary name allocation",
        kind: io::ErrorKind::AlreadyExists,
    })
}

#[cfg(unix)]
fn owner_session_temp_name(sequence: u64) -> OsString {
    OsString::from(format!(
        ".nokv-owner-session-{}-{sequence}.tmp",
        std::process::id()
    ))
}

#[cfg(unix)]
fn openat_create(
    parent_directory: &File,
    file_name: &std::ffi::OsStr,
    operation: &'static str,
) -> Result<File, OwnerSessionJournalError> {
    let name = c_file_name(file_name)?;
    // SAFETY: `parent_directory` is a live directory fd, `name` is a
    // NUL-terminated single component, and ownership of a successful fd is
    // transferred immediately into `File`.
    let descriptor = unsafe {
        libc::openat(
            parent_directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if descriptor < 0 {
        return Err(io_error(operation, io::Error::last_os_error()));
    }
    // SAFETY: `descriptor` was newly returned by `openat` and has not been
    // wrapped or closed elsewhere.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

#[cfg(unix)]
fn openat_read(
    parent_directory: &File,
    file_name: &std::ffi::OsStr,
    operation: &'static str,
) -> Result<File, OwnerSessionJournalError> {
    let name = c_file_name(file_name)?;
    // SAFETY: see `openat_create`; this call opens an existing entry read-only.
    let descriptor = unsafe {
        libc::openat(
            parent_directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        return Err(io_error(operation, io::Error::last_os_error()));
    }
    // SAFETY: `descriptor` is uniquely owned by this call.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

#[cfg(unix)]
fn renameat_file(
    parent_directory: &File,
    source: &std::ffi::OsStr,
    destination: &std::ffi::OsStr,
) -> Result<(), OwnerSessionJournalError> {
    let source = c_file_name(source)?;
    let destination = c_file_name(destination)?;
    // SAFETY: both names are valid single components relative to the same live
    // directory fd; `renameat` does not retain either pointer.
    let result = unsafe {
        libc::renameat(
            parent_directory.as_raw_fd(),
            source.as_ptr(),
            parent_directory.as_raw_fd(),
            destination.as_ptr(),
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io_error("replacement rename", io::Error::last_os_error()))
    }
}

#[cfg(unix)]
fn renameat_noreplace_file(
    parent_directory: &File,
    source: &std::ffi::OsStr,
    destination: &std::ffi::OsStr,
) -> Result<bool, OwnerSessionJournalError> {
    let source = c_file_name(source)?;
    let destination = c_file_name(destination)?;
    let descriptor = parent_directory.as_raw_fd();

    #[cfg(target_os = "linux")]
    // SAFETY: both names are valid single components relative to the same live
    // directory fd; `renameat2` does not retain either pointer.
    let result = unsafe {
        libc::renameat2(
            descriptor,
            source.as_ptr(),
            descriptor,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };

    #[cfg(target_os = "macos")]
    // SAFETY: both names are valid single components relative to the same live
    // directory fd; `renameatx_np` does not retain either pointer.
    let result = unsafe {
        libc::renameatx_np(
            descriptor,
            source.as_ptr(),
            descriptor,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (descriptor, source, destination);
        return Err(OwnerSessionJournalError::UnsupportedPlatform);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    if result == 0 {
        Ok(true)
    } else {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::AlreadyExists {
            Ok(false)
        } else {
            Err(io_error("initial no-replace rename", error))
        }
    }
}

#[cfg(unix)]
fn unlinkat_file(
    parent_directory: &File,
    file_name: &std::ffi::OsStr,
) -> Result<(), OwnerSessionJournalError> {
    let name = c_file_name(file_name)?;
    // SAFETY: `name` is relative to the held parent fd and is not retained.
    let result = unsafe { libc::unlinkat(parent_directory.as_raw_fd(), name.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(io_error("remove", io::Error::last_os_error()))
    }
}

#[cfg(unix)]
fn c_file_name(file_name: &std::ffi::OsStr) -> Result<CString, OwnerSessionJournalError> {
    if file_name.as_bytes().contains(&0) || Path::new(file_name).components().count() != 1 {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "journal file name is not one canonical path component",
        ));
    }
    CString::new(file_name.as_bytes())
        .map_err(|_| OwnerSessionJournalError::InvalidJournal("journal file name contains NUL"))
}

#[cfg(unix)]
fn validate_file_metadata(metadata: &Metadata) -> Result<FileIdentity, OwnerSessionJournalError> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "journal is not a regular file",
        ));
    }
    if metadata.permissions().mode() & 0o7777 != 0o600 {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "journal permissions must be exactly 0600",
        ));
    }
    if metadata.uid() != effective_user_id()
        || metadata.nlink() != 1
        || metadata.len() == 0
        || metadata.len() > MAX_JOURNAL_BYTES
    {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "journal owner, link count, or size is invalid",
        ));
    }
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(unix)]
fn capture_holt_object(path: &Path) -> Result<HoltObjectWire, OwnerSessionJournalError> {
    let directory =
        fs::symlink_metadata(path).map_err(|error| io_error("Holt directory inspection", error))?;
    if directory.file_type().is_symlink() || !directory.is_dir() {
        return Err(OwnerSessionJournalError::BindingMismatch("Holt directory"));
    }
    let lock = fs::symlink_metadata(path.join("store.lock"))
        .map_err(|error| io_error("Holt lock inspection", error))?;
    if lock.file_type().is_symlink() || !lock.is_file() {
        return Err(OwnerSessionJournalError::BindingMismatch("Holt lock file"));
    }
    Ok(HoltObjectWire {
        directory_device: directory.dev(),
        directory_inode: directory.ino(),
        lock_device: lock.dev(),
        lock_inode: lock.ino(),
    })
}

#[cfg(unix)]
fn prepare_holt_object(wire: &JournalWire) -> Result<HoltObjectWire, OwnerSessionJournalError> {
    let locator = decode_path(&wire.holt_locator)?;
    match fs::symlink_metadata(&locator) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(OwnerSessionJournalError::InvalidJournal(
                    "prepared Holt locator must be a real directory",
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(&locator).map_err(|error| io_error("Holt directory create", error))?;
            let parent = locator
                .parent()
                .ok_or(OwnerSessionJournalError::InvalidJournal(
                    "prepared Holt locator has no parent",
                ))?;
            sync_parent(parent)?;
        }
        Err(error) => return Err(io_error("Holt directory inspection", error)),
    }

    let lock_path = locator.join("store.lock");
    let mut entries = fs::read_dir(&locator)
        .map_err(|error| io_error("prepared Holt directory inspection", error))?;
    while let Some(entry) = entries
        .next()
        .transpose()
        .map_err(|error| io_error("prepared Holt directory inspection", error))?
    {
        if entry.file_name() != "store.lock" {
            return Err(OwnerSessionJournalError::InvalidJournal(
                "prepared Holt directory contains foreign entries",
            ));
        }
    }

    match fs::symlink_metadata(&lock_path) {
        Ok(metadata) => {
            validate_prepared_lock_metadata(&metadata)?;
            let lock = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&lock_path)
                .map_err(|error| io_error("prepared Holt lock open", error))?;
            let opened = lock
                .metadata()
                .map_err(|error| io_error("prepared Holt lock inspection", error))?;
            validate_prepared_lock_metadata(&opened)?;
            if metadata.dev() != opened.dev() || metadata.ino() != opened.ino() {
                return Err(OwnerSessionJournalError::Changed);
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let lock = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&lock_path)
                .map_err(|error| io_error("prepared Holt lock create", error))?;
            lock.sync_all()
                .map_err(|error| io_error("prepared Holt lock sync", error))?;
            validate_prepared_lock_metadata(
                &lock
                    .metadata()
                    .map_err(|error| io_error("prepared Holt lock inspection", error))?,
            )?;
            sync_parent(&locator)?;
        }
        Err(error) => return Err(io_error("prepared Holt lock inspection", error)),
    }
    capture_holt_object(&locator)
}

#[cfg(unix)]
fn validate_prepared_lock_metadata(metadata: &Metadata) -> Result<(), OwnerSessionJournalError> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "prepared Holt lock is not a regular file",
        ));
    }
    if metadata.permissions().mode() & 0o7777 != 0o600 {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "prepared Holt lock permissions must be exactly 0600",
        ));
    }
    if metadata.uid() != effective_user_id() || metadata.nlink() != 1 || metadata.len() != 0 {
        return Err(OwnerSessionJournalError::InvalidJournal(
            "prepared Holt lock owner, link count, or size is invalid",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn effective_user_id() -> u32 {
    // SAFETY: `geteuid` has no arguments, returns the calling process's
    // effective uid, and does not dereference memory.
    unsafe { libc::geteuid() }
}

#[cfg(unix)]
fn sync_parent(parent: &Path) -> Result<(), OwnerSessionJournalError> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("parent directory sync", error))
}

#[cfg(unix)]
fn path_from_bytes(bytes: &[u8]) -> &Path {
    use std::os::unix::ffi::OsStrExt as _;
    Path::new(std::ffi::OsStr::from_bytes(bytes))
}

#[cfg(unix)]
fn decode_path(encoded: &str) -> Result<PathBuf, OwnerSessionJournalError> {
    use std::os::unix::ffi::OsStringExt as _;
    Ok(PathBuf::from(std::ffi::OsString::from_vec(decode_hex_vec(
        encoded,
    )?)))
}

#[cfg(unix)]
fn encode_path(path: &Path) -> Result<String, OwnerSessionJournalError> {
    Ok(encode_hex(path.as_os_str().as_bytes()))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex_vec(value: &str) -> Result<Vec<u8>, OwnerSessionJournalError> {
    if !value.len().is_multiple_of(2) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(OwnerSessionJournalError::InvalidJournal("lowercase hex"));
    }
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        decoded.push(
            (decode_nibble(pair[0])
                .ok_or(OwnerSessionJournalError::InvalidJournal("lowercase hex"))?
                << 4)
                | decode_nibble(pair[1])
                    .ok_or(OwnerSessionJournalError::InvalidJournal("lowercase hex"))?,
        );
    }
    Ok(decoded)
}

fn decode_fixed_hex<const N: usize>(
    value: &str,
    field: &'static str,
) -> Result<[u8; N], OwnerSessionJournalError> {
    let decoded = decode_hex_vec(value)?;
    decoded
        .try_into()
        .map_err(|_| OwnerSessionJournalError::InvalidJournal(field))
}

fn decode_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn io_error(operation: &'static str, error: io::Error) -> OwnerSessionJournalError {
    OwnerSessionJournalError::Io {
        operation,
        kind: error.kind(),
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    use nokv_control::{
        ConsistencyDomainId, LogicalShardRecord, MetadataAuthorityBinding,
        MetadataAuthorityGeneration, MetadataAuthorityId, MetadataAuthorityRevision,
        MetadataContractDigest, MetadataProviderProfileId, OwnerEpoch, PlacementGeneration, RootId,
        RootLayoutGeneration, RootLayoutProfile, RootPartitionId, RootPlacementLifecycle,
    };
    use nokv_meta::built_in_holt::file_provider_factory_v1;
    use nokv_meta::provider::v1::{
        AtomicCommitOutcome, AtomicPlan, CreateRecoveryIntentV1, MetadataProvider,
        MetadataProviderFactoryV1, MetadataReadView, MetadataTransaction, OrderedSpaceId,
        ProviderCapabilities, ProviderContractOfferV1, ProviderCreateRequestV1,
        ProviderDiagnosticsV1, ProviderError, ProviderRecord, ProviderReopenRequestV1,
        ProviderScan, ProviderScanPage, ReadScope,
    };
    use nokv_meta::workspace::{
        AgentMetadataStore, MetadataCommitRecoveryFenceFactoryV1,
        MetadataOldDispatchExclusionInstallationV1, MetadataPendingRecoveryOpenCommandV1,
        MetadataPendingRecoveryOpenOutcomeV1, MetadataStoreCreateModeV1,
    };
    use nokv_types::LogicalShardId;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ReceiptResponseLoss {
        None,
        AfterPersist,
        AfterResolve,
        AfterPoison,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ProviderCommitFault {
        None,
        UnknownSettled,
        UnknownUnsettled,
    }

    struct JournalRuntimeBundle {
        factory: Arc<dyn MetadataCommitRecoveryFenceFactoryV1>,
        journal: Arc<OwnerSessionJournal>,
        response_loss: ReceiptResponseLoss,
        commit_fault: ProviderCommitFault,
        persist_calls: AtomicUsize,
        resolve_calls: AtomicUsize,
        poison_calls: AtomicUsize,
    }

    impl JournalRuntimeBundle {
        fn new(
            metadata_path: &Path,
            journal: Arc<OwnerSessionJournal>,
            response_loss: ReceiptResponseLoss,
            commit_fault: ProviderCommitFault,
        ) -> Arc<Self> {
            let runtime_guard: Arc<dyn HoltRuntimeGuard> = journal.clone();
            Arc::new(Self {
                factory: file_provider_factory_v1(metadata_path, runtime_guard),
                journal,
                response_loss,
                commit_fault,
                persist_calls: AtomicUsize::new(0),
                resolve_calls: AtomicUsize::new(0),
                poison_calls: AtomicUsize::new(0),
            })
        }

        fn wrap_provider(&self, provider: Arc<dyn MetadataProvider>) -> Arc<dyn MetadataProvider> {
            Arc::new(FaultingProvider {
                inner: provider,
                commit_fault: self.commit_fault,
            })
        }
    }

    impl MetadataProviderFactoryV1 for JournalRuntimeBundle {
        fn contract_offer(
            &self,
            schema: &nokv_meta::provider::v1::ProviderSchemaV1,
        ) -> Result<ProviderContractOfferV1, ProviderError> {
            self.factory.contract_offer(schema)
        }

        fn create(
            &self,
            request: &ProviderCreateRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.factory
                .create(request)
                .map(|provider| self.wrap_provider(provider))
        }

        fn reopen(
            &self,
            request: &ProviderReopenRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.factory
                .reopen(request)
                .map(|provider| self.wrap_provider(provider))
        }
    }

    impl MetadataCommitRecoveryFenceFactoryV1 for JournalRuntimeBundle {
        fn old_dispatch_exclusion_installation_v1(
            &self,
        ) -> MetadataOldDispatchExclusionInstallationV1 {
            self.factory.old_dispatch_exclusion_installation_v1()
        }

        fn reopen_pending_with_old_dispatch_excluded_v1(
            &self,
            command: MetadataPendingRecoveryOpenCommandV1,
        ) -> MetadataPendingRecoveryOpenOutcomeV1 {
            self.factory
                .reopen_pending_with_old_dispatch_excluded_v1(command)
        }
    }

    impl MetadataCommitReceiptStoreV1 for JournalRuntimeBundle {
        fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
            self.journal.commit_receipt_qualification_v1()
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
            self.journal.frozen_runtime_bundle_digest_v1()
        }

        fn load_commit_receipt_v1(
            &self,
            store_identity: MetadataStoreIdentity,
        ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
            self.journal.load_commit_receipt_v1(store_identity)
        }

        fn persist_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptPersistCommandV1,
        ) -> MetadataCommitReceiptPersistOutcomeV1 {
            self.persist_calls.fetch_add(1, Ordering::Relaxed);
            let outcome = self.journal.persist_pending_commit_v1(command);
            if self.response_loss == ReceiptResponseLoss::AfterPersist {
                panic!("lose the persist response after durable journal replacement");
            }
            outcome
        }

        fn resolve_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptResolveCommandV1,
        ) -> MetadataCommitReceiptResolveOutcomeV1 {
            self.resolve_calls.fetch_add(1, Ordering::Relaxed);
            let outcome = self.journal.resolve_pending_commit_v1(command);
            if self.response_loss == ReceiptResponseLoss::AfterResolve {
                panic!("lose the resolve response after durable journal replacement");
            }
            outcome
        }

        fn poison_commit_receipt_v1(
            &self,
            command: MetadataCommitReceiptPoisonCommandV1,
        ) -> MetadataCommitReceiptPoisonOutcomeV1 {
            self.poison_calls.fetch_add(1, Ordering::Relaxed);
            let outcome = self.journal.poison_commit_receipt_v1(command);
            if self.response_loss == ReceiptResponseLoss::AfterPoison {
                panic!("lose the poison response after durable journal replacement");
            }
            outcome
        }
    }

    struct FaultingProvider {
        inner: Arc<dyn MetadataProvider>,
        commit_fault: ProviderCommitFault,
    }

    impl MetadataProvider for FaultingProvider {
        fn logical_shard_id(&self) -> LogicalShardId {
            self.inner.logical_shard_id()
        }

        fn capabilities(&self) -> ProviderCapabilities {
            self.inner.capabilities()
        }

        fn validate_runtime(&self) -> Result<(), ProviderError> {
            self.inner.validate_runtime()
        }

        fn get(
            &self,
            space: OrderedSpaceId,
            key: &[u8],
        ) -> Result<Option<ProviderRecord>, ProviderError> {
            self.inner.get(space, key)
        }

        fn begin_read(
            &self,
            scopes: &[ReadScope],
        ) -> Result<Box<dyn MetadataReadView + 'static>, ProviderError> {
            self.inner.begin_read(scopes)
        }

        fn begin_write(&self) -> Result<Box<dyn MetadataTransaction + 'static>, ProviderError> {
            self.inner.begin_write().map(|inner| {
                Box::new(FaultingTransaction {
                    inner,
                    commit_fault: self.commit_fault,
                }) as Box<dyn MetadataTransaction>
            })
        }

        fn diagnostics(&self) -> Option<&dyn ProviderDiagnosticsV1> {
            self.inner.diagnostics()
        }
    }

    struct FaultingTransaction {
        inner: Box<dyn MetadataTransaction>,
        commit_fault: ProviderCommitFault,
    }

    impl MetadataReadView for FaultingTransaction {
        fn get(
            &self,
            space: OrderedSpaceId,
            key: &[u8],
        ) -> Result<Option<ProviderRecord>, ProviderError> {
            self.inner.get(space, key)
        }

        fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
            self.inner.scan(request)
        }
    }

    impl MetadataTransaction for FaultingTransaction {
        fn prefix_is_empty(
            &self,
            space: OrderedSpaceId,
            prefix: &[u8],
        ) -> Result<bool, ProviderError> {
            self.inner.prefix_is_empty(space, prefix)
        }

        fn commit(self: Box<Self>, plan: AtomicPlan) -> Result<AtomicCommitOutcome, ProviderError> {
            let Self {
                inner,
                commit_fault,
            } = *self;
            match commit_fault {
                ProviderCommitFault::None => inner.commit(plan),
                ProviderCommitFault::UnknownSettled => {
                    let _ = inner.commit(plan)?;
                    Err(ProviderError::unknown_commit_settled())
                }
                ProviderCommitFault::UnknownUnsettled => {
                    drop((inner, plan));
                    Err(ProviderError::unknown_commit_unsettled())
                }
            }
        }
    }

    fn placement() -> RootPlacement {
        RootPlacement {
            root_id: RootId::from_bytes([0x01; 16]),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id: LogicalShardId::from_bytes([0x02; 16]),
            placement_generation: PlacementGeneration::new(2).unwrap(),
            lifecycle: RootPlacementLifecycle::Active,
        }
    }

    fn authority() -> MetadataAuthorityRecord {
        MetadataAuthorityRecord {
            logical_shard_id: placement().logical_shard_id,
            record_revision: MetadataAuthorityRevision::new(1).unwrap(),
            authority_generation: MetadataAuthorityGeneration::new(5).unwrap(),
            active: MetadataAuthorityBinding {
                authority_id: MetadataAuthorityId::from_bytes([0xaa; 16]),
                provider_profile_id: MetadataProviderProfileId::new("holt-local-v1").unwrap(),
                profile_fingerprint: [0x11; SHA256_BYTES],
                consistency_domain_id: ConsistencyDomainId::from_bytes([0xbb; 16]),
                contract_digest: current_contract_digest(),
            },
            migration: None,
        }
    }

    fn owner() -> NodeId {
        NodeId::new("owner-a").unwrap()
    }

    fn endpoint() -> String {
        "metadata-a.internal:7750".to_owned()
    }

    fn current_contract_digest() -> MetadataContractDigest {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.workspace.metadata-contract.v1\0");
        hasher.update(
            b"nokv_workspace\0system-format-11\0cross-space-atomic-batch-v1\0opaque-record-witness-v1\0logical-commit-clock-v1\0recovery-outbox-v3\0authority-migration-receipt-v1\0",
        );
        MetadataContractDigest::from_bytes(hasher.finalize().into())
    }

    fn preparation(metadata_path: &Path, journal_path: &Path) -> OwnerSessionPreparation {
        OwnerSessionPreparation::new(
            &placement(),
            &authority(),
            owner(),
            endpoint(),
            metadata_path,
            journal_path,
        )
        .unwrap()
    }

    fn fixed_preparation() -> OwnerSessionPreparation {
        let metadata_store_identity = MetadataStoreIdentity {
            logical_shard_id: LogicalShardId::from_bytes([0x02; 16]),
            authority_id: MetadataAuthorityId::from_bytes([0xaa; 16]),
            authority_generation: MetadataAuthorityGeneration::new(5).unwrap(),
            consistency_domain_id: ConsistencyDomainId::from_bytes([0xbb; 16]),
            profile_fingerprint: [0x12; SHA256_BYTES],
            contract_digest: current_contract_digest(),
        };
        OwnerSessionPreparation {
            digest: [0x11; SHA256_BYTES],
            metadata_store_identity,
            frozen_runtime_bundle_digest: runtime_bundle_digest(
                [0x11; SHA256_BYTES],
                metadata_store_identity,
                b"/fixed/metadata",
            ),
            root_id: RootId::from_bytes([0x01; 16]),
            owner: owner(),
            endpoint: endpoint(),
            logical_shard_id: LogicalShardId::from_bytes([0x02; 16]),
            authority: MetadataAuthorityFence {
                logical_shard_id: LogicalShardId::from_bytes([0x02; 16]),
                authority_id: MetadataAuthorityId::from_bytes([0xaa; 16]),
                authority_generation: MetadataAuthorityGeneration::new(5).unwrap(),
            },
            canonical_holt_locator: b"/fixed/metadata".to_vec(),
        }
    }

    fn serving_token(owner_epoch: OwnerEpoch, lease_id: u64) -> OwnerSessionToken {
        serving_token_with_incarnation(owner_epoch, [0x44; 16], lease_id)
    }

    fn serving_token_with_incarnation(
        owner_epoch: OwnerEpoch,
        owner_incarnation_id: [u8; 16],
        lease_id: u64,
    ) -> OwnerSessionToken {
        let placement = placement();
        let authority = authority();
        let lease = LogicalShardLease {
            logical_shard_id: placement.logical_shard_id,
            owner: owner(),
            owner_epoch,
            owner_incarnation_id: OwnerIncarnationId::from_bytes(owner_incarnation_id),
            lease_id,
            authority: authority.fence(),
        };
        let serving = LogicalShardRecord {
            logical_shard_id: placement.logical_shard_id,
            owner: Some(owner()),
            owner_epoch: Some(owner_epoch),
            owner_incarnation_id: Some(lease.owner_incarnation_id),
            lease_id,
            state: LogicalShardState::Serving,
            endpoint: Some(endpoint()),
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        };
        OwnerSessionToken::from_serving(&placement, &serving, &lease, &endpoint(), &authority)
            .unwrap()
    }

    fn genesis_frontier() -> AcknowledgedMetadataFrontier {
        AcknowledgedMetadataFrontier {
            write_sequence: 0,
            commit_version: CommitVersion::new(1).unwrap(),
            recovery_lsn: 0,
            chain_digest: [0x33; SHA256_BYTES],
        }
    }

    fn exact_plan_fixture(preparation: &OwnerSessionPreparation) -> PlannedMetadataCommitV1 {
        let purpose = MetadataCommitPurposeV1::MetadataCommand {
            class: MetadataCommandCommitClassV1::Domain,
            root_id: preparation.root_id,
            request_id: RequestId::from_bytes([0x52; 16]),
            command_digest: CommandDigest::from_bytes([0x53; SHA256_BYTES]),
            lease_deadline_ms: Some(54),
        };
        let prior = MetadataFrontierPointV1::Exact(AcknowledgedMetadataFrontier {
            write_sequence: 10,
            commit_version: CommitVersion::new(11).unwrap(),
            recovery_lsn: 12,
            chain_digest: [0x55; SHA256_BYTES],
        });
        let exact_next = AcknowledgedMetadataFrontier {
            write_sequence: 11,
            commit_version: CommitVersion::new(12).unwrap(),
            recovery_lsn: 13,
            chain_digest: [0x56; SHA256_BYTES],
        };
        planned_fixture(preparation, purpose, prior, exact_next)
    }

    fn serving_plan_fixture(preparation: &OwnerSessionPreparation) -> PlannedMetadataCommitV1 {
        planned_fixture(
            preparation,
            MetadataCommitPurposeV1::MetadataCommand {
                class: MetadataCommandCommitClassV1::Domain,
                root_id: preparation.root_id,
                request_id: RequestId::from_bytes([0x57; 16]),
                command_digest: CommandDigest::from_bytes([0x58; SHA256_BYTES]),
                lease_deadline_ms: None,
            },
            MetadataFrontierPointV1::Exact(genesis_frontier()),
            AcknowledgedMetadataFrontier {
                write_sequence: 1,
                commit_version: CommitVersion::new(2).unwrap(),
                recovery_lsn: 1,
                chain_digest: [0x59; SHA256_BYTES],
            },
        )
    }

    fn all_plan_fixtures(preparation: &OwnerSessionPreparation) -> Vec<PlannedMetadataCommitV1> {
        let prior_frontier = AcknowledgedMetadataFrontier {
            write_sequence: 10,
            commit_version: CommitVersion::new(11).unwrap(),
            recovery_lsn: 12,
            chain_digest: [0x55; SHA256_BYTES],
        };
        let prior = MetadataFrontierPointV1::Exact(prior_frontier);
        let owner_next = AcknowledgedMetadataFrontier {
            write_sequence: 11,
            commit_version: CommitVersion::new(11).unwrap(),
            recovery_lsn: 13,
            chain_digest: [0x56; SHA256_BYTES],
        };
        let command_next = AcknowledgedMetadataFrontier {
            write_sequence: 11,
            commit_version: CommitVersion::new(12).unwrap(),
            recovery_lsn: 13,
            chain_digest: [0x5c; SHA256_BYTES],
        };
        let authority_next = AcknowledgedMetadataFrontier {
            write_sequence: 11,
            commit_version: CommitVersion::new(11).unwrap(),
            recovery_lsn: 12,
            chain_digest: prior_frontier.chain_digest,
        };
        let authority_actions = [
            MetadataAuthorityCommitActionV1::Quiesce {
                migration_id: OperationId::from_bytes([0x61; 16]),
                owner_epoch: OwnerEpoch::new(6).unwrap(),
            },
            MetadataAuthorityCommitActionV1::FenceQuiescedSource {
                migration_id: OperationId::from_bytes([0x62; 16]),
                source_receipt_digest: [0x63; SHA256_BYTES],
            },
            MetadataAuthorityCommitActionV1::ActivateTarget {
                migration_id: OperationId::from_bytes([0x64; 16]),
                activation_token_digest: [0x65; SHA256_BYTES],
            },
            MetadataAuthorityCommitActionV1::FenceTarget {
                migration_id: OperationId::from_bytes([0x66; 16]),
                target_binding_digest: [0x67; SHA256_BYTES],
            },
        ];
        let mut plans = vec![
            planned_fixture(
                preparation,
                MetadataCommitPurposeV1::Genesis {
                    authority_marker_digest: [0x60; SHA256_BYTES],
                },
                MetadataFrontierPointV1::Absent,
                AcknowledgedMetadataFrontier {
                    write_sequence: 0,
                    commit_version: CommitVersion::new(1).unwrap(),
                    recovery_lsn: 0,
                    chain_digest: [0x68; SHA256_BYTES],
                },
            ),
            planned_fixture(
                preparation,
                MetadataCommitPurposeV1::AdvanceOwnerEpoch {
                    expected: Some(OwnerEpoch::new(5).unwrap()),
                    next: OwnerEpoch::new(6).unwrap(),
                },
                prior,
                owner_next,
            ),
            planned_fixture(
                preparation,
                MetadataCommitPurposeV1::AdvanceOwnerEpoch {
                    expected: None,
                    next: OwnerEpoch::new(1).unwrap(),
                },
                prior,
                owner_next,
            ),
            planned_fixture(
                preparation,
                MetadataCommitPurposeV1::ObserveLeaseClock {
                    root_id: preparation.root_id,
                    placement_generation: PlacementGeneration::new(2).unwrap(),
                    owner_epoch: OwnerEpoch::new(6).unwrap(),
                    observed_ms: 99,
                },
                prior,
                owner_next,
            ),
            exact_plan_fixture(preparation),
            planned_fixture(
                preparation,
                MetadataCommitPurposeV1::MetadataCommand {
                    class: MetadataCommandCommitClassV1::RootFence,
                    root_id: preparation.root_id,
                    request_id: RequestId::from_bytes([0x5a; 16]),
                    command_digest: CommandDigest::from_bytes([0x5b; SHA256_BYTES]),
                    lease_deadline_ms: None,
                },
                prior,
                command_next,
            ),
        ];
        plans.extend(
            authority_actions
                .into_iter()
                .enumerate()
                .map(|(index, action)| {
                    planned_fixture(
                        preparation,
                        MetadataCommitPurposeV1::Authority {
                            action,
                            prior_marker_digest: [0x70 + index as u8; SHA256_BYTES],
                            next_marker_digest: [0x78 + index as u8; SHA256_BYTES],
                        },
                        prior,
                        authority_next,
                    )
                }),
        );
        plans
    }

    fn planned_fixture(
        preparation: &OwnerSessionPreparation,
        purpose: MetadataCommitPurposeV1,
        prior: MetadataFrontierPointV1,
        exact_next: AcknowledgedMetadataFrontier,
    ) -> PlannedMetadataCommitV1 {
        let purpose_debug = format!("{purpose:?}");
        let canonical_digest = plan_digest_fixture(
            preparation.metadata_store_identity,
            preparation.frozen_runtime_bundle_digest,
            &purpose,
            prior,
            exact_next,
        );
        PlannedMetadataCommitV1::from_durable_parts_v1(
            preparation.metadata_store_identity,
            preparation.frozen_runtime_bundle_digest,
            purpose,
            prior,
            exact_next,
            canonical_digest,
        )
        .unwrap_or_else(|error| panic!("invalid {purpose_debug} fixture: {error:?}"))
    }

    fn plan_digest_fixture(
        identity: MetadataStoreIdentity,
        frozen_bundle_digest: [u8; SHA256_BYTES],
        purpose: &MetadataCommitPurposeV1,
        prior: MetadataFrontierPointV1,
        exact_next: AcknowledgedMetadataFrontier,
    ) -> [u8; SHA256_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.metadata.commit-plan.v1\0");
        let mut encoded_identity = Vec::new();
        encoded_identity.push(1);
        encoded_identity.extend_from_slice(identity.logical_shard_id.as_bytes());
        encoded_identity.extend_from_slice(identity.authority_id.as_bytes());
        encoded_identity.extend_from_slice(&identity.authority_generation.get().to_be_bytes());
        encoded_identity.extend_from_slice(identity.consistency_domain_id.as_bytes());
        encoded_identity.extend_from_slice(&identity.profile_fingerprint);
        encoded_identity.extend_from_slice(identity.contract_digest.as_bytes());
        hasher.update((encoded_identity.len() as u64).to_be_bytes());
        hasher.update(encoded_identity);
        hasher.update(frozen_bundle_digest);
        match purpose {
            MetadataCommitPurposeV1::Genesis {
                authority_marker_digest,
            } => {
                hasher.update([1]);
                hasher.update(authority_marker_digest);
            }
            MetadataCommitPurposeV1::AdvanceOwnerEpoch { expected, next } => {
                hasher.update([2]);
                match expected {
                    Some(expected) => {
                        hasher.update([1]);
                        hasher.update(expected.get().to_be_bytes());
                    }
                    None => hasher.update([0]),
                }
                hasher.update(next.get().to_be_bytes());
            }
            MetadataCommitPurposeV1::ObserveLeaseClock {
                root_id,
                placement_generation,
                owner_epoch,
                observed_ms,
            } => {
                hasher.update([3]);
                hasher.update(root_id.as_bytes());
                hasher.update(placement_generation.get().to_be_bytes());
                hasher.update(owner_epoch.get().to_be_bytes());
                hasher.update(observed_ms.to_be_bytes());
            }
            MetadataCommitPurposeV1::MetadataCommand {
                class,
                root_id,
                request_id,
                command_digest,
                lease_deadline_ms,
            } => {
                hasher.update([4]);
                hasher.update([match class {
                    MetadataCommandCommitClassV1::Domain => 1,
                    MetadataCommandCommitClassV1::RootFence => 2,
                }]);
                hasher.update(root_id.as_bytes());
                hasher.update(request_id.as_bytes());
                hasher.update(command_digest.as_bytes());
                match lease_deadline_ms {
                    Some(deadline) => {
                        hasher.update([1]);
                        hasher.update(deadline.to_be_bytes());
                    }
                    None => hasher.update([0]),
                }
            }
            MetadataCommitPurposeV1::Authority {
                action,
                prior_marker_digest,
                next_marker_digest,
            } => {
                hasher.update([5]);
                hash_authority_action_fixture(&mut hasher, *action);
                hasher.update(prior_marker_digest);
                hasher.update(next_marker_digest);
            }
        }
        hash_frontier_point_fixture(&mut hasher, prior);
        hash_frontier_fixture(&mut hasher, exact_next);
        hasher.finalize().into()
    }

    fn hash_authority_action_fixture(hasher: &mut Sha256, action: MetadataAuthorityCommitActionV1) {
        match action {
            MetadataAuthorityCommitActionV1::Quiesce {
                migration_id,
                owner_epoch,
            } => {
                hasher.update([1]);
                hasher.update(migration_id.as_bytes());
                hasher.update(owner_epoch.get().to_be_bytes());
            }
            MetadataAuthorityCommitActionV1::FenceQuiescedSource {
                migration_id,
                source_receipt_digest,
            } => {
                hasher.update([2]);
                hasher.update(migration_id.as_bytes());
                hasher.update(source_receipt_digest);
            }
            MetadataAuthorityCommitActionV1::ActivateTarget {
                migration_id,
                activation_token_digest,
            } => {
                hasher.update([3]);
                hasher.update(migration_id.as_bytes());
                hasher.update(activation_token_digest);
            }
            MetadataAuthorityCommitActionV1::FenceTarget {
                migration_id,
                target_binding_digest,
            } => {
                hasher.update([4]);
                hasher.update(migration_id.as_bytes());
                hasher.update(target_binding_digest);
            }
        }
    }

    fn hash_frontier_point_fixture(hasher: &mut Sha256, point: MetadataFrontierPointV1) {
        match point {
            MetadataFrontierPointV1::Absent => hasher.update([0]),
            MetadataFrontierPointV1::Exact(frontier) => {
                hasher.update([1]);
                hash_frontier_fixture(hasher, frontier);
            }
        }
    }

    fn hash_frontier_fixture(hasher: &mut Sha256, frontier: AcknowledgedMetadataFrontier) {
        hasher.update(frontier.write_sequence.to_be_bytes());
        hasher.update(frontier.commit_version.get().to_be_bytes());
        hasher.update(frontier.recovery_lsn.to_be_bytes());
        hasher.update(frontier.chain_digest);
    }

    #[test]
    fn codec_has_one_frozen_canonical_prepared_encoding() {
        let preparation = fixed_preparation();
        let encoded = encode_wire(&prepared_wire(&preparation)).unwrap();
        assert_eq!(
            format!("{preparation:?}"),
            "OwnerSessionPreparation { binding: \"<redacted>\" }"
        );
        assert_eq!(decode_wire(&encoded).unwrap(), prepared_wire(&preparation));
        assert_eq!(encoded.len(), 1003);
        assert_eq!(
            encode_hex(&Sha256::digest(&encoded)),
            "0990b94c2fbd00f8531402a840111f9e9694c0606650f20560ca3062a7189066"
        );
        assert!(encoded.len() < MAX_JOURNAL_BYTES as usize);
        let legacy = String::from_utf8(encoded.clone()).unwrap().replacen(
            r#""version":4"#,
            r#""version":3"#,
            1,
        );
        assert!(decode_wire(legacy.as_bytes()).is_err());
        let mut trailing = encoded;
        trailing.push(b'\n');
        assert!(decode_wire(&trailing).is_err());
    }

    #[test]
    fn exact_receipt_wire_roundtrips_full_plan_and_keeps_unsettled_sticky() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let planned = exact_plan_fixture(&preparation);
        let mut maximum_wire_bytes = 0;

        for plan in all_plan_fixtures(&preparation) {
            for (receipt, expected) in [
                (
                    CommitReceiptWire::Pending {
                        planned: encode_planned_commit(&plan),
                    },
                    MetadataCommitReceiptStateV1::Pending(plan.clone()),
                ),
                (
                    CommitReceiptWire::PoisonedSettled {
                        planned: encode_planned_commit(&plan),
                    },
                    MetadataCommitReceiptStateV1::PoisonedSettled(plan.clone()),
                ),
                (
                    CommitReceiptWire::PoisonedUnsettled {
                        planned: encode_planned_commit(&plan),
                    },
                    MetadataCommitReceiptStateV1::PoisonedUnsettled(plan.clone()),
                ),
            ] {
                let mut wire = prepared_wire(&preparation);
                wire.commit_receipt = receipt;
                let encoded = encode_wire(&wire).unwrap();
                maximum_wire_bytes = maximum_wire_bytes.max(encoded.len());
                let decoded = decode_wire(&encoded).unwrap();
                assert_eq!(decode_commit_receipt(&decoded).unwrap(), expected);
            }
        }
        assert!(maximum_wire_bytes < 4 * 1024);
        assert!(maximum_wire_bytes < MAX_JOURNAL_BYTES as usize);

        let fixed = fixed_preparation();
        let maximum_pending = all_plan_fixtures(&fixed)
            .into_iter()
            .map(|plan| {
                let mut wire = prepared_wire(&fixed);
                wire.commit_receipt = CommitReceiptWire::Pending {
                    planned: encode_planned_commit(&plan),
                };
                encode_wire(&wire).unwrap()
            })
            .max_by_key(Vec::len)
            .unwrap();
        assert_eq!(maximum_pending.len(), 2269);
        assert_eq!(
            encode_hex(&Sha256::digest(&maximum_pending)),
            "10ce95e8adc1f24f81cefe204c546cd869f79c647fd4ba86548e3352b88d7798"
        );

        let pending = MetadataCommitReceiptStateV1::Pending(planned.clone());
        let settled = MetadataCommitReceiptStateV1::PoisonedSettled(planned.clone());
        let unsettled = MetadataCommitReceiptStateV1::PoisonedUnsettled(planned.clone());
        let prior = planned.prior();
        let next = planned.exact_next();
        for (durable, source) in [
            (&pending, MetadataCommitReceiptDirtySourceV1::Pending),
            (
                &settled,
                MetadataCommitReceiptDirtySourceV1::PoisonedSettled,
            ),
            (
                &unsettled,
                MetadataCommitReceiptDirtySourceV1::PoisonedUnsettled,
            ),
        ] {
            assert_eq!(
                resolve_frontier_transition(
                    durable,
                    &planned,
                    source,
                    MetadataCommitResolutionBasisV1::ExactNextApplied,
                    Some(next),
                    None,
                    [0x71; SHA256_BYTES],
                ),
                Some(MetadataFrontierPointV1::Exact(next))
            );
        }
        assert_eq!(
            resolve_frontier_transition(
                &pending,
                &planned,
                MetadataCommitReceiptDirtySourceV1::Pending,
                MetadataCommitResolutionBasisV1::ExactPriorNotAppliedSettled,
                None,
                Some(prior),
                [0x72; SHA256_BYTES],
            ),
            None
        );
        assert_eq!(
            resolve_frontier_transition(
                &settled,
                &planned,
                MetadataCommitReceiptDirtySourceV1::PoisonedSettled,
                MetadataCommitResolutionBasisV1::ExactPriorNotAppliedSettled,
                None,
                Some(prior),
                [0x72; SHA256_BYTES],
            ),
            Some(prior)
        );
        assert_eq!(
            resolve_frontier_transition(
                &unsettled,
                &planned,
                MetadataCommitReceiptDirtySourceV1::PoisonedUnsettled,
                MetadataCommitResolutionBasisV1::ExactPriorNotAppliedSettled,
                None,
                Some(prior),
                [0x72; SHA256_BYTES],
            ),
            None
        );
        assert!(matches!(
            poison_receipt_transition(
                &unsettled,
                &planned,
                MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome,
            ),
            Some(PoisonReceiptTransition::ExactNoChange)
        ));
        assert!(poison_receipt_transition(
            &unsettled,
            &planned,
            MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome,
        )
        .is_none());

        let mut tampered = prepared_wire(&preparation);
        let mut tampered_plan = encode_planned_commit(&planned);
        let replacement = if tampered_plan.canonical_digest.starts_with('0') {
            "1"
        } else {
            "0"
        };
        tampered_plan
            .canonical_digest
            .replace_range(0..1, replacement);
        tampered.commit_receipt = CommitReceiptWire::Pending {
            planned: tampered_plan,
        };
        assert!(decode_wire(&encode_wire(&tampered).unwrap()).is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn public_engine_prepared_create_is_nq_before_receipt_or_provider_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        let bundle = JournalRuntimeBundle::new(
            &metadata_path,
            journal.clone(),
            ReceiptResponseLoss::None,
            ProviderCommitFault::None,
        );

        assert!(matches!(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                bundle.clone(),
                preparation.metadata_store_identity,
                CreateRecoveryIntentV1::ReconcilePrepared,
                MetadataStoreCreateModeV1::Active,
            ),
            Err(nokv_meta::workspace::AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
        assert_eq!(bundle.persist_calls.load(Ordering::Relaxed), 0);
        assert_eq!(bundle.resolve_calls.load(Ordering::Relaxed), 0);
        assert_eq!(bundle.poison_calls.load(Ordering::Relaxed), 0);
        assert!(matches!(
            journal
                .load_commit_receipt_v1(preparation.metadata_store_identity)
                .unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                frontier: MetadataFrontierPointV1::Absent,
                ..
            }
        ));
        assert!(metadata_path.join("store.lock").is_file());
        assert_eq!(fs::read_dir(&metadata_path).unwrap().count(), 1);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn prepared_create_does_not_reach_persist_response_loss() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        let before_inode = fs::symlink_metadata(&journal_path).unwrap().ino();
        let bundle = JournalRuntimeBundle::new(
            &metadata_path,
            journal.clone(),
            ReceiptResponseLoss::AfterPersist,
            ProviderCommitFault::None,
        );

        assert!(matches!(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                bundle.clone(),
                preparation.metadata_store_identity,
                CreateRecoveryIntentV1::ReconcilePrepared,
                MetadataStoreCreateModeV1::Active,
            ),
            Err(nokv_meta::workspace::AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
        assert_eq!(bundle.persist_calls.load(Ordering::Relaxed), 0);
        assert_eq!(bundle.resolve_calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            fs::symlink_metadata(&journal_path).unwrap().ino(),
            before_inode
        );
        assert!(matches!(
            journal
                .load_commit_receipt_v1(preparation.metadata_store_identity)
                .unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                frontier: MetadataFrontierPointV1::Absent,
                ..
            }
        ));
        drop(bundle);
        drop(journal);

        let (restarted, disposition) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        assert_eq!(disposition, PreparedCreateDisposition::Replayed);
        let clean = restarted
            .load_commit_receipt_v1(preparation.metadata_store_identity)
            .unwrap();
        assert!(matches!(
            clean,
            MetadataCommitReceiptStateV1::Clean {
                frontier: MetadataFrontierPointV1::Absent,
                ..
            }
        ));
        let retry_bundle = JournalRuntimeBundle::new(
            &metadata_path,
            restarted.clone(),
            ReceiptResponseLoss::None,
            ProviderCommitFault::None,
        );
        assert!(AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            retry_bundle,
            preparation.metadata_store_identity,
            CreateRecoveryIntentV1::ReconcilePrepared,
            MetadataStoreCreateModeV1::Active,
        )
        .is_err());
        assert_eq!(
            restarted
                .load_commit_receipt_v1(preparation.metadata_store_identity)
                .unwrap(),
            clean
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn ordinary_prepared_create_never_reaches_post_rename_receipt_failure() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        let bundle = JournalRuntimeBundle::new(
            &metadata_path,
            journal.clone(),
            ReceiptResponseLoss::None,
            ProviderCommitFault::None,
        );
        REPLACE_AFTER_RENAME_TEST_FAILURE.with(|failure| failure.set(true));

        let result = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            bundle.clone(),
            preparation.metadata_store_identity,
            CreateRecoveryIntentV1::ReconcilePrepared,
            MetadataStoreCreateModeV1::Active,
        );
        REPLACE_AFTER_RENAME_TEST_FAILURE.with(|failure| failure.set(false));
        assert!(matches!(
            result,
            Err(nokv_meta::workspace::AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
        assert_eq!(bundle.persist_calls.load(Ordering::Relaxed), 0);
        assert_eq!(bundle.resolve_calls.load(Ordering::Relaxed), 0);
        assert_eq!(bundle.poison_calls.load(Ordering::Relaxed), 0);
        let clean = journal
            .load_commit_receipt_v1(preparation.metadata_store_identity)
            .unwrap();
        assert!(matches!(
            &clean,
            MetadataCommitReceiptStateV1::Clean {
                frontier: MetadataFrontierPointV1::Absent,
                ..
            }
        ));
        drop(bundle);
        drop(journal);

        let (restarted, disposition) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        assert_eq!(disposition, PreparedCreateDisposition::Replayed);
        let replayed = restarted
            .load_commit_receipt_v1(preparation.metadata_store_identity)
            .unwrap();
        assert_eq!(replayed, clean);
        let retry_bundle = JournalRuntimeBundle::new(
            &metadata_path,
            restarted.clone(),
            ReceiptResponseLoss::None,
            ProviderCommitFault::None,
        );
        assert!(matches!(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                retry_bundle,
                preparation.metadata_store_identity,
                CreateRecoveryIntentV1::ReconcilePrepared,
                MetadataStoreCreateModeV1::Active,
            ),
            Err(nokv_meta::workspace::AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
        assert_eq!(
            restarted
                .load_commit_receipt_v1(preparation.metadata_store_identity)
                .unwrap(),
            replayed
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn ordinary_prepared_create_does_not_reach_resolve_response_loss() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        let before_inode = fs::symlink_metadata(&journal_path).unwrap().ino();
        let bundle = JournalRuntimeBundle::new(
            &metadata_path,
            journal.clone(),
            ReceiptResponseLoss::AfterResolve,
            ProviderCommitFault::None,
        );

        assert!(matches!(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                bundle.clone(),
                preparation.metadata_store_identity,
                CreateRecoveryIntentV1::ReconcilePrepared,
                MetadataStoreCreateModeV1::Active,
            ),
            Err(nokv_meta::workspace::AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
        assert_eq!(bundle.persist_calls.load(Ordering::Relaxed), 0);
        assert_eq!(bundle.resolve_calls.load(Ordering::Relaxed), 0);
        assert_eq!(bundle.poison_calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            fs::symlink_metadata(&journal_path).unwrap().ino(),
            before_inode
        );
        assert!(matches!(
            journal
                .load_commit_receipt_v1(preparation.metadata_store_identity)
                .unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                frontier: MetadataFrontierPointV1::Absent,
                ..
            }
        ));
        drop(bundle);
        drop(journal);

        let (restarted, disposition) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        assert_eq!(disposition, PreparedCreateDisposition::Replayed);
        let retry_bundle = JournalRuntimeBundle::new(
            &metadata_path,
            restarted,
            ReceiptResponseLoss::None,
            ProviderCommitFault::None,
        );
        assert!(matches!(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                retry_bundle.clone(),
                preparation.metadata_store_identity,
                CreateRecoveryIntentV1::ReconcilePrepared,
                MetadataStoreCreateModeV1::Active,
            ),
            Err(nokv_meta::workspace::AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
        assert_eq!(retry_bundle.persist_calls.load(Ordering::Relaxed), 0);
        assert_eq!(retry_bundle.resolve_calls.load(Ordering::Relaxed), 0);
        assert_eq!(retry_bundle.poison_calls.load(Ordering::Relaxed), 0);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn ordinary_prepared_create_does_not_reach_poison_response_loss() {
        for commit_fault in [
            ProviderCommitFault::UnknownSettled,
            ProviderCommitFault::UnknownUnsettled,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let metadata_path = directory.path().join("metadata");
            let journal_path = directory.path().join("owner-session.json");
            let preparation = preparation(&metadata_path, &journal_path);
            let (journal, _) =
                OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
            let bundle = JournalRuntimeBundle::new(
                &metadata_path,
                journal.clone(),
                ReceiptResponseLoss::AfterPoison,
                commit_fault,
            );

            assert!(matches!(
                AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                    bundle.clone(),
                    preparation.metadata_store_identity,
                    CreateRecoveryIntentV1::ReconcilePrepared,
                    MetadataStoreCreateModeV1::Active,
                ),
                Err(nokv_meta::workspace::AgentMetadataError::ProviderAuthorityMismatch { .. })
            ));
            assert_eq!(bundle.persist_calls.load(Ordering::Relaxed), 0);
            assert_eq!(bundle.resolve_calls.load(Ordering::Relaxed), 0);
            assert_eq!(bundle.poison_calls.load(Ordering::Relaxed), 0);
            let durable = journal
                .load_commit_receipt_v1(preparation.metadata_store_identity)
                .unwrap();
            assert!(matches!(
                &durable,
                MetadataCommitReceiptStateV1::Clean {
                    frontier: MetadataFrontierPointV1::Absent,
                    ..
                }
            ));
            drop(bundle);
            drop(journal);

            let (restarted, disposition) =
                OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
            assert_eq!(disposition, PreparedCreateDisposition::Replayed);
            let restarted_state = restarted
                .load_commit_receipt_v1(preparation.metadata_store_identity)
                .unwrap();
            assert_eq!(restarted_state, durable);
        }
    }

    #[cfg(unix)]
    #[test]
    fn prepared_create_replays_every_recovery_receipt_state_without_downgrade() {
        for receipt_kind in [1, 2, 3] {
            let directory = tempfile::tempdir().unwrap();
            let metadata_path = directory.path().join("metadata");
            let journal_path = directory.path().join("owner-session.json");
            let preparation = preparation(&metadata_path, &journal_path);
            let planned = exact_plan_fixture(&preparation);
            let (journal, _) =
                OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
            journal
                .update(|wire| {
                    wire.commit_receipt = match receipt_kind {
                        1 => CommitReceiptWire::Pending {
                            planned: encode_planned_commit(&planned),
                        },
                        2 => CommitReceiptWire::PoisonedSettled {
                            planned: encode_planned_commit(&planned),
                        },
                        3 => CommitReceiptWire::PoisonedUnsettled {
                            planned: encode_planned_commit(&planned),
                        },
                        _ => unreachable!(),
                    };
                    Ok(())
                })
                .unwrap();
            drop(journal);

            let (reopened, disposition) =
                OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
            assert_eq!(disposition, PreparedCreateDisposition::Replayed);
            let state = MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                reopened.as_ref(),
                preparation.metadata_store_identity,
            )
            .unwrap();
            match receipt_kind {
                1 => assert_eq!(state, MetadataCommitReceiptStateV1::Pending(planned)),
                2 => assert_eq!(
                    state,
                    MetadataCommitReceiptStateV1::PoisonedSettled(planned)
                ),
                3 => assert_eq!(
                    state,
                    MetadataCommitReceiptStateV1::PoisonedUnsettled(planned)
                ),
                _ => unreachable!(),
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn prepared_receipt_precedes_store_and_exact_replay_is_no_change() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, disposition) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        assert_eq!(disposition, PreparedCreateDisposition::Created);
        assert!(journal.is_store_prepared().unwrap());
        assert_eq!(journal.preparation().unwrap(), preparation);
        assert_eq!(
            MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                journal.as_ref(),
                preparation.metadata_store_identity,
            )
            .unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                store_identity: preparation.metadata_store_identity,
                frozen_bundle_digest: preparation.frozen_runtime_bundle_digest,
                frontier: MetadataFrontierPointV1::Absent,
            }
        );
        assert_eq!(
            fs::metadata(metadata_path.join("store.lock"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        let before = fs::read(&journal_path).unwrap();
        let before_identity = fs::symlink_metadata(&journal_path).unwrap().ino();
        let (reopened, replayed) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        assert_eq!(replayed, PreparedCreateDisposition::Replayed);
        assert_eq!(fs::read(&journal_path).unwrap(), before);
        assert_eq!(
            fs::symlink_metadata(&journal_path).unwrap().ino(),
            before_identity
        );
        assert_eq!(
            MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                reopened.as_ref(),
                preparation.metadata_store_identity,
            )
            .unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                store_identity: preparation.metadata_store_identity,
                frozen_bundle_digest: preparation.frozen_runtime_bundle_digest,
                frontier: MetadataFrontierPointV1::Absent,
            }
        );

        journal
            .seed_clean_exact_fixture(genesis_frontier())
            .unwrap();
        let acknowledged = fs::read(&journal_path).unwrap();
        let acknowledged_identity = fs::symlink_metadata(&journal_path).unwrap().ino();
        journal
            .seed_clean_exact_fixture(genesis_frontier())
            .unwrap();
        assert_eq!(fs::read(&journal_path).unwrap(), acknowledged);
        assert_eq!(
            fs::symlink_metadata(&journal_path).unwrap().ino(),
            acknowledged_identity
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn initial_publish_is_no_replace_single_link_and_skips_orphan_temp_names() {
        let directory = tempfile::tempdir().unwrap();
        let journal_path = directory.path().join("owner-session.json");
        let (_, _, parent_directory, file_name) = canonical_file_path(&journal_path).unwrap();
        let first = br#"{"first":true}"#;
        let second = br#"{"second":true}"#;

        assert!(create_initial_file(&parent_directory, &file_name, first)
            .unwrap()
            .is_some());
        assert!(create_initial_file(&parent_directory, &file_name, second)
            .unwrap()
            .is_none());
        assert_eq!(fs::read(&journal_path).unwrap(), first);
        assert_eq!(fs::symlink_metadata(&journal_path).unwrap().nlink(), 1);

        let local_sequence = AtomicU64::new(41);
        let orphan = owner_session_temp_name(41);
        let orphan_file = openat_create(&parent_directory, &orphan, "test orphan create").unwrap();
        drop(orphan_file);
        let (fresh_file, fresh_name) = create_unique_temp_file(
            &parent_directory,
            "test unique temporary create",
            &local_sequence,
        )
        .unwrap();
        drop(fresh_file);
        assert_eq!(fresh_name, owner_session_temp_name(42));
        unlinkat_file(&parent_directory, &orphan).unwrap();
        unlinkat_file(&parent_directory, &fresh_name).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn stable_read_rejects_same_inode_same_length_content_change() {
        let directory = tempfile::tempdir().unwrap();
        let journal_path = directory.path().join("owner-session.json");
        let (_, _, parent_directory, file_name) = canonical_file_path(&journal_path).unwrap();
        let original = br#"{"value":"aaaa"}"#;
        let replacement = br#"{"value":"bbbb"}"#;
        assert_eq!(original.len(), replacement.len());
        create_initial_file(&parent_directory, &file_name, original)
            .unwrap()
            .expect("initial journal must be installed");
        let inode = fs::symlink_metadata(&journal_path).unwrap().ino();
        let tamper_path = journal_path.clone();
        STABLE_READ_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::write(tamper_path, replacement).unwrap();
            }));
        });

        assert_eq!(
            read_stable_file_at(&parent_directory, &file_name),
            Err(OwnerSessionJournalError::Changed)
        );
        assert_eq!(fs::symlink_metadata(&journal_path).unwrap().ino(), inode);
        assert_eq!(fs::read(&journal_path).unwrap(), replacement);
    }

    #[cfg(unix)]
    #[test]
    fn removal_rejects_a_valid_non_releasing_journal_without_unlinking() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();

        assert!(matches!(
            journal.remove_if_exact(),
            Err(OwnerSessionJournalError::InvalidJournal(
                "journal removal requires a releasing receipt"
            ))
        ));
        assert!(journal_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn local_uncertainty_is_closed_and_requires_a_new_allocation() {
        assert_eq!(
            persist_outcome_after_local_uncertainty(),
            MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired
        );
        assert_eq!(
            mutation_outcome_after_local_uncertainty(),
            MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown
        );
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = journal.state.lock().unwrap();
            panic!("poison cached journal state after durable access");
        }));
        assert_eq!(
            MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                journal.as_ref(),
                preparation.metadata_store_identity,
            ),
            Err(MetadataCommitReceiptErrorV1::Poisoned)
        );
        drop(journal);

        let (reopened, disposition) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        assert_eq!(disposition, PreparedCreateDisposition::Replayed);
        assert!(matches!(
            MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                reopened.as_ref(),
                preparation.metadata_store_identity,
            ),
            Ok(MetadataCommitReceiptStateV1::Clean {
                frontier: MetadataFrontierPointV1::Absent,
                ..
            })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn serving_completion_and_resume_are_exact_and_no_change() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        journal
            .seed_clean_exact_fixture(genesis_frontier())
            .unwrap();
        let token = serving_token(OwnerEpoch::new(3).unwrap(), 44);
        journal.complete_serving(&token).unwrap();
        let before = fs::read(&journal_path).unwrap();
        let before_identity = fs::symlink_metadata(&journal_path).unwrap().ino();
        journal.complete_serving(&token).unwrap();
        assert_eq!(fs::read(&journal_path).unwrap(), before);
        assert_eq!(
            fs::symlink_metadata(&journal_path).unwrap().ino(),
            before_identity
        );
        for foreign in [
            serving_token(OwnerEpoch::new(4).unwrap(), 44),
            serving_token_with_incarnation(OwnerEpoch::new(3).unwrap(), [0x45; 16], 44),
            serving_token(OwnerEpoch::new(3).unwrap(), 45),
        ] {
            assert!(matches!(
                journal.complete_serving(&foreign),
                Err(OwnerSessionJournalError::BindingMismatch(
                    "serving owner token"
                ))
            ));
            assert_eq!(fs::read(&journal_path).unwrap(), before);
            assert_eq!(
                fs::symlink_metadata(&journal_path).unwrap().ino(),
                before_identity
            );
        }

        let (loaded, resumed) =
            OwnerSessionJournal::load_resume(&journal_path, &metadata_path).unwrap();
        assert_eq!(loaded, token);
        HoltRuntimeGuard::validate_runtime(resumed.as_ref()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn serving_restart_preserves_pending_and_poisoned_recovery_authority() {
        for receipt_kind in [1, 2, 3] {
            let directory = tempfile::tempdir().unwrap();
            let metadata_path = directory.path().join("metadata");
            let journal_path = directory.path().join("owner-session.json");
            let preparation = preparation(&metadata_path, &journal_path);
            let planned = serving_plan_fixture(&preparation);
            let (journal, _) =
                OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
            journal
                .seed_clean_exact_fixture(genesis_frontier())
                .unwrap();
            let token = serving_token(OwnerEpoch::new(3).unwrap(), 44);
            journal.complete_serving(&token).unwrap();
            journal
                .update(|wire| {
                    wire.commit_receipt = match receipt_kind {
                        1 => CommitReceiptWire::Pending {
                            planned: encode_planned_commit(&planned),
                        },
                        2 => CommitReceiptWire::PoisonedSettled {
                            planned: encode_planned_commit(&planned),
                        },
                        3 => CommitReceiptWire::PoisonedUnsettled {
                            planned: encode_planned_commit(&planned),
                        },
                        _ => unreachable!(),
                    };
                    Ok(())
                })
                .unwrap();
            drop(journal);

            let (loaded_token, reopened) =
                OwnerSessionJournal::load_resume(&journal_path, &metadata_path).unwrap();
            assert_eq!(loaded_token, token);
            let state = MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                reopened.as_ref(),
                preparation.metadata_store_identity,
            )
            .unwrap();
            match receipt_kind {
                1 => assert_eq!(state, MetadataCommitReceiptStateV1::Pending(planned)),
                2 => assert_eq!(
                    state,
                    MetadataCommitReceiptStateV1::PoisonedSettled(planned)
                ),
                3 => assert_eq!(
                    state,
                    MetadataCommitReceiptStateV1::PoisonedUnsettled(planned)
                ),
                _ => unreachable!(),
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn releasing_restart_is_release_only_and_removes_after_terminal_reconcile() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        journal
            .seed_clean_exact_fixture(genesis_frontier())
            .unwrap();
        let token = serving_token(OwnerEpoch::new(3).unwrap(), 44);
        journal.complete_serving(&token).unwrap();

        journal.begin_releasing(token.lease()).unwrap();
        let mut foreign_incarnation = decode_wire(&fs::read(&journal_path).unwrap()).unwrap();
        foreign_incarnation.release_owner_incarnation_id = Some(encode_hex(
            OwnerIncarnationId::from_bytes([0x45; 16]).as_bytes(),
        ));
        assert!(decode_wire(&encode_wire(&foreign_incarnation).unwrap()).is_err());
        assert!(OwnerSessionJournal::load_resume(&journal_path, &metadata_path).is_err());
        let (release, restarted) =
            OwnerSessionJournal::load_releasing(&journal_path, &preparation.release_preparation())
                .unwrap()
                .expect("Releasing journal must restart as release-only");
        assert_eq!(release, *token.lease());
        assert!(HoltRuntimeGuard::validate_runtime(restarted.as_ref()).is_err());
        assert_eq!(
            MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                restarted.as_ref(),
                preparation.metadata_store_identity,
            ),
            Err(MetadataCommitReceiptErrorV1::Poisoned)
        );

        restarted.remove_if_exact().unwrap();
        assert!(!journal_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn releasing_restart_rejects_foreign_root_owner_endpoint_and_locator_bindings() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let foreign_metadata_path = directory.path().join("foreign-metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        let token = serving_token(OwnerEpoch::new(3).unwrap(), 44);
        journal.begin_releasing(token.lease()).unwrap();

        let foreign = [
            OwnerReleasePreparation::new(
                RootId::from_bytes([0xfe; 16]),
                owner(),
                endpoint(),
                &metadata_path,
                &journal_path,
            )
            .unwrap(),
            OwnerReleasePreparation::new(
                placement().root_id,
                NodeId::new("owner-b").unwrap(),
                endpoint(),
                &metadata_path,
                &journal_path,
            )
            .unwrap(),
            OwnerReleasePreparation::new(
                placement().root_id,
                owner(),
                "metadata-b.internal:7750".to_owned(),
                &metadata_path,
                &journal_path,
            )
            .unwrap(),
            OwnerReleasePreparation::new(
                placement().root_id,
                owner(),
                endpoint(),
                &foreign_metadata_path,
                &journal_path,
            )
            .unwrap(),
        ];

        for binding in foreign {
            assert!(matches!(
                OwnerSessionJournal::load_releasing(&journal_path, &binding),
                Err(OwnerSessionJournalError::BindingMismatch(_))
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn poisoned_or_prepared_journal_can_still_durably_enter_releasing() {
        for serving in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let metadata_path = directory.path().join("metadata");
            let journal_path = directory.path().join("owner-session.json");
            let preparation = preparation(&metadata_path, &journal_path);
            let (journal, _) =
                OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
            let token = serving_token(OwnerEpoch::new(5).unwrap(), 77);
            if serving {
                journal
                    .seed_clean_exact_fixture(genesis_frontier())
                    .unwrap();
                journal.complete_serving(&token).unwrap();
                journal.fail_closed();
            }

            journal.begin_releasing(token.lease()).unwrap();
            let (release, _) = OwnerSessionJournal::load_releasing(
                &journal_path,
                &preparation.release_preparation(),
            )
            .unwrap()
            .expect("release receipt must survive process restart");
            assert_eq!(release, *token.lease());
        }
    }

    #[cfg(unix)]
    #[test]
    fn clean_receipt_is_exact_and_store_bound() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        assert_eq!(
            MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                journal.as_ref(),
                preparation.metadata_store_identity,
            )
            .unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                store_identity: preparation.metadata_store_identity,
                frozen_bundle_digest: preparation.frozen_runtime_bundle_digest,
                frontier: MetadataFrontierPointV1::Absent,
            }
        );
        assert!(journal
            .complete_serving(&serving_token(OwnerEpoch::new(3).unwrap(), 44))
            .is_err());

        let exact = AcknowledgedMetadataFrontier {
            write_sequence: 2,
            commit_version: CommitVersion::new(3).unwrap(),
            recovery_lsn: 1,
            chain_digest: [0x44; SHA256_BYTES],
        };
        journal.seed_clean_exact_fixture(exact).unwrap();
        assert_eq!(
            MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                journal.as_ref(),
                preparation.metadata_store_identity,
            )
            .unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                store_identity: preparation.metadata_store_identity,
                frozen_bundle_digest: preparation.frozen_runtime_bundle_digest,
                frontier: MetadataFrontierPointV1::Exact(exact),
            }
        );
        let mut foreign_identity = preparation.metadata_store_identity;
        foreign_identity.profile_fingerprint[0] ^= 1;
        assert_eq!(
            MetadataCommitReceiptStoreV1::load_commit_receipt_v1(
                journal.as_ref(),
                foreign_identity,
            ),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        );
    }

    #[cfg(unix)]
    #[test]
    fn journal_inside_metadata_directory_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        fs::create_dir(&metadata_path).unwrap();
        let journal_path = metadata_path.join("owner-session.json");
        assert!(matches!(
            OwnerSessionPreparation::new(
                &placement(),
                &authority(),
                owner(),
                endpoint(),
                &metadata_path,
                &journal_path,
            ),
            Err(OwnerSessionJournalError::InvalidJournal(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn directory_replacement_and_clone_locator_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata-a");
        let journal_path = directory.path().join("owner-session-a.json");
        let preparation = preparation(&metadata_path, &journal_path);
        let (journal, _) =
            OwnerSessionJournal::prepare_create(&journal_path, &preparation).unwrap();
        journal
            .seed_clean_exact_fixture(genesis_frontier())
            .unwrap();
        let token = serving_token(OwnerEpoch::new(1).unwrap(), 9);
        journal.complete_serving(&token).unwrap();

        let clone_path = directory.path().join("metadata-b");
        fs::create_dir(&clone_path).unwrap();
        fs::copy(
            metadata_path.join("store.lock"),
            clone_path.join("store.lock"),
        )
        .unwrap();
        let clone_journal = directory.path().join("owner-session-b.json");
        fs::copy(&journal_path, &clone_journal).unwrap();
        assert!(matches!(
            OwnerSessionJournal::load_resume(&clone_journal, &clone_path),
            Err(OwnerSessionJournalError::BindingMismatch(
                "canonical Holt locator"
            ))
        ));

        let original = directory.path().join("metadata-original");
        fs::rename(&metadata_path, &original).unwrap();
        fs::create_dir(&metadata_path).unwrap();
        fs::copy(
            original.join("store.lock"),
            metadata_path.join("store.lock"),
        )
        .unwrap();
        assert!(HoltRuntimeGuard::validate_runtime(journal.as_ref()).is_err());
    }

    #[test]
    fn prepared_control_reconciliation_distinguishes_first_resume_and_successor() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("metadata");
        let journal_path = directory.path().join("owner-session.json");
        let preparation = preparation(&metadata_path, &journal_path);

        let mut shard = LogicalShardRecord {
            logical_shard_id: placement().logical_shard_id,
            owner: None,
            owner_epoch: None,
            owner_incarnation_id: None,
            lease_id: 0,
            state: LogicalShardState::Unassigned,
            endpoint: None,
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        };
        assert_eq!(
            preparation
                .reconcile_control_owner(&shard, &authority())
                .unwrap(),
            PreparedControlOwner::First
        );
        shard.owner_epoch = Some(OwnerEpoch::new(2).unwrap());
        shard.owner_incarnation_id = Some(OwnerIncarnationId::from_bytes([0x22; 16]));
        assert_eq!(
            preparation
                .reconcile_control_owner(&shard, &authority())
                .unwrap(),
            PreparedControlOwner::Successor(OwnerEpoch::new(2).unwrap())
        );
        shard.owner = Some(owner());
        shard.endpoint = Some(endpoint());
        shard.lease_id = 7;
        shard.state = LogicalShardState::Recovering;
        assert!(matches!(
            preparation
                .reconcile_control_owner(&shard, &authority())
                .unwrap(),
            PreparedControlOwner::ResumeOrSuccessor(LogicalShardLease { lease_id: 7, .. })
        ));
    }
}
