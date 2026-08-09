/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Private v4 store-owner journal state and wire characterization.
//!
//! The Unix submodule characterizes the held parent and permanent companion
//! lock that every later head mutation must borrow. Head publication,
//! bootstrap, provider, and control-store wiring are still absent, so this
//! module does not claim that the owner no-quorum path is qualified. The
//! provider-neutral exact-commit purpose set still belongs in `nokv-meta`;
//! until that type exists, only canonical genesis can advance through this
//! oracle. RootFence values are encoded and validated, but applying them
//! remains explicitly not qualified.

use std::fmt;
use std::marker::PhantomData;

use nokv_control::{
    decode_logical_shard_record, decode_owner_admission_claim, decode_owner_admission_intent,
    decode_owner_admission_plan_sentinel, decode_planned_owner_admission,
    encode_logical_shard_record, encode_owner_admission_claim, encode_owner_admission_intent,
    encode_owner_admission_plan_sentinel, encode_planned_owner_admission, LogicalShardRecord,
    LogicalShardState, OwnerAdmissionClaimPhaseV1, OwnerAdmissionClaimV1, OwnerAdmissionIntentV1,
    OwnerAdmissionPlanSentinelV1, OwnerAdmissionTerminationReasonV1, PlannedOwnerAdmissionV1,
    RootId, RootLayoutGeneration, RootLayoutProfile, RootPartitionId,
};
use nokv_meta::workspace::AcknowledgedMetadataFrontier;
use nokv_types::{CommandDigest, CommitVersion, LogicalShardId, RequestId, SHA256_BYTES};
use sha2::{Digest as _, Sha256};

#[cfg(unix)]
mod unix;

#[cfg(unix)]
use unix::StoreOwnerJournalAuthorityV4;

const WIRE_VERSION: u8 = 4;
const WIRE_MAGIC: &[u8; 16] = b"NOKV-OWNER-JNL4\0";
const STORE_UUID_BYTES: usize = 16;
const MAX_WIRE_BYTES: usize = 2 * 1024 * 1024;
const MAX_INLINE_PLAN_BYTES: usize = 8 * 1024;
const MAX_USED_REQUEST_IDS: usize = 1_024;
const WIRE_DIGEST_DOMAIN: &[u8] = b"nokv.server.store-owner-journal.wire.v4\0";
const BINDING_DIGEST_DOMAIN: &[u8] = b"nokv.server.store-owner-journal.binding.v4\0";
const PENDING_DIGEST_DOMAIN: &[u8] = b"nokv.server.store-owner-journal.pending.v4\0";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreOwnerStateV4 {
    Prepared,
    Ready {
        frontier: AcknowledgedMetadataFrontier,
    },
}

impl fmt::Debug for StoreOwnerStateV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Prepared => "Prepared",
            Self::Ready { .. } => "Ready(<redacted>)",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreOwnerAttemptStateV4 {
    Released,
    AdmissionIntent,
    OwnerPlanned,
    OwnerCommitted,
    Serving,
    Releasing,
}

impl StoreOwnerAttemptStateV4 {
    const fn tag(self) -> &'static str {
        match self {
            Self::Released => "Released",
            Self::AdmissionIntent => "AdmissionIntent",
            Self::OwnerPlanned => "OwnerPlanned",
            Self::OwnerCommitted => "OwnerCommitted",
            Self::Serving => "Serving",
            Self::Releasing => "Releasing",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StoreUuidV4([u8; STORE_UUID_BYTES]);

impl fmt::Debug for StoreUuidV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoreUuidV4(<redacted>)")
    }
}

impl StoreUuidV4 {
    pub(crate) fn from_bytes(
        bytes: [u8; STORE_UUID_BYTES],
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        require_nonzero(&bytes, "store UUID")?;
        Ok(Self(bytes))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreOwnerRootBindingV4 {
    root_id: RootId,
    layout_profile: RootLayoutProfile,
    layout_generation: RootLayoutGeneration,
    partition_id: RootPartitionId,
    logical_shard_id: LogicalShardId,
}

impl fmt::Debug for StoreOwnerRootBindingV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoreOwnerRootBindingV4(<redacted>)")
    }
}

impl StoreOwnerRootBindingV4 {
    pub(crate) fn new(
        root_id: RootId,
        layout_profile: RootLayoutProfile,
        layout_generation: RootLayoutGeneration,
        partition_id: RootPartitionId,
        logical_shard_id: LogicalShardId,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        require_nonzero(root_id.as_bytes(), "root id")?;
        require_nonzero(logical_shard_id.as_bytes(), "logical shard id")?;
        Ok(Self {
            root_id,
            layout_profile,
            layout_generation,
            partition_id,
            logical_shard_id,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreReservationBindingDigestV4([u8; SHA256_BYTES]);

impl fmt::Debug for StoreReservationBindingDigestV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoreReservationBindingDigestV4(<redacted>)")
    }
}

impl StoreReservationBindingDigestV4 {
    pub(crate) fn from_bytes(bytes: [u8; SHA256_BYTES]) -> Result<Self, StoreOwnerJournalErrorV4> {
        require_nonzero(&bytes, "store reservation binding digest")?;
        Ok(Self(bytes))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreOwnerPhysicalBindingV4 {
    canonical_journal_locator_digest: [u8; SHA256_BYTES],
    canonical_provider_locator_digest: [u8; SHA256_BYTES],
    held_directory_lock_identity_digest: [u8; SHA256_BYTES],
    reservation_binding_digest: StoreReservationBindingDigestV4,
}

impl fmt::Debug for StoreOwnerPhysicalBindingV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoreOwnerPhysicalBindingV4(<redacted>)")
    }
}

impl StoreOwnerPhysicalBindingV4 {
    pub(crate) fn from_digests(
        canonical_journal_locator_digest: [u8; SHA256_BYTES],
        canonical_provider_locator_digest: [u8; SHA256_BYTES],
        held_directory_lock_identity_digest: [u8; SHA256_BYTES],
        reservation_binding_digest: StoreReservationBindingDigestV4,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        require_nonzero(
            &canonical_journal_locator_digest,
            "canonical journal locator digest",
        )?;
        require_nonzero(
            &canonical_provider_locator_digest,
            "canonical provider locator digest",
        )?;
        require_nonzero(
            &held_directory_lock_identity_digest,
            "held directory and lock identity digest",
        )?;
        Ok(Self {
            canonical_journal_locator_digest,
            canonical_provider_locator_digest,
            held_directory_lock_identity_digest,
            reservation_binding_digest,
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct StoreOwnerBindingV4 {
    store_uuid: StoreUuidV4,
    root: StoreOwnerRootBindingV4,
    physical: StoreOwnerPhysicalBindingV4,
}

impl fmt::Debug for StoreOwnerBindingV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoreOwnerBindingV4(<redacted>)")
    }
}

impl StoreOwnerBindingV4 {
    pub(crate) fn new(
        store_uuid: StoreUuidV4,
        root: StoreOwnerRootBindingV4,
        physical: StoreOwnerPhysicalBindingV4,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        Ok(Self {
            store_uuid,
            root,
            physical,
        })
    }

    fn matches_intent(&self, intent: &OwnerAdmissionIntentV1) -> bool {
        let placement = intent.admission().placement();
        self.root.root_id == placement.root_id
            && self.root.layout_profile == placement.layout_profile
            && self.root.layout_generation == placement.layout_generation
            && self.root.partition_id == placement.partition_id
            && self.root.logical_shard_id == placement.logical_shard_id
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct CanonicalGenesisBindingV4 {
    schema_digest: [u8; SHA256_BYTES],
    store_identity_digest: [u8; SHA256_BYTES],
    authority_digest: [u8; SHA256_BYTES],
    zero_user_state_digest: [u8; SHA256_BYTES],
}

impl fmt::Debug for CanonicalGenesisBindingV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CanonicalGenesisBindingV4(<redacted>)")
    }
}

impl CanonicalGenesisBindingV4 {
    pub(crate) fn new(
        schema_digest: [u8; SHA256_BYTES],
        store_binding: &StoreOwnerBindingV4,
        authority_digest: [u8; SHA256_BYTES],
        zero_user_state_digest: [u8; SHA256_BYTES],
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        Self::from_durable_parts(
            schema_digest,
            store_binding_digest(store_binding)?,
            authority_digest,
            zero_user_state_digest,
        )
    }

    fn from_durable_parts(
        schema_digest: [u8; SHA256_BYTES],
        store_identity_digest: [u8; SHA256_BYTES],
        authority_digest: [u8; SHA256_BYTES],
        zero_user_state_digest: [u8; SHA256_BYTES],
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        require_nonzero(&schema_digest, "canonical schema digest")?;
        require_nonzero(&store_identity_digest, "canonical store identity digest")?;
        require_nonzero(&authority_digest, "canonical authority digest")?;
        require_nonzero(&zero_user_state_digest, "canonical zero-user-state digest")?;
        Ok(Self {
            schema_digest,
            store_identity_digest,
            authority_digest,
            zero_user_state_digest,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProviderCommandIdentityV4 {
    request_id: RequestId,
    command_digest: CommandDigest,
}

impl fmt::Debug for ProviderCommandIdentityV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProviderCommandIdentityV4(<redacted>)")
    }
}

impl ProviderCommandIdentityV4 {
    pub(crate) fn new(
        request_id: RequestId,
        command_digest: CommandDigest,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        require_nonzero(request_id.as_bytes(), "provider request id")?;
        require_nonzero(command_digest.as_bytes(), "provider command digest")?;
        Ok(Self {
            request_id,
            command_digest,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderCommitPurposeV4 {
    CanonicalGenesis(CanonicalGenesisBindingV4),
    RootFenceInstall(ProviderCommandIdentityV4),
    RootFenceActivate(ProviderCommandIdentityV4),
}

impl fmt::Debug for ProviderCommitPurposeV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CanonicalGenesis(_) => "CanonicalGenesis(<redacted>)",
            Self::RootFenceInstall(_) => "RootFenceInstall(<redacted>)",
            Self::RootFenceActivate(_) => "RootFenceActivate(<redacted>)",
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrontierPointV4 {
    Absent,
    Acknowledged(AcknowledgedMetadataFrontier),
}

impl fmt::Debug for FrontierPointV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Absent => "Absent",
            Self::Acknowledged(_) => "Acknowledged(<redacted>)",
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingProviderCommitV4 {
    purpose: ProviderCommitPurposeV4,
    prior: FrontierPointV4,
    exact_next: AcknowledgedMetadataFrontier,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct CanonicalGenesisReceiptV4 {
    binding: CanonicalGenesisBindingV4,
    exact_next: AcknowledgedMetadataFrontier,
}

impl fmt::Debug for CanonicalGenesisReceiptV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CanonicalGenesisReceiptV4(<redacted>)")
    }
}

impl fmt::Debug for PendingProviderCommitV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingProviderCommitV4")
            .field("purpose", &self.purpose)
            .field("receipt", &"<redacted>")
            .finish()
    }
}

impl PendingProviderCommitV4 {
    pub(crate) fn canonical_genesis(
        exact_next: AcknowledgedMetadataFrontier,
        binding: CanonicalGenesisBindingV4,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        let pending = Self {
            purpose: ProviderCommitPurposeV4::CanonicalGenesis(binding),
            prior: FrontierPointV4::Absent,
            exact_next,
        };
        pending.validate()?;
        Ok(pending)
    }

    pub(crate) fn root_fence_install(
        prior: AcknowledgedMetadataFrontier,
        exact_next: AcknowledgedMetadataFrontier,
        command: ProviderCommandIdentityV4,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        Self::root_fence(
            ProviderCommitPurposeV4::RootFenceInstall(command),
            prior,
            exact_next,
        )
    }

    pub(crate) fn root_fence_activate(
        prior: AcknowledgedMetadataFrontier,
        exact_next: AcknowledgedMetadataFrontier,
        command: ProviderCommandIdentityV4,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        Self::root_fence(
            ProviderCommitPurposeV4::RootFenceActivate(command),
            prior,
            exact_next,
        )
    }

    fn root_fence(
        purpose: ProviderCommitPurposeV4,
        prior: AcknowledgedMetadataFrontier,
        exact_next: AcknowledgedMetadataFrontier,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        let pending = Self {
            purpose,
            prior: FrontierPointV4::Acknowledged(prior),
            exact_next,
        };
        pending.validate()?;
        Ok(pending)
    }

    fn validate(&self) -> Result<(), StoreOwnerJournalErrorV4> {
        validate_frontier(self.exact_next)?;
        match (self.purpose, self.prior) {
            (ProviderCommitPurposeV4::CanonicalGenesis(_), FrontierPointV4::Absent) => {
                validate_canonical_genesis(self.exact_next)
            }
            (ProviderCommitPurposeV4::CanonicalGenesis(_), _) | (_, FrontierPointV4::Absent) => {
                Err(StoreOwnerJournalErrorV4::InvalidJournal(
                    "provider purpose does not match its prior frontier point",
                ))
            }
            (_, FrontierPointV4::Acknowledged(prior)) => {
                validate_frontier(prior)?;
                validate_root_fence_delta(prior, self.exact_next)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VerifiedProviderCommitOutcomeV4 {
    NotAppliedAtPrior,
    TerminalAtExactNext,
}

/// Opaque seam for an exact provider/dedupe observation.
///
/// Phase one intentionally exposes no production constructor. The later
/// `nokv-meta` bridge must retain its exact inspection guard for this proof's
/// full lifetime and bind the complete pending receipt.
pub(crate) struct VerifiedProviderCommitReceiptV4<'verification> {
    store_binding_digest: [u8; SHA256_BYTES],
    pending_receipt_digest: [u8; SHA256_BYTES],
    observed: FrontierPointV4,
    outcome: VerifiedProviderCommitOutcomeV4,
    _verified: PhantomData<&'verification mut ()>,
    _private: (),
}

impl fmt::Debug for VerifiedProviderCommitReceiptV4<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VerifiedProviderCommitReceiptV4(<redacted>)")
    }
}

/// Opaque seam for the real cross-process exclusive store reservation.
///
/// This is not the stable runtime-reservation digest. A later registry bridge
/// must borrow the non-Clone held directory/lock guard for this proof's full
/// lifetime. Phase one intentionally exposes no production constructor.
pub(crate) struct StoreOwnerJournalExclusiveReservationV4<'reservation> {
    store_binding_digest: [u8; SHA256_BYTES],
    authority: StoreOwnerJournalAuthorityProofV4<'reservation>,
    _private: (),
}

enum StoreOwnerJournalAuthorityProofV4<'reservation> {
    #[cfg(unix)]
    Held(&'reservation StoreOwnerJournalAuthorityV4),
    #[cfg(test)]
    Characterization(PhantomData<&'reservation mut ()>),
}

impl fmt::Debug for StoreOwnerJournalExclusiveReservationV4<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoreOwnerJournalExclusiveReservationV4(<redacted>)")
    }
}

impl<'reservation> StoreOwnerJournalExclusiveReservationV4<'reservation> {
    #[cfg(unix)]
    fn from_authority(
        binding: &StoreOwnerBindingV4,
        authority: &'reservation StoreOwnerJournalAuthorityV4,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        authority
            .validate_binding()
            .map_err(|_| StoreOwnerJournalErrorV4::BindingMismatch("journal authority"))?;
        if binding.physical.canonical_journal_locator_digest
            != authority
                .canonical_locator_digest()
                .map_err(|_| StoreOwnerJournalErrorV4::BindingMismatch("journal locator"))?
            || binding.physical.held_directory_lock_identity_digest
                != authority.authority_identity_digest().map_err(|_| {
                    StoreOwnerJournalErrorV4::BindingMismatch("journal authority identity")
                })?
        {
            return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                "immutable journal authority",
            ));
        }
        Ok(Self {
            store_binding_digest: store_binding_digest(binding)?,
            authority: StoreOwnerJournalAuthorityProofV4::Held(authority),
            _private: (),
        })
    }

    fn validate_authority(&self) -> Result<(), StoreOwnerJournalErrorV4> {
        match &self.authority {
            #[cfg(unix)]
            StoreOwnerJournalAuthorityProofV4::Held(authority) => authority
                .validate_binding()
                .map_err(|_| StoreOwnerJournalErrorV4::BindingMismatch("journal authority")),
            #[cfg(test)]
            StoreOwnerJournalAuthorityProofV4::Characterization(_) => Ok(()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct RootFenceAttemptRequestIdsV4 {
    install: Option<RequestId>,
    activate: RequestId,
}

impl fmt::Debug for RootFenceAttemptRequestIdsV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootFenceAttemptRequestIdsV4")
            .field("has_install", &self.install.is_some())
            .field("activate", &"<redacted>")
            .finish()
    }
}

impl RootFenceAttemptRequestIdsV4 {
    pub(crate) fn new(
        install: Option<RequestId>,
        activate: RequestId,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        require_nonzero(activate.as_bytes(), "RootFence activate request id")?;
        if install.is_some_and(|request| {
            request == activate || request.as_bytes().iter().all(|byte| *byte == 0)
        }) {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "RootFence request ids must be non-zero and distinct",
            ));
        }
        Ok(Self { install, activate })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct TerminalProviderCommandV4 {
    command: ProviderCommandIdentityV4,
    prior: AcknowledgedMetadataFrontier,
    exact_next: AcknowledgedMetadataFrontier,
}

impl fmt::Debug for TerminalProviderCommandV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TerminalProviderCommandV4(<redacted>)")
    }
}

impl TerminalProviderCommandV4 {
    fn from_pending(pending: PendingProviderCommitV4) -> Result<Self, StoreOwnerJournalErrorV4> {
        let command = match pending.purpose {
            ProviderCommitPurposeV4::RootFenceInstall(command)
            | ProviderCommitPurposeV4::RootFenceActivate(command) => command,
            ProviderCommitPurposeV4::CanonicalGenesis(_) => {
                return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                    "canonical genesis is not a RootFence command",
                ));
            }
        };
        let FrontierPointV4::Acknowledged(prior) = pending.prior else {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "RootFence command has no acknowledged prior",
            ));
        };
        pending.validate()?;
        Ok(Self {
            command,
            prior,
            exact_next: pending.exact_next,
        })
    }

    fn validate(&self) -> Result<(), StoreOwnerJournalErrorV4> {
        ProviderCommandIdentityV4::new(self.command.request_id, self.command.command_digest)?;
        validate_frontier(self.prior)?;
        validate_frontier(self.exact_next)?;
        validate_root_fence_delta(self.prior, self.exact_next)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct InstalledRootFenceReceiptV4 {
    attempt_sequence: u64,
    plan_digest: [u8; SHA256_BYTES],
    terminal: TerminalProviderCommandV4,
}

impl fmt::Debug for InstalledRootFenceReceiptV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstalledRootFenceReceiptV4(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Eq)]
enum AttemptReceiptV4 {
    Prepared {
        claim: OwnerAdmissionClaimV1,
        sentinel: OwnerAdmissionPlanSentinelV1,
    },
    Committed {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    Rejected {
        claim: OwnerAdmissionClaimV1,
    },
    Aborted {
        claim: OwnerAdmissionClaimV1,
    },
    Terminated {
        committed_shard: LogicalShardRecord,
        committed_claim: Box<OwnerAdmissionClaimV1>,
        shard: LogicalShardRecord,
        claim: Box<OwnerAdmissionClaimV1>,
    },
}

#[derive(Clone, PartialEq, Eq)]
struct ServingPublicationReceiptV4 {
    shard: LogicalShardRecord,
    claim: OwnerAdmissionClaimV1,
}

impl fmt::Debug for ServingPublicationReceiptV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServingPublicationReceiptV4(<redacted>)")
    }
}

impl fmt::Debug for AttemptReceiptV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Prepared { .. } => "Prepared(<redacted>)",
            Self::Committed { .. } => "Committed(<redacted>)",
            Self::Rejected { .. } => "Rejected(<redacted>)",
            Self::Aborted { .. } => "Aborted(<redacted>)",
            Self::Terminated { .. } => "Terminated(<redacted>)",
        })
    }
}

impl AttemptReceiptV4 {
    fn claim(&self) -> &OwnerAdmissionClaimV1 {
        match self {
            Self::Prepared { claim, .. }
            | Self::Committed { claim, .. }
            | Self::Rejected { claim }
            | Self::Aborted { claim } => claim,
            Self::Terminated { claim, .. } => claim.as_ref(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct AttemptWireV4 {
    sequence: u64,
    state: StoreOwnerAttemptStateV4,
    request_ids: Option<RootFenceAttemptRequestIdsV4>,
    intent: Option<OwnerAdmissionIntentV1>,
    plan: Option<PlannedOwnerAdmissionV1>,
    receipt: Option<AttemptReceiptV4>,
    serving_publication: Option<ServingPublicationReceiptV4>,
    requested_termination: Option<OwnerAdmissionClaimV1>,
    activated_root_fence: Option<TerminalProviderCommandV4>,
}

impl fmt::Debug for AttemptWireV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttemptWireV4")
            .field("sequence", &self.sequence)
            .field("state", &self.state.tag())
            .field("contents", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
struct HeadWireV4 {
    generation: u64,
    binding: StoreOwnerBindingV4,
    store_state: StoreOwnerStateV4,
    canonical_genesis: Option<CanonicalGenesisReceiptV4>,
    installed_root_fence: Option<InstalledRootFenceReceiptV4>,
    attempt: AttemptWireV4,
    pending_provider_commit: Option<PendingProviderCommitV4>,
    used_request_ids: Vec<RequestId>,
}

impl fmt::Debug for HeadWireV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HeadWireV4")
            .field("generation", &self.generation)
            .field("store_state", &self.store_state)
            .field("attempt", &self.attempt)
            .field("contents", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct StoreOwnerJournalOracleV4 {
    head: HeadWireV4,
}

impl fmt::Debug for StoreOwnerJournalOracleV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreOwnerJournalOracleV4")
            .field("generation", &self.head.generation)
            .field("store_state", &self.head.store_state)
            .field("attempt_state", &self.head.attempt.state.tag())
            .field("contents", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StoreOwnerJournalErrorV4 {
    InvalidJournal(&'static str),
    BindingMismatch(&'static str),
    IllegalTransition {
        from: &'static str,
        to: &'static str,
    },
    PendingFrontierMismatch,
    DependencyNotQualified(&'static str),
    InlinePlanBudgetExceeded {
        bytes: usize,
        inline_limit: usize,
    },
}

impl fmt::Display for StoreOwnerJournalErrorV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJournal(reason) => write!(formatter, "invalid v4 owner journal: {reason}"),
            Self::BindingMismatch(field) => write!(formatter, "v4 owner binding mismatch: {field}"),
            Self::IllegalTransition { from, to } => {
                write!(
                    formatter,
                    "v4 owner transition {from} -> {to} is not allowed"
                )
            }
            Self::PendingFrontierMismatch => formatter.write_str(
                "verified provider receipt matches neither the exact prior nor exact next",
            ),
            Self::DependencyNotQualified(dependency) => {
                write!(
                    formatter,
                    "v4 owner dependency is not qualified: {dependency}"
                )
            }
            Self::InlinePlanBudgetExceeded {
                bytes,
                inline_limit,
            } => write!(
                formatter,
                "immutable plan exceeds the inline budget ({bytes} bytes exceeds {inline_limit})"
            ),
        }
    }
}

impl std::error::Error for StoreOwnerJournalErrorV4 {}

impl StoreOwnerJournalOracleV4 {
    pub(crate) fn new(
        binding: StoreOwnerBindingV4,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        validate_exclusive_reservation(&binding, exclusive)?;
        let oracle = Self {
            head: HeadWireV4 {
                generation: 1,
                binding,
                store_state: StoreOwnerStateV4::Prepared,
                canonical_genesis: None,
                installed_root_fence: None,
                attempt: empty_attempt(),
                pending_provider_commit: None,
                used_request_ids: Vec::new(),
            },
        };
        validate_head(&oracle.head)?;
        Ok(oracle)
    }

    pub(crate) fn decode(
        bytes: &[u8],
        expected_binding: &StoreOwnerBindingV4,
    ) -> Result<Self, StoreOwnerJournalErrorV4> {
        let head = decode_head(bytes)?;
        if &head.binding != expected_binding {
            return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                "immutable store binding",
            ));
        }
        validate_head(&head)?;
        let canonical = encode_head(&head)?;
        if canonical != bytes {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "wire is not canonical",
            ));
        }
        Ok(Self { head })
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, StoreOwnerJournalErrorV4> {
        validate_head(&self.head)?;
        encode_head(&self.head)
    }

    pub(crate) const fn store_state(&self) -> StoreOwnerStateV4 {
        self.head.store_state
    }

    pub(crate) const fn attempt_state(&self) -> StoreOwnerAttemptStateV4 {
        self.head.attempt.state
    }

    pub(crate) const fn pending_provider_commit(&self) -> Option<PendingProviderCommitV4> {
        self.head.pending_provider_commit
    }

    pub(crate) fn begin_admission_intent(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        intent: OwnerAdmissionIntentV1,
        request_ids: RootFenceAttemptRequestIdsV4,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        self.mutate(exclusive, |head| {
            if head.attempt.state == StoreOwnerAttemptStateV4::AdmissionIntent
                && head.attempt.intent.as_ref() == Some(&intent)
                && head.attempt.request_ids == Some(request_ids)
            {
                return Ok(false);
            }
            require_transition(
                head.attempt.state,
                StoreOwnerAttemptStateV4::AdmissionIntent,
                StoreOwnerAttemptStateV4::Released,
            )?;
            if head.pending_provider_commit.is_some() {
                return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                    "a new attempt cannot begin with a pending provider commit",
                ));
            }
            if !head.binding.matches_intent(&intent) {
                return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                    "intent root and shard binding",
                ));
            }
            validate_attempt_predecessor(&head.attempt, &intent)?;
            validate_request_id_shape(head.installed_root_fence.as_ref(), None, request_ids)?;
            let mut requested = Vec::with_capacity(2);
            if let Some(install) = request_ids.install {
                requested.push(install);
            }
            requested.push(request_ids.activate);
            for request in &requested {
                if head.used_request_ids.contains(request) {
                    return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                        "RootFence request id was already consumed by another attempt",
                    ));
                }
            }
            if head.used_request_ids.len().saturating_add(requested.len()) > MAX_USED_REQUEST_IDS {
                return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                    "RootFence request-id history exceeds its bound",
                ));
            }
            head.used_request_ids.extend(requested);
            head.attempt = AttemptWireV4 {
                sequence: head.attempt.sequence.checked_add(1).ok_or(
                    StoreOwnerJournalErrorV4::InvalidJournal("attempt sequence is exhausted"),
                )?,
                state: StoreOwnerAttemptStateV4::AdmissionIntent,
                request_ids: Some(request_ids),
                intent: Some(intent),
                plan: None,
                receipt: None,
                serving_publication: None,
                requested_termination: None,
                activated_root_fence: None,
            };
            Ok(true)
        })
    }

    pub(crate) fn record_owner_plan(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        plan: PlannedOwnerAdmissionV1,
        claim: OwnerAdmissionClaimV1,
        sentinel: OwnerAdmissionPlanSentinelV1,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        let plan_bytes = encode_planned_owner_admission(&plan).map_err(control_codec_error)?;
        if plan_bytes.len() > MAX_INLINE_PLAN_BYTES {
            return Err(StoreOwnerJournalErrorV4::InlinePlanBudgetExceeded {
                bytes: plan_bytes.len(),
                inline_limit: MAX_INLINE_PLAN_BYTES,
            });
        }
        validate_prepared_receipt(&plan, &claim, &sentinel)?;
        self.mutate(exclusive, |head| {
            let receipt = AttemptReceiptV4::Prepared {
                claim: claim.clone(),
                sentinel: sentinel.clone(),
            };
            if head.attempt.state == StoreOwnerAttemptStateV4::OwnerPlanned
                && head.attempt.plan.as_ref() == Some(&plan)
                && head.attempt.receipt.as_ref() == Some(&receipt)
            {
                return Ok(false);
            }
            require_transition(
                head.attempt.state,
                StoreOwnerAttemptStateV4::OwnerPlanned,
                StoreOwnerAttemptStateV4::AdmissionIntent,
            )?;
            if head.attempt.intent.as_ref() != Some(plan.intent()) {
                return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                    "planned owner intent",
                ));
            }
            head.attempt.state = StoreOwnerAttemptStateV4::OwnerPlanned;
            head.attempt.plan = Some(plan);
            head.attempt.receipt = Some(receipt);
            Ok(true)
        })
    }

    pub(crate) fn record_owner_rejected(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        claim: OwnerAdmissionClaimV1,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        self.mutate(exclusive, |head| {
            if head.attempt.state == StoreOwnerAttemptStateV4::Released
                && head.attempt.receipt.as_ref()
                    == Some(&AttemptReceiptV4::Rejected {
                        claim: claim.clone(),
                    })
            {
                return Ok(false);
            }
            require_transition(
                head.attempt.state,
                StoreOwnerAttemptStateV4::Released,
                StoreOwnerAttemptStateV4::AdmissionIntent,
            )?;
            let intent =
                head.attempt
                    .intent
                    .as_ref()
                    .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                        "admission intent is absent",
                    ))?;
            validate_rejected_claim(intent, &claim)?;
            head.attempt.state = StoreOwnerAttemptStateV4::Released;
            head.attempt.receipt = Some(AttemptReceiptV4::Rejected { claim });
            Ok(true)
        })
    }

    pub(crate) fn record_owner_aborted(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        claim: OwnerAdmissionClaimV1,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        self.mutate(exclusive, |head| {
            if head.attempt.state == StoreOwnerAttemptStateV4::Released
                && head.attempt.receipt.as_ref()
                    == Some(&AttemptReceiptV4::Aborted {
                        claim: claim.clone(),
                    })
            {
                return Ok(false);
            }
            require_transition(
                head.attempt.state,
                StoreOwnerAttemptStateV4::Released,
                StoreOwnerAttemptStateV4::OwnerPlanned,
            )?;
            let plan =
                head.attempt
                    .plan
                    .as_ref()
                    .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                        "owner plan is absent",
                    ))?;
            validate_aborted_claim(plan, &claim)?;
            head.attempt.state = StoreOwnerAttemptStateV4::Released;
            head.attempt.receipt = Some(AttemptReceiptV4::Aborted { claim });
            Ok(true)
        })
    }

    pub(crate) fn record_owner_committed(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        self.mutate(exclusive, |head| {
            let receipt = AttemptReceiptV4::Committed {
                shard: shard.clone(),
                claim: claim.clone(),
            };
            if head.attempt.state == StoreOwnerAttemptStateV4::OwnerCommitted
                && head.attempt.receipt.as_ref() == Some(&receipt)
            {
                return Ok(false);
            }
            require_transition(
                head.attempt.state,
                StoreOwnerAttemptStateV4::OwnerCommitted,
                StoreOwnerAttemptStateV4::OwnerPlanned,
            )?;
            let plan =
                head.attempt
                    .plan
                    .as_ref()
                    .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                        "owner plan is absent",
                    ))?;
            validate_committed_receipt(plan, &shard, &claim)?;
            head.attempt.state = StoreOwnerAttemptStateV4::OwnerCommitted;
            head.attempt.receipt = Some(receipt);
            Ok(true)
        })
    }

    pub(crate) fn begin_provider_commit(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        pending: PendingProviderCommitV4,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        pending.validate()?;
        match pending.purpose {
            ProviderCommitPurposeV4::CanonicalGenesis(binding) => {
                validate_genesis_binding(&self.head.binding, binding)?;
            }
            ProviderCommitPurposeV4::RootFenceInstall(_)
            | ProviderCommitPurposeV4::RootFenceActivate(_) => {
                return Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
                    "nokv-meta bootstrap owner-epoch and RootFence exact receipts",
                ));
            }
        }
        self.mutate(exclusive, |head| {
            if head.pending_provider_commit == Some(pending) {
                return Ok(false);
            }
            if head.pending_provider_commit.is_some()
                || head.canonical_genesis.is_some()
                || head.store_state != StoreOwnerStateV4::Prepared
            {
                return Err(StoreOwnerJournalErrorV4::IllegalTransition {
                    from: "store-state",
                    to: "CanonicalGenesisPending",
                });
            }
            head.pending_provider_commit = Some(pending);
            Ok(true)
        })
    }

    pub(crate) fn reconcile_pending_provider_commit(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        verified: &VerifiedProviderCommitReceiptV4<'_>,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        validate_exclusive_reservation(&self.head.binding, exclusive)?;
        let pending =
            self.head
                .pending_provider_commit
                .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                    "provider commit is not pending",
                ))?;
        match pending.purpose {
            ProviderCommitPurposeV4::CanonicalGenesis(_) => {}
            _ => {
                return Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
                    "nokv-meta bootstrap owner-epoch and RootFence exact receipts",
                ));
            }
        }
        if verified.store_binding_digest != store_binding_digest(&self.head.binding)?
            || verified.pending_receipt_digest != pending_receipt_digest(pending)?
        {
            return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                "verified provider receipt",
            ));
        }
        match (verified.outcome, verified.observed) {
            (VerifiedProviderCommitOutcomeV4::NotAppliedAtPrior, FrontierPointV4::Absent) => Ok(()),
            (
                VerifiedProviderCommitOutcomeV4::TerminalAtExactNext,
                FrontierPointV4::Acknowledged(observed),
            ) if observed == pending.exact_next => self.mutate(exclusive, |head| {
                let ProviderCommitPurposeV4::CanonicalGenesis(binding) = pending.purpose else {
                    return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                        "terminal genesis receipt has another purpose",
                    ));
                };
                head.store_state = StoreOwnerStateV4::Ready {
                    frontier: pending.exact_next,
                };
                head.canonical_genesis = Some(CanonicalGenesisReceiptV4 {
                    binding,
                    exact_next: pending.exact_next,
                });
                head.pending_provider_commit = None;
                Ok(true)
            }),
            _ => Err(StoreOwnerJournalErrorV4::PendingFrontierMismatch),
        }
    }

    pub(crate) fn begin_releasing(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        requested_termination: OwnerAdmissionClaimV1,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        self.mutate(exclusive, |head| {
            if head.attempt.state == StoreOwnerAttemptStateV4::Releasing
                && head.attempt.requested_termination.as_ref() == Some(&requested_termination)
            {
                return Ok(false);
            }
            require_transition(
                head.attempt.state,
                StoreOwnerAttemptStateV4::Releasing,
                StoreOwnerAttemptStateV4::OwnerCommitted,
            )?;
            let (plan, committed_claim) = committed_attempt(head)?;
            validate_termination_claim(committed_claim, &requested_termination)?;
            validate_claim_identity(plan, committed_claim)?;
            head.attempt.state = StoreOwnerAttemptStateV4::Releasing;
            head.attempt.requested_termination = Some(requested_termination);
            Ok(true)
        })
    }

    pub(crate) fn complete_released(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        self.mutate(exclusive, |head| {
            if head.attempt.state == StoreOwnerAttemptStateV4::Released {
                if let Some(AttemptReceiptV4::Terminated {
                    shard: persisted_shard,
                    claim: persisted_claim,
                    ..
                }) = head.attempt.receipt.as_ref()
                {
                    if persisted_shard == &shard && persisted_claim.as_ref() == &claim {
                        return Ok(false);
                    }
                }
            }
            require_transition(
                head.attempt.state,
                StoreOwnerAttemptStateV4::Released,
                StoreOwnerAttemptStateV4::Releasing,
            )?;
            let plan =
                head.attempt
                    .plan
                    .as_ref()
                    .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                        "owner plan is absent",
                    ))?;
            let (committed_shard, committed_claim) = match head.attempt.receipt.as_ref() {
                Some(AttemptReceiptV4::Committed { shard, claim }) => {
                    (shard.clone(), claim.clone())
                }
                _ => {
                    return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                        "releasing attempt has no committed receipt",
                    ));
                }
            };
            let requested = head.attempt.requested_termination.as_ref().ok_or(
                StoreOwnerJournalErrorV4::InvalidJournal("termination claim is absent"),
            )?;
            if requested != &claim {
                return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                    "terminal owner claim",
                ));
            }
            validate_terminal_record(plan, &committed_shard, &shard, &claim)?;
            head.attempt.state = StoreOwnerAttemptStateV4::Released;
            head.attempt.receipt = Some(AttemptReceiptV4::Terminated {
                committed_shard,
                committed_claim: Box::new(committed_claim),
                shard,
                claim: Box::new(claim),
            });
            head.attempt.requested_termination = None;
            Ok(true)
        })
    }

    fn mutate(
        &mut self,
        exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
        mutation: impl FnOnce(&mut HeadWireV4) -> Result<bool, StoreOwnerJournalErrorV4>,
    ) -> Result<(), StoreOwnerJournalErrorV4> {
        validate_exclusive_reservation(&self.head.binding, exclusive)?;
        validate_head(&self.head)?;
        let mut next = self.head.clone();
        if !mutation(&mut next)? {
            return Ok(());
        }
        next.generation =
            next.generation
                .checked_add(1)
                .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                    "head generation is exhausted",
                ))?;
        validate_head(&next)?;
        self.head = next;
        Ok(())
    }
}

fn empty_attempt() -> AttemptWireV4 {
    AttemptWireV4 {
        sequence: 0,
        state: StoreOwnerAttemptStateV4::Released,
        request_ids: None,
        intent: None,
        plan: None,
        receipt: None,
        serving_publication: None,
        requested_termination: None,
        activated_root_fence: None,
    }
}

fn require_transition(
    from: StoreOwnerAttemptStateV4,
    to: StoreOwnerAttemptStateV4,
    required: StoreOwnerAttemptStateV4,
) -> Result<(), StoreOwnerJournalErrorV4> {
    if from != required {
        return Err(StoreOwnerJournalErrorV4::IllegalTransition {
            from: from.tag(),
            to: to.tag(),
        });
    }
    Ok(())
}

fn validate_exclusive_reservation(
    binding: &StoreOwnerBindingV4,
    exclusive: &StoreOwnerJournalExclusiveReservationV4<'_>,
) -> Result<(), StoreOwnerJournalErrorV4> {
    exclusive.validate_authority()?;
    if exclusive.store_binding_digest != store_binding_digest(binding)? {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "exclusive store reservation",
        ));
    }
    Ok(())
}

fn validate_request_id_shape(
    installed: Option<&InstalledRootFenceReceiptV4>,
    current_attempt_sequence: Option<u64>,
    request_ids: RootFenceAttemptRequestIdsV4,
) -> Result<(), StoreOwnerJournalErrorV4> {
    RootFenceAttemptRequestIdsV4::new(request_ids.install, request_ids.activate)?;
    let requires_install = match installed {
        None => true,
        Some(receipt) => current_attempt_sequence == Some(receipt.attempt_sequence),
    };
    if requires_install != request_ids.install.is_some() {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "RootFence install request presence does not match installed store state",
        ));
    }
    Ok(())
}

fn validate_attempt_predecessor(
    previous: &AttemptWireV4,
    intent: &OwnerAdmissionIntentV1,
) -> Result<(), StoreOwnerJournalErrorV4> {
    match previous.receipt.as_ref() {
        Some(AttemptReceiptV4::Terminated { claim, shard, .. }) => {
            if intent.expected_previous_claim() != Some(claim)
                || intent.expected_unowned_shard() != shard
            {
                return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                    "successor predecessor claim and shard",
                ));
            }
        }
        Some(AttemptReceiptV4::Rejected { .. }) | Some(AttemptReceiptV4::Aborted { .. }) | None => {
            if intent.expected_previous_claim().is_some() {
                return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                    "fresh attempt unexpectedly names a predecessor claim",
                ));
            }
        }
        Some(_) => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "released predecessor receipt is not terminal",
            ));
        }
    }
    Ok(())
}

fn validate_prepared_receipt(
    plan: &PlannedOwnerAdmissionV1,
    claim: &OwnerAdmissionClaimV1,
    sentinel: &OwnerAdmissionPlanSentinelV1,
) -> Result<(), StoreOwnerJournalErrorV4> {
    let expected = OwnerAdmissionClaimV1::prepared(plan).map_err(control_value_error)?;
    if claim != &expected {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "prepared owner claim",
        ));
    }
    sentinel.validate_plan(plan).map_err(control_value_error)
}

fn validate_claim_identity(
    plan: &PlannedOwnerAdmissionV1,
    claim: &OwnerAdmissionClaimV1,
) -> Result<(), StoreOwnerJournalErrorV4> {
    let prepared = OwnerAdmissionClaimV1::prepared(plan).map_err(control_value_error)?;
    if claim.identity() != prepared.identity() {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "owner claim identity",
        ));
    }
    Ok(())
}

fn validate_rejected_claim(
    intent: &OwnerAdmissionIntentV1,
    claim: &OwnerAdmissionClaimV1,
) -> Result<(), StoreOwnerJournalErrorV4> {
    let OwnerAdmissionClaimPhaseV1::Rejected { reason } = claim.phase() else {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "rejected attempt does not contain a Rejected claim",
        ));
    };
    let expected = OwnerAdmissionClaimV1::rejected_from_absent(intent, *reason)
        .map_err(control_value_error)?;
    if claim != &expected {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "rejected owner claim",
        ));
    }
    Ok(())
}

fn validate_aborted_claim(
    plan: &PlannedOwnerAdmissionV1,
    claim: &OwnerAdmissionClaimV1,
) -> Result<(), StoreOwnerJournalErrorV4> {
    let OwnerAdmissionClaimPhaseV1::Aborted { reason, .. } = claim.phase() else {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "aborted attempt does not contain an Aborted claim",
        ));
    };
    let expected = OwnerAdmissionClaimV1::prepared(plan)
        .map_err(control_value_error)?
        .abort(*reason)
        .map_err(control_value_error)?;
    if claim != &expected {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "aborted owner claim",
        ));
    }
    Ok(())
}

fn validate_committed_receipt(
    plan: &PlannedOwnerAdmissionV1,
    shard: &LogicalShardRecord,
    claim: &OwnerAdmissionClaimV1,
) -> Result<(), StoreOwnerJournalErrorV4> {
    let expected = OwnerAdmissionClaimV1::prepared(plan)
        .map_err(control_value_error)?
        .commit()
        .map_err(control_value_error)?;
    if claim != &expected {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "committed owner claim",
        ));
    }
    validate_committed_record(plan, shard)
}

fn validate_committed_record(
    plan: &PlannedOwnerAdmissionV1,
    shard: &LogicalShardRecord,
) -> Result<(), StoreOwnerJournalErrorV4> {
    encode_logical_shard_record(shard).map_err(control_codec_error)?;
    let lease = plan.lease();
    let intent = plan.intent();
    if shard.logical_shard_id != lease.logical_shard_id
        || shard.owner.as_ref() != Some(intent.owner())
        || shard.owner_epoch != Some(lease.owner_epoch)
        || shard.owner_incarnation_id != Some(lease.owner_incarnation_id)
        || shard.lease_id != lease.lease_id
        || shard.endpoint.as_deref() != Some(intent.endpoint())
        || shard.state != LogicalShardState::Recovering
    {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "committed logical shard record",
        ));
    }
    Ok(())
}

fn validate_termination_claim(
    committed: &OwnerAdmissionClaimV1,
    terminal: &OwnerAdmissionClaimV1,
) -> Result<(), StoreOwnerJournalErrorV4> {
    let OwnerAdmissionClaimPhaseV1::Terminated { reason, .. } = terminal.phase() else {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "release request does not contain a Terminated claim",
        ));
    };
    let expected = committed
        .clone()
        .terminate(reason.clone())
        .map_err(control_value_error)?;
    if terminal != &expected {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "termination owner claim",
        ));
    }
    Ok(())
}

fn validate_terminal_record(
    plan: &PlannedOwnerAdmissionV1,
    committed: &LogicalShardRecord,
    terminal: &LogicalShardRecord,
    claim: &OwnerAdmissionClaimV1,
) -> Result<(), StoreOwnerJournalErrorV4> {
    validate_committed_record(plan, committed)?;
    validate_claim_identity(plan, claim)?;
    encode_logical_shard_record(terminal).map_err(control_codec_error)?;
    if terminal.logical_shard_id != committed.logical_shard_id
        || terminal.owner.is_some()
        || terminal.owner_epoch != committed.owner_epoch
        || terminal.owner_incarnation_id != committed.owner_incarnation_id
        || terminal.lease_id != 0
        || terminal.endpoint.is_some()
        || terminal.state != LogicalShardState::Unassigned
    {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "terminal logical shard identity",
        ));
    }
    if terminal.checkpoint != committed.checkpoint
        || terminal.log != committed.log
        || terminal.durable_lsn != committed.durable_lsn
    {
        return Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
            "nokv-control exact recovery-descendant receipt",
        ));
    }
    Ok(())
}

fn committed_attempt(
    head: &HeadWireV4,
) -> Result<(&PlannedOwnerAdmissionV1, &OwnerAdmissionClaimV1), StoreOwnerJournalErrorV4> {
    let plan = head
        .attempt
        .plan
        .as_ref()
        .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
            "owner plan is absent",
        ))?;
    let claim = match head.attempt.receipt.as_ref() {
        Some(AttemptReceiptV4::Committed { claim, .. }) => claim,
        _ => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "committed attempt receipt is absent",
            ));
        }
    };
    Ok((plan, claim))
}

fn validate_head(head: &HeadWireV4) -> Result<(), StoreOwnerJournalErrorV4> {
    if head.generation == 0 {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "head generation is zero",
        ));
    }
    validate_store_binding(&head.binding)?;
    match (head.store_state, head.canonical_genesis) {
        (StoreOwnerStateV4::Prepared, None) => {}
        (StoreOwnerStateV4::Ready { frontier }, Some(receipt)) => {
            validate_frontier(frontier)?;
            validate_canonical_genesis(receipt.exact_next)?;
            validate_genesis_binding(&head.binding, receipt.binding)?;
        }
        (StoreOwnerStateV4::Prepared, Some(_)) => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "Prepared store retains a terminal genesis receipt",
            ));
        }
        (StoreOwnerStateV4::Ready { .. }, None) => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "Ready store has no exact canonical genesis receipt",
            ));
        }
    }
    if head.used_request_ids.len() > MAX_USED_REQUEST_IDS {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "RootFence request-id history exceeds its bound",
        ));
    }
    for (index, request) in head.used_request_ids.iter().enumerate() {
        require_nonzero(request.as_bytes(), "used RootFence request id")?;
        if head.used_request_ids[..index].contains(request) {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "RootFence request-id history contains a duplicate",
            ));
        }
    }
    validate_attempt(head)?;
    if let Some(pending) = head.pending_provider_commit {
        pending.validate()?;
        match pending.purpose {
            ProviderCommitPurposeV4::CanonicalGenesis(binding) => {
                if head.store_state != StoreOwnerStateV4::Prepared
                    || head.canonical_genesis.is_some()
                {
                    return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                        "canonical genesis is pending after the store became Ready",
                    ));
                }
                validate_genesis_binding(&head.binding, binding)?;
            }
            ProviderCommitPurposeV4::RootFenceInstall(_)
            | ProviderCommitPurposeV4::RootFenceActivate(_) => {
                validate_root_pending_for_head(head, pending)?;
                return Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
                    "nokv-meta bootstrap owner-epoch and RootFence exact receipts",
                ));
            }
        }
    }
    if head.attempt.activated_root_fence.is_some() {
        validate_root_fence_chain(head)?;
    }
    if let StoreOwnerStateV4::Ready { frontier } = head.store_state {
        let genesis = head.canonical_genesis.unwrap();
        if frontier != genesis.exact_next {
            let activated = head.attempt.activated_root_fence.as_ref().ok_or(
                StoreOwnerJournalErrorV4::InvalidJournal(
                    "Ready frontier advanced without an exact typed receipt",
                ),
            )?;
            if activated.exact_next != frontier {
                return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                    "Ready frontier and RootFence activate terminal",
                ));
            }
        }
    }
    if head.attempt.state == StoreOwnerAttemptStateV4::Serving {
        return Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
            "nokv-meta owner-epoch/RootFence chain and exact control Serving publication receipt",
        ));
    }
    if let Some(installed) = head.installed_root_fence {
        installed.terminal.validate()?;
        require_nonzero(&installed.plan_digest, "installed RootFence plan digest")?;
        if installed.attempt_sequence == 0 {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "installed RootFence attempt sequence is zero",
            ));
        }
        return Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
            "nokv-meta bootstrap owner-epoch and RootFence exact receipts",
        ));
    }
    Ok(())
}

fn validate_attempt(head: &HeadWireV4) -> Result<(), StoreOwnerJournalErrorV4> {
    let attempt = &head.attempt;
    if attempt.sequence == 0 {
        if attempt != &empty_attempt() {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "attempt sequence zero is not the canonical empty Released attempt",
            ));
        }
        return Ok(());
    }
    let request_ids = attempt
        .request_ids
        .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
            "attempt RootFence request ids are absent",
        ))?;
    validate_request_id_shape(
        head.installed_root_fence.as_ref(),
        Some(attempt.sequence),
        request_ids,
    )?;
    for request in request_ids
        .install
        .into_iter()
        .chain([request_ids.activate])
    {
        if !head.used_request_ids.contains(&request) {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "attempt RootFence request id is absent from durable history",
            ));
        }
    }
    let intent = attempt
        .intent
        .as_ref()
        .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
            "non-empty attempt has no intent",
        ))?;
    if !head.binding.matches_intent(intent) {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "attempt intent root and shard binding",
        ));
    }
    encode_owner_admission_intent(intent).map_err(control_codec_error)?;
    if let Some(plan) = attempt.plan.as_ref() {
        if plan.intent() != intent {
            return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                "attempt plan and intent",
            ));
        }
        let plan_bytes = encode_planned_owner_admission(plan).map_err(control_codec_error)?;
        if plan_bytes.len() > MAX_INLINE_PLAN_BYTES {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "inline immutable owner plan exceeds the wire budget",
            ));
        }
    }
    if attempt.state != StoreOwnerAttemptStateV4::Serving && attempt.serving_publication.is_some() {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "non-Serving attempt contains a Serving publication receipt",
        ));
    }
    if matches!(
        attempt.state,
        StoreOwnerAttemptStateV4::AdmissionIntent | StoreOwnerAttemptStateV4::OwnerPlanned
    ) && attempt.activated_root_fence.is_some()
    {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "pre-commit attempt contains a RootFence activation receipt",
        ));
    }
    match attempt.state {
        StoreOwnerAttemptStateV4::AdmissionIntent => {
            require_attempt_fields(attempt, false, false, false)?;
        }
        StoreOwnerAttemptStateV4::OwnerPlanned => {
            require_attempt_fields(attempt, true, true, false)?;
            let plan = attempt.plan.as_ref().unwrap();
            match attempt.receipt.as_ref() {
                Some(AttemptReceiptV4::Prepared { claim, sentinel }) => {
                    validate_prepared_receipt(plan, claim, sentinel)?;
                }
                _ => {
                    return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                        "OwnerPlanned attempt has no exact Prepared receipt",
                    ));
                }
            }
        }
        StoreOwnerAttemptStateV4::OwnerCommitted => {
            require_attempt_fields(attempt, true, true, false)?;
            validate_attempt_committed(attempt)?;
        }
        StoreOwnerAttemptStateV4::Releasing => {
            require_attempt_fields(attempt, true, true, true)?;
            let (_, committed_claim) = committed_attempt(head)?;
            validate_termination_claim(
                committed_claim,
                attempt.requested_termination.as_ref().unwrap(),
            )?;
        }
        StoreOwnerAttemptStateV4::Released => {
            if attempt.requested_termination.is_some() {
                return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                    "Released attempt retains a pending termination request",
                ));
            }
            match attempt.receipt.as_ref() {
                Some(AttemptReceiptV4::Rejected { claim }) => {
                    if attempt.plan.is_some() {
                        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                            "Rejected attempt retains an owner plan",
                        ));
                    }
                    validate_rejected_claim(intent, claim)?;
                }
                Some(AttemptReceiptV4::Aborted { claim }) => {
                    let plan =
                        attempt
                            .plan
                            .as_ref()
                            .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                                "Aborted attempt has no immutable owner plan",
                            ))?;
                    validate_aborted_claim(plan, claim)?;
                }
                Some(AttemptReceiptV4::Terminated {
                    committed_shard,
                    committed_claim,
                    shard,
                    claim,
                }) => {
                    let plan =
                        attempt
                            .plan
                            .as_ref()
                            .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                                "Terminated attempt has no immutable owner plan",
                            ))?;
                    validate_committed_receipt(plan, committed_shard, committed_claim)?;
                    validate_termination_claim(committed_claim, claim)?;
                    validate_terminal_record(plan, committed_shard, shard, claim)?;
                }
                _ => {
                    return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                        "non-empty Released attempt has no permanent terminal receipt",
                    ));
                }
            }
        }
        StoreOwnerAttemptStateV4::Serving => {
            if matches!(head.store_state, StoreOwnerStateV4::Prepared) {
                return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                    "Serving attempt belongs to a Prepared store",
                ));
            }
            require_attempt_fields(attempt, true, true, false)?;
            validate_attempt_committed(attempt)?;
            if attempt.serving_publication.is_none() || attempt.activated_root_fence.is_none() {
                return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                    "Serving attempt lacks exact control and RootFence receipts",
                ));
            }
            validate_serving_receipts(head)?;
        }
    }
    Ok(())
}

fn require_attempt_fields(
    attempt: &AttemptWireV4,
    plan: bool,
    receipt: bool,
    termination: bool,
) -> Result<(), StoreOwnerJournalErrorV4> {
    if attempt.plan.is_some() != plan
        || attempt.receipt.is_some() != receipt
        || attempt.requested_termination.is_some() != termination
    {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "attempt fields do not match the durable phase",
        ));
    }
    Ok(())
}

fn validate_attempt_committed(attempt: &AttemptWireV4) -> Result<(), StoreOwnerJournalErrorV4> {
    let plan = attempt.plan.as_ref().unwrap();
    match attempt.receipt.as_ref() {
        Some(AttemptReceiptV4::Committed { shard, claim }) => {
            validate_committed_receipt(plan, shard, claim)
        }
        _ => Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "committed attempt has no exact Committed receipt",
        )),
    }
}

fn validate_serving_receipts(head: &HeadWireV4) -> Result<(), StoreOwnerJournalErrorV4> {
    let attempt = &head.attempt;
    let plan = attempt.plan.as_ref().unwrap();
    let (committed_shard, committed_claim) = match attempt.receipt.as_ref() {
        Some(AttemptReceiptV4::Committed { shard, claim }) => (shard, claim),
        _ => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "Serving attempt has no exact Committed receipt",
            ));
        }
    };
    let publication = attempt.serving_publication.as_ref().unwrap();
    if &publication.claim != committed_claim {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "Serving publication claim",
        ));
    }
    validate_serving_record(plan, committed_shard, &publication.shard)
}

fn validate_root_fence_chain(head: &HeadWireV4) -> Result<(), StoreOwnerJournalErrorV4> {
    let attempt = &head.attempt;
    let plan = attempt
        .plan
        .as_ref()
        .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
            "RootFence activation receipt has no immutable owner plan",
        ))?;
    let installed =
        head.installed_root_fence
            .as_ref()
            .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                "RootFence activation has no terminal install receipt",
            ))?;
    let activated = attempt.activated_root_fence.as_ref().unwrap();
    installed.terminal.validate()?;
    activated.validate()?;
    let request_ids = attempt.request_ids.unwrap();
    if installed.attempt_sequence == attempt.sequence {
        if request_ids.install != Some(installed.terminal.command.request_id)
            || installed.plan_digest != *plan.digest().as_bytes()
            || activated.prior != installed.terminal.exact_next
        {
            return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                "initial RootFence install-to-activate chain",
            ));
        }
    } else if request_ids.install.is_some() {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "successor attempt unexpectedly carries a RootFence install request",
        ));
    }
    if activated.command.request_id != request_ids.activate {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "RootFence activate request id",
        ));
    }
    let StoreOwnerStateV4::Ready { frontier } = head.store_state else {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "Serving attempt belongs to a Prepared store",
        ));
    };
    if frontier != activated.exact_next {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "Ready frontier and RootFence activate terminal",
        ));
    }
    Ok(())
}

fn validate_root_pending_for_head(
    head: &HeadWireV4,
    pending: PendingProviderCommitV4,
) -> Result<(), StoreOwnerJournalErrorV4> {
    if head.attempt.state != StoreOwnerAttemptStateV4::OwnerCommitted {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "RootFence provider commit is pending outside OwnerCommitted",
        ));
    }
    let request_ids = head
        .attempt
        .request_ids
        .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
            "RootFence attempt request ids are absent",
        ))?;
    let FrontierPointV4::Acknowledged(prior) = pending.prior else {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "RootFence provider commit has no acknowledged prior",
        ));
    };
    let StoreOwnerStateV4::Ready { frontier } = head.store_state else {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "RootFence provider commit belongs to a Prepared store",
        ));
    };
    if frontier != prior {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "RootFence pending prior and exact Ready frontier",
        ));
    }
    match pending.purpose {
        ProviderCommitPurposeV4::RootFenceInstall(command) => {
            if head.installed_root_fence.is_some()
                || request_ids.install != Some(command.request_id)
            {
                return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                    "RootFence install request and store identity",
                ));
            }
        }
        ProviderCommitPurposeV4::RootFenceActivate(command) => {
            let installed = head.installed_root_fence.as_ref().ok_or(
                StoreOwnerJournalErrorV4::InvalidJournal(
                    "RootFence activate is pending before install is terminal",
                ),
            )?;
            if request_ids.activate != command.request_id || installed.terminal.exact_next != prior
            {
                return Err(StoreOwnerJournalErrorV4::BindingMismatch(
                    "RootFence activate request and install terminal",
                ));
            }
        }
        ProviderCommitPurposeV4::CanonicalGenesis(_) => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "canonical genesis entered RootFence validation",
            ));
        }
    }
    Ok(())
}

fn validate_serving_record(
    plan: &PlannedOwnerAdmissionV1,
    committed: &LogicalShardRecord,
    serving: &LogicalShardRecord,
) -> Result<(), StoreOwnerJournalErrorV4> {
    validate_committed_record(plan, committed)?;
    encode_logical_shard_record(serving).map_err(control_codec_error)?;
    if serving.logical_shard_id != committed.logical_shard_id
        || serving.owner != committed.owner
        || serving.owner_epoch != committed.owner_epoch
        || serving.owner_incarnation_id != committed.owner_incarnation_id
        || serving.lease_id != committed.lease_id
        || serving.endpoint != committed.endpoint
        || serving.state != LogicalShardState::Serving
    {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "Serving logical shard record",
        ));
    }
    if serving.checkpoint != committed.checkpoint
        || serving.log != committed.log
        || serving.durable_lsn != committed.durable_lsn
    {
        return Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
            "nokv-control exact recovery-descendant receipt",
        ));
    }
    Ok(())
}

fn validate_store_binding(binding: &StoreOwnerBindingV4) -> Result<(), StoreOwnerJournalErrorV4> {
    StoreUuidV4::from_bytes(binding.store_uuid.0)?;
    StoreOwnerRootBindingV4::new(
        binding.root.root_id,
        binding.root.layout_profile,
        binding.root.layout_generation,
        binding.root.partition_id,
        binding.root.logical_shard_id,
    )?;
    StoreOwnerPhysicalBindingV4::from_digests(
        binding.physical.canonical_journal_locator_digest,
        binding.physical.canonical_provider_locator_digest,
        binding.physical.held_directory_lock_identity_digest,
        binding.physical.reservation_binding_digest,
    )?;
    Ok(())
}

fn validate_genesis_binding(
    store: &StoreOwnerBindingV4,
    genesis: CanonicalGenesisBindingV4,
) -> Result<(), StoreOwnerJournalErrorV4> {
    CanonicalGenesisBindingV4::from_durable_parts(
        genesis.schema_digest,
        genesis.store_identity_digest,
        genesis.authority_digest,
        genesis.zero_user_state_digest,
    )?;
    if genesis.store_identity_digest != store_binding_digest(store)? {
        return Err(StoreOwnerJournalErrorV4::BindingMismatch(
            "canonical genesis store identity",
        ));
    }
    Ok(())
}

fn validate_canonical_genesis(
    frontier: AcknowledgedMetadataFrontier,
) -> Result<(), StoreOwnerJournalErrorV4> {
    validate_frontier(frontier)?;
    if frontier.write_sequence != 0
        || frontier.commit_version.get() != 1
        || frontier.recovery_lsn != 0
    {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "canonical genesis frontier is not exactly (write=0, commit=1, recovery=0)",
        ));
    }
    Ok(())
}

fn validate_root_fence_delta(
    prior: AcknowledgedMetadataFrontier,
    exact_next: AcknowledgedMetadataFrontier,
) -> Result<(), StoreOwnerJournalErrorV4> {
    let expected_write =
        prior
            .write_sequence
            .checked_add(1)
            .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                "RootFence write sequence is exhausted",
            ))?;
    if exact_next.write_sequence != expected_write
        || exact_next.commit_version != prior.commit_version
        || exact_next.recovery_lsn != prior.recovery_lsn
        || exact_next.chain_digest != prior.chain_digest
    {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "RootFence receipt does not have its exact authority-only frontier delta",
        ));
    }
    Ok(())
}

fn validate_frontier(
    frontier: AcknowledgedMetadataFrontier,
) -> Result<(), StoreOwnerJournalErrorV4> {
    require_nonzero(&frontier.chain_digest, "metadata recovery chain digest")
}

fn require_nonzero(bytes: &[u8], field: &'static str) -> Result<(), StoreOwnerJournalErrorV4> {
    if bytes.iter().all(|byte| *byte == 0) {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(field));
    }
    Ok(())
}

fn control_codec_error(_error: nokv_control::ControlError) -> StoreOwnerJournalErrorV4 {
    StoreOwnerJournalErrorV4::InvalidJournal("nested control value is invalid or non-canonical")
}

fn control_value_error(_error: nokv_control::ControlError) -> StoreOwnerJournalErrorV4 {
    StoreOwnerJournalErrorV4::InvalidJournal("nested control value has an invalid phase")
}

fn encode_head(head: &HeadWireV4) -> Result<Vec<u8>, StoreOwnerJournalErrorV4> {
    let mut encoder = Encoder::default();
    encoder.fixed(WIRE_MAGIC);
    encoder.u8(WIRE_VERSION);
    encoder.u64(head.generation);
    encode_store_binding(&mut encoder, &head.binding);
    encode_store_state(&mut encoder, head.store_state);
    encoder.option(head.canonical_genesis, encode_genesis_receipt);
    encoder.option(head.installed_root_fence, encode_installed_root_fence);
    encode_attempt(&mut encoder, &head.attempt)?;
    encoder.option(head.pending_provider_commit, encode_pending_provider_commit);
    encoder.u32(u32::try_from(head.used_request_ids.len()).map_err(|_| {
        StoreOwnerJournalErrorV4::InvalidJournal("request-id history length overflows u32")
    })?);
    for request in &head.used_request_ids {
        encoder.fixed(request.as_bytes());
    }
    if encoder.bytes.len().saturating_add(SHA256_BYTES) > MAX_WIRE_BYTES {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "wire exceeds the v4 size limit",
        ));
    }
    let digest = integrity_digest(&encoder.bytes);
    encoder.fixed(&digest);
    Ok(encoder.bytes)
}

fn decode_head(bytes: &[u8]) -> Result<HeadWireV4, StoreOwnerJournalErrorV4> {
    if bytes.len() > MAX_WIRE_BYTES {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "wire exceeds the v4 size limit",
        ));
    }
    if bytes.len() < WIRE_MAGIC.len() + 1 + SHA256_BYTES {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "wire is truncated",
        ));
    }
    let payload_len = bytes.len() - SHA256_BYTES;
    let (payload, stored_digest) = bytes.split_at(payload_len);
    if integrity_digest(payload).as_slice() != stored_digest {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "wire integrity digest does not match",
        ));
    }
    let mut decoder = Decoder::new(payload);
    if decoder.fixed::<16>()? != *WIRE_MAGIC {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "wire magic does not identify a v4 owner journal",
        ));
    }
    if decoder.u8()? != WIRE_VERSION {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "wire version is not v4",
        ));
    }
    let generation = decoder.u64()?;
    let binding = decode_store_binding(&mut decoder)?;
    let store_state = decode_store_state(&mut decoder)?;
    let canonical_genesis = decoder.option(decode_genesis_receipt)?;
    let installed_root_fence = decoder.option(decode_installed_root_fence)?;
    let attempt = decode_attempt(&mut decoder)?;
    let pending_provider_commit = decoder.option(decode_pending_provider_commit)?;
    let request_count = decoder.u32()? as usize;
    if request_count > MAX_USED_REQUEST_IDS {
        return Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "RootFence request-id history exceeds its bound",
        ));
    }
    let mut used_request_ids = Vec::with_capacity(request_count);
    for _ in 0..request_count {
        used_request_ids.push(RequestId::from_bytes(decoder.fixed()?));
    }
    decoder.finish()?;
    Ok(HeadWireV4 {
        generation,
        binding,
        store_state,
        canonical_genesis,
        installed_root_fence,
        attempt,
        pending_provider_commit,
        used_request_ids,
    })
}

fn encode_store_binding(encoder: &mut Encoder, binding: &StoreOwnerBindingV4) {
    encoder.fixed(&binding.store_uuid.0);
    encoder.fixed(binding.root.root_id.as_bytes());
    encoder.u8(binding.root.layout_profile.into());
    encoder.u64(binding.root.layout_generation.get());
    encoder.fixed(binding.root.partition_id.as_bytes());
    encoder.fixed(binding.root.logical_shard_id.as_bytes());
    encoder.fixed(&binding.physical.canonical_journal_locator_digest);
    encoder.fixed(&binding.physical.canonical_provider_locator_digest);
    encoder.fixed(&binding.physical.held_directory_lock_identity_digest);
    encoder.fixed(&binding.physical.reservation_binding_digest.0);
}

fn decode_store_binding(
    decoder: &mut Decoder<'_>,
) -> Result<StoreOwnerBindingV4, StoreOwnerJournalErrorV4> {
    let store_uuid = StoreUuidV4::from_bytes(decoder.fixed()?)?;
    let root_id = RootId::from_bytes(decoder.fixed()?);
    let layout_profile = RootLayoutProfile::try_from(decoder.u8()?).map_err(|_| {
        StoreOwnerJournalErrorV4::InvalidJournal("root layout profile discriminant is unknown")
    })?;
    let layout_generation = RootLayoutGeneration::new(decoder.u64()?)
        .map_err(|_| StoreOwnerJournalErrorV4::InvalidJournal("root layout generation is zero"))?;
    let partition_id = RootPartitionId::from_bytes(decoder.fixed()?);
    let logical_shard_id = LogicalShardId::from_bytes(decoder.fixed()?);
    let root = StoreOwnerRootBindingV4::new(
        root_id,
        layout_profile,
        layout_generation,
        partition_id,
        logical_shard_id,
    )?;
    let physical = StoreOwnerPhysicalBindingV4::from_digests(
        decoder.fixed()?,
        decoder.fixed()?,
        decoder.fixed()?,
        StoreReservationBindingDigestV4::from_bytes(decoder.fixed()?)?,
    )?;
    StoreOwnerBindingV4::new(store_uuid, root, physical)
}

fn encode_store_state(encoder: &mut Encoder, state: StoreOwnerStateV4) {
    match state {
        StoreOwnerStateV4::Prepared => encoder.u8(1),
        StoreOwnerStateV4::Ready { frontier } => {
            encoder.u8(2);
            encode_frontier(encoder, frontier);
        }
    }
}

fn decode_store_state(
    decoder: &mut Decoder<'_>,
) -> Result<StoreOwnerStateV4, StoreOwnerJournalErrorV4> {
    match decoder.u8()? {
        1 => Ok(StoreOwnerStateV4::Prepared),
        2 => Ok(StoreOwnerStateV4::Ready {
            frontier: decode_frontier(decoder)?,
        }),
        _ => Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "store-state discriminant is unknown",
        )),
    }
}

fn encode_genesis_binding(encoder: &mut Encoder, binding: CanonicalGenesisBindingV4) {
    encoder.fixed(&binding.schema_digest);
    encoder.fixed(&binding.store_identity_digest);
    encoder.fixed(&binding.authority_digest);
    encoder.fixed(&binding.zero_user_state_digest);
}

fn decode_genesis_binding(
    decoder: &mut Decoder<'_>,
) -> Result<CanonicalGenesisBindingV4, StoreOwnerJournalErrorV4> {
    CanonicalGenesisBindingV4::from_durable_parts(
        decoder.fixed()?,
        decoder.fixed()?,
        decoder.fixed()?,
        decoder.fixed()?,
    )
}

fn encode_genesis_receipt(encoder: &mut Encoder, receipt: CanonicalGenesisReceiptV4) {
    encode_genesis_binding(encoder, receipt.binding);
    encode_frontier(encoder, receipt.exact_next);
}

fn decode_genesis_receipt(
    decoder: &mut Decoder<'_>,
) -> Result<CanonicalGenesisReceiptV4, StoreOwnerJournalErrorV4> {
    Ok(CanonicalGenesisReceiptV4 {
        binding: decode_genesis_binding(decoder)?,
        exact_next: decode_frontier(decoder)?,
    })
}

fn encode_command_identity(encoder: &mut Encoder, command: ProviderCommandIdentityV4) {
    encoder.fixed(command.request_id.as_bytes());
    encoder.fixed(command.command_digest.as_bytes());
}

fn decode_command_identity(
    decoder: &mut Decoder<'_>,
) -> Result<ProviderCommandIdentityV4, StoreOwnerJournalErrorV4> {
    ProviderCommandIdentityV4::new(
        RequestId::from_bytes(decoder.fixed()?),
        CommandDigest::from_bytes(decoder.fixed()?),
    )
}

fn encode_pending_provider_commit(encoder: &mut Encoder, pending: PendingProviderCommitV4) {
    match pending.purpose {
        ProviderCommitPurposeV4::CanonicalGenesis(binding) => {
            encoder.u8(1);
            encode_genesis_binding(encoder, binding);
        }
        ProviderCommitPurposeV4::RootFenceInstall(command) => {
            encoder.u8(2);
            encode_command_identity(encoder, command);
        }
        ProviderCommitPurposeV4::RootFenceActivate(command) => {
            encoder.u8(3);
            encode_command_identity(encoder, command);
        }
    }
    encode_frontier_point(encoder, pending.prior);
    encode_frontier(encoder, pending.exact_next);
}

fn decode_pending_provider_commit(
    decoder: &mut Decoder<'_>,
) -> Result<PendingProviderCommitV4, StoreOwnerJournalErrorV4> {
    let purpose = match decoder.u8()? {
        1 => ProviderCommitPurposeV4::CanonicalGenesis(decode_genesis_binding(decoder)?),
        2 => ProviderCommitPurposeV4::RootFenceInstall(decode_command_identity(decoder)?),
        3 => ProviderCommitPurposeV4::RootFenceActivate(decode_command_identity(decoder)?),
        _ => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "provider commit purpose discriminant is unknown",
            ));
        }
    };
    let pending = PendingProviderCommitV4 {
        purpose,
        prior: decode_frontier_point(decoder)?,
        exact_next: decode_frontier(decoder)?,
    };
    pending.validate()?;
    Ok(pending)
}

fn encode_frontier_point(encoder: &mut Encoder, point: FrontierPointV4) {
    match point {
        FrontierPointV4::Absent => encoder.u8(1),
        FrontierPointV4::Acknowledged(frontier) => {
            encoder.u8(2);
            encode_frontier(encoder, frontier);
        }
    }
}

fn decode_frontier_point(
    decoder: &mut Decoder<'_>,
) -> Result<FrontierPointV4, StoreOwnerJournalErrorV4> {
    match decoder.u8()? {
        1 => Ok(FrontierPointV4::Absent),
        2 => Ok(FrontierPointV4::Acknowledged(decode_frontier(decoder)?)),
        _ => Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "frontier-point discriminant is unknown",
        )),
    }
}

fn encode_frontier(encoder: &mut Encoder, frontier: AcknowledgedMetadataFrontier) {
    encoder.u64(frontier.write_sequence);
    encoder.u64(frontier.commit_version.get());
    encoder.u64(frontier.recovery_lsn);
    encoder.fixed(&frontier.chain_digest);
}

fn decode_frontier(
    decoder: &mut Decoder<'_>,
) -> Result<AcknowledgedMetadataFrontier, StoreOwnerJournalErrorV4> {
    let frontier = AcknowledgedMetadataFrontier {
        write_sequence: decoder.u64()?,
        commit_version: CommitVersion::new(decoder.u64()?).map_err(|_| {
            StoreOwnerJournalErrorV4::InvalidJournal("metadata commit version is zero")
        })?,
        recovery_lsn: decoder.u64()?,
        chain_digest: decoder.fixed()?,
    };
    validate_frontier(frontier)?;
    Ok(frontier)
}

fn encode_terminal_command(encoder: &mut Encoder, terminal: TerminalProviderCommandV4) {
    encode_command_identity(encoder, terminal.command);
    encode_frontier(encoder, terminal.prior);
    encode_frontier(encoder, terminal.exact_next);
}

fn decode_terminal_command(
    decoder: &mut Decoder<'_>,
) -> Result<TerminalProviderCommandV4, StoreOwnerJournalErrorV4> {
    let terminal = TerminalProviderCommandV4 {
        command: decode_command_identity(decoder)?,
        prior: decode_frontier(decoder)?,
        exact_next: decode_frontier(decoder)?,
    };
    terminal.validate()?;
    Ok(terminal)
}

fn encode_installed_root_fence(encoder: &mut Encoder, installed: InstalledRootFenceReceiptV4) {
    encoder.u64(installed.attempt_sequence);
    encoder.fixed(&installed.plan_digest);
    encode_terminal_command(encoder, installed.terminal);
}

fn decode_installed_root_fence(
    decoder: &mut Decoder<'_>,
) -> Result<InstalledRootFenceReceiptV4, StoreOwnerJournalErrorV4> {
    Ok(InstalledRootFenceReceiptV4 {
        attempt_sequence: decoder.u64()?,
        plan_digest: decoder.fixed()?,
        terminal: decode_terminal_command(decoder)?,
    })
}

fn encode_attempt(
    encoder: &mut Encoder,
    attempt: &AttemptWireV4,
) -> Result<(), StoreOwnerJournalErrorV4> {
    encoder.u64(attempt.sequence);
    encoder.u8(match attempt.state {
        StoreOwnerAttemptStateV4::Released => 1,
        StoreOwnerAttemptStateV4::AdmissionIntent => 2,
        StoreOwnerAttemptStateV4::OwnerPlanned => 3,
        StoreOwnerAttemptStateV4::OwnerCommitted => 4,
        StoreOwnerAttemptStateV4::Serving => 5,
        StoreOwnerAttemptStateV4::Releasing => 6,
    });
    encoder.option(attempt.request_ids, |encoder, ids| {
        encoder.option(ids.install, |encoder, request| {
            encoder.fixed(request.as_bytes());
        });
        encoder.fixed(ids.activate.as_bytes());
    });
    encode_control_option(
        encoder,
        attempt.intent.as_ref(),
        encode_owner_admission_intent,
    )?;
    encode_control_option(
        encoder,
        attempt.plan.as_ref(),
        encode_planned_owner_admission,
    )?;
    encode_attempt_receipt(encoder, attempt.receipt.as_ref())?;
    match attempt.serving_publication.as_ref() {
        Some(receipt) => {
            encoder.u8(1);
            encode_control(encoder, &receipt.shard, encode_logical_shard_record)?;
            encode_control(encoder, &receipt.claim, encode_owner_admission_claim)?;
        }
        None => encoder.u8(0),
    }
    encode_control_option(
        encoder,
        attempt.requested_termination.as_ref(),
        encode_owner_admission_claim,
    )?;
    encoder.option(attempt.activated_root_fence, encode_terminal_command);
    Ok(())
}

fn decode_attempt(decoder: &mut Decoder<'_>) -> Result<AttemptWireV4, StoreOwnerJournalErrorV4> {
    let sequence = decoder.u64()?;
    let state = match decoder.u8()? {
        1 => StoreOwnerAttemptStateV4::Released,
        2 => StoreOwnerAttemptStateV4::AdmissionIntent,
        3 => StoreOwnerAttemptStateV4::OwnerPlanned,
        4 => StoreOwnerAttemptStateV4::OwnerCommitted,
        5 => StoreOwnerAttemptStateV4::Serving,
        6 => StoreOwnerAttemptStateV4::Releasing,
        _ => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "attempt-state discriminant is unknown",
            ));
        }
    };
    let request_ids = decoder.option(|decoder| {
        let install = decoder.option(|decoder| Ok(RequestId::from_bytes(decoder.fixed()?)))?;
        RootFenceAttemptRequestIdsV4::new(install, RequestId::from_bytes(decoder.fixed()?))
    })?;
    let intent = decode_control_option(decoder, decode_owner_admission_intent)?;
    let plan = decode_control_option(decoder, decode_planned_owner_admission)?;
    let receipt = decode_attempt_receipt(decoder)?;
    let serving_publication = match decoder.u8()? {
        0 => None,
        1 => Some(ServingPublicationReceiptV4 {
            shard: decode_control(decoder, decode_logical_shard_record)?,
            claim: decode_control(decoder, decode_owner_admission_claim)?,
        }),
        _ => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "Serving-publication option discriminant is unknown",
            ));
        }
    };
    let requested_termination = decode_control_option(decoder, decode_owner_admission_claim)?;
    let activated_root_fence = decoder.option(decode_terminal_command)?;
    Ok(AttemptWireV4 {
        sequence,
        state,
        request_ids,
        intent,
        plan,
        receipt,
        serving_publication,
        requested_termination,
        activated_root_fence,
    })
}

fn encode_attempt_receipt(
    encoder: &mut Encoder,
    receipt: Option<&AttemptReceiptV4>,
) -> Result<(), StoreOwnerJournalErrorV4> {
    let Some(receipt) = receipt else {
        encoder.u8(0);
        return Ok(());
    };
    match receipt {
        AttemptReceiptV4::Prepared { claim, sentinel } => {
            encoder.u8(1);
            encode_control(encoder, claim, encode_owner_admission_claim)?;
            encode_control(encoder, sentinel, encode_owner_admission_plan_sentinel)?;
        }
        AttemptReceiptV4::Committed { shard, claim } => {
            encoder.u8(2);
            encode_control(encoder, shard, encode_logical_shard_record)?;
            encode_control(encoder, claim, encode_owner_admission_claim)?;
        }
        AttemptReceiptV4::Rejected { claim } => {
            encoder.u8(3);
            encode_control(encoder, claim, encode_owner_admission_claim)?;
        }
        AttemptReceiptV4::Aborted { claim } => {
            encoder.u8(4);
            encode_control(encoder, claim, encode_owner_admission_claim)?;
        }
        AttemptReceiptV4::Terminated {
            committed_shard,
            committed_claim,
            shard,
            claim,
        } => {
            encoder.u8(5);
            encode_control(encoder, committed_shard, encode_logical_shard_record)?;
            encode_control(
                encoder,
                committed_claim.as_ref(),
                encode_owner_admission_claim,
            )?;
            encode_control(encoder, shard, encode_logical_shard_record)?;
            encode_control(encoder, claim.as_ref(), encode_owner_admission_claim)?;
        }
    }
    Ok(())
}

fn decode_attempt_receipt(
    decoder: &mut Decoder<'_>,
) -> Result<Option<AttemptReceiptV4>, StoreOwnerJournalErrorV4> {
    Ok(match decoder.u8()? {
        0 => None,
        1 => Some(AttemptReceiptV4::Prepared {
            claim: decode_control(decoder, decode_owner_admission_claim)?,
            sentinel: decode_control(decoder, decode_owner_admission_plan_sentinel)?,
        }),
        2 => Some(AttemptReceiptV4::Committed {
            shard: decode_control(decoder, decode_logical_shard_record)?,
            claim: decode_control(decoder, decode_owner_admission_claim)?,
        }),
        3 => Some(AttemptReceiptV4::Rejected {
            claim: decode_control(decoder, decode_owner_admission_claim)?,
        }),
        4 => Some(AttemptReceiptV4::Aborted {
            claim: decode_control(decoder, decode_owner_admission_claim)?,
        }),
        5 => Some(AttemptReceiptV4::Terminated {
            committed_shard: decode_control(decoder, decode_logical_shard_record)?,
            committed_claim: Box::new(decode_control(decoder, decode_owner_admission_claim)?),
            shard: decode_control(decoder, decode_logical_shard_record)?,
            claim: Box::new(decode_control(decoder, decode_owner_admission_claim)?),
        }),
        _ => {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "attempt-receipt discriminant is unknown",
            ));
        }
    })
}

fn encode_control_option<T>(
    encoder: &mut Encoder,
    value: Option<&T>,
    encode: impl Fn(&T) -> Result<Vec<u8>, nokv_control::ControlError>,
) -> Result<(), StoreOwnerJournalErrorV4> {
    match value {
        Some(value) => {
            encoder.u8(1);
            encode_control(encoder, value, encode)
        }
        None => {
            encoder.u8(0);
            Ok(())
        }
    }
}

fn decode_control_option<T>(
    decoder: &mut Decoder<'_>,
    decode: impl Fn(&[u8]) -> Result<T, nokv_control::ControlError>,
) -> Result<Option<T>, StoreOwnerJournalErrorV4> {
    match decoder.u8()? {
        0 => Ok(None),
        1 => decode_control(decoder, decode).map(Some),
        _ => Err(StoreOwnerJournalErrorV4::InvalidJournal(
            "nested control option discriminant is unknown",
        )),
    }
}

fn encode_control<T>(
    encoder: &mut Encoder,
    value: &T,
    encode: impl Fn(&T) -> Result<Vec<u8>, nokv_control::ControlError>,
) -> Result<(), StoreOwnerJournalErrorV4> {
    let bytes = encode(value).map_err(control_codec_error)?;
    encoder.bytes(&bytes)
}

fn decode_control<T>(
    decoder: &mut Decoder<'_>,
    decode: impl Fn(&[u8]) -> Result<T, nokv_control::ControlError>,
) -> Result<T, StoreOwnerJournalErrorV4> {
    let bytes = decoder.bytes(MAX_WIRE_BYTES)?;
    let value = decode(bytes).map_err(control_codec_error)?;
    Ok(value)
}

fn store_binding_digest(
    binding: &StoreOwnerBindingV4,
) -> Result<[u8; SHA256_BYTES], StoreOwnerJournalErrorV4> {
    validate_store_binding(binding)?;
    let mut encoder = Encoder::default();
    encode_store_binding(&mut encoder, binding);
    Ok(domain_digest(BINDING_DIGEST_DOMAIN, &encoder.bytes))
}

fn pending_receipt_digest(
    pending: PendingProviderCommitV4,
) -> Result<[u8; SHA256_BYTES], StoreOwnerJournalErrorV4> {
    pending.validate()?;
    let mut encoder = Encoder::default();
    encode_pending_provider_commit(&mut encoder, pending);
    Ok(domain_digest(PENDING_DIGEST_DOMAIN, &encoder.bytes))
}

fn integrity_digest(payload: &[u8]) -> [u8; SHA256_BYTES] {
    domain_digest(WIRE_DIGEST_DOMAIN, payload)
}

fn domain_digest(domain: &[u8], payload: &[u8]) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(payload);
    hasher.finalize().into()
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn fixed(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), StoreOwnerJournalErrorV4> {
        self.u32(u32::try_from(value.len()).map_err(|_| {
            StoreOwnerJournalErrorV4::InvalidJournal("nested value length overflows u32")
        })?);
        self.fixed(value);
        Ok(())
    }

    fn option<T>(&mut self, value: Option<T>, encode: impl FnOnce(&mut Self, T)) {
        match value {
            Some(value) => {
                self.u8(1);
                encode(self, value);
            }
            None => self.u8(0),
        }
    }
}

struct Decoder<'wire> {
    bytes: &'wire [u8],
    offset: usize,
}

impl<'wire> Decoder<'wire> {
    const fn new(bytes: &'wire [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Result<u8, StoreOwnerJournalErrorV4> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, StoreOwnerJournalErrorV4> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }

    fn u64(&mut self) -> Result<u64, StoreOwnerJournalErrorV4> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], StoreOwnerJournalErrorV4> {
        self.take(N)?
            .try_into()
            .map_err(|_| StoreOwnerJournalErrorV4::InvalidJournal("wire is truncated"))
    }

    fn bytes(&mut self, max: usize) -> Result<&'wire [u8], StoreOwnerJournalErrorV4> {
        let length = self.u32()? as usize;
        if length > max {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "nested value exceeds its size limit",
            ));
        }
        self.take(length)
    }

    fn option<T>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, StoreOwnerJournalErrorV4>,
    ) -> Result<Option<T>, StoreOwnerJournalErrorV4> {
        match self.u8()? {
            0 => Ok(None),
            1 => decode(self).map(Some),
            _ => Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "option discriminant is unknown",
            )),
        }
    }

    fn take(&mut self, length: usize) -> Result<&'wire [u8], StoreOwnerJournalErrorV4> {
        let end =
            self.offset
                .checked_add(length)
                .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                    "wire offset overflows",
                ))?;
        let value =
            self.bytes
                .get(self.offset..end)
                .ok_or(StoreOwnerJournalErrorV4::InvalidJournal(
                    "wire is truncated",
                ))?;
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), StoreOwnerJournalErrorV4> {
        if self.offset != self.bytes.len() {
            return Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "wire has trailing bytes",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_control::{
        CheckpointRef, ConsistencyDomainId, LogicalShardLease, MetadataAuthorityBinding,
        MetadataAuthorityGeneration, MetadataAuthorityId, MetadataAuthorityRecord,
        MetadataAuthorityRevision, MetadataContractDigest, MetadataProviderProfileId, NodeId,
        OwnerAdmissionAbortReasonV1, OwnerAdmissionRejectionReasonV1,
        OwnerRuntimeReservationDigest, OwnerServingAdmission, PlacementGeneration, RootPlacement,
        RootPlacementLifecycle,
    };
    use nokv_types::OwnerIncarnationId;
    #[cfg(unix)]
    use std::fs;

    fn binding(reservation_binding: u8) -> StoreOwnerBindingV4 {
        let root = StoreOwnerRootBindingV4::new(
            RootId::from_bytes([1; 16]),
            RootLayoutProfile::SingleShardRoot,
            RootLayoutGeneration::new(1).unwrap(),
            RootPartitionId::SINGLE_SHARD,
            LogicalShardId::from_bytes([2; 16]),
        )
        .unwrap();
        let physical = StoreOwnerPhysicalBindingV4::from_digests(
            [3; SHA256_BYTES],
            [4; SHA256_BYTES],
            [5; SHA256_BYTES],
            StoreReservationBindingDigestV4::from_bytes([reservation_binding; SHA256_BYTES])
                .unwrap(),
        )
        .unwrap();
        StoreOwnerBindingV4::new(StoreUuidV4::from_bytes([6; 16]).unwrap(), root, physical).unwrap()
    }

    fn exclusive(
        binding: &StoreOwnerBindingV4,
    ) -> StoreOwnerJournalExclusiveReservationV4<'static> {
        StoreOwnerJournalExclusiveReservationV4 {
            store_binding_digest: store_binding_digest(binding).unwrap(),
            authority: StoreOwnerJournalAuthorityProofV4::Characterization(PhantomData),
            _private: (),
        }
    }

    fn request_ids(install: u8, activate: u8) -> RootFenceAttemptRequestIdsV4 {
        RootFenceAttemptRequestIdsV4::new(
            Some(RequestId::from_bytes([install; 16])),
            RequestId::from_bytes([activate; 16]),
        )
        .unwrap()
    }

    fn admission(placement_generation: u64, authority_generation: u64) -> OwnerServingAdmission {
        let logical_shard_id = LogicalShardId::from_bytes([2; 16]);
        let placement = RootPlacement {
            root_id: RootId::from_bytes([1; 16]),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id,
            placement_generation: PlacementGeneration::new(placement_generation).unwrap(),
            lifecycle: RootPlacementLifecycle::Active,
        };
        let authority = MetadataAuthorityRecord {
            logical_shard_id,
            record_revision: MetadataAuthorityRevision::new(authority_generation).unwrap(),
            authority_generation: MetadataAuthorityGeneration::new(authority_generation).unwrap(),
            active: MetadataAuthorityBinding {
                authority_id: MetadataAuthorityId::from_bytes([authority_generation as u8; 16]),
                provider_profile_id: MetadataProviderProfileId::new(format!(
                    "provider-{authority_generation}"
                ))
                .unwrap(),
                profile_fingerprint: [7; SHA256_BYTES],
                consistency_domain_id: ConsistencyDomainId::from_bytes([8; 16]),
                contract_digest: MetadataContractDigest::from_bytes([9; SHA256_BYTES]),
            },
            migration: None,
        };
        OwnerServingAdmission::stable(placement, authority).unwrap()
    }

    fn fresh_plan(
        endpoint: String,
        runtime_reservation: u8,
        incarnation: u8,
    ) -> PlannedOwnerAdmissionV1 {
        let admission = admission(2, 1);
        let intent = OwnerAdmissionIntentV1::fresh(
            admission.clone(),
            LogicalShardRecord::unassigned(LogicalShardId::from_bytes([2; 16])),
            NodeId::new("node-a").unwrap(),
            OwnerIncarnationId::from_bytes([incarnation; 16]),
            endpoint,
            OwnerRuntimeReservationDigest::from_bytes([runtime_reservation; SHA256_BYTES]).unwrap(),
        )
        .unwrap();
        plan_for_intent(intent, 100 + u64::from(incarnation))
    }

    fn plan_for_intent(intent: OwnerAdmissionIntentV1, lease_id: u64) -> PlannedOwnerAdmissionV1 {
        let lease = LogicalShardLease {
            logical_shard_id: intent.logical_shard_id(),
            owner: intent.owner().clone(),
            owner_epoch: intent.planned_epoch(),
            owner_incarnation_id: intent.owner_incarnation_id(),
            lease_id,
            authority: intent.admission().authority().fence(),
        };
        PlannedOwnerAdmissionV1::new(intent, lease).unwrap()
    }

    fn successor_plan(
        released: LogicalShardRecord,
        predecessor: OwnerAdmissionClaimV1,
    ) -> PlannedOwnerAdmissionV1 {
        let admission = admission(4, 2);
        let intent = OwnerAdmissionIntentV1::successor(
            admission.clone(),
            released,
            predecessor,
            NodeId::new("node-b").unwrap(),
            OwnerIncarnationId::from_bytes([22; 16]),
            "node-b:7000".to_owned(),
            OwnerRuntimeReservationDigest::from_bytes([99; SHA256_BYTES]).unwrap(),
        )
        .unwrap();
        plan_for_intent(intent, 222)
    }

    fn committed_record(plan: &PlannedOwnerAdmissionV1) -> LogicalShardRecord {
        let mut record = plan.intent().expected_unowned_shard().clone();
        record.owner = Some(plan.intent().owner().clone());
        record.owner_epoch = Some(plan.lease().owner_epoch);
        record.owner_incarnation_id = Some(plan.lease().owner_incarnation_id);
        record.lease_id = plan.lease().lease_id;
        record.state = LogicalShardState::Recovering;
        record.endpoint = Some(plan.intent().endpoint().to_owned());
        record
    }

    fn terminal_record(committed: &LogicalShardRecord) -> LogicalShardRecord {
        let mut record = committed.clone();
        record.owner = None;
        record.lease_id = 0;
        record.state = LogicalShardState::Unassigned;
        record.endpoint = None;
        record
    }

    fn frontier(
        write_sequence: u64,
        commit_version: u64,
        recovery_lsn: u64,
        chain: u8,
    ) -> AcknowledgedMetadataFrontier {
        AcknowledgedMetadataFrontier {
            write_sequence,
            commit_version: CommitVersion::new(commit_version).unwrap(),
            recovery_lsn,
            chain_digest: [chain; SHA256_BYTES],
        }
    }

    fn genesis_binding(binding: &StoreOwnerBindingV4) -> CanonicalGenesisBindingV4 {
        CanonicalGenesisBindingV4::new(
            [31; SHA256_BYTES],
            binding,
            [32; SHA256_BYTES],
            [33; SHA256_BYTES],
        )
        .unwrap()
    }

    fn verified(
        binding: &StoreOwnerBindingV4,
        pending: PendingProviderCommitV4,
        observed: FrontierPointV4,
        outcome: VerifiedProviderCommitOutcomeV4,
    ) -> VerifiedProviderCommitReceiptV4<'static> {
        VerifiedProviderCommitReceiptV4 {
            store_binding_digest: store_binding_digest(binding).unwrap(),
            pending_receipt_digest: pending_receipt_digest(pending).unwrap(),
            observed,
            outcome,
            _verified: PhantomData,
            _private: (),
        }
    }

    fn committed_oracle() -> (
        StoreOwnerBindingV4,
        StoreOwnerJournalExclusiveReservationV4<'static>,
        StoreOwnerJournalOracleV4,
        PlannedOwnerAdmissionV1,
        LogicalShardRecord,
        OwnerAdmissionClaimV1,
    ) {
        let binding = binding(13);
        let exclusive = exclusive(&binding);
        let mut oracle = StoreOwnerJournalOracleV4::new(binding.clone(), &exclusive).unwrap();
        let plan = fresh_plan("node-a:7000".to_owned(), 42, 11);
        oracle
            .begin_admission_intent(&exclusive, plan.intent().clone(), request_ids(10, 20))
            .unwrap();
        let prepared = OwnerAdmissionClaimV1::prepared(&plan).unwrap();
        oracle
            .record_owner_plan(
                &exclusive,
                plan.clone(),
                prepared.clone(),
                OwnerAdmissionPlanSentinelV1::for_plan(&plan),
            )
            .unwrap();
        let committed = prepared.commit().unwrap();
        let shard = committed_record(&plan);
        oracle
            .record_owner_committed(&exclusive, shard.clone(), committed.clone())
            .unwrap();
        (binding, exclusive, oracle, plan, shard, committed)
    }

    #[test]
    fn v4_wire_roundtrips_and_rejects_v3_tamper_size_and_illegal_phase() {
        let binding = binding(13);
        let exclusive = exclusive(&binding);
        let oracle = StoreOwnerJournalOracleV4::new(binding.clone(), &exclusive).unwrap();
        let bytes = oracle.encode().unwrap();
        let golden_digest: [u8; SHA256_BYTES] = Sha256::digest(&bytes).into();
        assert_eq!(bytes.len(), 282);
        assert_eq!(
            golden_digest,
            [
                141, 202, 213, 169, 98, 174, 11, 34, 155, 182, 50, 179, 241, 142, 133, 32, 109,
                254, 204, 15, 55, 216, 122, 78, 49, 234, 246, 64, 194, 190, 231, 58,
            ]
        );
        assert_eq!(
            StoreOwnerJournalOracleV4::decode(&bytes, &binding)
                .unwrap()
                .encode()
                .unwrap(),
            bytes
        );

        let mut tampered = bytes.clone();
        tampered[40] ^= 1;
        assert!(matches!(
            StoreOwnerJournalOracleV4::decode(&tampered, &binding),
            Err(StoreOwnerJournalErrorV4::InvalidJournal(_))
        ));

        let mut v3 = bytes.clone();
        v3[WIRE_MAGIC.len()] = 3;
        let digest_offset = v3.len() - SHA256_BYTES;
        let digest = integrity_digest(&v3[..digest_offset]);
        v3[digest_offset..].copy_from_slice(&digest);
        assert!(matches!(
            StoreOwnerJournalOracleV4::decode(&v3, &binding),
            Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "wire version is not v4"
            ))
        ));
        assert!(matches!(
            StoreOwnerJournalOracleV4::decode(&vec![0; MAX_WIRE_BYTES + 1], &binding),
            Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "wire exceeds the v4 size limit"
            ))
        ));

        let (_, _, mut committed, _, _, _) = committed_oracle();
        committed.head.attempt.state = StoreOwnerAttemptStateV4::OwnerPlanned;
        assert!(matches!(
            committed.encode(),
            Err(StoreOwnerJournalErrorV4::InvalidJournal(_))
        ));
    }

    #[test]
    fn canonical_genesis_requires_an_exact_bound_provider_receipt() {
        let binding = binding(13);
        let exclusive = exclusive(&binding);
        let mut oracle = StoreOwnerJournalOracleV4::new(binding.clone(), &exclusive).unwrap();
        let genesis = frontier(0, 1, 0, 41);
        let pending =
            PendingProviderCommitV4::canonical_genesis(genesis, genesis_binding(&binding)).unwrap();
        oracle.begin_provider_commit(&exclusive, pending).unwrap();

        let reopened =
            StoreOwnerJournalOracleV4::decode(&oracle.encode().unwrap(), &binding).unwrap();
        assert_eq!(reopened.pending_provider_commit(), Some(pending));

        let not_applied = verified(
            &binding,
            pending,
            FrontierPointV4::Absent,
            VerifiedProviderCommitOutcomeV4::NotAppliedAtPrior,
        );
        oracle
            .reconcile_pending_provider_commit(&exclusive, &not_applied)
            .unwrap();
        assert_eq!(oracle.pending_provider_commit(), Some(pending));

        let forged = verified(
            &binding,
            pending,
            FrontierPointV4::Acknowledged(frontier(0, 1, 0, 42)),
            VerifiedProviderCommitOutcomeV4::TerminalAtExactNext,
        );
        assert_eq!(
            oracle.reconcile_pending_provider_commit(&exclusive, &forged),
            Err(StoreOwnerJournalErrorV4::PendingFrontierMismatch)
        );
        assert_eq!(oracle.pending_provider_commit(), Some(pending));

        let terminal = verified(
            &binding,
            pending,
            FrontierPointV4::Acknowledged(genesis),
            VerifiedProviderCommitOutcomeV4::TerminalAtExactNext,
        );
        oracle
            .reconcile_pending_provider_commit(&exclusive, &terminal)
            .unwrap();
        assert_eq!(
            oracle.store_state(),
            StoreOwnerStateV4::Ready { frontier: genesis }
        );
        assert_eq!(oracle.pending_provider_commit(), None);
        StoreOwnerJournalOracleV4::decode(&oracle.encode().unwrap(), &binding).unwrap();

        let mut blind_ready = StoreOwnerJournalOracleV4::new(binding.clone(), &exclusive).unwrap();
        blind_ready.head.store_state = StoreOwnerStateV4::Ready { frontier: genesis };
        assert!(matches!(
            blind_ready.encode(),
            Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "Ready store has no exact canonical genesis receipt"
            ))
        ));
    }

    #[test]
    fn release_retains_exact_plan_committed_and_terminated_receipts() {
        let (binding, exclusive, mut oracle, plan, committed_shard, committed_claim) =
            committed_oracle();
        let terminal_claim = committed_claim
            .clone()
            .terminate(OwnerAdmissionTerminationReasonV1::Released)
            .unwrap();
        let terminal_shard = terminal_record(&committed_shard);
        oracle
            .begin_releasing(&exclusive, terminal_claim.clone())
            .unwrap();
        let mut recovery_descendant = terminal_shard.clone();
        recovery_descendant.checkpoint = Some(CheckpointRef {
            object_key: "checkpoint-1".to_owned(),
            lsn: 1,
            image_bytes: 1,
            image_digest: "image-1".to_owned(),
            digest: "state-1".to_owned(),
        });
        recovery_descendant.durable_lsn = 1;
        assert_eq!(
            oracle.complete_released(&exclusive, recovery_descendant, terminal_claim.clone(),),
            Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
                "nokv-control exact recovery-descendant receipt"
            ))
        );
        assert_eq!(oracle.attempt_state(), StoreOwnerAttemptStateV4::Releasing);
        oracle
            .complete_released(&exclusive, terminal_shard.clone(), terminal_claim.clone())
            .unwrap();
        let released_generation = oracle.head.generation;
        oracle
            .complete_released(&exclusive, terminal_shard.clone(), terminal_claim.clone())
            .unwrap();
        assert_eq!(oracle.head.generation, released_generation);
        let reopened =
            StoreOwnerJournalOracleV4::decode(&oracle.encode().unwrap(), &binding).unwrap();
        assert_eq!(reopened.attempt_state(), StoreOwnerAttemptStateV4::Released);
        assert_eq!(reopened.head.attempt.plan.as_ref(), Some(&plan));
        match reopened.head.attempt.receipt.as_ref().unwrap() {
            AttemptReceiptV4::Terminated {
                committed_shard: persisted_committed_shard,
                committed_claim: persisted_committed_claim,
                shard,
                claim,
            } => {
                assert_eq!(persisted_committed_shard, &committed_shard);
                assert_eq!(persisted_committed_claim.as_ref(), &committed_claim);
                assert_eq!(shard, &terminal_shard);
                assert_eq!(claim.as_ref(), &terminal_claim);
                assert_eq!(
                    reopened.head.attempt.receipt.as_ref().unwrap().claim(),
                    claim.as_ref()
                );
            }
            receipt => panic!("unexpected terminal receipt: {receipt:?}"),
        }
    }

    #[test]
    fn physical_binding_is_stable_while_attempt_authority_and_reservation_change() {
        let (store_binding, reservation, mut oracle, _, committed_shard, committed_claim) =
            committed_oracle();
        let terminal_claim = committed_claim
            .clone()
            .terminate(OwnerAdmissionTerminationReasonV1::Released)
            .unwrap();
        let terminal_shard = terminal_record(&committed_shard);
        oracle
            .begin_releasing(&reservation, terminal_claim.clone())
            .unwrap();
        oracle
            .complete_released(&reservation, terminal_shard.clone(), terminal_claim.clone())
            .unwrap();
        let successor = successor_plan(terminal_shard, terminal_claim);

        let before = oracle.encode().unwrap();
        let wrong_physical_binding = binding(14);
        let wrong_exclusive = exclusive(&wrong_physical_binding);
        assert!(matches!(
            oracle.begin_admission_intent(
                &wrong_exclusive,
                successor.intent().clone(),
                request_ids(30, 40),
            ),
            Err(StoreOwnerJournalErrorV4::BindingMismatch(
                "exclusive store reservation"
            ))
        ));
        assert_eq!(oracle.encode().unwrap(), before);

        assert!(matches!(
            oracle.begin_admission_intent(
                &reservation,
                successor.intent().clone(),
                request_ids(10, 20),
            ),
            Err(StoreOwnerJournalErrorV4::InvalidJournal(
                "RootFence request id was already consumed by another attempt"
            ))
        ));
        assert_eq!(oracle.encode().unwrap(), before);

        oracle
            .begin_admission_intent(
                &reservation,
                successor.intent().clone(),
                request_ids(30, 40),
            )
            .unwrap();
        assert_eq!(
            oracle
                .head
                .attempt
                .intent
                .as_ref()
                .unwrap()
                .reservation_digest(),
            OwnerRuntimeReservationDigest::from_bytes([99; SHA256_BYTES]).unwrap()
        );
        assert_eq!(oracle.head.binding, store_binding);
    }

    #[test]
    fn root_fence_uses_request_and_command_digest_but_remains_typed_nq() {
        let binding = binding(13);
        let exclusive = exclusive(&binding);
        let mut oracle = StoreOwnerJournalOracleV4::new(binding, &exclusive).unwrap();
        let command = ProviderCommandIdentityV4::new(
            RequestId::from_bytes([51; 16]),
            CommandDigest::from_bytes([52; SHA256_BYTES]),
        )
        .unwrap();
        let prior = frontier(7, 5, 3, 53);
        let next = frontier(8, 5, 3, 53);
        let pending = PendingProviderCommitV4::root_fence_install(prior, next, command).unwrap();
        let terminal = TerminalProviderCommandV4::from_pending(pending).unwrap();
        assert_eq!(terminal.command, command);
        assert_eq!(
            oracle.begin_provider_commit(&exclusive, pending),
            Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
                "nokv-meta bootstrap owner-epoch and RootFence exact receipts"
            ))
        );
        assert_eq!(oracle.pending_provider_commit(), None);

        assert!(PendingProviderCommitV4::root_fence_activate(
            prior,
            frontier(8, 6, 3, 53),
            command,
        )
        .is_err());

        let partitioned = StoreOwnerRootBindingV4::new(
            RootId::from_bytes([61; 16]),
            RootLayoutProfile::PartitionedRoot,
            RootLayoutGeneration::new(1).unwrap(),
            RootPartitionId::from_bytes([62; 16]),
            LogicalShardId::from_bytes([63; 16]),
        );
        assert!(partitioned.is_ok());
    }

    #[test]
    fn serving_wire_is_structurally_exact_but_typed_nq_without_receipt_bridges() {
        let (binding, _, mut oracle, plan, committed_shard, committed_claim) = committed_oracle();
        let genesis = frontier(0, 1, 0, 71);
        oracle.head.canonical_genesis = Some(CanonicalGenesisReceiptV4 {
            binding: genesis_binding(&binding),
            exact_next: genesis,
        });

        let owner_epoch_terminal = frontier(1, 1, 1, 72);
        let install_next = frontier(2, 1, 1, 72);
        let activate_next = frontier(3, 1, 1, 72);
        let install_command = ProviderCommandIdentityV4::new(
            RequestId::from_bytes([10; 16]),
            CommandDigest::from_bytes([73; SHA256_BYTES]),
        )
        .unwrap();
        let activate_command = ProviderCommandIdentityV4::new(
            RequestId::from_bytes([20; 16]),
            CommandDigest::from_bytes([74; SHA256_BYTES]),
        )
        .unwrap();
        let install = TerminalProviderCommandV4::from_pending(
            PendingProviderCommitV4::root_fence_install(
                owner_epoch_terminal,
                install_next,
                install_command,
            )
            .unwrap(),
        )
        .unwrap();
        let activate = TerminalProviderCommandV4::from_pending(
            PendingProviderCommitV4::root_fence_activate(
                install_next,
                activate_next,
                activate_command,
            )
            .unwrap(),
        )
        .unwrap();
        let mut serving_shard = committed_shard;
        serving_shard.state = LogicalShardState::Serving;
        oracle.head.store_state = StoreOwnerStateV4::Ready {
            frontier: activate_next,
        };
        oracle.head.installed_root_fence = Some(InstalledRootFenceReceiptV4 {
            attempt_sequence: oracle.head.attempt.sequence,
            plan_digest: *plan.digest().as_bytes(),
            terminal: install,
        });
        oracle.head.attempt.state = StoreOwnerAttemptStateV4::Serving;
        oracle.head.attempt.serving_publication = Some(ServingPublicationReceiptV4 {
            shard: serving_shard,
            claim: committed_claim,
        });
        oracle.head.attempt.activated_root_fence = Some(activate);
        assert_eq!(
            oracle.encode(),
            Err(StoreOwnerJournalErrorV4::DependencyNotQualified(
                "nokv-meta owner-epoch/RootFence chain and exact control Serving publication receipt"
            ))
        );
    }

    #[test]
    fn permanent_rejected_and_aborted_claims_are_phase_exact() {
        let binding = binding(13);
        let exclusive = exclusive(&binding);
        let plan = fresh_plan("node-a:7000".to_owned(), 42, 11);

        let mut rejected = StoreOwnerJournalOracleV4::new(binding.clone(), &exclusive).unwrap();
        rejected
            .begin_admission_intent(&exclusive, plan.intent().clone(), request_ids(10, 20))
            .unwrap();
        let rejected_claim = OwnerAdmissionClaimV1::rejected_from_absent(
            plan.intent(),
            OwnerAdmissionRejectionReasonV1::ExpectedShardChanged,
        )
        .unwrap();
        rejected
            .record_owner_rejected(&exclusive, rejected_claim.clone())
            .unwrap();
        assert!(matches!(
            rejected.head.attempt.receipt,
            Some(AttemptReceiptV4::Rejected { ref claim }) if claim == &rejected_claim
        ));

        let mut aborted = StoreOwnerJournalOracleV4::new(binding, &exclusive).unwrap();
        aborted
            .begin_admission_intent(&exclusive, plan.intent().clone(), request_ids(30, 40))
            .unwrap();
        let prepared = OwnerAdmissionClaimV1::prepared(&plan).unwrap();
        aborted
            .record_owner_plan(
                &exclusive,
                plan.clone(),
                prepared.clone(),
                OwnerAdmissionPlanSentinelV1::for_plan(&plan),
            )
            .unwrap();
        let aborted_claim = prepared
            .abort(OwnerAdmissionAbortReasonV1::OwnerCasRejected)
            .unwrap();
        aborted
            .record_owner_aborted(&exclusive, aborted_claim.clone())
            .unwrap();
        assert!(matches!(
            aborted.head.attempt.receipt,
            Some(AttemptReceiptV4::Aborted { ref claim }) if claim == &aborted_claim
        ));
    }

    #[test]
    fn oversized_plan_fails_closed_without_a_sidecar_or_mutation_facade() {
        let binding = binding(13);
        let exclusive = exclusive(&binding);
        let plan = fresh_plan("x".repeat(2_048), 42, 11);
        assert!(encode_planned_owner_admission(&plan).unwrap().len() > MAX_INLINE_PLAN_BYTES);
        let mut oracle = StoreOwnerJournalOracleV4::new(binding, &exclusive).unwrap();
        oracle
            .begin_admission_intent(&exclusive, plan.intent().clone(), request_ids(10, 20))
            .unwrap();
        let before = oracle.encode().unwrap();
        let prepared = OwnerAdmissionClaimV1::prepared(&plan).unwrap();
        assert!(matches!(
            oracle.record_owner_plan(
                &exclusive,
                plan.clone(),
                prepared,
                OwnerAdmissionPlanSentinelV1::for_plan(&plan),
            ),
            Err(StoreOwnerJournalErrorV4::InlinePlanBudgetExceeded { .. })
        ));
        assert_eq!(oracle.encode().unwrap(), before);
    }

    #[test]
    fn debug_output_redacts_durable_identities() {
        let binding = binding(13);
        let exclusive = exclusive(&binding);
        let mut oracle = StoreOwnerJournalOracleV4::new(binding, &exclusive).unwrap();
        let plan = fresh_plan("secret-node:7000".to_owned(), 42, 11);
        oracle
            .begin_admission_intent(&exclusive, plan.intent().clone(), request_ids(10, 20))
            .unwrap();
        let debug = format!("{oracle:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-node"));
        assert!(!debug.contains("node-a"));
    }

    #[cfg(unix)]
    #[test]
    fn held_exclusive_reservation_revalidates_the_journal_authority() {
        let outer = tempfile::tempdir().unwrap();
        let configured_parent = outer.path().join("journal-parent");
        let moved_parent = outer.path().join("journal-parent-moved");
        fs::create_dir(&configured_parent).unwrap();
        let authority =
            StoreOwnerJournalAuthorityV4::acquire(&configured_parent.join("owner.head")).unwrap();

        let mut binding = binding(13);
        binding.physical = StoreOwnerPhysicalBindingV4::from_digests(
            authority.canonical_locator_digest().unwrap(),
            [4; SHA256_BYTES],
            authority.authority_identity_digest().unwrap(),
            StoreReservationBindingDigestV4::from_bytes([13; SHA256_BYTES]).unwrap(),
        )
        .unwrap();
        let exclusive =
            StoreOwnerJournalExclusiveReservationV4::from_authority(&binding, &authority).unwrap();
        let mut oracle = StoreOwnerJournalOracleV4::new(binding, &exclusive).unwrap();
        let before = oracle.encode().unwrap();

        fs::rename(&configured_parent, &moved_parent).unwrap();
        fs::create_dir(&configured_parent).unwrap();
        let plan = fresh_plan("node-a:7000".to_owned(), 42, 11);
        assert_eq!(
            oracle.begin_admission_intent(&exclusive, plan.intent().clone(), request_ids(10, 20),),
            Err(StoreOwnerJournalErrorV4::BindingMismatch(
                "journal authority"
            ))
        );
        assert_eq!(oracle.encode().unwrap(), before);
    }
}
