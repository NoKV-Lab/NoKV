use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::digest::{hex, sha256};
use crate::{
    ArtifactObjectStore, ImmutableCreateOutcome, ObjectError, ObjectKey, ObjectRange,
    DEFAULT_S3_MULTIPART_PART_SIZE,
};

pub const PROVIDER_ADMISSION_CONTRACT_VERSION: u8 = 1;
pub const MAX_SINGLE_PUT_ADMISSION_BYTES: usize = DEFAULT_S3_MULTIPART_PART_SIZE - 1;
const PROVIDER_ADMISSION_KEY_PREFIX: &str = "nokv/system/provider-admission/v1";
const MIN_ADMISSION_PROBE_BYTES: usize = 4;

static PROBE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static HANDLE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderHandleIdentity([u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmittedCreateMode {
    SinglePutIfAbsent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderAdmissionCapability {
    AtomicCreateIfAbsent,
    ExactRangeRead,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderAdmissionError {
    Rejected {
        capability: ProviderAdmissionCapability,
    },
    Unavailable,
    Inconclusive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderAdmissionProfile {
    max_verified_object_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProviderAdmissionReceipt {
    contract_version: u8,
    admitted_create_mode: AdmittedCreateMode,
    max_verified_object_bytes: usize,
    range_read_verified: bool,
    handle_identity: ProviderHandleIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CreateObservation {
    Created,
    Replayed,
    Collision,
    ReconciledMatch,
    ReconciledDifferent,
}

impl ProviderAdmissionProfile {
    pub fn single_put(max_verified_object_bytes: usize) -> Result<Self, ProviderAdmissionError> {
        if !(MIN_ADMISSION_PROBE_BYTES..=MAX_SINGLE_PUT_ADMISSION_BYTES)
            .contains(&max_verified_object_bytes)
        {
            return Err(ProviderAdmissionError::Inconclusive);
        }
        Ok(Self {
            max_verified_object_bytes,
        })
    }

    pub const fn max_verified_object_bytes(self) -> usize {
        self.max_verified_object_bytes
    }
}

impl ProviderAdmissionReceipt {
    pub const fn contract_version(&self) -> u8 {
        self.contract_version
    }

    pub const fn admitted_create_mode(&self) -> AdmittedCreateMode {
        self.admitted_create_mode
    }

    pub const fn max_verified_object_bytes(&self) -> usize {
        self.max_verified_object_bytes
    }

    pub const fn range_read_verified(&self) -> bool {
        self.range_read_verified
    }

    pub fn is_bound_to_store(&self, store: &dyn ArtifactObjectStore) -> bool {
        self.contract_version == PROVIDER_ADMISSION_CONTRACT_VERSION
            && self.admitted_create_mode == AdmittedCreateMode::SinglePutIfAbsent
            && self.range_read_verified
            && self.handle_identity == store.provider_handle_identity()
    }

    pub fn admits_store(&self, store: &dyn ArtifactObjectStore, block_size: usize) -> bool {
        self.is_bound_to_store(store)
            && block_size != 0
            && block_size <= self.max_verified_object_bytes
    }

    pub(crate) const fn trusted_in_process(
        handle_identity: ProviderHandleIdentity,
        max_verified_object_bytes: usize,
    ) -> Self {
        Self {
            contract_version: PROVIDER_ADMISSION_CONTRACT_VERSION,
            admitted_create_mode: AdmittedCreateMode::SinglePutIfAbsent,
            max_verified_object_bytes,
            range_read_verified: true,
            handle_identity,
        }
    }
}

impl ProviderHandleIdentity {
    /// Mint one process-local identity for a concrete provider handle.
    ///
    /// Provider implementations keep this value alongside their immutable
    /// connection profile and preserve it only when cloning that same handle.
    pub fn new() -> Self {
        let sequence = HANDLE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut seed = Vec::with_capacity(32);
        seed.extend_from_slice(&std::process::id().to_be_bytes());
        seed.extend_from_slice(&sequence.to_be_bytes());
        seed.extend_from_slice(
            &SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .to_be_bytes(),
        );
        Self(sha256(&seed))
    }

    pub(crate) fn from_handle_address(address: usize) -> Self {
        let mut seed = Vec::with_capacity(32);
        seed.extend_from_slice(b"nokv.provider-handle-address.v1\0");
        seed.extend_from_slice(&std::process::id().to_be_bytes());
        seed.extend_from_slice(&address.to_be_bytes());
        Self(sha256(&seed))
    }
}

impl Default for ProviderHandleIdentity {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ProviderHandleIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProviderHandleIdentity(..)")
    }
}

impl ProviderAdmissionError {
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

impl fmt::Display for ProviderAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { capability } => write!(
                formatter,
                "object provider admission rejected required {} capability",
                capability.label()
            ),
            Self::Unavailable => formatter.write_str("object provider admission is unavailable"),
            Self::Inconclusive => {
                formatter.write_str("object provider admission result is inconclusive")
            }
        }
    }
}

impl std::error::Error for ProviderAdmissionError {}

impl ProviderAdmissionCapability {
    const fn label(self) -> &'static str {
        match self {
            Self::AtomicCreateIfAbsent => "atomic create-if-absent",
            Self::ExactRangeRead => "exact range-read",
        }
    }
}

/// Prove the provider contract by writing only reserved, unreferenced system keys.
///
/// The receipt qualifies single-PUT objects no larger than the probe payload.
/// Multipart completion is deliberately not qualified by this contract version.
pub fn admit_artifact_provider(
    store: &(dyn ArtifactObjectStore + Sync),
    profile: ProviderAdmissionProfile,
) -> Result<ProviderAdmissionReceipt, ProviderAdmissionError> {
    let capabilities = store.capabilities();
    if !capabilities.atomic_create_if_absent {
        return Err(ProviderAdmissionError::Rejected {
            capability: ProviderAdmissionCapability::AtomicCreateIfAbsent,
        });
    }
    if !capabilities.range_read {
        return Err(ProviderAdmissionError::Rejected {
            capability: ProviderAdmissionCapability::ExactRangeRead,
        });
    }

    let nonce = fresh_probe_nonce();
    let sequence_key = probe_key(&nonce, "sequence")?;
    let race_key = probe_key(&nonce, "race")?;
    let a = probe_payload(profile.max_verified_object_bytes, 0x5a);
    let b = probe_payload(profile.max_verified_object_bytes, 0xa5);

    let result = (|| {
        let mut atomic_semantics_rejected = false;

        // A fresh key must end with exactly these bytes durably present.
        // `Replayed` is a valid first observation: the provider accepted the
        // conditional create, lost the response, and reconciliation found the
        // identical bytes. A provider that ignores the condition still fails
        // the replay and collision steps below.
        let fresh = observe_create(store, &sequence_key, &a)?;
        if !observation_confirms_own_bytes(fresh) {
            atomic_semantics_rejected = true;
        }

        let replay = observe_create(store, &sequence_key, &a)?;
        if !matches!(
            replay,
            CreateObservation::Replayed | CreateObservation::ReconciledMatch
        ) {
            atomic_semantics_rejected = true;
        }

        let collision = observe_create(store, &sequence_key, &b)?;
        if !matches!(
            collision,
            CreateObservation::Collision | CreateObservation::ReconciledDifferent
        ) {
            atomic_semantics_rejected = true;
        }

        match store.read(&sequence_key, None) {
            Ok(bytes) if bytes == a => {}
            Ok(_) => atomic_semantics_rejected = true,
            Err(error) => return Err(classify_provider_error(&error)),
        }

        if !concurrent_create_has_one_winner(store, &race_key, &a, &b)? {
            atomic_semantics_rejected = true;
        }

        if atomic_semantics_rejected {
            return Err(ProviderAdmissionError::Rejected {
                capability: ProviderAdmissionCapability::AtomicCreateIfAbsent,
            });
        }

        let offset = profile.max_verified_object_bytes / 4;
        let len = profile.max_verified_object_bytes / 2;
        let range = ObjectRange::new(offset as u64, len)
            .map_err(|_| ProviderAdmissionError::Inconclusive)?;
        match store.read(&sequence_key, Some(range)) {
            Ok(bytes) if bytes == a[offset..offset + len] => {}
            Ok(_) => {
                return Err(ProviderAdmissionError::Rejected {
                    capability: ProviderAdmissionCapability::ExactRangeRead,
                });
            }
            Err(error) => return Err(classify_provider_error(&error)),
        }

        Ok(ProviderAdmissionReceipt {
            contract_version: PROVIDER_ADMISSION_CONTRACT_VERSION,
            admitted_create_mode: AdmittedCreateMode::SinglePutIfAbsent,
            max_verified_object_bytes: profile.max_verified_object_bytes,
            range_read_verified: true,
            handle_identity: store.provider_handle_identity(),
        })
    })();

    // Probe keys never enter namespace metadata. Cleanup cannot affect the
    // result because an ambiguous delete is safer as an unreferenced residual
    // object handled by bucket lifecycle than as a false admission failure.
    let _ = store.delete(&sequence_key);
    let _ = store.delete(&race_key);
    result
}

fn concurrent_create_has_one_winner(
    store: &(dyn ArtifactObjectStore + Sync),
    key: &ObjectKey,
    a: &[u8],
    b: &[u8],
) -> Result<bool, ProviderAdmissionError> {
    let barrier = Arc::new(Barrier::new(3));
    let observations = std::thread::scope(|scope| {
        let first_barrier = Arc::clone(&barrier);
        let first = scope.spawn(move || {
            first_barrier.wait();
            observe_create(store, key, a)
        });
        let second_barrier = Arc::clone(&barrier);
        let second = scope.spawn(move || {
            second_barrier.wait();
            observe_create(store, key, b)
        });
        barrier.wait();
        (first.join(), second.join())
    });
    let first = observations
        .0
        .map_err(|_| ProviderAdmissionError::Inconclusive)??;
    let second = observations
        .1
        .map_err(|_| ProviderAdmissionError::Inconclusive)??;

    // Exactly one racer may end up owning the key and the other must observe
    // foreign bytes. Either side may have lost its response and reconciled,
    // so the winner is any observation that confirms its own bytes and the
    // loser is any observation of different bytes.
    let winners = usize::from(observation_confirms_own_bytes(first))
        + usize::from(observation_confirms_own_bytes(second));
    let losers = usize::from(observation_sees_foreign_bytes(first))
        + usize::from(observation_sees_foreign_bytes(second));
    if winners != 1 || losers != 1 {
        return Ok(false);
    }

    let stored = store
        .read(key, None)
        .map_err(|error| classify_provider_error(&error))?;
    Ok(stored == a || stored == b)
}

fn observation_confirms_own_bytes(observation: CreateObservation) -> bool {
    matches!(
        observation,
        CreateObservation::Created
            | CreateObservation::Replayed
            | CreateObservation::ReconciledMatch
    )
}

fn observation_sees_foreign_bytes(observation: CreateObservation) -> bool {
    matches!(
        observation,
        CreateObservation::Collision | CreateObservation::ReconciledDifferent
    )
}

fn observe_create(
    store: &(dyn ArtifactObjectStore + Sync),
    key: &ObjectKey,
    expected: &[u8],
) -> Result<CreateObservation, ProviderAdmissionError> {
    match store.create_immutable(key, expected) {
        Ok(ImmutableCreateOutcome::Created) => Ok(CreateObservation::Created),
        Ok(ImmutableCreateOutcome::Replayed) => Ok(CreateObservation::Replayed),
        Err(ObjectError::ImmutableCollision { .. }) => Ok(CreateObservation::Collision),
        Err(ObjectError::CreateAmbiguous { .. }) => match store.read(key, None) {
            Ok(actual) if actual == expected => Ok(CreateObservation::ReconciledMatch),
            Ok(_) => Ok(CreateObservation::ReconciledDifferent),
            Err(_) => Err(ProviderAdmissionError::Inconclusive),
        },
        Err(error) => Err(classify_provider_error(&error)),
    }
}

fn classify_provider_error(error: &ObjectError) -> ProviderAdmissionError {
    match error {
        ObjectError::AtomicCreateUnsupported => ProviderAdmissionError::Rejected {
            capability: ProviderAdmissionCapability::AtomicCreateIfAbsent,
        },
        ObjectError::Backend {
            retryable: true, ..
        } => ProviderAdmissionError::Unavailable,
        _ => ProviderAdmissionError::Inconclusive,
    }
}

fn fresh_probe_nonce() -> String {
    let mut seed = Vec::with_capacity(32);
    seed.extend_from_slice(&std::process::id().to_be_bytes());
    seed.extend_from_slice(&PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    seed.extend_from_slice(
        &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_be_bytes(),
    );
    hex(&sha256(&seed))
}

fn probe_key(nonce: &str, suffix: &str) -> Result<ObjectKey, ProviderAdmissionError> {
    ObjectKey::new(format!("{PROVIDER_ADMISSION_KEY_PREFIX}/{nonce}/{suffix}"))
        .map_err(|_| ProviderAdmissionError::Inconclusive)
}

fn probe_payload(len: usize, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(31) ^ salt)
        .collect()
}
