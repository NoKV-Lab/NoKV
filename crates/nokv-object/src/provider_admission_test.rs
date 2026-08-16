use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::{
    admit_artifact_provider, ArtifactObjectStore, ArtifactStoreCapabilities,
    ImmutableCreateOutcome, ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectKey, ObjectRange,
    ProviderAdmissionCapability, ProviderAdmissionError, ProviderAdmissionProfile, S3ArtifactStore,
    S3ArtifactStoreOptions, DEFAULT_ARTIFACT_BLOCK_SIZE, PROVIDER_ADMISSION_CONTRACT_VERSION,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FakeBehavior {
    Correct,
    RejectConditional,
    Unavailable,
    IgnoreConditional,
    LoseResponses,
}

#[derive(Debug)]
struct FakeProvider {
    behavior: FakeBehavior,
    objects: Mutex<BTreeMap<ObjectKey, Vec<u8>>>,
    overwrote_different_bytes: Mutex<bool>,
}

impl FakeProvider {
    fn new(behavior: FakeBehavior) -> Self {
        Self {
            behavior,
            objects: Mutex::new(BTreeMap::new()),
            overwrote_different_bytes: Mutex::new(false),
        }
    }

    fn apply_create(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        let mut objects = self.objects.lock().unwrap();
        match objects.get(key) {
            Some(existing) if existing.as_slice() == bytes => Ok(ImmutableCreateOutcome::Replayed),
            Some(existing) => Err(ObjectError::ImmutableCollision {
                key: key.clone(),
                expected_sha256: format!("expected-{}", bytes.len()),
                actual_sha256: format!("actual-{}", existing.len()),
            }),
            None => {
                objects.insert(key.clone(), bytes.to_vec());
                Ok(ImmutableCreateOutcome::Created)
            }
        }
    }
}

impl ArtifactObjectStore for FakeProvider {
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        ArtifactStoreCapabilities {
            atomic_create_if_absent: true,
            range_read: true,
            multipart_create: true,
        }
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        match self.behavior {
            FakeBehavior::Correct => self.apply_create(key, bytes),
            FakeBehavior::RejectConditional => Err(ObjectError::AtomicCreateUnsupported),
            FakeBehavior::Unavailable => Err(ObjectError::backend_failure(
                "endpoint=https://sentinel.invalid is temporarily unavailable",
                true,
            )),
            FakeBehavior::IgnoreConditional => {
                let mut objects = self.objects.lock().unwrap();
                if objects
                    .insert(key.clone(), bytes.to_vec())
                    .is_some_and(|existing| existing.as_slice() != bytes)
                {
                    *self.overwrote_different_bytes.lock().unwrap() = true;
                }
                Ok(ImmutableCreateOutcome::Created)
            }
            FakeBehavior::LoseResponses => {
                let _ = self.apply_create(key, bytes);
                Err(ObjectError::CreateAmbiguous {
                    key: key.clone(),
                    detail: "endpoint=https://sentinel.invalid bucket=sentinel-bucket".to_owned(),
                })
            }
        }
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        let objects = self.objects.lock().unwrap();
        let bytes = objects
            .get(key)
            .ok_or_else(|| ObjectError::ObjectNotFound { key: key.clone() })?;
        crate::store::strict_range(bytes, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(|bytes| ObjectInfo {
                key: key.clone(),
                size: bytes.len() as u64,
            }))
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        Ok(if self.objects.lock().unwrap().remove(key).is_some() {
            ObjectDeleteOutcome::Deleted
        } else {
            ObjectDeleteOutcome::Absent
        })
    }
}

fn profile() -> ProviderAdmissionProfile {
    ProviderAdmissionProfile::single_put(64).unwrap()
}

#[test]
fn actual_writes_admit_fresh_replay_collision_readback_range_and_concurrency() {
    let store = FakeProvider::new(FakeBehavior::Correct);
    let receipt = admit_artifact_provider(&store, profile()).unwrap();

    assert_eq!(
        receipt.contract_version(),
        PROVIDER_ADMISSION_CONTRACT_VERSION
    );
    assert_eq!(receipt.max_verified_object_bytes(), 64);
    assert!(receipt.range_read_verified());
    assert_eq!(
        receipt.admitted_create_mode(),
        crate::AdmittedCreateMode::SinglePutIfAbsent
    );
    assert!(store.objects.lock().unwrap().is_empty());
}

#[test]
fn receipt_is_bound_to_the_exact_provider_handle() {
    let provider_a = FakeProvider::new(FakeBehavior::Correct);
    let provider_b = FakeProvider::new(FakeBehavior::Correct);
    let receipt = admit_artifact_provider(&provider_a, profile()).unwrap();

    assert!(receipt.admits_store(&provider_a, 64));
    assert!(!receipt.admits_store(&provider_b, 64));
}

#[test]
fn a_static_true_provider_that_rejects_conditional_create_is_rejected() {
    let store = FakeProvider::new(FakeBehavior::RejectConditional);
    assert_eq!(
        admit_artifact_provider(&store, profile()),
        Err(ProviderAdmissionError::Rejected {
            capability: ProviderAdmissionCapability::AtomicCreateIfAbsent,
        })
    );
}

#[test]
fn a_temporarily_unavailable_provider_is_typed_retryable_and_redacted() {
    let store = FakeProvider::new(FakeBehavior::Unavailable);
    let error = admit_artifact_provider(&store, profile()).unwrap_err();
    assert_eq!(error, ProviderAdmissionError::Unavailable);
    assert!(error.retryable());
    let rendered = error.to_string();
    assert!(!rendered.contains("sentinel"));
    assert!(!rendered.contains("endpoint"));
}

#[test]
fn a_provider_that_ignores_conditions_and_overwrites_is_rejected() {
    let store = FakeProvider::new(FakeBehavior::IgnoreConditional);
    assert_eq!(
        admit_artifact_provider(&store, profile()),
        Err(ProviderAdmissionError::Rejected {
            capability: ProviderAdmissionCapability::AtomicCreateIfAbsent,
        })
    );
    assert!(*store.overwrote_different_bytes.lock().unwrap());
}

#[test]
fn response_loss_is_reconciled_from_exact_readback() {
    let store = FakeProvider::new(FakeBehavior::LoseResponses);
    let receipt = admit_artifact_provider(&store, profile()).unwrap();
    assert_eq!(receipt.max_verified_object_bytes(), 64);
}

#[test]
fn public_provider_errors_never_render_endpoint_bucket_or_key_details() {
    let key = ObjectKey::new("nokv/system/sentinel-secret-key").unwrap();
    let object_errors = [
        ObjectError::ObjectNotFound { key: key.clone() },
        ObjectError::ImmutableCollision {
            key: key.clone(),
            expected_sha256: "sentinel-expected-digest".to_owned(),
            actual_sha256: "sentinel-actual-digest".to_owned(),
        },
        ObjectError::DigestMismatch {
            key: key.clone(),
            expected_sha256: "sentinel-expected-digest".to_owned(),
            actual_sha256: "sentinel-actual-digest".to_owned(),
        },
        ObjectError::InvalidManifest(
            "endpoint=https://sentinel.invalid bucket=sentinel-bucket key=nokv/system/sentinel"
                .to_owned(),
        ),
        ObjectError::CreateAmbiguous {
            key: key.clone(),
            detail: "endpoint=https://sentinel.invalid bucket=sentinel-bucket".to_owned(),
        },
        ObjectError::DeleteAmbiguous {
            key: key.clone(),
            detail: "endpoint=https://sentinel.invalid bucket=sentinel-bucket".to_owned(),
        },
        ObjectError::backend_failure(
            "endpoint=https://sentinel.invalid bucket=sentinel-bucket",
            true,
        ),
    ];
    let mut errors = object_errors
        .iter()
        .flat_map(|error| [error.to_string(), format!("{error:?}")])
        .collect::<Vec<_>>();
    errors.extend([
        ProviderAdmissionError::Inconclusive.to_string(),
        ProviderAdmissionError::Unavailable.to_string(),
    ]);
    for rendered in errors {
        assert!(!rendered.contains("sentinel"), "leaked detail: {rendered}");
        assert!(!rendered.contains("endpoint"), "leaked detail: {rendered}");
        assert!(!rendered.contains("bucket"), "leaked detail: {rendered}");
        assert!(!rendered.contains("nokv/system"), "leaked key: {rendered}");
    }
}

fn live_s3_store() -> S3ArtifactStore {
    let required = |name: &str| {
        std::env::var(name)
            .unwrap_or_else(|_| panic!("{name} is required for the live provider test"))
    };
    S3ArtifactStore::new(S3ArtifactStoreOptions {
        bucket: required("NOKV_TEST_S3_BUCKET"),
        root: required("NOKV_TEST_S3_ROOT"),
        region: std::env::var("NOKV_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned()),
        endpoint: Some(required("NOKV_TEST_S3_ENDPOINT")),
        access_key_id: Some(required("NOKV_TEST_S3_ACCESS_KEY_ID")),
        secret_access_key: Some(required("NOKV_TEST_S3_SECRET_ACCESS_KEY")),
        session_token: None,
        virtual_host_style: false,
        skip_signature: false,
    })
    .expect("construct live S3-compatible provider")
}

#[test]
#[ignore = "requires an explicitly configured live S3-compatible endpoint"]
fn live_provider_admission_passes_the_single_put_profile() {
    let store = live_s3_store();
    let profile = ProviderAdmissionProfile::single_put(DEFAULT_ARTIFACT_BLOCK_SIZE).unwrap();
    let receipt = admit_artifact_provider(&store, profile).expect("live provider admission");
    assert_eq!(
        receipt.max_verified_object_bytes(),
        DEFAULT_ARTIFACT_BLOCK_SIZE
    );
    assert!(receipt.range_read_verified());
    assert!(receipt.admits_store(&store, DEFAULT_ARTIFACT_BLOCK_SIZE));
}

#[test]
#[ignore = "requires an explicitly configured incompatible S3-compatible endpoint"]
fn live_provider_admission_fails_closed_with_a_redacted_typed_result() {
    let store = live_s3_store();
    assert!(store.capabilities().atomic_create_if_absent);
    let profile = ProviderAdmissionProfile::single_put(DEFAULT_ARTIFACT_BLOCK_SIZE).unwrap();
    let error = admit_artifact_provider(&store, profile).expect_err("provider must fail admission");
    assert!(matches!(
        error,
        ProviderAdmissionError::Rejected { .. } | ProviderAdmissionError::Inconclusive
    ));
    let rendered = error.to_string();
    for secret in [
        std::env::var("NOKV_TEST_S3_ENDPOINT").unwrap(),
        std::env::var("NOKV_TEST_S3_BUCKET").unwrap(),
        std::env::var("NOKV_TEST_S3_ROOT").unwrap(),
    ] {
        assert!(!rendered.contains(&secret));
    }
    eprintln!("provider admission classification: {error:?}");
}
