/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_types::{LogicalShardId, ObjectNamespaceId};

use crate::digest::{hex, sha256};
use crate::{ArtifactObjectStore, ImmutableCreateOutcome, ObjectError, ObjectKey};

const CHUNK_DESCRIPTOR_BYTES: usize = 4 + 4 + 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChunkedBlobBounds {
    pub max_payload_bytes: usize,
    pub max_chunks: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChunkedBlobKeyspace {
    pub prefix: &'static str,
    pub object_namespace: ObjectNamespaceId,
    pub logical_shard: LogicalShardId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChunkedBlobReceipt {
    pub payload_len: u64,
    pub payload_digest: [u8; 32],
    pub manifest_len: u64,
    pub manifest_digest: [u8; 32],
    pub chunk_size: u32,
    pub chunk_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChunkedBlobPlan {
    keyspace: ChunkedBlobKeyspace,
    bounds: ChunkedBlobBounds,
    manifest_header: Vec<u8>,
    manifest_bytes: Vec<u8>,
    receipt: ChunkedBlobReceipt,
    object_keys: Vec<ObjectKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChunkedBlobWrite {
    pub chunks_created: u32,
    pub chunks_replayed: u32,
    pub manifest_outcome: ImmutableCreateOutcome,
}

#[derive(PartialEq, Eq)]
pub(crate) enum ChunkedBlobError {
    EmptyPayload,
    PayloadTooLarge { actual: usize, maximum: usize },
    InvalidChunkSize,
    TooManyChunks { actual: usize, maximum: usize },
    PayloadMismatch,
    InvalidPlan(&'static str),
    ObjectNamespaceRequired,
    ForeignNamespace,
    ProviderAdmissionRequired,
    ProviderObjectTooLarge { requested: usize, admitted: usize },
    InvalidManifest(&'static str),
    CreateOutcomeUnknown { key: ObjectKey },
    Object(ObjectError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ChunkDescriptor {
    index: u32,
    len: u32,
    digest: [u8; 32],
}

impl ChunkedBlobPlan {
    pub(crate) const fn receipt(&self) -> ChunkedBlobReceipt {
        self.receipt
    }

    pub(crate) fn manifest_bytes(&self) -> &[u8] {
        &self.manifest_bytes
    }

    pub(crate) fn object_keys(&self) -> &[ObjectKey] {
        &self.object_keys
    }
}

pub(crate) fn plan_chunked_blob(
    keyspace: ChunkedBlobKeyspace,
    bounds: ChunkedBlobBounds,
    payload: &[u8],
    chunk_size: usize,
    encode_header: impl FnOnce(u64, [u8; 32]) -> Vec<u8>,
) -> Result<ChunkedBlobPlan, ChunkedBlobError> {
    validate_payload_bounds(payload, chunk_size, bounds)?;
    let payload_digest = sha256(payload);
    let manifest_header = encode_header(payload.len() as u64, payload_digest);
    if manifest_header.is_empty() {
        return Err(ChunkedBlobError::InvalidPlan("manifest header is empty"));
    }
    let descriptors = describe_chunks(payload, chunk_size);
    let manifest_bytes = encode_manifest(&manifest_header, chunk_size as u32, &descriptors);
    let receipt = ChunkedBlobReceipt {
        payload_len: payload.len() as u64,
        payload_digest,
        manifest_len: manifest_bytes.len() as u64,
        manifest_digest: sha256(&manifest_bytes),
        chunk_size: chunk_size as u32,
        chunk_count: descriptors.len() as u32,
    };
    let object_keys = derive_object_keys(keyspace, receipt, bounds)?;
    Ok(ChunkedBlobPlan {
        keyspace,
        bounds,
        manifest_header,
        manifest_bytes,
        receipt,
        object_keys,
    })
}

pub(crate) fn restore_chunked_blob_plan(
    keyspace: ChunkedBlobKeyspace,
    bounds: ChunkedBlobBounds,
    manifest_header: Vec<u8>,
    receipt: ChunkedBlobReceipt,
    manifest_bytes: Vec<u8>,
) -> Result<ChunkedBlobPlan, ChunkedBlobError> {
    validate_chunked_blob_receipt(receipt, bounds)?;
    if manifest_bytes.len() as u64 != receipt.manifest_len {
        return Err(ChunkedBlobError::InvalidPlan(
            "manifest length does not match receipt",
        ));
    }
    if sha256(&manifest_bytes) != receipt.manifest_digest {
        return Err(ChunkedBlobError::InvalidPlan(
            "manifest digest does not match receipt",
        ));
    }
    decode_manifest(&manifest_bytes, &manifest_header, receipt, bounds)?;
    let object_keys = derive_object_keys(keyspace, receipt, bounds)?;
    Ok(ChunkedBlobPlan {
        keyspace,
        bounds,
        manifest_header,
        manifest_bytes,
        receipt,
        object_keys,
    })
}

pub(crate) fn write_chunked_blob_from_plan(
    store: &dyn ArtifactObjectStore,
    plan: &ChunkedBlobPlan,
    payload: &[u8],
) -> Result<ChunkedBlobWrite, ChunkedBlobError> {
    validate_bound_namespace(store, plan.keyspace.object_namespace)?;
    validate_chunked_blob_receipt(plan.receipt, plan.bounds)?;
    validate_payload_matches_plan(plan, payload)?;
    preflight_provider_admission(
        store,
        plan.receipt.chunk_size as usize,
        plan.manifest_bytes.len(),
    )?;

    let chunk_count = plan.receipt.chunk_count as usize;
    let mut chunks_created = 0_u32;
    let mut chunks_replayed = 0_u32;
    for (key, bytes) in plan.object_keys[..chunk_count]
        .iter()
        .zip(payload.chunks(plan.receipt.chunk_size as usize))
    {
        match create_with_exact_readback(store, key, bytes)? {
            ImmutableCreateOutcome::Created => chunks_created += 1,
            ImmutableCreateOutcome::Replayed => chunks_replayed += 1,
        }
    }
    let manifest_key = plan
        .object_keys
        .last()
        .ok_or(ChunkedBlobError::InvalidPlan("manifest key is missing"))?;
    let manifest_outcome = create_with_exact_readback(store, manifest_key, &plan.manifest_bytes)?;
    Ok(ChunkedBlobWrite {
        chunks_created,
        chunks_replayed,
        manifest_outcome,
    })
}

pub(crate) fn read_chunked_blob(
    store: &dyn ArtifactObjectStore,
    keyspace: ChunkedBlobKeyspace,
    bounds: ChunkedBlobBounds,
    manifest_header: Vec<u8>,
    receipt: ChunkedBlobReceipt,
) -> Result<Vec<u8>, ChunkedBlobError> {
    validate_bound_namespace(store, keyspace.object_namespace)?;
    validate_chunked_blob_receipt(receipt, bounds)?;
    let object_keys = derive_object_keys(keyspace, receipt, bounds)?;
    let manifest_key = object_keys
        .last()
        .ok_or(ChunkedBlobError::InvalidManifest("manifest key is missing"))?;
    let manifest_bytes = store.read(manifest_key, None)?;
    let plan =
        restore_chunked_blob_plan(keyspace, bounds, manifest_header, receipt, manifest_bytes)
            .map_err(plan_error_as_manifest)?;

    let capacity = usize::try_from(receipt.payload_len)
        .map_err(|_| ChunkedBlobError::InvalidManifest("payload length overflows usize"))?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(capacity)
        .map_err(|_| ChunkedBlobError::InvalidManifest("payload allocation failed"))?;
    let descriptors =
        decode_manifest(&plan.manifest_bytes, &plan.manifest_header, receipt, bounds)?;
    for (descriptor, key) in descriptors
        .iter()
        .zip(plan.object_keys[..receipt.chunk_count as usize].iter())
    {
        let bytes = store.read(key, None)?;
        if bytes.len() != descriptor.len as usize {
            return Err(ChunkedBlobError::InvalidManifest(
                "chunk length does not match manifest",
            ));
        }
        if sha256(&bytes) != descriptor.digest {
            return Err(ChunkedBlobError::InvalidManifest(
                "chunk digest does not match manifest",
            ));
        }
        payload.extend_from_slice(&bytes);
    }
    if payload.len() as u64 != receipt.payload_len || sha256(&payload) != receipt.payload_digest {
        return Err(ChunkedBlobError::InvalidManifest(
            "assembled payload does not match receipt",
        ));
    }
    Ok(payload)
}

pub(crate) fn derive_object_keys(
    keyspace: ChunkedBlobKeyspace,
    receipt: ChunkedBlobReceipt,
    bounds: ChunkedBlobBounds,
) -> Result<Vec<ObjectKey>, ChunkedBlobError> {
    validate_chunked_blob_receipt(receipt, bounds)?;
    let mut keys = Vec::with_capacity(receipt.chunk_count as usize + 1);
    for index in 0..receipt.chunk_count {
        keys.push(derive_chunk_key(keyspace, receipt.manifest_digest, index)?);
    }
    keys.push(derive_manifest_key(keyspace, receipt.manifest_digest)?);
    Ok(keys)
}

pub(crate) fn derive_manifest_key(
    keyspace: ChunkedBlobKeyspace,
    manifest_digest: [u8; 32],
) -> Result<ObjectKey, ChunkedBlobError> {
    ObjectKey::new(format!(
        "{}/{}/{}/manifest",
        keyspace.prefix,
        hex(keyspace.logical_shard.as_bytes()),
        hex(&manifest_digest)
    ))
    .map_err(ChunkedBlobError::Object)
}

pub(crate) fn derive_chunk_key(
    keyspace: ChunkedBlobKeyspace,
    manifest_digest: [u8; 32],
    index: u32,
) -> Result<ObjectKey, ChunkedBlobError> {
    ObjectKey::new(format!(
        "{}/{}/{}/chunks/{index:016x}",
        keyspace.prefix,
        hex(keyspace.logical_shard.as_bytes()),
        hex(&manifest_digest)
    ))
    .map_err(ChunkedBlobError::Object)
}

fn validate_payload_bounds(
    payload: &[u8],
    chunk_size: usize,
    bounds: ChunkedBlobBounds,
) -> Result<(), ChunkedBlobError> {
    if payload.is_empty() {
        return Err(ChunkedBlobError::EmptyPayload);
    }
    if payload.len() > bounds.max_payload_bytes {
        return Err(ChunkedBlobError::PayloadTooLarge {
            actual: payload.len(),
            maximum: bounds.max_payload_bytes,
        });
    }
    if chunk_size == 0 || chunk_size > u32::MAX as usize {
        return Err(ChunkedBlobError::InvalidChunkSize);
    }
    let chunk_count = payload
        .len()
        .checked_add(chunk_size - 1)
        .ok_or(ChunkedBlobError::InvalidChunkSize)?
        / chunk_size;
    if chunk_count > bounds.max_chunks || chunk_count > u32::MAX as usize {
        return Err(ChunkedBlobError::TooManyChunks {
            actual: chunk_count,
            maximum: bounds.max_chunks,
        });
    }
    Ok(())
}

fn validate_payload_matches_plan(
    plan: &ChunkedBlobPlan,
    payload: &[u8],
) -> Result<(), ChunkedBlobError> {
    validate_payload_bounds(payload, plan.receipt.chunk_size as usize, plan.bounds)?;
    if payload.len() as u64 != plan.receipt.payload_len
        || sha256(payload) != plan.receipt.payload_digest
    {
        return Err(ChunkedBlobError::PayloadMismatch);
    }
    let descriptors = describe_chunks(payload, plan.receipt.chunk_size as usize);
    let manifest = encode_manifest(&plan.manifest_header, plan.receipt.chunk_size, &descriptors);
    if manifest != plan.manifest_bytes || sha256(&manifest) != plan.receipt.manifest_digest {
        return Err(ChunkedBlobError::InvalidPlan(
            "payload does not reproduce the planned manifest",
        ));
    }
    Ok(())
}

pub(crate) fn validate_chunked_blob_receipt(
    receipt: ChunkedBlobReceipt,
    bounds: ChunkedBlobBounds,
) -> Result<(), ChunkedBlobError> {
    let payload_len = usize::try_from(receipt.payload_len)
        .map_err(|_| ChunkedBlobError::InvalidPlan("payload length overflows usize"))?;
    if payload_len == 0 || payload_len > bounds.max_payload_bytes {
        return Err(ChunkedBlobError::InvalidPlan(
            "payload length is outside bounds",
        ));
    }
    let chunk_size = receipt.chunk_size as usize;
    if chunk_size == 0 {
        return Err(ChunkedBlobError::InvalidPlan("chunk size is zero"));
    }
    let expected_count = payload_len
        .checked_add(chunk_size - 1)
        .ok_or(ChunkedBlobError::InvalidPlan("chunk count overflows"))?
        / chunk_size;
    if expected_count != receipt.chunk_count as usize || expected_count > bounds.max_chunks {
        return Err(ChunkedBlobError::InvalidPlan(
            "chunk count does not match payload length",
        ));
    }
    Ok(())
}

fn describe_chunks(payload: &[u8], chunk_size: usize) -> Vec<ChunkDescriptor> {
    payload
        .chunks(chunk_size)
        .enumerate()
        .map(|(index, bytes)| ChunkDescriptor {
            index: index as u32,
            len: bytes.len() as u32,
            digest: sha256(bytes),
        })
        .collect()
}

fn encode_manifest(header: &[u8], chunk_size: u32, descriptors: &[ChunkDescriptor]) -> Vec<u8> {
    let mut bytes =
        Vec::with_capacity(header.len() + 8 + descriptors.len() * CHUNK_DESCRIPTOR_BYTES);
    bytes.extend_from_slice(header);
    bytes.extend_from_slice(&chunk_size.to_be_bytes());
    bytes.extend_from_slice(&(descriptors.len() as u32).to_be_bytes());
    for descriptor in descriptors {
        bytes.extend_from_slice(&descriptor.index.to_be_bytes());
        bytes.extend_from_slice(&descriptor.len.to_be_bytes());
        bytes.extend_from_slice(&descriptor.digest);
    }
    bytes
}

fn decode_manifest(
    bytes: &[u8],
    expected_header: &[u8],
    receipt: ChunkedBlobReceipt,
    bounds: ChunkedBlobBounds,
) -> Result<Vec<ChunkDescriptor>, ChunkedBlobError> {
    if expected_header.is_empty() || !bytes.starts_with(expected_header) {
        return Err(ChunkedBlobError::InvalidManifest(
            "typed manifest header does not match receipt",
        ));
    }
    let mut offset = expected_header.len();
    let chunk_size = take_u32(bytes, &mut offset)?;
    let chunk_count = take_u32(bytes, &mut offset)? as usize;
    if chunk_size != receipt.chunk_size || chunk_count != receipt.chunk_count as usize {
        return Err(ChunkedBlobError::InvalidManifest(
            "chunk layout does not match receipt",
        ));
    }
    if chunk_count == 0 || chunk_count > bounds.max_chunks {
        return Err(ChunkedBlobError::InvalidManifest(
            "chunk count is outside bounds",
        ));
    }
    let expected_len = expected_header
        .len()
        .checked_add(8)
        .and_then(|len| len.checked_add(chunk_count * CHUNK_DESCRIPTOR_BYTES))
        .ok_or(ChunkedBlobError::InvalidManifest(
            "manifest length overflows",
        ))?;
    if bytes.len() != expected_len {
        return Err(ChunkedBlobError::InvalidManifest(
            "manifest length is not canonical",
        ));
    }
    let mut descriptors = Vec::with_capacity(chunk_count);
    for expected_index in 0..chunk_count {
        let index = take_u32(bytes, &mut offset)?;
        if index as usize != expected_index {
            return Err(ChunkedBlobError::InvalidManifest(
                "chunk indexes are not contiguous and ordered",
            ));
        }
        let len = take_u32(bytes, &mut offset)?;
        if len == 0 || len > chunk_size {
            return Err(ChunkedBlobError::InvalidManifest(
                "chunk length is outside bounds",
            ));
        }
        descriptors.push(ChunkDescriptor {
            index,
            len,
            digest: take_array(bytes, &mut offset)?,
        });
    }
    let total = descriptors.iter().try_fold(0_u64, |total, descriptor| {
        total.checked_add(u64::from(descriptor.len))
    });
    if total != Some(receipt.payload_len) {
        return Err(ChunkedBlobError::InvalidManifest(
            "chunk lengths do not sum to payload length",
        ));
    }
    for (index, descriptor) in descriptors.iter().enumerate() {
        if index + 1 != descriptors.len() && descriptor.len != chunk_size {
            return Err(ChunkedBlobError::InvalidManifest(
                "non-final chunk is not full sized",
            ));
        }
    }
    Ok(descriptors)
}

fn validate_bound_namespace(
    store: &dyn ArtifactObjectStore,
    expected: ObjectNamespaceId,
) -> Result<(), ChunkedBlobError> {
    match store.object_namespace() {
        Some(actual) if actual == expected => Ok(()),
        Some(_) => Err(ChunkedBlobError::ForeignNamespace),
        None => Err(ChunkedBlobError::ObjectNamespaceRequired),
    }
}

fn preflight_provider_admission(
    store: &dyn ArtifactObjectStore,
    chunk_size: usize,
    manifest_size: usize,
) -> Result<(), ChunkedBlobError> {
    let admission = store
        .provider_admission_receipt()
        .ok_or(ChunkedBlobError::ProviderAdmissionRequired)?;
    if !admission.is_bound_to_store(store) {
        return Err(ChunkedBlobError::ProviderAdmissionRequired);
    }
    for requested in [chunk_size, manifest_size] {
        if !admission.admits_store(store, requested) {
            return Err(ChunkedBlobError::ProviderObjectTooLarge {
                requested,
                admitted: admission.max_verified_object_bytes(),
            });
        }
    }
    Ok(())
}

fn create_with_exact_readback(
    store: &dyn ArtifactObjectStore,
    key: &ObjectKey,
    bytes: &[u8],
) -> Result<ImmutableCreateOutcome, ChunkedBlobError> {
    match store.create_immutable(key, bytes) {
        Ok(outcome) => Ok(outcome),
        Err(ObjectError::CreateAmbiguous { .. }) => match store.read(key, None) {
            Ok(actual) if actual == bytes => Ok(ImmutableCreateOutcome::Replayed),
            Ok(actual) => Err(ChunkedBlobError::Object(ObjectError::ImmutableCollision {
                key: key.clone(),
                expected_sha256: hex(&sha256(bytes)),
                actual_sha256: hex(&sha256(&actual)),
            })),
            Err(_) => Err(ChunkedBlobError::CreateOutcomeUnknown { key: key.clone() }),
        },
        Err(error) => Err(ChunkedBlobError::Object(error)),
    }
}

fn plan_error_as_manifest(error: ChunkedBlobError) -> ChunkedBlobError {
    match error {
        ChunkedBlobError::InvalidPlan(reason) => ChunkedBlobError::InvalidManifest(reason),
        other => other,
    }
}

impl From<ObjectError> for ChunkedBlobError {
    fn from(error: ObjectError) -> Self {
        Self::Object(error)
    }
}

fn take_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<[u8; N], ChunkedBlobError> {
    let end = offset
        .checked_add(N)
        .ok_or(ChunkedBlobError::InvalidManifest(
            "manifest offset overflows",
        ))?;
    let slice = bytes
        .get(*offset..end)
        .ok_or(ChunkedBlobError::InvalidManifest("manifest is truncated"))?;
    *offset = end;
    let mut array = [0_u8; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn take_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, ChunkedBlobError> {
    Ok(u32::from_be_bytes(take_array(bytes, offset)?))
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;
    use crate::{
        ArtifactStoreCapabilities, ObjectDeleteOutcome, ObjectInfo, ProviderAdmissionReceipt,
        ProviderHandleIdentity,
    };

    #[derive(Clone, Copy)]
    pub(crate) enum CreateBehavior {
        Normal,
        AmbiguousAfterApply,
        AmbiguousBeforeApply,
    }

    pub(crate) struct TestStore {
        namespace: Option<ObjectNamespaceId>,
        handle: ProviderHandleIdentity,
        admission: Option<ProviderAdmissionReceipt>,
        create_behavior: CreateBehavior,
        fail_before_apply_at: Option<usize>,
        state: Mutex<TestStoreState>,
    }

    #[derive(Default)]
    struct TestStoreState {
        objects: BTreeMap<ObjectKey, Vec<u8>>,
        creates: Vec<ObjectKey>,
        reads: Vec<ObjectKey>,
        head_calls: usize,
    }

    impl TestStore {
        pub(crate) fn admitted(namespace: ObjectNamespaceId, max_object_bytes: usize) -> Self {
            Self::with_behavior(namespace, max_object_bytes, CreateBehavior::Normal)
        }

        pub(crate) fn with_behavior(
            namespace: ObjectNamespaceId,
            max_object_bytes: usize,
            create_behavior: CreateBehavior,
        ) -> Self {
            let handle = ProviderHandleIdentity::new();
            Self {
                namespace: Some(namespace),
                handle,
                admission: Some(ProviderAdmissionReceipt::trusted_in_process(
                    handle,
                    max_object_bytes,
                )),
                create_behavior,
                fail_before_apply_at: None,
                state: Mutex::new(TestStoreState::default()),
            }
        }

        pub(crate) fn fail_before_apply_at(
            namespace: ObjectNamespaceId,
            max_object_bytes: usize,
            create_number: usize,
        ) -> Self {
            let mut store = Self::admitted(namespace, max_object_bytes);
            store.fail_before_apply_at = Some(create_number);
            store
        }

        pub(crate) fn bind_admission_to_another_handle(&mut self) {
            self.admission = Some(ProviderAdmissionReceipt::trusted_in_process(
                ProviderHandleIdentity::new(),
                self.admission.as_ref().map_or(
                    usize::MAX,
                    ProviderAdmissionReceipt::max_verified_object_bytes,
                ),
            ));
        }

        pub(crate) fn remove_namespace_binding(&mut self) {
            self.namespace = None;
        }

        pub(crate) fn remove_admission(&mut self) {
            self.admission = None;
        }

        pub(crate) fn create_keys(&self) -> Vec<ObjectKey> {
            self.state.lock().unwrap().creates.clone()
        }

        pub(crate) fn read_count(&self) -> usize {
            self.state.lock().unwrap().reads.len()
        }

        pub(crate) fn head_calls(&self) -> usize {
            self.state.lock().unwrap().head_calls
        }

        pub(crate) fn resident_keys(&self) -> Vec<ObjectKey> {
            self.state.lock().unwrap().objects.keys().cloned().collect()
        }

        pub(crate) fn replace(&self, key: &ObjectKey, bytes: Vec<u8>) {
            self.state
                .lock()
                .unwrap()
                .objects
                .insert(key.clone(), bytes);
        }

        pub(crate) fn remove(&self, key: &ObjectKey) {
            self.state.lock().unwrap().objects.remove(key);
        }

        pub(crate) fn bytes(&self, key: &ObjectKey) -> Vec<u8> {
            self.state.lock().unwrap().objects[key].clone()
        }
    }

    impl ArtifactObjectStore for TestStore {
        fn object_namespace(&self) -> Option<ObjectNamespaceId> {
            self.namespace
        }

        fn capabilities(&self) -> ArtifactStoreCapabilities {
            ArtifactStoreCapabilities {
                range_read: true,
                multipart_create: false,
                atomic_create_if_absent: true,
            }
        }

        fn provider_handle_identity(&self) -> ProviderHandleIdentity {
            self.handle
        }

        fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
            self.admission.as_ref()
        }

        fn create_immutable(
            &self,
            key: &ObjectKey,
            bytes: &[u8],
        ) -> Result<ImmutableCreateOutcome, ObjectError> {
            let mut state = self.state.lock().unwrap();
            state.creates.push(key.clone());
            if self.fail_before_apply_at == Some(state.creates.len()) {
                return Err(ObjectError::Backend {
                    detail: "injected definite create failure".to_owned(),
                    retryable: false,
                });
            }
            if matches!(self.create_behavior, CreateBehavior::AmbiguousBeforeApply) {
                return Err(ObjectError::CreateAmbiguous {
                    key: key.clone(),
                    detail: "injected before apply".to_owned(),
                });
            }
            if let Some(existing) = state.objects.get(key) {
                if existing == bytes {
                    return Ok(ImmutableCreateOutcome::Replayed);
                }
                return Err(ObjectError::ImmutableCollision {
                    key: key.clone(),
                    expected_sha256: hex(&sha256(bytes)),
                    actual_sha256: hex(&sha256(existing)),
                });
            }
            state.objects.insert(key.clone(), bytes.to_vec());
            if matches!(self.create_behavior, CreateBehavior::AmbiguousAfterApply) {
                return Err(ObjectError::CreateAmbiguous {
                    key: key.clone(),
                    detail: "injected after apply".to_owned(),
                });
            }
            Ok(ImmutableCreateOutcome::Created)
        }

        fn read(
            &self,
            key: &ObjectKey,
            range: Option<crate::ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            assert!(range.is_none(), "chunked recovery blobs require full reads");
            let mut state = self.state.lock().unwrap();
            state.reads.push(key.clone());
            state
                .objects
                .get(key)
                .cloned()
                .ok_or_else(|| ObjectError::ObjectNotFound { key: key.clone() })
        }

        fn head(&self, _key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.state.lock().unwrap().head_calls += 1;
            Ok(None)
        }

        fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
            Ok(
                if self.state.lock().unwrap().objects.remove(key).is_some() {
                    ObjectDeleteOutcome::Deleted
                } else {
                    ObjectDeleteOutcome::Absent
                },
            )
        }
    }
}
