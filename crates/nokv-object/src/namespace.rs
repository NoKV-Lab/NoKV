//! Durable identity for one physical artifact-object namespace.
//!
//! Provider endpoints, credentials, buckets, and prefixes remain deployment
//! configuration. This marker gives the bytes reached by that configuration a
//! provider-neutral identity which root placement and Holt fences can verify.

use nokv_types::{ObjectNamespaceId, FIXED_ID_BYTES};

use crate::{
    ArtifactObjectStore, ArtifactStoreCapabilities, ImmutableCreateOutcome, ObjectDeleteOutcome,
    ObjectError, ObjectInfo, ObjectKey, ObjectRange,
};

const MARKER_MAGIC: &[u8] = b"nokv.object-namespace\0";
const MARKER_VERSION: u8 = 1;
const MARKER_BYTES: usize = 1 + FIXED_ID_BYTES;
pub const OBJECT_NAMESPACE_MARKER_KEY: &str = "nokv/system/object-namespace";

/// An object handle admitted against the namespace persisted by metadata.
///
/// Construction performs the remote marker read once. Metadata/object
/// operations can then reject raw or differently-bound provider handles
/// without repeating network I/O for every block.
#[derive(Clone, Debug)]
pub struct BoundArtifactStore<Store> {
    inner: Store,
    namespace_id: ObjectNamespaceId,
}

impl<Store> BoundArtifactStore<Store>
where
    Store: ArtifactObjectStore,
{
    pub fn open(inner: Store, expected: ObjectNamespaceId) -> Result<Self, ObjectError> {
        verify_object_namespace(&inner, expected)?;
        Ok(Self {
            inner,
            namespace_id: expected,
        })
    }

    pub const fn namespace_id(&self) -> ObjectNamespaceId {
        self.namespace_id
    }

    pub fn inner(&self) -> &Store {
        &self.inner
    }
}

impl<Store> ArtifactObjectStore for BoundArtifactStore<Store>
where
    Store: ArtifactObjectStore,
{
    fn object_namespace(&self) -> Option<ObjectNamespaceId> {
        Some(self.namespace_id)
    }

    fn capabilities(&self) -> ArtifactStoreCapabilities {
        self.inner.capabilities()
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        self.inner.create_immutable(key, bytes)
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.read(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        self.inner.delete(key)
    }
}

pub fn load_object_namespace(
    store: &impl ArtifactObjectStore,
) -> Result<Option<ObjectNamespaceId>, ObjectError> {
    let key = marker_key()?;
    match store.read(&key, None) {
        Ok(bytes) => decode_marker(&bytes).map(Some),
        Err(ObjectError::ObjectNotFound { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn ensure_object_namespace(
    store: &impl ArtifactObjectStore,
    namespace_id: ObjectNamespaceId,
) -> Result<ImmutableCreateOutcome, ObjectError> {
    let key = marker_key()?;
    let bytes = encode_marker(namespace_id);
    match store.create_immutable(&key, &bytes) {
        Ok(outcome) => Ok(outcome),
        Err(ObjectError::ImmutableCollision { .. }) => {
            verify_object_namespace(store, namespace_id)?;
            Ok(ImmutableCreateOutcome::Replayed)
        }
        Err(error) => Err(error),
    }
}

pub fn verify_object_namespace(
    store: &impl ArtifactObjectStore,
    expected: ObjectNamespaceId,
) -> Result<(), ObjectError> {
    let actual = load_object_namespace(store)?.ok_or(ObjectError::ObjectNamespaceUninitialized)?;
    if actual != expected {
        return Err(ObjectError::ObjectNamespaceMismatch { expected, actual });
    }
    Ok(())
}

fn marker_key() -> Result<ObjectKey, ObjectError> {
    ObjectKey::new(OBJECT_NAMESPACE_MARKER_KEY)
}

fn encode_marker(namespace_id: ObjectNamespaceId) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(MARKER_MAGIC.len() + MARKER_BYTES);
    bytes.extend_from_slice(MARKER_MAGIC);
    bytes.push(MARKER_VERSION);
    bytes.extend_from_slice(namespace_id.as_bytes());
    bytes
}

fn decode_marker(bytes: &[u8]) -> Result<ObjectNamespaceId, ObjectError> {
    if bytes.len() != MARKER_MAGIC.len() + MARKER_BYTES || !bytes.starts_with(MARKER_MAGIC) {
        return Err(ObjectError::InvalidObjectNamespaceMarker);
    }
    let version = bytes[MARKER_MAGIC.len()];
    if version != MARKER_VERSION {
        return Err(ObjectError::UnsupportedObjectNamespaceMarkerVersion {
            actual: version,
            expected: MARKER_VERSION,
        });
    }
    let id_start = MARKER_MAGIC.len() + 1;
    let mut namespace_id = [0_u8; FIXED_ID_BYTES];
    namespace_id.copy_from_slice(&bytes[id_start..]);
    Ok(ObjectNamespaceId::from_bytes(namespace_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryArtifactStore;

    fn namespace(byte: u8) -> ObjectNamespaceId {
        ObjectNamespaceId::from_bytes([byte; FIXED_ID_BYTES])
    }

    #[test]
    fn namespace_marker_is_immutable_and_exact_replay_is_idempotent() {
        let store = MemoryArtifactStore::new();
        assert_eq!(
            ensure_object_namespace(&store, namespace(1)).expect("create marker"),
            ImmutableCreateOutcome::Created
        );
        assert_eq!(
            ensure_object_namespace(&store, namespace(1)).expect("replay marker"),
            ImmutableCreateOutcome::Replayed
        );
        assert_eq!(load_object_namespace(&store), Ok(Some(namespace(1))));
    }

    #[test]
    fn namespace_mismatch_fails_closed() {
        let store = MemoryArtifactStore::new();
        ensure_object_namespace(&store, namespace(1)).expect("create marker");
        assert_eq!(
            BoundArtifactStore::open(store, namespace(2)).expect_err("reject wrong namespace"),
            ObjectError::ObjectNamespaceMismatch {
                expected: namespace(2),
                actual: namespace(1),
            }
        );
    }

    #[test]
    fn uninitialized_namespace_is_not_implicitly_adopted() {
        let store = MemoryArtifactStore::new();
        assert_eq!(
            BoundArtifactStore::open(store, namespace(1)).expect_err("marker is required"),
            ObjectError::ObjectNamespaceUninitialized
        );
    }

    #[test]
    fn bound_store_exposes_only_verified_identity() {
        let store = MemoryArtifactStore::new();
        ensure_object_namespace(&store, namespace(3)).expect("create marker");
        let store = BoundArtifactStore::open(store, namespace(3)).expect("bind store");
        assert_eq!(store.object_namespace(), Some(namespace(3)));
    }
}
