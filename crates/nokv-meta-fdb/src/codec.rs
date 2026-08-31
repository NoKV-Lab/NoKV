/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_fdb::{lexicographic_successor, FdbStorePrefix, FdbSubspace, FdbSubspaceKind};
#[cfg(feature = "fdb")]
use nokv_meta_store::Key;
use nokv_meta_store::{Keyspace, Scan, StoreError};

#[derive(Clone, Debug)]
pub(crate) struct KeyCodec {
    _store_prefix: FdbStorePrefix,
    metadata: FdbSubspace,
}

impl KeyCodec {
    pub(crate) fn new(namespace: &[u8]) -> Result<Self, StoreError> {
        let store_prefix = FdbStorePrefix::new(namespace)
            .map_err(|error| StoreError::InvalidRequest(error.to_string()))?;
        let metadata = store_prefix.subspace(FdbSubspaceKind::Metadata);
        Ok(Self {
            _store_prefix: store_prefix,
            metadata,
        })
    }

    #[cfg(feature = "fdb")]
    pub(crate) fn encode_key(&self, key: &Key) -> Vec<u8> {
        self.encode(key.keyspace, &key.bytes)
    }

    pub(crate) fn encode(&self, keyspace: Keyspace, logical_key: &[u8]) -> Vec<u8> {
        let mut encoded = self.keyspace_prefix(keyspace);
        encoded.extend_from_slice(logical_key);
        encoded
    }

    pub(crate) fn encoded_len(&self, logical_key_bytes: usize) -> Result<usize, StoreError> {
        self.metadata
            .as_bytes()
            .len()
            .checked_add(4)
            .and_then(|bytes| bytes.checked_add(logical_key_bytes))
            .ok_or_else(|| {
                StoreError::InvalidRequest(
                    "FdbStore physical key length overflows usize".to_owned(),
                )
            })
    }

    pub(crate) fn keyspace_prefix(&self, keyspace: Keyspace) -> Vec<u8> {
        self.metadata
            .component(&keyspace.get().to_be_bytes())
            .expect("a two-byte keyspace is a valid FDB component")
            .as_bytes()
            .to_vec()
    }

    pub(crate) fn scan_bounds(&self, scan: &Scan) -> Result<(Vec<u8>, Vec<u8>), StoreError> {
        let encoded_prefix = self.encode(scan.keyspace, &scan.prefix);
        let end = lexicographic_successor(&encoded_prefix).ok_or_else(|| {
            StoreError::InvalidRequest(
                "FdbStore scan prefix has no lexicographic successor".to_owned(),
            )
        })?;
        let begin = match &scan.after {
            None => encoded_prefix,
            Some(after) => {
                let encoded_after = self.encode(scan.keyspace, after);
                if is_common_prefix_cursor(scan, after) {
                    lexicographic_successor(&encoded_after).ok_or_else(|| {
                        StoreError::InvalidRequest(
                            "FdbStore common-prefix cursor has no successor".to_owned(),
                        )
                    })?
                } else {
                    let mut successor = encoded_after;
                    successor.push(0);
                    successor
                }
            }
        };
        Ok((begin, end))
    }

    pub(crate) fn decode_key(
        &self,
        keyspace: Keyspace,
        physical_key: &[u8],
    ) -> Result<Vec<u8>, StoreError> {
        let prefix = self.keyspace_prefix(keyspace);
        physical_key
            .strip_prefix(prefix.as_slice())
            .map(<[u8]>::to_vec)
            .ok_or_else(|| {
                StoreError::Corrupt(format!(
                    "FoundationDB returned a key outside keyspace {:04x}",
                    keyspace.get()
                ))
            })
    }

    #[cfg(test)]
    pub(crate) fn store_prefix(&self) -> &[u8] {
        self._store_prefix.as_bytes()
    }
}

fn is_common_prefix_cursor(scan: &Scan, after: &[u8]) -> bool {
    let Some(delimiter) = scan.delimiter else {
        return false;
    };
    let Some(suffix) = after.strip_prefix(scan.prefix.as_slice()) else {
        return false;
    };
    suffix
        .iter()
        .position(|byte| *byte == delimiter)
        .is_some_and(|offset| offset + 1 == suffix.len())
}
