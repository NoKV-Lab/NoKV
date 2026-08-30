/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#[cfg(feature = "fdb")]
use nokv_meta_store::Key;
use nokv_meta_store::{Keyspace, Scan, StoreError};

const PHYSICAL_MAGIC: &[u8] = b"\x15nokv-meta-fdb\x00";
const PHYSICAL_ENCODING_VERSION: u8 = 1;

#[derive(Clone, Debug)]
pub(crate) struct KeyCodec {
    store_prefix: Vec<u8>,
}

impl KeyCodec {
    pub(crate) fn new(namespace: &[u8]) -> Self {
        let mut store_prefix = Vec::with_capacity(PHYSICAL_MAGIC.len() + 2 + namespace.len());
        store_prefix.extend_from_slice(PHYSICAL_MAGIC);
        store_prefix.push(PHYSICAL_ENCODING_VERSION);
        store_prefix.push(
            u8::try_from(namespace.len()).expect("validated FdbStore namespace fits one byte"),
        );
        store_prefix.extend_from_slice(namespace);
        Self { store_prefix }
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
        self.store_prefix
            .len()
            .checked_add(2)
            .and_then(|bytes| bytes.checked_add(logical_key_bytes))
            .ok_or_else(|| {
                StoreError::InvalidRequest(
                    "FdbStore physical key length overflows usize".to_owned(),
                )
            })
    }

    pub(crate) fn keyspace_prefix(&self, keyspace: Keyspace) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(self.store_prefix.len() + 2);
        prefix.extend_from_slice(&self.store_prefix);
        prefix.extend_from_slice(&keyspace.get().to_be_bytes());
        prefix
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
        &self.store_prefix
    }
}

pub(crate) fn lexicographic_successor(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut successor = bytes.to_vec();
    while let Some(byte) = successor.pop() {
        if byte != u8::MAX {
            successor.push(byte + 1);
            return Some(successor);
        }
    }
    None
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
