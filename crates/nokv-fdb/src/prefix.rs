/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::FdbConfigError;

const PHYSICAL_MAGIC: &[u8] = b"\x15nokv-fdb\x00";
const PHYSICAL_ENCODING_VERSION: u8 = 1;
const MAX_COMPONENT_BYTES: usize = u16::MAX as usize;
pub const MAX_STORE_PREFIX_BYTES: usize = 64;

/// Stable top-level NoKV FoundationDB subspaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FdbSubspaceKind {
    System = 1,
    CatalogRoot = 2,
    CatalogShard = 3,
    RouteShard = 4,
    LeaseSession = 5,
    LeaseHeartbeat = 6,
    Metadata = 7,
}

/// Versioned physical isolation envelope for one NoKV store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbStorePrefix {
    token: Vec<u8>,
    encoded: Vec<u8>,
}

impl FdbStorePrefix {
    pub fn new(token: impl AsRef<[u8]>) -> Result<Self, FdbConfigError> {
        let token = token.as_ref();
        if token.is_empty() || token.len() > MAX_STORE_PREFIX_BYTES {
            return Err(FdbConfigError::StorePrefixLength {
                actual: token.len(),
                minimum: 1,
                maximum: MAX_STORE_PREFIX_BYTES,
            });
        }
        let mut encoded = Vec::with_capacity(PHYSICAL_MAGIC.len() + 2 + token.len());
        encoded.extend_from_slice(PHYSICAL_MAGIC);
        encoded.push(PHYSICAL_ENCODING_VERSION);
        encoded.push(
            u8::try_from(token.len()).expect("validated FoundationDB store prefix fits one byte"),
        );
        encoded.extend_from_slice(token);
        Ok(Self {
            token: token.to_vec(),
            encoded,
        })
    }

    pub fn token(&self) -> &[u8] {
        &self.token
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    pub fn subspace(&self, kind: FdbSubspaceKind) -> FdbSubspace {
        let mut encoded = Vec::with_capacity(self.encoded.len() + 1);
        encoded.extend_from_slice(&self.encoded);
        encoded.push(kind as u8);
        FdbSubspace { encoded }
    }
}

/// Component-safe prefix below one stable store subspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbSubspace {
    encoded: Vec<u8>,
}

impl FdbSubspace {
    pub fn component(&self, component: &[u8]) -> Result<Self, FdbConfigError> {
        if component.len() > MAX_COMPONENT_BYTES {
            return Err(FdbConfigError::ComponentLength {
                actual: component.len(),
                maximum: MAX_COMPONENT_BYTES,
            });
        }
        let mut encoded = Vec::with_capacity(self.encoded.len() + 2 + component.len());
        encoded.extend_from_slice(&self.encoded);
        encoded.extend_from_slice(
            &u16::try_from(component.len())
                .expect("validated FoundationDB component fits u16")
                .to_be_bytes(),
        );
        encoded.extend_from_slice(component);
        Ok(Self { encoded })
    }

    pub fn key(&self, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.encoded.len() + suffix.len());
        key.extend_from_slice(&self.encoded);
        key.extend_from_slice(suffix);
        key
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }
}

pub fn lexicographic_successor(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut successor = bytes.to_vec();
    while let Some(byte) = successor.pop() {
        if byte != u8::MAX {
            successor.push(byte + 1);
            return Some(successor);
        }
    }
    None
}
