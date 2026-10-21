/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Whole-body SHA-256 that resumes from a body prefix.
//!
//! An artifact descriptor commits to the SHA-256 of its complete body. The
//! chaining state after a prefix, with the prefix length and its unprocessed
//! tail, continues that digest over appended bytes, so an append hashes only
//! the bytes it has not seen instead of re-reading the whole base body.

use std::collections::{HashMap, VecDeque};

use nokv_protocol::{ArtifactManifestRow, ArtifactRevisionIdentity};
use sha2::block_api::compress256;
use sha2::{Digest as _, Sha256};

const BLOCK_BYTES: usize = 64;
const INITIAL_STATE: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];
/// Revisions whose resumable digests one client keeps.
const MAX_CACHED_BODY_DIGESTS: usize = 256;

/// SHA-256 over a byte stream that can continue after any prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BodyDigestState {
    state: [u32; 8],
    pending: [u8; BLOCK_BYTES],
    pending_len: usize,
    length: u64,
}

impl BodyDigestState {
    pub(crate) fn new() -> Self {
        Self {
            state: INITIAL_STATE,
            pending: [0; BLOCK_BYTES],
            pending_len: 0,
            length: 0,
        }
    }

    /// Bytes hashed so far.
    pub(crate) fn length(&self) -> u64 {
        self.length
    }

    pub(crate) fn update(&mut self, mut bytes: &[u8]) {
        self.length += bytes.len() as u64;
        if self.pending_len > 0 {
            let take = (BLOCK_BYTES - self.pending_len).min(bytes.len());
            self.pending[self.pending_len..self.pending_len + take].copy_from_slice(&bytes[..take]);
            self.pending_len += take;
            bytes = &bytes[take..];
            if self.pending_len < BLOCK_BYTES {
                return;
            }
            compress256(&mut self.state, std::slice::from_ref(&self.pending));
            self.pending_len = 0;
        }
        let (blocks, rest) = bytes.as_chunks::<BLOCK_BYTES>();
        compress256(&mut self.state, blocks);
        self.pending[..rest.len()].copy_from_slice(rest);
        self.pending_len = rest.len();
    }

    /// The SHA-256 of every byte hashed so far; the state stays resumable.
    pub(crate) fn digest(&self) -> [u8; 32] {
        let mut state = self.state;
        let mut tail = [0_u8; 2 * BLOCK_BYTES];
        tail[..self.pending_len].copy_from_slice(&self.pending[..self.pending_len]);
        tail[self.pending_len] = 0x80;
        let tail_len = if self.pending_len < BLOCK_BYTES - 8 {
            BLOCK_BYTES
        } else {
            2 * BLOCK_BYTES
        };
        tail[tail_len - 8..tail_len].copy_from_slice(&(self.length * 8).to_be_bytes());
        let (blocks, _) = tail[..tail_len].as_chunks::<BLOCK_BYTES>();
        compress256(&mut state, blocks);
        let mut digest = [0_u8; 32];
        let (words, _) = digest.as_chunks_mut::<4>();
        for (word, out) in state.iter().zip(words) {
            *out = word.to_be_bytes();
        }
        digest
    }
}

/// Commitment to the manifest rows that describe a body.
///
/// Rows name immutable, digest-verified objects, so two bodies whose row
/// prefixes commit equally hold the same bytes over that prefix.
#[derive(Clone)]
pub(crate) struct RowsCommitment {
    hasher: Sha256,
    rows: usize,
}

impl RowsCommitment {
    pub(crate) fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.client.body-rows.v1\0");
        Self { hasher, rows: 0 }
    }

    pub(crate) fn push(&mut self, row: &ArtifactManifestRow) {
        self.hasher.update(row.physical_owner_revision_id.0);
        self.hasher.update(row.physical_object_index.to_be_bytes());
        let object = row.object_identity.as_str().as_bytes();
        self.hasher.update((object.len() as u64).to_be_bytes());
        self.hasher.update(object);
        self.hasher.update(row.logical_offset.to_be_bytes());
        self.hasher.update(row.object_offset.to_be_bytes());
        self.hasher.update(row.length.to_be_bytes());
        let digest = row.digest.as_str().as_bytes();
        self.hasher.update((digest.len() as u64).to_be_bytes());
        self.hasher.update(digest);
        self.rows += 1;
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    pub(crate) fn finish(&self) -> [u8; 32] {
        self.hasher.clone().finalize().into()
    }

    pub(crate) fn of(rows: &[ArtifactManifestRow]) -> [u8; 32] {
        let mut commitment = Self::new();
        for row in rows {
            commitment.push(row);
        }
        commitment.finish()
    }
}

#[derive(Clone, Debug)]
struct CachedBodyDigest {
    state: BodyDigestState,
    rows: usize,
    rows_commitment: [u8; 32],
}

/// Resumable body digests of the revisions one client recently published or
/// read, keyed by the immutable revision identity.
#[derive(Debug, Default)]
pub(crate) struct BodyDigestCache {
    entries: HashMap<ArtifactRevisionIdentity, CachedBodyDigest>,
    order: VecDeque<ArtifactRevisionIdentity>,
}

impl BodyDigestCache {
    pub(crate) fn insert(
        &mut self,
        revision: ArtifactRevisionIdentity,
        state: BodyDigestState,
        rows: &[ArtifactManifestRow],
    ) {
        let entry = CachedBodyDigest {
            state,
            rows: rows.len(),
            rows_commitment: RowsCommitment::of(rows),
        };
        if self.entries.insert(revision, entry).is_none() {
            self.order.push_back(revision);
            while self.order.len() > MAX_CACHED_BODY_DIGESTS {
                if let Some(evicted) = self.order.pop_front() {
                    self.entries.remove(&evicted);
                }
            }
        }
    }

    /// The longest cached prefix of `rows`: the number of rows it covers and
    /// the digest state after them. Candidates are the body of each owner
    /// whose rows end a run of `rows`, matched by row commitment.
    pub(crate) fn longest_prefix(
        &self,
        rows: &[ArtifactManifestRow],
    ) -> Option<(usize, BodyDigestState)> {
        let mut commitment = RowsCommitment::new();
        let mut found = None;
        for (index, row) in rows.iter().enumerate() {
            commitment.push(row);
            let ends_run = rows.get(index + 1).is_none_or(|next| {
                next.physical_owner_revision_id != row.physical_owner_revision_id
            });
            if !ends_run {
                continue;
            }
            let Some(entry) = self.entries.get(&row.physical_owner_revision_id) else {
                continue;
            };
            if entry.rows == commitment.rows() && entry.rows_commitment == commitment.finish() {
                found = Some((entry.rows, entry.state.clone()));
            }
        }
        found
    }

    /// The cached digest of exactly `rows` under `revision`.
    pub(crate) fn exact(
        &self,
        revision: ArtifactRevisionIdentity,
        rows: &[ArtifactManifestRow],
    ) -> Option<BodyDigestState> {
        let entry = self.entries.get(&revision)?;
        (entry.rows == rows.len() && entry.rows_commitment == RowsCommitment::of(rows))
            .then(|| entry.state.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
            .collect()
    }

    #[test]
    fn resumed_digest_equals_sha256_at_every_split() {
        for len in [0, 1, 55, 56, 63, 64, 65, 119, 120, 127, 128, 1_000, 4_097] {
            let body = bytes(len, len as u8);
            let expected: [u8; 32] = Sha256::digest(&body).into();
            for split in [0, 1, len / 3, len / 2, len.saturating_sub(1), len] {
                let split = split.min(len);
                let mut state = BodyDigestState::new();
                state.update(&body[..split]);
                let resumed = state.clone();
                let mut continued = resumed;
                continued.update(&body[split..]);
                assert_eq!(continued.digest(), expected, "len {len} split {split}");
                assert_eq!(continued.length(), len as u64);
                // Taking a digest does not consume the resumable state.
                assert_eq!(
                    state.digest(),
                    <[u8; 32]>::from(Sha256::digest(&body[..split]))
                );
            }
        }
    }
}
