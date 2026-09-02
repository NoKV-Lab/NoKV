/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_control::{ControlError, OwnerSession};

use crate::codec::encode_session;
use crate::FdbControlKeys;

/// Exact stable session key/value predicate installed on owner-required FDB
/// metadata transactions. Heartbeat bytes are deliberately absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbSessionFence {
    session: OwnerSession,
    key: Vec<u8>,
    expected_value: Vec<u8>,
}

impl FdbSessionFence {
    pub fn new(keys: &FdbControlKeys, session: OwnerSession) -> Result<Self, ControlError> {
        let key = keys.session_key(&session.logical_shard_id());
        let expected_value = encode_session(&session)?;
        Ok(Self {
            session,
            key,
            expected_value,
        })
    }

    pub fn session(&self) -> &OwnerSession {
        &self.session
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn expected_value(&self) -> &[u8] {
        &self.expected_value
    }
}
