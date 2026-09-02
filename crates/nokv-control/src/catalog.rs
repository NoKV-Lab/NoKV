/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{
    AgentId, ControlError, LogicalShardId, ObjectNamespaceId, PlacementGeneration, RootId,
};

pub const STORE_ID_BYTES: usize = 16;
pub const PROVIDER_NAMESPACE_DIGEST_BYTES: usize = 32;
pub const MAX_CREATED_BY_VERSION_BYTES: usize = 128;
pub const SUPPORTED_WORKSPACE_FORMAT_VERSION: u32 = 12;

/// Stable identity generated once when a metadata store is formatted.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreId([u8; STORE_ID_BYTES]);

impl StoreId {
    pub const fn from_bytes(bytes: [u8; STORE_ID_BYTES]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; STORE_ID_BYTES] {
        &self.0
    }
}

/// Physical provider durably bound to one formatted store.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoreProvider {
    Holt = 1,
    FoundationDb = 2,
}

impl TryFrom<u8> for StoreProvider {
    type Error = ControlError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Holt),
            2 => Ok(Self::FoundationDb),
            value => Err(ControlError::InvalidRecord(format!(
                "unknown store provider discriminant {value}"
            ))),
        }
    }
}

/// Durable store admission marker shared by the standalone and FDB modes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreManifest {
    store_id: StoreId,
    provider: StoreProvider,
    workspace_format_version: u32,
    physical_encoding_version: u8,
    provider_namespace_digest: [u8; PROVIDER_NAMESPACE_DIGEST_BYTES],
    created_by_version: String,
}

impl StoreManifest {
    pub fn new(
        store_id: StoreId,
        provider: StoreProvider,
        workspace_format_version: u32,
        physical_encoding_version: u8,
        provider_namespace_digest: [u8; PROVIDER_NAMESPACE_DIGEST_BYTES],
        created_by_version: impl Into<String>,
    ) -> Result<Self, ControlError> {
        if store_id.as_bytes().iter().all(|byte| *byte == 0) {
            return Err(ControlError::InvalidRecord(
                "store id must not be all zero".to_owned(),
            ));
        }
        if workspace_format_version == 0 {
            return Err(ControlError::InvalidRecord(
                "workspace format version must be nonzero".to_owned(),
            ));
        }
        if physical_encoding_version == 0 {
            return Err(ControlError::InvalidRecord(
                "physical encoding version must be nonzero".to_owned(),
            ));
        }
        if provider_namespace_digest.iter().all(|byte| *byte == 0) {
            return Err(ControlError::InvalidRecord(
                "provider namespace digest must not be all zero".to_owned(),
            ));
        }
        let created_by_version = created_by_version.into();
        if created_by_version.is_empty()
            || created_by_version.len() > MAX_CREATED_BY_VERSION_BYTES
            || created_by_version.trim() != created_by_version
            || created_by_version.chars().any(char::is_control)
            || !created_by_version.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+' | b'_')
            })
        {
            return Err(ControlError::InvalidRecord(format!(
                "created-by version must contain 1..={MAX_CREATED_BY_VERSION_BYTES} canonical bytes"
            )));
        }
        Ok(Self {
            store_id,
            provider,
            workspace_format_version,
            physical_encoding_version,
            provider_namespace_digest,
            created_by_version,
        })
    }

    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    pub const fn provider(&self) -> StoreProvider {
        self.provider
    }

    pub const fn workspace_format_version(&self) -> u32 {
        self.workspace_format_version
    }

    pub const fn physical_encoding_version(&self) -> u8 {
        self.physical_encoding_version
    }

    pub const fn provider_namespace_digest(&self) -> &[u8; PROVIDER_NAMESPACE_DIGEST_BYTES] {
        &self.provider_namespace_digest
    }

    pub fn created_by_version(&self) -> &str {
        &self.created_by_version
    }
}

/// Provisioning lifecycle for root and logical-shard catalog entries.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CatalogEntryState {
    Provisioning = 1,
    Ready = 2,
    Retired = 3,
}

impl TryFrom<u8> for CatalogEntryState {
    type Error = ControlError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Provisioning),
            2 => Ok(Self::Ready),
            3 => Ok(Self::Retired),
            value => Err(ControlError::InvalidRecord(format!(
                "unknown catalog entry state discriminant {value}"
            ))),
        }
    }
}

/// One create-only root catalog record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootCatalogEntry {
    root_id: RootId,
    agent_id: AgentId,
    object_namespace_id: ObjectNamespaceId,
    logical_shard_id: LogicalShardId,
    placement_generation: PlacementGeneration,
    state: CatalogEntryState,
}

impl RootCatalogEntry {
    pub const fn new(
        root_id: RootId,
        agent_id: AgentId,
        object_namespace_id: ObjectNamespaceId,
        logical_shard_id: LogicalShardId,
        placement_generation: PlacementGeneration,
        state: CatalogEntryState,
    ) -> Self {
        Self {
            root_id,
            agent_id,
            object_namespace_id,
            logical_shard_id,
            placement_generation,
            state,
        }
    }

    pub const fn root_id(&self) -> RootId {
        self.root_id
    }

    pub const fn agent_id(&self) -> AgentId {
        self.agent_id
    }

    pub const fn object_namespace_id(&self) -> ObjectNamespaceId {
        self.object_namespace_id
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub const fn placement_generation(&self) -> PlacementGeneration {
        self.placement_generation
    }

    pub const fn state(&self) -> CatalogEntryState {
        self.state
    }

    pub const fn with_state(self, state: CatalogEntryState) -> Self {
        Self { state, ..self }
    }
}

/// One create-only logical-shard catalog record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardCatalogEntry {
    logical_shard_id: LogicalShardId,
    state: CatalogEntryState,
}

impl ShardCatalogEntry {
    pub const fn new(logical_shard_id: LogicalShardId, state: CatalogEntryState) -> Self {
        Self {
            logical_shard_id,
            state,
        }
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub const fn state(&self) -> CatalogEntryState {
        self.state
    }

    pub const fn with_state(self, state: CatalogEntryState) -> Self {
        Self { state, ..self }
    }
}

/// Result of a create-only record write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateOutcome<T> {
    Created(T),
    Existing(T),
}

pub fn validate_root_catalog_transition(
    expected: &RootCatalogEntry,
    next: &RootCatalogEntry,
) -> Result<(), ControlError> {
    if expected.root_id != next.root_id
        || expected.agent_id != next.agent_id
        || expected.object_namespace_id != next.object_namespace_id
        || expected.logical_shard_id != next.logical_shard_id
        || expected.placement_generation != next.placement_generation
    {
        return Err(ControlError::InvalidCatalogTransition {
            record: "root catalog",
            reason: "identity and placement fields are immutable".to_owned(),
        });
    }
    validate_catalog_state_transition("root catalog", expected.state, next.state)
}

pub fn validate_shard_catalog_transition(
    expected: &ShardCatalogEntry,
    next: &ShardCatalogEntry,
) -> Result<(), ControlError> {
    if expected.logical_shard_id != next.logical_shard_id {
        return Err(ControlError::InvalidCatalogTransition {
            record: "shard catalog",
            reason: "logical shard identity is immutable".to_owned(),
        });
    }
    validate_catalog_state_transition("shard catalog", expected.state, next.state)
}

fn validate_catalog_state_transition(
    record: &'static str,
    expected: CatalogEntryState,
    next: CatalogEntryState,
) -> Result<(), ControlError> {
    let valid = expected == next
        || matches!(
            (expected, next),
            (CatalogEntryState::Provisioning, CatalogEntryState::Ready)
                | (CatalogEntryState::Provisioning, CatalogEntryState::Retired)
                | (CatalogEntryState::Ready, CatalogEntryState::Retired)
        );
    if valid {
        Ok(())
    } else {
        Err(ControlError::InvalidCatalogTransition {
            record,
            reason: format!("cannot move from {expected:?} to {next:?}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(value: u8) -> RootId {
        RootId::from_bytes([value; 16])
    }

    fn shard(value: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([value; 16])
    }

    #[test]
    fn manifest_and_catalog_transitions_fail_closed() {
        assert!(StoreManifest::new(
            StoreId::from_bytes([1; STORE_ID_BYTES]),
            StoreProvider::FoundationDb,
            SUPPORTED_WORKSPACE_FORMAT_VERSION,
            1,
            [2; PROVIDER_NAMESPACE_DIGEST_BYTES],
            "0.11.0",
        )
        .is_ok());
        assert!(StoreManifest::new(
            StoreId::from_bytes([1; STORE_ID_BYTES]),
            StoreProvider::FoundationDb,
            0,
            1,
            [2; PROVIDER_NAMESPACE_DIGEST_BYTES],
            "0.11.0",
        )
        .is_err());
        assert!(StoreManifest::new(
            StoreId::from_bytes([0; STORE_ID_BYTES]),
            StoreProvider::FoundationDb,
            SUPPORTED_WORKSPACE_FORMAT_VERSION,
            1,
            [2; PROVIDER_NAMESPACE_DIGEST_BYTES],
            "0.11.0",
        )
        .is_err());

        let provisioning = RootCatalogEntry::new(
            root(1),
            AgentId::from_bytes([4; 16]),
            ObjectNamespaceId::from_bytes([2; 16]),
            shard(3),
            PlacementGeneration::new(1).unwrap(),
            CatalogEntryState::Provisioning,
        );
        let ready = provisioning.with_state(CatalogEntryState::Ready);
        validate_root_catalog_transition(&provisioning, &ready).unwrap();
        assert!(validate_root_catalog_transition(&ready, &provisioning).is_err());

        let rebound = RootCatalogEntry::new(
            root(1),
            AgentId::from_bytes([4; 16]),
            ObjectNamespaceId::from_bytes([9; 16]),
            shard(3),
            PlacementGeneration::new(1).unwrap(),
            CatalogEntryState::Ready,
        );
        assert!(validate_root_catalog_transition(&provisioning, &rebound).is_err());

        let rebound_agent = RootCatalogEntry::new(
            root(1),
            AgentId::from_bytes([5; 16]),
            ObjectNamespaceId::from_bytes([2; 16]),
            shard(3),
            PlacementGeneration::new(1).unwrap(),
            CatalogEntryState::Ready,
        );
        assert!(validate_root_catalog_transition(&provisioning, &rebound_agent).is_err());
    }
}
