/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable, explicit owner-session resume tokens for the `nokv` executable.
//!
//! The token is a local operator artifact, not control-plane authority. A
//! resumed process must still prove the exact live etcd lease and every
//! control/runtime fence before `bootstrap_root_owner` renews the lease.

use std::fmt;

use nokv_control::{
    ConsistencyDomainId, LogicalShardLease, LogicalShardRecord, LogicalShardState,
    MetadataAuthorityFence, MetadataAuthorityGeneration, MetadataAuthorityId,
    MetadataAuthorityRecord, MetadataContractDigest, MetadataProviderProfileId, NodeId, OwnerEpoch,
    OwnerIncarnationId, PlacementGeneration, RootId, RootLayoutGeneration, RootLayoutProfile,
    RootPartitionId, RootPlacement,
};
use nokv_types::{LogicalShardId, RootPlacementLifecycle, SHA256_BYTES};
use serde::{Deserialize, Serialize};

const OWNER_SESSION_TOKEN_VERSION: u8 = 2;
const MAX_OWNER_BYTES: usize = 255;
const MAX_ENDPOINT_BYTES: usize = 1024;

/// Exact local evidence required to resume an established Serving owner.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerSessionToken {
    pub(crate) root_id: RootId,
    pub(crate) layout_profile: RootLayoutProfile,
    pub(crate) layout_generation: RootLayoutGeneration,
    pub(crate) partition_id: RootPartitionId,
    pub(crate) logical_shard_id: LogicalShardId,
    pub(crate) placement_generation: PlacementGeneration,
    pub(crate) endpoint: String,
    pub(crate) lease: LogicalShardLease,
    pub(crate) provider_profile_id: MetadataProviderProfileId,
    pub(crate) profile_fingerprint: [u8; SHA256_BYTES],
    pub(crate) consistency_domain_id: ConsistencyDomainId,
    pub(crate) contract_digest: MetadataContractDigest,
}

impl fmt::Debug for OwnerSessionToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionToken")
            .field("version", &OWNER_SESSION_TOKEN_VERSION)
            .field("contents", &"<redacted>")
            .finish()
    }
}

pub struct OwnerSessionResumeBinding<'a> {
    pub root_id: RootId,
    pub layout_profile: RootLayoutProfile,
    pub owner: &'a NodeId,
    pub endpoint: &'a str,
    pub metadata_profile: MetadataProviderProfileId,
    pub placement: &'a RootPlacement,
    pub shard: &'a LogicalShardRecord,
    pub authority: &'a MetadataAuthorityRecord,
}

#[derive(Debug, PartialEq, Eq)]
pub enum OwnerSessionError {
    InvalidToken(&'static str),
    BindingMismatch(&'static str),
}

impl fmt::Display for OwnerSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidToken(reason) => {
                write!(formatter, "invalid owner-session token: {reason}")
            }
            Self::BindingMismatch(field) => {
                write!(formatter, "owner-session binding mismatch: {field}")
            }
        }
    }
}

impl std::error::Error for OwnerSessionError {}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OwnerSessionTokenWire {
    version: u8,
    root_id: String,
    layout_profile: String,
    layout_generation: u64,
    partition_id: String,
    logical_shard_id: String,
    placement_generation: u64,
    owner: String,
    endpoint: String,
    owner_epoch: u64,
    owner_incarnation_id: String,
    lease_id: u64,
    authority_id: String,
    authority_generation: u64,
    provider_profile_id: String,
    profile_fingerprint: String,
    consistency_domain_id: String,
    contract_digest: String,
}

impl OwnerSessionToken {
    pub fn validate_process_binding(
        owner: &NodeId,
        endpoint: &str,
    ) -> Result<(), OwnerSessionError> {
        validate_text(owner.as_str(), MAX_OWNER_BYTES, "owner")?;
        validate_text(endpoint, MAX_ENDPOINT_BYTES, "endpoint")
    }

    pub fn from_serving(
        placement: &RootPlacement,
        serving: &LogicalShardRecord,
        lease: &LogicalShardLease,
        endpoint: &str,
        authority: &MetadataAuthorityRecord,
    ) -> Result<Self, OwnerSessionError> {
        if placement.lifecycle != RootPlacementLifecycle::Active {
            return Err(OwnerSessionError::BindingMismatch(
                "root placement is not Active",
            ));
        }
        if serving.state != LogicalShardState::Serving {
            return Err(OwnerSessionError::BindingMismatch(
                "logical shard is not Serving",
            ));
        }
        Self::validate_process_binding(&lease.owner, endpoint)?;
        if placement.logical_shard_id != lease.logical_shard_id
            || serving.logical_shard_id != lease.logical_shard_id
            || authority.logical_shard_id != lease.logical_shard_id
            || lease.authority.logical_shard_id != lease.logical_shard_id
        {
            return Err(OwnerSessionError::BindingMismatch("logical shard"));
        }
        if serving.owner.as_ref() != Some(&lease.owner)
            || serving.owner_epoch != Some(lease.owner_epoch)
            || serving.owner_incarnation_id != Some(lease.owner_incarnation_id)
            || serving.lease_id != lease.lease_id
            || serving.endpoint.as_deref() != Some(endpoint)
        {
            return Err(OwnerSessionError::BindingMismatch("serving owner lease"));
        }
        if lease.lease_id == 0 || lease.authority != authority.fence() {
            return Err(OwnerSessionError::BindingMismatch(
                "metadata authority fence",
            ));
        }
        let token = Self {
            root_id: placement.root_id,
            layout_profile: placement.layout_profile,
            layout_generation: placement.layout_generation,
            partition_id: placement.partition_id,
            logical_shard_id: placement.logical_shard_id,
            placement_generation: placement.placement_generation,
            endpoint: endpoint.to_owned(),
            lease: lease.clone(),
            provider_profile_id: authority.active.provider_profile_id.clone(),
            profile_fingerprint: authority.active.profile_fingerprint,
            consistency_domain_id: authority.active.consistency_domain_id,
            contract_digest: authority.active.contract_digest,
        };
        token.validate_internal()?;
        Ok(token)
    }

    pub fn lease(&self) -> &LogicalShardLease {
        &self.lease
    }

    /// Validate every process-local and control-plane input before the server
    /// is allowed to present this token as `OwnerAdmission::Resume`.
    pub fn validate_resume(
        &self,
        binding: OwnerSessionResumeBinding<'_>,
    ) -> Result<(), OwnerSessionError> {
        self.validate_internal()?;
        if self.root_id != binding.root_id {
            return Err(OwnerSessionError::BindingMismatch("root id"));
        }
        if self.layout_profile != binding.layout_profile {
            return Err(OwnerSessionError::BindingMismatch("root layout profile"));
        }
        if &self.lease.owner != binding.owner {
            return Err(OwnerSessionError::BindingMismatch("owner node"));
        }
        if self.endpoint != binding.endpoint {
            return Err(OwnerSessionError::BindingMismatch("advertise endpoint"));
        }
        if self.provider_profile_id != binding.metadata_profile {
            return Err(OwnerSessionError::BindingMismatch(
                "metadata runtime profile",
            ));
        }
        let placement = binding.placement;
        if placement.lifecycle != RootPlacementLifecycle::Active {
            return Err(OwnerSessionError::BindingMismatch(
                "root placement is not Active",
            ));
        }
        if self.root_id != placement.root_id
            || self.layout_profile != placement.layout_profile
            || self.layout_generation != placement.layout_generation
            || self.partition_id != placement.partition_id
            || self.logical_shard_id != placement.logical_shard_id
            || self.placement_generation != placement.placement_generation
        {
            return Err(OwnerSessionError::BindingMismatch("root placement fence"));
        }
        let shard = binding.shard;
        if shard.state != LogicalShardState::Serving {
            return Err(OwnerSessionError::BindingMismatch(
                "logical shard is not exact Serving",
            ));
        }
        if shard.logical_shard_id != self.logical_shard_id
            || shard.owner.as_ref() != Some(&self.lease.owner)
            || shard.owner_epoch != Some(self.lease.owner_epoch)
            || shard.owner_incarnation_id != Some(self.lease.owner_incarnation_id)
            || shard.lease_id != self.lease.lease_id
            || shard.endpoint.as_deref() != Some(self.endpoint.as_str())
        {
            return Err(OwnerSessionError::BindingMismatch("serving owner lease"));
        }
        let authority = binding.authority;
        if authority.logical_shard_id != self.logical_shard_id
            || authority.fence() != self.lease.authority
            || authority.active.provider_profile_id != self.provider_profile_id
            || authority.active.profile_fingerprint != self.profile_fingerprint
            || authority.active.consistency_domain_id != self.consistency_domain_id
            || authority.active.contract_digest != self.contract_digest
        {
            return Err(OwnerSessionError::BindingMismatch(
                "metadata authority binding",
            ));
        }
        Ok(())
    }

    fn validate_internal(&self) -> Result<(), OwnerSessionError> {
        validate_text(self.lease.owner.as_str(), MAX_OWNER_BYTES, "owner")?;
        validate_text(&self.endpoint, MAX_ENDPOINT_BYTES, "endpoint")?;
        if self.lease.lease_id == 0 {
            return Err(OwnerSessionError::InvalidToken("lease id must be non-zero"));
        }
        if self
            .lease
            .owner_incarnation_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        {
            return Err(OwnerSessionError::InvalidToken(
                "owner incarnation id must be non-zero",
            ));
        }
        if self.logical_shard_id != self.lease.logical_shard_id
            || self.logical_shard_id != self.lease.authority.logical_shard_id
        {
            return Err(OwnerSessionError::InvalidToken(
                "logical shard identities differ",
            ));
        }
        if self
            .lease
            .authority
            .authority_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        {
            return Err(OwnerSessionError::InvalidToken(
                "authority id must be non-zero",
            ));
        }
        if self.profile_fingerprint.iter().all(|byte| *byte == 0) {
            return Err(OwnerSessionError::InvalidToken(
                "profile fingerprint must be non-zero",
            ));
        }
        if self
            .consistency_domain_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        {
            return Err(OwnerSessionError::InvalidToken(
                "consistency domain id must be non-zero",
            ));
        }
        if self
            .contract_digest
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        {
            return Err(OwnerSessionError::InvalidToken(
                "metadata contract digest must be non-zero",
            ));
        }
        if self.layout_profile == RootLayoutProfile::SingleShardRoot
            && self.partition_id != RootPartitionId::SINGLE_SHARD
        {
            return Err(OwnerSessionError::InvalidToken(
                "SingleShardRoot has a foreign partition id",
            ));
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, OwnerSessionError> {
        self.validate_internal()?;
        serde_json::to_vec(&OwnerSessionTokenWire {
            version: OWNER_SESSION_TOKEN_VERSION,
            root_id: encode_hex(self.root_id.as_bytes()),
            layout_profile: encode_layout_profile(self.layout_profile).to_owned(),
            layout_generation: self.layout_generation.get(),
            partition_id: encode_hex(self.partition_id.as_bytes()),
            logical_shard_id: encode_hex(self.logical_shard_id.as_bytes()),
            placement_generation: self.placement_generation.get(),
            owner: self.lease.owner.as_str().to_owned(),
            endpoint: self.endpoint.clone(),
            owner_epoch: self.lease.owner_epoch.get(),
            owner_incarnation_id: encode_hex(self.lease.owner_incarnation_id.as_bytes()),
            lease_id: self.lease.lease_id,
            authority_id: encode_hex(self.lease.authority.authority_id.as_bytes()),
            authority_generation: self.lease.authority.authority_generation.get(),
            provider_profile_id: self.provider_profile_id.as_str().to_owned(),
            profile_fingerprint: encode_hex(&self.profile_fingerprint),
            consistency_domain_id: encode_hex(self.consistency_domain_id.as_bytes()),
            contract_digest: encode_hex(self.contract_digest.as_bytes()),
        })
        .map_err(|_| OwnerSessionError::InvalidToken("cannot encode canonical token"))
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, OwnerSessionError> {
        let wire: OwnerSessionTokenWire = serde_json::from_slice(encoded)
            .map_err(|_| OwnerSessionError::InvalidToken("malformed canonical JSON"))?;
        if wire.version != OWNER_SESSION_TOKEN_VERSION {
            return Err(OwnerSessionError::InvalidToken("unsupported token version"));
        }
        let layout_profile = decode_layout_profile(&wire.layout_profile)?;
        let logical_shard_id =
            LogicalShardId::from_bytes(decode_hex(&wire.logical_shard_id, "logical shard id")?);
        let authority_id = MetadataAuthorityId::from_bytes(decode_hex(
            &wire.authority_id,
            "metadata authority id",
        )?);
        let token = Self {
            root_id: RootId::from_bytes(decode_hex(&wire.root_id, "root id")?),
            layout_profile,
            layout_generation: RootLayoutGeneration::new(wire.layout_generation)
                .map_err(|_| OwnerSessionError::InvalidToken("invalid layout generation"))?,
            partition_id: RootPartitionId::from_bytes(decode_hex(
                &wire.partition_id,
                "partition id",
            )?),
            logical_shard_id,
            placement_generation: PlacementGeneration::new(wire.placement_generation)
                .map_err(|_| OwnerSessionError::InvalidToken("invalid placement generation"))?,
            endpoint: wire.endpoint,
            lease: LogicalShardLease {
                logical_shard_id,
                owner: NodeId::new(wire.owner)
                    .map_err(|_| OwnerSessionError::InvalidToken("invalid owner"))?,
                owner_epoch: OwnerEpoch::new(wire.owner_epoch)
                    .map_err(|_| OwnerSessionError::InvalidToken("invalid owner epoch"))?,
                owner_incarnation_id: OwnerIncarnationId::from_bytes(decode_hex(
                    &wire.owner_incarnation_id,
                    "owner incarnation id",
                )?),
                lease_id: wire.lease_id,
                authority: MetadataAuthorityFence {
                    logical_shard_id,
                    authority_id,
                    authority_generation: MetadataAuthorityGeneration::new(
                        wire.authority_generation,
                    )
                    .map_err(|_| OwnerSessionError::InvalidToken("invalid authority generation"))?,
                },
            },
            provider_profile_id: MetadataProviderProfileId::new(wire.provider_profile_id)
                .map_err(|_| OwnerSessionError::InvalidToken("invalid provider profile id"))?,
            profile_fingerprint: decode_hex(&wire.profile_fingerprint, "profile fingerprint")?,
            consistency_domain_id: ConsistencyDomainId::from_bytes(decode_hex(
                &wire.consistency_domain_id,
                "consistency domain id",
            )?),
            contract_digest: MetadataContractDigest::from_bytes(decode_hex(
                &wire.contract_digest,
                "metadata contract digest",
            )?),
        };
        token.validate_internal()?;
        if token.encode()?.as_slice() != encoded {
            return Err(OwnerSessionError::InvalidToken(
                "input is not the canonical token encoding",
            ));
        }
        Ok(token)
    }
}

fn validate_text(
    value: &str,
    max_bytes: usize,
    field: &'static str,
) -> Result<(), OwnerSessionError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(OwnerSessionError::InvalidToken(field));
    }
    Ok(())
}

fn encode_layout_profile(profile: RootLayoutProfile) -> &'static str {
    match profile {
        RootLayoutProfile::SingleShardRoot => "single-shard-root",
        RootLayoutProfile::PartitionedRoot => "partitioned-root",
    }
}

fn decode_layout_profile(value: &str) -> Result<RootLayoutProfile, OwnerSessionError> {
    match value {
        "single-shard-root" => Ok(RootLayoutProfile::SingleShardRoot),
        "partitioned-root" => Ok(RootLayoutProfile::PartitionedRoot),
        _ => Err(OwnerSessionError::InvalidToken(
            "unknown root layout profile",
        )),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex<const N: usize>(
    value: &str,
    field: &'static str,
) -> Result<[u8; N], OwnerSessionError> {
    if value.len() != N * 2 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(OwnerSessionError::InvalidToken(field));
    }
    let bytes = value.as_bytes();
    let mut decoded = [0_u8; N];
    for (index, output) in decoded.iter_mut().enumerate() {
        let high =
            decode_lower_hex(bytes[index * 2]).ok_or(OwnerSessionError::InvalidToken(field))?;
        let low =
            decode_lower_hex(bytes[index * 2 + 1]).ok_or(OwnerSessionError::InvalidToken(field))?;
        *output = (high << 4) | low;
    }
    Ok(decoded)
}

fn decode_lower_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holt_profile_id() -> MetadataProviderProfileId {
        MetadataProviderProfileId::new(nokv_server::HOLT_LOCAL_METADATA_PROFILE_ID).unwrap()
    }
    use nokv_control::{
        MetadataAuthorityBinding, MetadataAuthorityRevision, MetadataProviderProfileId,
    };
    use nokv_types::FIXED_ID_BYTES;

    fn token() -> OwnerSessionToken {
        OwnerSessionToken {
            root_id: RootId::from_bytes([0x01; FIXED_ID_BYTES]),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id: LogicalShardId::from_bytes([0x02; FIXED_ID_BYTES]),
            placement_generation: PlacementGeneration::new(2).unwrap(),
            endpoint: "metadata-a.internal:7750".to_owned(),
            lease: LogicalShardLease {
                logical_shard_id: LogicalShardId::from_bytes([0x02; FIXED_ID_BYTES]),
                owner: NodeId::new("owner-a").unwrap(),
                owner_epoch: OwnerEpoch::new(3).unwrap(),
                owner_incarnation_id: OwnerIncarnationId::from_bytes([0x33; FIXED_ID_BYTES]),
                lease_id: 44,
                authority: MetadataAuthorityFence {
                    logical_shard_id: LogicalShardId::from_bytes([0x02; FIXED_ID_BYTES]),
                    authority_id: MetadataAuthorityId::from_bytes([0xaa; FIXED_ID_BYTES]),
                    authority_generation: MetadataAuthorityGeneration::new(5).unwrap(),
                },
            },
            provider_profile_id: MetadataProviderProfileId::new("holt-local-v1").unwrap(),
            profile_fingerprint: [0x11; SHA256_BYTES],
            consistency_domain_id: ConsistencyDomainId::from_bytes([0xbb; FIXED_ID_BYTES]),
            contract_digest: MetadataContractDigest::from_bytes([0x22; SHA256_BYTES]),
        }
    }

    fn placement() -> RootPlacement {
        let token = token();
        RootPlacement {
            root_id: token.root_id,
            layout_profile: token.layout_profile,
            layout_generation: token.layout_generation,
            partition_id: token.partition_id,
            logical_shard_id: token.logical_shard_id,
            placement_generation: token.placement_generation,
            lifecycle: RootPlacementLifecycle::Active,
        }
    }

    fn shard() -> LogicalShardRecord {
        let token = token();
        LogicalShardRecord {
            logical_shard_id: token.logical_shard_id,
            owner: Some(token.lease.owner.clone()),
            owner_epoch: Some(token.lease.owner_epoch),
            owner_incarnation_id: Some(token.lease.owner_incarnation_id),
            lease_id: token.lease.lease_id,
            state: LogicalShardState::Serving,
            endpoint: Some(token.endpoint),
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        }
    }

    fn authority() -> MetadataAuthorityRecord {
        let token = token();
        MetadataAuthorityRecord {
            logical_shard_id: token.logical_shard_id,
            record_revision: MetadataAuthorityRevision::new(1).unwrap(),
            authority_generation: token.lease.authority.authority_generation,
            active: MetadataAuthorityBinding {
                authority_id: token.lease.authority.authority_id,
                provider_profile_id: token.provider_profile_id,
                profile_fingerprint: token.profile_fingerprint,
                consistency_domain_id: token.consistency_domain_id,
                contract_digest: token.contract_digest,
            },
            migration: None,
        }
    }

    #[test]
    fn codec_has_one_frozen_canonical_encoding() {
        let encoded = token().encode().unwrap();
        let expected = r#"{"version":2,"root_id":"01010101010101010101010101010101","layout_profile":"single-shard-root","layout_generation":1,"partition_id":"00000000000000000000000000000000","logical_shard_id":"02020202020202020202020202020202","placement_generation":2,"owner":"owner-a","endpoint":"metadata-a.internal:7750","owner_epoch":3,"owner_incarnation_id":"33333333333333333333333333333333","lease_id":44,"authority_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","authority_generation":5,"provider_profile_id":"holt-local-v1","profile_fingerprint":"1111111111111111111111111111111111111111111111111111111111111111","consistency_domain_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","contract_digest":"2222222222222222222222222222222222222222222222222222222222222222"}"#;
        assert_eq!(encoded, expected.as_bytes());
        assert_eq!(OwnerSessionToken::decode(&encoded).unwrap(), token());

        let mut trailing = encoded.clone();
        trailing.push(b'\n');
        assert!(OwnerSessionToken::decode(&trailing).is_err());

        let unknown = expected.replacen("{", r#"{"unknown":true,"#, 1);
        assert!(OwnerSessionToken::decode(unknown.as_bytes()).is_err());

        let legacy = expected
            .replacen(r#""version":2"#, r#""version":1"#, 1)
            .replace(
                r#","owner_incarnation_id":"33333333333333333333333333333333""#,
                "",
            );
        assert!(OwnerSessionToken::decode(legacy.as_bytes()).is_err());

        let future = expected.replacen(r#""version":2"#, r#""version":3"#, 1);
        assert!(OwnerSessionToken::decode(future.as_bytes()).is_err());

        let uppercase = expected.replacen("aaaaaaaa", "AAAAAAAA", 1);
        assert!(OwnerSessionToken::decode(uppercase.as_bytes()).is_err());
    }

    #[test]
    fn resume_requires_exact_cli_control_layout_and_authority_bindings() {
        let token = token();
        token
            .validate_resume(OwnerSessionResumeBinding {
                root_id: token.root_id,
                layout_profile: token.layout_profile,
                owner: &token.lease.owner,
                endpoint: &token.endpoint,
                metadata_profile: holt_profile_id(),
                placement: &placement(),
                shard: &shard(),
                authority: &authority(),
            })
            .unwrap();

        assert!(matches!(
            token.validate_resume(OwnerSessionResumeBinding {
                root_id: RootId::from_bytes([9; FIXED_ID_BYTES]),
                layout_profile: token.layout_profile,
                owner: &token.lease.owner,
                endpoint: &token.endpoint,
                metadata_profile: holt_profile_id(),
                placement: &placement(),
                shard: &shard(),
                authority: &authority(),
            }),
            Err(OwnerSessionError::BindingMismatch("root id"))
        ));
        assert!(matches!(
            token.validate_resume(OwnerSessionResumeBinding {
                root_id: token.root_id,
                layout_profile: RootLayoutProfile::PartitionedRoot,
                owner: &token.lease.owner,
                endpoint: &token.endpoint,
                metadata_profile: holt_profile_id(),
                placement: &placement(),
                shard: &shard(),
                authority: &authority(),
            }),
            Err(OwnerSessionError::BindingMismatch("root layout profile"))
        ));
        assert!(matches!(
            token.validate_resume(OwnerSessionResumeBinding {
                root_id: token.root_id,
                layout_profile: token.layout_profile,
                owner: &token.lease.owner,
                endpoint: "foreign.internal:7750",
                metadata_profile: holt_profile_id(),
                placement: &placement(),
                shard: &shard(),
                authority: &authority(),
            }),
            Err(OwnerSessionError::BindingMismatch("advertise endpoint"))
        ));

        let mut foreign_profile = token.clone();
        foreign_profile.provider_profile_id =
            MetadataProviderProfileId::new("foundationdb-v1").unwrap();
        assert!(matches!(
            foreign_profile.validate_resume(OwnerSessionResumeBinding {
                root_id: token.root_id,
                layout_profile: token.layout_profile,
                owner: &token.lease.owner,
                endpoint: &token.endpoint,
                metadata_profile: holt_profile_id(),
                placement: &placement(),
                shard: &shard(),
                authority: &authority(),
            }),
            Err(OwnerSessionError::BindingMismatch(
                "metadata runtime profile"
            ))
        ));

        let mut foreign_authority = authority();
        foreign_authority.active.profile_fingerprint = [0x33; SHA256_BYTES];
        assert!(matches!(
            token.validate_resume(OwnerSessionResumeBinding {
                root_id: token.root_id,
                layout_profile: token.layout_profile,
                owner: &token.lease.owner,
                endpoint: &token.endpoint,
                metadata_profile: holt_profile_id(),
                placement: &placement(),
                shard: &shard(),
                authority: &foreign_authority,
            }),
            Err(OwnerSessionError::BindingMismatch(
                "metadata authority binding"
            ))
        ));

        let mut foreign_shard = shard();
        foreign_shard.state = LogicalShardState::Recovering;
        assert!(matches!(
            token.validate_resume(OwnerSessionResumeBinding {
                root_id: token.root_id,
                layout_profile: token.layout_profile,
                owner: &token.lease.owner,
                endpoint: &token.endpoint,
                metadata_profile: holt_profile_id(),
                placement: &placement(),
                shard: &foreign_shard,
                authority: &authority(),
            }),
            Err(OwnerSessionError::BindingMismatch(
                "logical shard is not exact Serving"
            ))
        ));

        let mut foreign_incarnation = shard();
        foreign_incarnation.owner_incarnation_id =
            Some(OwnerIncarnationId::from_bytes([0x44; FIXED_ID_BYTES]));
        assert!(matches!(
            token.validate_resume(OwnerSessionResumeBinding {
                root_id: token.root_id,
                layout_profile: token.layout_profile,
                owner: &token.lease.owner,
                endpoint: &token.endpoint,
                metadata_profile: holt_profile_id(),
                placement: &placement(),
                shard: &foreign_incarnation,
                authority: &authority(),
            }),
            Err(OwnerSessionError::BindingMismatch("serving owner lease"))
        ));

        for invalid in [
            {
                let mut invalid = token.clone();
                invalid.profile_fingerprint = [0; SHA256_BYTES];
                invalid
            },
            {
                let mut invalid = token.clone();
                invalid.consistency_domain_id =
                    ConsistencyDomainId::from_bytes([0; FIXED_ID_BYTES]);
                invalid
            },
            {
                let mut invalid = token.clone();
                invalid.contract_digest = MetadataContractDigest::from_bytes([0; SHA256_BYTES]);
                invalid
            },
            {
                let mut invalid = token.clone();
                invalid.lease.owner_incarnation_id =
                    OwnerIncarnationId::from_bytes([0; FIXED_ID_BYTES]);
                invalid
            },
        ] {
            assert!(matches!(
                invalid.validate_internal(),
                Err(OwnerSessionError::InvalidToken(_))
            ));
        }
    }

    #[test]
    fn in_memory_exact_resume_never_falls_back_after_lease_release() {
        use nokv_control::{
            ControlStore, InMemoryControlStore, OwnerServingAdmission, RecoveryPublication,
        };

        let control = InMemoryControlStore::new();
        let runtime = nokv_server::holt_runtime_descriptor().unwrap();
        let root_id = RootId::from_bytes([0x31; FIXED_ID_BYTES]);
        let logical_shard_id = LogicalShardId::from_bytes([0x32; FIXED_ID_BYTES]);
        let provisioning = RootPlacement {
            root_id,
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id,
            placement_generation: PlacementGeneration::new(1).unwrap(),
            lifecycle: RootPlacementLifecycle::Provisioning,
        };
        let authority = runtime.initial_authority(logical_shard_id);
        control
            .provision_fresh_root(provisioning.clone(), authority.clone())
            .unwrap();
        let active = control
            .compare_and_set_root_placement(
                &provisioning,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(2).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..provisioning
                },
            )
            .unwrap();
        let serving_admission =
            OwnerServingAdmission::stable(active.clone(), authority.clone()).unwrap();
        let lease = control
            .acquire_owner(
                &serving_admission,
                NodeId::new("owner-resume").unwrap(),
                OwnerIncarnationId::from_bytes([0x33; FIXED_ID_BYTES]),
                "metadata-resume.internal:7750".to_owned(),
            )
            .unwrap();
        let serving = control
            .mark_serving(
                &lease,
                &serving_admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            )
            .unwrap();
        let token = OwnerSessionToken::from_serving(
            &active,
            &serving,
            &lease,
            "metadata-resume.internal:7750",
            &authority,
        )
        .unwrap();
        control
            .renew_owner(token.lease(), &serving_admission)
            .unwrap();

        control.release_owner(&lease).unwrap();
        let released = control
            .get_logical_shard(&logical_shard_id)
            .unwrap()
            .unwrap();
        assert!(matches!(
            token.validate_resume(OwnerSessionResumeBinding {
                root_id,
                layout_profile: RootLayoutProfile::SingleShardRoot,
                owner: &lease.owner,
                endpoint: "metadata-resume.internal:7750",
                metadata_profile: holt_profile_id(),
                placement: &active,
                shard: &released,
                authority: &authority,
            }),
            Err(OwnerSessionError::BindingMismatch(
                "logical shard is not exact Serving"
            ))
        ));
        assert!(control
            .renew_owner(token.lease(), &serving_admission)
            .is_err());
        let unchanged = control
            .get_logical_shard(&logical_shard_id)
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.owner, None);
        assert_eq!(unchanged.owner_epoch, Some(lease.owner_epoch));
        assert_eq!(
            unchanged.owner_incarnation_id,
            Some(lease.owner_incarnation_id)
        );
        assert_eq!(unchanged.lease_id, 0);
    }
}
