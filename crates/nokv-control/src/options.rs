#[cfg(any(feature = "etcd", test))]
use crate::codec::encode_fixed_id;
use crate::ControlError;
#[cfg(any(feature = "etcd", test))]
use crate::{LogicalShardId, OwnerIncarnationId, RootId};

const DEFAULT_ETCD_KEY_PREFIX: &str = "/nokv/control";
const DEFAULT_ETCD_LEASE_TTL_SECONDS: i64 = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EtcdControlStoreOptions {
    endpoints: Vec<String>,
    key_prefix: String,
    lease_ttl_seconds: i64,
}

impl EtcdControlStoreOptions {
    pub fn new(endpoints: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            endpoints: endpoints.into_iter().map(Into::into).collect(),
            key_prefix: DEFAULT_ETCD_KEY_PREFIX.to_owned(),
            lease_ttl_seconds: DEFAULT_ETCD_LEASE_TTL_SECONDS,
        }
    }

    pub fn with_key_prefix(mut self, key_prefix: impl Into<String>) -> Self {
        self.key_prefix = key_prefix.into();
        self
    }

    pub fn with_lease_ttl_seconds(mut self, lease_ttl_seconds: i64) -> Self {
        self.lease_ttl_seconds = lease_ttl_seconds;
        self
    }

    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    pub fn lease_ttl_seconds(&self) -> i64 {
        self.lease_ttl_seconds
    }

    pub fn validate(&self) -> Result<(), ControlError> {
        if self.endpoints.is_empty() {
            return Err(ControlError::InvalidOptions(
                "at least one etcd endpoint is required".to_owned(),
            ));
        }
        if self
            .endpoints
            .iter()
            .any(|endpoint| endpoint.trim().is_empty())
        {
            return Err(ControlError::InvalidOptions(
                "etcd endpoints must not be empty".to_owned(),
            ));
        }
        if self.normalized_key_prefix().is_empty() {
            return Err(ControlError::InvalidOptions(
                "etcd key prefix must not be empty".to_owned(),
            ));
        }
        if self.lease_ttl_seconds <= 0 {
            return Err(ControlError::InvalidOptions(
                "etcd lease TTL must be positive".to_owned(),
            ));
        }
        Ok(())
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn root_placement_key(&self, root_id: &RootId) -> Vec<u8> {
        format!(
            "{}/roots/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(root_id.as_bytes())
        )
        .into_bytes()
    }

    #[cfg(feature = "etcd")]
    pub(crate) fn root_placements_prefix(&self) -> Vec<u8> {
        format!("{}/roots/", self.normalized_key_prefix()).into_bytes()
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn logical_shard_record_key(&self, logical_shard_id: &LogicalShardId) -> Vec<u8> {
        format!(
            "{}/logical-shards/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(logical_shard_id.as_bytes())
        )
        .into_bytes()
    }

    #[cfg(feature = "etcd")]
    pub(crate) fn logical_shard_records_prefix(&self) -> Vec<u8> {
        format!("{}/logical-shards/", self.normalized_key_prefix()).into_bytes()
    }

    /// Stable session key shared by every owner generation of one logical shard.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn logical_shard_session_key(&self, logical_shard_id: &LogicalShardId) -> Vec<u8> {
        format!(
            "{}/sessions/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(logical_shard_id.as_bytes())
        )
        .into_bytes()
    }

    /// Shard-singleton durable owner-admission plan.
    ///
    /// The plan is deliberately not attached to a lease. Its exact permanent
    /// claim and lease-attached sentinel distinguish a prepared plan from an
    /// expired or committed attempt after restart.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn owner_admission_plan_key(&self, logical_shard_id: &LogicalShardId) -> Vec<u8> {
        format!(
            "{}/owner-admission/plans/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(logical_shard_id.as_bytes())
        )
        .into_bytes()
    }

    /// Shard-singleton liveness sentinel for one prepared admission plan.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn owner_admission_sentinel_key(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Vec<u8> {
        format!(
            "{}/owner-admission/sentinels/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(logical_shard_id.as_bytes())
        )
        .into_bytes()
    }

    /// Permanent claim for one never-reused shard owner incarnation.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn owner_incarnation_claim_key(
        &self,
        logical_shard_id: &LogicalShardId,
        owner_incarnation_id: &OwnerIncarnationId,
    ) -> Vec<u8> {
        format!(
            "{}/owner-admission/claims/{}/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(logical_shard_id.as_bytes()),
            encode_fixed_id(owner_incarnation_id.as_bytes())
        )
        .into_bytes()
    }

    fn normalized_key_prefix(&self) -> String {
        self.key_prefix.trim_end_matches('/').to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root_id(value: u8) -> RootId {
        RootId::from_bytes([value; 16])
    }

    fn shard_id(value: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([value; 16])
    }

    fn incarnation_id(value: u8) -> OwnerIncarnationId {
        OwnerIncarnationId::from_bytes([value; 16])
    }

    #[test]
    fn etcd_options_validate_required_fields() {
        assert!(matches!(
            EtcdControlStoreOptions::new(Vec::<String>::new()).validate(),
            Err(ControlError::InvalidOptions(_))
        ));
        assert!(matches!(
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"])
                .with_lease_ttl_seconds(0)
                .validate(),
            Err(ControlError::InvalidOptions(_))
        ));
    }

    #[test]
    fn etcd_keys_use_fixed_storage_identities() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");

        assert_eq!(
            String::from_utf8(options.root_placement_key(&root_id(0x01))).unwrap(),
            "/nokv/test/roots/01010101010101010101010101010101"
        );
        assert_eq!(
            String::from_utf8(options.logical_shard_record_key(&shard_id(0x02))).unwrap(),
            "/nokv/test/logical-shards/02020202020202020202020202020202"
        );
        assert_eq!(
            String::from_utf8(options.logical_shard_session_key(&shard_id(0x02))).unwrap(),
            "/nokv/test/sessions/02020202020202020202020202020202"
        );
        assert_eq!(
            String::from_utf8(options.owner_admission_plan_key(&shard_id(0x02))).unwrap(),
            "/nokv/test/owner-admission/plans/02020202020202020202020202020202"
        );
        assert_eq!(
            String::from_utf8(options.owner_admission_sentinel_key(&shard_id(0x02))).unwrap(),
            "/nokv/test/owner-admission/sentinels/02020202020202020202020202020202"
        );
        assert_eq!(
            String::from_utf8(options.owner_incarnation_claim_key(
                &shard_id(0x02),
                &incarnation_id(0x03),
            ))
            .unwrap(),
            "/nokv/test/owner-admission/claims/02020202020202020202020202020202/03030303030303030303030303030303"
        );
    }
}
