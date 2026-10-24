#[cfg(any(feature = "etcd", test))]
use crate::codec::encode_fixed_id;
use crate::ControlError;
#[cfg(any(feature = "etcd", test))]
use crate::{
    CredentialFingerprint, LogicalShardId, PrincipalId, RoleBinding, RoleScope, RootId, TenantId,
};

const DEFAULT_ETCD_KEY_PREFIX: &str = "/nokv/control";
const DEFAULT_ETCD_LEASE_TTL_SECONDS: i64 = 10;
const MAX_ETCD_KEY_PREFIX_BYTES: usize = 4 * 1024;

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
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn owner_lease_renew_interval(&self) -> Option<std::time::Duration> {
        let ttl_seconds = u64::try_from(self.lease_ttl_seconds).ok()?;
        if ttl_seconds == 0 {
            return None;
        }
        Some(std::time::Duration::from_secs(
            ttl_seconds.saturating_div(3).max(1),
        ))
    }

    /// Bound on one etcd call. A call that outlives the renewal interval
    /// cannot renew the owner's lease in time anyway, so waiting longer only
    /// stalls the caller.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn request_timeout(&self) -> std::time::Duration {
        self.owner_lease_renew_interval()
            .unwrap_or(std::time::Duration::from_secs(1))
    }

    /// Bound on establishing the etcd connection: one lease TTL, so an owner
    /// that cannot reach etcd learns it before its lease would have lapsed.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn connect_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(u64::try_from(self.lease_ttl_seconds).unwrap_or(1).max(1))
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
        if self.normalized_key_prefix().len() > MAX_ETCD_KEY_PREFIX_BYTES {
            return Err(ControlError::InvalidOptions(format!(
                "etcd key prefix is {} bytes; maximum is {MAX_ETCD_KEY_PREFIX_BYTES}",
                self.normalized_key_prefix().len()
            )));
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

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn root_agent_key(&self, root_id: &RootId) -> Vec<u8> {
        format!(
            "{}/root-agents/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(root_id.as_bytes())
        )
        .into_bytes()
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn root_object_namespace_key(&self, root_id: &RootId) -> Vec<u8> {
        format!(
            "{}/root-object-namespaces/{}",
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

    /// Owner-only recovery state stored next to the client-facing routing record.
    ///
    /// The prefix is deliberately disjoint from `logical-shards/` so a routing
    /// listing never sees recovery values and readers that predate the split
    /// never touch this key.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn logical_shard_recovery_key(&self, logical_shard_id: &LogicalShardId) -> Vec<u8> {
        format!(
            "{}/logical-shard-recovery/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(logical_shard_id.as_bytes())
        )
        .into_bytes()
    }

    #[cfg(feature = "etcd")]
    pub(crate) fn logical_shard_recovery_prefix(&self) -> Vec<u8> {
        format!("{}/logical-shard-recovery/", self.normalized_key_prefix()).into_bytes()
    }

    /// Every identity record of one tenant lives under this prefix; the tenant
    /// record itself sits at the prefix without its trailing slash.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn tenant_key(&self, tenant_id: &TenantId) -> Vec<u8> {
        format!(
            "{}/tenants/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(tenant_id.as_bytes())
        )
        .into_bytes()
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn tenant_records_prefix(&self, tenant_id: &TenantId) -> Vec<u8> {
        let mut prefix = self.tenant_key(tenant_id);
        prefix.push(b'/');
        prefix
    }

    /// The tenant's policy revision and record counts.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn tenant_counter_key(&self, tenant_id: &TenantId) -> Vec<u8> {
        tenant_record_key(self.tenant_records_prefix(tenant_id), "revision")
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn tenant_principal_key(
        &self,
        tenant_id: &TenantId,
        principal_id: &PrincipalId,
    ) -> Vec<u8> {
        tenant_record_key(
            self.tenant_records_prefix(tenant_id),
            &format!("principals/{}", encode_fixed_id(principal_id.as_bytes())),
        )
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn tenant_credential_key(
        &self,
        tenant_id: &TenantId,
        fingerprint: &CredentialFingerprint,
    ) -> Vec<u8> {
        tenant_record_key(
            self.tenant_records_prefix(tenant_id),
            &format!("credentials/{}", encode_fixed_id(fingerprint.as_bytes())),
        )
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn tenant_role_binding_key(&self, binding: &RoleBinding) -> Vec<u8> {
        let scope = match binding.scope {
            RoleScope::Tenant => "tenant".to_owned(),
            RoleScope::Root(root_id) => encode_fixed_id(root_id.as_bytes()),
        };
        tenant_record_key(
            self.tenant_records_prefix(&binding.tenant_id),
            &format!(
                "role-bindings/{}/{}/{scope}",
                encode_fixed_id(binding.principal_id.as_bytes()),
                u8::from(binding.role)
            ),
        )
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn root_tenant_key(&self, root_id: &RootId) -> Vec<u8> {
        format!(
            "{}/root-tenants/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(root_id.as_bytes())
        )
        .into_bytes()
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn root_presentation_key(&self, root_id: &RootId) -> Vec<u8> {
        format!(
            "{}/root-presentations/{}",
            self.normalized_key_prefix(),
            encode_fixed_id(root_id.as_bytes())
        )
        .into_bytes()
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

    fn normalized_key_prefix(&self) -> String {
        self.key_prefix.trim_end_matches('/').to_owned()
    }
}

#[cfg(any(feature = "etcd", test))]
fn tenant_record_key(mut prefix: Vec<u8>, suffix: &str) -> Vec<u8> {
    prefix.extend_from_slice(suffix.as_bytes());
    prefix
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
        assert!(matches!(
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"])
                .with_key_prefix("x".repeat(MAX_ETCD_KEY_PREFIX_BYTES + 1))
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
            String::from_utf8(options.root_object_namespace_key(&root_id(0x01))).unwrap(),
            "/nokv/test/root-object-namespaces/01010101010101010101010101010101"
        );
        assert_eq!(
            String::from_utf8(options.root_agent_key(&root_id(0x01))).unwrap(),
            "/nokv/test/root-agents/01010101010101010101010101010101"
        );
        assert_eq!(
            String::from_utf8(options.logical_shard_record_key(&shard_id(0x02))).unwrap(),
            "/nokv/test/logical-shards/02020202020202020202020202020202"
        );
        assert_eq!(
            String::from_utf8(options.logical_shard_recovery_key(&shard_id(0x02))).unwrap(),
            "/nokv/test/logical-shard-recovery/02020202020202020202020202020202"
        );
        assert_eq!(
            String::from_utf8(options.logical_shard_session_key(&shard_id(0x02))).unwrap(),
            "/nokv/test/sessions/02020202020202020202020202020202"
        );
    }

    #[test]
    fn tenant_identity_keys_nest_under_their_tenant() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let tenant_id = TenantId::from_bytes([0x05; 16]);
        let principal_id = PrincipalId::from_bytes([0x06; 16]);
        let key = |bytes: Vec<u8>| String::from_utf8(bytes).unwrap();
        let tenant = "/nokv/test/tenants/05050505050505050505050505050505";

        assert_eq!(key(options.tenant_key(&tenant_id)), tenant);
        assert_eq!(
            key(options.tenant_records_prefix(&tenant_id)),
            format!("{tenant}/")
        );
        assert_eq!(
            key(options.tenant_counter_key(&tenant_id)),
            format!("{tenant}/revision")
        );
        assert_eq!(
            key(options.tenant_principal_key(&tenant_id, &principal_id)),
            format!("{tenant}/principals/06060606060606060606060606060606")
        );
        assert_eq!(
            key(options
                .tenant_credential_key(&tenant_id, &CredentialFingerprint::from_bytes([0x07; 32]))),
            format!("{tenant}/credentials/{}", "07".repeat(32))
        );
        let binding = |scope| RoleBinding {
            tenant_id,
            principal_id,
            role: crate::Role::Writer,
            scope,
        };
        assert_eq!(
            key(options.tenant_role_binding_key(&binding(RoleScope::Tenant))),
            format!("{tenant}/role-bindings/06060606060606060606060606060606/2/tenant")
        );
        assert_eq!(
            key(options.tenant_role_binding_key(&binding(RoleScope::Root(root_id(0x01))))),
            format!(
                "{tenant}/role-bindings/06060606060606060606060606060606/2/01010101010101010101010101010101"
            )
        );
        assert_eq!(
            key(options.root_tenant_key(&root_id(0x01))),
            "/nokv/test/root-tenants/01010101010101010101010101010101"
        );
        assert_eq!(
            key(options.root_presentation_key(&root_id(0x01))),
            "/nokv/test/root-presentations/01010101010101010101010101010101"
        );
    }

    #[test]
    fn etcd_call_bounds_follow_the_lease_ttl() {
        let endpoint = ["http://127.0.0.1:2379"];
        let options = EtcdControlStoreOptions::new(endpoint);
        assert_eq!(options.request_timeout(), std::time::Duration::from_secs(3));
        assert_eq!(
            options.connect_timeout(),
            std::time::Duration::from_secs(10)
        );
        let short = EtcdControlStoreOptions::new(endpoint).with_lease_ttl_seconds(1);
        assert_eq!(short.request_timeout(), std::time::Duration::from_secs(1));
        assert_eq!(short.connect_timeout(), std::time::Duration::from_secs(1));
    }

    #[test]
    fn owner_lease_renew_interval_tracks_ttl_third_with_floor() {
        let endpoint = ["http://127.0.0.1:2379"];

        assert_eq!(
            EtcdControlStoreOptions::new(endpoint)
                .with_lease_ttl_seconds(1)
                .owner_lease_renew_interval(),
            Some(std::time::Duration::from_secs(1))
        );
        assert_eq!(
            EtcdControlStoreOptions::new(endpoint)
                .with_lease_ttl_seconds(10)
                .owner_lease_renew_interval(),
            Some(std::time::Duration::from_secs(3))
        );
        assert_eq!(
            EtcdControlStoreOptions::new(endpoint)
                .with_lease_ttl_seconds(60)
                .owner_lease_renew_interval(),
            Some(std::time::Duration::from_secs(20))
        );
    }
}
