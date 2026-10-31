#[cfg(any(feature = "etcd", test))]
use crate::codec::encode_fixed_id;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use crate::ControlError;
#[cfg(any(feature = "etcd", test))]
use crate::{
    CredentialFingerprint, LogicalShardId, PrincipalId, RoleBinding, RoleScope, RootId, TenantId,
};

const DEFAULT_ETCD_KEY_PREFIX: &str = "/nokv/control";
/// Largest PEM document accepted for the CA bundle, the certificate chain, or
/// the private key.
pub(crate) const MAX_TLS_PEM_BYTES: usize = 1024 * 1024;
const DEFAULT_ETCD_LEASE_TTL_SECONDS: i64 = 10;
const MAX_ETCD_KEY_PREFIX_BYTES: usize = 4 * 1024;

/// This process's identity towards etcd over mutual TLS: the roots that sign
/// etcd's certificate, and the certificate chain and private key etcd
/// authenticates. With `--client-cert-auth`, etcd takes the certificate's
/// Common Name as its user, and that user's roles decide which keys the
/// process may touch. The PEM bytes are shared between clones and never
/// appear in `Debug` output; their validity is checked when connecting.
#[derive(Clone, PartialEq, Eq)]
pub struct EtcdTlsConfig {
    pub(crate) trusted_ca_pem: Arc<[u8]>,
    pub(crate) certificate_pem: Arc<[u8]>,
    pub(crate) private_key_pem: Arc<[u8]>,
}

impl EtcdTlsConfig {
    pub fn new(
        trusted_ca_pem: impl Into<Arc<[u8]>>,
        certificate_pem: impl Into<Arc<[u8]>>,
        private_key_pem: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            trusted_ca_pem: trusted_ca_pem.into(),
            certificate_pem: certificate_pem.into(),
            private_key_pem: private_key_pem.into(),
        }
    }

    fn validate(&self) -> Result<(), ControlError> {
        for (name, bytes) in [
            ("CA", &self.trusted_ca_pem),
            ("certificate", &self.certificate_pem),
            ("private key", &self.private_key_pem),
        ] {
            if bytes.is_empty() || bytes.len() > MAX_TLS_PEM_BYTES {
                return Err(ControlError::InvalidOptions(format!(
                    "etcd TLS {name} PEM must contain 1..={MAX_TLS_PEM_BYTES} bytes"
                )));
            }
        }
        Ok(())
    }
}

impl fmt::Debug for EtcdTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EtcdTlsConfig")
            .field("credentials", &"<redacted>")
            .finish()
    }
}

/// Refuse, before any connection, an etcd endpoint that cannot name an etcd
/// server: it is an `http://` or `https://` URL whose authority is a host
/// name, an IPv4 address or a bracketed IPv6 address, with an optional port
/// in 1..=65535. What follows the authority is left to the client library.
/// One that carries credentials is refused first, whatever else is wrong
/// with it, and no refusal repeats them.
fn validate_endpoint(endpoint: &str) -> Result<(), ControlError> {
    let shown = redacted_endpoint(endpoint);
    let invalid =
        |why: &str| ControlError::InvalidOptions(format!("etcd endpoint {shown:?} {why}"));
    if shown != endpoint {
        return Err(invalid(
            "carries credentials, which etcd never receives and the command line would keep; \
             use the etcd TLS options to authenticate",
        ));
    }
    // The client library matches these schemes case-sensitively.
    let rest = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .ok_or_else(|| invalid("must start with http:// or https://, in lowercase"))?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let port = match authority.strip_prefix('[') {
        Some(bracketed) => {
            let invalid_address = || invalid("has an invalid IPv6 address");
            let (address, tail) = bracketed.split_once(']').ok_or_else(invalid_address)?;
            address
                .parse::<std::net::Ipv6Addr>()
                .map_err(|_| invalid_address())?;
            match tail {
                "" => None,
                tail => Some(tail.strip_prefix(':').ok_or_else(invalid_address)?),
            }
        }
        None => {
            let (host, port) = match authority.rsplit_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            };
            if host.is_empty() {
                return Err(invalid("names no host"));
            }
            // A host name or an IPv4 address; an IPv6 address needs brackets.
            if !host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".-_~".contains(&byte))
            {
                return Err(invalid("has an invalid host"));
            }
            port
        }
    };
    if let Some(port) = port {
        let number = port
            .bytes()
            .all(|byte| byte.is_ascii_digit())
            .then(|| port.parse::<u16>().ok())
            .flatten()
            .filter(|number| *number != 0);
        if number.is_none() {
            return Err(invalid("has an invalid port; use one in 1..=65535"));
        }
    }
    Ok(())
}

/// `endpoint` as a message may show it. Anything after its scheme up to the
/// last `@` could be credentials, so it is replaced; a scheme cannot carry
/// them.
fn redacted_endpoint(endpoint: &str) -> Cow<'_, str> {
    let start = endpoint
        .find("://")
        .filter(|&end| {
            let scheme = &endpoint[..end];
            scheme.starts_with(|character: char| character.is_ascii_alphabetic())
                && scheme
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "+-.".contains(character))
        })
        .map_or(0, |end| end + "://".len());
    match endpoint[start..].rfind('@') {
        Some(at) => Cow::Owned(format!(
            "{}<redacted>{}",
            &endpoint[..start],
            &endpoint[start + at..]
        )),
        None => Cow::Borrowed(endpoint),
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct EtcdControlStoreOptions {
    endpoints: Vec<String>,
    key_prefix: String,
    lease_ttl_seconds: i64,
    tls: Option<EtcdTlsConfig>,
}

/// Endpoints appear with any credentials redacted.
impl fmt::Debug for EtcdControlStoreOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let endpoints = self
            .endpoints
            .iter()
            .map(|endpoint| redacted_endpoint(endpoint))
            .collect::<Vec<_>>();
        formatter
            .debug_struct("EtcdControlStoreOptions")
            .field("endpoints", &endpoints)
            .field("key_prefix", &self.key_prefix)
            .field("lease_ttl_seconds", &self.lease_ttl_seconds)
            .field("tls", &self.tls)
            .finish()
    }
}

impl EtcdControlStoreOptions {
    pub fn new(endpoints: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            endpoints: endpoints.into_iter().map(Into::into).collect(),
            key_prefix: DEFAULT_ETCD_KEY_PREFIX.to_owned(),
            lease_ttl_seconds: DEFAULT_ETCD_LEASE_TTL_SECONDS,
            tls: None,
        }
    }

    /// Authenticate to etcd over mutual TLS. Every endpoint must then be an
    /// `https://` URL; there is no fallback to plaintext.
    pub fn with_tls(mut self, tls: EtcdTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn tls(&self) -> Option<&EtcdTlsConfig> {
        self.tls.as_ref()
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
        for endpoint in &self.endpoints {
            validate_endpoint(endpoint)?;
        }
        let secured = self
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.trim_start().starts_with("https://"))
            .count();
        match (&self.tls, secured) {
            (Some(tls), count) if count == self.endpoints.len() => tls.validate()?,
            (Some(_), _) => {
                return Err(ControlError::InvalidOptions(
                    "etcd TLS requires every endpoint to be an https:// URL".to_owned(),
                ));
            }
            (None, 0) => {}
            (None, _) => {
                return Err(ControlError::InvalidOptions(
                    "an https:// etcd endpoint requires the etcd TLS CA, certificate, and key"
                        .to_owned(),
                ));
            }
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

    fn tls() -> EtcdTlsConfig {
        EtcdTlsConfig::new(
            b"ca".as_slice(),
            b"secret-cert".as_slice(),
            b"secret-key".as_slice(),
        )
    }

    #[test]
    fn etcd_tls_binds_https_endpoints_and_complete_pems() {
        EtcdControlStoreOptions::new(["https://127.0.0.1:2379"])
            .with_tls(tls())
            .validate()
            .unwrap();
        EtcdControlStoreOptions::new(["http://127.0.0.1:2379"])
            .validate()
            .unwrap();
        let oversized = vec![b'x'; MAX_TLS_PEM_BYTES + 1];
        for (endpoints, tls) in [
            (vec!["http://127.0.0.1:2379"], Some(tls())),
            (
                vec!["https://127.0.0.1:2379", "http://127.0.0.1:22379"],
                Some(tls()),
            ),
            (vec!["https://127.0.0.1:2379"], None),
            (
                vec!["https://127.0.0.1:2379"],
                Some(EtcdTlsConfig::new(
                    b"".as_slice(),
                    b"cert".as_slice(),
                    b"key".as_slice(),
                )),
            ),
            (
                vec!["https://127.0.0.1:2379"],
                Some(EtcdTlsConfig::new(
                    oversized.as_slice(),
                    b"cert".as_slice(),
                    b"key".as_slice(),
                )),
            ),
        ] {
            let mut options = EtcdControlStoreOptions::new(endpoints.clone());
            if let Some(tls) = tls {
                options = options.with_tls(tls);
            }
            assert!(
                matches!(options.validate(), Err(ControlError::InvalidOptions(_))),
                "{endpoints:?}"
            );
        }
    }

    /// An endpoint that cannot name an etcd server is the caller's mistake,
    /// refused before any connection; a well-formed one that nothing answers
    /// fails later, as unreachable.
    #[test]
    fn etcd_endpoints_must_be_http_urls_naming_a_host_and_a_valid_port() {
        for endpoint in [
            "http://127.0.0.1:2379",
            "http://localhost",
            "http://[::1]:2379",
            "http://etcd-0.etcd.nokv.svc:2379",
            "http://127.0.0.1:2379/",
        ] {
            EtcdControlStoreOptions::new([endpoint])
                .validate()
                .unwrap_or_else(|error| panic!("{endpoint}: {error}"));
        }
        for (endpoint, why) in [
            ("127.0.0.1:2379", "must start with http:// or https://"),
            (
                "ftp://127.0.0.1:2379",
                "must start with http:// or https://",
            ),
            ("HTTP://127.0.0.1:2379", "in lowercase"),
            ("http://", "names no host"),
            ("http://:2379", "names no host"),
            ("http://exa mple:2379", "invalid host"),
            ("http://[::1", "invalid IPv6 address"),
            ("http://[::1]x:2379", "invalid IPv6 address"),
            ("http://[zz]:2379", "invalid IPv6 address"),
            ("http://::1:2379", "invalid host"),
            ("http://127.0.0.1:", "invalid port"),
            ("http://127.0.0.1:0", "invalid port"),
            ("http://127.0.0.1:99999", "invalid port"),
            ("http://127.0.0.1:23x9", "invalid port"),
        ] {
            match EtcdControlStoreOptions::new([endpoint]).validate() {
                Err(ControlError::InvalidOptions(message)) => {
                    assert!(
                        message.contains(&format!("{endpoint:?}")) && message.contains(why),
                        "{endpoint}: {message}"
                    );
                }
                other => panic!("{endpoint}: expected a refusal, got {other:?}"),
            }
        }
        // Endpoints are checked before the TLS pairing, whose refusal would
        // otherwise hide the malformed one.
        match EtcdControlStoreOptions::new(["https://"])
            .with_tls(tls())
            .validate()
        {
            Err(ControlError::InvalidOptions(message)) => {
                assert!(message.contains("names no host"), "{message}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// Credentials in an endpoint would stay in argv, and etcd never receives
    /// them. The endpoint is refused for them first, whatever else is wrong
    /// with it, and no message or debug output repeats them.
    #[test]
    fn an_endpoint_with_credentials_is_refused_without_repeating_them() {
        for endpoint in [
            "user:s3cr3t@127.0.0.1:2379",
            "HTTP://user:s3cr3t@127.0.0.1:2379",
            "ftp://user:s3cr3t@127.0.0.1:2379",
            "http://user:s3cr3t@127.0.0.1:2379",
            "http://user:pass/s3cr3t@127.0.0.1:2379",
            "user:s3cr3t://@127.0.0.1:2379",
        ] {
            let options = EtcdControlStoreOptions::new([endpoint]);
            let Err(ControlError::InvalidOptions(message)) = options.validate() else {
                panic!("an endpoint with credentials was not refused as invalid options");
            };
            assert!(
                !message.contains("s3cr3t"),
                "the refusal repeats credentials"
            );
            assert!(!message.contains("user"), "the refusal repeats the user");
            assert!(message.contains("carries credentials"), "{message}");
            assert!(message.contains("<redacted>@127.0.0.1:2379"), "{message}");
            let debug = format!("{options:?}");
            assert!(
                !debug.contains("s3cr3t"),
                "debug output repeats credentials"
            );
        }
    }

    #[test]
    fn etcd_tls_config_debug_redacts_credentials() {
        let options = EtcdControlStoreOptions::new(["https://127.0.0.1:2379"]).with_tls(tls());
        assert!(options.tls().is_some());
        for rendered in [format!("{:?}", tls()), format!("{options:?}")] {
            assert!(!rendered.contains("secret"), "{rendered}");
        }
    }

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
