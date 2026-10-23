//! Tenant identity records in etcd.
//!
//! A tenant keeps its records under `tenants/{tenant}/`, beside a counter
//! record holding its policy revision and record counts. Every change writes
//! its record and the advanced counter in one transaction guarded by the
//! counter's modification revision, so concurrent changes serialize and the
//! counts bound a policy read. A policy read fetches the whole prefix at one
//! etcd revision. Root ownership lives under `root-tenants/{root}`.

use etcd_client::{Client, Compare, CompareOp, GetOptions, KeyValue, Txn, TxnOp, TxnOpResponse};

use crate::codec::{
    decode_credential_binding, decode_principal_record, decode_role_binding,
    decode_root_tenant_binding, decode_tenant_policy_counter, decode_tenant_record,
    encode_credential_binding, encode_principal_record, encode_role_binding,
    encode_root_tenant_binding, encode_tenant_policy_counter, encode_tenant_record,
    TenantPolicyCounter,
};
use crate::etcd::etcd_backend;
use crate::identity::{check_limit, require_root_in_tenant, validate_credential};
use crate::{
    ControlError, CredentialBinding, CredentialFingerprint, EtcdControlStore,
    EtcdControlStoreOptions, IdentityLimits, IdentityStore, PrincipalId, PrincipalRecord,
    RoleBinding, RoleScope, RootId, RootTenantBinding, TenantId, TenantPolicy, TenantRecord,
};

/// Bound on optimistic retries when other changes to the same tenant keep
/// winning the counter.
const MAX_IDENTITY_TRANSACTION_ATTEMPTS: usize = 16;

/// Which count of the tenant counter a record kind uses.
#[derive(Clone, Copy)]
enum CountedRecord {
    Principal,
    Credential,
    RoleBinding,
}

impl CountedRecord {
    const fn name(self) -> &'static str {
        match self {
            Self::Principal => "principal",
            Self::Credential => "credential",
            Self::RoleBinding => "role binding",
        }
    }

    fn limit(self) -> usize {
        let limits = IdentityLimits::DEFAULT;
        match self {
            Self::Principal => limits.principals,
            Self::Credential => limits.credentials,
            Self::RoleBinding => limits.role_bindings,
        }
    }

    fn count(self, counter: &mut TenantPolicyCounter) -> &mut u64 {
        match self {
            Self::Principal => &mut counter.principals,
            Self::Credential => &mut counter.credentials,
            Self::RoleBinding => &mut counter.role_bindings,
        }
    }
}

/// Facts a new record depends on, read in the same transaction as the
/// counter: the principal it names, and the root a root-scoped role names.
/// Neither can disappear once present.
struct Prerequisites {
    principal: Option<PrincipalId>,
    root: Option<RootId>,
}

/// One create-only record of a tenant.
struct NewRecord {
    tenant_id: TenantId,
    kind: CountedRecord,
    key: Vec<u8>,
    value: Vec<u8>,
    prerequisites: Prerequisites,
    /// The error for an existing record that differs from `value`.
    conflict: ControlError,
}

impl IdentityStore for EtcdControlStore {
    fn create_tenant(&self, tenant: TenantRecord) -> Result<TenantRecord, ControlError> {
        let mut client = self.client();
        let options = self.options().clone();
        self.block_on(async move {
            let key = options.tenant_key(&tenant.tenant_id);
            let value = encode_tenant_record(&tenant)?;
            let counter = encode_tenant_policy_counter(&TenantPolicyCounter {
                tenant_id: tenant.tenant_id,
                revision: 1,
                principals: 0,
                credentials: 0,
                role_bindings: 0,
            })?;
            let response = client
                .txn(
                    Txn::new()
                        .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
                        .and_then(vec![
                            TxnOp::put(key.clone(), value.clone(), None),
                            TxnOp::put(
                                options.tenant_counter_key(&tenant.tenant_id),
                                counter,
                                None,
                            ),
                        ])
                        .or_else(vec![TxnOp::get(key, None)]),
                )
                .await
                .map_err(etcd_backend)?;
            if response.succeeded() {
                return Ok(tenant);
            }
            match first_value(response.op_responses())? {
                Some(existing) if decode_tenant_record(&existing)? == tenant => Ok(tenant),
                _ => Err(ControlError::Codec(
                    "tenant key and value identities differ".to_owned(),
                )),
            }
        })
    }

    fn create_principal(
        &self,
        principal: PrincipalRecord,
    ) -> Result<PrincipalRecord, ControlError> {
        let options = self.options();
        self.create_record(NewRecord {
            tenant_id: principal.tenant_id,
            kind: CountedRecord::Principal,
            key: options.tenant_principal_key(&principal.tenant_id, &principal.principal_id),
            value: encode_principal_record(&principal)?,
            prerequisites: Prerequisites {
                principal: None,
                root: None,
            },
            conflict: ControlError::Codec("principal key and value identities differ".to_owned()),
        })?;
        Ok(principal)
    }

    fn add_credential(
        &self,
        credential: CredentialBinding,
    ) -> Result<CredentialBinding, ControlError> {
        validate_credential(&credential)?;
        let options = self.options();
        self.create_record(NewRecord {
            tenant_id: credential.tenant_id,
            kind: CountedRecord::Credential,
            key: options.tenant_credential_key(&credential.tenant_id, &credential.fingerprint),
            value: encode_credential_binding(&credential)?,
            prerequisites: Prerequisites {
                principal: Some(credential.principal_id),
                root: None,
            },
            conflict: ControlError::CredentialAlreadyBound {
                tenant_id: credential.tenant_id,
                fingerprint: credential.fingerprint,
            },
        })?;
        Ok(credential)
    }

    fn revoke_credential(
        &self,
        tenant_id: &TenantId,
        fingerprint: &CredentialFingerprint,
    ) -> Result<bool, ControlError> {
        self.remove_record(
            *tenant_id,
            CountedRecord::Credential,
            self.options().tenant_credential_key(tenant_id, fingerprint),
        )
    }

    fn grant_role(&self, binding: RoleBinding) -> Result<RoleBinding, ControlError> {
        let options = self.options();
        self.create_record(NewRecord {
            tenant_id: binding.tenant_id,
            kind: CountedRecord::RoleBinding,
            key: options.tenant_role_binding_key(&binding),
            value: encode_role_binding(&binding)?,
            prerequisites: Prerequisites {
                principal: Some(binding.principal_id),
                root: match binding.scope {
                    RoleScope::Tenant => None,
                    RoleScope::Root(root_id) => Some(root_id),
                },
            },
            conflict: ControlError::Codec(
                "role binding key and value identities differ".to_owned(),
            ),
        })?;
        Ok(binding)
    }

    fn revoke_role(&self, binding: &RoleBinding) -> Result<bool, ControlError> {
        self.remove_record(
            binding.tenant_id,
            CountedRecord::RoleBinding,
            self.options().tenant_role_binding_key(binding),
        )
    }

    fn bind_root_tenant(
        &self,
        binding: RootTenantBinding,
    ) -> Result<RootTenantBinding, ControlError> {
        let mut client = self.client();
        let options = self.options().clone();
        self.block_on(async move {
            let tenant_key = options.tenant_key(&binding.tenant_id);
            let key = options.root_tenant_key(&binding.root_id);
            let value = encode_root_tenant_binding(&binding)?;
            let response = client
                .txn(
                    Txn::new()
                        .when(vec![
                            Compare::version(tenant_key.clone(), CompareOp::Greater, 0),
                            Compare::version(key.clone(), CompareOp::Equal, 0),
                        ])
                        .and_then(vec![TxnOp::put(key.clone(), value, None)])
                        .or_else(vec![
                            TxnOp::get(tenant_key, None),
                            TxnOp::get(key.clone(), None),
                        ]),
                )
                .await
                .map_err(etcd_backend)?;
            if response.succeeded() {
                return Ok(binding);
            }
            let mut responses = response.op_responses().into_iter();
            let (tenant, existing) = match (responses.next(), responses.next()) {
                (Some(TxnOpResponse::Get(tenant)), Some(TxnOpResponse::Get(existing))) => {
                    (tenant, existing)
                }
                _ => return Err(unexpected_response()),
            };
            if tenant.kvs().is_empty() {
                return Err(ControlError::TenantNotFound(binding.tenant_id));
            }
            let Some(existing) = existing.kvs().first() else {
                return Err(ControlError::Backend(
                    "root tenant binding disappeared after a create conflict".to_owned(),
                ));
            };
            let existing = validate_root_tenant_entry(&options, &binding.root_id, existing)?;
            if existing == binding {
                Ok(binding)
            } else {
                Err(ControlError::RootTenantAlreadyBound {
                    root_id: binding.root_id,
                })
            }
        })
    }

    fn get_root_tenant(&self, root_id: &RootId) -> Result<Option<RootTenantBinding>, ControlError> {
        let mut client = self.client();
        let options = self.options().clone();
        let root_id = *root_id;
        self.block_on(async move { fetch_root_tenant(&mut client, &options, &root_id).await })
    }

    fn load_tenant_policy(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantPolicy>, ControlError> {
        let mut client = self.client();
        let options = self.options().clone();
        let tenant_id = *tenant_id;
        self.block_on(async move {
            let prefix = options.tenant_records_prefix(&tenant_id);
            // One transaction reads the tenant and all of its records at one
            // etcd revision, so a policy is never observed torn.
            let response = client
                .txn(Txn::new().and_then(vec![
                    TxnOp::get(options.tenant_key(&tenant_id), None),
                    TxnOp::get(prefix.clone(), Some(GetOptions::new().with_prefix())),
                ]))
                .await
                .map_err(etcd_backend)?;
            let mut responses = response.op_responses().into_iter();
            let (tenant, records) = match (responses.next(), responses.next()) {
                (Some(TxnOpResponse::Get(tenant)), Some(TxnOpResponse::Get(records))) => {
                    (tenant, records)
                }
                _ => return Err(unexpected_response()),
            };
            let Some(tenant) = tenant.kvs().first() else {
                if records.kvs().is_empty() {
                    return Ok(None);
                }
                return Err(ControlError::Codec(
                    "tenant records exist without their tenant".to_owned(),
                ));
            };
            if decode_tenant_record(tenant.value())?.tenant_id != tenant_id {
                return Err(ControlError::Codec(
                    "tenant key and value identities differ".to_owned(),
                ));
            }
            parse_tenant_policy(&options, tenant_id, &prefix, records.kvs()).map(Some)
        })
    }

    fn tenant_policy_revision(&self, tenant_id: &TenantId) -> Result<Option<u64>, ControlError> {
        let mut client = self.client();
        let options = self.options().clone();
        let tenant_id = *tenant_id;
        self.block_on(async move {
            let key = options.tenant_counter_key(&tenant_id);
            let response = client.get(key, None).await.map_err(etcd_backend)?;
            response
                .kvs()
                .first()
                .map(|counter| decode_counter(tenant_id, counter.value()))
                .transpose()
                .map(|counter| counter.map(|counter| counter.revision))
        })
    }
}

impl EtcdControlStore {
    fn create_record(&self, record: NewRecord) -> Result<(), ControlError> {
        let mut client = self.client();
        let options = self.options().clone();
        self.block_on(async move {
            let counter_key = options.tenant_counter_key(&record.tenant_id);
            for _ in 0..MAX_IDENTITY_TRANSACTION_ATTEMPTS {
                let mut reads = vec![
                    TxnOp::get(counter_key.clone(), None),
                    TxnOp::get(record.key.clone(), None),
                ];
                if let Some(principal_id) = &record.prerequisites.principal {
                    reads.push(TxnOp::get(
                        options.tenant_principal_key(&record.tenant_id, principal_id),
                        None,
                    ));
                }
                if let Some(root_id) = &record.prerequisites.root {
                    reads.push(TxnOp::get(options.root_tenant_key(root_id), None));
                }
                let response = client
                    .txn(Txn::new().and_then(reads))
                    .await
                    .map_err(etcd_backend)?;
                let mut values = get_values(response.op_responses())?.into_iter();
                let Some(Some(counter)) = values.next() else {
                    return Err(ControlError::TenantNotFound(record.tenant_id));
                };
                let mut next = decode_counter(record.tenant_id, counter.value())?;
                let existing = values.next().flatten();
                if let Some(principal_id) = record.prerequisites.principal {
                    if values.next().flatten().is_none() {
                        return Err(ControlError::PrincipalNotFound {
                            tenant_id: record.tenant_id,
                            principal_id,
                        });
                    }
                }
                if let Some(root_id) = record.prerequisites.root {
                    let owner = values
                        .next()
                        .flatten()
                        .map(|entry| validate_root_tenant_entry(&options, &root_id, &entry))
                        .transpose()?
                        .map(|binding| binding.tenant_id);
                    require_root_in_tenant(root_id, owner, record.tenant_id)?;
                }
                if let Some(existing) = existing {
                    if existing.value() == record.value.as_slice() {
                        return Ok(());
                    }
                    return Err(record.conflict);
                }
                let count = record.kind.count(&mut next);
                check_limit(
                    record.tenant_id,
                    record.kind.name(),
                    usize::try_from(*count).unwrap_or(usize::MAX),
                    record.kind.limit(),
                )?;
                *count += 1;
                next.revision = advance(next.revision)?;
                let response = client
                    .txn(
                        Txn::new()
                            .when(vec![
                                Compare::mod_revision(
                                    counter_key.clone(),
                                    CompareOp::Equal,
                                    counter.mod_revision(),
                                ),
                                Compare::version(record.key.clone(), CompareOp::Equal, 0),
                            ])
                            .and_then(vec![
                                TxnOp::put(record.key.clone(), record.value.clone(), None),
                                TxnOp::put(
                                    counter_key.clone(),
                                    encode_tenant_policy_counter(&next)?,
                                    None,
                                ),
                            ]),
                    )
                    .await
                    .map_err(etcd_backend)?;
                if response.succeeded() {
                    return Ok(());
                }
            }
            Err(contended(record.tenant_id))
        })
    }

    fn remove_record(
        &self,
        tenant_id: TenantId,
        kind: CountedRecord,
        key: Vec<u8>,
    ) -> Result<bool, ControlError> {
        let mut client = self.client();
        let options = self.options().clone();
        self.block_on(async move {
            let counter_key = options.tenant_counter_key(&tenant_id);
            for _ in 0..MAX_IDENTITY_TRANSACTION_ATTEMPTS {
                let response = client
                    .txn(Txn::new().and_then(vec![
                        TxnOp::get(counter_key.clone(), None),
                        TxnOp::get(key.clone(), None),
                    ]))
                    .await
                    .map_err(etcd_backend)?;
                let mut values = get_values(response.op_responses())?.into_iter();
                let Some(Some(counter)) = values.next() else {
                    return Err(ControlError::TenantNotFound(tenant_id));
                };
                let Some(Some(existing)) = values.next() else {
                    return Ok(false);
                };
                let mut next = decode_counter(tenant_id, counter.value())?;
                let count = kind.count(&mut next);
                *count = count.checked_sub(1).ok_or_else(|| {
                    ControlError::Codec(format!("tenant counter has no {} to remove", kind.name()))
                })?;
                next.revision = advance(next.revision)?;
                let response = client
                    .txn(
                        Txn::new()
                            .when(vec![
                                Compare::mod_revision(
                                    counter_key.clone(),
                                    CompareOp::Equal,
                                    counter.mod_revision(),
                                ),
                                Compare::mod_revision(
                                    key.clone(),
                                    CompareOp::Equal,
                                    existing.mod_revision(),
                                ),
                            ])
                            .and_then(vec![
                                TxnOp::delete(key.clone(), None),
                                TxnOp::put(
                                    counter_key.clone(),
                                    encode_tenant_policy_counter(&next)?,
                                    None,
                                ),
                            ]),
                    )
                    .await
                    .map_err(etcd_backend)?;
                if response.succeeded() {
                    return Ok(true);
                }
            }
            Err(contended(tenant_id))
        })
    }
}

fn parse_tenant_policy(
    options: &EtcdControlStoreOptions,
    tenant_id: TenantId,
    prefix: &[u8],
    entries: &[KeyValue],
) -> Result<TenantPolicy, ControlError> {
    let mismatch = |record: &str| ControlError::Codec(format!("{record} key and value differ"));
    let mut counter = None;
    let mut principals = Vec::new();
    let mut credentials = Vec::new();
    let mut role_bindings = Vec::new();
    for entry in entries {
        let suffix = entry
            .key()
            .strip_prefix(prefix)
            .ok_or_else(|| ControlError::Codec("tenant record outside its prefix".to_owned()))?;
        if suffix == b"revision" {
            counter = Some(decode_counter(tenant_id, entry.value())?);
        } else if suffix.starts_with(b"principals/") {
            let principal = decode_principal_record(entry.value())?;
            if principal.tenant_id != tenant_id
                || entry.key() != options.tenant_principal_key(&tenant_id, &principal.principal_id)
            {
                return Err(mismatch("principal"));
            }
            principals.push(principal);
        } else if suffix.starts_with(b"credentials/") {
            let credential = decode_credential_binding(entry.value())?;
            if credential.tenant_id != tenant_id
                || entry.key() != options.tenant_credential_key(&tenant_id, &credential.fingerprint)
            {
                return Err(mismatch("credential"));
            }
            credentials.push(credential);
        } else if suffix.starts_with(b"role-bindings/") {
            let binding = decode_role_binding(entry.value())?;
            if binding.tenant_id != tenant_id
                || entry.key() != options.tenant_role_binding_key(&binding)
            {
                return Err(mismatch("role binding"));
            }
            role_bindings.push(binding);
        } else {
            return Err(ControlError::Codec(
                "unknown record under a tenant prefix".to_owned(),
            ));
        }
    }
    let counter =
        counter.ok_or_else(|| ControlError::Codec("tenant has no policy counter".to_owned()))?;
    let counts = [
        (counter.principals, principals.len()),
        (counter.credentials, credentials.len()),
        (counter.role_bindings, role_bindings.len()),
    ];
    if counts
        .iter()
        .any(|(counted, held)| usize::try_from(*counted).ok() != Some(*held))
    {
        return Err(ControlError::Codec(
            "tenant policy counter disagrees with its records".to_owned(),
        ));
    }
    let registered = |principal_id| {
        principals
            .iter()
            .any(|principal: &PrincipalRecord| principal.principal_id == principal_id)
    };
    if credentials
        .iter()
        .any(|credential| !registered(credential.principal_id))
        || role_bindings
            .iter()
            .any(|binding| !registered(binding.principal_id))
    {
        return Err(ControlError::Codec(
            "a tenant record names an unregistered principal".to_owned(),
        ));
    }
    // Key order differs from the policy's documented order for scopes.
    principals.sort();
    credentials.sort_by_key(|credential| credential.fingerprint);
    role_bindings.sort();
    Ok(TenantPolicy {
        tenant_id,
        revision: counter.revision,
        principals,
        credentials,
        role_bindings,
    })
}

async fn fetch_root_tenant(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    root_id: &RootId,
) -> Result<Option<RootTenantBinding>, ControlError> {
    let response = client
        .get(options.root_tenant_key(root_id), None)
        .await
        .map_err(etcd_backend)?;
    response
        .kvs()
        .first()
        .map(|entry| validate_root_tenant_entry(options, root_id, entry))
        .transpose()
}

fn validate_root_tenant_entry(
    options: &EtcdControlStoreOptions,
    root_id: &RootId,
    entry: &KeyValue,
) -> Result<RootTenantBinding, ControlError> {
    let binding = decode_root_tenant_binding(entry.value())?;
    if binding.root_id != *root_id || entry.key() != options.root_tenant_key(root_id) {
        return Err(ControlError::Codec(
            "root tenant key and value identities differ".to_owned(),
        ));
    }
    Ok(binding)
}

fn decode_counter(tenant_id: TenantId, bytes: &[u8]) -> Result<TenantPolicyCounter, ControlError> {
    let counter = decode_tenant_policy_counter(bytes)?;
    if counter.tenant_id != tenant_id {
        return Err(ControlError::Codec(
            "tenant counter belongs to another tenant".to_owned(),
        ));
    }
    Ok(counter)
}

fn advance(revision: u64) -> Result<u64, ControlError> {
    revision
        .checked_add(1)
        .ok_or_else(|| ControlError::Backend("tenant policy revision overflowed".to_owned()))
}

fn get_values(responses: Vec<TxnOpResponse>) -> Result<Vec<Option<KeyValue>>, ControlError> {
    responses
        .into_iter()
        .map(|response| match response {
            TxnOpResponse::Get(response) => Ok(response.kvs().first().cloned()),
            _ => Err(unexpected_response()),
        })
        .collect()
}

fn first_value(responses: Vec<TxnOpResponse>) -> Result<Option<Vec<u8>>, ControlError> {
    Ok(get_values(responses)?
        .into_iter()
        .next()
        .flatten()
        .map(|entry| entry.value().to_vec()))
}

fn unexpected_response() -> ControlError {
    ControlError::Backend("etcd identity transaction returned an unexpected response".to_owned())
}

fn contended(tenant_id: TenantId) -> ControlError {
    ControlError::Backend(format!(
        "tenant {tenant_id:?} identity changed concurrently {MAX_IDENTITY_TRANSACTION_ATTEMPTS} times; retry"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etcd::tests::EtcdHarness;

    #[test]
    #[ignore = "requires NOKV_TEST_ETCD_ENDPOINT"]
    fn etcd_store_passes_identity_conformance() {
        crate::identity_conformance::run_all(&|scenario| {
            scenario(&EtcdHarness::connect("identity-conformance").store);
        });
    }

    #[test]
    #[ignore = "requires NOKV_TEST_ETCD_ENDPOINT"]
    fn concurrent_changes_to_one_tenant_all_land_with_exact_counts() {
        let harness = std::sync::Arc::new(EtcdHarness::connect("identity-concurrency"));
        let tenant_id = TenantId::from_bytes([1; 16]);
        harness
            .store
            .create_tenant(TenantRecord { tenant_id })
            .unwrap();
        let workers: Vec<_> = (0..8_u8)
            .map(|worker| {
                let harness = std::sync::Arc::clone(&harness);
                std::thread::spawn(move || {
                    harness
                        .store
                        .create_principal(PrincipalRecord {
                            tenant_id,
                            principal_id: PrincipalId::from_bytes([worker + 10; 16]),
                        })
                        .unwrap();
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let policy = harness
            .store
            .load_tenant_policy(&tenant_id)
            .unwrap()
            .unwrap();
        assert_eq!(policy.principals.len(), 8);
        assert_eq!(policy.revision, 9);
    }

    #[test]
    #[ignore = "requires NOKV_TEST_ETCD_ENDPOINT"]
    fn a_counter_that_disagrees_with_its_records_fails_closed() {
        let harness = EtcdHarness::connect("identity-counter");
        let tenant_id = TenantId::from_bytes([1; 16]);
        harness
            .store
            .create_tenant(TenantRecord { tenant_id })
            .unwrap();
        let counter = encode_tenant_policy_counter(&TenantPolicyCounter {
            tenant_id,
            revision: 2,
            principals: 1,
            credentials: 0,
            role_bindings: 0,
        })
        .unwrap();
        let key = harness.store.options().tenant_counter_key(&tenant_id);
        let mut client = harness.store.client();
        harness
            .store
            .block_on(async move {
                client.put(key, counter, None).await.map_err(etcd_backend)?;
                Ok(())
            })
            .unwrap();
        assert!(matches!(
            harness.store.load_tenant_policy(&tenant_id),
            Err(ControlError::Codec(_))
        ));
    }
}
