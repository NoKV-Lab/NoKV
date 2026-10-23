//! Tenant identity records: tenants, their principals, the credentials that
//! authenticate those principals, the roles they hold, and the tenant that
//! owns each root.
//!
//! Records are created once, and an exact replay is idempotent. Every change
//! to a tenant's principals, credentials, or roles advances that tenant's
//! policy revision by one, so a reader can poll one value to learn when to
//! reload. A root-scoped role must name a root the tenant owns, so no record
//! grants access across tenants. These records authorize nothing by
//! themselves: the server enforces them.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ControlError, CredentialBinding, CredentialFingerprint, InMemoryControlStore, PrincipalId,
    PrincipalRecord, RoleBinding, RoleScope, RootId, RootTenantBinding, TenantId, TenantPolicy,
    TenantRecord,
};

/// Upper bounds on one tenant's records, so a policy read stays bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdentityLimits {
    pub principals: usize,
    pub credentials: usize,
    pub role_bindings: usize,
}

impl IdentityLimits {
    pub const DEFAULT: Self = Self {
        principals: 1_024,
        credentials: 2_048,
        role_bindings: 4_096,
    };
}

/// Durable tenant identity records.
pub trait IdentityStore: Send + Sync {
    fn create_tenant(&self, tenant: TenantRecord) -> Result<TenantRecord, ControlError>;

    /// Register a principal in an existing tenant.
    fn create_principal(&self, principal: PrincipalRecord)
        -> Result<PrincipalRecord, ControlError>;

    /// Bind a credential to a registered principal. A fingerprint already
    /// bound otherwise in the tenant is refused.
    fn add_credential(
        &self,
        credential: CredentialBinding,
    ) -> Result<CredentialBinding, ControlError>;

    /// Remove one credential; `false` when it was absent.
    fn revoke_credential(
        &self,
        tenant_id: &TenantId,
        fingerprint: &CredentialFingerprint,
    ) -> Result<bool, ControlError>;

    /// Grant a role to a registered principal on its tenant or on a root the
    /// tenant owns.
    fn grant_role(&self, binding: RoleBinding) -> Result<RoleBinding, ControlError>;

    /// Remove one role binding; `false` when it was absent.
    fn revoke_role(&self, binding: &RoleBinding) -> Result<bool, ControlError>;

    /// Bind a root to the tenant that owns it. A root is never rebound.
    fn bind_root_tenant(
        &self,
        binding: RootTenantBinding,
    ) -> Result<RootTenantBinding, ControlError>;

    fn get_root_tenant(&self, root_id: &RootId) -> Result<Option<RootTenantBinding>, ControlError>;

    /// Read a tenant's records at one revision; `None` for an absent tenant.
    fn load_tenant_policy(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantPolicy>, ControlError>;

    /// The tenant's policy revision; `None` for an absent tenant.
    fn tenant_policy_revision(&self, tenant_id: &TenantId) -> Result<Option<u64>, ControlError>;
}

/// One tenant's records in the in-memory and local stores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TenantState {
    pub(crate) revision: u64,
    pub(crate) principals: BTreeSet<PrincipalId>,
    pub(crate) credentials: BTreeMap<CredentialFingerprint, CredentialBinding>,
    pub(crate) role_bindings: BTreeSet<RoleBinding>,
}

/// The identity records that the in-memory and local stores hold, with the
/// transitions both run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct IdentityState {
    pub(crate) tenants: BTreeMap<TenantId, TenantState>,
    pub(crate) root_tenants: BTreeMap<RootId, TenantId>,
}

impl IdentityState {
    pub(crate) fn create_tenant(&mut self, tenant: TenantRecord) -> TenantRecord {
        self.tenants
            .entry(tenant.tenant_id)
            .or_insert_with(|| TenantState {
                revision: 1,
                principals: BTreeSet::new(),
                credentials: BTreeMap::new(),
                role_bindings: BTreeSet::new(),
            });
        tenant
    }

    pub(crate) fn create_principal(
        &mut self,
        principal: PrincipalRecord,
        limits: IdentityLimits,
    ) -> Result<PrincipalRecord, ControlError> {
        let tenant = self.tenant_mut(&principal.tenant_id)?;
        if tenant.principals.contains(&principal.principal_id) {
            return Ok(principal);
        }
        check_limit(
            principal.tenant_id,
            "principal",
            tenant.principals.len(),
            limits.principals,
        )?;
        tenant.principals.insert(principal.principal_id);
        advance_revision(tenant)?;
        Ok(principal)
    }

    pub(crate) fn add_credential(
        &mut self,
        credential: CredentialBinding,
        limits: IdentityLimits,
    ) -> Result<CredentialBinding, ControlError> {
        validate_credential(&credential)?;
        let tenant = self.tenant_mut(&credential.tenant_id)?;
        require_principal(tenant, credential.tenant_id, credential.principal_id)?;
        match tenant.credentials.get(&credential.fingerprint) {
            Some(existing) if *existing == credential => return Ok(credential),
            Some(_) => {
                return Err(ControlError::CredentialAlreadyBound {
                    tenant_id: credential.tenant_id,
                    fingerprint: credential.fingerprint,
                })
            }
            None => {}
        }
        check_limit(
            credential.tenant_id,
            "credential",
            tenant.credentials.len(),
            limits.credentials,
        )?;
        tenant
            .credentials
            .insert(credential.fingerprint, credential);
        advance_revision(tenant)?;
        Ok(credential)
    }

    pub(crate) fn revoke_credential(
        &mut self,
        tenant_id: &TenantId,
        fingerprint: &CredentialFingerprint,
    ) -> Result<bool, ControlError> {
        let tenant = self.tenant_mut(tenant_id)?;
        if tenant.credentials.remove(fingerprint).is_none() {
            return Ok(false);
        }
        advance_revision(tenant)?;
        Ok(true)
    }

    pub(crate) fn grant_role(
        &mut self,
        binding: RoleBinding,
        limits: IdentityLimits,
    ) -> Result<RoleBinding, ControlError> {
        let root_tenant = match binding.scope {
            RoleScope::Tenant => None,
            RoleScope::Root(root_id) => Some((root_id, self.root_tenants.get(&root_id).copied())),
        };
        let tenant = self.tenant_mut(&binding.tenant_id)?;
        require_principal(tenant, binding.tenant_id, binding.principal_id)?;
        if let Some((root_id, owner)) = root_tenant {
            require_root_in_tenant(root_id, owner, binding.tenant_id)?;
        }
        if tenant.role_bindings.contains(&binding) {
            return Ok(binding);
        }
        check_limit(
            binding.tenant_id,
            "role binding",
            tenant.role_bindings.len(),
            limits.role_bindings,
        )?;
        tenant.role_bindings.insert(binding);
        advance_revision(tenant)?;
        Ok(binding)
    }

    pub(crate) fn revoke_role(&mut self, binding: &RoleBinding) -> Result<bool, ControlError> {
        let tenant = self.tenant_mut(&binding.tenant_id)?;
        if !tenant.role_bindings.remove(binding) {
            return Ok(false);
        }
        advance_revision(tenant)?;
        Ok(true)
    }

    pub(crate) fn bind_root_tenant(
        &mut self,
        binding: RootTenantBinding,
    ) -> Result<RootTenantBinding, ControlError> {
        if !self.tenants.contains_key(&binding.tenant_id) {
            return Err(ControlError::TenantNotFound(binding.tenant_id));
        }
        match self.root_tenants.get(&binding.root_id) {
            Some(tenant_id) if *tenant_id == binding.tenant_id => Ok(binding),
            Some(_) => Err(ControlError::RootTenantAlreadyBound {
                root_id: binding.root_id,
            }),
            None => {
                self.root_tenants.insert(binding.root_id, binding.tenant_id);
                Ok(binding)
            }
        }
    }

    pub(crate) fn root_tenant(&self, root_id: &RootId) -> Option<RootTenantBinding> {
        self.root_tenants
            .get(root_id)
            .map(|tenant_id| RootTenantBinding {
                root_id: *root_id,
                tenant_id: *tenant_id,
            })
    }

    pub(crate) fn policy(&self, tenant_id: &TenantId) -> Option<TenantPolicy> {
        let tenant = self.tenants.get(tenant_id)?;
        Some(TenantPolicy {
            tenant_id: *tenant_id,
            revision: tenant.revision,
            principals: tenant
                .principals
                .iter()
                .map(|principal_id| PrincipalRecord {
                    tenant_id: *tenant_id,
                    principal_id: *principal_id,
                })
                .collect(),
            credentials: tenant.credentials.values().copied().collect(),
            role_bindings: tenant.role_bindings.iter().copied().collect(),
        })
    }

    pub(crate) fn revision(&self, tenant_id: &TenantId) -> Option<u64> {
        self.tenants.get(tenant_id).map(|tenant| tenant.revision)
    }

    fn tenant_mut(&mut self, tenant_id: &TenantId) -> Result<&mut TenantState, ControlError> {
        self.tenants
            .get_mut(tenant_id)
            .ok_or(ControlError::TenantNotFound(*tenant_id))
    }
}

pub(crate) fn validate_credential(credential: &CredentialBinding) -> Result<(), ControlError> {
    if credential.expires_at_unix_seconds == 0 {
        return Err(ControlError::InvalidOptions(
            "a credential must expire at a non-zero Unix time".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn check_limit(
    tenant_id: TenantId,
    record: &'static str,
    current: usize,
    limit: usize,
) -> Result<(), ControlError> {
    if current >= limit {
        return Err(ControlError::TenantPolicyLimitExceeded {
            tenant_id,
            record,
            limit,
        });
    }
    Ok(())
}

pub(crate) fn require_root_in_tenant(
    root_id: RootId,
    owner: Option<TenantId>,
    tenant_id: TenantId,
) -> Result<(), ControlError> {
    if owner != Some(tenant_id) {
        return Err(ControlError::RootNotInTenant { root_id, tenant_id });
    }
    Ok(())
}

fn require_principal(
    tenant: &TenantState,
    tenant_id: TenantId,
    principal_id: PrincipalId,
) -> Result<(), ControlError> {
    if !tenant.principals.contains(&principal_id) {
        return Err(ControlError::PrincipalNotFound {
            tenant_id,
            principal_id,
        });
    }
    Ok(())
}

fn advance_revision(tenant: &mut TenantState) -> Result<(), ControlError> {
    tenant.revision = tenant
        .revision
        .checked_add(1)
        .ok_or_else(|| ControlError::Backend("tenant policy revision overflowed".to_owned()))?;
    Ok(())
}

impl IdentityStore for InMemoryControlStore {
    fn create_tenant(&self, tenant: TenantRecord) -> Result<TenantRecord, ControlError> {
        Ok(self.identity().create_tenant(tenant))
    }

    fn create_principal(
        &self,
        principal: PrincipalRecord,
    ) -> Result<PrincipalRecord, ControlError> {
        self.identity()
            .create_principal(principal, IdentityLimits::DEFAULT)
    }

    fn add_credential(
        &self,
        credential: CredentialBinding,
    ) -> Result<CredentialBinding, ControlError> {
        self.identity()
            .add_credential(credential, IdentityLimits::DEFAULT)
    }

    fn revoke_credential(
        &self,
        tenant_id: &TenantId,
        fingerprint: &CredentialFingerprint,
    ) -> Result<bool, ControlError> {
        self.identity().revoke_credential(tenant_id, fingerprint)
    }

    fn grant_role(&self, binding: RoleBinding) -> Result<RoleBinding, ControlError> {
        self.identity().grant_role(binding, IdentityLimits::DEFAULT)
    }

    fn revoke_role(&self, binding: &RoleBinding) -> Result<bool, ControlError> {
        self.identity().revoke_role(binding)
    }

    fn bind_root_tenant(
        &self,
        binding: RootTenantBinding,
    ) -> Result<RootTenantBinding, ControlError> {
        self.identity().bind_root_tenant(binding)
    }

    fn get_root_tenant(&self, root_id: &RootId) -> Result<Option<RootTenantBinding>, ControlError> {
        Ok(self.identity().root_tenant(root_id))
    }

    fn load_tenant_policy(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantPolicy>, ControlError> {
        Ok(self.identity().policy(tenant_id))
    }

    fn tenant_policy_revision(&self, tenant_id: &TenantId) -> Result<Option<u64>, ControlError> {
        Ok(self.identity().revision(tenant_id))
    }
}

#[cfg(test)]
mod tests {
    use nokv_types::Role;

    use super::*;

    fn tenant(fill: u8) -> TenantId {
        TenantId::from_bytes([fill; 16])
    }

    fn principal(fill: u8) -> PrincipalRecord {
        PrincipalRecord {
            tenant_id: tenant(1),
            principal_id: PrincipalId::from_bytes([fill; 16]),
        }
    }

    #[test]
    fn in_memory_store_passes_identity_conformance() {
        crate::identity_conformance::run_all(&|scenario| scenario(&InMemoryControlStore::new()));
    }

    #[test]
    fn a_full_tenant_refuses_new_records_but_accepts_exact_replays() {
        let limits = IdentityLimits {
            principals: 1,
            credentials: 1,
            role_bindings: 1,
        };
        let mut state = IdentityState::default();
        state.create_tenant(TenantRecord {
            tenant_id: tenant(1),
        });
        state.create_principal(principal(2), limits).unwrap();
        assert!(matches!(
            state.create_principal(principal(3), limits),
            Err(ControlError::TenantPolicyLimitExceeded {
                record: "principal",
                limit: 1,
                ..
            })
        ));
        assert_eq!(
            state.create_principal(principal(2), limits).unwrap(),
            principal(2)
        );

        let credential = |fill: u8| CredentialBinding {
            tenant_id: tenant(1),
            principal_id: principal(2).principal_id,
            fingerprint: CredentialFingerprint::from_bytes([fill; 32]),
            expires_at_unix_seconds: 9,
        };
        state.add_credential(credential(4), limits).unwrap();
        assert!(matches!(
            state.add_credential(credential(5), limits),
            Err(ControlError::TenantPolicyLimitExceeded {
                record: "credential",
                ..
            })
        ));
        assert_eq!(
            state.add_credential(credential(4), limits).unwrap(),
            credential(4)
        );

        let role = |role: Role| RoleBinding {
            tenant_id: tenant(1),
            principal_id: principal(2).principal_id,
            role,
            scope: RoleScope::Tenant,
        };
        state.grant_role(role(Role::Reader), limits).unwrap();
        assert!(matches!(
            state.grant_role(role(Role::Writer), limits),
            Err(ControlError::TenantPolicyLimitExceeded {
                record: "role binding",
                ..
            })
        ));
        // Three accepted changes after creation.
        assert_eq!(state.revision(&tenant(1)), Some(4));
    }
}
