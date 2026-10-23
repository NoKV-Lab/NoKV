/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Which authenticated principal may make which request.
//!
//! An owner serves the roots of one tenant. It keeps a copy of that tenant's
//! identity records, refreshed from the control plane, and checks every TLS
//! handshake and every request against the latest copy: the certificate must
//! still be a current credential, and the principal must hold a role on the
//! request's root that permits the request. A denied request learns nothing
//! about routes.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nokv_control::{ControlError, CredentialBinding, IdentityStore, RoleBinding, TenantPolicy};
use nokv_protocol::{WorkspaceRequest, WorkspaceRpcRequest};
use nokv_types::{CredentialFingerprint, PrincipalId, Role, RoleScope, RootId, TenantId};
use rustls::pki_types::CertificateDer;
use sha2::{Digest as _, Sha256};

use crate::ServerError;

/// How often an owner checks whether its tenant's policy changed. A revoked
/// credential or role stops working within this interval, plus the duration
/// of a request already admitted.
pub const POLICY_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// How long an owner trusts its copy of the policy without a successful
/// refresh when its control plane sets no other bound. An owner on etcd uses
/// its lease TTL instead: past that it has lost the right to serve anyway.
pub const DEFAULT_MAX_POLICY_AGE: Duration = Duration::from_secs(10);

/// The latest copy of one tenant's identity records, and the decisions an
/// owner makes from it.
pub struct TenantAccess {
    identity: Arc<dyn IdentityStore>,
    tenant_id: TenantId,
    max_age: Duration,
    copy: RwLock<PolicyCopy>,
}

/// One policy revision, indexed for the two lookups every decision makes.
struct PolicyCopy {
    revision: u64,
    credentials: HashMap<CredentialFingerprint, CredentialBinding>,
    roles: HashMap<PrincipalId, Vec<RoleBinding>>,
    refreshed_at: Instant,
}

impl PolicyCopy {
    fn index(policy: TenantPolicy) -> Self {
        let mut roles: HashMap<PrincipalId, Vec<RoleBinding>> = HashMap::new();
        for binding in policy.role_bindings {
            roles.entry(binding.principal_id).or_default().push(binding);
        }
        Self {
            revision: policy.revision,
            credentials: policy
                .credentials
                .into_iter()
                .map(|credential| (credential.fingerprint, credential))
                .collect(),
            roles,
            refreshed_at: Instant::now(),
        }
    }

    /// The credential behind `fingerprint`, when it is current.
    fn current_credential(
        &self,
        fingerprint: &CredentialFingerprint,
        now_unix_seconds: u64,
    ) -> Option<&CredentialBinding> {
        self.credentials
            .get(fingerprint)
            .filter(|credential| credential.expires_at_unix_seconds > now_unix_seconds)
    }

    /// The highest role `principal_id` holds on `root_id`: tenant-wide roles
    /// cover every root of the tenant, and root roles cover their own root.
    fn effective_role(&self, principal_id: PrincipalId, root_id: RootId) -> Option<Role> {
        self.roles
            .get(&principal_id)?
            .iter()
            .filter(|binding| match binding.scope {
                RoleScope::Tenant => true,
                RoleScope::Root(scope) => scope == root_id,
            })
            .map(|binding| binding.role)
            .max()
    }
}

/// The credential a TLS connection authenticated with, and its principal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConnectionPrincipal {
    pub(crate) principal_id: PrincipalId,
    pub(crate) fingerprint: CredentialFingerprint,
}

/// Why a connection or a request is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessDenial {
    /// The certificate is not, or is no longer, a current credential of the
    /// tenant for this principal.
    NotACurrentCredential,
    /// The principal holds no role on the request's root that includes
    /// `required`.
    PermissionDenied { required: Role },
    /// The owner has not refreshed its copy of the policy within
    /// `MAX_POLICY_AGE`; a later attempt may succeed.
    PolicyStale,
}

impl TenantAccess {
    /// Load the tenant's records; an owner without them cannot authenticate
    /// anyone and must not start. `max_age` bounds how long the copy serves
    /// without a successful refresh.
    pub fn load(
        identity: Arc<dyn IdentityStore>,
        tenant_id: TenantId,
        max_age: Duration,
    ) -> Result<Self, ServerError> {
        let policy = identity
            .load_tenant_policy(&tenant_id)?
            .ok_or(ServerError::Control(ControlError::TenantNotFound(
                tenant_id,
            )))?;
        Ok(Self {
            identity,
            tenant_id,
            max_age,
            copy: RwLock::new(PolicyCopy::index(policy)),
        })
    }

    /// How long the copy serves without a successful refresh.
    pub fn max_age(&self) -> Duration {
        self.max_age
    }

    /// Reload the policy when its revision moved. A failure keeps the last
    /// copy, which stops authorizing once it is older than `max_age`.
    pub fn refresh(&self) -> Result<(), ControlError> {
        let revision = self.identity.tenant_policy_revision(&self.tenant_id)?;
        let current = self.read_copy().revision;
        let reloaded = if revision == Some(current) {
            None
        } else {
            Some(
                self.identity
                    .load_tenant_policy(&self.tenant_id)?
                    .ok_or(ControlError::TenantNotFound(self.tenant_id))?,
            )
        };
        let mut copy = self
            .copy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match reloaded {
            // Revisions only grow; a slower concurrent read never wins.
            Some(policy) if policy.revision >= copy.revision => {
                *copy = PolicyCopy::index(policy);
            }
            _ => copy.refreshed_at = Instant::now(),
        }
        Ok(())
    }

    /// The principal whose current credential is `certificate`.
    pub(crate) fn authenticate(
        &self,
        certificate: &CertificateDer<'_>,
        now_unix_seconds: u64,
    ) -> Result<ConnectionPrincipal, AccessDenial> {
        let fingerprint = CredentialFingerprint::from_bytes(Sha256::digest(certificate).into());
        let copy = self.fresh_copy()?;
        let credential = copy
            .current_credential(&fingerprint, now_unix_seconds)
            .ok_or(AccessDenial::NotACurrentCredential)?;
        Ok(ConnectionPrincipal {
            principal_id: credential.principal_id,
            fingerprint,
        })
    }

    /// Whether `principal` may make `request` now. The credential is checked
    /// again, so a revoked or expired one stops an open connection too.
    pub(crate) fn authorize(
        &self,
        principal: &ConnectionPrincipal,
        request: &WorkspaceRpcRequest,
        now_unix_seconds: u64,
    ) -> Result<(), AccessDenial> {
        let copy = self.fresh_copy()?;
        let current = copy
            .current_credential(&principal.fingerprint, now_unix_seconds)
            .is_some_and(|credential| credential.principal_id == principal.principal_id);
        if !current {
            return Err(AccessDenial::NotACurrentCredential);
        }
        let required = classify(&request.operation).role;
        let root_id = RootId::from_bytes(request.route.root_id.0);
        let held = copy.effective_role(principal.principal_id, root_id);
        if held.is_some_and(|role| role >= required) {
            Ok(())
        } else {
            Err(AccessDenial::PermissionDenied { required })
        }
    }

    fn read_copy(&self) -> std::sync::RwLockReadGuard<'_, PolicyCopy> {
        self.copy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn fresh_copy(&self) -> Result<std::sync::RwLockReadGuard<'_, PolicyCopy>, AccessDenial> {
        let copy = self.read_copy();
        if copy.refreshed_at.elapsed() > self.max_age {
            return Err(AccessDenial::PolicyStale);
        }
        Ok(copy)
    }

    #[cfg(test)]
    pub(crate) fn age_for_test(&self, age: Duration) {
        let mut copy = self.copy.write().unwrap();
        copy.refreshed_at = Instant::now() - age;
    }
}

/// What an operation is called in logs and the least role that permits it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OperationClass {
    pub(crate) name: &'static str,
    pub(crate) role: Role,
}

/// Every operation is listed, so a new one cannot compile until it is
/// classified.
pub(crate) fn classify(operation: &WorkspaceRequest) -> OperationClass {
    let (name, role) = match operation {
        WorkspaceRequest::Preflight(_) => ("Preflight", Role::Reader),
        WorkspaceRequest::GetWorkspace(_) => ("GetWorkspace", Role::Reader),
        WorkspaceRequest::GetPath(_) => ("GetPath", Role::Reader),
        WorkspaceRequest::ListPaths(_) => ("ListPaths", Role::Reader),
        WorkspaceRequest::GetSnapshot(_) => ("GetSnapshot", Role::Reader),
        WorkspaceRequest::ListSnapshots(_) => ("ListSnapshots", Role::Reader),
        WorkspaceRequest::ReadRestoreSourceRunManifest(_) => {
            ("ReadRestoreSourceRunManifest", Role::Reader)
        }
        WorkspaceRequest::GetOperation(_) => ("GetOperation", Role::Reader),
        WorkspaceRequest::InspectAppendCleanup(_) => ("InspectAppendCleanup", Role::Reader),
        WorkspaceRequest::GetGenericIndexRegistration(_) => {
            ("GetGenericIndexRegistration", Role::Reader)
        }
        WorkspaceRequest::Search(_) => ("Search", Role::Reader),
        WorkspaceRequest::Aggregate(_) => ("Aggregate", Role::Reader),
        WorkspaceRequest::Catalog(_) => ("Catalog", Role::Reader),
        WorkspaceRequest::FindWorkspaces(_) => ("FindWorkspaces", Role::Reader),
        WorkspaceRequest::ReadChanges(_) => ("ReadChanges", Role::Reader),
        WorkspaceRequest::CreateWorkspace(_) => ("CreateWorkspace", Role::Writer),
        WorkspaceRequest::RenamePath(_) => ("RenamePath", Role::Writer),
        WorkspaceRequest::RemovePath(_) => ("RemovePath", Role::Writer),
        WorkspaceRequest::BeginArtifactPublish(_) => ("BeginArtifactPublish", Role::Writer),
        WorkspaceRequest::StageArtifactObjects(_) => ("StageArtifactObjects", Role::Writer),
        WorkspaceRequest::MarkArtifactObjectsUploaded(_) => {
            ("MarkArtifactObjectsUploaded", Role::Writer)
        }
        WorkspaceRequest::StageArtifactManifest(_) => ("StageArtifactManifest", Role::Writer),
        WorkspaceRequest::CompleteArtifactPublish(_) => ("CompleteArtifactPublish", Role::Writer),
        WorkspaceRequest::PrepareArtifactPublish(_) => ("PrepareArtifactPublish", Role::Writer),
        WorkspaceRequest::FinishArtifactPublish(_) => ("FinishArtifactPublish", Role::Writer),
        WorkspaceRequest::PublishInlineArtifact(_) => ("PublishInlineArtifact", Role::Writer),
        WorkspaceRequest::AbortArtifactPublish(_) => ("AbortArtifactPublish", Role::Writer),
        WorkspaceRequest::Commit(_) => ("Commit", Role::Writer),
        WorkspaceRequest::MintSnapshot(_) => ("MintSnapshot", Role::Writer),
        WorkspaceRequest::RenewSnapshot(_) => ("RenewSnapshot", Role::Writer),
        WorkspaceRequest::RetireSnapshot(_) => ("RetireSnapshot", Role::Writer),
        WorkspaceRequest::PrepareRestore(_) => ("PrepareRestore", Role::Writer),
        WorkspaceRequest::BindRestoreDestination(_) => ("BindRestoreDestination", Role::Writer),
        WorkspaceRequest::FinalizeRestore(_) => ("FinalizeRestore", Role::Writer),
        // An Agent retries its own quarantined append cleanup.
        WorkspaceRequest::RetryAppendCleanup(_) => ("RetryAppendCleanup", Role::Writer),
        WorkspaceRequest::BeginGenericIndexRegistration(_) => {
            ("BeginGenericIndexRegistration", Role::Writer)
        }
        WorkspaceRequest::AppendGenericIndexRows(_) => ("AppendGenericIndexRows", Role::Writer),
        WorkspaceRequest::FinalizeGenericIndexRegistration(_) => {
            ("FinalizeGenericIndexRegistration", Role::Writer)
        }
        WorkspaceRequest::AbortGenericIndexRegistration(_) => {
            ("AbortGenericIndexRegistration", Role::Writer)
        }
        WorkspaceRequest::ReconcileQuarantinedArtifactPublish(_) => {
            ("ReconcileQuarantinedArtifactPublish", Role::Operator)
        }
    };
    OperationClass { name, role }
}

#[cfg(test)]
mod tests {
    use nokv_control::{
        CredentialBinding, InMemoryControlStore, PrincipalRecord, RoleBinding, RootTenantBinding,
        TenantRecord,
    };
    use nokv_protocol::{
        CreateWorkspaceRequest, Digest, GetWorkspaceRequest, LogicalShardIdentity,
        ObjectNamespaceIdentity, OperationIdentity, OperationToken, QuarantineResolution,
        ReconcileQuarantinedArtifactPublishRequest, RequestIdentity, RootIdentity, RootRoute,
        WorkbenchName, WorkspaceIdentity,
    };

    use super::*;

    const NOW: u64 = 1_800_000_000;
    const TENANT: TenantId = TenantId::from_bytes([5; 16]);
    const AGENT: PrincipalId = PrincipalId::from_bytes([6; 16]);
    const FINGERPRINT: CredentialFingerprint = CredentialFingerprint::from_bytes([7; 32]);

    fn agent() -> ConnectionPrincipal {
        ConnectionPrincipal {
            principal_id: AGENT,
            fingerprint: FINGERPRINT,
        }
    }

    /// A tenant owning roots 1 and 2, with one principal and its credential.
    fn store() -> Arc<InMemoryControlStore> {
        let store = Arc::new(InMemoryControlStore::new());
        store
            .create_tenant(TenantRecord { tenant_id: TENANT })
            .unwrap();
        store
            .create_principal(PrincipalRecord {
                tenant_id: TENANT,
                principal_id: AGENT,
            })
            .unwrap();
        store
            .add_credential(CredentialBinding {
                tenant_id: TENANT,
                principal_id: AGENT,
                fingerprint: FINGERPRINT,
                expires_at_unix_seconds: NOW + 60,
            })
            .unwrap();
        for root in [1, 2] {
            store
                .bind_root_tenant(RootTenantBinding {
                    root_id: RootId::from_bytes([root; 16]),
                    tenant_id: TENANT,
                })
                .unwrap();
        }
        store
    }

    fn grant(store: &InMemoryControlStore, role: Role, scope: RoleScope) -> RoleBinding {
        let binding = RoleBinding {
            tenant_id: TENANT,
            principal_id: AGENT,
            role,
            scope,
        };
        store.grant_role(binding).unwrap();
        binding
    }

    fn request(root: u8, operation: WorkspaceRequest) -> WorkspaceRpcRequest {
        WorkspaceRpcRequest {
            route: RootRoute {
                root_id: RootIdentity([root; 16]),
                logical_shard_id: LogicalShardIdentity([3; 16]),
                object_namespace_id: ObjectNamespaceIdentity([4; 16]),
                placement_generation: 1,
                owner_epoch: 1,
            },
            request_id: RequestIdentity([8; 16]),
            operation,
        }
    }

    fn read(root: u8) -> WorkspaceRpcRequest {
        request(
            root,
            WorkspaceRequest::GetWorkspace(GetWorkspaceRequest {
                workbench: WorkbenchName::new("run").unwrap(),
            }),
        )
    }

    fn write(root: u8) -> WorkspaceRpcRequest {
        request(
            root,
            WorkspaceRequest::CreateWorkspace(CreateWorkspaceRequest {
                workbench: WorkbenchName::new("run").unwrap(),
                workspace_incarnation_id: WorkspaceIdentity([9; 16]),
            }),
        )
    }

    fn operate(root: u8) -> WorkspaceRpcRequest {
        request(
            root,
            WorkspaceRequest::ReconcileQuarantinedArtifactPublish(
                ReconcileQuarantinedArtifactPublishRequest {
                    token: OperationToken {
                        operation_id: OperationIdentity([10; 16]),
                        state_digest: Digest([11; 32]),
                    },
                    resolution: QuarantineResolution::ProviderObjectsAbsent,
                    reason: "verified".to_owned(),
                    evidence_digest: Digest([12; 32]),
                },
            ),
        )
    }

    #[test]
    fn each_role_permits_its_own_requests_and_those_of_lesser_roles() {
        let denied = |required| Err(AccessDenial::PermissionDenied { required });
        for (role, expected) in [
            (
                None,
                [
                    denied(Role::Reader),
                    denied(Role::Writer),
                    denied(Role::Operator),
                ],
            ),
            (
                Some(Role::Reader),
                [Ok(()), denied(Role::Writer), denied(Role::Operator)],
            ),
            (Some(Role::Writer), [Ok(()), Ok(()), denied(Role::Operator)]),
            (Some(Role::Operator), [Ok(()), Ok(()), Ok(())]),
        ] {
            let store = store();
            if let Some(role) = role {
                grant(&store, role, RoleScope::Tenant);
            }
            let access = TenantAccess::load(store, TENANT, DEFAULT_MAX_POLICY_AGE).unwrap();
            let decisions = [read(1), write(1), operate(1)]
                .map(|request| access.authorize(&agent(), &request, NOW));
            assert_eq!(decisions, expected, "{role:?}");
        }
    }

    #[test]
    fn a_root_role_covers_only_its_root_and_the_highest_role_wins() {
        let store = store();
        grant(&store, Role::Reader, RoleScope::Tenant);
        grant(
            &store,
            Role::Writer,
            RoleScope::Root(RootId::from_bytes([1; 16])),
        );
        let access = TenantAccess::load(store, TENANT, DEFAULT_MAX_POLICY_AGE).unwrap();

        assert_eq!(access.authorize(&agent(), &write(1), NOW), Ok(()));
        assert_eq!(access.authorize(&agent(), &read(2), NOW), Ok(()));
        assert_eq!(
            access.authorize(&agent(), &write(2), NOW),
            Err(AccessDenial::PermissionDenied {
                required: Role::Writer
            })
        );
    }

    #[test]
    fn a_revoked_or_expired_credential_stops_an_open_connection() {
        let store = store();
        grant(&store, Role::Writer, RoleScope::Tenant);
        let access = TenantAccess::load(store.clone(), TENANT, DEFAULT_MAX_POLICY_AGE).unwrap();
        assert_eq!(access.authorize(&agent(), &write(1), NOW), Ok(()));
        assert_eq!(
            access.authorize(&agent(), &write(1), NOW + 60),
            Err(AccessDenial::NotACurrentCredential)
        );
        // Another principal cannot ride on this credential.
        let other = ConnectionPrincipal {
            principal_id: PrincipalId::from_bytes([13; 16]),
            ..agent()
        };
        assert_eq!(
            access.authorize(&other, &read(1), NOW),
            Err(AccessDenial::NotACurrentCredential)
        );

        store.revoke_credential(&TENANT, &FINGERPRINT).unwrap();
        // Until the next refresh the owner still holds the old copy.
        assert_eq!(access.authorize(&agent(), &write(1), NOW), Ok(()));
        access.refresh().unwrap();
        assert_eq!(
            access.authorize(&agent(), &write(1), NOW),
            Err(AccessDenial::NotACurrentCredential)
        );
    }

    #[test]
    fn a_granted_or_revoked_role_takes_effect_at_the_next_refresh() {
        let store = store();
        let access = TenantAccess::load(store.clone(), TENANT, DEFAULT_MAX_POLICY_AGE).unwrap();
        let reader_denied = Err(AccessDenial::PermissionDenied {
            required: Role::Reader,
        });
        assert_eq!(access.authorize(&agent(), &read(1), NOW), reader_denied);

        let binding = grant(&store, Role::Reader, RoleScope::Tenant);
        access.refresh().unwrap();
        assert_eq!(access.authorize(&agent(), &read(1), NOW), Ok(()));

        store.revoke_role(&binding).unwrap();
        access.refresh().unwrap();
        assert_eq!(access.authorize(&agent(), &read(1), NOW), reader_denied);
    }

    #[test]
    fn a_copy_that_cannot_be_refreshed_stops_authorizing_after_its_bound() {
        let store = store();
        grant(&store, Role::Writer, RoleScope::Tenant);
        let access = TenantAccess::load(store, TENANT, DEFAULT_MAX_POLICY_AGE).unwrap();
        access.age_for_test(DEFAULT_MAX_POLICY_AGE + Duration::from_secs(1));
        assert_eq!(
            access.authorize(&agent(), &read(1), NOW),
            Err(AccessDenial::PolicyStale)
        );
        access.refresh().unwrap();
        assert_eq!(access.authorize(&agent(), &read(1), NOW), Ok(()));
    }

    fn certificate() -> CertificateDer<'static> {
        let key = rcgen::KeyPair::generate().unwrap();
        rcgen::CertificateParams::new(vec!["agent".to_owned()])
            .unwrap()
            .self_signed(&key)
            .unwrap()
            .der()
            .clone()
    }

    #[test]
    fn a_certificate_authenticates_the_principal_of_its_current_credential() {
        let store = store();
        let (current, expired, unknown) = (certificate(), certificate(), certificate());
        for (certificate, expires_at_unix_seconds) in [(&current, NOW + 1), (&expired, NOW)] {
            store
                .add_credential(CredentialBinding {
                    tenant_id: TENANT,
                    principal_id: AGENT,
                    fingerprint: CredentialFingerprint::from_bytes(
                        Sha256::digest(certificate).into(),
                    ),
                    expires_at_unix_seconds,
                })
                .unwrap();
        }
        let access = TenantAccess::load(store, TENANT, DEFAULT_MAX_POLICY_AGE).unwrap();

        let principal = access.authenticate(&current, NOW).unwrap();
        assert_eq!(principal.principal_id, AGENT);
        // Expiry is exclusive.
        assert_eq!(
            access.authenticate(&expired, NOW),
            Err(AccessDenial::NotACurrentCredential)
        );
        assert_eq!(
            access.authenticate(&unknown, NOW),
            Err(AccessDenial::NotACurrentCredential)
        );
    }

    #[test]
    fn an_owner_without_tenant_records_does_not_start() {
        let store = Arc::new(InMemoryControlStore::new());
        assert!(matches!(
            TenantAccess::load(store, TENANT, DEFAULT_MAX_POLICY_AGE),
            Err(ServerError::Control(ControlError::TenantNotFound(tenant))) if tenant == TENANT
        ));
    }
}
