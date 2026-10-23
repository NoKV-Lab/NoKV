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

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nokv_control::{ControlError, IdentityStore, TenantPolicy};
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
/// refresh. After that it refuses every request until a refresh succeeds.
pub const MAX_POLICY_AGE: Duration = Duration::from_secs(10);

/// The latest copy of one tenant's identity records, and the decisions an
/// owner makes from it.
pub struct TenantAccess {
    identity: Arc<dyn IdentityStore>,
    tenant_id: TenantId,
    copy: RwLock<PolicyCopy>,
}

struct PolicyCopy {
    policy: TenantPolicy,
    refreshed_at: Instant,
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
    /// anyone and must not start.
    pub fn load(
        identity: Arc<dyn IdentityStore>,
        tenant_id: TenantId,
    ) -> Result<Self, ServerError> {
        let policy = identity
            .load_tenant_policy(&tenant_id)?
            .ok_or(ServerError::Control(ControlError::TenantNotFound(
                tenant_id,
            )))?;
        Ok(Self {
            identity,
            tenant_id,
            copy: RwLock::new(PolicyCopy {
                policy,
                refreshed_at: Instant::now(),
            }),
        })
    }

    /// Reload the policy when its revision moved. A failure keeps the last
    /// copy, which stops authorizing once it is older than `MAX_POLICY_AGE`.
    pub fn refresh(&self) -> Result<(), ControlError> {
        let revision = self.identity.tenant_policy_revision(&self.tenant_id)?;
        let current = self.read_copy().policy.revision;
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
        if let Some(policy) = reloaded {
            // Revisions only grow; a slower concurrent read never wins.
            if policy.revision >= copy.policy.revision {
                copy.policy = policy;
            }
        }
        copy.refreshed_at = Instant::now();
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
            .policy
            .credentials
            .iter()
            .find(|credential| credential.fingerprint == fingerprint)
            .filter(|credential| credential.expires_at_unix_seconds > now_unix_seconds)
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
        let current = copy.policy.credentials.iter().any(|credential| {
            credential.fingerprint == principal.fingerprint
                && credential.principal_id == principal.principal_id
                && credential.expires_at_unix_seconds > now_unix_seconds
        });
        if !current {
            return Err(AccessDenial::NotACurrentCredential);
        }
        let required = required_role(&request.operation);
        let root_id = RootId::from_bytes(request.route.root_id.0);
        let held = effective_role(&copy.policy, principal.principal_id, root_id);
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
        if copy.refreshed_at.elapsed() > MAX_POLICY_AGE {
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

/// The highest role `principal_id` holds on `root_id`: tenant-wide roles
/// cover every root of the tenant, and root roles cover their own root.
fn effective_role(
    policy: &TenantPolicy,
    principal_id: PrincipalId,
    root_id: RootId,
) -> Option<Role> {
    policy
        .role_bindings
        .iter()
        .filter(|binding| binding.principal_id == principal_id)
        .filter(|binding| match binding.scope {
            RoleScope::Tenant => true,
            RoleScope::Root(scope) => scope == root_id,
        })
        .map(|binding| binding.role)
        .max()
}

/// The least role that permits an operation. Every operation is listed, so a
/// new one cannot compile until it is classified.
pub(crate) fn required_role(operation: &WorkspaceRequest) -> Role {
    match operation {
        WorkspaceRequest::Preflight(_)
        | WorkspaceRequest::GetWorkspace(_)
        | WorkspaceRequest::GetPath(_)
        | WorkspaceRequest::ListPaths(_)
        | WorkspaceRequest::GetSnapshot(_)
        | WorkspaceRequest::ListSnapshots(_)
        | WorkspaceRequest::ReadRestoreSourceRunManifest(_)
        | WorkspaceRequest::GetOperation(_)
        | WorkspaceRequest::InspectAppendCleanup(_)
        | WorkspaceRequest::GetGenericIndexRegistration(_)
        | WorkspaceRequest::Search(_)
        | WorkspaceRequest::Aggregate(_)
        | WorkspaceRequest::Catalog(_)
        | WorkspaceRequest::FindWorkspaces(_)
        | WorkspaceRequest::ReadChanges(_) => Role::Reader,
        WorkspaceRequest::CreateWorkspace(_)
        | WorkspaceRequest::RenamePath(_)
        | WorkspaceRequest::RemovePath(_)
        | WorkspaceRequest::BeginArtifactPublish(_)
        | WorkspaceRequest::StageArtifactObjects(_)
        | WorkspaceRequest::MarkArtifactObjectsUploaded(_)
        | WorkspaceRequest::StageArtifactManifest(_)
        | WorkspaceRequest::CompleteArtifactPublish(_)
        | WorkspaceRequest::PrepareArtifactPublish(_)
        | WorkspaceRequest::FinishArtifactPublish(_)
        | WorkspaceRequest::PublishInlineArtifact(_)
        | WorkspaceRequest::AbortArtifactPublish(_)
        | WorkspaceRequest::Commit(_)
        | WorkspaceRequest::MintSnapshot(_)
        | WorkspaceRequest::RenewSnapshot(_)
        | WorkspaceRequest::RetireSnapshot(_)
        | WorkspaceRequest::PrepareRestore(_)
        | WorkspaceRequest::BindRestoreDestination(_)
        | WorkspaceRequest::FinalizeRestore(_)
        // An Agent retries its own quarantined append cleanup.
        | WorkspaceRequest::RetryAppendCleanup(_)
        | WorkspaceRequest::BeginGenericIndexRegistration(_)
        | WorkspaceRequest::AppendGenericIndexRows(_)
        | WorkspaceRequest::FinalizeGenericIndexRegistration(_)
        | WorkspaceRequest::AbortGenericIndexRegistration(_) => Role::Writer,
        WorkspaceRequest::ReconcileQuarantinedArtifactPublish(_) => Role::Operator,
    }
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
            let access = TenantAccess::load(store, TENANT).unwrap();
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
        let access = TenantAccess::load(store, TENANT).unwrap();

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
        let access = TenantAccess::load(store.clone(), TENANT).unwrap();
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
        let access = TenantAccess::load(store.clone(), TENANT).unwrap();
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
        let access = TenantAccess::load(store, TENANT).unwrap();
        access.age_for_test(MAX_POLICY_AGE + Duration::from_secs(1));
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
        let access = TenantAccess::load(store, TENANT).unwrap();

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
            TenantAccess::load(store, TENANT),
            Err(ServerError::Control(ControlError::TenantNotFound(tenant))) if tenant == TENANT
        ));
    }
}
