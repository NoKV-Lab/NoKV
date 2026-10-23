//! Provider-neutral identity-store conformance checks.
//!
//! Every `IdentityStore` backend runs the same scenarios, so the in-memory,
//! local, and etcd stores cannot diverge on idempotence, tenant isolation, or
//! revision accounting.

use crate::{
    ControlError, CredentialBinding, CredentialFingerprint, IdentityStore, PrincipalId,
    PrincipalRecord, Role, RoleBinding, RoleScope, RootId, RootTenantBinding, TenantId,
    TenantPolicy, TenantRecord,
};

fn tenant(fill: u8) -> TenantId {
    TenantId::from_bytes([fill; 16])
}

fn root(fill: u8) -> RootId {
    RootId::from_bytes([fill; 16])
}

fn principal(tenant_fill: u8, fill: u8) -> PrincipalRecord {
    PrincipalRecord {
        tenant_id: tenant(tenant_fill),
        principal_id: PrincipalId::from_bytes([fill; 16]),
    }
}

fn credential(tenant_fill: u8, principal_fill: u8, fingerprint_fill: u8) -> CredentialBinding {
    CredentialBinding {
        tenant_id: tenant(tenant_fill),
        principal_id: PrincipalId::from_bytes([principal_fill; 16]),
        fingerprint: CredentialFingerprint::from_bytes([fingerprint_fill; 32]),
        expires_at_unix_seconds: 1_800_000_000,
    }
}

fn role(tenant_fill: u8, principal_fill: u8, role: Role, scope: RoleScope) -> RoleBinding {
    RoleBinding {
        tenant_id: tenant(tenant_fill),
        principal_id: PrincipalId::from_bytes([principal_fill; 16]),
        role,
        scope,
    }
}

fn create_tenant(store: &dyn IdentityStore, fill: u8) {
    let record = TenantRecord {
        tenant_id: tenant(fill),
    };
    assert_eq!(store.create_tenant(record).unwrap(), record);
}

fn revision(store: &dyn IdentityStore, fill: u8) -> u64 {
    store
        .tenant_policy_revision(&tenant(fill))
        .unwrap()
        .expect("tenant exists")
}

pub(crate) fn changes_advance_the_revision_once_and_replays_never(store: &dyn IdentityStore) {
    assert_eq!(store.tenant_policy_revision(&tenant(1)).unwrap(), None);
    assert_eq!(store.load_tenant_policy(&tenant(1)).unwrap(), None);
    create_tenant(store, 1);
    create_tenant(store, 1);
    assert_eq!(revision(store, 1), 1);
    assert_eq!(
        store.load_tenant_policy(&tenant(1)).unwrap(),
        Some(TenantPolicy {
            tenant_id: tenant(1),
            revision: 1,
            principals: Vec::new(),
            credentials: Vec::new(),
            role_bindings: Vec::new(),
        })
    );

    for _ in 0..2 {
        assert_eq!(
            store.create_principal(principal(1, 10)).unwrap(),
            principal(1, 10)
        );
    }
    assert_eq!(revision(store, 1), 2);
    for _ in 0..2 {
        assert_eq!(
            store.add_credential(credential(1, 10, 20)).unwrap(),
            credential(1, 10, 20)
        );
    }
    assert_eq!(revision(store, 1), 3);
    let reader = role(1, 10, Role::Reader, RoleScope::Tenant);
    for _ in 0..2 {
        assert_eq!(store.grant_role(reader).unwrap(), reader);
    }
    assert_eq!(revision(store, 1), 4);
    assert_eq!(
        store.load_tenant_policy(&tenant(1)).unwrap().unwrap(),
        TenantPolicy {
            tenant_id: tenant(1),
            revision: 4,
            principals: vec![principal(1, 10)],
            credentials: vec![credential(1, 10, 20)],
            role_bindings: vec![reader],
        }
    );

    assert!(store.revoke_role(&reader).unwrap());
    assert!(!store.revoke_role(&reader).unwrap());
    assert_eq!(revision(store, 1), 5);
    let fingerprint = credential(1, 10, 20).fingerprint;
    assert!(store.revoke_credential(&tenant(1), &fingerprint).unwrap());
    assert!(!store.revoke_credential(&tenant(1), &fingerprint).unwrap());
    assert_eq!(revision(store, 1), 6);
    let policy = store.load_tenant_policy(&tenant(1)).unwrap().unwrap();
    assert_eq!(policy.revision, 6);
    assert_eq!(policy.principals, vec![principal(1, 10)]);
    assert!(policy.credentials.is_empty() && policy.role_bindings.is_empty());
}

pub(crate) fn records_require_their_tenant_principal_and_root(store: &dyn IdentityStore) {
    assert_eq!(
        store.create_principal(principal(9, 10)),
        Err(ControlError::TenantNotFound(tenant(9)))
    );
    assert_eq!(
        store.revoke_credential(&tenant(9), &CredentialFingerprint::from_bytes([1; 32])),
        Err(ControlError::TenantNotFound(tenant(9)))
    );
    assert_eq!(
        store.revoke_role(&role(9, 10, Role::Reader, RoleScope::Tenant)),
        Err(ControlError::TenantNotFound(tenant(9)))
    );
    create_tenant(store, 1);
    let unregistered = ControlError::PrincipalNotFound {
        tenant_id: tenant(1),
        principal_id: PrincipalId::from_bytes([11; 16]),
    };
    assert_eq!(
        store.add_credential(credential(1, 11, 20)),
        Err(unregistered.clone())
    );
    assert_eq!(
        store.grant_role(role(1, 11, Role::Reader, RoleScope::Tenant)),
        Err(unregistered)
    );

    store.create_principal(principal(1, 12)).unwrap();
    store.create_principal(principal(1, 11)).unwrap();
    let mut unexpiring = credential(1, 11, 20);
    unexpiring.expires_at_unix_seconds = 0;
    assert!(matches!(
        store.add_credential(unexpiring),
        Err(ControlError::InvalidOptions(_))
    ));
    store.add_credential(credential(1, 11, 20)).unwrap();
    let mut extended = credential(1, 11, 20);
    extended.expires_at_unix_seconds += 1;
    let bound = ControlError::CredentialAlreadyBound {
        tenant_id: tenant(1),
        fingerprint: credential(1, 11, 20).fingerprint,
    };
    assert_eq!(store.add_credential(extended), Err(bound.clone()));
    assert_eq!(store.add_credential(credential(1, 12, 20)), Err(bound));

    let root_role = role(1, 11, Role::Writer, RoleScope::Root(root(30)));
    let not_owned = ControlError::RootNotInTenant {
        root_id: root(30),
        tenant_id: tenant(1),
    };
    assert_eq!(store.grant_role(root_role), Err(not_owned));
    let binding = RootTenantBinding {
        root_id: root(30),
        tenant_id: tenant(1),
    };
    assert_eq!(
        store.bind_root_tenant(RootTenantBinding {
            root_id: root(30),
            tenant_id: tenant(9),
        }),
        Err(ControlError::TenantNotFound(tenant(9)))
    );
    assert_eq!(store.get_root_tenant(&root(30)).unwrap(), None);
    for _ in 0..2 {
        assert_eq!(store.bind_root_tenant(binding).unwrap(), binding);
    }
    assert_eq!(store.get_root_tenant(&root(30)).unwrap(), Some(binding));
    assert_eq!(store.get_root_tenant(&root(31)).unwrap(), None);
    assert_eq!(store.grant_role(root_role).unwrap(), root_role);

    let policy = store.load_tenant_policy(&tenant(1)).unwrap().unwrap();
    // Sorted by principal id, whatever the creation order.
    assert_eq!(policy.principals, vec![principal(1, 11), principal(1, 12)]);
    assert_eq!(policy.role_bindings, vec![root_role]);
}

pub(crate) fn tenants_never_reach_each_other(store: &dyn IdentityStore) {
    create_tenant(store, 1);
    create_tenant(store, 2);
    store.create_principal(principal(1, 10)).unwrap();
    store.create_principal(principal(2, 10)).unwrap();
    store
        .bind_root_tenant(RootTenantBinding {
            root_id: root(30),
            tenant_id: tenant(1),
        })
        .unwrap();
    let before = revision(store, 1);

    // A root of tenant 1 is never granted within tenant 2, and a root is
    // never rebound.
    assert_eq!(
        store.grant_role(role(2, 10, Role::Reader, RoleScope::Root(root(30)))),
        Err(ControlError::RootNotInTenant {
            root_id: root(30),
            tenant_id: tenant(2),
        })
    );
    assert_eq!(
        store.bind_root_tenant(RootTenantBinding {
            root_id: root(30),
            tenant_id: tenant(2),
        }),
        Err(ControlError::RootTenantAlreadyBound { root_id: root(30) })
    );

    // One fingerprint may authenticate one principal in each tenant.
    store.add_credential(credential(1, 10, 20)).unwrap();
    store.add_credential(credential(2, 10, 20)).unwrap();
    store
        .grant_role(role(2, 10, Role::Operator, RoleScope::Tenant))
        .unwrap();
    assert_eq!(revision(store, 1), before + 1);
    let first = store.load_tenant_policy(&tenant(1)).unwrap().unwrap();
    assert_eq!(first.credentials, vec![credential(1, 10, 20)]);
    assert!(first.role_bindings.is_empty());
    let second = store.load_tenant_policy(&tenant(2)).unwrap().unwrap();
    assert_eq!(second.principals, vec![principal(2, 10)]);
    assert_eq!(
        second.role_bindings,
        vec![role(2, 10, Role::Operator, RoleScope::Tenant)]
    );
}

/// One conformance scenario.
type Scenario = dyn Fn(&dyn IdentityStore);

/// Run every scenario; `with_fresh_store` runs one scenario against a store
/// no other scenario has touched.
pub(crate) fn run_all(with_fresh_store: &dyn Fn(&Scenario)) {
    with_fresh_store(&changes_advance_the_revision_once_and_replays_never);
    with_fresh_store(&records_require_their_tenant_principal_and_root);
    with_fresh_store(&tenants_never_reach_each_other);
}
