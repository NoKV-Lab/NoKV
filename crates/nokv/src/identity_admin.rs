/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! `nokv identity`: administer tenant identity records in the control plane.
//!
//! These commands write the control store directly, so only an operator that
//! holds control-plane credentials can run them. The records authorize
//! nothing until the server enforces them.

use std::fs::File;
use std::io::Read as _;
use std::path::Path;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use nokv_control::{
    ControlError, CredentialBinding, IdentityStore, PrincipalRecord, RoleBinding,
    RootTenantBinding, TenantPolicy, TenantRecord,
};
use nokv_types::{CredentialFingerprint, IdempotencyScope, RoleScope};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

use crate::cli::{CredentialSource, IdentityCommand};
use crate::encode_lowercase_hex;
use crate::failure_envelope::CliFailure;

/// Largest certificate file `--certificate` reads.
const MAX_CERTIFICATE_FILE_BYTES: u64 = 1024 * 1024;
const PEM_CERTIFICATE_BEGIN: &str = "-----BEGIN CERTIFICATE-----";
const PEM_CERTIFICATE_END: &str = "-----END CERTIFICATE-----";

/// Run one identity command; `now_unix_seconds` rejects a credential that
/// would already be expired.
pub(crate) fn run(
    store: &dyn IdentityStore,
    command: &IdentityCommand,
    now_unix_seconds: u64,
) -> Result<Value, CliFailure> {
    let changed = |error| identity_failure(error, IdempotencyScope::Convergent);
    match command {
        IdentityCommand::CreateTenant { tenant_id } => {
            store
                .create_tenant(TenantRecord {
                    tenant_id: *tenant_id,
                })
                .map_err(changed)?;
            Ok(json!({
                "status": "success",
                "operation": "tenant_create",
                "tenant_id": encode_lowercase_hex(tenant_id.as_bytes()),
            }))
        }
        IdentityCommand::CreatePrincipal {
            tenant_id,
            principal_id,
        } => {
            store
                .create_principal(PrincipalRecord {
                    tenant_id: *tenant_id,
                    principal_id: *principal_id,
                })
                .map_err(changed)?;
            Ok(json!({
                "status": "success",
                "operation": "principal_create",
                "tenant_id": encode_lowercase_hex(tenant_id.as_bytes()),
                "principal_id": encode_lowercase_hex(principal_id.as_bytes()),
            }))
        }
        IdentityCommand::AddCredential {
            tenant_id,
            principal_id,
            credential,
            expires_at_unix_seconds,
        } => {
            if *expires_at_unix_seconds <= now_unix_seconds {
                return Err(CliFailure::invalid_input(
                    "--expires-at-unix must name a future Unix time",
                ));
            }
            let fingerprint = match credential {
                CredentialSource::Fingerprint(fingerprint) => *fingerprint,
                CredentialSource::Certificate(path) => certificate_fingerprint(path)?,
            };
            let binding = store
                .add_credential(CredentialBinding {
                    tenant_id: *tenant_id,
                    principal_id: *principal_id,
                    fingerprint,
                    expires_at_unix_seconds: *expires_at_unix_seconds,
                })
                .map_err(changed)?;
            let mut result = credential_json(&binding);
            result["status"] = json!("success");
            result["operation"] = json!("credential_add");
            result["tenant_id"] = json!(encode_lowercase_hex(tenant_id.as_bytes()));
            Ok(result)
        }
        IdentityCommand::RevokeCredential {
            tenant_id,
            fingerprint,
        } => {
            let revoked = store
                .revoke_credential(tenant_id, fingerprint)
                .map_err(changed)?;
            Ok(json!({
                "status": "success",
                "operation": "credential_revoke",
                "tenant_id": encode_lowercase_hex(tenant_id.as_bytes()),
                "fingerprint": encode_lowercase_hex(fingerprint.as_bytes()),
                "revoked": revoked,
            }))
        }
        IdentityCommand::GrantRole(binding) => {
            store.grant_role(*binding).map_err(changed)?;
            let mut result = role_json(binding);
            result["status"] = json!("success");
            result["operation"] = json!("role_grant");
            result["tenant_id"] = json!(encode_lowercase_hex(binding.tenant_id.as_bytes()));
            Ok(result)
        }
        IdentityCommand::RevokeRole(binding) => {
            let revoked = store.revoke_role(binding).map_err(changed)?;
            let mut result = role_json(binding);
            result["status"] = json!("success");
            result["operation"] = json!("role_revoke");
            result["tenant_id"] = json!(encode_lowercase_hex(binding.tenant_id.as_bytes()));
            result["revoked"] = json!(revoked);
            Ok(result)
        }
        IdentityCommand::BindRoot { root_id, tenant_id } => {
            store
                .bind_root_tenant(RootTenantBinding {
                    root_id: *root_id,
                    tenant_id: *tenant_id,
                })
                .map_err(changed)?;
            Ok(json!({
                "status": "success",
                "operation": "root_bind",
                "root_id": encode_lowercase_hex(root_id.as_bytes()),
                "tenant_id": encode_lowercase_hex(tenant_id.as_bytes()),
            }))
        }
        IdentityCommand::Show { tenant_id } => {
            let policy = store
                .load_tenant_policy(tenant_id)
                .map_err(|error| identity_failure(error, IdempotencyScope::ReadOnly))?
                .ok_or_else(|| {
                    identity_failure(
                        ControlError::TenantNotFound(*tenant_id),
                        IdempotencyScope::ReadOnly,
                    )
                })?;
            Ok(policy_json(&policy))
        }
    }
}

fn policy_json(policy: &TenantPolicy) -> Value {
    json!({
        "status": "success",
        "operation": "tenant_show",
        "tenant_id": encode_lowercase_hex(policy.tenant_id.as_bytes()),
        "revision": policy.revision,
        "principals": policy
            .principals
            .iter()
            .map(|principal| encode_lowercase_hex(principal.principal_id.as_bytes()))
            .collect::<Vec<_>>(),
        "credentials": policy.credentials.iter().map(credential_json).collect::<Vec<_>>(),
        "role_bindings": policy.role_bindings.iter().map(role_json).collect::<Vec<_>>(),
    })
}

fn credential_json(binding: &CredentialBinding) -> Value {
    json!({
        "principal_id": encode_lowercase_hex(binding.principal_id.as_bytes()),
        "fingerprint": encode_lowercase_hex(binding.fingerprint.as_bytes()),
        "expires_at_unix_seconds": binding.expires_at_unix_seconds,
    })
}

fn role_json(binding: &RoleBinding) -> Value {
    json!({
        "principal_id": encode_lowercase_hex(binding.principal_id.as_bytes()),
        "role": binding.role.name(),
        "scope": match binding.scope {
            RoleScope::Tenant => json!("tenant"),
            RoleScope::Root(root_id) => json!({"root_id": encode_lowercase_hex(root_id.as_bytes())}),
        },
    })
}

/// SHA-256 of the DER bytes of the first certificate in a PEM file: the leaf
/// a TLS client presents first.
fn certificate_fingerprint(path: &Path) -> Result<CredentialFingerprint, CliFailure> {
    let unreadable = |reason: &str| {
        CliFailure::invalid_input(format!("certificate {} {reason}", path.display()))
    };
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| {
            file.take(MAX_CERTIFICATE_FILE_BYTES + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(|error| unreadable(&format!("cannot be read: {error}")))?;
    if bytes.len() as u64 > MAX_CERTIFICATE_FILE_BYTES {
        return Err(unreadable("is larger than 1 MiB"));
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| unreadable("is not PEM text"))?;
    let start = text
        .find(PEM_CERTIFICATE_BEGIN)
        .ok_or_else(|| unreadable("contains no PEM certificate"))?
        + PEM_CERTIFICATE_BEGIN.len();
    let length = text[start..]
        .find(PEM_CERTIFICATE_END)
        .ok_or_else(|| unreadable("has an unterminated PEM certificate"))?;
    let body: String = text[start..start + length]
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    let der = STANDARD
        .decode(body)
        .map_err(|_| unreadable("has an invalid PEM certificate body"))?;
    // A DER certificate is one ASN.1 SEQUENCE.
    if der.first() != Some(&0x30) {
        return Err(unreadable("does not hold a DER certificate"));
    }
    Ok(CredentialFingerprint::from_bytes(
        Sha256::digest(&der).into(),
    ))
}

/// A failed identity command, with the record identities in `details`.
fn identity_failure(error: ControlError, scope: IdempotencyScope) -> CliFailure {
    let (code, details) = match &error {
        ControlError::TenantNotFound(tenant_id) => (
            "TenantNotFound",
            json!({"tenant_id": encode_lowercase_hex(tenant_id.as_bytes())}),
        ),
        ControlError::PrincipalNotFound {
            tenant_id,
            principal_id,
        } => (
            "PrincipalNotFound",
            json!({
                "tenant_id": encode_lowercase_hex(tenant_id.as_bytes()),
                "principal_id": encode_lowercase_hex(principal_id.as_bytes()),
            }),
        ),
        ControlError::CredentialAlreadyBound {
            tenant_id,
            fingerprint,
        } => (
            "CredentialAlreadyBound",
            json!({
                "tenant_id": encode_lowercase_hex(tenant_id.as_bytes()),
                "fingerprint": encode_lowercase_hex(fingerprint.as_bytes()),
            }),
        ),
        ControlError::RootTenantAlreadyBound { root_id } => (
            "RootTenantAlreadyBound",
            json!({"root_id": encode_lowercase_hex(root_id.as_bytes())}),
        ),
        ControlError::RootNotInTenant { root_id, tenant_id } => (
            "RootNotInTenant",
            json!({
                "root_id": encode_lowercase_hex(root_id.as_bytes()),
                "tenant_id": encode_lowercase_hex(tenant_id.as_bytes()),
            }),
        ),
        ControlError::TenantPolicyLimitExceeded {
            tenant_id,
            record,
            limit,
        } => (
            "TenantPolicyLimitExceeded",
            json!({
                "tenant_id": encode_lowercase_hex(tenant_id.as_bytes()),
                "record": record,
                "limit": limit,
            }),
        ),
        _ => ("ControlFailure", json!({})),
    };
    let mut failure = CliFailure::new(code, error.to_string(), error.class(), scope);
    *failure.details_mut() = details;
    failure
}

#[cfg(test)]
mod tests {
    use nokv_control::InMemoryControlStore;
    use nokv_types::{PrincipalId, Role, RootId, TenantId};

    use super::*;

    const NOW: u64 = 1_800_000_000;

    fn tenant() -> TenantId {
        TenantId::from_bytes([5; 16])
    }

    fn principal() -> PrincipalId {
        PrincipalId::from_bytes([6; 16])
    }

    fn failure_line(store: &dyn IdentityStore, command: IdentityCommand) -> Value {
        serde_json::from_str(&run(store, &command, NOW).unwrap_err().to_line()).unwrap()
    }

    #[test]
    fn identity_commands_record_a_tenant_principal_credential_and_roles() {
        let store = InMemoryControlStore::new();
        for command in [
            IdentityCommand::CreateTenant {
                tenant_id: tenant(),
            },
            IdentityCommand::CreatePrincipal {
                tenant_id: tenant(),
                principal_id: principal(),
            },
            IdentityCommand::BindRoot {
                root_id: RootId::from_bytes([1; 16]),
                tenant_id: tenant(),
            },
            IdentityCommand::AddCredential {
                tenant_id: tenant(),
                principal_id: principal(),
                credential: CredentialSource::Fingerprint(CredentialFingerprint::from_bytes(
                    [7; 32],
                )),
                expires_at_unix_seconds: NOW + 60,
            },
            IdentityCommand::GrantRole(RoleBinding {
                tenant_id: tenant(),
                principal_id: principal(),
                role: Role::Writer,
                scope: RoleScope::Root(RootId::from_bytes([1; 16])),
            }),
        ] {
            assert_eq!(run(&store, &command, NOW).unwrap()["status"], "success");
        }
        let shown = run(
            &store,
            &IdentityCommand::Show {
                tenant_id: tenant(),
            },
            NOW,
        )
        .unwrap();
        assert_eq!(shown["revision"], 4);
        assert_eq!(shown["principals"], json!(["06".repeat(16)]));
        assert_eq!(shown["credentials"][0]["fingerprint"], "07".repeat(32));
        assert_eq!(
            shown["role_bindings"],
            json!([{
                "principal_id": "06".repeat(16),
                "role": "writer",
                "scope": {"root_id": "01".repeat(16)},
            }])
        );
    }

    #[test]
    fn identity_failures_name_their_class_and_records() {
        let store = InMemoryControlStore::new();
        let missing = failure_line(
            &store,
            IdentityCommand::CreatePrincipal {
                tenant_id: tenant(),
                principal_id: principal(),
            },
        );
        assert_eq!(missing["code"], "TenantNotFound");
        assert_eq!(missing["class"], "not_found");
        assert_eq!(missing["details"]["tenant_id"], "05".repeat(16));

        run(
            &store,
            &IdentityCommand::CreateTenant {
                tenant_id: tenant(),
            },
            NOW,
        )
        .unwrap();
        let unregistered = failure_line(
            &store,
            IdentityCommand::GrantRole(RoleBinding {
                tenant_id: tenant(),
                principal_id: principal(),
                role: Role::Reader,
                scope: RoleScope::Tenant,
            }),
        );
        assert_eq!(unregistered["code"], "PrincipalNotFound");
        assert_eq!(unregistered["next_action"], "reread_then_retry");

        let expired = failure_line(
            &store,
            IdentityCommand::AddCredential {
                tenant_id: tenant(),
                principal_id: principal(),
                credential: CredentialSource::Fingerprint(CredentialFingerprint::from_bytes(
                    [7; 32],
                )),
                expires_at_unix_seconds: NOW,
            },
        );
        assert_eq!(expired["class"], "invalid_input");
        assert_eq!(expired["next_action"], "fix_input");
    }

    #[test]
    fn a_certificate_fingerprint_is_the_sha256_of_the_first_der_certificate() {
        let directory = tempfile::tempdir().unwrap();
        let der = [0x30_u8, 0x03, 0x02, 0x01, 0x05];
        let second = [0x30_u8, 0x00];
        let pem = format!(
            "{PEM_CERTIFICATE_BEGIN}\n{}\n{PEM_CERTIFICATE_END}\n{PEM_CERTIFICATE_BEGIN}\n{}\n{PEM_CERTIFICATE_END}\n",
            STANDARD.encode(der),
            STANDARD.encode(second),
        );
        let path = directory.path().join("agent.pem");
        std::fs::write(&path, pem).unwrap();
        assert_eq!(
            certificate_fingerprint(&path).unwrap(),
            CredentialFingerprint::from_bytes(Sha256::digest(der).into())
        );

        for (name, contents) in [
            ("empty.pem", "no certificate here".to_owned()),
            (
                "not-der.pem",
                format!("{PEM_CERTIFICATE_BEGIN}\nAQID\n{PEM_CERTIFICATE_END}\n"),
            ),
            ("open.pem", format!("{PEM_CERTIFICATE_BEGIN}\nMAA=\n")),
        ] {
            let path = directory.path().join(name);
            std::fs::write(&path, contents).unwrap();
            assert_eq!(
                certificate_fingerprint(&path).unwrap_err().class(),
                nokv_types::ErrorClass::InvalidInput,
                "{name}"
            );
        }
    }
}
