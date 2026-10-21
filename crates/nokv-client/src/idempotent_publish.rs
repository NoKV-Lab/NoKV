/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Publication under a caller-chosen operation identity.
//!
//! A caller names one intended publication with one operation id. A retry with
//! the same id and inputs from any process, including after the owner
//! restarted, returns the original receipt instead of publishing again, and the
//! same id with other inputs is refused.
//!
//! The owner binds a visible publication's identity to its initiating owner
//! epoch, so a retry cannot simply repeat `Begin`. It first reads the durable
//! operation rows of the id's attempts, without a mutation, and publishes only
//! when none succeeded. A durably failed attempt is followed by the next one;
//! the owner re-evaluates the caller's precondition atomically at its
//! admission. Attempt and revision identities derive from the caller's id and
//! the intent, and keep the caller id's time-ordered prefix.

use std::fmt;

use nokv_object::ArtifactObjectStore;
use nokv_protocol::{
    sha256_digest_uri, ArtifactRevisionIdentity, ContentType, Digest, ErrorCode,
    GetOperationRequest, OperationIdentity, OperationKind, OperationResult, OperationState,
    PublishCondition, PublishResult, RootIdentity, WorkspaceIdentity, WorkspacePath,
};
use nokv_types::{ErrorClass, FIXED_ID_BYTES, TIME_ORDERED_PREFIX_BYTES};
use sha2::{Digest as _, Sha256};

use crate::{ArtifactPublishOptions, ClientError, RouteResolver, RpcTransport, WorkspaceClient};

/// Bound on attempts under one caller identity; each failed attempt is a
/// durable row, so reaching it means the intent keeps failing.
pub const MAX_IDEMPOTENT_PUBLISH_ATTEMPTS: u64 = 1_024;

const INTENT_DOMAIN: &[u8] = b"nokv.publish.intent.v1\0";
const ATTEMPT_DOMAIN: &[u8] = b"nokv.publish.attempt.v1\0";
const REVISION_DOMAIN: &[u8] = b"nokv.publish.revision.v1\0";

/// One publication intent under a caller-chosen operation identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotentPublishOptions {
    pub operation_id: OperationIdentity,
    pub target: WorkspacePath,
    /// `CreateOnly` or `ReplaceOnly`; an append has its own logical identity.
    pub condition: PublishCondition,
    pub content_type: ContentType,
    pub expected_workspace_incarnation_id: Option<WorkspaceIdentity>,
    /// An explicit block size; `None` uses the SDK default and does not
    /// enter the intent, so the default can change without breaking replay.
    pub block_size: Option<usize>,
}

impl IdempotentPublishOptions {
    pub fn new(
        operation_id: OperationIdentity,
        target: WorkspacePath,
        condition: PublishCondition,
        content_type: ContentType,
    ) -> Self {
        Self {
            operation_id,
            target,
            condition,
            content_type,
            expected_workspace_incarnation_id: None,
            block_size: None,
        }
    }

    pub fn with_expected_workspace_incarnation(mut self, incarnation: WorkspaceIdentity) -> Self {
        self.expected_workspace_incarnation_id = Some(incarnation);
        self
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = Some(block_size);
        self
    }

    /// The digest of everything that makes this publication what it is.
    pub fn intent_digest(&self, body: &[u8]) -> Digest {
        let mut hasher = Sha256::new();
        hasher.update(INTENT_DOMAIN);
        // Principal: none. An authenticated principal binds here.
        hasher.update([0]);
        hash_bytes(&mut hasher, self.target.workbench.as_str().as_bytes());
        hash_bytes(&mut hasher, self.target.path.as_str().as_bytes());
        match self.condition {
            PublishCondition::CreateOnly => hasher.update([1]),
            PublishCondition::ReplaceOnly {
                expected_generation,
            } => {
                hasher.update([2]);
                hasher.update(expected_generation.to_be_bytes());
            }
            PublishCondition::Append { .. } => hasher.update([3]),
        }
        hash_bytes(&mut hasher, self.content_type.as_str().as_bytes());
        hasher.update(Sha256::digest(body));
        hasher.update((body.len() as u64).to_be_bytes());
        match self.expected_workspace_incarnation_id {
            None => hasher.update([0]),
            Some(incarnation) => {
                hasher.update([1]);
                hasher.update(incarnation.0);
            }
        }
        match self.block_size {
            None => hasher.update([0]),
            Some(block_size) => {
                hasher.update([1]);
                hasher.update((block_size as u64).to_be_bytes());
            }
        }
        Digest(hasher.finalize().into())
    }
}

/// The identities of one attempt of a caller-identified publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishAttemptIdentity {
    pub attempt: u64,
    pub operation_id: OperationIdentity,
    pub artifact_revision_id: ArtifactRevisionIdentity,
}

/// Derive attempt `attempt`'s identities. Attempt 0 publishes under the
/// caller's own id; later attempts derive theirs. Both keep the caller id's
/// time-ordered prefix, as the owner's step identities do.
pub fn publish_attempt_identity(
    root_id: RootIdentity,
    operation_id: OperationIdentity,
    attempt: u64,
    intent: Digest,
) -> PublishAttemptIdentity {
    let derive = |domain: &[u8], intent: Option<Digest>| {
        let mut hasher = Sha256::new();
        hasher.update(domain);
        hasher.update(root_id.0);
        hasher.update(operation_id.0);
        hasher.update(attempt.to_be_bytes());
        if let Some(intent) = intent {
            hasher.update(intent.0);
        }
        let digest: [u8; 32] = hasher.finalize().into();
        let mut identity = operation_id.0;
        identity[TIME_ORDERED_PREFIX_BYTES..]
            .copy_from_slice(&digest[..FIXED_ID_BYTES - TIME_ORDERED_PREFIX_BYTES]);
        identity
    };
    PublishAttemptIdentity {
        attempt,
        operation_id: if attempt == 0 {
            operation_id
        } else {
            OperationIdentity(derive(ATTEMPT_DOMAIN, None))
        },
        artifact_revision_id: ArtifactRevisionIdentity(derive(REVISION_DOMAIN, Some(intent))),
    }
}

/// A publication receipt under a caller identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotentPublishReceipt {
    pub publication: PublishResult,
    pub attempt: u64,
    /// The receipt was recorded earlier, not produced by this call.
    pub replayed: bool,
    pub commit_version: Option<u64>,
}

/// Why a caller-identified publication did not produce a receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdempotentPublishError {
    /// The id already names another publication or another kind of operation.
    IdentityConflict {
        operation_id: OperationIdentity,
        message: String,
    },
    /// An attempt is still running or aborting; observe it again later.
    InProgress {
        operation_id: OperationIdentity,
        attempt: u64,
        state: OperationState,
    },
    /// An attempt is quarantined and needs an operator.
    Quarantined {
        operation_id: OperationIdentity,
        attempt: u64,
    },
    /// The attempt itself failed.
    Client {
        attempt: u64,
        source: Box<ClientError>,
    },
}

impl IdempotentPublishError {
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::IdentityConflict { .. } => ErrorClass::IdentityConflict,
            Self::InProgress { .. } => ErrorClass::InProgress,
            Self::Quarantined { .. } => ErrorClass::OperatorRequired,
            Self::Client { source, .. } => source.class(),
        }
    }

    pub fn attempt(&self) -> Option<u64> {
        match self {
            Self::IdentityConflict { .. } => None,
            Self::InProgress { attempt, .. }
            | Self::Quarantined { attempt, .. }
            | Self::Client { attempt, .. } => Some(*attempt),
        }
    }
}

impl fmt::Display for IdempotentPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityConflict { message, .. } => formatter.write_str(message),
            Self::InProgress { attempt, state, .. } => write!(
                formatter,
                "publication attempt {attempt} under this operation id is {state:?}; observe it again later"
            ),
            Self::Quarantined { attempt, .. } => write!(
                formatter,
                "publication attempt {attempt} under this operation id is quarantined"
            ),
            Self::Client { source, .. } => write!(formatter, "{source}"),
        }
    }
}

impl std::error::Error for IdempotentPublishError {}

/// What the attempt chain of one caller identity says.
enum AttemptChain {
    /// An attempt succeeded with this receipt.
    Succeeded(IdempotentPublishReceipt),
    /// Attempt `attempt` has no row yet and should be published.
    Next(PublishAttemptIdentity),
    /// Attempt `attempt` is running and may be resumed with the same inputs.
    Running(PublishAttemptIdentity),
}

impl<Transport, Resolver> WorkspaceClient<Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    /// The stored receipt of `body`'s publication under the caller's
    /// operation identity, read without a mutation or an object store. `None`
    /// means an attempt still has to publish, which
    /// [`Self::publish_idempotent`] does.
    pub fn replay_idempotent_publish(
        &self,
        options: &IdempotentPublishOptions,
        body: &[u8],
    ) -> Result<Option<IdempotentPublishReceipt>, IdempotentPublishError> {
        require_whole_body_condition(options)?;
        match self.observe_attempt_chain(options, options.intent_digest(body), body)? {
            AttemptChain::Succeeded(receipt) => Ok(Some(receipt)),
            AttemptChain::Next(_) | AttemptChain::Running(_) => Ok(None),
        }
    }

    /// Publish `body` once under the caller's operation identity.
    pub fn publish_idempotent(
        &self,
        store: &dyn ArtifactObjectStore,
        options: &IdempotentPublishOptions,
        body: &[u8],
    ) -> Result<IdempotentPublishReceipt, IdempotentPublishError> {
        require_whole_body_condition(options)?;
        let intent = options.intent_digest(body);
        let attempt = match self.observe_attempt_chain(options, intent, body)? {
            AttemptChain::Succeeded(receipt) => return Ok(receipt),
            AttemptChain::Next(attempt) | AttemptChain::Running(attempt) => attempt,
        };
        let mut publish = ArtifactPublishOptions::new(
            attempt.operation_id,
            attempt.artifact_revision_id,
            options.target.clone(),
            options.condition,
            options.content_type.clone(),
        );
        if let Some(block_size) = options.block_size {
            publish = publish.with_block_size(block_size);
        }
        if let Some(incarnation) = options.expected_workspace_incarnation_id {
            publish = publish.with_expected_workspace_incarnation(incarnation);
        }
        match self.publish_artifact(store, publish, body) {
            Ok(outcome) => {
                let call = outcome.publication;
                if !receipt_records(&call.value, options, attempt, body) {
                    return Err(IdempotentPublishError::Client {
                        attempt: attempt.attempt,
                        source: Box::new(ClientError::ResponseMismatch(
                            "publication receipt does not record this intent".to_owned(),
                        )),
                    });
                }
                Ok(IdempotentPublishReceipt {
                    publication: call.value,
                    attempt: attempt.attempt,
                    replayed: call.replayed,
                    commit_version: call.commit_version,
                })
            }
            // The owner refuses a running attempt whose epoch-bound identity
            // predates its restart; it aborts that attempt on its own.
            Err(source) if source.rpc_code() == Some(ErrorCode::RequestReplayMismatch) => {
                Err(IdempotentPublishError::InProgress {
                    operation_id: attempt.operation_id,
                    attempt: attempt.attempt,
                    state: OperationState::Running,
                })
            }
            Err(source) => Err(IdempotentPublishError::Client {
                attempt: attempt.attempt,
                source: Box::new(source),
            }),
        }
    }

    /// Walk the attempts of one caller identity with reads only.
    fn observe_attempt_chain(
        &self,
        options: &IdempotentPublishOptions,
        intent: Digest,
        body: &[u8],
    ) -> Result<AttemptChain, IdempotentPublishError> {
        let root_id = self.root_id();
        for index in 0..MAX_IDEMPOTENT_PUBLISH_ATTEMPTS {
            let attempt = publish_attempt_identity(root_id, options.operation_id, index, intent);
            let status = match self.get_operation(GetOperationRequest {
                operation_id: attempt.operation_id,
            }) {
                Ok(call) => call.value,
                Err(source) if source.rpc_code() == Some(ErrorCode::NotFound) => {
                    return Ok(AttemptChain::Next(attempt));
                }
                Err(source) => {
                    return Err(IdempotentPublishError::Client {
                        attempt: index,
                        source: Box::new(source),
                    })
                }
            };
            let conflict = |message: &str| IdempotentPublishError::IdentityConflict {
                operation_id: options.operation_id,
                message: message.to_owned(),
            };
            if status.token.operation_id != attempt.operation_id
                || status.kind != OperationKind::ArtifactPublish
            {
                return Err(conflict(
                    "operation id already names another kind of operation",
                ));
            }
            let preparation = status
                .publish_preparation
                .as_deref()
                .ok_or_else(|| conflict("publication attempt has no durable preparation"))?;
            if preparation.append_attempt.is_some()
                || preparation.target != options.target
                || preparation.artifact_revision_id != attempt.artifact_revision_id
            {
                return Err(conflict(
                    "operation id was already used for another publication",
                ));
            }
            match status.state {
                OperationState::Succeeded => {
                    let Some(OperationResult::ArtifactPublish(publication)) = status.result else {
                        return Err(IdempotentPublishError::Client {
                            attempt: index,
                            source: Box::new(ClientError::ResponseMismatch(
                                "succeeded publication has no receipt".to_owned(),
                            )),
                        });
                    };
                    if !receipt_records(&publication, options, attempt, body) {
                        return Err(conflict(
                            "operation id was already used for another publication",
                        ));
                    }
                    return Ok(AttemptChain::Succeeded(IdempotentPublishReceipt {
                        publication,
                        attempt: index,
                        replayed: true,
                        commit_version: None,
                    }));
                }
                OperationState::Running => return Ok(AttemptChain::Running(attempt)),
                OperationState::Aborting => {
                    return Err(IdempotentPublishError::InProgress {
                        operation_id: attempt.operation_id,
                        attempt: index,
                        state: OperationState::Aborting,
                    })
                }
                OperationState::Quarantined => {
                    return Err(IdempotentPublishError::Quarantined {
                        operation_id: attempt.operation_id,
                        attempt: index,
                    })
                }
                // A durably failed attempt admitted nothing; the next attempt
                // re-evaluates the precondition at its own admission.
                OperationState::Failed => continue,
            }
        }
        Err(IdempotentPublishError::Client {
            attempt: MAX_IDEMPOTENT_PUBLISH_ATTEMPTS,
            source: Box::new(ClientError::InvalidOptions(
                "this operation id exhausted its publication attempts; use a new one".to_owned(),
            )),
        })
    }
}

fn require_whole_body_condition(
    options: &IdempotentPublishOptions,
) -> Result<(), IdempotentPublishError> {
    match options.condition {
        PublishCondition::CreateOnly | PublishCondition::ReplaceOnly { .. } => Ok(()),
        PublishCondition::Append { .. } => Err(IdempotentPublishError::Client {
            attempt: 0,
            source: Box::new(ClientError::InvalidOptions(
                "a caller-identified publication is create-only or replace-only".to_owned(),
            )),
        }),
    }
}

/// Whether a receipt records exactly this attempt of this intent.
fn receipt_records(
    publication: &PublishResult,
    options: &IdempotentPublishOptions,
    attempt: PublishAttemptIdentity,
    body: &[u8],
) -> bool {
    let generation_follows_claim = match options.condition {
        PublishCondition::CreateOnly => publication.generation == 1,
        PublishCondition::ReplaceOnly {
            expected_generation,
        } => expected_generation.checked_add(1) == Some(publication.generation),
        PublishCondition::Append { .. } => false,
    };
    publication.operation_id == attempt.operation_id
        && publication.target == options.target
        && publication.artifact_revision_id == attempt.artifact_revision_id
        && publication.logical_size == body.len() as u64
        && publication.body_digest == sha256_digest_uri(Digest(Sha256::digest(body).into()))
        && generation_follows_claim
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use nokv_protocol::{RelativePath, WorkbenchName};

    use super::*;

    fn options() -> IdempotentPublishOptions {
        let mut operation_id = [0x11; FIXED_ID_BYTES];
        operation_id[..TIME_ORDERED_PREFIX_BYTES].copy_from_slice(&[0, 0, 1, 0, 0, 7]);
        IdempotentPublishOptions::new(
            OperationIdentity(operation_id),
            WorkspacePath {
                workbench: WorkbenchName::new("run-42").unwrap(),
                path: RelativePath::new("outputs/result.json").unwrap(),
            },
            PublishCondition::CreateOnly,
            ContentType::new("application/json").unwrap(),
        )
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn the_attempt_derivation_is_frozen() {
        // A retry from a later CLI or SDK release must derive the same
        // identities, so this encoding changes only under new domains.
        let options = options();
        let intent = options.intent_digest(b"{}");
        let root = RootIdentity([0x22; FIXED_ID_BYTES]);
        let first = publish_attempt_identity(root, options.operation_id, 0, intent);
        let second = publish_attempt_identity(root, options.operation_id, 1, intent);
        assert_eq!(first.operation_id, options.operation_id);
        assert_eq!(
            hex(&intent.0),
            "7231f559a3d51c6df2b622e0ad79a1b3621a1ac29983b47327cdba725f146460"
        );
        assert_eq!(
            hex(&first.artifact_revision_id.0),
            "000001000007257b714d7af751272fd0"
        );
        assert_eq!(
            hex(&second.operation_id.0),
            "000001000007c86db95d422faa4f9189"
        );
        for identity in [
            first.artifact_revision_id.0,
            second.operation_id.0,
            second.artifact_revision_id.0,
        ] {
            assert_eq!(
                identity[..TIME_ORDERED_PREFIX_BYTES],
                options.operation_id.0[..TIME_ORDERED_PREFIX_BYTES]
            );
        }
        assert_ne!(first.artifact_revision_id, second.artifact_revision_id);
    }

    #[test]
    fn every_input_of_the_intent_changes_it_except_the_default_block_size() {
        let base = options();
        let original = base.intent_digest(b"{}");
        let mut variants = vec![base.intent_digest(b"{ }")];
        let mut changed = base.clone();
        changed.target.path = RelativePath::new("outputs/other.json").unwrap();
        variants.push(changed.intent_digest(b"{}"));
        let mut changed = base.clone();
        changed.condition = PublishCondition::ReplaceOnly {
            expected_generation: 1,
        };
        variants.push(changed.intent_digest(b"{}"));
        let mut changed = base.clone();
        changed.content_type = ContentType::new("text/plain").unwrap();
        variants.push(changed.intent_digest(b"{}"));
        variants.push(
            base.clone()
                .with_expected_workspace_incarnation(WorkspaceIdentity([3; FIXED_ID_BYTES]))
                .intent_digest(b"{}"),
        );
        variants.push(base.clone().with_block_size(4096).intent_digest(b"{}"));
        for variant in variants {
            assert_ne!(variant, original);
        }
        // The operation id names the intent; it is not part of it.
        let mut renamed = base.clone();
        renamed.operation_id = OperationIdentity([9; FIXED_ID_BYTES]);
        assert_eq!(renamed.intent_digest(b"{}"), original);
    }

    #[test]
    fn a_receipt_records_only_its_own_attempt_target_body_and_claim() {
        let options = options();
        let body = b"{}";
        let attempt = publish_attempt_identity(
            RootIdentity([0x22; FIXED_ID_BYTES]),
            options.operation_id,
            0,
            options.intent_digest(body),
        );
        let receipt = PublishResult {
            operation_id: attempt.operation_id,
            target: options.target.clone(),
            workspace_revision: 4,
            generation: 1,
            artifact_revision_id: attempt.artifact_revision_id,
            logical_size: body.len() as u64,
            body_digest: sha256_digest_uri(Digest(Sha256::digest(body).into())),
        };
        assert!(receipt_records(&receipt, &options, attempt, body));
        assert!(!receipt_records(&receipt, &options, attempt, b"{ }"));
        let mut other = receipt.clone();
        other.generation = 2;
        assert!(!receipt_records(&other, &options, attempt, body));
        let mut other = receipt.clone();
        other.artifact_revision_id = ArtifactRevisionIdentity([5; FIXED_ID_BYTES]);
        assert!(!receipt_records(&other, &options, attempt, body));
        let mut replace = options.clone();
        replace.condition = PublishCondition::ReplaceOnly {
            expected_generation: 3,
        };
        let mut replaced = receipt;
        replaced.generation = 4;
        assert!(receipt_records(&replaced, &replace, attempt, body));
    }
}
