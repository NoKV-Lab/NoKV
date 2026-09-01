/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Session-fenced lifecycle inspection and the one retirement precondition.

use std::sync::Arc;

use nokv_meta::workspace as meta;
use nokv_protocol::RootRoute;
use nokv_types::{
    ArtifactRevisionId, CommandDigest, CommitId, CommitState, CommitVersion, ConsumerEpoch,
    GcClaimState, GcPhase, OperationKind, PlacementGeneration, ReferenceEpoch, RequestId,
    RevisionState, SnapshotState, WorkspaceIncarnationId, FIXED_ID_BYTES, SHA256_BYTES,
};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct OperationEvidence {
    pub(crate) kind: String,
    pub(crate) operation_id: String,
    pub(crate) phase: String,
    pub(crate) quarantine_evidence_bytes: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CommitEvidence {
    pub(crate) commit_id: String,
    pub(crate) state: String,
    pub(crate) consumer_count: u64,
    pub(crate) consumer_epoch: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SnapshotEvidence {
    pub(crate) snapshot_id: u64,
    pub(crate) state: String,
    pub(crate) consumer_count: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RevisionEvidence {
    pub(crate) revision_id: String,
    pub(crate) state: String,
    pub(crate) strong_reference_count: u64,
    pub(crate) reference_epoch: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GcCandidateEvidence {
    pub(crate) revision_id: String,
    pub(crate) reference_epoch: u64,
    pub(crate) claim_state: String,
    pub(crate) quarantine_evidence_bytes: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct LifecycleSnapshot {
    pub(crate) read_version: u64,
    pub(crate) operations: Vec<OperationEvidence>,
    pub(crate) commits: Vec<CommitEvidence>,
    pub(crate) snapshots: Vec<SnapshotEvidence>,
    pub(crate) revisions: Vec<RevisionEvidence>,
    pub(crate) gc_candidates: Vec<GcCandidateEvidence>,
}

pub(crate) struct LifecycleInspector {
    meta: Arc<meta::MetaShard>,
    route: RootRoute,
}

impl LifecycleInspector {
    pub(crate) fn new(meta: Arc<meta::MetaShard>, route: RootRoute) -> Self {
        Self { meta, route }
    }

    pub(crate) fn capture(&self) -> Result<LifecycleSnapshot, String> {
        let read = self.read_context()?;
        let mut operations = Vec::new();
        for kind in [
            OperationKind::Publish,
            OperationKind::Restore,
            OperationKind::CommitRetire,
            OperationKind::Gc,
        ] {
            for row in self.scan_all(
                read,
                meta::MetadataFamily::Operation,
                &meta::operation_prefix(read.root_id, kind),
            )? {
                let operation_id = meta::decode_operation_key(read.root_id, kind, &row.key)
                    .ok_or_else(|| format!("malformed {kind:?} operation key"))?;
                let (phase, quarantine_evidence_bytes) = match kind {
                    OperationKind::Publish => {
                        let record = meta::PublishOperationRecord::decode(&row.value)
                            .map_err(|error| error.to_string())?;
                        (format!("{:?}", record.phase), None)
                    }
                    OperationKind::Restore => {
                        let record = meta::RestoreOperationRecord::decode(&row.value)
                            .map_err(|error| error.to_string())?;
                        (format!("{:?}", record.phase), None)
                    }
                    OperationKind::CommitRetire => {
                        let record = meta::CommitRetireOperationRecord::decode(&row.value)
                            .map_err(|error| error.to_string())?;
                        (
                            format!("{:?}", record.phase),
                            record
                                .terminal_error
                                .as_ref()
                                .map(|error| error.message.len()),
                        )
                    }
                    OperationKind::Gc => {
                        let record = meta::GcOperationRecord::decode(&row.value)
                            .map_err(|error| error.to_string())?;
                        (
                            format!("{:?}", record.phase),
                            record.quarantine_evidence.as_ref().map(Vec::len),
                        )
                    }
                    _ => unreachable!("only lifecycle operation kinds are inspected"),
                };
                operations.push(OperationEvidence {
                    kind: format!("{kind:?}"),
                    operation_id: hex(operation_id.as_bytes()),
                    phase,
                    quarantine_evidence_bytes,
                });
            }
        }

        let commits = self
            .scan_all(
                read,
                meta::MetadataFamily::Commit,
                &meta::commit_prefix(read.root_id),
            )?
            .into_iter()
            .map(|row| {
                let commit_id = meta::decode_commit_key(read.root_id, &row.key)
                    .ok_or_else(|| "malformed commit key".to_owned())?;
                let record =
                    meta::CommitRecord::decode(&row.value).map_err(|error| error.to_string())?;
                Ok(CommitEvidence {
                    commit_id: hex(commit_id.as_bytes()),
                    state: format!("{:?}", record.state),
                    consumer_count: record.consumer_count,
                    consumer_epoch: record.consumer_epoch.get(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let snapshots = self
            .scan_all(
                read,
                meta::MetadataFamily::SnapshotRef,
                read.root_id.as_bytes(),
            )?
            .into_iter()
            .filter_map(|row| {
                meta::decode_snapshot_ref_key(read.root_id, &row.key)
                    .map(|(_workspace, snapshot)| (snapshot, row.value))
            })
            .map(|(snapshot, value)| {
                let record =
                    meta::SnapshotRefRecord::decode(&value).map_err(|error| error.to_string())?;
                Ok(SnapshotEvidence {
                    snapshot_id: snapshot.get(),
                    state: format!("{:?}", record.state),
                    consumer_count: record.consumer_count,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let revisions = self
            .scan_all(
                read,
                meta::MetadataFamily::ArtifactRevision,
                read.root_id.as_bytes(),
            )?
            .into_iter()
            .map(|row| {
                let revision_bytes: [u8; FIXED_ID_BYTES] = row
                    .key
                    .strip_prefix(read.root_id.as_bytes())
                    .and_then(|suffix| suffix.try_into().ok())
                    .ok_or_else(|| "malformed artifact-revision key".to_owned())?;
                let record = meta::ArtifactRevisionRecord::decode(&row.value)
                    .map_err(|error| error.to_string())?;
                Ok(RevisionEvidence {
                    revision_id: hex(&revision_bytes),
                    state: format!("{:?}", record.state),
                    strong_reference_count: record.strong_reference_count,
                    reference_epoch: record.reference_epoch.get(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let gc_candidates = self
            .scan_all(
                read,
                meta::MetadataFamily::GcCandidate,
                &meta::gc_candidate_prefix(read.root_id),
            )?
            .into_iter()
            .map(|row| {
                let (revision, epoch) = meta::decode_gc_candidate_key(read.root_id, &row.key)
                    .ok_or_else(|| "malformed GC-candidate key".to_owned())?;
                let record = meta::GcCandidateRecord::decode(&row.value)
                    .map_err(|error| error.to_string())?;
                Ok(GcCandidateEvidence {
                    revision_id: hex(revision.as_bytes()),
                    reference_epoch: epoch.get(),
                    claim_state: format!("{:?}", record.claim_state),
                    quarantine_evidence_bytes: record.quarantine_evidence.as_ref().map(Vec::len),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        Ok(LifecycleSnapshot {
            read_version: read.read_version.get(),
            operations,
            commits,
            snapshots,
            revisions,
            gc_candidates,
        })
    }

    pub(crate) fn seed_zero_consumer_commit(
        &self,
        commit_id: CommitId,
        revision_id: ArtifactRevisionId,
        workspace: WorkspaceIncarnationId,
        request_id: RequestId,
    ) -> Result<(), String> {
        let read = self.read_context()?;
        let zero_version = read
            .read_version
            .get()
            .checked_add(1)
            .ok_or_else(|| "retirement seed commit version overflows".to_owned())?;
        let zero_version = CommitVersion::new(zero_version).map_err(|error| error.to_string())?;
        let reference_epoch = ReferenceEpoch::new(1);
        let reference = meta::RevisionRefRecord {
            reference_epoch_at_add: reference_epoch,
        }
        .encode()
        .map_err(|error| error.to_string())?;
        let revision_digest = meta::advance_commit_revision_rolling_digest(
            [0; SHA256_BYTES],
            0,
            revision_id,
            &reference,
        );
        let revision =
            meta::ArtifactRevisionRecord {
                logical_size: 0,
                body_digest_uri:
                    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        .to_owned(),
                manifest_digest_uri:
                    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        .to_owned(),
                block_count: 0,
                dependency_count: 0,
                dependency_depth: 0,
                dependency_digest: meta::dependency_owner_digest(&[])
                    .map_err(|error| error.to_string())?,
                content_type: "application/octet-stream".to_owned(),
                state: RevisionState::Available,
                reference_epoch,
                strong_reference_count: 1,
                last_zero_ref_version: None,
            }
            .encode()
            .map_err(|error| error.to_string())?;
        let commit = meta::CommitRecord {
            source_workspace_incarnation_id: workspace,
            content_digest_uri: "sha256:gate8-retirement-content".to_owned(),
            manifest_digest_uri: "sha256:gate8-retirement-manifest".to_owned(),
            tree_manifest_revision_id: revision_id,
            tree_digest_uri: "sha256:gate8-retirement-tree".to_owned(),
            member_count: 0,
            member_digest: [0; SHA256_BYTES],
            unique_revision_count: 1,
            revision_digest,
            parent_commits: Vec::new(),
            parent_digest: [0; SHA256_BYTES],
            generic_index_count: 0,
            generic_index_digest: [0; SHA256_BYTES],
            producer: Some("nokv-fdb-lifecycle-qualification".to_owned()),
            lineage_projection: Vec::new(),
            consumer_count: 0,
            consumer_epoch: ConsumerEpoch::new(1),
            last_zero_consumer_version: Some(zero_version),
            state: CommitState::Sealed,
        }
        .encode()
        .map_err(|error| error.to_string())?;
        let mutations = vec![
            meta::CommandMutation::Put {
                family: meta::MetadataFamily::ArtifactRevision,
                key: meta::artifact_revision_key(read.root_id, revision_id),
                value: revision,
            },
            meta::CommandMutation::Put {
                family: meta::MetadataFamily::RevisionRef,
                key: meta::commit_revision_ref_key(read.root_id, commit_id, revision_id),
                value: reference,
            },
            meta::CommandMutation::Put {
                family: meta::MetadataFamily::Commit,
                key: meta::commit_key(read.root_id, commit_id),
                value: commit,
            },
        ];
        let predicates = mutations
            .iter()
            .map(|mutation| match mutation {
                meta::CommandMutation::Put { family, key, .. } => meta::CommandPredicate::Value {
                    family: *family,
                    key: key.clone(),
                    expected: None,
                },
                meta::CommandMutation::Delete { .. } => {
                    unreachable!("retirement seed has no deletes")
                }
            })
            .collect();
        let command = meta::MetadataCommand {
            schema_id: meta::SCHEMA_ID.to_owned(),
            root_id: read.root_id,
            logical_shard_id: self.route.logical_shard_id.into(),
            object_namespace_id: Some(self.route.object_namespace_id.into()),
            placement_generation: read.placement_generation,
            owner_epoch: read.owner_epoch,
            request_id,
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: read.read_version,
            root_fence_action: meta::RootFenceAction::RequireActive,
            predicates,
            mutations,
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: b"gate8-zero-consumer-commit".to_vec(),
        }
        .seal();
        self.meta
            .execute(&command)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn read_context(&self) -> Result<meta::RootReadContext, String> {
        let placement = PlacementGeneration::new(self.route.placement_generation)
            .map_err(|error| error.to_string())?;
        let owner = nokv_types::OwnerEpoch::new(self.route.owner_epoch)
            .map_err(|error| error.to_string())?;
        meta::RootReadContext::current(&self.meta, self.route.root_id.into(), placement, owner)
            .map_err(|error| error.to_string())
    }

    fn scan_all(
        &self,
        context: meta::RootReadContext,
        family: meta::MetadataFamily,
        prefix: &[u8],
    ) -> Result<Vec<meta::MetadataScanItem>, String> {
        const PAGE_SIZE: usize = 512;
        let mut rows = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .meta
                .scan_prefix_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    family,
                    prefix,
                    context.read_version,
                    after.as_deref(),
                    PAGE_SIZE,
                )
                .map_err(|error| error.to_string())?;
            let complete = page.len() < PAGE_SIZE;
            after = page.last().map(|row| row.key.clone());
            rows.extend(page);
            if complete {
                return Ok(rows);
            }
        }
    }
}

pub(crate) fn has_operation_phase(snapshot: &LifecycleSnapshot, kind: &str, phase: &str) -> bool {
    snapshot
        .operations
        .iter()
        .any(|operation| operation.kind == kind && operation.phase == phase)
}

pub(crate) fn has_commit_state(
    snapshot: &LifecycleSnapshot,
    commit_id: CommitId,
    state: CommitState,
) -> bool {
    let commit_id = hex(commit_id.as_bytes());
    let state = format!("{state:?}");
    snapshot
        .commits
        .iter()
        .any(|commit| commit.commit_id == commit_id && commit.state == state)
}

pub(crate) fn quarantined_candidate_count(snapshot: &LifecycleSnapshot) -> usize {
    snapshot
        .gc_candidates
        .iter()
        .filter(|candidate| {
            candidate.claim_state == format!("{:?}", GcClaimState::Quarantined)
                && candidate
                    .quarantine_evidence_bytes
                    .is_some_and(|bytes| bytes > 0)
        })
        .count()
}

pub(crate) fn completed_gc_count(snapshot: &LifecycleSnapshot) -> usize {
    snapshot
        .operations
        .iter()
        .filter(|operation| {
            operation.kind == format!("{:?}", OperationKind::Gc)
                && operation.phase == format!("{:?}", GcPhase::Deleted)
        })
        .count()
}

pub(crate) fn retired_snapshot_count(snapshot: &LifecycleSnapshot) -> usize {
    snapshot
        .snapshots
        .iter()
        .filter(|snapshot| snapshot.state == format!("{:?}", SnapshotState::Retired))
        .count()
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}
