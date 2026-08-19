//! The canonical Workbench projection: one adapter binding the durable
//! manifest formats to the SDK lifecycle trait.
//!
//! `nokv-agent` owns the durable run- and restore-manifest formats and
//! deliberately depends on nothing but `nokv-types`, so it cannot see the
//! SDK. `nokv-client` defines [`WorkbenchProjection`] and deliberately does
//! not depend on the tool layer, so it cannot see the formats. Something has
//! to join them, and every caller that drives a Workbench commit or restore
//! needs the same join: the CLI, the MCP server, and the Python SDK all
//! publish manifests that must be byte-identical, because a commit written
//! by one is restored by another.
//!
//! Holding one adapter here rather than one per caller is not tidiness. The
//! trait grows as the lifecycle grows, and a per-caller copy makes every such
//! change an opportunity for two callers to disagree about a durable format.

#![forbid(unsafe_code)]

use nokv_client::{
    RestoreManifestProjectionContext, RestoredRunManifestProjectionContext,
    RunManifestProjectionContext, VerifiedWorkbenchRestoreManifest, VerifiedWorkbenchRunManifest,
    WorkbenchProjection,
};
use nokv_types::WorkbenchId;

/// The canonical projection every SDK consumer should use.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CanonicalWorkbenchProjection;

impl WorkbenchProjection for CanonicalWorkbenchProjection {
    type Error = nokv_agent::ProjectionError;

    fn build_run_manifest(
        &self,
        context: RunManifestProjectionContext<'_>,
        committed_at_unix_seconds: u64,
    ) -> Result<Vec<u8>, Self::Error> {
        nokv_agent::build_run_manifest_v1(
            context.workbench_id,
            context.workbench_path,
            context.content_digest_uri,
            context.canonical_manifest,
            context.manifest_digest_uri,
            context.commit_identity,
            committed_at_unix_seconds,
        )
    }

    fn run_manifest_projection_input_digest(
        &self,
        context: RunManifestProjectionContext<'_>,
    ) -> [u8; 32] {
        nokv_agent::run_manifest_projection_input_digest_v1(
            context.workbench_id,
            context.workbench_path,
            context.content_digest_uri,
            context.canonical_manifest,
            context.manifest_digest_uri,
            context.commit_identity,
        )
    }

    fn verify_run_manifest(
        &self,
        bytes: &[u8],
    ) -> Result<VerifiedWorkbenchRunManifest, Self::Error> {
        let verified = nokv_agent::verify_run_manifest_v1(bytes)?;
        Ok(VerifiedWorkbenchRunManifest {
            workbench_id: verified.workbench_id,
            workbench_path: verified.workbench_path,
            content_digest_uri: verified.content_digest_uri,
            manifest_digest_uri: verified.manifest_digest_uri,
            commit_identity: verified.commit_identity,
            canonical_manifest: verified.canonical_manifest,
            canonical_envelope: verified.canonical_envelope,
            envelope_digest_uri: verified.envelope_digest_uri,
        })
    }

    fn build_restore_manifest(
        &self,
        context: RestoreManifestProjectionContext<'_>,
    ) -> Result<Vec<u8>, Self::Error> {
        nokv_agent::build_restore_manifest_v1(
            context.operation_id,
            context.source_workbench_id,
            context.source_path,
            context.destination_workbench_id,
            context.destination_path,
            context.snapshot_id,
        )
    }

    fn verify_restore_manifest(
        &self,
        bytes: &[u8],
    ) -> Result<VerifiedWorkbenchRestoreManifest, Self::Error> {
        let verified = nokv_agent::verify_restore_manifest_v1(bytes)?;
        Ok(VerifiedWorkbenchRestoreManifest {
            operation_id: verified.operation_id,
            source_workbench_id: verified.source_workbench_id,
            source_path: verified.source_path,
            destination_workbench_id: verified.destination_workbench_id,
            destination_path: verified.destination_path,
            snapshot_id: verified.snapshot_id,
            canonical_envelope: verified.canonical_envelope,
            envelope_digest_uri: verified.envelope_digest_uri,
        })
    }

    fn restore_effective_content_digest_uri(
        &self,
        source_content_digest_uri: &str,
        source_matches_base_commit: bool,
        materialized_member_digest: [u8; 32],
    ) -> Result<String, Self::Error> {
        nokv_agent::restore_effective_content_digest_uri_v1(
            source_content_digest_uri,
            source_matches_base_commit,
            materialized_member_digest,
        )
    }

    fn workbench_commit_identity(
        &self,
        workbench_id: &WorkbenchId,
        content_digest_uri: &str,
        manifest_digest_uri: &str,
    ) -> [u8; 32] {
        nokv_agent::workbench_commit_identity(workbench_id, content_digest_uri, manifest_digest_uri)
    }

    fn build_restored_run_manifest(
        &self,
        context: RestoredRunManifestProjectionContext<'_>,
    ) -> Result<Vec<u8>, Self::Error> {
        nokv_agent::build_restored_run_manifest_v1(
            context.source_run_manifest,
            context.destination_workbench_id,
            context.destination_workbench_path,
            context.effective_content_digest_uri,
            context.destination_commit_identity,
            context.destination_committed_at_unix_seconds,
        )
    }
}
