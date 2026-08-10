/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_meta::workspace::{HoltReadStats, MetadataReadStats};
use serde::Serialize;

use crate::report::{measure_with_diagnostics, WorkloadReport};

use super::fixture::Harness;

#[derive(Clone, Debug, Serialize)]
pub struct MetadataBenchmarkReport {
    pub schema: &'static str,
    pub evidence_level: &'static str,
    pub protocol_schema: &'static str,
    pub profile: MetadataProfile,
    pub setup_seconds: f64,
    pub workloads: Vec<MetadataWorkloadReport>,
    pub correctness: CorrectnessEvidence,
    pub amplification: AmplificationQualification,
    pub qualification: MetadataQualification,
}

#[derive(Clone, Debug, Serialize)]
pub struct MetadataProfile {
    pub revision: String,
    pub harness_revision: String,
    pub dirty_worktree: bool,
    pub rust_toolchain: String,
    pub machine: String,
    pub operating_system: &'static str,
    pub architecture: &'static str,
    pub metadata_device: String,
    pub metadata_store: String,
    pub metadata_engine: &'static str,
    pub object_provider: &'static str,
    pub object_endpoint_class: &'static str,
    pub durability_profile: &'static str,
    pub logical_shards: u32,
    pub physical_owners: u32,
    pub roots: u32,
    pub workspace_count: u32,
    pub paths_per_workspace: usize,
    pub payload_distribution: &'static str,
    pub setup_method: &'static str,
    pub concurrency: u32,
    pub iterations: u64,
    pub warmup: u64,
    pub seed: u64,
    pub cache_state: &'static str,
    pub direct_children: usize,
    pub leaves_per_child: usize,
    pub page_limit: u32,
    pub seed_batch_size: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct MetadataWorkloadReport {
    pub performance: WorkloadReport,
    pub read_amplification: MetadataReadAmplification,
}

#[derive(Clone, Debug, Serialize)]
pub struct MetadataReadAmplification {
    pub scope: &'static str,
    pub operations: u64,
    pub totals: MetadataReadCounterTotals,
    pub per_successful_operation: MetadataReadCountersPerOperation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MetadataReadCounterTotals {
    pub point_reads_total: u64,
    pub point_reads_fencing: u64,
    pub point_reads_authoritative: u64,
    pub point_reads_system: u64,
    pub point_reads_root_fence: u64,
    pub point_reads_workspace_current: u64,
    pub point_reads_path_current: u64,
    pub point_reads_other: u64,
    pub point_hits: u64,
    pub point_misses: u64,
    pub point_value_bytes: u64,
    pub scan_calls: u64,
    pub scan_cursors: u64,
    pub holt_scan_visited_units: u64,
    pub holt_scan_returned_keys: u64,
    pub holt_scan_common_prefixes: u64,
    pub holt_scan_restarts: u64,
    pub scan_key_bytes: u64,
    pub scan_value_bytes: u64,
    pub scan_raw_limit_stops: u64,
    pub holt_cache_hits: u64,
    pub holt_cache_misses: u64,
    pub holt_full_blob_reads: u64,
    pub holt_full_blob_read_bytes: u64,
    pub holt_point_full_blob_reads: u64,
    pub holt_scan_full_blob_reads: u64,
    pub holt_silent_full_blob_reads: u64,
    pub holt_read_page_hits: u64,
    pub holt_read_page_misses: u64,
    pub holt_read_index_cache_hits: u64,
    pub holt_read_index_cache_misses: u64,
    pub holt_read_index_loads: u64,
    pub holt_read_index_dir_read_bytes: u64,
    pub holt_read_index_bucket_reads: u64,
    pub holt_read_index_bucket_read_bytes: u64,
    pub holt_read_index_inline_hits: u64,
    pub holt_read_index_value_hits: u64,
    pub holt_read_index_value_read_bytes: u64,
    pub holt_read_index_offset_hits: u64,
    pub holt_read_index_negative_hits: u64,
    pub holt_read_index_crossing_hits: u64,
    pub holt_read_index_unknowns: u64,
    pub holt_optimistic_restarts: u64,
    pub holt_range_restarts: u64,
    pub holt_exposed_read_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct MetadataReadCountersPerOperation {
    pub point_reads_total: f64,
    pub point_reads_fencing: f64,
    pub point_reads_authoritative: f64,
    pub point_reads_workspace_current: f64,
    pub point_reads_path_current: f64,
    pub scan_calls: f64,
    pub scan_cursors: f64,
    pub holt_scan_visited_units: f64,
    pub holt_scan_returned_keys: f64,
    pub holt_scan_common_prefixes: f64,
    pub holt_scan_restarts: f64,
    pub scan_key_bytes: f64,
    pub scan_value_bytes: f64,
    pub scan_raw_limit_stops: f64,
    pub holt_full_blob_reads: f64,
    pub holt_exposed_read_bytes: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct AmplificationQualification {
    pub counter_semantics: &'static str,
    pub counter_scope: &'static str,
    pub attribution: &'static str,
    pub latency_instrumentation: &'static str,
    pub logical_point_reads: &'static str,
    pub logical_counter_coverage: &'static str,
    pub cursor_scan_work: &'static str,
    pub materialized_bytes: &'static str,
    pub holt_storage_io: &'static str,
    pub holt_internal_seek_count: &'static str,
    pub metadata_decoded_bytes: &'static str,
    pub host_cpu_memory_device_io: &'static str,
    pub network_utilization: &'static str,
}

pub(super) struct ReadStatsSample {
    pub(super) metadata: MetadataReadStats,
    pub(super) holt: HoltReadStats,
}

impl From<ReadStatsSample> for MetadataReadCounterTotals {
    fn from(sample: ReadStatsSample) -> Self {
        let metadata = sample.metadata;
        let holt = sample.holt;
        let point_reads_fencing = metadata
            .point_reads_system
            .saturating_add(metadata.point_reads_root_fence);
        let point_reads_authoritative = metadata
            .point_reads_workspace_current
            .saturating_add(metadata.point_reads_path_current)
            .saturating_add(metadata.point_reads_other);
        let holt_exposed_read_bytes = holt
            .full_blob_read_bytes
            .saturating_add(holt.read_index_dir_read_bytes)
            .saturating_add(holt.read_index_bucket_read_bytes)
            .saturating_add(holt.read_index_value_read_bytes);
        Self {
            point_reads_total: metadata.point_reads_total(),
            point_reads_fencing,
            point_reads_authoritative,
            point_reads_system: metadata.point_reads_system,
            point_reads_root_fence: metadata.point_reads_root_fence,
            point_reads_workspace_current: metadata.point_reads_workspace_current,
            point_reads_path_current: metadata.point_reads_path_current,
            point_reads_other: metadata.point_reads_other,
            point_hits: metadata.point_hits,
            point_misses: metadata.point_misses,
            point_value_bytes: metadata.point_value_bytes,
            scan_calls: metadata.scan_calls,
            scan_cursors: holt.scan_cursors,
            holt_scan_visited_units: holt.scan_visited_units,
            holt_scan_returned_keys: holt.scan_returned_keys,
            holt_scan_common_prefixes: holt.scan_common_prefixes,
            holt_scan_restarts: holt.scan_restarts,
            scan_key_bytes: metadata.scan_key_bytes,
            scan_value_bytes: metadata.scan_value_bytes,
            scan_raw_limit_stops: metadata.scan_raw_limit_stops,
            holt_cache_hits: holt.cache_hits,
            holt_cache_misses: holt.cache_misses,
            holt_full_blob_reads: holt.full_blob_reads,
            holt_full_blob_read_bytes: holt.full_blob_read_bytes,
            holt_point_full_blob_reads: holt.point_full_blob_reads,
            holt_scan_full_blob_reads: holt.scan_full_blob_reads,
            holt_silent_full_blob_reads: holt.silent_full_blob_reads,
            holt_read_page_hits: holt.read_page_hits,
            holt_read_page_misses: holt.read_page_misses,
            holt_read_index_cache_hits: holt.read_index_cache_hits,
            holt_read_index_cache_misses: holt.read_index_cache_misses,
            holt_read_index_loads: holt.read_index_loads,
            holt_read_index_dir_read_bytes: holt.read_index_dir_read_bytes,
            holt_read_index_bucket_reads: holt.read_index_bucket_reads,
            holt_read_index_bucket_read_bytes: holt.read_index_bucket_read_bytes,
            holt_read_index_inline_hits: holt.read_index_inline_hits,
            holt_read_index_value_hits: holt.read_index_value_hits,
            holt_read_index_value_read_bytes: holt.read_index_value_read_bytes,
            holt_read_index_offset_hits: holt.read_index_offset_hits,
            holt_read_index_negative_hits: holt.read_index_negative_hits,
            holt_read_index_crossing_hits: holt.read_index_crossing_hits,
            holt_read_index_unknowns: holt.read_index_unknowns,
            holt_optimistic_restarts: holt.optimistic_restarts,
            holt_range_restarts: holt.range_restarts,
            holt_exposed_read_bytes,
        }
    }
}

impl MetadataReadCounterTotals {
    fn per_operation(&self, operations: u64) -> MetadataReadCountersPerOperation {
        let divisor = operations as f64;
        let per_operation = |value| value as f64 / divisor;
        MetadataReadCountersPerOperation {
            point_reads_total: per_operation(self.point_reads_total),
            point_reads_fencing: per_operation(self.point_reads_fencing),
            point_reads_authoritative: per_operation(self.point_reads_authoritative),
            point_reads_workspace_current: per_operation(self.point_reads_workspace_current),
            point_reads_path_current: per_operation(self.point_reads_path_current),
            scan_calls: per_operation(self.scan_calls),
            scan_cursors: per_operation(self.scan_cursors),
            holt_scan_visited_units: per_operation(self.holt_scan_visited_units),
            holt_scan_returned_keys: per_operation(self.holt_scan_returned_keys),
            holt_scan_common_prefixes: per_operation(self.holt_scan_common_prefixes),
            holt_scan_restarts: per_operation(self.holt_scan_restarts),
            scan_key_bytes: per_operation(self.scan_key_bytes),
            scan_value_bytes: per_operation(self.scan_value_bytes),
            scan_raw_limit_stops: per_operation(self.scan_raw_limit_stops),
            holt_full_blob_reads: per_operation(self.holt_full_blob_reads),
            holt_exposed_read_bytes: per_operation(self.holt_exposed_read_bytes),
        }
    }
}

pub(super) fn measure_metadata_workload(
    harness: &Harness,
    workload: &'static str,
    warmup: u64,
    iterations: u64,
    operation: impl FnMut() -> Result<u64, String>,
) -> Result<MetadataWorkloadReport, String> {
    let measured = measure_with_diagnostics(
        workload,
        warmup,
        iterations,
        operation,
        || {
            let store = harness.executor.store();
            let metadata = store
                .begin_read_stats_session()
                .map_err(|error| error.to_string())?;
            let holt = store
                .begin_holt_read_stats_session()
                .map_err(|error| error.to_string())?;
            Ok((metadata, holt))
        },
        |(metadata, holt)| {
            let metadata = metadata.finish().map_err(|error| error.to_string())?;
            let holt = holt.finish().map_err(|error| error.to_string())?;
            Ok(ReadStatsSample { metadata, holt })
        },
    )?;
    let delta = measured.diagnostics;
    let operations = measured.report.successful;
    let totals = MetadataReadCounterTotals::from(delta);
    let per_successful_operation = totals.per_operation(operations);
    Ok(MetadataWorkloadReport {
        performance: measured.report,
        read_amplification: MetadataReadAmplification {
            scope: "logical_successful_operations_physical_timed_interval",
            operations,
            totals,
            per_successful_operation,
        },
    })
}

#[derive(Clone, Debug, Serialize)]
pub struct MetadataQualification {
    pub workspace_acceptance_gate_8: &'static str,
    pub acceptance_gate: &'static str,
    pub metadata_domain: &'static str,
    pub protocol_dto_executor: &'static str,
    pub framed_transport: &'static str,
    pub object_data_path: &'static str,
    pub end_to_end_sdk: &'static str,
    pub openviking_facade: &'static str,
    pub cold_cache: &'static str,
    pub failover: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct CorrectnessEvidence {
    pub before_timing: &'static str,
    pub after_timing: &'static str,
    pub semantic_digest_fnv1a64: String,
    pub scope: &'static str,
}
