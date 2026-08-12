/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::env;
use std::time::Instant;

use nokv_protocol::WORKSPACE_PROTOCOL_SCHEMA;

mod correctness;
mod evidence;
mod fixture;
mod options;
mod workload;

use correctness::semantic_digest;
use evidence::{
    measure_metadata_workload, AmplificationQualification, CorrectnessEvidence, MetadataProfile,
    MetadataQualification,
};
use fixture::{fixture_path_count, Harness, SEED_BATCH_SIZE};
use options::cache_state;

pub use evidence::MetadataBenchmarkReport;
pub use options::{usage, MetadataOptions};

pub fn run(options: MetadataOptions) -> Result<MetadataBenchmarkReport, String> {
    options.validate()?;
    let setup_started = Instant::now();
    let harness = Harness::new(&options)?;
    let (pages, direct_entries_before) = harness.prepare_direct_pages(options.page_limit)?;
    let setup_seconds = setup_started.elapsed().as_secs_f64();

    let shallow_existing = harness.get_request("outputs/hot", 10_001)?;
    let deep_existing = harness.get_request(
        &format!(
            "outputs/hot/child-{:04}/deep/{:04}.bin",
            options.direct_children - 1,
            options.leaves_per_child - 1
        ),
        10_002,
    )?;
    let shallow_missing = harness.get_request("outputs/missing", 10_003)?;
    let deep_missing =
        harness.get_request("outputs/hot/child-0000/deep/missing/level/file.bin", 10_004)?;
    let recursive_first = harness.list_request(
        Some("outputs/hot"),
        true,
        None,
        None,
        options.page_limit,
        10_005,
    )?;

    let correctness_before = harness.correctness_snapshot(
        &shallow_existing,
        &deep_existing,
        &shallow_missing,
        &deep_missing,
        &recursive_first,
        direct_entries_before,
        options.page_limit,
    )?;

    let mut workloads = vec![
        measure_metadata_workload(
            &harness,
            "exact_get_existing_depth_2",
            options.warmup,
            options.iterations,
            || harness.exact_existing_checksum(&shallow_existing),
        )?,
        measure_metadata_workload(
            &harness,
            "exact_get_existing_depth_5",
            options.warmup,
            options.iterations,
            || harness.exact_existing_checksum(&deep_existing),
        )?,
        measure_metadata_workload(
            &harness,
            "exact_get_missing_depth_2",
            options.warmup,
            options.iterations,
            || harness.exact_missing_checksum(&shallow_missing),
        )?,
        measure_metadata_workload(
            &harness,
            "exact_get_missing_depth_7",
            options.warmup,
            options.iterations,
            || harness.exact_missing_checksum(&deep_missing),
        )?,
        measure_metadata_workload(
            &harness,
            "recursive_list_first_page",
            options.warmup,
            options.iterations,
            || harness.page_checksum(&recursive_first),
        )?,
    ];
    for (name, request) in [
        ("direct_list_first_page", &pages.first),
        ("direct_list_middle_page", &pages.middle),
        ("direct_list_final_page", &pages.final_page),
    ] {
        workloads.push(measure_metadata_workload(
            &harness,
            name,
            options.warmup,
            options.iterations,
            || harness.page_checksum(request),
        )?);
    }

    let (_, direct_entries_after) = harness.prepare_direct_pages(options.page_limit)?;
    let correctness_after = harness.correctness_snapshot(
        &shallow_existing,
        &deep_existing,
        &shallow_missing,
        &deep_missing,
        &recursive_first,
        direct_entries_after,
        options.page_limit,
    )?;
    if correctness_before != correctness_after {
        return Err("logical results changed between pre- and post-timing assertions".to_owned());
    }
    let semantic_digest = semantic_digest(&correctness_before)?;

    let path_count = fixture_path_count(&options)?;
    let (metadata_store, metadata_device, durability_profile) = match &options.metadata_dir {
        Some(path) => (
            path.display().to_string(),
            env::var("NOKV_BENCH_METADATA_DEVICE").unwrap_or_else(|_| "unknown".to_owned()),
            "local_wal",
        ),
        None => (
            "memory".to_owned(),
            "process_memory".to_owned(),
            "memory_not_durable",
        ),
    };
    Ok(MetadataBenchmarkReport {
        schema: "nokv.metadata-read.benchmark.v3",
        evidence_level: "metadata_domain_with_protocol_executor",
        protocol_schema: WORKSPACE_PROTOCOL_SCHEMA,
        profile: MetadataProfile {
            revision: options.revision,
            harness_revision: options.harness_revision,
            dirty_worktree: options.dirty_worktree,
            rust_toolchain: env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_owned()),
            machine: env::var("NOKV_BENCH_MACHINE")
                .or_else(|_| env::var("HOSTNAME"))
                .unwrap_or_else(|_| "unknown".to_owned()),
            operating_system: env::consts::OS,
            architecture: env::consts::ARCH,
            metadata_device,
            metadata_store,
            metadata_engine: "holt-0.8.5",
            object_provider: "not_applicable",
            object_endpoint_class: "not_applicable",
            durability_profile,
            logical_shards: 1,
            physical_owners: 1,
            roots: 1,
            workspace_count: 1,
            paths_per_workspace: path_count,
            payload_distribution: "zero_length_revision_owned_artifacts_no_object_bytes",
            setup_method: "bounded_metadata_commands_with_revision_and_reference_records",
            concurrency: 1,
            iterations: options.iterations,
            warmup: options.warmup,
            seed: options.seed,
            cache_state: cache_state(options.warmup),
            direct_children: options.direct_children,
            leaves_per_child: options.leaves_per_child,
            page_limit: options.page_limit,
            seed_batch_size: SEED_BATCH_SIZE,
        },
        setup_seconds,
        workloads,
        correctness: CorrectnessEvidence {
            before_timing: "passed",
            after_timing: "passed",
            semantic_digest_fnv1a64: format!("{semantic_digest:016x}"),
            scope: "expected exact identity plus ordered list kind, path, and full metadata",
        },
        amplification: AmplificationQualification {
            counter_semantics: "nokv.metadata-read-stats.v1",
            counter_scope: "thread_bound_store_scoped_timed_interval",
            attribution:
                "logical_session_local_physical_store_wide_dedicated_store_concurrency_1_background_maintenance_may_contribute",
            latency_instrumentation:
                "thread_local_logical_counter_updates_included_session_boundaries_excluded",
            logical_point_reads: "measured_by_nokv_meta_fenced_query_source",
            logical_counter_coverage:
                "fenced_query_paths_only_excludes_write_transaction_and_recovery_internal_reads",
            cursor_scan_work: "measured_from_holt_range_iter_scan_stats",
            materialized_bytes: "measured_for_values_and_keys_emitted_by_holt",
            holt_storage_io: "measured_as_shared_db_counter_delta",
            holt_internal_seek_count: "unavailable_in_holt_0.8.5",
            metadata_decoded_bytes:
                "unavailable_materialized_bytes_are_reported_without_claiming_decode_cost",
            host_cpu_memory_device_io: "not_measured",
            network_utilization: "not_applicable_in_process",
        },
        qualification: MetadataQualification {
            workspace_acceptance_gate_8: "not_qualified",
            acceptance_gate:
                "docs/development/workspace-acceptance.md#gate-8-performance-and-scale",
            metadata_domain: "measured",
            protocol_dto_executor: "measured",
            framed_transport: "not_qualified",
            object_data_path: "not_qualified",
            end_to_end_sdk: "not_qualified",
            openviking_facade: "not_qualified",
            cold_cache: "not_qualified",
            failover: "not_qualified",
        },
    })
}

#[cfg(test)]
mod tests;
