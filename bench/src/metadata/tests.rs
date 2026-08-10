/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_meta::workspace::MetadataReadStats;
use nokv_meta_holt::HoltReadStats;

use super::evidence::{MetadataReadCounterTotals, ReadStatsSample};
use super::fixture::MAX_FIXTURE_PATHS;
use super::options::cache_state;
use super::{run, MetadataOptions};

#[test]
fn metadata_options_validate_page_shape() {
    let options = MetadataOptions::parse(
        [
            "--iterations",
            "4",
            "--warmup",
            "0",
            "--direct-children",
            "8",
            "--leaves-per-child",
            "2",
            "--page-limit",
            "4",
            "--seed",
            "7",
            "--revision",
            "candidate",
            "--harness-revision",
            "harness-sha256",
            "--dirty-worktree",
        ]
        .into_iter()
        .map(str::to_owned),
    )
    .unwrap();
    assert_eq!(options.iterations, 4);
    assert_eq!(options.warmup, 0);
    assert_eq!(options.direct_children, 8);
    assert_eq!(options.page_limit, 4);
    assert_eq!(options.revision, "candidate");
    assert_eq!(options.harness_revision, "harness-sha256");
    assert!(options.dirty_worktree);
    assert_eq!(cache_state(options.warmup), "uncontrolled");
    assert_eq!(cache_state(1), "same_request_warmup");
}

#[test]
fn metadata_runner_exercises_exact_and_cursor_reads() {
    let report = run(MetadataOptions {
        iterations: 2,
        warmup: 1,
        direct_children: 8,
        leaves_per_child: 2,
        page_limit: 2,
        seed: 9,
        metadata_dir: None,
        revision: "test".to_owned(),
        harness_revision: "test-harness".to_owned(),
        dirty_worktree: true,
    })
    .unwrap();
    assert_eq!(report.schema, "nokv.metadata-read.benchmark.v3");
    assert_eq!(report.profile.paths_per_workspace, 25);
    assert_eq!(report.workloads.len(), 8);
    assert!(report
        .workloads
        .iter()
        .all(|row| row.performance.successful == 2));
    for row in report
        .workloads
        .iter()
        .filter(|row| row.performance.workload.starts_with("exact_get_"))
    {
        assert_eq!(row.read_amplification.totals.point_reads_total, 22);
        assert_eq!(row.read_amplification.totals.point_reads_fencing, 18);
        assert_eq!(row.read_amplification.totals.point_reads_authoritative, 4);
        assert_eq!(row.read_amplification.totals.point_reads_system, 12);
        assert_eq!(row.read_amplification.totals.point_reads_root_fence, 6);
        assert_eq!(
            row.read_amplification.totals.point_reads_workspace_current,
            2
        );
        assert_eq!(row.read_amplification.totals.point_reads_path_current, 2);
        assert_eq!(row.read_amplification.totals.point_reads_other, 0);
    }
    let recursive = report
        .workloads
        .iter()
        .find(|row| row.performance.workload == "recursive_list_first_page")
        .unwrap();
    assert_eq!(recursive.read_amplification.totals.scan_calls, 2);
    assert_eq!(recursive.read_amplification.totals.scan_cursors, 2);
    assert_eq!(recursive.read_amplification.totals.scan_raw_limit_stops, 2);
    assert!(recursive.read_amplification.totals.holt_scan_returned_keys >= 4);
    let direct = report
        .workloads
        .iter()
        .find(|row| row.performance.workload == "direct_list_middle_page")
        .unwrap();
    assert_eq!(direct.read_amplification.totals.scan_calls, 2);
    assert_eq!(direct.read_amplification.totals.scan_cursors, 2);
    assert_eq!(direct.read_amplification.totals.scan_raw_limit_stops, 2);
    assert!(direct.read_amplification.totals.holt_scan_returned_keys >= 4);
    assert_eq!(report.profile.cache_state, "same_request_warmup");
    assert_eq!(report.correctness.before_timing, "passed");
    assert_eq!(report.correctness.after_timing, "passed");
    assert_eq!(
        report.qualification.workspace_acceptance_gate_8,
        "not_qualified"
    );
}

#[test]
fn metadata_options_reject_oversized_fixture() {
    let options = MetadataOptions {
        direct_children: MAX_FIXTURE_PATHS,
        leaves_per_child: 1,
        revision: "test".to_owned(),
        harness_revision: "test-harness".to_owned(),
        ..MetadataOptions::default()
    };
    assert!(options.validate().unwrap_err().contains("maximum"));
}

#[test]
fn metadata_read_counter_mapping_covers_every_source_and_output_field() {
    let totals = MetadataReadCounterTotals::from(ReadStatsSample {
        metadata: MetadataReadStats {
            point_reads_system: 1,
            point_reads_root_fence: 2,
            point_reads_workspace_current: 3,
            point_reads_path_current: 4,
            point_reads_other: 5,
            point_hits: 6,
            point_misses: 7,
            point_value_bytes: 8,
            scan_calls: 9,
            scan_key_bytes: 10,
            scan_value_bytes: 11,
            scan_raw_limit_stops: 12,
        },
        holt: HoltReadStats {
            scan_cursors: 13,
            scan_visited_units: 14,
            scan_returned_keys: 15,
            scan_common_prefixes: 16,
            scan_restarts: 17,
            cache_hits: 18,
            cache_misses: 19,
            full_blob_reads: 20,
            full_blob_read_bytes: 21,
            point_full_blob_reads: 22,
            scan_full_blob_reads: 23,
            silent_full_blob_reads: 24,
            read_page_hits: 25,
            read_page_misses: 26,
            read_index_cache_hits: 27,
            read_index_cache_misses: 28,
            read_index_loads: 29,
            read_index_dir_read_bytes: 30,
            read_index_bucket_reads: 31,
            read_index_bucket_read_bytes: 32,
            read_index_inline_hits: 33,
            read_index_value_hits: 34,
            read_index_value_read_bytes: 35,
            read_index_offset_hits: 36,
            read_index_negative_hits: 37,
            read_index_crossing_hits: 38,
            read_index_unknowns: 39,
            optimistic_restarts: 40,
            range_restarts: 41,
        },
    });

    assert_eq!(
        totals,
        MetadataReadCounterTotals {
            point_reads_total: 15,
            point_reads_fencing: 3,
            point_reads_authoritative: 12,
            point_reads_system: 1,
            point_reads_root_fence: 2,
            point_reads_workspace_current: 3,
            point_reads_path_current: 4,
            point_reads_other: 5,
            point_hits: 6,
            point_misses: 7,
            point_value_bytes: 8,
            scan_calls: 9,
            scan_cursors: 13,
            holt_scan_visited_units: 14,
            holt_scan_returned_keys: 15,
            holt_scan_common_prefixes: 16,
            holt_scan_restarts: 17,
            scan_key_bytes: 10,
            scan_value_bytes: 11,
            scan_raw_limit_stops: 12,
            holt_cache_hits: 18,
            holt_cache_misses: 19,
            holt_full_blob_reads: 20,
            holt_full_blob_read_bytes: 21,
            holt_point_full_blob_reads: 22,
            holt_scan_full_blob_reads: 23,
            holt_silent_full_blob_reads: 24,
            holt_read_page_hits: 25,
            holt_read_page_misses: 26,
            holt_read_index_cache_hits: 27,
            holt_read_index_cache_misses: 28,
            holt_read_index_loads: 29,
            holt_read_index_dir_read_bytes: 30,
            holt_read_index_bucket_reads: 31,
            holt_read_index_bucket_read_bytes: 32,
            holt_read_index_inline_hits: 33,
            holt_read_index_value_hits: 34,
            holt_read_index_value_read_bytes: 35,
            holt_read_index_offset_hits: 36,
            holt_read_index_negative_hits: 37,
            holt_read_index_crossing_hits: 38,
            holt_read_index_unknowns: 39,
            holt_optimistic_restarts: 40,
            holt_range_restarts: 41,
            holt_exposed_read_bytes: 118,
        }
    );
}
