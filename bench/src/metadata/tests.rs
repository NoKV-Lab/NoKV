/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_meta::workspace::MetadataReadStats;

use super::evidence::MetadataReadCounterTotals;
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
        assert!(row.read_amplification.totals.point_reads_total >= 4);
        assert!(row.read_amplification.totals.point_reads_authoritative >= 4);
        assert_eq!(
            row.read_amplification.totals.point_reads_workspace_current,
            2
        );
        assert_eq!(row.read_amplification.totals.point_reads_path_current, 2);
    }
    let recursive = report
        .workloads
        .iter()
        .find(|row| row.performance.workload == "recursive_list_first_page")
        .unwrap();
    assert_eq!(recursive.read_amplification.totals.scan_calls, 2);
    assert_eq!(recursive.read_amplification.totals.scan_cursors, 2);
    assert!(recursive.read_amplification.totals.holt_scan_returned_keys >= 4);
    let direct = report
        .workloads
        .iter()
        .find(|row| row.performance.workload == "direct_list_middle_page")
        .unwrap();
    assert_eq!(direct.read_amplification.totals.scan_calls, 2);
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
fn physical_holt_counters_remain_nonzero_in_json_mapping() {
    let totals = MetadataReadCounterTotals::from(MetadataReadStats {
        holt_full_blob_reads: 2,
        holt_full_blob_read_bytes: 11,
        holt_read_index_dir_read_bytes: 13,
        holt_read_index_bucket_read_bytes: 17,
        holt_read_index_value_read_bytes: 19,
        ..MetadataReadStats::default()
    });
    let json = serde_json::to_value(totals).unwrap();

    assert_eq!(json["holt_full_blob_reads"], 2);
    assert_eq!(json["holt_exposed_read_bytes"], 60);
}
