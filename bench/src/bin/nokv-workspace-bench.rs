/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Focused workspace-protocol codec benchmark.
//!
//! This is not durability, failover, object-store, or end-to-end performance
//! evidence. The output states that qualification boundary explicitly.

use std::env;
use std::hint::black_box;
use std::time::Instant;

use nokv_protocol::{
    decode_request, encode_request, ListPathsRequest, LogicalShardIdentity,
    ObjectNamespaceIdentity, PageRequest, RequestIdentity, RootIdentity, RootRoute, RpcRequest,
    WorkbenchName, WorkspaceReadView, WorkspaceRequest, WorkspaceRpcRequest,
    WORKSPACE_PROTOCOL_SCHEMA,
};
use serde_json::json;

const DEFAULT_ITERATIONS: u64 = 100_000;

fn main() {
    match run(env::args().skip(1)) {
        Ok(()) => {}
        Err(message) => {
            eprintln!("error: {message}");
            std::process::exit(2);
        }
    }
}

fn run(arguments: impl Iterator<Item = String>) -> Result<(), String> {
    let iterations = parse_iterations(arguments)?;
    let request = RpcRequest::Workspace(Box::new(sample_request()?));
    let encoded = encode_request(&request).map_err(|error| error.to_string())?;

    let started = Instant::now();
    for _ in 0..iterations {
        let frame = encode_request(black_box(&request)).map_err(|error| error.to_string())?;
        let decoded = decode_request(black_box(&frame)).map_err(|error| error.to_string())?;
        black_box(decoded);
    }
    let elapsed = started.elapsed();
    let seconds = elapsed.as_secs_f64();
    let operations_per_second = if seconds == 0.0 {
        0.0
    } else {
        iterations as f64 / seconds
    };
    println!(
        "{}",
        json!({
            "schema": "nokv.workspace.benchmark",
            "protocol_schema": WORKSPACE_PROTOCOL_SCHEMA,
            "workload": "list_paths_request_codec_roundtrip",
            "iterations": iterations,
            "frame_bytes": encoded.len(),
            "elapsed_seconds": seconds,
            "operations_per_second": operations_per_second,
            "qualification": {
                "codec": "measured",
                "metadata_durability": "not_qualified",
                "failover": "not_qualified",
                "object_data_path": "not_qualified",
                "end_to_end_sdk": "not_qualified"
            }
        })
    );
    Ok(())
}

fn parse_iterations(arguments: impl Iterator<Item = String>) -> Result<u64, String> {
    let arguments = arguments.collect::<Vec<_>>();
    match arguments.as_slice() {
        [] => Ok(DEFAULT_ITERATIONS),
        [flag, value] if flag == "--iterations" => {
            let iterations = value
                .parse::<u64>()
                .map_err(|_| "--iterations must be a positive integer".to_owned())?;
            if iterations == 0 {
                return Err("--iterations must be greater than zero".to_owned());
            }
            Ok(iterations)
        }
        _ => Err("usage: nokv-workspace-bench [--iterations N]".to_owned()),
    }
}

fn sample_request() -> Result<WorkspaceRpcRequest, String> {
    Ok(WorkspaceRpcRequest {
        route: RootRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            object_namespace_id: ObjectNamespaceIdentity([8; 16]),
            placement_generation: 1,
            owner_epoch: 1,
        },
        request_id: RequestIdentity([3; 16]),
        operation: WorkspaceRequest::ListPaths(ListPathsRequest {
            workbench: WorkbenchName::new("benchmark").map_err(|error| error.to_string())?,
            prefix: None,
            recursive: false,
            view: WorkspaceReadView::Live,
            expected_read_version: None,
            workspace_continuation_fence: None,
            page: PageRequest {
                cursor: None,
                limit: 100,
            },
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterations_are_positive_and_explicit() {
        assert_eq!(
            parse_iterations(std::iter::empty()).unwrap(),
            DEFAULT_ITERATIONS
        );
        assert_eq!(
            parse_iterations(["--iterations".to_owned(), "7".to_owned()].into_iter()).unwrap(),
            7
        );
        assert!(parse_iterations(["--iterations".to_owned(), "0".to_owned()].into_iter()).is_err());
    }

    #[test]
    fn benchmark_request_uses_workspace_route() {
        let request = sample_request().unwrap();
        request.validate().unwrap();
        assert!(matches!(request.operation, WorkspaceRequest::ListPaths(_)));
    }
}
