/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::request::WorkspaceRpcRequest;
use crate::response::WorkspaceRpcResponse;

/// The exact and only accepted wire schema.
pub const WORKSPACE_PROTOCOL_SCHEMA: &str = "nokv.workspace.rpc.v2";
/// Exact schema for the versioned workspace RPC preflight exchange.
pub const WORKSPACE_PREFLIGHT_SCHEMA: &str = "nokv.workspace.rpc_preflight.v1";
/// Exact schema for the advertised workspace RPC capability set.
pub const WORKSPACE_CAPABILITY_SCHEMA: &str = "nokv.workspace.rpc_capabilities.v1";
/// Hard limit applied before decoding or after encoding.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Frame<T> {
    schema: String,
    payload: T,
}

pub fn encode_request(request: &WorkspaceRpcRequest) -> Result<Vec<u8>, ProtocolError> {
    request.validate()?;
    encode_payload(request)
}

pub fn decode_request(encoded: &[u8]) -> Result<WorkspaceRpcRequest, ProtocolError> {
    let request: WorkspaceRpcRequest = decode_payload(encoded)?;
    request.validate()?;
    Ok(request)
}

pub fn encode_response(response: &WorkspaceRpcResponse) -> Result<Vec<u8>, ProtocolError> {
    response.validate()?;
    encode_payload(response)
}

pub fn decode_response(encoded: &[u8]) -> Result<WorkspaceRpcResponse, ProtocolError> {
    let response: WorkspaceRpcResponse = decode_payload(encoded)?;
    response.validate()?;
    Ok(response)
}

fn encode_payload<T: Serialize>(payload: &T) -> Result<Vec<u8>, ProtocolError> {
    let frame = Frame {
        schema: WORKSPACE_PROTOCOL_SCHEMA.to_owned(),
        payload,
    };
    let encoded = rmp_serde::to_vec_named(&frame)
        .map_err(|error| ProtocolError::Encode(error.to_string()))?;
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge {
            bytes: encoded.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    Ok(encoded)
}

fn decode_payload<T: DeserializeOwned>(encoded: &[u8]) -> Result<T, ProtocolError> {
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge {
            bytes: encoded.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    let frame: Frame<T> =
        rmp_serde::from_slice(encoded).map_err(|error| ProtocolError::Decode(error.to_string()))?;
    if frame.schema != WORKSPACE_PROTOCOL_SCHEMA {
        return Err(ProtocolError::SchemaMismatch {
            actual: frame.schema,
            expected: WORKSPACE_PROTOCOL_SCHEMA,
        });
    }
    Ok(frame.payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArtifactDescriptor, ArtifactRevisionIdentity, CommitIdentity, CommitPreparation,
        CommitRequest, ContentType, CreateWorkspaceRequest, Digest, DigestUri, GetSnapshotRequest,
        LogicalShardIdentity, OperationIdentity, OperationKind, OperationProgress, OperationState,
        OperationStatus, OperationToken, PathListEntry, PathMetadata, PathPage, PublishCondition,
        RelativePath, RequestIdentity, RootIdentity, RootRoute, SnapshotAlias, SnapshotSelector,
        WorkbenchName, WorkspaceCapability, WorkspaceIdentity, WorkspacePath,
        WorkspacePreflightRequest, WorkspacePreflightResult, WorkspaceRequest, WorkspaceResult,
        WorkspaceRpcOutcome, WorkspaceSummary,
    };

    fn route() -> RootRoute {
        RootRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            placement_generation: 7,
            owner_epoch: 11,
        }
    }

    fn request() -> WorkspaceRpcRequest {
        WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([3; 16]),
            operation: WorkspaceRequest::CreateWorkspace(CreateWorkspaceRequest {
                workbench: WorkbenchName::new("run-42").unwrap(),
                workspace_incarnation_id: WorkspaceIdentity([4; 16]),
            }),
        }
    }

    #[test]
    fn request_round_trips_with_exact_schema() {
        let expected = request();
        let encoded = encode_request(&expected).unwrap();
        assert!(encoded
            .windows(WORKSPACE_PROTOCOL_SCHEMA.len())
            .any(|window| window == WORKSPACE_PROTOCOL_SCHEMA.as_bytes()));
        assert_eq!(decode_request(&encoded).unwrap(), expected);
    }

    #[test]
    fn commit_request_and_preparation_round_trip_the_projection_input_digest() {
        let commit = CommitRequest {
            operation_id: OperationIdentity([0x10; 16]),
            workbench: WorkbenchName::new("run-42").unwrap(),
            workspace_incarnation_id: WorkspaceIdentity([0x11; 16]),
            commit_id: CommitIdentity([0x12; 32]),
            content_digest: DigestUri::new(format!("sha256:{}", "13".repeat(32))).unwrap(),
            manifest_digest: DigestUri::new(format!("sha256:{}", "14".repeat(32))).unwrap(),
            projection_input_digest: Digest([0x15; 32]),
            tree_manifest_revision_id: ArtifactRevisionIdentity([0x16; 16]),
            replace: false,
            run_manifest_condition: PublishCondition::CreateOnly,
            expected_head_generation: None,
            parents: Vec::new(),
            producer: None,
            lineage_projection: Vec::new(),
        };
        let request = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x17; 16]),
            operation: WorkspaceRequest::Commit(commit.clone()),
        };
        assert_eq!(
            decode_request(&encode_request(&request).unwrap()).unwrap(),
            request
        );

        let status = OperationStatus {
            token: OperationToken {
                operation_id: commit.operation_id,
                state_digest: Digest([0x18; 32]),
            },
            kind: OperationKind::Commit,
            commit_preparation: Some(Box::new(CommitPreparation {
                request: Box::new(commit),
                committed_at_unix_seconds: 1_700_000_000,
                manifest: None,
            })),
            restore_preparation: None,
            state: OperationState::Running,
            progress: OperationProgress {
                completed_rows: 0,
                total_rows: None,
                completed_bytes: 0,
                total_bytes: None,
            },
            result: None,
            failure: None,
        };
        let response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x17; 16]),
            commit_version: Some(19),
            replayed: true,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Operation(status))),
        };
        assert_eq!(
            decode_response(&encode_response(&response).unwrap()).unwrap(),
            response
        );
    }

    #[test]
    fn snapshot_point_request_round_trips_with_an_alias_selector() {
        let expected = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([8; 16]),
            operation: WorkspaceRequest::GetSnapshot(GetSnapshotRequest {
                workbench: WorkbenchName::new("run-42").unwrap(),
                selector: SnapshotSelector::Alias(SnapshotAlias::new("checkpoint").unwrap()),
            }),
        };

        assert_eq!(
            decode_request(&encode_request(&expected).unwrap()).unwrap(),
            expected
        );
    }

    #[test]
    fn response_round_trips() {
        let expected = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([3; 16]),
            commit_version: Some(19),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Workspace(
                WorkspaceSummary {
                    workbench: WorkbenchName::new("run-42").unwrap(),
                    workspace_incarnation_id: WorkspaceIdentity([4; 16]),
                    workspace_revision: 0,
                    commit_head: None,
                    commit_head_generation: None,
                },
            ))),
        };
        let encoded = encode_response(&expected).unwrap();
        assert_eq!(decode_response(&encoded).unwrap(), expected);
    }

    #[test]
    fn path_page_round_trips_artifact_and_implicit_prefix_variants() {
        let workbench = WorkbenchName::new("run-42").unwrap();
        let path = |value: &str| WorkspacePath {
            workbench: workbench.clone(),
            path: RelativePath::new(value).unwrap(),
        };
        let expected = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([3; 16]),
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Paths(PathPage {
                entries: vec![
                    PathListEntry::Prefix(path("outputs/nested")),
                    PathListEntry::Artifact(PathMetadata {
                        path: path("outputs/result.json"),
                        workspace_incarnation_id: WorkspaceIdentity([4; 16]),
                        workspace_revision: 2,
                        generation: 1,
                        artifact_revision_id: ArtifactRevisionIdentity([5; 16]),
                        dependency_count: 0,
                        dependency_depth: 0,
                        descriptor: ArtifactDescriptor {
                            logical_size: 7,
                            body_digest: DigestUri::new(format!("sha256:{}", "06".repeat(32)))
                                .unwrap(),
                            manifest_digest: DigestUri::new(format!("sha256:{}", "07".repeat(32)))
                                .unwrap(),
                            content_type: ContentType::new("application/json").unwrap(),
                            producer: None,
                            manifest_identity: None,
                            index_fields: Vec::new(),
                        },
                    }),
                ],
                next_cursor: Some(b"outputs/result.json".to_vec()),
                read_version: 9,
            }))),
        };

        assert_eq!(
            decode_response(&encode_response(&expected).unwrap()).unwrap(),
            expected
        );
    }

    #[test]
    fn schema_mismatch_fails_closed() {
        let encoded = rmp_serde::to_vec_named(&Frame {
            schema: "nokv.workspace.rpc.v1".to_owned(),
            payload: request(),
        })
        .unwrap();
        assert!(matches!(
            decode_request(&encoded),
            Err(ProtocolError::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn zero_route_fence_is_rejected_before_encode() {
        let mut invalid = request();
        invalid.route.owner_epoch = 0;
        assert!(matches!(
            encode_request(&invalid),
            Err(ProtocolError::InvalidField {
                field: "route.owner_epoch",
                ..
            })
        ));
    }

    #[test]
    fn oversized_frame_is_rejected_before_decode() {
        let encoded = vec![0; MAX_FRAME_BYTES + 1];
        assert_eq!(
            decode_request(&encoded),
            Err(ProtocolError::FrameTooLarge {
                bytes: MAX_FRAME_BYTES + 1,
                max: MAX_FRAME_BYTES,
            })
        );
    }

    #[test]
    fn preflight_round_trips_with_exact_versioned_schemas_and_canonical_sets() {
        let expected = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([8; 16]),
            operation: WorkspaceRequest::Preflight(WorkspacePreflightRequest::new([
                WorkspaceCapability::RestoreV1,
                WorkspaceCapability::QueryV1,
                WorkspaceCapability::RestoreV1,
            ])),
        };
        let WorkspaceRequest::Preflight(preflight) = &expected.operation else {
            unreachable!();
        };
        assert_eq!(preflight.preflight_schema, WORKSPACE_PREFLIGHT_SCHEMA);
        assert_eq!(preflight.protocol_schema, WORKSPACE_PROTOCOL_SCHEMA);
        assert_eq!(preflight.capability_schema, WORKSPACE_CAPABILITY_SCHEMA);
        assert_eq!(
            preflight.required_capabilities,
            vec![WorkspaceCapability::QueryV1, WorkspaceCapability::RestoreV1]
        );
        assert_eq!(
            decode_request(&encode_request(&expected).unwrap()).unwrap(),
            expected
        );

        let response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([8; 16]),
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Preflight(
                WorkspacePreflightResult::new(
                    route(),
                    [WorkspaceCapability::RestoreV1, WorkspaceCapability::QueryV1],
                ),
            ))),
        };
        assert_eq!(
            decode_response(&encode_response(&response).unwrap()).unwrap(),
            response
        );
    }

    #[test]
    fn preflight_rejects_schema_drift_and_noncanonical_capability_sets() {
        let mut preflight = WorkspacePreflightRequest::new([
            WorkspaceCapability::QueryV1,
            WorkspaceCapability::RestoreV1,
        ]);
        preflight.capability_schema = "another.capability.schema".to_owned();
        let mut invalid = request();
        invalid.operation = WorkspaceRequest::Preflight(preflight);
        assert!(matches!(
            encode_request(&invalid),
            Err(ProtocolError::InvalidField {
                field: "preflight.capability_schema",
                ..
            })
        ));

        let mut duplicate = WorkspacePreflightRequest::new([WorkspaceCapability::QueryV1]);
        duplicate
            .required_capabilities
            .push(WorkspaceCapability::QueryV1);
        invalid.operation = WorkspaceRequest::Preflight(duplicate);
        assert!(matches!(
            encode_request(&invalid),
            Err(ProtocolError::InvalidField {
                field: "preflight.required_capabilities",
                ..
            })
        ));
    }

    #[test]
    fn preflight_response_is_bound_to_envelope_route_and_canonical_set() {
        let mut result = WorkspacePreflightResult::new(
            route(),
            [WorkspaceCapability::QueryV1, WorkspaceCapability::RestoreV1],
        );
        result
            .supported_capabilities
            .push(WorkspaceCapability::RestoreV1);
        let mut response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([8; 16]),
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Preflight(result))),
        };
        assert!(matches!(
            encode_response(&response),
            Err(ProtocolError::InvalidField {
                field: "preflight.supported_capabilities",
                ..
            })
        ));

        let mut other_route = route();
        other_route.owner_epoch += 1;
        response.outcome = WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Preflight(
            WorkspacePreflightResult::new(other_route, WorkspaceCapability::ALL),
        )));
        assert!(matches!(
            encode_response(&response),
            Err(ProtocolError::InvalidField {
                field: "preflight.route",
                ..
            })
        ));
    }
}
