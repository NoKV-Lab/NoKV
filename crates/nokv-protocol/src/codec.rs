/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use serde::{de::DeserializeOwned, de::IgnoredAny, Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::request::RpcRequest;
use crate::response::RpcResponse;

/// The exact and only accepted wire schema.
pub const WORKSPACE_PROTOCOL_SCHEMA: &str = "nokv.workspace.rpc.v10";
/// Exact schema for the versioned workspace RPC preflight exchange.
pub const WORKSPACE_PREFLIGHT_SCHEMA: &str = "nokv.workspace.rpc_preflight.v1";
/// Exact schema for the advertised workspace RPC capability set.
pub const WORKSPACE_CAPABILITY_SCHEMA: &str = "nokv.workspace.rpc_capabilities.v2";
/// Hard limit applied before decoding or after encoding.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Frame<T> {
    schema: String,
    payload: T,
}

pub fn encode_request(request: &RpcRequest) -> Result<Vec<u8>, ProtocolError> {
    request.validate()?;
    encode_payload(request)
}

pub fn decode_request(encoded: &[u8]) -> Result<RpcRequest, ProtocolError> {
    let request: RpcRequest = decode_payload(encoded)?;
    request.validate()?;
    Ok(request)
}

pub fn encode_response(response: &RpcResponse) -> Result<Vec<u8>, ProtocolError> {
    response.validate()?;
    encode_payload(response)
}

pub fn decode_response(encoded: &[u8]) -> Result<RpcResponse, ProtocolError> {
    let response: RpcResponse = decode_payload(encoded)?;
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
    // Inspect the version envelope before decoding a version-specific payload.
    // Otherwise a structurally older, valid frame could surface as a generic
    // payload decode failure before its explicit schema mismatch is reported.
    let header: Frame<IgnoredAny> =
        rmp_serde::from_slice(encoded).map_err(|error| ProtocolError::Decode(error.to_string()))?;
    if header.schema != WORKSPACE_PROTOCOL_SCHEMA {
        return Err(ProtocolError::SchemaMismatch {
            actual: header.schema,
            expected: WORKSPACE_PROTOCOL_SCHEMA,
        });
    }
    let frame: Frame<T> =
        rmp_serde::from_slice(encoded).map_err(|error| ProtocolError::Decode(error.to_string()))?;
    Ok(frame.payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AbortGenericIndexRegistrationRequest, AppendGenericIndexRowsRequest, ArtifactDescriptor,
        ArtifactRevisionIdentity, BeginGenericIndexRegistrationRequest,
        BindRestoreDestinationRequest, CatalogField, CatalogPathMatch, CatalogRequest,
        CatalogResult, CommitIdentity, CommitPreparation, CommitRequest, ContentType,
        CreateWorkspaceRequest, Digest, DigestUri, FacetBucket, FacetResult, FieldValue,
        FinalizeGenericIndexRegistrationRequest, GenericIndexAbortResult,
        GenericIndexAppendReceipt, GenericIndexAppendResult, GenericIndexFieldCapability,
        GenericIndexFieldValues, GenericIndexGenerationIdentity, GenericIndexRegistrationPhase,
        GenericIndexRegistrationStatus, GenericIndexRow, GenericNamespaceArtifact,
        GenericNamespaceHit, GenericNamespaceKind, GetGenericIndexRegistrationRequest,
        GetPathRequest, GetSnapshotRequest, ListPathsRequest, LogicalShardIdentity,
        ObjectNamespaceIdentity, OperationIdentity, OperationKind, OperationProgress,
        OperationState, OperationStatus, OperationToken, PageRequest, PathListEntry, PathMetadata,
        PathPage, PathReadResult, PrepareRestoreRequest, PublishCondition, QueryOperand,
        QueryOperator, QueryPredicate, QueryProfile, QueryScope,
        ReadRestoreSourceRunManifestRequest, RelativePath, RenamePathRequest, RenamePathResult,
        RequestIdentity, RestoreManifestDescriptor, RestoreManifestIdentity, RestorePreparation,
        RestoreSource, RestoreSourceCommitBinding, RootIdentity, RootRoute, ScalarValue, SearchHit,
        SearchRequest, SearchResult, SearchRow, SnapshotAlias, SnapshotSelector, SortDirection,
        SortField, WorkbenchName, WorkspaceCapability, WorkspaceContinuationFence,
        WorkspaceIdentity, WorkspacePath, WorkspacePreflightRequest, WorkspacePreflightResult,
        WorkspaceReadView, WorkspaceRequest, WorkspaceResult, WorkspaceRpcOutcome,
        WorkspaceRpcRequest, WorkspaceRpcResponse, WorkspaceSummary,
    };
    use sha2::{Digest as _, Sha256};

    fn encode_request(request: &WorkspaceRpcRequest) -> Result<Vec<u8>, ProtocolError> {
        super::encode_request(&RpcRequest::Workspace(Box::new(request.clone())))
    }

    fn decode_request(encoded: &[u8]) -> Result<WorkspaceRpcRequest, ProtocolError> {
        match super::decode_request(encoded)? {
            RpcRequest::Workspace(request) => Ok(*request),
            RpcRequest::DiscoverRoute(_) => Err(ProtocolError::invalid(
                "rpc",
                "expected a workspace request",
            )),
        }
    }

    fn encode_response(response: &WorkspaceRpcResponse) -> Result<Vec<u8>, ProtocolError> {
        super::encode_response(&RpcResponse::Workspace(Box::new(response.clone())))
    }

    fn decode_response(encoded: &[u8]) -> Result<WorkspaceRpcResponse, ProtocolError> {
        match super::decode_response(encoded)? {
            RpcResponse::Workspace(response) => Ok(*response),
            RpcResponse::DiscoverRoute(_) => Err(ProtocolError::invalid(
                "rpc",
                "expected a workspace response",
            )),
        }
    }

    fn encode_unvalidated_request(request: &WorkspaceRpcRequest) -> Result<Vec<u8>, ProtocolError> {
        super::encode_payload(&RpcRequest::Workspace(Box::new(request.clone())))
    }

    fn encode_unvalidated_response(
        response: &WorkspaceRpcResponse,
    ) -> Result<Vec<u8>, ProtocolError> {
        super::encode_payload(&RpcResponse::Workspace(Box::new(response.clone())))
    }

    fn route() -> RootRoute {
        RootRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            object_namespace_id: ObjectNamespaceIdentity([8; 16]),
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

    fn generic_registration_status(
        operation_fill: u8,
        generation_fill: u8,
        phase: GenericIndexRegistrationPhase,
    ) -> GenericIndexRegistrationStatus {
        let terminal = phase == GenericIndexRegistrationPhase::Quarantined;
        let complete = phase == GenericIndexRegistrationPhase::Complete;
        GenericIndexRegistrationStatus {
            operation_id: OperationIdentity([operation_fill; 16]),
            generation_id: GenericIndexGenerationIdentity([generation_fill; 16]),
            workspace_incarnation_id: WorkspaceIdentity([0x52; 16]),
            index_path: Some(RelativePath::new("outputs").unwrap()),
            source_read_version: 100,
            last_transition_version: 101,
            expected_current_generation: Some(9),
            capability_digest: Digest([0x53; 32]),
            declared_row_count: 0,
            appended_row_count: 0,
            row_digest: Digest([0; 32]),
            phase,
            published_pointer_generation: complete.then_some(10),
            terminal_error: terminal.then(|| "registration quarantined".to_owned()),
        }
    }

    fn generic_begin_request() -> WorkspaceRpcRequest {
        WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x54; 16]),
            operation: WorkspaceRequest::BeginGenericIndexRegistration(
                BeginGenericIndexRegistrationRequest {
                    operation_id: OperationIdentity([0x50; 16]),
                    generation_id: GenericIndexGenerationIdentity([0x51; 16]),
                    workbench: WorkbenchName::new("run-42").unwrap(),
                    workspace_incarnation_id: WorkspaceIdentity([0x52; 16]),
                    index_path: Some(RelativePath::new("outputs").unwrap()),
                    expected_current_generation: Some(9),
                    capabilities: vec![
                        GenericIndexFieldCapability {
                            field_id: "run.labels".to_owned(),
                            operators: vec![
                                QueryOperator::Equal,
                                QueryOperator::In,
                                QueryOperator::Exists,
                            ],
                            sortable: false,
                            facetable: true,
                        },
                        GenericIndexFieldCapability {
                            field_id: "run.score".to_owned(),
                            operators: vec![
                                QueryOperator::Equal,
                                QueryOperator::Greater,
                                QueryOperator::Less,
                            ],
                            sortable: true,
                            facetable: false,
                        },
                    ],
                    declared_row_count: 0,
                },
            ),
        }
    }

    fn generic_append_request() -> WorkspaceRpcRequest {
        WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x64; 16]),
            operation: WorkspaceRequest::AppendGenericIndexRows(AppendGenericIndexRowsRequest {
                operation_id: OperationIdentity([0x60; 16]),
                first_sequence: 0,
                rows: vec![GenericIndexRow {
                    relative_path: Some(RelativePath::new("result.bin").unwrap()),
                    values: vec![
                        GenericIndexFieldValues {
                            field_id: "run.labels".to_owned(),
                            values: vec![
                                ScalarValue::String("alpha".to_owned()),
                                ScalarValue::String("alpha".to_owned()),
                            ],
                        },
                        GenericIndexFieldValues {
                            field_id: "run.score".to_owned(),
                            values: vec![
                                ScalarValue::Unsigned(7),
                                ScalarValue::Decimal("7.5".to_owned()),
                            ],
                        },
                    ],
                }],
            }),
        }
    }

    #[test]
    fn request_round_trips_with_exact_schema() {
        assert_eq!(WORKSPACE_PROTOCOL_SCHEMA, "nokv.workspace.rpc.v10");
        let expected = request();
        let encoded = encode_request(&expected).unwrap();
        assert!(encoded
            .windows(WORKSPACE_PROTOCOL_SCHEMA.len())
            .any(|window| window == WORKSPACE_PROTOCOL_SCHEMA.as_bytes()));
        assert_eq!(decode_request(&encoded).unwrap(), expected);
    }

    #[test]
    fn discovery_and_workspace_use_distinct_top_level_envelopes() {
        let discovery = RpcRequest::DiscoverRoute(crate::DiscoverRouteRequest {
            root_id: route().root_id,
        });
        let encoded = super::encode_request(&discovery).unwrap();
        assert_eq!(super::decode_request(&encoded).unwrap(), discovery);

        let discovered = crate::DiscoveredRoute::new(
            route(),
            13,
            crate::OwnerEndpoint::new("127.0.0.1:7750").unwrap(),
            crate::RouteState::Serving,
        )
        .unwrap();
        let response = RpcResponse::DiscoverRoute(crate::DiscoverRouteResponse {
            root_id: discovered.root_id,
            outcome: crate::DiscoverRouteOutcome::Found(discovered),
        });
        let encoded = super::encode_response(&response).unwrap();
        assert_eq!(super::decode_response(&encoded).unwrap(), response);
    }

    #[test]
    fn discovery_rejects_nonserving_routes_and_zero_sessions() {
        let mut discovered = crate::DiscoveredRoute {
            root_id: route().root_id,
            logical_shard_id: route().logical_shard_id,
            object_namespace_id: route().object_namespace_id,
            placement_generation: route().placement_generation,
            owner_epoch: route().owner_epoch,
            session_generation: 0,
            owner_endpoint: crate::OwnerEndpoint::new("127.0.0.1:7750").unwrap(),
            route_state: crate::RouteState::Serving,
        };
        assert!(discovered.validate().is_err());
        discovered.session_generation = 1;
        discovered.route_state = crate::RouteState::Activating;
        assert!(discovered.validate().is_err());
        assert!(crate::OwnerEndpoint::new("127.0.0.1:0").is_err());
        assert!(crate::OwnerEndpoint::new("localhost:7750").is_err());
    }

    #[test]
    fn get_path_read_version_fence_has_one_exact_v10_encoding() {
        let expected = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x41; 16]),
            operation: WorkspaceRequest::GetPath(GetPathRequest {
                target: WorkspacePath {
                    workbench: WorkbenchName::new("run-42").unwrap(),
                    path: RelativePath::new("outputs/result.bin").unwrap(),
                },
                view: WorkspaceReadView::Live,
                expected_read_version: Some(41),
                range: None,
                plan_page: None,
                if_none_match: None,
            }),
        };

        let encoded = encode_request(&expected).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), expected);
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(&encoded)),
            [
                246, 253, 30, 111, 243, 183, 7, 10, 201, 73, 139, 236, 187, 29, 123, 15, 24, 252,
                147, 204, 11, 233, 103, 147, 139, 227, 2, 166, 239, 11, 95, 132,
            ],
            "update only for an intentional GetPath v10 wire change"
        );
    }

    #[test]
    fn artifact_v1_query_and_catalog_keep_one_exact_v10_encoding() {
        let search = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x3a; 16]),
            operation: WorkspaceRequest::Search(SearchRequest {
                profile: QueryProfile::ArtifactV1,
                scope: QueryScope::Workspace {
                    workbench: WorkbenchName::new("run-42").unwrap(),
                    path_prefix: Some(RelativePath::new("outputs").unwrap()),
                },
                predicates: vec![QueryPredicate {
                    field_id: "producer".to_owned(),
                    operator: QueryOperator::Equal,
                    operand: QueryOperand::Scalar(ScalarValue::String("agent".to_owned())),
                }],
                projection: vec!["generation".to_owned()],
                sort: vec![SortField {
                    field_id: "generation".to_owned(),
                    direction: SortDirection::Descending,
                }],
                facets: vec!["content_type".to_owned()],
                page: PageRequest {
                    cursor: None,
                    limit: 7,
                },
            }),
        };
        let search_response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x3a; 16]),
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Search(
                SearchResult {
                    hits: vec![SearchRow::Artifact(SearchHit {
                        metadata: PathMetadata {
                            path: WorkspacePath {
                                workbench: WorkbenchName::new("run-42").unwrap(),
                                path: RelativePath::new("outputs/result.bin").unwrap(),
                            },
                            workspace_incarnation_id: WorkspaceIdentity([0x3b; 16]),
                            workspace_revision: 3,
                            generation: 2,
                            artifact_revision_id: ArtifactRevisionIdentity([0x3c; 16]),
                            dependency_count: 0,
                            dependency_depth: 0,
                            descriptor: ArtifactDescriptor {
                                logical_size: 7,
                                body_digest: DigestUri::new(format!("sha256:{}", "3d".repeat(32)))
                                    .unwrap(),
                                manifest_digest: DigestUri::new(format!(
                                    "sha256:{}",
                                    "3e".repeat(32)
                                ))
                                .unwrap(),
                                content_type: ContentType::new("application/octet-stream").unwrap(),
                                producer: Some("agent".to_owned()),
                                manifest_identity: None,
                                index_fields: Vec::new(),
                            },
                        },
                        projection: vec![FieldValue {
                            field_id: "generation".to_owned(),
                            value: ScalarValue::Unsigned(2),
                        }],
                    })],
                    match_count: 1,
                    facets: Vec::new(),
                    next_cursor: None,
                    read_version: 41,
                },
            ))),
        };
        let catalog_response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x3f; 16]),
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Catalog(
                CatalogResult {
                    fields: vec![CatalogField {
                        field_id: "generation".to_owned(),
                        scalar_type: "unsigned".to_owned(),
                        scalar_types: Vec::new(),
                        generic_custom: false,
                        operators: vec![QueryOperator::Equal, QueryOperator::Greater],
                        sortable: true,
                        facetable: true,
                        aggregatable: true,
                    }],
                    facets: Vec::new(),
                    next_cursor: None,
                    read_version: 41,
                },
            ))),
        };

        let encoded_search = encode_request(&search).unwrap();
        let encoded_search_response = encode_response(&search_response).unwrap();
        let encoded_catalog_response = encode_response(&catalog_response).unwrap();
        assert!(!encoded_catalog_response
            .windows(b"scalar_types".len())
            .any(|window| window == b"scalar_types"));
        assert!(!encoded_catalog_response
            .windows(b"generic_custom".len())
            .any(|window| window == b"generic_custom"));
        let mut golden = Sha256::new();
        for encoded in [
            &encoded_search,
            &encoded_search_response,
            &encoded_catalog_response,
        ] {
            golden.update((encoded.len() as u64).to_be_bytes());
            golden.update(encoded);
        }
        assert_eq!(
            <[u8; 32]>::from(golden.finalize()),
            [
                34, 20, 63, 219, 1, 231, 247, 108, 66, 46, 37, 71, 56, 101, 26, 144, 21, 199, 64,
                67, 14, 118, 86, 137, 67, 140, 123, 246, 230, 132, 228, 64,
            ],
            "update only for an intentional ArtifactV1 v10 wire change"
        );
        assert_eq!(decode_request(&encoded_search).unwrap(), search);
        assert_eq!(
            decode_response(&encoded_search_response).unwrap(),
            search_response
        );
        assert_eq!(
            decode_response(&encoded_catalog_response).unwrap(),
            catalog_response
        );
    }

    #[test]
    fn generic_exact_catalog_path_match_has_one_exact_v10_encoding() {
        let expected = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x42; 16]),
            operation: WorkspaceRequest::Catalog(CatalogRequest {
                profile: crate::QueryProfile::GenericCustomIndexV1 {
                    presentation_path_root: "/agents".to_owned(),
                },
                scope: crate::QueryScope::Workspace {
                    workbench: WorkbenchName::new("run-42").unwrap(),
                    path_prefix: Some(RelativePath::new("outputs/result.bin").unwrap()),
                },
                path_match: CatalogPathMatch::Exact,
                field_prefix: Some("run.".to_owned()),
                include_facets: true,
                page: PageRequest {
                    cursor: None,
                    limit: 7,
                },
            }),
        };

        let encoded = encode_request(&expected).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), expected);
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(&encoded)),
            [
                77, 102, 45, 236, 123, 111, 26, 16, 81, 249, 215, 136, 95, 66, 99, 192, 134, 120,
                69, 15, 51, 130, 237, 27, 59, 225, 222, 245, 196, 212, 109, 167,
            ],
            "update only for an intentional Catalog Exact v10 wire change"
        );
    }

    #[test]
    fn generic_registration_surface_has_one_exact_v10_encoding() {
        let begin = generic_begin_request();
        let append = generic_append_request();
        let finalize = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x65; 16]),
            operation: WorkspaceRequest::FinalizeGenericIndexRegistration(
                FinalizeGenericIndexRegistrationRequest {
                    operation_id: OperationIdentity([0x50; 16]),
                },
            ),
        };
        let abort = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x66; 16]),
            operation: WorkspaceRequest::AbortGenericIndexRegistration(
                AbortGenericIndexRegistrationRequest {
                    operation_id: OperationIdentity([0x60; 16]),
                    limit: 120,
                },
            ),
        };
        let get = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x67; 16]),
            operation: WorkspaceRequest::GetGenericIndexRegistration(
                GetGenericIndexRegistrationRequest {
                    operation_id: OperationIdentity([0x50; 16]),
                },
            ),
        };

        let complete = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x54; 16]),
            commit_version: Some(101),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(
                WorkspaceResult::GenericIndexRegistration(generic_registration_status(
                    0x50,
                    0x51,
                    GenericIndexRegistrationPhase::Complete,
                )),
            )),
        };
        let mut append_status =
            generic_registration_status(0x60, 0x61, GenericIndexRegistrationPhase::Appending);
        append_status.workspace_incarnation_id = WorkspaceIdentity([0x62; 16]);
        append_status.index_path = None;
        append_status.expected_current_generation = None;
        append_status.capability_digest = Digest([0x63; 32]);
        append_status.declared_row_count = 2;
        append_status.appended_row_count = 1;
        append_status.row_digest = Digest([0x68; 32]);
        append_status.last_transition_version = 102;
        let append_result = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x64; 16]),
            commit_version: Some(102),
            replayed: true,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::GenericIndexAppend(
                GenericIndexAppendResult {
                    registration: append_status.clone(),
                    receipt: GenericIndexAppendReceipt {
                        first_sequence: 0,
                        row_count: 1,
                        commit_version: 102,
                        input_digest: Digest([0x69; 32]),
                        resulting_row_count: 1,
                        resulting_row_digest: Digest([0x68; 32]),
                    },
                },
            ))),
        };
        let mut abort_status = append_status;
        abort_status.last_transition_version = 103;
        abort_status.phase = GenericIndexRegistrationPhase::Cleaning;
        let abort_result = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x66; 16]),
            commit_version: Some(103),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::GenericIndexAbort(
                GenericIndexAbortResult {
                    registration: abort_status,
                    removed_rows: 1,
                    removed_receipts: 0,
                    cleanup_complete: false,
                },
            ))),
        };

        let mut golden = Sha256::new();
        for encoded in [
            encode_request(&begin).unwrap(),
            encode_request(&append).unwrap(),
            encode_request(&finalize).unwrap(),
            encode_request(&abort).unwrap(),
            encode_request(&get).unwrap(),
            encode_response(&complete).unwrap(),
            encode_response(&append_result).unwrap(),
            encode_response(&abort_result).unwrap(),
        ] {
            golden.update((encoded.len() as u64).to_be_bytes());
            golden.update(&encoded);
        }

        assert_eq!(
            <[u8; 32]>::from(golden.finalize()),
            [
                165, 5, 250, 152, 58, 248, 106, 79, 166, 247, 84, 90, 144, 29, 249, 142, 73, 227,
                41, 163, 226, 133, 248, 169, 61, 159, 138, 196, 189, 3, 10, 4,
            ],
            "update only for an intentional Generic registration v10 wire change"
        );
        for request in [&begin, &append, &finalize, &abort, &get] {
            assert_eq!(
                decode_request(&encode_request(request).unwrap()).unwrap(),
                *request
            );
        }
        for response in [&complete, &append_result, &abort_result] {
            assert_eq!(
                decode_response(&encode_response(response).unwrap()).unwrap(),
                *response
            );
        }
    }

    #[test]
    fn generic_registration_codec_rejects_tampered_catalog_rows_receipts_and_aba_fields() {
        let mut unsorted_catalog = generic_begin_request();
        let WorkspaceRequest::BeginGenericIndexRegistration(begin) =
            &mut unsorted_catalog.operation
        else {
            unreachable!();
        };
        begin.capabilities.swap(0, 1);
        let encoded = encode_unvalidated_request(&unsorted_catalog).unwrap();
        assert!(matches!(
            decode_request(&encoded),
            Err(ProtocolError::InvalidField {
                field: "generic_index.capabilities",
                ..
            })
        ));

        let mut duplicate_operator = generic_begin_request();
        let WorkspaceRequest::BeginGenericIndexRegistration(begin) =
            &mut duplicate_operator.operation
        else {
            unreachable!();
        };
        begin.capabilities[0]
            .operators
            .insert(1, QueryOperator::Equal);
        assert!(decode_request(&encode_unvalidated_request(&duplicate_operator).unwrap()).is_err());

        let mut unsupported_value = generic_append_request();
        let WorkspaceRequest::AppendGenericIndexRows(append) = &mut unsupported_value.operation
        else {
            unreachable!();
        };
        append.rows[0].values[0].values[0] = ScalarValue::Boolean(true);
        assert!(decode_request(&encode_unvalidated_request(&unsupported_value).unwrap()).is_err());

        let mut oversized_row = generic_append_request();
        let WorkspaceRequest::AppendGenericIndexRows(append) = &mut oversized_row.operation else {
            unreachable!();
        };
        append.rows[0].values[0].values = vec![ScalarValue::String(
            "x".repeat(crate::MAX_GENERIC_INDEX_ROW_BYTES),
        )];
        assert!(matches!(
            decode_request(&encode_unvalidated_request(&oversized_row).unwrap()),
            Err(ProtocolError::InvalidField {
                field: "generic_index.row",
                ..
            })
        ));

        let mut aba =
            generic_registration_status(0x50, 0x51, GenericIndexRegistrationPhase::Complete);
        aba.published_pointer_generation = Some(11);
        let response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x54; 16]),
            commit_version: Some(101),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(
                WorkspaceResult::GenericIndexRegistration(aba),
            )),
        };
        assert!(decode_response(&encode_unvalidated_response(&response).unwrap()).is_err());

        let mut invalid_empty_closure =
            generic_registration_status(0x50, 0x51, GenericIndexRegistrationPhase::Appending);
        invalid_empty_closure.row_digest = Digest([1; 32]);
        let response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x54; 16]),
            commit_version: Some(101),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(
                WorkspaceResult::GenericIndexRegistration(invalid_empty_closure),
            )),
        };
        assert!(matches!(
            decode_response(&encode_unvalidated_response(&response).unwrap()),
            Err(ProtocolError::InvalidField {
                field: "generic_index.row_digest",
                ..
            })
        ));

        let mut append_status =
            generic_registration_status(0x60, 0x61, GenericIndexRegistrationPhase::Appending);
        append_status.declared_row_count = 2;
        append_status.appended_row_count = 1;
        append_status.row_digest = Digest([0x68; 32]);
        append_status.last_transition_version = 102;
        let response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x64; 16]),
            commit_version: Some(102),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::GenericIndexAppend(
                GenericIndexAppendResult {
                    registration: append_status,
                    receipt: GenericIndexAppendReceipt {
                        first_sequence: 0,
                        row_count: 1,
                        commit_version: 102,
                        input_digest: Digest([0x69; 32]),
                        resulting_row_count: 2,
                        resulting_row_digest: Digest([0x68; 32]),
                    },
                },
            ))),
        };
        assert!(decode_response(&encode_unvalidated_response(&response).unwrap()).is_err());
    }

    #[test]
    fn generic_catalog_zero_row_and_multivalue_types_are_explicit_and_canonical() {
        let zero_row = CatalogField {
            field_id: "run.score".to_owned(),
            scalar_type: "string".to_owned(),
            scalar_types: Vec::new(),
            generic_custom: true,
            operators: Vec::new(),
            sortable: false,
            facetable: false,
            aggregatable: false,
        };
        zero_row.validate().unwrap();

        let mut mixed = CatalogField {
            field_id: "run.score".to_owned(),
            scalar_type: "unsigned".to_owned(),
            scalar_types: vec![
                "unsigned".to_owned(),
                "float".to_owned(),
                "string".to_owned(),
            ],
            generic_custom: true,
            operators: vec![QueryOperator::Equal, QueryOperator::Greater],
            sortable: true,
            facetable: true,
            aggregatable: true,
        };
        mixed.validate().unwrap();

        let response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x6a; 16]),
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Catalog(
                CatalogResult {
                    fields: vec![zero_row, mixed.clone()],
                    facets: Vec::new(),
                    next_cursor: Some(vec![0x6b; 32]),
                    read_version: 41,
                },
            ))),
        };
        assert_eq!(
            decode_response(&encode_response(&response).unwrap()).unwrap(),
            response
        );

        mixed.scalar_types.swap(0, 1);
        assert!(mixed.validate().is_err());
    }

    #[test]
    fn generic_namespace_search_rows_round_trip_and_validate_under_v10() {
        let response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x43; 16]),
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Search(
                SearchResult {
                    hits: vec![
                        SearchRow::GenericNamespace(GenericNamespaceHit {
                            workbench: WorkbenchName::new("run-42").unwrap(),
                            relative_path: Some(RelativePath::new("outputs/dir").unwrap()),
                            kind: GenericNamespaceKind::Directory,
                            artifact: None,
                            projection: vec![FieldValue {
                                field_id: "kind".to_owned(),
                                value: ScalarValue::String("directory".to_owned()),
                            }],
                            indexed_values: Vec::new(),
                        }),
                        SearchRow::GenericNamespace(GenericNamespaceHit {
                            workbench: WorkbenchName::new("run-42").unwrap(),
                            relative_path: Some(
                                RelativePath::new("outputs/dir/result.bin").unwrap(),
                            ),
                            kind: GenericNamespaceKind::Artifact,
                            artifact: Some(GenericNamespaceArtifact {
                                generation: 1,
                                logical_size: 7,
                                body_digest: DigestUri::new(format!("sha256:{}", "44".repeat(32)))
                                    .unwrap(),
                                content_type: ContentType::new("application/octet-stream").unwrap(),
                                producer: Some("agent".to_owned()),
                                manifest_identity: Some("manifest-1".to_owned()),
                            }),
                            projection: vec![FieldValue {
                                field_id: "size_bytes".to_owned(),
                                value: ScalarValue::Unsigned(7),
                            }],
                            indexed_values: vec![GenericIndexFieldValues {
                                field_id: "run.score".to_owned(),
                                values: vec![
                                    ScalarValue::Unsigned(7),
                                    ScalarValue::Unsigned(7),
                                    ScalarValue::Decimal("7.5".to_owned()),
                                ],
                            }],
                        }),
                    ],
                    match_count: 2,
                    facets: vec![FacetResult {
                        field_id: "kind".to_owned(),
                        buckets: vec![FacetBucket {
                            value: ScalarValue::String("directory".to_owned()),
                            count: 1,
                        }],
                        distinct_count: 2,
                        truncated: true,
                    }],
                    next_cursor: Some(vec![0x44; 32]),
                    read_version: 41,
                },
            ))),
        };

        assert_eq!(
            decode_response(&encode_response(&response).unwrap()).unwrap(),
            response
        );

        let mut invalid = response;
        let WorkspaceRpcOutcome::Success(result) = &mut invalid.outcome else {
            unreachable!("fixture is a successful response");
        };
        let WorkspaceResult::Search(search) = result.as_mut() else {
            unreachable!("fixture is a search response");
        };
        let SearchRow::GenericNamespace(hit) = &mut search.hits[1] else {
            unreachable!("fixture contains generic namespace rows");
        };
        hit.artifact = None;
        assert!(encode_response(&invalid).is_err());
    }

    #[test]
    fn rename_path_request_and_result_round_trip_under_v10() {
        let source = WorkspacePath {
            workbench: WorkbenchName::new("run-42").unwrap(),
            path: RelativePath::new("outputs/a.bin").unwrap(),
        };
        let destination = WorkspacePath {
            workbench: source.workbench.clone(),
            path: RelativePath::new("outputs/b.bin").unwrap(),
        };
        let request = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x0a; 16]),
            operation: WorkspaceRequest::RenamePath(RenamePathRequest {
                source: source.clone(),
                destination: destination.clone(),
                expected_generation: 7,
            }),
        };
        assert_eq!(
            decode_request(&encode_request(&request).unwrap()).unwrap(),
            request
        );

        let response = WorkspaceRpcResponse {
            route: route(),
            request_id: request.request_id,
            commit_version: Some(11),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Renamed(
                RenamePathResult {
                    source,
                    destination,
                    workspace_revision: 3,
                    generation: 7,
                    artifact_revision_id: ArtifactRevisionIdentity([0x0b; 16]),
                },
            ))),
        };
        assert_eq!(
            decode_response(&encode_response(&response).unwrap()).unwrap(),
            response
        );
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
    fn restore_v10_late_binding_and_source_manifest_read_round_trip() {
        let restore_identity = RestoreManifestIdentity {
            publication_operation_id: OperationIdentity([0x21; 16]),
            artifact_revision_id: ArtifactRevisionIdentity([0x22; 16]),
        };
        let prepare = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x23; 16]),
            operation: WorkspaceRequest::PrepareRestore(PrepareRestoreRequest {
                operation_id: OperationIdentity([0x24; 16]),
                source_workbench: WorkbenchName::new("source").unwrap(),
                source_workspace_incarnation_id: WorkspaceIdentity([0x25; 16]),
                source: RestoreSource::Snapshot(SnapshotSelector::Id(7)),
                destination_workbench: WorkbenchName::new("destination").unwrap(),
                destination_workspace_incarnation_id: WorkspaceIdentity([0x26; 16]),
                destination_restore_manifest_identity: restore_identity,
                restore_manifest: RestoreManifestDescriptor {
                    body_digest: DigestUri::new(format!("sha256:{}", "27".repeat(32))).unwrap(),
                    logical_size: 128,
                    content_type: ContentType::new("application/json").unwrap(),
                },
            }),
        };
        let encoded_prepare = encode_request(&prepare).unwrap();
        assert_eq!(decode_request(&encoded_prepare).unwrap(), prepare);
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(&encoded_prepare)),
            [
                217, 135, 250, 74, 147, 11, 191, 151, 86, 139, 126, 68, 40, 249, 107, 179, 242, 18,
                20, 236, 219, 214, 58, 148, 143, 54, 167, 189, 163, 222, 86, 67,
            ],
            "update only for an intentional restore-v10 wire change"
        );

        let bind = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x28; 16]),
            operation: WorkspaceRequest::BindRestoreDestination(BindRestoreDestinationRequest {
                operation_id: OperationIdentity([0x24; 16]),
                destination_commit_id: CommitIdentity([0x29; 32]),
                effective_content_digest: DigestUri::new(format!("sha256:{}", "2a".repeat(32)))
                    .unwrap(),
                destination_run_manifest_projection_input_digest: Digest([0x2b; 32]),
                destination_run_manifest_identity: RestoreManifestIdentity {
                    publication_operation_id: OperationIdentity([0x2c; 16]),
                    artifact_revision_id: ArtifactRevisionIdentity([0x2d; 16]),
                },
                destination_restore_manifest_identity: restore_identity,
            }),
        };
        let encoded_bind = encode_request(&bind).unwrap();
        assert_eq!(decode_request(&encoded_bind).unwrap(), bind);

        let read = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([0x2e; 16]),
            operation: WorkspaceRequest::ReadRestoreSourceRunManifest(
                ReadRestoreSourceRunManifestRequest {
                    operation_id: OperationIdentity([0x24; 16]),
                    range: None,
                    plan_page: None,
                },
            ),
        };
        let encoded_read = encode_request(&read).unwrap();
        assert_eq!(decode_request(&encoded_read).unwrap(), read);

        let prepared = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x23; 16]),
            commit_version: Some(19),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::RestorePrepared(
                RestorePreparation {
                    operation_id: OperationIdentity([0x24; 16]),
                    destination_workbench: WorkbenchName::new("destination").unwrap(),
                    destination_workspace_incarnation_id: WorkspaceIdentity([0x26; 16]),
                    source_commit: RestoreSourceCommitBinding {
                        commit_id: CommitIdentity([0x2f; 32]),
                        content_digest: DigestUri::new(format!("sha256:{}", "30".repeat(32)))
                            .unwrap(),
                        manifest_digest: DigestUri::new(format!("sha256:{}", "31".repeat(32)))
                            .unwrap(),
                        tree_manifest_revision_id: ArtifactRevisionIdentity([0x32; 16]),
                        member_count: 1,
                        member_digest: Digest([0x35; 32]),
                    },
                    destination_committed_at_unix_seconds: 1_700_000_000,
                    source_member_count: 1,
                    source_member_digest: Digest([0x35; 32]),
                    materialized_member_count: 0,
                    materialized_member_digest: Digest([0; 32]),
                    source_matches_base_commit: true,
                    destination_binding: None,
                },
            ))),
        };
        let encoded_prepared = encode_response(&prepared).unwrap();
        assert_eq!(decode_response(&encoded_prepared).unwrap(), prepared);

        let source_manifest = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([0x2e; 16]),
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(
                WorkspaceResult::RestoreSourceRunManifest(PathReadResult {
                    not_modified: false,
                    metadata: Some(PathMetadata {
                        path: WorkspacePath {
                            workbench: WorkbenchName::new("source").unwrap(),
                            path: RelativePath::new("metadata/run_manifest.json").unwrap(),
                        },
                        workspace_incarnation_id: WorkspaceIdentity([0x25; 16]),
                        workspace_revision: 7,
                        generation: 1,
                        artifact_revision_id: ArtifactRevisionIdentity([0x32; 16]),
                        dependency_count: 0,
                        dependency_depth: 0,
                        descriptor: ArtifactDescriptor {
                            logical_size: 128,
                            body_digest: DigestUri::new(format!("sha256:{}", "33".repeat(32)))
                                .unwrap(),
                            manifest_digest: DigestUri::new(format!("sha256:{}", "34".repeat(32)))
                                .unwrap(),
                            content_type: ContentType::new("application/json").unwrap(),
                            producer: None,
                            manifest_identity: None,
                            index_fields: Vec::new(),
                        },
                    }),
                    range: None,
                    blocks: Vec::new(),
                    next_cursor: None,
                }),
            )),
        };
        let encoded_source_manifest = encode_response(&source_manifest).unwrap();
        assert_eq!(
            decode_response(&encoded_source_manifest).unwrap(),
            source_manifest
        );

        let mut complete_v10_golden = Sha256::new();
        for encoded in [
            encoded_prepare,
            encoded_bind,
            encoded_read,
            encoded_prepared,
            encoded_source_manifest,
        ] {
            complete_v10_golden.update((encoded.len() as u64).to_be_bytes());
            complete_v10_golden.update(encoded);
        }
        assert_eq!(
            <[u8; 32]>::from(complete_v10_golden.finalize()),
            [
                117, 212, 171, 155, 122, 234, 7, 47, 63, 111, 69, 230, 234, 116, 106, 86, 224, 82,
                125, 12, 240, 113, 15, 162, 170, 112, 120, 10, 40, 123, 124, 54,
            ],
            "update only for an intentional restore-v10 wire change"
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
    fn live_list_workspace_continuation_fence_round_trips() {
        let expected = WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([9; 16]),
            operation: WorkspaceRequest::ListPaths(ListPathsRequest {
                workbench: WorkbenchName::new("run-42").unwrap(),
                prefix: Some(RelativePath::new("outputs").unwrap()),
                recursive: true,
                view: WorkspaceReadView::Live,
                expected_read_version: None,
                workspace_continuation_fence: Some(WorkspaceContinuationFence {
                    workspace_incarnation_id: WorkspaceIdentity([4; 16]),
                    workspace_revision: 7,
                }),
                page: PageRequest {
                    cursor: Some(b"outputs/a".to_vec()),
                    limit: 10,
                },
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
            schema: "nokv.workspace.rpc.v8".to_owned(),
            payload: request(),
        })
        .unwrap();
        assert_eq!(
            decode_request(&encoded),
            Err(ProtocolError::SchemaMismatch {
                actual: "nokv.workspace.rpc.v8".to_owned(),
                expected: "nokv.workspace.rpc.v10",
            })
        );

        #[derive(Serialize)]
        #[serde(deny_unknown_fields)]
        struct V4PrepareRestoreRequest {
            source_workbench: WorkbenchName,
            source_workspace_incarnation_id: WorkspaceIdentity,
            source: RestoreSource,
            destination_workbench: WorkbenchName,
            destination_workspace_incarnation_id: WorkspaceIdentity,
            restore_manifest: RestoreManifestDescriptor,
        }

        #[derive(Serialize)]
        #[serde(tag = "operation", content = "request", rename_all = "snake_case")]
        enum V4WorkspaceRequest {
            PrepareRestore(V4PrepareRestoreRequest),
        }

        #[derive(Serialize)]
        #[serde(deny_unknown_fields)]
        struct V4WorkspaceRpcRequest {
            route: RootRoute,
            request_id: RequestIdentity,
            operation: V4WorkspaceRequest,
        }

        let actual_v4 = rmp_serde::to_vec_named(&Frame {
            schema: "nokv.workspace.rpc.v4".to_owned(),
            payload: V4WorkspaceRpcRequest {
                route: route(),
                request_id: RequestIdentity([0x41; 16]),
                operation: V4WorkspaceRequest::PrepareRestore(V4PrepareRestoreRequest {
                    source_workbench: WorkbenchName::new("source").unwrap(),
                    source_workspace_incarnation_id: WorkspaceIdentity([0x42; 16]),
                    source: RestoreSource::Snapshot(SnapshotSelector::Id(7)),
                    destination_workbench: WorkbenchName::new("destination").unwrap(),
                    destination_workspace_incarnation_id: WorkspaceIdentity([0x43; 16]),
                    restore_manifest: RestoreManifestDescriptor {
                        body_digest: DigestUri::new(format!("sha256:{}", "44".repeat(32))).unwrap(),
                        logical_size: 128,
                        content_type: ContentType::new("application/json").unwrap(),
                    },
                }),
            },
        })
        .unwrap();
        assert_eq!(
            decode_request(&actual_v4),
            Err(ProtocolError::SchemaMismatch {
                actual: "nokv.workspace.rpc.v4".to_owned(),
                expected: "nokv.workspace.rpc.v10",
            })
        );

        let response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([3; 16]),
            commit_version: None,
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
        let encoded = rmp_serde::to_vec_named(&Frame {
            schema: "nokv.workspace.rpc.v4".to_owned(),
            payload: response,
        })
        .unwrap();
        assert_eq!(
            decode_response(&encoded),
            Err(ProtocolError::SchemaMismatch {
                actual: "nokv.workspace.rpc.v4".to_owned(),
                expected: "nokv.workspace.rpc.v10",
            })
        );
    }

    #[test]
    fn v6_through_v9_are_rejected_before_zero_payload_dispatch() {
        for schema in [
            "nokv.workspace.rpc.v6",
            "nokv.workspace.rpc.v7",
            "nokv.workspace.rpc.v8",
            "nokv.workspace.rpc.v9",
        ] {
            let encoded = rmp_serde::to_vec_named(&Frame {
                schema: schema.to_owned(),
                payload: 0_u8,
            })
            .unwrap();
            assert_eq!(
                decode_request(&encoded),
                Err(ProtocolError::SchemaMismatch {
                    actual: schema.to_owned(),
                    expected: WORKSPACE_PROTOCOL_SCHEMA,
                })
            );
            assert_eq!(
                decode_response(&encoded),
                Err(ProtocolError::SchemaMismatch {
                    actual: schema.to_owned(),
                    expected: WORKSPACE_PROTOCOL_SCHEMA,
                })
            );
        }
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
                WorkspaceCapability::GenericCustomIndexV1,
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
            vec![
                WorkspaceCapability::GenericCustomIndexV1,
                WorkspaceCapability::QueryV1,
                WorkspaceCapability::RestoreV1
            ]
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
                    [
                        WorkspaceCapability::RestoreV1,
                        WorkspaceCapability::GenericCustomIndexV1,
                        WorkspaceCapability::QueryV1,
                    ],
                ),
            ))),
        };
        assert_eq!(
            decode_response(&encode_response(&response).unwrap()).unwrap(),
            response
        );
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(encode_request(&expected).unwrap())),
            [
                222, 84, 135, 253, 77, 252, 32, 108, 186, 186, 73, 14, 245, 144, 24, 33, 15, 37,
                194, 180, 96, 219, 141, 39, 103, 33, 240, 169, 19, 236, 2, 108,
            ],
            "update only for an intentional capability-v2 wire change"
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

        let mut v1 = WorkspacePreflightRequest::new([WorkspaceCapability::QueryV1]);
        v1.capability_schema = "nokv.workspace.rpc_capabilities.v1".to_owned();
        invalid.operation = WorkspaceRequest::Preflight(v1);
        assert!(matches!(
            decode_request(&encode_unvalidated_request(&invalid).unwrap()),
            Err(ProtocolError::InvalidField {
                field: "preflight.capability_schema",
                ..
            })
        ));

        let mut v1_result =
            WorkspacePreflightResult::new(route(), [WorkspaceCapability::GenericCustomIndexV1]);
        v1_result.capability_schema = "nokv.workspace.rpc_capabilities.v1".to_owned();
        let v1_response = WorkspaceRpcResponse {
            route: route(),
            request_id: RequestIdentity([8; 16]),
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Preflight(v1_result))),
        };
        assert!(matches!(
            decode_response(&encode_unvalidated_response(&v1_response).unwrap()),
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
