/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io::Cursor;

use nokv_protocol::{
    encode_response, ErrorCode, RequestIdentity, RootRoute, RpcFailure, RpcResponse,
    WorkspaceRpcOutcome, WorkspaceRpcResponse,
};
use serde::{de::IgnoredAny, Deserialize, Serialize};

pub(crate) const LEGACY_V2_SCHEMA: &str = "nokv.workspace.rpc.v2";
pub(crate) const LEGACY_V3_SCHEMA: &str = "nokv.workspace.rpc.v3";
pub(crate) const LEGACY_CLIENT_UPGRADE_MESSAGE: &str =
    "this NoKV client uses an unsupported workspace RPC schema; upgrade the NoKV client";
pub(crate) const MAX_LEGACY_FIRST_FRAME_BYTES: usize = 64 * 1024;

const LEGACY_DECODE_MAX_DEPTH: usize = 32;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaEnvelope {
    schema: String,
    #[serde(rename = "payload")]
    _payload: IgnoredAny,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V2RequestEnvelope {
    schema: String,
    payload: V2RequestHeader,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V2RequestHeader {
    route: V2RootRoute,
    request_id: [u8; 16],
    operation: IgnoredAny,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct V2RootRoute {
    root_id: [u8; 16],
    logical_shard_id: [u8; 16],
    placement_generation: u64,
    owner_epoch: u64,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct V2ResponseEnvelope {
    schema: &'static str,
    payload: V2Response,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct V2Response {
    route: V2RootRoute,
    request_id: [u8; 16],
    commit_version: Option<u64>,
    replayed: bool,
    outcome: V2Outcome,
}

#[derive(Serialize)]
#[serde(tag = "status", content = "body", rename_all = "snake_case")]
enum V2Outcome {
    Failure(V2Failure),
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct V2Failure {
    code: V2ErrorCode,
    message: &'static str,
    retryable: bool,
    conflict: Option<()>,
    current_generation: Option<u64>,
    route_hint: Option<V2RootRoute>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum V2ErrorCode {
    PreconditionFailed,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V3RequestEnvelope {
    schema: String,
    payload: V3RequestHeader,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V3RequestHeader {
    route: RootRoute,
    request_id: RequestIdentity,
    operation: IgnoredAny,
}

/// Return a schema-readable rejection only for the two known public legacy
/// envelopes. No legacy operation is ever decoded or dispatched.
pub(crate) fn legacy_rejection_response(encoded: &[u8]) -> Option<Vec<u8>> {
    if encoded.len() > MAX_LEGACY_FIRST_FRAME_BYTES {
        return None;
    }
    let schema = decode_bounded::<SchemaEnvelope>(encoded)?.schema;
    match schema.as_str() {
        LEGACY_V2_SCHEMA => reject_v2(encoded),
        LEGACY_V3_SCHEMA => reject_v3(encoded),
        _ => None,
    }
}

fn reject_v2(encoded: &[u8]) -> Option<Vec<u8>> {
    let envelope = decode_bounded::<V2RequestEnvelope>(encoded)?;
    if envelope.schema != LEGACY_V2_SCHEMA
        || envelope.payload.route.placement_generation == 0
        || envelope.payload.route.owner_epoch == 0
    {
        return None;
    }
    let _ = envelope.payload.operation;
    rmp_serde::to_vec_named(&V2ResponseEnvelope {
        schema: LEGACY_V2_SCHEMA,
        payload: V2Response {
            route: envelope.payload.route,
            request_id: envelope.payload.request_id,
            commit_version: None,
            replayed: false,
            outcome: V2Outcome::Failure(V2Failure {
                code: V2ErrorCode::PreconditionFailed,
                message: LEGACY_CLIENT_UPGRADE_MESSAGE,
                retryable: false,
                conflict: None,
                current_generation: None,
                route_hint: None,
            }),
        },
    })
    .ok()
}

fn reject_v3(encoded: &[u8]) -> Option<Vec<u8>> {
    let envelope = decode_bounded::<V3RequestEnvelope>(encoded)?;
    if envelope.schema != LEGACY_V3_SCHEMA {
        return None;
    }
    let _ = envelope.payload.operation;
    encode_response(&RpcResponse::Workspace(Box::new(WorkspaceRpcResponse {
        route: envelope.payload.route,
        request_id: envelope.payload.request_id,
        commit_version: None,
        replayed: false,
        outcome: WorkspaceRpcOutcome::Failure(RpcFailure {
            code: ErrorCode::PreconditionFailed,
            message: LEGACY_CLIENT_UPGRADE_MESSAGE.to_owned(),
            retryable: false,
            conflict: None,
            current_generation: None,
            route_hint: None,
        }),
    })))
    .ok()
}

fn decode_bounded<'de, T>(encoded: &'de [u8]) -> Option<T>
where
    T: Deserialize<'de>,
{
    let cursor = Cursor::new(encoded);
    let mut decoder = rmp_serde::Deserializer::new(cursor);
    decoder.set_max_depth(LEGACY_DECODE_MAX_DEPTH);
    let decoded = T::deserialize(&mut decoder).ok()?;
    if decoder.position() != encoded.len() as u64 {
        return None;
    }
    Some(decoded)
}

#[cfg(test)]
mod tests {
    use nokv_protocol::{
        ErrorCode, LogicalShardIdentity, ObjectNamespaceIdentity, ProtocolError, RequestIdentity,
        RootIdentity, RootRoute, RpcResponse, WorkspaceRpcOutcome, WorkspaceRpcResponse,
        WORKSPACE_PROTOCOL_SCHEMA,
    };
    use serde::{Deserialize, Serialize};

    use super::*;

    fn decode_response(encoded: &[u8]) -> Result<WorkspaceRpcResponse, ProtocolError> {
        match nokv_protocol::decode_response(encoded)? {
            RpcResponse::Workspace(response) => Ok(*response),
            RpcResponse::DiscoverRoute(_) => Err(ProtocolError::Decode(
                "expected a workspace response".to_owned(),
            )),
        }
    }

    #[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct V2Route {
        root_id: [u8; 16],
        logical_shard_id: [u8; 16],
        placement_generation: u64,
        owner_epoch: u64,
    }

    #[derive(Serialize)]
    #[serde(deny_unknown_fields)]
    struct Frame<T> {
        schema: &'static str,
        payload: T,
    }

    #[derive(Serialize)]
    #[serde(deny_unknown_fields)]
    struct Request<Route> {
        route: Route,
        request_id: [u8; 16],
        operation: FixtureOperation,
    }

    #[derive(Serialize)]
    #[serde(deny_unknown_fields)]
    struct DeepRequest<Route> {
        route: Route,
        request_id: [u8; 16],
        operation: DeepValue,
    }

    #[derive(Serialize)]
    enum DeepValue {
        Leaf,
        Next(Box<DeepValue>),
    }

    fn deep_value(depth: usize) -> DeepValue {
        (0..depth).fold(DeepValue::Leaf, |value, _| DeepValue::Next(Box::new(value)))
    }

    #[derive(Serialize)]
    #[serde(tag = "operation", content = "request", rename_all = "snake_case")]
    enum FixtureOperation {
        CreateWorkspace {
            workbench: String,
            workspace_incarnation_id: [u8; 16],
        },
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct V2Response {
        route: V2Route,
        request_id: [u8; 16],
        commit_version: Option<u64>,
        replayed: bool,
        outcome: V2Outcome,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    #[serde(tag = "status", content = "body", rename_all = "snake_case")]
    enum V2Outcome {
        Failure(V2Failure),
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct V2Failure {
        code: V2ErrorCode,
        message: String,
        retryable: bool,
        conflict: Option<V2ConflictKind>,
        current_generation: Option<u64>,
        route_hint: Option<V2Route>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    enum V2ErrorCode {
        PreconditionFailed,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    enum V2ConflictKind {
        Workspace,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct V2ResponseFrame {
        schema: String,
        payload: V2Response,
    }

    fn v2_route() -> V2Route {
        V2Route {
            root_id: [1; 16],
            logical_shard_id: [2; 16],
            placement_generation: 7,
            owner_epoch: 11,
        }
    }

    fn v3_route() -> RootRoute {
        RootRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            object_namespace_id: ObjectNamespaceIdentity([3; 16]),
            placement_generation: 7,
            owner_epoch: 11,
        }
    }

    fn request<Route: Serialize>(schema: &'static str, route: Route, secret: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&Frame {
            schema,
            payload: Request {
                route,
                request_id: [4; 16],
                operation: FixtureOperation::CreateWorkspace {
                    workbench: secret.to_owned(),
                    workspace_incarnation_id: [5; 16],
                },
            },
        })
        .unwrap()
    }

    #[test]
    fn public_v2_gets_an_exact_v2_nonretryable_upgrade_failure() {
        let request = request(LEGACY_V2_SCHEMA, v2_route(), "private-client-input");
        let response = legacy_rejection_response(&request).unwrap();
        assert!(!response
            .windows("private-client-input".len())
            .any(|window| window == b"private-client-input"));

        let decoded: V2ResponseFrame = rmp_serde::from_slice(&response).unwrap();
        assert_eq!(decoded.schema, LEGACY_V2_SCHEMA);
        assert_eq!(decoded.payload.route, v2_route());
        assert_eq!(decoded.payload.request_id, [4; 16]);
        assert_eq!(decoded.payload.commit_version, None);
        assert!(!decoded.payload.replayed);
        assert_eq!(
            decoded.payload.outcome,
            V2Outcome::Failure(V2Failure {
                code: V2ErrorCode::PreconditionFailed,
                message: LEGACY_CLIENT_UPGRADE_MESSAGE.to_owned(),
                retryable: false,
                conflict: None,
                current_generation: None,
                route_hint: None,
            })
        );
    }

    #[test]
    fn post_tag_v3_gets_a_v10_failure_that_its_decoder_reports_as_schema_mismatch() {
        let route = v3_route();
        let request = request(LEGACY_V3_SCHEMA, route, "run-42");
        let response = legacy_rejection_response(&request).unwrap();
        let decoded = decode_response(&response).unwrap();
        assert_eq!(decoded.route, route);
        assert_eq!(decoded.request_id, RequestIdentity([4; 16]));
        assert_eq!(decoded.commit_version, None);
        assert!(!decoded.replayed);
        let WorkspaceRpcOutcome::Failure(failure) = decoded.outcome else {
            panic!("legacy rejection must not report success");
        };
        assert_eq!(failure.code, ErrorCode::PreconditionFailed);
        assert_eq!(failure.message, LEGACY_CLIENT_UPGRADE_MESSAGE);
        assert!(!failure.retryable);

        #[derive(Deserialize)]
        struct V3TypedResponseFrame {
            schema: String,
            payload: RpcResponse,
        }
        let envelope: V3TypedResponseFrame = rmp_serde::from_slice(&response).unwrap();
        let RpcResponse::Workspace(payload) = envelope.payload else {
            panic!("legacy workspace request must receive a workspace response");
        };
        assert_eq!(payload.route, route);
        assert_eq!(envelope.schema, WORKSPACE_PROTOCOL_SCHEMA);
        let mismatch = ProtocolError::SchemaMismatch {
            actual: envelope.schema,
            expected: LEGACY_V3_SCHEMA,
        };
        assert!(matches!(
            mismatch,
            ProtocolError::SchemaMismatch {
                actual,
                expected: LEGACY_V3_SCHEMA,
            } if actual == WORKSPACE_PROTOCOL_SCHEMA
        ));
    }

    #[test]
    fn unknown_malformed_and_trailing_frames_are_closed_without_a_response() {
        assert!(
            legacy_rejection_response(&request("nokv.workspace.rpc.v4", v3_route(), "run-42"))
                .is_none()
        );
        assert!(legacy_rejection_response(b"not messagepack").is_none());

        let malformed = rmp_serde::to_vec_named(&Frame {
            schema: LEGACY_V3_SCHEMA,
            payload: (),
        })
        .unwrap();
        assert!(legacy_rejection_response(&malformed).is_none());

        let mut trailing = request(LEGACY_V2_SCHEMA, v2_route(), "run-42");
        trailing.push(0);
        assert!(legacy_rejection_response(&trailing).is_none());

        let too_deep = rmp_serde::to_vec_named(&Frame {
            schema: LEGACY_V3_SCHEMA,
            payload: DeepRequest {
                route: v3_route(),
                request_id: [4; 16],
                operation: deep_value(40),
            },
        })
        .unwrap();
        assert!(legacy_rejection_response(&too_deep).is_none());

        assert!(legacy_rejection_response(&vec![0; MAX_LEGACY_FIRST_FRAME_BYTES + 1]).is_none());
    }
}
