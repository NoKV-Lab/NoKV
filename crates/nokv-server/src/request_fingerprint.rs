/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The fingerprint that binds one mutating request id to its inputs.

use nokv_protocol as protocol;
use nokv_types as types;
use sha2::{Digest as _, Sha256};

/// Fingerprint one mutating request for request-id replay.
///
/// A same-id retry must replay after its owner restarts or its placement
/// moves, so the fingerprint omits the owner epoch, the placement generation
/// and the frame's protocol schema; every read and commit fences those
/// instead. It binds the root, logical shard and object namespace the request
/// targets, the request id, the encoded operation, and the authenticated
/// principal when there is one.
///
/// CommandDedupe stores this value, so its encoding is part of the metadata
/// format: changing it requires a new format version.
pub(crate) fn request_fingerprint(
    request: &protocol::WorkspaceRpcRequest,
    principal: Option<types::AgentId>,
) -> Result<[u8; types::SHA256_BYTES], rmp_serde::encode::Error> {
    let operation = rmp_serde::to_vec_named(&request.operation)?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.server.request-fingerprint.v1\0");
    match principal {
        None => hasher.update([0]),
        Some(principal) => {
            hasher.update([1]);
            hasher.update(principal.as_bytes());
        }
    }
    hasher.update(request.route.root_id.0);
    hasher.update(request.route.logical_shard_id.0);
    hasher.update(request.route.object_namespace_id.0);
    hasher.update(request.request_id.0);
    hasher.update((operation.len() as u64).to_be_bytes());
    hasher.update(&operation);
    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rename(owner_epoch: u64) -> protocol::WorkspaceRpcRequest {
        protocol::WorkspaceRpcRequest {
            route: protocol::RootRoute {
                root_id: protocol::RootIdentity([1; types::FIXED_ID_BYTES]),
                logical_shard_id: protocol::LogicalShardIdentity([2; types::FIXED_ID_BYTES]),
                object_namespace_id: protocol::ObjectNamespaceIdentity([3; types::FIXED_ID_BYTES]),
                placement_generation: 1,
                owner_epoch,
            },
            request_id: protocol::RequestIdentity([4; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::RenamePath(protocol::RenamePathRequest {
                source: protocol::WorkspacePath {
                    workbench: protocol::WorkbenchName::new("run").unwrap(),
                    path: protocol::RelativePath::new("outputs/a.txt").unwrap(),
                },
                destination: protocol::WorkspacePath {
                    workbench: protocol::WorkbenchName::new("run").unwrap(),
                    path: protocol::RelativePath::new("outputs/b.txt").unwrap(),
                },
                expected_generation: 1,
            }),
        }
    }

    fn hex(bytes: [u8; types::SHA256_BYTES]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn the_fingerprint_encoding_is_frozen() {
        assert_eq!(
            hex(request_fingerprint(&rename(1), None).unwrap()),
            "f4b304074a74fa514c4b3acafce1df8263dd6f95af5cdd01ac51fa892e80c098"
        );
        let principal = types::AgentId::from_bytes([7; types::FIXED_ID_BYTES]);
        assert_eq!(
            hex(request_fingerprint(&rename(1), Some(principal)).unwrap()),
            "b9a260415b7b27183a12bf092ae9d9ffdc256933a532972fe3057394315767f9"
        );
    }

    #[test]
    fn owner_epoch_placement_and_frame_schema_do_not_change_the_fingerprint() {
        let original = request_fingerprint(&rename(1), None).unwrap();
        assert_eq!(request_fingerprint(&rename(2), None).unwrap(), original);
        let mut moved = rename(1);
        moved.route.placement_generation = 9;
        assert_eq!(request_fingerprint(&moved, None).unwrap(), original);
    }

    #[test]
    fn target_request_operation_and_principal_change_the_fingerprint() {
        let original = request_fingerprint(&rename(1), None).unwrap();
        let mut variants = Vec::new();
        let mut request = rename(1);
        request.route.root_id.0[0] ^= 1;
        variants.push(request);
        let mut request = rename(1);
        request.route.logical_shard_id.0[0] ^= 1;
        variants.push(request);
        let mut request = rename(1);
        request.route.object_namespace_id.0[0] ^= 1;
        variants.push(request);
        let mut request = rename(1);
        request.request_id.0[0] ^= 1;
        variants.push(request);
        for change in 0..3 {
            let mut request = rename(1);
            let protocol::WorkspaceRequest::RenamePath(rename) = &mut request.operation else {
                unreachable!()
            };
            match change {
                0 => rename.source.path = protocol::RelativePath::new("outputs/c.txt").unwrap(),
                1 => {
                    rename.destination.path = protocol::RelativePath::new("outputs/c.txt").unwrap()
                }
                _ => rename.expected_generation = 2,
            }
            variants.push(request);
        }
        for variant in variants {
            assert_ne!(request_fingerprint(&variant, None).unwrap(), original);
        }
        let principal = types::AgentId::from_bytes([7; types::FIXED_ID_BYTES]);
        assert_ne!(
            request_fingerprint(&rename(1), Some(principal)).unwrap(),
            original
        );
    }
}
