/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use nokv_protocol::{RpcFailure, WorkspaceResult, WorkspaceRpcRequest};
use nokv_types::PrincipalId;

/// The authority the server established for one request: the principal it
/// authenticated and authorized for the request's root, or none on a trusted
/// plaintext deployment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestAuthority {
    pub principal: Option<PrincipalId>,
}

impl RequestAuthority {
    /// A plaintext connection: nobody is authenticated and nothing is refused.
    pub const TRUSTED: Self = Self { principal: None };

    /// What metadata writes record for this request.
    pub fn write_authority(self) -> nokv_meta::workspace::WriteAuthority {
        nokv_meta::workspace::WriteAuthority {
            initiator: self.principal,
        }
    }
}

/// Successful metadata execution before the server adds the route envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutedRequest {
    pub result: WorkspaceResult,
    pub commit_version: Option<u64>,
    pub replayed: bool,
}

/// Protocol-facing adapter implemented by one fenced metadata shard owner.
///
/// Implementations must revalidate `request.route` at the metadata commit
/// boundary; the server registry check is only admission control.
pub trait WorkspaceRequestExecutor: Send + Sync {
    fn execute(
        &self,
        request: &WorkspaceRpcRequest,
        authority: RequestAuthority,
    ) -> Result<ExecutedRequest, RpcFailure>;
}

impl<T> WorkspaceRequestExecutor for Arc<T>
where
    T: WorkspaceRequestExecutor + ?Sized,
{
    fn execute(
        &self,
        request: &WorkspaceRpcRequest,
        authority: RequestAuthority,
    ) -> Result<ExecutedRequest, RpcFailure> {
        (**self).execute(request, authority)
    }
}
