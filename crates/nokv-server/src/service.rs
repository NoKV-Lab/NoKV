/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use nokv_protocol::{RpcFailure, WorkspaceResult, WorkspaceRpcRequest};

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
    fn execute(&self, request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure>;
}

impl<T> WorkspaceRequestExecutor for Arc<T>
where
    T: WorkspaceRequestExecutor + ?Sized,
{
    fn execute(&self, request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure> {
        (**self).execute(request)
    }
}
