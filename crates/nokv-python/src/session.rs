/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! One process's connection to NoKV, shared by the clients of every root it
//! uses.

use std::sync::Arc;
use std::time::Duration;

use nokv_client::{
    ClientOptions, FramedTcpOptions, FramedTcpTransport, RouteResolver, RpcClientTls,
    WorkspaceClient,
};
use nokv_protocol::RootIdentity;
use nokv_types::IdempotencyScope;
use pyo3::prelude::*;

use crate::client::PythonWorkspaceClient;
use crate::object_store::PythonObjectStoreConfig;
use crate::python_error::{configuration_failure, invalid_input, runtime_client_failure};
use crate::python_value::parse_fixed_hex;
use crate::routing::PythonRoutingConfig;
use crate::tls::PythonTlsConfig;

type SharedSession = nokv_client::Session<FramedTcpTransport, Arc<dyn RouteResolver>>;

/// The timeouts and the retry policy every client of a session runs with.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SessionSettings {
    pub(crate) max_attempts: u32,
    pub(crate) connect_timeout_ms: u64,
    pub(crate) read_timeout_ms: u64,
    pub(crate) write_timeout_ms: u64,
    pub(crate) handshake_timeout_ms: u64,
    pub(crate) retry_backoff_ms: u64,
    pub(crate) max_retry_backoff_ms: u64,
}

/// What a process shares across the roots it uses: the transport, with its
/// TLS identity and connection pool; the route resolver, with its
/// control-plane connection and route cache; the retry policy; and the
/// object-store configuration. `client(root_id)` derives a `Client` of one
/// root from it. A static route pins one root, so a session over static
/// routing serves that root only.
#[pyclass(name = "Session")]
pub(crate) struct PythonSession {
    routing: PythonRoutingConfig,
    /// The shared session of a control-backed configuration; a static route
    /// builds its one resolver per client instead.
    shared: Option<SharedSession>,
    transport: FramedTcpTransport,
    options: ClientOptions,
    object_store: Option<PythonObjectStoreConfig>,
}

#[pymethods]
impl PythonSession {
    #[new]
    #[pyo3(signature = (
        routing,
        object_store = None,
        tls = None,
        max_attempts = 3,
        connect_timeout_ms = 5_000,
        read_timeout_ms = 30_000,
        write_timeout_ms = 30_000,
        handshake_timeout_ms = 5_000,
        retry_backoff_ms = 0,
        max_retry_backoff_ms = 2_000
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        routing: PyRef<'_, PythonRoutingConfig>,
        object_store: Option<PyRef<'_, PythonObjectStoreConfig>>,
        tls: Option<PyRef<'_, PythonTlsConfig>>,
        max_attempts: u32,
        connect_timeout_ms: u64,
        read_timeout_ms: u64,
        write_timeout_ms: u64,
        handshake_timeout_ms: u64,
        retry_backoff_ms: u64,
        max_retry_backoff_ms: u64,
    ) -> PyResult<Self> {
        Self::open(
            py,
            (*routing).clone(),
            object_store.map(|config| (*config).clone()),
            tls.map(|tls| tls.rpc_tls()),
            SessionSettings {
                max_attempts,
                connect_timeout_ms,
                read_timeout_ms,
                write_timeout_ms,
                handshake_timeout_ms,
                retry_backoff_ms,
                max_retry_backoff_ms,
            },
        )
    }

    /// A client of one root over this session. Its Workbench root is the one
    /// the control plane binds the root to; a `workbench_root` given here
    /// must agree with that binding, and is required only over static
    /// routing, which has no control plane to ask.
    #[pyo3(signature = (root_id, workbench_root = None))]
    fn client(
        &self,
        py: Python<'_>,
        root_id: &str,
        workbench_root: Option<String>,
    ) -> PyResult<PythonWorkspaceClient> {
        let root_id = RootIdentity(parse_fixed_hex("root_id", root_id)?);
        self.open_client(py, root_id, workbench_root)
    }

    /// The Workbench root the control plane binds `root_id` to, or `None`
    /// when none is bound or the session routes statically.
    fn workbench_root(&self, py: Python<'_>, root_id: &str) -> PyResult<Option<String>> {
        let root_id = RootIdentity(parse_fixed_hex("root_id", root_id)?);
        self.bound_workbench_root(py, root_id)
    }
}

impl PythonSession {
    pub(crate) fn open(
        py: Python<'_>,
        routing: PythonRoutingConfig,
        object_store: Option<PythonObjectStoreConfig>,
        tls: Option<RpcClientTls>,
        settings: SessionSettings,
    ) -> PyResult<Self> {
        let options = ClientOptions {
            max_attempts: settings.max_attempts,
            retry_backoff: Duration::from_millis(settings.retry_backoff_ms),
            max_retry_backoff: Duration::from_millis(settings.max_retry_backoff_ms),
        };
        options.validate().map_err(invalid_input)?;
        let mut transport = FramedTcpTransport::new(FramedTcpOptions {
            connect_timeout: Duration::from_millis(settings.connect_timeout_ms),
            handshake_timeout: Duration::from_millis(settings.handshake_timeout_ms),
            read_timeout: Duration::from_millis(settings.read_timeout_ms),
            write_timeout: Duration::from_millis(settings.write_timeout_ms),
        })
        .map_err(invalid_input)?;
        if let Some(tls) = tls {
            transport = transport.with_tls(tls);
        }
        let shared = {
            let routing = routing.clone();
            let transport = transport.clone();
            py.detach(move || {
                routing
                    .shared_resolver()?
                    .map(|resolver| nokv_client::Session::new(transport, resolver, options))
                    .transpose()
            })
            .map_err(|error| configuration_failure(py, error))?
        };
        Ok(Self {
            routing,
            shared,
            transport,
            options,
            object_store,
        })
    }

    fn bound_workbench_root(
        &self,
        py: Python<'_>,
        root_id: RootIdentity,
    ) -> PyResult<Option<String>> {
        let Some(shared) = &self.shared else {
            return Ok(None);
        };
        let shared = shared.clone();
        py.detach(move || shared.workbench_root(root_id))
            .map(|root| root.map(|root| root.as_str().to_owned()))
            .map_err(|error| runtime_client_failure(py, error, IdempotencyScope::ReadOnly))
    }

    pub(crate) fn open_client(
        &self,
        py: Python<'_>,
        root_id: RootIdentity,
        workbench_root: Option<String>,
    ) -> PyResult<PythonWorkspaceClient> {
        let bound = self.bound_workbench_root(py, root_id)?;
        let workbench_root = match (workbench_root, bound) {
            (Some(given), Some(bound)) if given != bound => {
                return Err(invalid_input(format!(
                    "workbench_root {given:?} differs from the Workbench root the control plane \
                     binds this root to, {bound:?}; omit it or pass the bound value"
                )));
            }
            (given, bound) => given.or(bound),
        };
        let client = match &self.shared {
            Some(shared) => shared.client(root_id),
            None => {
                let routing = self.routing.clone();
                let transport = self.transport.clone();
                let options = self.options;
                py.detach(move || {
                    let resolver = routing.build(root_id)?;
                    WorkspaceClient::new(root_id, transport, resolver, options)
                })
            }
        }
        .map_err(|error| configuration_failure(py, error))?;
        py.detach(|| client.preflight(std::iter::empty()))
            .map_err(|error| runtime_client_failure(py, error, IdempotencyScope::ReadOnly))?;
        Ok(PythonWorkspaceClient::from_parts(
            client,
            self.object_store.clone(),
            workbench_root,
        ))
    }
}
