/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! One process's connection to NoKV, shared by the clients of every root it
//! uses.

use nokv_protocol::RootIdentity;
use nokv_types::WorkbenchRoot;

use crate::{ClientError, ClientOptions, RouteResolver, RpcTransport, WorkspaceClient};

/// What a process shares across roots: the transport, with its TLS identity
/// and its connection pool; the route resolver, with its control-plane
/// connection and route cache; and the retry policy. A client of any root is
/// derived from it, and every client derived from one session draws on the
/// same pool, cache, and credentials.
///
/// The session does not decide how those are built: a plaintext or TLS
/// [`crate::FramedTcpTransport`], a [`crate::ControlRouteResolver`] over etcd
/// or a control directory, or a pinned [`crate::StaticRouteResolver`] are
/// passed in as they are built today.
#[derive(Clone)]
pub struct Session<Transport, Resolver> {
    transport: Transport,
    routes: Resolver,
    options: ClientOptions,
}

impl<Transport, Resolver> std::fmt::Debug for Session<Transport, Resolver> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Session")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<Transport, Resolver> Session<Transport, Resolver>
where
    Transport: RpcTransport + Clone,
    Resolver: RouteResolver + Clone,
{
    /// Validates the retry policy once, before any client exists.
    pub fn new(
        transport: Transport,
        routes: Resolver,
        options: ClientOptions,
    ) -> Result<Self, ClientError> {
        options.validate()?;
        Ok(Self {
            transport,
            routes,
            options,
        })
    }

    /// A client of one root over this session's transport, resolver and
    /// retry policy. Each client keeps its own request identities and body
    /// digest cache; everything else is shared.
    pub fn client(
        &self,
        root_id: RootIdentity,
    ) -> Result<WorkspaceClient<Transport, Resolver>, ClientError> {
        WorkspaceClient::new(
            root_id,
            self.transport.clone(),
            self.routes.clone(),
            self.options,
        )
    }

    /// The Workbench root the control plane binds `root_id` to, through the
    /// session's resolver; see [`RouteResolver::workbench_root`].
    pub fn workbench_root(
        &self,
        root_id: RootIdentity,
    ) -> Result<Option<WorkbenchRoot>, ClientError> {
        self.routes.workbench_root(root_id)
    }

    pub const fn options(&self) -> ClientOptions {
        self.options
    }

    pub const fn transport(&self) -> &Transport {
        &self.transport
    }

    pub const fn routes(&self) -> &Resolver {
        &self.routes
    }
}

#[cfg(all(test, feature = "control"))]
mod tests {
    use std::sync::Arc;

    use nokv_control::{
        ControlStore, IdentityStore, InMemoryControlStore, LogicalShardId, NodeId,
        PlacementGeneration, RecoveryPublication, RootId, RootObjectNamespaceBinding,
        RootPlacement, RootPlacementLifecycle, RootPresentationBinding,
    };
    use nokv_protocol::RootIdentity;
    use nokv_types::WorkbenchRoot;

    use super::*;
    use crate::{
        ClientOptions, ControlRouteResolver, FramedTcpOptions, FramedTcpTransport, RouteResolver,
    };

    fn id(fill: u8) -> [u8; 16] {
        [fill; 16]
    }

    /// Two roots of one shard, served by one owner, with one root's
    /// Workbench root bound.
    fn two_roots() -> (Arc<InMemoryControlStore>, RootIdentity, RootIdentity) {
        let store = Arc::new(InMemoryControlStore::new());
        let logical_shard_id = LogicalShardId::from_bytes(id(2));
        store.create_logical_shard(logical_shard_id).unwrap();
        for fill in [1, 3] {
            let root_id = RootId::from_bytes(id(fill));
            store
                .create_root_object_namespace_binding(RootObjectNamespaceBinding {
                    root_id,
                    object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes(id(8)),
                })
                .unwrap();
            let provisioning = store
                .create_root_placement(RootPlacement {
                    root_id,
                    logical_shard_id,
                    placement_generation: PlacementGeneration::new(1).unwrap(),
                    lifecycle: RootPlacementLifecycle::Provisioning,
                })
                .unwrap();
            let active = RootPlacement {
                placement_generation: PlacementGeneration::new(2).unwrap(),
                lifecycle: RootPlacementLifecycle::Active,
                ..provisioning.clone()
            };
            store
                .compare_and_set_root_placement(&provisioning, active)
                .unwrap();
        }
        let lease = store
            .acquire_owner(
                &logical_shard_id,
                NodeId::new("node-a").unwrap(),
                "127.0.0.1:7750".to_owned(),
            )
            .unwrap();
        store
            .mark_serving(
                &lease,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            )
            .unwrap();
        store
            .bind_root_presentation(RootPresentationBinding {
                root_id: RootId::from_bytes(id(1)),
                workbench_root: WorkbenchRoot::new("/agents/one/wb").unwrap(),
            })
            .unwrap();
        (store, RootIdentity(id(1)), RootIdentity(id(3)))
    }

    #[test]
    fn clients_of_one_session_share_its_routes_transport_and_options() {
        let (store, first, second) = two_roots();
        let identity: Arc<dyn IdentityStore> = store.clone();
        let control: Arc<dyn ControlStore> = store;
        let routes = Arc::new(ControlRouteResolver::with_identity(control, identity));
        let options = ClientOptions {
            max_attempts: 5,
            ..ClientOptions::default()
        };
        let session = Session::new(
            FramedTcpTransport::new(FramedTcpOptions::default()).unwrap(),
            routes.clone(),
            options,
        )
        .unwrap();

        let one = session.client(first).unwrap();
        let other = session.client(second).unwrap();
        assert_eq!(one.root_id(), first);
        assert_eq!(other.root_id(), second);
        assert_eq!(session.options(), options);

        // One resolver, one route cache: a route the first client resolved
        // is already cached for the second.
        one.routes().resolve(first, false).unwrap();
        assert!(routes.cached_route(first).is_some());
        assert!(other.routes().cached_route(first).is_some());
        assert!(other.routes().cached_route(second).is_none());
        // One transport: the clients draw on the same connection pool.
        assert!(one.transport().shares_pool_with(other.transport()));
    }

    #[test]
    fn a_session_reads_the_workbench_root_the_control_plane_binds() {
        let (store, bound, unbound) = two_roots();
        let identity: Arc<dyn IdentityStore> = store.clone();
        let control: Arc<dyn ControlStore> = store;
        let session = Session::new(
            FramedTcpTransport::new(FramedTcpOptions::default()).unwrap(),
            Arc::new(ControlRouteResolver::with_identity(control, identity)),
            ClientOptions::default(),
        )
        .unwrap();
        assert_eq!(
            session
                .workbench_root(bound)
                .unwrap()
                .as_ref()
                .map(WorkbenchRoot::as_str),
            Some("/agents/one/wb")
        );
        assert_eq!(session.workbench_root(unbound).unwrap(), None);
    }

    #[test]
    fn invalid_options_are_refused_before_any_client_exists() {
        let (store, _, _) = two_roots();
        let control: Arc<dyn ControlStore> = store;
        let error = Session::new(
            FramedTcpTransport::new(FramedTcpOptions::default()).unwrap(),
            Arc::new(ControlRouteResolver::new(control)),
            ClientOptions {
                max_attempts: 0,
                ..ClientOptions::default()
            },
        )
        .unwrap_err();
        assert!(
            matches!(error, crate::ClientError::InvalidOptions(_)),
            "{error}"
        );
    }
}
