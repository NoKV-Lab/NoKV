/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::net::SocketAddr;
use std::sync::Arc;

use nokv_client::{
    ClientError, ControlRouteResolver, EtcdRouteOptions, RouteResolver, StaticRouteResolver,
};
use nokv_protocol::{LogicalShardIdentity, RootIdentity, RootRoute};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::python_value::parse_fixed_hex;

#[derive(Clone, Debug)]
enum ConfiguredRouting {
    Static {
        endpoint: SocketAddr,
        logical_shard_id: LogicalShardIdentity,
        placement_generation: u64,
        owner_epoch: u64,
    },
    Etcd(EtcdRouteOptions),
}

/// Explicit route discovery configuration for one Agent root.
#[pyclass(name = "RoutingConfig", frozen, skip_from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct PythonRoutingConfig {
    routing: ConfiguredRouting,
}

#[pymethods]
impl PythonRoutingConfig {
    /// Pin one logical-shard owner. Intended for tests and single-node setups.
    #[staticmethod]
    #[pyo3(name = "static")]
    fn static_route(
        endpoint: &str,
        logical_shard_id: &str,
        placement_generation: u64,
        owner_epoch: u64,
    ) -> PyResult<Self> {
        let endpoint = endpoint.parse::<SocketAddr>().map_err(|error| {
            PyValueError::new_err(format!("invalid endpoint {endpoint:?}: {error}"))
        })?;
        let logical_shard_id =
            LogicalShardIdentity(parse_fixed_hex("logical_shard_id", logical_shard_id)?);
        RootRoute {
            root_id: RootIdentity([0; 16]),
            logical_shard_id,
            placement_generation,
            owner_epoch,
        }
        .validate()
        .map_err(value_error)?;
        Ok(Self {
            routing: ConfiguredRouting::Static {
                endpoint,
                logical_shard_id,
                placement_generation,
                owner_epoch,
            },
        })
    }

    /// Discover placement and ownership from the authoritative etcd control plane.
    #[staticmethod]
    #[pyo3(signature = (
        endpoints,
        key_prefix = "/nokv/control",
        lease_ttl_seconds = 10
    ))]
    fn etcd(endpoints: Vec<String>, key_prefix: &str, lease_ttl_seconds: i64) -> PyResult<Self> {
        let options = EtcdRouteOptions::new(endpoints)
            .with_key_prefix(key_prefix)
            .with_lease_ttl_seconds(lease_ttl_seconds);
        options.validate().map_err(value_error)?;
        Ok(Self {
            routing: ConfiguredRouting::Etcd(options),
        })
    }
}

impl PythonRoutingConfig {
    pub(crate) fn build(
        &self,
        root_id: RootIdentity,
    ) -> Result<Arc<dyn RouteResolver>, ClientError> {
        match &self.routing {
            ConfiguredRouting::Static {
                endpoint,
                logical_shard_id,
                placement_generation,
                owner_epoch,
            } => Ok(Arc::new(StaticRouteResolver::new(
                RootRoute {
                    root_id,
                    logical_shard_id: *logical_shard_id,
                    placement_generation: *placement_generation,
                    owner_epoch: *owner_epoch,
                },
                *endpoint,
            )?)),
            ConfiguredRouting::Etcd(options) => Ok(Arc::new(ControlRouteResolver::connect_etcd(
                options.clone(),
            )?)),
        }
    }
}

fn value_error(error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_configuration_builds_the_exact_root_route() {
        let config =
            PythonRoutingConfig::static_route("127.0.0.1:17750", &"22".repeat(16), 7, 9).unwrap();
        let root_id = RootIdentity([0x11; 16]);
        let resolver = config.build(root_id).unwrap();
        let resolved = resolver.resolve(root_id, false).unwrap();
        assert_eq!(resolved.route.root_id, root_id);
        assert_eq!(
            resolved.route.logical_shard_id,
            LogicalShardIdentity([0x22; 16])
        );
        assert_eq!(resolved.route.placement_generation, 7);
        assert_eq!(resolved.route.owner_epoch, 9);
        assert_eq!(resolved.endpoint, "127.0.0.1:17750".parse().unwrap());
    }

    #[test]
    fn static_configuration_rejects_invalid_fences() {
        Python::initialize();
        let error = PythonRoutingConfig::static_route("127.0.0.1:17750", &"22".repeat(16), 0, 9)
            .unwrap_err();
        assert!(error.to_string().contains("placement_generation"));
    }

    #[test]
    fn etcd_configuration_validates_without_connecting() {
        Python::initialize();
        let error = PythonRoutingConfig::etcd(Vec::new(), "/nokv/control", 10).unwrap_err();
        assert!(error.to_string().contains("at least one etcd endpoint"));
    }
}
