/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::net::SocketAddr;
use std::sync::Arc;

use nokv_client::{
    ClientError, FramedTcpOptions, FramedTcpTransport, RouteResolver, SeedRouteOptions,
    SeedRouteResolver,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// NoKV seed discovery configuration for one Agent root.
#[pyclass(name = "RoutingConfig", frozen, skip_from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct PythonRoutingConfig {
    seeds: Vec<SocketAddr>,
}

#[pymethods]
impl PythonRoutingConfig {
    /// Discover the current owner through one or more NoKV seed servers.
    #[staticmethod]
    fn seeds(endpoints: Vec<String>) -> PyResult<Self> {
        let seeds = endpoints
            .into_iter()
            .map(|endpoint| {
                endpoint.parse::<SocketAddr>().map_err(|error| {
                    PyValueError::new_err(format!(
                        "invalid NoKV seed endpoint {endpoint:?}: {error}"
                    ))
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        let transport =
            FramedTcpTransport::new(FramedTcpOptions::default()).map_err(value_error)?;
        SeedRouteResolver::new(
            transport,
            seeds.iter().copied(),
            SeedRouteOptions::default(),
        )
        .map_err(value_error)?;
        Ok(Self { seeds })
    }
}

impl PythonRoutingConfig {
    pub(crate) fn build(
        &self,
        transport_options: FramedTcpOptions,
        max_attempts: u32,
    ) -> Result<Arc<dyn RouteResolver>, ClientError> {
        let transport = FramedTcpTransport::new(transport_options).map_err(ClientError::from)?;
        let resolver = SeedRouteResolver::new(
            transport,
            self.seeds.iter().copied(),
            SeedRouteOptions {
                max_attempts,
                ..SeedRouteOptions::default()
            },
        )?;
        Ok(Arc::new(resolver))
    }
}

fn value_error(error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_configuration_validates_without_connecting() {
        Python::initialize();
        let config = PythonRoutingConfig::seeds(vec![
            "127.0.0.1:17750".to_owned(),
            "127.0.0.1:17751".to_owned(),
        ])
        .unwrap();
        config.build(FramedTcpOptions::default(), 3).unwrap();
    }

    #[test]
    fn seed_configuration_rejects_empty_or_unconnectable_endpoints() {
        Python::initialize();
        assert!(PythonRoutingConfig::seeds(Vec::new())
            .unwrap_err()
            .to_string()
            .contains("at least one NoKV seed"));
        assert!(PythonRoutingConfig::seeds(vec!["0.0.0.0:7750".to_owned()])
            .unwrap_err()
            .to_string()
            .contains("connectable addresses"));
    }
}
