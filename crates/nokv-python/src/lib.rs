/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Direct Python bindings for Agent workspaces and immutable artifacts.
//!
//! Metadata and lifecycle semantics remain in `nokv-client`. Durable bytes
//! remain behind `nokv-object`. The only local-filesystem behavior in this
//! crate is the explicit materialize/collect adapter.

mod client;
mod local_adapter;
mod object_store;
mod python_value;
mod routing;

use pyo3::prelude::*;

use crate::client::PythonWorkspaceClient;
use crate::object_store::PythonObjectStoreConfig;
use crate::routing::PythonRoutingConfig;

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PythonWorkspaceClient>()?;
    module.add_class::<PythonRoutingConfig>()?;
    module.add_class::<PythonObjectStoreConfig>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_module_exports_only_the_agent_sdk_configuration_surface() {
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::new(py, "_native").unwrap();
            _native(&module).unwrap();

            let routing = module.getattr("RoutingConfig").unwrap();
            assert!(routing.getattr("static").is_ok());
            assert!(routing.getattr("etcd").is_ok());
            assert!(module.getattr("Client").is_ok());
            assert!(module.getattr("ObjectStoreConfig").is_ok());

            for removed in [
                "NoKvFsClient",
                "RangeBatchPlan",
                "RangeBatchReader",
                "ReadBuffer",
            ] {
                assert!(
                    module.getattr(removed).is_err(),
                    "unexpected export {removed}"
                );
            }
        });
    }
}
