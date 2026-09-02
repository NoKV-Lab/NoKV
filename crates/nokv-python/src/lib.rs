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
    fn native_module_exports_the_versioned_path_native_workbench_surface() {
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::new(py, "_native").unwrap();
            _native(&module).unwrap();

            let routing = module.getattr("RoutingConfig").unwrap();
            assert!(routing.getattr("seeds").is_ok());
            assert!(module.getattr("Client").is_ok());
            assert!(module.getattr("ObjectStoreConfig").is_ok());

            let client = module.getattr("Client").unwrap();
            for method in [
                "create_workspace",
                "stat",
                "exists",
                "list",
                "remove",
                "rename",
                "publish_bytes",
                "publish_file",
                "read",
                "read_range",
                "read_ranges_batch",
                "snapshot",
                "renew_snapshot",
                "retire_snapshot",
                "list_snapshots",
                "materialize",
                "collect",
            ] {
                assert!(
                    client.getattr(method).is_ok(),
                    "missing path-native Client method {method}"
                );
            }

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
