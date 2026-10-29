/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The client's identity towards an owner that serves workspace RPC over TLS.

use nokv_client::RpcClientTls;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// The PEM documents a client presents to an owner that requires TLS: the
/// roots that sign the owner's certificate, this client's certificate chain
/// (its fingerprint is the credential the tenant registered) and its private
/// key, plus the name the owner's certificate must carry. The documents are
/// parsed once, here; the key never appears in `repr`, errors, or logs.
#[pyclass(name = "TlsConfig", frozen)]
#[derive(Clone)]
pub(crate) struct PythonTlsConfig {
    tls: RpcClientTls,
    server_name: String,
}

#[pymethods]
impl PythonTlsConfig {
    #[new]
    fn new(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8], server_name: &str) -> PyResult<Self> {
        let tls = RpcClientTls::from_pem(server_name, ca_pem, cert_pem, key_pem)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self {
            tls,
            server_name: server_name.to_owned(),
        })
    }

    #[getter]
    fn server_name(&self) -> &str {
        &self.server_name
    }

    fn __repr__(&self) -> String {
        format!("TlsConfig(server_name={:?})", self.server_name)
    }
}

impl PythonTlsConfig {
    pub(crate) fn rpc_tls(&self) -> RpcClientTls {
        self.tls.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_documents_are_value_errors_that_quote_no_bytes() {
        Python::initialize();
        let error = PythonTlsConfig::new(b"not pem", b"secret-cert", b"secret-key", "owner.test")
            .err()
            .expect("malformed documents are refused");
        let message = error.to_string();
        assert!(
            message.contains("invalid RPC TLS configuration"),
            "{message}"
        );
        assert!(!message.contains("secret"), "{message}");
    }
}
