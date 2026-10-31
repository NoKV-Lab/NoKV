/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use nokv_client::{
    ClientError, ControlRouteResolver, EtcdRouteOptions, EtcdTlsConfig, RouteResolver,
    StaticRouteResolver,
};
use nokv_protocol::{LogicalShardIdentity, ObjectNamespaceIdentity, RootIdentity, RootRoute};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::python_value::parse_fixed_hex;

#[derive(Clone, Debug)]
enum ConfiguredRouting {
    Static {
        endpoint: SocketAddr,
        logical_shard_id: LogicalShardIdentity,
        object_namespace_id: ObjectNamespaceIdentity,
        placement_generation: u64,
        owner_epoch: u64,
    },
    Etcd(EtcdRouteOptions),
    Local(PathBuf),
}

/// Explicit route discovery configuration for one Agent root.
#[pyclass(name = "RoutingConfig", frozen)]
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
        object_namespace_id: &str,
        placement_generation: u64,
        owner_epoch: u64,
    ) -> PyResult<Self> {
        let endpoint = endpoint.parse::<SocketAddr>().map_err(|error| {
            PyValueError::new_err(format!("invalid endpoint {endpoint:?}: {error}"))
        })?;
        let logical_shard_id =
            LogicalShardIdentity(parse_fixed_hex("logical_shard_id", logical_shard_id)?);
        let object_namespace_id =
            ObjectNamespaceIdentity(parse_fixed_hex("object_namespace_id", object_namespace_id)?);
        RootRoute {
            root_id: RootIdentity([0; 16]),
            logical_shard_id,
            object_namespace_id,
            placement_generation,
            owner_epoch,
        }
        .validate()
        .map_err(value_error)?;
        Ok(Self {
            routing: ConfiguredRouting::Static {
                endpoint,
                logical_shard_id,
                object_namespace_id,
                placement_generation,
                owner_epoch,
            },
        })
    }

    /// Discover placement and ownership from the authoritative etcd control
    /// plane. With `tls_ca_pem`, `tls_cert_pem` and `tls_key_pem` (PEM bytes,
    /// given together), this process authenticates to etcd over mutual TLS
    /// as the certificate's Common Name, and every endpoint must be an
    /// `https://` URL; `https://` endpoints require them.
    #[staticmethod]
    #[pyo3(signature = (
        endpoints,
        key_prefix = "/nokv/control",
        lease_ttl_seconds = 10,
        tls_ca_pem = None,
        tls_cert_pem = None,
        tls_key_pem = None
    ))]
    fn etcd(
        endpoints: Vec<String>,
        key_prefix: &str,
        lease_ttl_seconds: i64,
        tls_ca_pem: Option<Vec<u8>>,
        tls_cert_pem: Option<Vec<u8>>,
        tls_key_pem: Option<Vec<u8>>,
    ) -> PyResult<Self> {
        let mut options = EtcdRouteOptions::new(endpoints)
            .with_key_prefix(key_prefix)
            .with_lease_ttl_seconds(lease_ttl_seconds);
        let identity = [
            ("tls_ca_pem", tls_ca_pem),
            ("tls_cert_pem", tls_cert_pem),
            ("tls_key_pem", tls_key_pem),
        ];
        if identity.iter().any(|(_, pem)| pem.is_some()) {
            let mut pems = Vec::with_capacity(3);
            for (name, pem) in identity {
                pems.push(pem.ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "{name} is required: tls_ca_pem, tls_cert_pem and tls_key_pem are given \
                         together"
                    ))
                })?);
            }
            let [ca, certificate, key] =
                <[Vec<u8>; 3]>::try_from(pems).expect("three PEM documents were collected");
            options = options.with_tls(EtcdTlsConfig::new(ca, certificate, key));
        } else if options
            .endpoints()
            .iter()
            .any(|endpoint| endpoint.trim_start().starts_with("https://"))
        {
            return Err(PyValueError::new_err(
                "an https:// etcd endpoint requires tls_ca_pem, tls_cert_pem and tls_key_pem",
            ));
        }
        options.validate().map_err(value_error)?;
        Ok(Self {
            routing: ConfiguredRouting::Etcd(options),
        })
    }

    /// Discover placement and ownership from the single-host control
    /// directory that the owner serves from.
    #[staticmethod]
    fn local(directory: PathBuf) -> PyResult<Self> {
        if directory.as_os_str().is_empty() {
            return Err(PyValueError::new_err(
                "local control directory must not be empty",
            ));
        }
        Ok(Self {
            routing: ConfiguredRouting::Local(directory),
        })
    }
}

impl PythonRoutingConfig {
    /// One resolver for every root of a control-backed configuration, which
    /// a session shares between its clients; `None` for a static route,
    /// which pins one root and is built per client.
    pub(crate) fn shared_resolver(&self) -> Result<Option<Arc<dyn RouteResolver>>, ClientError> {
        match &self.routing {
            ConfiguredRouting::Static { .. } => Ok(None),
            ConfiguredRouting::Etcd(options) => Ok(Some(Arc::new(
                ControlRouteResolver::connect_etcd(options.clone())?,
            ))),
            ConfiguredRouting::Local(directory) => Ok(Some(Arc::new(
                ControlRouteResolver::open_local(directory.clone())?,
            ))),
        }
    }

    pub(crate) fn build(
        &self,
        root_id: RootIdentity,
    ) -> Result<Arc<dyn RouteResolver>, ClientError> {
        match &self.routing {
            ConfiguredRouting::Static {
                endpoint,
                logical_shard_id,
                object_namespace_id,
                placement_generation,
                owner_epoch,
            } => Ok(Arc::new(StaticRouteResolver::new(
                RootRoute {
                    root_id,
                    logical_shard_id: *logical_shard_id,
                    object_namespace_id: *object_namespace_id,
                    placement_generation: *placement_generation,
                    owner_epoch: *owner_epoch,
                },
                *endpoint,
            )?)),
            ConfiguredRouting::Etcd(options) => Ok(Arc::new(ControlRouteResolver::connect_etcd(
                options.clone(),
            )?)),
            ConfiguredRouting::Local(directory) => Ok(Arc::new(ControlRouteResolver::open_local(
                directory.clone(),
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
        let config = PythonRoutingConfig::static_route(
            "127.0.0.1:17750",
            &"22".repeat(16),
            &"33".repeat(16),
            7,
            9,
        )
        .unwrap();
        let root_id = RootIdentity([0x11; 16]);
        let resolver = config.build(root_id).unwrap();
        let resolved = resolver.resolve(root_id, false).unwrap();
        assert_eq!(resolved.route.root_id, root_id);
        assert_eq!(
            resolved.route.logical_shard_id,
            LogicalShardIdentity([0x22; 16])
        );
        assert_eq!(
            resolved.route.object_namespace_id,
            ObjectNamespaceIdentity([0x33; 16])
        );
        assert_eq!(resolved.route.placement_generation, 7);
        assert_eq!(resolved.route.owner_epoch, 9);
        assert_eq!(resolved.endpoint, "127.0.0.1:17750".parse().unwrap());
    }

    #[test]
    fn static_configuration_rejects_invalid_fences() {
        Python::initialize();
        let error = PythonRoutingConfig::static_route(
            "127.0.0.1:17750",
            &"22".repeat(16),
            &"33".repeat(16),
            0,
            9,
        )
        .unwrap_err();
        assert!(error.to_string().contains("placement_generation"));
    }

    #[test]
    fn local_configuration_resolves_through_its_control_directory() {
        Python::initialize();
        assert!(PythonRoutingConfig::local(PathBuf::new()).is_err());
        let directory = tempfile::tempdir().unwrap();
        let root_id = RootIdentity([0x11; 16]);
        let resolver = PythonRoutingConfig::local(directory.path().join("control"))
            .unwrap()
            .build(root_id)
            .unwrap();
        let error = resolver.resolve(root_id, true).unwrap_err();
        assert!(error.to_string().contains("root placement does not exist"));
    }

    /// The SDK classifies a control directory that group or other users can
    /// write as the CLI does: for an operator, who checks its records.
    #[test]
    fn an_unsafe_local_control_directory_is_for_an_operator() {
        use std::os::unix::fs::PermissionsExt as _;

        Python::initialize();
        let directory = tempfile::tempdir().unwrap();
        let control = directory.path().join("control");
        std::fs::create_dir(&control).unwrap();
        std::fs::set_permissions(&control, std::fs::Permissions::from_mode(0o770)).unwrap();
        let root_id = RootIdentity([0x11; 16]);
        let error = PythonRoutingConfig::local(control)
            .unwrap()
            .build(root_id)
            .unwrap()
            .resolve(root_id, true)
            .unwrap_err();
        assert_eq!(
            error.class(),
            nokv_types::ErrorClass::OperatorRequired,
            "{error}"
        );
        assert!(
            error
                .to_string()
                .contains("group or other users can write it"),
            "{error}"
        );
    }

    #[test]
    fn control_backed_configurations_share_one_resolver_and_static_ones_pin_one_root() {
        Python::initialize();
        let directory = tempfile::tempdir().unwrap();
        let local = PythonRoutingConfig::local(directory.path().join("control")).unwrap();
        let shared = local
            .shared_resolver()
            .unwrap()
            .expect("a control directory serves every root");
        let first = RootIdentity([0x11; 16]);
        let second = RootIdentity([0x12; 16]);
        for root_id in [first, second] {
            let error = shared.resolve(root_id, true).unwrap_err();
            assert!(
                error.to_string().contains("root placement does not exist"),
                "{error}"
            );
        }

        let pinned = PythonRoutingConfig::static_route(
            "127.0.0.1:17750",
            &"22".repeat(16),
            &"33".repeat(16),
            7,
            9,
        )
        .unwrap();
        assert!(pinned.shared_resolver().unwrap().is_none());
    }

    #[test]
    fn etcd_configuration_validates_without_connecting() {
        Python::initialize();
        let error = PythonRoutingConfig::etcd(Vec::new(), "/nokv/control", 10, None, None, None)
            .unwrap_err();
        assert!(error.to_string().contains("at least one etcd endpoint"));
    }

    #[test]
    fn etcd_tls_identity_is_complete_and_binds_https_endpoints() {
        Python::initialize();
        let pem = |text: &str| Some(text.as_bytes().to_vec());
        let secured = PythonRoutingConfig::etcd(
            vec!["https://127.0.0.1:2379".to_owned()],
            "/nokv/control",
            10,
            pem("ca"),
            pem("secret-cert"),
            pem("secret-key"),
        )
        .unwrap();
        assert!(!format!("{secured:?}").contains("secret"));

        let partial = PythonRoutingConfig::etcd(
            vec!["https://127.0.0.1:2379".to_owned()],
            "/nokv/control",
            10,
            pem("ca"),
            pem("cert"),
            None,
        )
        .unwrap_err();
        assert!(partial.to_string().contains("tls_key_pem"), "{partial}");

        let plaintext_with_identity = PythonRoutingConfig::etcd(
            vec!["http://127.0.0.1:2379".to_owned()],
            "/nokv/control",
            10,
            pem("ca"),
            pem("cert"),
            pem("key"),
        )
        .unwrap_err();
        assert!(
            plaintext_with_identity.to_string().contains("https://"),
            "{plaintext_with_identity}"
        );

        let https_without_identity = PythonRoutingConfig::etcd(
            vec!["https://127.0.0.1:2379".to_owned()],
            "/nokv/control",
            10,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(
            https_without_identity.to_string().contains("tls_ca_pem"),
            "{https_without_identity}"
        );
    }
}
