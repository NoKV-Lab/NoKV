/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! PEM files for workspace RPC over TLS.

use std::fs::File;
use std::io::Read as _;
use std::path::Path;

use nokv_client::RpcClientTls;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};

use crate::cli::{ClientTlsConfig, ServerTlsConfig};

/// Largest PEM file an `--rpc-tls-*` option reads.
const MAX_PEM_FILE_BYTES: u64 = 1024 * 1024;

/// The TLS settings of a command that connects to an owner.
pub(crate) fn client_tls(config: &ClientTlsConfig) -> Result<RpcClientTls, String> {
    let server_name = ServerName::try_from(config.server_name.clone()).map_err(|_| {
        format!(
            "--rpc-tls-server-name {:?} is not a DNS name or IP address",
            config.server_name
        )
    })?;
    RpcClientTls::new(
        server_name,
        certificates("--rpc-tls-ca", &config.server_roots)?,
        certificates("--rpc-tls-cert", &config.certificate)?,
        private_key(&config.key)?,
    )
    .map_err(|error| error.to_string())
}

/// The owner's certificate chain and private key for `serve`.
pub(crate) fn server_identity(
    config: &ServerTlsConfig,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
    Ok((
        certificates("--rpc-tls-cert", &config.certificate)?,
        private_key(&config.key)?,
    ))
}

/// Every certificate in a PEM file, in order; at least one.
fn certificates(option: &str, path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let bytes = read_pem(option, path)?;
    let certificates = CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("{option} {} is not valid PEM: {error}", path.display()))?;
    if certificates.is_empty() {
        return Err(format!(
            "{option} {} contains no certificate",
            path.display()
        ));
    }
    Ok(certificates)
}

fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let bytes = read_pem("--rpc-tls-key", path)?;
    // The error never quotes key bytes.
    PrivateKeyDer::from_pem_slice(&bytes).map_err(|_| {
        format!(
            "--rpc-tls-key {} contains no PKCS#8, PKCS#1, or SEC1 private key",
            path.display()
        )
    })
}

pub(crate) fn read_pem(option: &str, path: &Path) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| file.take(MAX_PEM_FILE_BYTES + 1).read_to_end(&mut bytes))
        .map_err(|error| format!("{option} {} cannot be read: {error}", path.display()))?;
    if bytes.len() as u64 > MAX_PEM_FILE_BYTES {
        return Err(format!("{option} {} is larger than 1 MiB", path.display()));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};

    use super::*;

    /// A CA PEM, and a certificate it signs with its key, in `directory`.
    fn write_pki(directory: &Path) -> ClientTlsConfig {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
        let key = KeyPair::generate().unwrap();
        let certificate = CertificateParams::new(vec!["agent".to_owned()])
            .unwrap()
            .signed_by(&key, &ca)
            .unwrap();
        let file = |name: &str, contents: String| {
            let path = directory.join(name);
            std::fs::write(&path, contents).unwrap();
            path
        };
        ClientTlsConfig {
            server_roots: file("ca.pem", ca.pem()),
            certificate: file("agent.pem", certificate.pem()),
            key: file("agent.key", key.serialize_pem()),
            server_name: "owner.test".to_owned(),
        }
    }

    #[test]
    fn client_tls_loads_roots_certificate_key_and_name() {
        let directory = tempfile::tempdir().unwrap();
        let config = write_pki(directory.path());
        client_tls(&config).unwrap();
        let (chain, _) = server_identity(&ServerTlsConfig {
            certificate: config.certificate,
            key: config.key,
        })
        .unwrap();
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn unusable_tls_files_are_named_without_quoting_key_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let config = write_pki(directory.path());
        let key_text = std::fs::read_to_string(&config.key).unwrap();
        let with = |change: &dyn Fn(&mut ClientTlsConfig)| {
            let mut changed = config.clone();
            change(&mut changed);
            client_tls(&changed).unwrap_err()
        };

        let missing = with(&|config| config.server_roots = PathBuf::from("/nonexistent/ca.pem"));
        assert!(missing.contains("--rpc-tls-ca") && missing.contains("cannot be read"));

        let no_certificate = with(&|config| config.certificate = config.key.clone());
        assert!(
            no_certificate.contains("contains no certificate"),
            "{no_certificate}"
        );

        let no_key = with(&|config| config.key = config.certificate.clone());
        assert!(no_key.contains("contains no PKCS#8"), "{no_key}");
        assert!(!no_key.contains(key_text.lines().nth(1).unwrap()));

        let bad_name = with(&|config| config.server_name = "not a name".to_owned());
        assert!(bad_name.contains("--rpc-tls-server-name"), "{bad_name}");

        let oversized = directory.path().join("large.pem");
        std::fs::write(&oversized, vec![b'a'; 1024 * 1024 + 1]).unwrap();
        let large = with(&|config| config.server_roots = oversized.clone());
        assert!(large.contains("larger than 1 MiB"), "{large}");
    }
}
