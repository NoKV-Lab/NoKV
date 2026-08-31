/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[cfg(unix)]
use std::ffi::OsString;

use percent_encoding::percent_decode_str;
use url::Url;

pub const MAX_FOUNDATIONDB_PREFIX_BYTES: usize = 64;

const HOLT_SCHEME: &str = "holt";
const FOUNDATIONDB_SCHEME: &str = "fdb";

/// One explicit metadata runtime selected by URL scheme.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataUrl {
    Holt(HoltMetadataUrl),
    FoundationDb(FoundationDbMetadataUrl),
}

impl MetadataUrl {
    pub fn as_holt(&self) -> Option<&HoltMetadataUrl> {
        match self {
            Self::Holt(url) => Some(url),
            Self::FoundationDb(_) => None,
        }
    }

    pub fn as_foundationdb(&self) -> Option<&FoundationDbMetadataUrl> {
        match self {
            Self::Holt(_) => None,
            Self::FoundationDb(url) => Some(url),
        }
    }
}

impl FromStr for MetadataUrl {
    type Err = MetadataUrlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed =
            Url::parse(value).map_err(|error| MetadataUrlError::InvalidUrl(error.to_string()))?;
        match parsed.scheme() {
            HOLT_SCHEME => parse_holt(parsed).map(Self::Holt),
            FOUNDATIONDB_SCHEME => parse_foundationdb(parsed).map(Self::FoundationDb),
            scheme => Err(MetadataUrlError::UnsupportedScheme(scheme.to_owned())),
        }
    }
}

/// Validated standalone Holt metadata location.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HoltMetadataUrl {
    path: PathBuf,
}

impl HoltMetadataUrl {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Validated FoundationDB cluster file and physical NoKV prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FoundationDbMetadataUrl {
    cluster_file: PathBuf,
    prefix: String,
}

impl FoundationDbMetadataUrl {
    pub fn cluster_file(&self) -> &Path {
        &self.cluster_file
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataUrlError {
    InvalidUrl(String),
    UnsupportedScheme(String),
    AuthorityNotAllowed {
        scheme: &'static str,
    },
    QueryNotAllowed {
        scheme: &'static str,
    },
    FragmentNotAllowed {
        scheme: &'static str,
    },
    InvalidPath {
        scheme: &'static str,
        reason: &'static str,
    },
    MissingFoundationDbPrefix,
    DuplicateFoundationDbPrefix,
    UnknownFoundationDbParameter(String),
    InvalidFoundationDbQueryEncoding,
    InvalidFoundationDbPrefixLength {
        bytes: usize,
        maximum: usize,
    },
}

impl fmt::Display for MetadataUrlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(message) => write!(formatter, "invalid metadata URL: {message}"),
            Self::UnsupportedScheme(scheme) => write!(
                formatter,
                "unsupported metadata URL scheme {scheme:?}; expected {HOLT_SCHEME:?} or \
                 {FOUNDATIONDB_SCHEME:?}"
            ),
            Self::AuthorityNotAllowed { scheme } => {
                write!(formatter, "{scheme} metadata URL authority must be empty")
            }
            Self::QueryNotAllowed { scheme } => {
                write!(formatter, "{scheme} metadata URL does not accept a query")
            }
            Self::FragmentNotAllowed { scheme } => {
                write!(
                    formatter,
                    "{scheme} metadata URL does not accept a fragment"
                )
            }
            Self::InvalidPath { scheme, reason } => {
                write!(formatter, "{scheme} metadata URL path {reason}")
            }
            Self::MissingFoundationDbPrefix => formatter.write_str(
                "fdb metadata URL requires exactly one non-empty prefix query parameter",
            ),
            Self::DuplicateFoundationDbPrefix => formatter
                .write_str("fdb metadata URL contains more than one prefix query parameter"),
            Self::UnknownFoundationDbParameter(parameter) => write!(
                formatter,
                "fdb metadata URL contains unknown query parameter {parameter:?}"
            ),
            Self::InvalidFoundationDbQueryEncoding => {
                formatter.write_str("fdb metadata URL query must use valid percent-encoded UTF-8")
            }
            Self::InvalidFoundationDbPrefixLength { bytes, maximum } => write!(
                formatter,
                "fdb metadata URL prefix is {bytes} bytes; expected 1 through {maximum} bytes"
            ),
        }
    }
}

impl std::error::Error for MetadataUrlError {}

fn parse_holt(parsed: Url) -> Result<HoltMetadataUrl, MetadataUrlError> {
    validate_empty_authority(&parsed, HOLT_SCHEME)?;
    validate_no_fragment(&parsed, HOLT_SCHEME)?;
    if parsed.query().is_some() {
        return Err(MetadataUrlError::QueryNotAllowed {
            scheme: HOLT_SCHEME,
        });
    }
    Ok(HoltMetadataUrl {
        path: decode_absolute_path(&parsed, HOLT_SCHEME)?,
    })
}

fn parse_foundationdb(parsed: Url) -> Result<FoundationDbMetadataUrl, MetadataUrlError> {
    validate_empty_authority(&parsed, FOUNDATIONDB_SCHEME)?;
    validate_no_fragment(&parsed, FOUNDATIONDB_SCHEME)?;
    let cluster_file = decode_absolute_path(&parsed, FOUNDATIONDB_SCHEME)?;
    if cluster_file.to_str().is_none() {
        return Err(MetadataUrlError::InvalidPath {
            scheme: FOUNDATIONDB_SCHEME,
            reason: "must be valid UTF-8",
        });
    }
    let prefix = parse_foundationdb_prefix(parsed.query())?;
    Ok(FoundationDbMetadataUrl {
        cluster_file,
        prefix,
    })
}

fn validate_empty_authority(parsed: &Url, scheme: &'static str) -> Result<(), MetadataUrlError> {
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.host().is_some()
        || parsed.port().is_some()
    {
        return Err(MetadataUrlError::AuthorityNotAllowed { scheme });
    }
    Ok(())
}

fn validate_no_fragment(parsed: &Url, scheme: &'static str) -> Result<(), MetadataUrlError> {
    if parsed.fragment().is_some() {
        return Err(MetadataUrlError::FragmentNotAllowed { scheme });
    }
    Ok(())
}

fn decode_absolute_path(parsed: &Url, scheme: &'static str) -> Result<PathBuf, MetadataUrlError> {
    let encoded = parsed.path();
    validate_percent_encoding(encoded).map_err(|()| MetadataUrlError::InvalidPath {
        scheme,
        reason: "must use valid percent encoding",
    })?;
    let decoded = percent_decode_str(encoded).collect::<Vec<_>>();
    if decoded.is_empty() {
        return Err(MetadataUrlError::InvalidPath {
            scheme,
            reason: "must not be empty",
        });
    }
    if decoded.contains(&0) {
        return Err(MetadataUrlError::InvalidPath {
            scheme,
            reason: "must not contain NUL",
        });
    }
    let path = path_from_bytes(decoded).map_err(|()| MetadataUrlError::InvalidPath {
        scheme,
        reason: "must be valid UTF-8 on this platform",
    })?;
    if !path.is_absolute() {
        return Err(MetadataUrlError::InvalidPath {
            scheme,
            reason: "must be absolute",
        });
    }
    Ok(path)
}

#[cfg(unix)]
fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf, ()> {
    use std::os::unix::ffi::OsStringExt;

    Ok(PathBuf::from(OsString::from_vec(bytes)))
}

#[cfg(not(unix))]
fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf, ()> {
    String::from_utf8(bytes).map(PathBuf::from).map_err(|_| ())
}

fn parse_foundationdb_prefix(query: Option<&str>) -> Result<String, MetadataUrlError> {
    let query = query.ok_or(MetadataUrlError::MissingFoundationDbPrefix)?;
    let mut prefix = None;
    for component in query.split('&') {
        if component.is_empty() {
            return Err(MetadataUrlError::InvalidFoundationDbQueryEncoding);
        }
        let (encoded_name, encoded_value) = component.split_once('=').unwrap_or((component, ""));
        let name = decode_query_component(encoded_name)?;
        if name != "prefix" {
            return Err(MetadataUrlError::UnknownFoundationDbParameter(name));
        }
        if prefix.is_some() {
            return Err(MetadataUrlError::DuplicateFoundationDbPrefix);
        }
        prefix = Some(decode_query_component(encoded_value)?);
    }

    let prefix = prefix.ok_or(MetadataUrlError::MissingFoundationDbPrefix)?;
    let bytes = prefix.len();
    if !(1..=MAX_FOUNDATIONDB_PREFIX_BYTES).contains(&bytes) {
        return Err(MetadataUrlError::InvalidFoundationDbPrefixLength {
            bytes,
            maximum: MAX_FOUNDATIONDB_PREFIX_BYTES,
        });
    }
    Ok(prefix)
}

fn decode_query_component(encoded: &str) -> Result<String, MetadataUrlError> {
    validate_percent_encoding(encoded)
        .map_err(|()| MetadataUrlError::InvalidFoundationDbQueryEncoding)?;
    let form_encoded = encoded.replace('+', " ");
    percent_decode_str(&form_encoded)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| MetadataUrlError::InvalidFoundationDbQueryEncoding)
}

fn validate_percent_encoding(encoded: &str) -> Result<(), ()> {
    let bytes = encoded.as_bytes();
    let mut offset = 0;
    while offset < bytes.len() {
        if bytes[offset] != b'%' {
            offset += 1;
            continue;
        }
        let encoded_byte = bytes.get(offset + 1..offset + 3).ok_or(())?;
        if !encoded_byte.iter().all(u8::is_ascii_hexdigit) {
            return Err(());
        }
        offset += 3;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(value: &str) -> Result<MetadataUrl, MetadataUrlError> {
        value.parse()
    }

    #[test]
    fn parses_absolute_holt_path() {
        let parsed = parse("holt:///var/lib/nokv%20metadata").unwrap();
        assert_eq!(
            parsed.as_holt().unwrap().path(),
            Path::new("/var/lib/nokv metadata")
        );
        assert!(parsed.as_foundationdb().is_none());
    }

    #[test]
    fn parses_foundationdb_cluster_and_prefix() {
        let parsed = parse("fdb:///etc/foundationdb/main%20cluster?prefix=nokv-prod").unwrap();
        let fdb = parsed.as_foundationdb().unwrap();
        assert_eq!(
            fdb.cluster_file(),
            Path::new("/etc/foundationdb/main cluster")
        );
        assert_eq!(fdb.prefix(), "nokv-prod");
        assert!(parsed.as_holt().is_none());
    }

    #[test]
    fn decodes_utf8_and_form_encoded_foundationdb_prefixes() {
        let unicode = parse("fdb:///etc/fdb.cluster?prefix=nokv-%E7%94%9F%E4%BA%A7").unwrap();
        assert_eq!(unicode.as_foundationdb().unwrap().prefix(), "nokv-生产");

        let spaced = parse("fdb:///etc/fdb.cluster?prefix=nokv+prod").unwrap();
        assert_eq!(spaced.as_foundationdb().unwrap().prefix(), "nokv prod");
    }

    #[test]
    fn rejects_unsupported_scheme() {
        assert_eq!(
            parse("etcd:///tmp/control"),
            Err(MetadataUrlError::UnsupportedScheme("etcd".to_owned()))
        );
    }

    #[test]
    fn rejects_nonempty_authorities() {
        for value in [
            "holt://node.example/var/lib/nokv",
            "fdb://node.example/etc/fdb.cluster?prefix=nokv",
            "fdb://user@node.example/etc/fdb.cluster?prefix=nokv",
            "fdb://node.example:4500/etc/fdb.cluster?prefix=nokv",
        ] {
            assert!(matches!(
                parse(value),
                Err(MetadataUrlError::AuthorityNotAllowed { .. })
            ));
        }
    }

    #[test]
    fn rejects_empty_relative_invalid_and_nul_paths() {
        for value in [
            "holt:",
            "holt:relative/path",
            "holt:///tmp/%GG",
            "holt:///tmp/%00",
            "fdb:relative/path?prefix=nokv",
            "fdb:///tmp/%GG?prefix=nokv",
            "fdb:///tmp/%00?prefix=nokv",
        ] {
            assert!(matches!(
                parse(value),
                Err(MetadataUrlError::InvalidPath { .. }) | Err(MetadataUrlError::InvalidUrl(_))
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn permits_non_utf8_holt_path_but_rejects_non_utf8_cluster_file() {
        assert!(parse("holt:///var/lib/nokv-%FF").is_ok());
        assert!(matches!(
            parse("fdb:///etc/fdb-%FF.cluster?prefix=nokv"),
            Err(MetadataUrlError::InvalidPath {
                scheme: FOUNDATIONDB_SCHEME,
                reason: "must be valid UTF-8"
            })
        ));
    }

    #[test]
    fn rejects_holt_query_and_fragments() {
        assert_eq!(
            parse("holt:///var/lib/nokv?prefix=no"),
            Err(MetadataUrlError::QueryNotAllowed {
                scheme: HOLT_SCHEME
            })
        );
        assert_eq!(
            parse("holt:///var/lib/nokv#fragment"),
            Err(MetadataUrlError::FragmentNotAllowed {
                scheme: HOLT_SCHEME
            })
        );
    }

    #[test]
    fn requires_exactly_one_known_foundationdb_parameter() {
        assert_eq!(
            parse("fdb:///etc/fdb.cluster"),
            Err(MetadataUrlError::MissingFoundationDbPrefix)
        );
        assert_eq!(
            parse("fdb:///etc/fdb.cluster?prefix=a&prefix=b"),
            Err(MetadataUrlError::DuplicateFoundationDbPrefix)
        );
        assert_eq!(
            parse("fdb:///etc/fdb.cluster?namespace=nokv"),
            Err(MetadataUrlError::UnknownFoundationDbParameter(
                "namespace".to_owned()
            ))
        );
        assert_eq!(
            parse("fdb:///etc/fdb.cluster?prefix=nokv&"),
            Err(MetadataUrlError::InvalidFoundationDbQueryEncoding)
        );
    }

    #[test]
    fn rejects_invalid_foundationdb_prefix_encoding_and_length() {
        assert_eq!(
            parse("fdb:///etc/fdb.cluster?prefix="),
            Err(MetadataUrlError::InvalidFoundationDbPrefixLength {
                bytes: 0,
                maximum: MAX_FOUNDATIONDB_PREFIX_BYTES,
            })
        );
        assert_eq!(
            parse("fdb:///etc/fdb.cluster?prefix=%FF"),
            Err(MetadataUrlError::InvalidFoundationDbQueryEncoding)
        );
        assert_eq!(
            parse("fdb:///etc/fdb.cluster?prefix=%GG"),
            Err(MetadataUrlError::InvalidFoundationDbQueryEncoding)
        );

        let too_long = "x".repeat(MAX_FOUNDATIONDB_PREFIX_BYTES + 1);
        assert_eq!(
            parse(&format!("fdb:///etc/fdb.cluster?prefix={too_long}")),
            Err(MetadataUrlError::InvalidFoundationDbPrefixLength {
                bytes: MAX_FOUNDATIONDB_PREFIX_BYTES + 1,
                maximum: MAX_FOUNDATIONDB_PREFIX_BYTES,
            })
        );
    }

    #[test]
    fn rejects_foundationdb_fragments() {
        assert_eq!(
            parse("fdb:///etc/fdb.cluster?prefix=nokv#fragment"),
            Err(MetadataUrlError::FragmentNotAllowed {
                scheme: FOUNDATIONDB_SCHEME
            })
        );
    }
}
