/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_client::{ArtifactPublishOutcome, ArtifactReadOutcome};
use nokv_protocol::{
    AggregateFunction, AggregateResult, AggregateSpec, CatalogResult, CommitResult, FacetResult,
    FieldValue, FindWorkspacesResult, PathListEntry, PathMetadata, PathPage, PublishResult,
    QueryOperand, QueryOperator, QueryPredicate, ScalarValue, SearchResult, SearchRow,
    SnapshotResult, SnapshotStatus, SortDirection, SortField, WorkspaceSummary,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

pub(crate) type PythonFieldSpec = (String, String, String);
pub(crate) type PythonPredicateSpec = (String, String, String, String);
pub(crate) type PythonSortSpec = (String, String);
pub(crate) type PythonAggregateSpec = (String, Option<String>, String);

pub(crate) fn parse_fixed_hex<const WIDTH: usize>(
    field: &str,
    encoded: &str,
) -> PyResult<[u8; WIDTH]> {
    if encoded.len() != WIDTH * 2
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PyValueError::new_err(format!(
            "{field} must contain exactly {} lower-case hexadecimal characters",
            WIDTH * 2
        )));
    }
    let mut decoded = [0_u8; WIDTH];
    for (index, pair) in encoded.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        decoded[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(decoded)
}

pub(crate) fn parse_field_specs(specs: Vec<PythonFieldSpec>) -> PyResult<Vec<FieldValue>> {
    specs
        .into_iter()
        .map(|(field_id, kind, value)| {
            let field = FieldValue {
                field_id,
                value: parse_scalar(&kind, &value)?,
            };
            field
                .validate()
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
            Ok(field)
        })
        .collect()
}

pub(crate) fn parse_predicates(specs: Vec<PythonPredicateSpec>) -> PyResult<Vec<QueryPredicate>> {
    specs
        .into_iter()
        .map(|(field_id, operator, kind, value)| {
            let operator = parse_query_operator(&operator)?;
            let operand = match operator {
                QueryOperator::Exists | QueryOperator::NotExists => {
                    if !kind.is_empty() || !value.is_empty() {
                        return Err(PyValueError::new_err(
                            "exists and not_exists predicates require empty kind and value strings",
                        ));
                    }
                    QueryOperand::None
                }
                QueryOperator::In => {
                    return Err(PyValueError::new_err(
                        "in predicates require a set-valued API and are not accepted by the scalar tuple surface",
                    ));
                }
                _ => QueryOperand::Scalar(parse_scalar(&kind, &value)?),
            };
            Ok(QueryPredicate {
                field_id,
                operator,
                operand,
            })
        })
        .collect()
}

pub(crate) fn parse_sort(specs: Vec<PythonSortSpec>) -> PyResult<Vec<SortField>> {
    specs
        .into_iter()
        .map(|(field_id, direction)| {
            Ok(SortField {
                field_id,
                direction: match direction.as_str() {
                    "ascending" => SortDirection::Ascending,
                    "descending" => SortDirection::Descending,
                    _ => {
                        return Err(PyValueError::new_err(format!(
                            "invalid sort direction {direction:?}; expected 'ascending' or 'descending'"
                        )));
                    }
                },
            })
        })
        .collect()
}

pub(crate) fn parse_aggregates(specs: Vec<PythonAggregateSpec>) -> PyResult<Vec<AggregateSpec>> {
    specs
        .into_iter()
        .map(|(function, field_id, result_id)| {
            Ok(AggregateSpec {
                function: match function.as_str() {
                    "count" => AggregateFunction::Count,
                    "sum" => AggregateFunction::Sum,
                    "average" => AggregateFunction::Average,
                    "minimum" => AggregateFunction::Minimum,
                    "maximum" => AggregateFunction::Maximum,
                    _ => {
                        return Err(PyValueError::new_err(format!(
                            "invalid aggregate function {function:?}"
                        )));
                    }
                },
                field_id,
                result_id,
            })
        })
        .collect()
}

fn parse_query_operator(operator: &str) -> PyResult<QueryOperator> {
    match operator {
        "equal" => Ok(QueryOperator::Equal),
        "not_equal" => Ok(QueryOperator::NotEqual),
        "in" => Ok(QueryOperator::In),
        "less" => Ok(QueryOperator::Less),
        "less_or_equal" => Ok(QueryOperator::LessOrEqual),
        "greater" => Ok(QueryOperator::Greater),
        "greater_or_equal" => Ok(QueryOperator::GreaterOrEqual),
        "prefix" => Ok(QueryOperator::Prefix),
        "suffix" => Ok(QueryOperator::Suffix),
        "contains" => Ok(QueryOperator::Contains),
        "exists" => Ok(QueryOperator::Exists),
        "not_exists" => Ok(QueryOperator::NotExists),
        _ => Err(PyValueError::new_err(format!(
            "invalid query operator {operator:?}"
        ))),
    }
}

fn parse_scalar(kind: &str, value: &str) -> PyResult<ScalarValue> {
    let scalar = match kind {
        "null" if value.is_empty() => ScalarValue::Null,
        "null" => {
            return Err(PyValueError::new_err(
                "null scalar values require an empty encoded value",
            ));
        }
        "boolean" => match value {
            "true" => ScalarValue::Boolean(true),
            "false" => ScalarValue::Boolean(false),
            _ => {
                return Err(PyValueError::new_err(
                    "boolean scalar values must be 'true' or 'false'",
                ));
            }
        },
        "signed" => ScalarValue::Signed(parse_number("signed", value)?),
        "unsigned" => ScalarValue::Unsigned(parse_number("unsigned", value)?),
        "decimal" => ScalarValue::Decimal(value.to_owned()),
        "timestamp" => ScalarValue::Timestamp(parse_number("timestamp", value)?),
        "string" => ScalarValue::String(value.to_owned()),
        "bytes" => ScalarValue::Bytes(parse_variable_hex("bytes", value)?),
        _ => {
            return Err(PyValueError::new_err(format!(
                "invalid scalar kind {kind:?}; expected null, boolean, signed, unsigned, decimal, timestamp, string, or bytes"
            )));
        }
    };
    scalar
        .validate()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(scalar)
}

fn parse_number<T>(kind: &str, value: &str) -> PyResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse::<T>().map_err(|error| {
        PyValueError::new_err(format!("invalid {kind} scalar value {value:?}: {error}"))
    })
}

fn parse_variable_hex(field: &str, encoded: &str) -> PyResult<Vec<u8>> {
    if !encoded.len().is_multiple_of(2)
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PyValueError::new_err(format!(
            "{field} must contain an even number of lower-case hexadecimal characters"
        )));
    }
    Ok(encoded
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect())
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("hex input was validated"),
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(crate) fn workspace_summary_to_py<'py>(
    py: Python<'py>,
    summary: &WorkspaceSummary,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("workbench", summary.workbench.as_str())?;
    dict.set_item(
        "workspace_incarnation_id",
        hex(&summary.workspace_incarnation_id.0),
    )?;
    dict.set_item("workspace_revision", summary.workspace_revision)?;
    set_optional_hex(
        &dict,
        "commit_head",
        summary
            .commit_head
            .as_ref()
            .map(|identity| identity.0.as_slice()),
    )?;
    set_optional_u64(
        py,
        &dict,
        "commit_head_generation",
        summary.commit_head_generation,
    )?;
    Ok(dict)
}

pub(crate) fn snapshot_result_to_py<'py>(
    py: Python<'py>,
    snapshot: &SnapshotResult,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("snapshot_id", snapshot.snapshot_id)?;
    dict.set_item("workbench", snapshot.workbench.as_str())?;
    dict.set_item(
        "workspace_incarnation_id",
        hex(&snapshot.workspace_incarnation_id.0),
    )?;
    dict.set_item("read_version", snapshot.read_version)?;
    dict.set_item("lease_deadline_ms", snapshot.lease_deadline_ms)?;
    set_optional_string(
        py,
        &dict,
        "alias",
        snapshot.alias.as_ref().map(|alias| alias.as_str()),
    )?;
    dict.set_item("annotation", PyBytes::new(py, &snapshot.annotation))?;
    set_optional_bytes(
        py,
        &dict,
        "retire_annotation",
        snapshot.retire_annotation.as_deref(),
    )?;
    dict.set_item(
        "status",
        match snapshot.status {
            SnapshotStatus::Alive => "alive",
            SnapshotStatus::Expired => "expired",
            SnapshotStatus::ReapClaimed => "reap_claimed",
            SnapshotStatus::Retired => "retired",
            SnapshotStatus::Reaped => "reaped",
        },
    )?;
    dict.set_item("consumer_count", snapshot.consumer_count)?;
    Ok(dict)
}

pub(crate) fn path_metadata_to_py<'py>(
    py: Python<'py>,
    metadata: &PathMetadata,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("workbench", metadata.path.workbench.as_str())?;
    dict.set_item("path", metadata.path.path.as_str())?;
    dict.set_item(
        "workspace_incarnation_id",
        hex(&metadata.workspace_incarnation_id.0),
    )?;
    dict.set_item("workspace_revision", metadata.workspace_revision)?;
    dict.set_item("generation", metadata.generation)?;
    dict.set_item(
        "artifact_revision_id",
        hex(&metadata.artifact_revision_id.0),
    )?;
    dict.set_item("dependency_count", metadata.dependency_count)?;
    dict.set_item("dependency_depth", metadata.dependency_depth)?;
    dict.set_item("logical_size", metadata.descriptor.logical_size)?;
    dict.set_item("body_digest", metadata.descriptor.body_digest.as_str())?;
    dict.set_item(
        "manifest_digest",
        metadata.descriptor.manifest_digest.as_str(),
    )?;
    dict.set_item("content_type", metadata.descriptor.content_type.as_str())?;
    set_optional_string(
        py,
        &dict,
        "producer",
        metadata.descriptor.producer.as_deref(),
    )?;
    set_optional_string(
        py,
        &dict,
        "manifest_identity",
        metadata.descriptor.manifest_identity.as_deref(),
    )?;
    dict.set_item(
        "index_fields",
        field_values_to_py(py, &metadata.descriptor.index_fields)?,
    )?;
    Ok(dict)
}

pub(crate) fn path_page_to_py<'py>(
    py: Python<'py>,
    page: &PathPage,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let entries = PyList::empty(py);
    for entry in &page.entries {
        let item = match entry {
            PathListEntry::Artifact(metadata) => {
                let item = path_metadata_to_py(py, metadata)?;
                item.set_item("kind", "artifact")?;
                item
            }
            PathListEntry::Prefix(path) => {
                let item = PyDict::new(py);
                item.set_item("kind", "prefix")?;
                item.set_item("workbench", path.workbench.as_str())?;
                item.set_item("path", path.path.as_str())?;
                item
            }
        };
        entries.append(item)?;
    }
    dict.set_item("entries", entries)?;
    set_optional_bytes(py, &dict, "next_cursor", page.next_cursor.as_deref())?;
    dict.set_item("read_version", page.read_version)?;
    Ok(dict)
}

pub(crate) fn publish_result_to_py<'py>(
    py: Python<'py>,
    result: &PublishResult,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("operation_id", hex(&result.operation_id.0))?;
    dict.set_item("workbench", result.target.workbench.as_str())?;
    dict.set_item("path", result.target.path.as_str())?;
    dict.set_item("workspace_revision", result.workspace_revision)?;
    dict.set_item("generation", result.generation)?;
    dict.set_item("artifact_revision_id", hex(&result.artifact_revision_id.0))?;
    dict.set_item("logical_size", result.logical_size)?;
    dict.set_item("body_digest", result.body_digest.as_str())?;
    Ok(dict)
}

pub(crate) fn publish_outcome_to_py<'py>(
    py: Python<'py>,
    outcome: &ArtifactPublishOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = publish_result_to_py(py, &outcome.publication.value)?;
    dict.set_item("commit_version", outcome.publication.commit_version)?;
    dict.set_item("replayed", outcome.publication.replayed)?;
    let upload = PyDict::new(py);
    upload.set_item("blocks", outcome.upload_stats.blocks)?;
    upload.set_item("bytes", outcome.upload_stats.bytes)?;
    upload.set_item("created", outcome.upload_stats.created)?;
    upload.set_item("replayed", outcome.upload_stats.replayed)?;
    dict.set_item("upload", upload)?;
    Ok(dict)
}

pub(crate) fn read_outcome_to_py<'py>(
    py: Python<'py>,
    outcome: &ArtifactReadOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("metadata", path_metadata_to_py(py, &outcome.metadata)?)?;
    dict.set_item("bytes", PyBytes::new(py, &outcome.bytes))?;
    let stats = PyDict::new(py);
    stats.set_item("planned_blocks", outcome.stats.planned_blocks)?;
    stats.set_item("cache_hits", outcome.stats.cache_hits)?;
    stats.set_item("cache_misses", outcome.stats.cache_misses)?;
    stats.set_item("store_reads", outcome.stats.store_reads)?;
    stats.set_item("store_read_bytes", outcome.stats.store_read_bytes)?;
    stats.set_item("verified_blocks", outcome.stats.verified_blocks)?;
    dict.set_item("stats", stats)?;
    Ok(dict)
}

pub(crate) fn search_result_to_py<'py>(
    py: Python<'py>,
    result: &SearchResult,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let hits = PyList::empty(py);
    for row in &result.hits {
        let SearchRow::Artifact(hit) = row else {
            return Err(PyRuntimeError::new_err(
                "ArtifactV1 search returned a generic custom-index row",
            ));
        };
        let item = PyDict::new(py);
        item.set_item("metadata", path_metadata_to_py(py, &hit.metadata)?)?;
        item.set_item("projection", field_values_to_py(py, &hit.projection)?)?;
        hits.append(item)?;
    }
    dict.set_item("hits", hits)?;
    dict.set_item("facets", facet_results_to_py(py, &result.facets)?)?;
    set_optional_bytes(py, &dict, "next_cursor", result.next_cursor.as_deref())?;
    dict.set_item("read_version", result.read_version)?;
    Ok(dict)
}

pub(crate) fn aggregate_result_to_py<'py>(
    py: Python<'py>,
    result: &AggregateResult,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let groups = PyList::empty(py);
    for group in &result.groups {
        let item = PyDict::new(py);
        item.set_item("keys", field_values_to_py(py, &group.keys)?)?;
        item.set_item("values", field_values_to_py(py, &group.values)?)?;
        groups.append(item)?;
    }
    dict.set_item("groups", groups)?;
    set_optional_bytes(py, &dict, "next_cursor", result.next_cursor.as_deref())?;
    dict.set_item("read_version", result.read_version)?;
    Ok(dict)
}

pub(crate) fn catalog_result_to_py<'py>(
    py: Python<'py>,
    result: &CatalogResult,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let fields = PyList::empty(py);
    for field in &result.fields {
        let item = PyDict::new(py);
        item.set_item("field_id", &field.field_id)?;
        item.set_item("scalar_type", &field.scalar_type)?;
        item.set_item(
            "operators",
            field
                .operators
                .iter()
                .copied()
                .map(query_operator_name)
                .collect::<Vec<_>>(),
        )?;
        item.set_item("sortable", field.sortable)?;
        item.set_item("facetable", field.facetable)?;
        item.set_item("aggregatable", field.aggregatable)?;
        fields.append(item)?;
    }
    dict.set_item("fields", fields)?;
    set_optional_bytes(py, &dict, "next_cursor", result.next_cursor.as_deref())?;
    dict.set_item("read_version", result.read_version)?;
    Ok(dict)
}

pub(crate) fn find_workspaces_result_to_py<'py>(
    py: Python<'py>,
    result: &FindWorkspacesResult,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let workspaces = PyList::empty(py);
    for result in &result.workspaces {
        let item = PyDict::new(py);
        item.set_item("workspace", workspace_summary_to_py(py, &result.workspace)?)?;
        match &result.commit {
            Some(commit) => item.set_item("commit", commit_result_to_py(py, commit)?)?,
            None => item.set_item("commit", py.None())?,
        }
        workspaces.append(item)?;
    }
    dict.set_item("workspaces", workspaces)?;
    set_optional_bytes(py, &dict, "next_cursor", result.next_cursor.as_deref())?;
    dict.set_item("read_version", result.read_version)?;
    Ok(dict)
}

fn commit_result_to_py<'py>(
    py: Python<'py>,
    result: &CommitResult,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("operation_id", hex(&result.operation_id.0))?;
    dict.set_item("commit_id", hex(&result.commit_id.0))?;
    dict.set_item("workbench", result.workbench.as_str())?;
    dict.set_item("head_generation", result.head_generation)?;
    dict.set_item("member_count", result.member_count)?;
    dict.set_item("member_digest", hex(&result.member_digest.0))?;
    Ok(dict)
}

fn field_values_to_py<'py>(py: Python<'py>, fields: &[FieldValue]) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for field in fields {
        let item = PyDict::new(py);
        item.set_item("field_id", &field.field_id)?;
        let (kind, value) = scalar_to_py(py, &field.value)?;
        item.set_item("kind", kind)?;
        item.set_item("value", value)?;
        list.append(item)?;
    }
    Ok(list)
}

fn facet_results_to_py<'py>(
    py: Python<'py>,
    facets: &[FacetResult],
) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for facet in facets {
        let item = PyDict::new(py);
        item.set_item("field_id", &facet.field_id)?;
        let buckets = PyList::empty(py);
        for bucket in &facet.buckets {
            let bucket_item = PyDict::new(py);
            let (kind, value) = scalar_to_py(py, &bucket.value)?;
            bucket_item.set_item("kind", kind)?;
            bucket_item.set_item("value", value)?;
            bucket_item.set_item("count", bucket.count)?;
            buckets.append(bucket_item)?;
        }
        item.set_item("buckets", buckets)?;
        item.set_item("distinct_count", facet.distinct_count)?;
        item.set_item("truncated", facet.truncated)?;
        list.append(item)?;
    }
    Ok(list)
}

fn scalar_to_py<'py>(py: Python<'py>, scalar: &ScalarValue) -> PyResult<(&'static str, Py<PyAny>)> {
    let (kind, value) = match scalar {
        ScalarValue::Null => ("null", py.None()),
        ScalarValue::Boolean(value) => (
            "boolean",
            value.into_pyobject(py)?.to_owned().unbind().into_any(),
        ),
        ScalarValue::Signed(value) => (
            "signed",
            value.into_pyobject(py)?.to_owned().unbind().into_any(),
        ),
        ScalarValue::Unsigned(value) => (
            "unsigned",
            value.into_pyobject(py)?.to_owned().unbind().into_any(),
        ),
        ScalarValue::Decimal(value) => (
            "decimal",
            value.into_pyobject(py)?.to_owned().unbind().into_any(),
        ),
        ScalarValue::Timestamp(value) => (
            "timestamp",
            value.into_pyobject(py)?.to_owned().unbind().into_any(),
        ),
        ScalarValue::String(value) => (
            "string",
            value.into_pyobject(py)?.to_owned().unbind().into_any(),
        ),
        ScalarValue::Bytes(value) => ("bytes", PyBytes::new(py, value).unbind().into_any()),
    };
    Ok((kind, value))
}

fn query_operator_name(operator: QueryOperator) -> &'static str {
    match operator {
        QueryOperator::Equal => "equal",
        QueryOperator::NotEqual => "not_equal",
        QueryOperator::In => "in",
        QueryOperator::Less => "less",
        QueryOperator::LessOrEqual => "less_or_equal",
        QueryOperator::Greater => "greater",
        QueryOperator::GreaterOrEqual => "greater_or_equal",
        QueryOperator::Prefix => "prefix",
        QueryOperator::Suffix => "suffix",
        QueryOperator::Contains => "contains",
        QueryOperator::Exists => "exists",
        QueryOperator::NotExists => "not_exists",
    }
}

fn set_optional_bytes(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    key: &str,
    value: Option<&[u8]>,
) -> PyResult<()> {
    match value {
        Some(value) => dict.set_item(key, PyBytes::new(py, value)),
        None => dict.set_item(key, py.None()),
    }
}

fn set_optional_hex(dict: &Bound<'_, PyDict>, key: &str, value: Option<&[u8]>) -> PyResult<()> {
    match value {
        Some(value) => dict.set_item(key, hex(value)),
        None => dict.set_item(key, dict.py().None()),
    }
}

fn set_optional_string(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    key: &str,
    value: Option<&str>,
) -> PyResult<()> {
    match value {
        Some(value) => dict.set_item(key, value),
        None => dict.set_item(key, py.None()),
    }
}

fn set_optional_u64(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    key: &str,
    value: Option<u64>,
) -> PyResult<()> {
    match value {
        Some(value) => dict.set_item(key, value),
        None => dict.set_item(key, py.None()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_protocol::{
        ArtifactDescriptor, ArtifactRevisionIdentity, ContentType, DigestUri, GenericNamespaceHit,
        GenericNamespaceKind, RelativePath, SearchHit, SearchRow, WorkbenchName, WorkspaceIdentity,
        WorkspacePath,
    };

    fn dict_item<'py>(dict: &Bound<'py, PyDict>, key: &str) -> Bound<'py, PyAny> {
        dict.get_item(key)
            .unwrap()
            .unwrap_or_else(|| panic!("missing Python dictionary key {key:?}"))
    }

    #[test]
    fn fixed_identity_requires_canonical_lower_hex() {
        assert_eq!(parse_fixed_hex::<2>("id", "00ff").unwrap(), [0, 255]);
        assert!(parse_fixed_hex::<2>("id", "00FF").is_err());
        assert!(parse_fixed_hex::<2>("id", "0ff").is_err());
    }

    #[test]
    fn typed_scalar_specs_preserve_timestamp_and_bytes() {
        let fields = parse_field_specs(vec![
            (
                "run.bytes".to_owned(),
                "bytes".to_owned(),
                "00ff".to_owned(),
            ),
            (
                "run.timestamp".to_owned(),
                "timestamp".to_owned(),
                "1721234567".to_owned(),
            ),
        ])
        .unwrap();
        assert_eq!(fields[0].value, ScalarValue::Bytes(vec![0, 255]));
        assert_eq!(fields[1].value, ScalarValue::Timestamp(1_721_234_567));
    }

    #[test]
    fn predicate_specs_use_the_current_operand_contract() {
        let predicates = parse_predicates(vec![
            (
                "run.state".to_owned(),
                "suffix".to_owned(),
                "string".to_owned(),
                "done".to_owned(),
            ),
            (
                "run.score".to_owned(),
                "not_exists".to_owned(),
                String::new(),
                String::new(),
            ),
        ])
        .unwrap();
        assert_eq!(
            predicates[0].operand,
            QueryOperand::Scalar(ScalarValue::String("done".to_owned()))
        );
        assert_eq!(predicates[1].operand, QueryOperand::None);
    }

    #[test]
    fn path_page_conversion_preserves_artifact_and_prefix_abi() {
        let workbench = WorkbenchName::new("run-42").unwrap();
        let path = |value: &str| WorkspacePath {
            workbench: workbench.clone(),
            path: RelativePath::new(value).unwrap(),
        };
        let page = PathPage {
            entries: vec![
                PathListEntry::Artifact(PathMetadata {
                    path: path("outputs/result.json"),
                    workspace_incarnation_id: WorkspaceIdentity([4; 16]),
                    workspace_revision: 7,
                    generation: 3,
                    artifact_revision_id: ArtifactRevisionIdentity([5; 16]),
                    dependency_count: 2,
                    dependency_depth: 1,
                    descriptor: ArtifactDescriptor {
                        logical_size: 42,
                        body_digest: DigestUri::new(format!("sha256:{}", "06".repeat(32))).unwrap(),
                        manifest_digest: DigestUri::new(format!("sha256:{}", "07".repeat(32)))
                            .unwrap(),
                        content_type: ContentType::new("application/json").unwrap(),
                        producer: Some("agent-runner".to_owned()),
                        manifest_identity: Some("manifest-42".to_owned()),
                        index_fields: vec![FieldValue {
                            field_id: "run.score".to_owned(),
                            value: ScalarValue::Unsigned(9),
                        }],
                    },
                }),
                PathListEntry::Prefix(path("outputs/nested")),
            ],
            next_cursor: Some(vec![0, 255, b'/']),
            read_version: 19,
        };

        Python::initialize();
        Python::attach(|py| {
            let converted = path_page_to_py(py, &page).unwrap();
            let entries = dict_item(&converted, "entries")
                .cast_into::<PyList>()
                .unwrap();
            assert_eq!(entries.len(), 2);

            let artifact = entries.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
            for (key, expected) in [
                ("kind", "artifact".to_owned()),
                ("workbench", "run-42".to_owned()),
                ("path", "outputs/result.json".to_owned()),
                ("workspace_incarnation_id", "04".repeat(16)),
                ("artifact_revision_id", "05".repeat(16)),
                ("body_digest", format!("sha256:{}", "06".repeat(32))),
                ("manifest_digest", format!("sha256:{}", "07".repeat(32))),
                ("content_type", "application/json".to_owned()),
                ("producer", "agent-runner".to_owned()),
                ("manifest_identity", "manifest-42".to_owned()),
            ] {
                assert_eq!(
                    dict_item(&artifact, key).extract::<String>().unwrap(),
                    expected,
                    "Python artifact field {key:?} changed"
                );
            }
            for (key, expected) in [
                ("workspace_revision", 7_u64),
                ("generation", 3),
                ("dependency_count", 2),
                ("dependency_depth", 1),
                ("logical_size", 42),
            ] {
                assert_eq!(
                    dict_item(&artifact, key).extract::<u64>().unwrap(),
                    expected,
                    "Python artifact field {key:?} changed"
                );
            }
            let index_fields = dict_item(&artifact, "index_fields")
                .cast_into::<PyList>()
                .unwrap();
            let index_field = index_fields
                .get_item(0)
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap();
            assert_eq!(
                dict_item(&index_field, "field_id")
                    .extract::<String>()
                    .unwrap(),
                "run.score"
            );
            assert_eq!(
                dict_item(&index_field, "kind").extract::<String>().unwrap(),
                "unsigned"
            );
            assert_eq!(
                dict_item(&index_field, "value").extract::<u64>().unwrap(),
                9
            );

            let prefix = entries.get_item(1).unwrap().cast_into::<PyDict>().unwrap();
            for (key, expected) in [
                ("kind", "prefix"),
                ("workbench", "run-42"),
                ("path", "outputs/nested"),
            ] {
                assert_eq!(
                    dict_item(&prefix, key).extract::<String>().unwrap(),
                    expected,
                    "Python prefix field {key:?} changed"
                );
            }

            assert_eq!(
                dict_item(&converted, "next_cursor")
                    .extract::<Vec<u8>>()
                    .unwrap(),
                vec![0, 255, b'/']
            );
            assert_eq!(
                dict_item(&converted, "read_version")
                    .extract::<u64>()
                    .unwrap(),
                19
            );
        });
    }

    #[test]
    fn artifact_search_conversion_preserves_the_python_result_abi() {
        let metadata = PathMetadata {
            path: WorkspacePath {
                workbench: WorkbenchName::new("run-42").unwrap(),
                path: RelativePath::new("outputs/result.json").unwrap(),
            },
            workspace_incarnation_id: WorkspaceIdentity([4; 16]),
            workspace_revision: 7,
            generation: 3,
            artifact_revision_id: ArtifactRevisionIdentity([5; 16]),
            dependency_count: 2,
            dependency_depth: 1,
            descriptor: ArtifactDescriptor {
                logical_size: 42,
                body_digest: DigestUri::new(format!("sha256:{}", "06".repeat(32))).unwrap(),
                manifest_digest: DigestUri::new(format!("sha256:{}", "07".repeat(32))).unwrap(),
                content_type: ContentType::new("application/json").unwrap(),
                producer: Some("agent-runner".to_owned()),
                manifest_identity: Some("manifest-42".to_owned()),
                index_fields: Vec::new(),
            },
        };
        let result = SearchResult {
            hits: vec![SearchRow::Artifact(SearchHit {
                metadata,
                projection: vec![FieldValue {
                    field_id: "run.score".to_owned(),
                    value: ScalarValue::Unsigned(9),
                }],
            })],
            match_count: 1,
            facets: Vec::new(),
            next_cursor: Some(vec![0, 255]),
            read_version: 19,
        };

        Python::initialize();
        Python::attach(|py| {
            let converted = search_result_to_py(py, &result).unwrap();
            assert_eq!(converted.len(), 4, "the direct Python result ABI changed");
            let hits = dict_item(&converted, "hits").cast_into::<PyList>().unwrap();
            let hit = hits.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
            assert_eq!(hit.len(), 2, "the direct Python hit ABI changed");
            let metadata = dict_item(&hit, "metadata").cast_into::<PyDict>().unwrap();
            assert_eq!(
                dict_item(&metadata, "path").extract::<String>().unwrap(),
                "outputs/result.json"
            );
            let projection = dict_item(&hit, "projection").cast_into::<PyList>().unwrap();
            let field = projection
                .get_item(0)
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap();
            assert_eq!(
                dict_item(&field, "field_id").extract::<String>().unwrap(),
                "run.score"
            );
            assert_eq!(dict_item(&field, "value").extract::<u64>().unwrap(), 9);
        });
    }

    #[test]
    fn artifact_search_conversion_rejects_generic_rows_without_fabricating_metadata() {
        let result = SearchResult {
            hits: vec![SearchRow::GenericNamespace(GenericNamespaceHit {
                workbench: WorkbenchName::new("run-42").unwrap(),
                relative_path: None,
                kind: GenericNamespaceKind::Directory,
                artifact: None,
                projection: Vec::new(),
                indexed_values: Vec::new(),
            })],
            match_count: 1,
            facets: Vec::new(),
            next_cursor: None,
            read_version: 19,
        };

        Python::initialize();
        Python::attach(|py| {
            let error = search_result_to_py(py, &result).unwrap_err();
            assert!(error.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py));
            assert!(error.to_string().contains("ArtifactV1"));
        });
    }
}
