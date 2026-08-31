/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nokv_client::{
    ArtifactPublishOptions, ArtifactPublishOutcome, ArtifactRangeBatchRequest, ClientError,
    ClientOptions, FramedTcpOptions, FramedTcpTransport, RouteResolver, SnapshotMintOptions,
    SnapshotRenewOptions, SnapshotRetireOptions, WorkbenchCommitRequest, WorkbenchLifecycleFacade,
    WorkbenchLifecycleOptions, WorkbenchRestoreOrigin, WorkbenchRestoreRequest,
    WorkbenchRestoreSource, WorkbenchSnapshotSelector, WorkspaceClient,
};
use nokv_object::ArtifactObjectStore;
use nokv_protocol::{
    AggregateRequest, ArtifactRevisionIdentity, ByteRange, CatalogRequest, ContentType,
    CreateWorkspaceRequest, FindWorkspacesRequest, GetPathRequest, GetWorkspaceRequest,
    OperationIdentity, PageRequest, PathListEntry, PathMetadata, PathPage, PublishCondition,
    QueryProfile, QueryScope, RelativePath, RemovePathRequest, RenamePathRequest, RootIdentity,
    SearchRequest, SnapshotAlias, SnapshotSelector, WorkbenchName, WorkspaceIdentity,
    WorkspacePath, WorkspaceReadView,
};
use nokv_types::WorkbenchId;
use nokv_workbench_projection::CanonicalWorkbenchProjection;
use pyo3::exceptions::{PyFileExistsError, PyFileNotFoundError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDictMethods, PyListMethods};
use pyo3::types::{PyBytes, PyDict, PyList};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::local_adapter::{
    collect_local_files, create_materialized_file, join_remote_path, materialized_relative_path,
    prepare_materialize_root,
};
use crate::object_store::{ConfiguredObjectStore, PythonObjectStoreConfig};
use crate::python_value::{
    aggregate_result_to_py, catalog_result_to_py, find_workspaces_result_to_py, hex,
    parse_aggregates, parse_field_specs, parse_fixed_hex, parse_predicates, parse_sort,
    path_metadata_to_py, path_page_to_py, publish_outcome_to_py, read_outcome_to_py,
    search_result_to_py, snapshot_result_to_py, workspace_summary_to_py, PythonAggregateSpec,
    PythonFieldSpec, PythonPredicateSpec, PythonSortSpec,
};
use crate::routing::PythonRoutingConfig;

type RustWorkspaceClient = WorkspaceClient<FramedTcpTransport, Arc<dyn RouteResolver>>;
type PythonRangeBatchRequest = (String, Vec<(u64, u64)>, Option<u64>, Option<u64>);

const DEFAULT_SNAPSHOT_LEASE_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_SNAPSHOT_LEASE_SECONDS: u64 = 90 * 24 * 60 * 60;

#[derive(Clone, Debug)]
struct MaterializedFile {
    remote_path: String,
    local_path: PathBuf,
    bytes: u64,
    generation: u64,
}

#[derive(Clone, Debug)]
struct CollectedFile {
    local_path: PathBuf,
    outcome: ArtifactPublishOutcome,
}

#[pyclass(name = "Client")]
pub(crate) struct PythonWorkspaceClient {
    client: Arc<RustWorkspaceClient>,
    objects: Arc<nokv_object::BoundArtifactStore<ConfiguredObjectStore>>,
    /// Presentation root the durable run manifest records, for example
    /// `/agents/<agent-id>/wb`. It is not addressing: artifacts resolve by
    /// workbench name either way. But it is hashed into the commit
    /// operation's projection digest, so a commit written here with a
    /// different root than the CLI uses is a *different* operation over the
    /// same content. Lifecycle calls therefore refuse to guess it.
    workbench_root: Option<String>,
}

#[pymethods]
impl PythonWorkspaceClient {
    #[new]
    #[pyo3(signature = (
        root_id,
        routing,
        object_store,
        max_attempts = 3,
        connect_timeout_ms = 5_000,
        read_timeout_ms = 30_000,
        write_timeout_ms = 30_000,
        handshake_timeout_ms = 5_000,
        workbench_root = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        root_id: &str,
        routing: PyRef<'_, PythonRoutingConfig>,
        object_store: PyRef<'_, PythonObjectStoreConfig>,
        max_attempts: u32,
        connect_timeout_ms: u64,
        read_timeout_ms: u64,
        write_timeout_ms: u64,
        handshake_timeout_ms: u64,
        workbench_root: Option<String>,
    ) -> PyResult<Self> {
        let root_id = RootIdentity(parse_fixed_hex("root_id", root_id)?);
        let transport = FramedTcpTransport::new(FramedTcpOptions {
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            handshake_timeout: Duration::from_millis(handshake_timeout_ms),
            read_timeout: Duration::from_millis(read_timeout_ms),
            write_timeout: Duration::from_millis(write_timeout_ms),
        })
        .map_err(value_error)?;
        let routing = (*routing).clone();
        let resolver = py
            .detach(move || routing.build(root_id))
            .map_err(value_error)?;
        let client =
            WorkspaceClient::new(root_id, transport, resolver, ClientOptions { max_attempts })
                .map_err(value_error)?;
        let object_store = (*object_store).clone();
        let objects = py
            .detach(move || object_store.build())
            .map_err(value_error)?;
        let preflight = py
            .detach(|| client.preflight(std::iter::empty()))
            .map_err(runtime_error)?;
        let namespace_id = preflight.value.route.object_namespace_id.into();
        if objects.is_memory() {
            nokv_object::ensure_object_namespace(&objects, namespace_id).map_err(value_error)?;
        }
        let objects =
            nokv_object::BoundArtifactStore::open(objects, namespace_id).map_err(value_error)?;
        Ok(Self {
            client: Arc::new(client),
            objects: Arc::new(objects),
            workbench_root,
        })
    }

    /// Publish or replay one canonical Workbench commit.
    ///
    /// `manifest` is the caller's provenance mapping; it is canonicalised and
    /// digested exactly as the CLI does, so a commit made here and one made
    /// by `nokv workbench workbench_commit` over the same inputs are the same
    /// durable commit.
    #[pyo3(signature = (workbench, manifest, content_digest_uri, replace = false))]
    fn commit<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        manifest: &Bound<'py, PyAny>,
        content_digest_uri: &str,
        replace: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        let workbench_id = parse_workbench_id(workbench)?;
        let workbench_path = self.lifecycle_workbench_path(&workbench_id)?;
        let manifest_value = json_object_from_py(manifest)?;
        let content_digest_uri = content_digest_uri.to_owned();
        // The digest and the stable id come from nokv-agent, not from here:
        // the SDK must not own metadata layout, and a second implementation
        // would let the SDK and the CLI disagree about one commit.
        let inputs = nokv_agent::workbench_commit_inputs(
            &workbench_id,
            &manifest_value,
            &content_digest_uri,
        )
        .map_err(value_error)?;
        let request = WorkbenchCommitRequest {
            workbench_id,
            canonical_manifest: inputs.canonical_manifest,
            workbench_path,
            content_digest_uri,
            manifest_digest_uri: inputs.manifest_digest_uri,
            stable_commit_id: inputs.stable_commit_id,
            replace,
        };
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let max_artifact_bytes = LIFECYCLE_MAX_MANIFEST_BYTES;
        let outcome = py
            .detach(move || {
                let options =
                    WorkbenchLifecycleOptions::new(max_artifact_bytes).map_err(lifecycle_error)?;
                WorkbenchLifecycleFacade::new(
                    &client,
                    objects.as_ref(),
                    options,
                    CanonicalWorkbenchProjection,
                )
                .commit(request)
                .map_err(lifecycle_error)
            })
            .map_err(runtime_error)?;
        let result = PyDict::new(py);
        result.set_item("commit_id", hex(&outcome.commit_id))?;
        result.set_item("commit_head_generation", outcome.commit_head_generation)?;
        result.set_item("manifest_size_bytes", outcome.manifest_size_bytes)?;
        result.set_item("envelope_digest_uri", outcome.envelope_digest_uri)?;
        result.set_item("tree_digest_uri", outcome.tree_digest_uri)?;
        result.set_item("replayed", outcome.idempotent_replay)?;
        Ok(result)
    }

    /// Restore a frozen state into an absent destination workbench.
    ///
    /// Name exactly one source. `at_snapshot` takes a snapshot id or alias
    /// and is bounded by that snapshot's lease; `at_commit` takes a commit
    /// identity and is not, so a decision point that has to stay citable
    /// after the lease runs out is restored from its commit.
    #[pyo3(signature = (workbench, destination, at_snapshot = None, at_commit = None))]
    fn restore<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        destination: &str,
        at_snapshot: Option<&Bound<'py, PyAny>>,
        at_commit: Option<&str>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let source_workbench_id = parse_workbench_id(workbench)?;
        let destination_workbench_id = parse_workbench_id(destination)?;
        if source_workbench_id == destination_workbench_id {
            return Err(value_error("destination must differ from workbench"));
        }
        let origin = match (at_snapshot, at_commit) {
            (Some(_), Some(_)) => {
                return Err(value_error("give at_snapshot or at_commit, not both"))
            }
            (None, None) => return Err(value_error("give at_snapshot or at_commit")),
            (Some(value), None) => {
                if let Ok(snapshot_id) = value.extract::<u64>() {
                    if snapshot_id == 0 {
                        return Err(value_error("at_snapshot id must be greater than zero"));
                    }
                    WorkbenchRestoreOrigin::Snapshot(WorkbenchSnapshotSelector::Id(snapshot_id))
                } else {
                    let alias: String = value
                        .extract()
                        .map_err(|_| value_error("at_snapshot must be an id or an alias"))?;
                    WorkbenchRestoreOrigin::Snapshot(WorkbenchSnapshotSelector::Name(alias))
                }
            }
            (None, Some(commit_id)) => WorkbenchRestoreOrigin::Commit(
                nokv_agent::decode_commit_identity(commit_id).map_err(value_error)?,
            ),
        };
        let request = WorkbenchRestoreRequest {
            source_workbench_path: self.lifecycle_workbench_path(&source_workbench_id)?,
            destination_workbench_path: self.lifecycle_workbench_path(&destination_workbench_id)?,
            source_workbench_id,
            origin,
            destination_workbench_id,
        };
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let outcome = py
            .detach(move || {
                let options = WorkbenchLifecycleOptions::new(LIFECYCLE_MAX_MANIFEST_BYTES)
                    .map_err(lifecycle_error)?;
                WorkbenchLifecycleFacade::new(
                    &client,
                    objects.as_ref(),
                    options,
                    CanonicalWorkbenchProjection,
                )
                .restore(request)
                .map_err(lifecycle_error)
            })
            .map_err(runtime_error)?;
        let result = PyDict::new(py);
        result.set_item("operation_id", hex(&outcome.operation_id))?;
        match outcome.source {
            WorkbenchRestoreSource::Snapshot { snapshot_id } => {
                result.set_item("snapshot_id", snapshot_id)?;
                result.set_item("commit_id", py.None())?;
            }
            WorkbenchRestoreSource::Commit { commit_id } => {
                result.set_item("snapshot_id", py.None())?;
                result.set_item("commit_id", hex(&commit_id))?;
            }
        }
        result.set_item("read_version", outcome.source_snapshot_read_version)?;
        result.set_item(
            "destination_generation",
            outcome.destination_workspace_revision,
        )?;
        result.set_item("replayed", outcome.idempotent_replay)?;
        Ok(result)
    }

    #[pyo3(signature = (workbench, workspace_incarnation_id = None))]
    fn create_workspace<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        workspace_incarnation_id: Option<&str>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let workbench = parse_workbench(workbench)?;
        let workspace_incarnation_id = match workspace_incarnation_id {
            Some(identity) => {
                WorkspaceIdentity(parse_fixed_hex("workspace_incarnation_id", identity)?)
            }
            None => WorkspaceIdentity(self.client.new_request_id().0),
        };
        let request_id = self.client.new_request_id();
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || {
                client.create_workspace(
                    request_id,
                    CreateWorkspaceRequest {
                        workbench,
                        workspace_incarnation_id,
                    },
                )
            })
            .map_err(runtime_error)?;
        let dict = workspace_summary_to_py(py, &result.value)?;
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    #[pyo3(signature = (workbench, path, snapshot_id = None))]
    fn stat<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        path: &str,
        snapshot_id: Option<u64>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let target = parse_workspace_path(workbench, path)?;
        let view = parse_read_view(snapshot_id)?;
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || {
                client.get_path(GetPathRequest {
                    target,
                    view,
                    expected_read_version: None,
                    range: None,
                    plan_page: None,
                    if_none_match: None,
                })
            })
            .map_err(client_error)?;
        let metadata = result.value.metadata.ok_or_else(|| {
            PyRuntimeError::new_err("metadata response omitted the requested path")
        })?;
        let dict = path_metadata_to_py(py, &metadata)?;
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    #[pyo3(signature = (workbench, path, snapshot_id = None))]
    fn exists(
        &self,
        py: Python<'_>,
        workbench: &str,
        path: &str,
        snapshot_id: Option<u64>,
    ) -> PyResult<bool> {
        let target = parse_workspace_path(workbench, path)?;
        let view = parse_read_view(snapshot_id)?;
        let client = Arc::clone(&self.client);
        py.detach(move || {
            match client.get_path(GetPathRequest {
                target,
                view,
                expected_read_version: None,
                range: None,
                plan_page: None,
                if_none_match: None,
            }) {
                Ok(_) => Ok(true),
                Err(error)
                    if client_error_code(&error) == Some(nokv_protocol::ErrorCode::NotFound) =>
                {
                    Ok(false)
                }
                Err(error) => Err(client_error(error)),
            }
        })
    }

    #[pyo3(signature = (
        workbench,
        prefix = None,
        recursive = false,
        cursor = None,
        limit = 1_000,
        expected_read_version = None,
        snapshot_id = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn list<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        prefix: Option<&str>,
        recursive: bool,
        cursor: Option<Vec<u8>>,
        limit: u32,
        expected_read_version: Option<u64>,
        snapshot_id: Option<u64>,
    ) -> PyResult<Bound<'py, PyDict>> {
        validate_list_page_fence(cursor.as_deref(), expected_read_version)?;
        let workbench = parse_workbench(workbench)?;
        let prefix = parse_optional_relative_path(prefix)?;
        let view = parse_read_view(snapshot_id)?;
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || {
                client.list_paths(nokv_protocol::ListPathsRequest {
                    workbench,
                    prefix,
                    recursive,
                    view,
                    expected_read_version,
                    workspace_continuation_fence: None,
                    page: PageRequest { cursor, limit },
                })
            })
            .map_err(client_error)?;
        let dict = path_page_to_py(py, &result.value)?;
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    fn remove<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        path: &str,
        expected_generation: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        let target = parse_workspace_path(workbench, path)?;
        let request_id = self.client.new_request_id();
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || {
                client.remove_path(
                    request_id,
                    RemovePathRequest {
                        target,
                        expected_generation,
                    },
                )
            })
            .map_err(client_error)?;
        let dict = PyDict::new(py);
        dict.set_item("removed", result.value.removed)?;
        dict.set_item("workspace_revision", result.value.workspace_revision)?;
        match result.value.removed_artifact_revision_id {
            Some(identity) => dict.set_item("removed_artifact_revision_id", hex(&identity.0))?,
            None => dict.set_item("removed_artifact_revision_id", py.None())?,
        }
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    fn rename<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        source: &str,
        destination: &str,
        expected_generation: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        let source = parse_workspace_path(workbench, source)?;
        let destination = parse_workspace_path(workbench, destination)?;
        let request_id = self.client.new_request_id();
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || {
                client.rename_path(
                    request_id,
                    RenamePathRequest {
                        source,
                        destination,
                        expected_generation,
                    },
                )
            })
            .map_err(client_error)?;
        let dict = PyDict::new(py);
        dict.set_item("source", result.value.source.path.as_str())?;
        dict.set_item("destination", result.value.destination.path.as_str())?;
        dict.set_item("workspace_revision", result.value.workspace_revision)?;
        dict.set_item("generation", result.value.generation)?;
        dict.set_item(
            "artifact_revision_id",
            hex(&result.value.artifact_revision_id.0),
        )?;
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    /// Publish bytes with create-only or explicit generation-CAS replacement.
    ///
    /// `index_fields` entries are `(field_id, scalar_kind, encoded_value)`.
    #[pyo3(signature = (
        workbench,
        path,
        data,
        content_type = "application/octet-stream",
        expected_generation = None,
        producer = None,
        manifest_identity = None,
        index_fields = None,
        block_size = 4_194_304,
        operation_id = None,
        artifact_revision_id = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn publish_bytes<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        path: &str,
        data: Vec<u8>,
        content_type: &str,
        expected_generation: Option<u64>,
        producer: Option<String>,
        manifest_identity: Option<String>,
        index_fields: Option<Vec<PythonFieldSpec>>,
        block_size: usize,
        operation_id: Option<&str>,
        artifact_revision_id: Option<&str>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let options = self.publish_options(
            workbench,
            path,
            content_type,
            expected_generation,
            producer,
            manifest_identity,
            index_fields.unwrap_or_default(),
            block_size,
            operation_id,
            artifact_revision_id,
        )?;
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let outcome = py
            .detach(move || client.publish_artifact(objects.as_ref(), options, &data))
            .map_err(client_error)?;
        publish_outcome_to_py(py, &outcome)
    }

    /// Publish one explicit regular local file without following a symlink.
    #[pyo3(signature = (
        workbench,
        path,
        local_file,
        content_type = "application/octet-stream",
        expected_generation = None,
        producer = None,
        manifest_identity = None,
        index_fields = None,
        block_size = 4_194_304,
        operation_id = None,
        artifact_revision_id = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn publish_file<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        path: &str,
        local_file: &str,
        content_type: &str,
        expected_generation: Option<u64>,
        producer: Option<String>,
        manifest_identity: Option<String>,
        index_fields: Option<Vec<PythonFieldSpec>>,
        block_size: usize,
        operation_id: Option<&str>,
        artifact_revision_id: Option<&str>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let options = self.publish_options(
            workbench,
            path,
            content_type,
            expected_generation,
            producer,
            manifest_identity,
            index_fields.unwrap_or_default(),
            block_size,
            operation_id,
            artifact_revision_id,
        )?;
        let local_file = PathBuf::from(local_file);
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let outcome = py.detach(move || {
            let bytes = read_regular_file(&local_file).map_err(runtime_error)?;
            client
                .publish_artifact(objects.as_ref(), options, &bytes)
                .map_err(client_error)
        })?;
        publish_outcome_to_py(py, &outcome)
    }

    #[pyo3(signature = (workbench, path, snapshot_id = None))]
    fn read<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        path: &str,
        snapshot_id: Option<u64>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let target = parse_workspace_path(workbench, path)?;
        let view = parse_read_view(snapshot_id)?;
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let outcome = py
            .detach(move || client.read_artifact(objects.as_ref(), None, target, view))
            .map_err(client_error)?;
        read_outcome_to_py(py, &outcome)
    }

    #[pyo3(signature = (workbench, path, offset, length, snapshot_id = None))]
    fn read_range<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        path: &str,
        offset: u64,
        length: usize,
        snapshot_id: Option<u64>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let target = parse_workspace_path(workbench, path)?;
        let view = parse_read_view(snapshot_id)?;
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let outcome = py
            .detach(move || {
                client.read_artifact_range(objects.as_ref(), None, target, view, offset, length)
            })
            .map_err(client_error)?;
        read_outcome_to_py(py, &outcome)
    }

    #[pyo3(signature = (workbench, requests, snapshot_id = None))]
    fn read_ranges_batch<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        requests: Vec<PythonRangeBatchRequest>,
        snapshot_id: Option<u64>,
    ) -> PyResult<Bound<'py, PyList>> {
        let workbench = parse_workbench(workbench)?;
        let view = parse_read_view(snapshot_id)?;
        let requests = requests
            .into_iter()
            .map(|(path, ranges, expected_generation, max_gap_bytes)| {
                let ranges = ranges
                    .into_iter()
                    .map(|(offset, length)| ByteRange { offset, length })
                    .collect();
                Ok(ArtifactRangeBatchRequest {
                    target: WorkspacePath {
                        workbench: workbench.clone(),
                        path: parse_relative_path(&path)?,
                    },
                    ranges,
                    expected_generation,
                    max_gap_bytes: max_gap_bytes.unwrap_or(0),
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let outcome = py
            .detach(move || {
                client.read_artifact_ranges_batch(objects.as_ref(), None, requests, view)
            })
            .map_err(client_error)?;
        let outer = PyList::empty(py);
        for item in outcome.items {
            let inner = PyList::empty(py);
            for bytes in item.ranges {
                inner.append(PyBytes::new(py, &bytes))?;
            }
            outer.append(inner)?;
        }
        Ok(outer)
    }

    #[pyo3(signature = (
        workbench,
        lease_ttl_seconds = DEFAULT_SNAPSHOT_LEASE_SECONDS,
        alias = None,
        annotation = None
    ))]
    fn snapshot<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        lease_ttl_seconds: u64,
        alias: Option<&str>,
        annotation: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let workbench = parse_workbench(workbench)?;
        let alias = alias
            .map(|value| SnapshotAlias::new(value.to_owned()).map_err(value_error))
            .transpose()?;
        let lease_deadline_ms = snapshot_deadline_ms(lease_ttl_seconds)?;
        let client = Arc::clone(&self.client);
        let outcome = py
            .detach(move || {
                let workspace = client.get_workspace(GetWorkspaceRequest {
                    workbench: workbench.clone(),
                })?;
                client.mint_snapshot_workflow(SnapshotMintOptions {
                    workbench,
                    workspace_incarnation_id: workspace.value.workspace_incarnation_id,
                    lease_deadline_ms,
                    alias,
                    annotation: annotation.unwrap_or_default(),
                })
            })
            .map_err(client_error)?;
        let dict = snapshot_result_to_py(py, &outcome.value)?;
        set_call_metadata(&dict, outcome.commit_version, outcome.replayed)?;
        Ok(dict)
    }

    #[pyo3(signature = (
        workbench,
        snapshot_id,
        lease_ttl_seconds = DEFAULT_SNAPSHOT_LEASE_SECONDS
    ))]
    fn renew_snapshot<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        snapshot_id: u64,
        lease_ttl_seconds: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        let options = SnapshotRenewOptions {
            workbench: parse_workbench(workbench)?,
            selector: parse_snapshot_selector(snapshot_id)?,
            lease_deadline_ms: snapshot_deadline_ms(lease_ttl_seconds)?,
        };
        let client = Arc::clone(&self.client);
        let outcome = py
            .detach(move || client.renew_snapshot_workflow(options))
            .map_err(client_error)?;
        let dict = snapshot_result_to_py(py, &outcome.value)?;
        set_call_metadata(&dict, outcome.commit_version, outcome.replayed)?;
        Ok(dict)
    }

    #[pyo3(signature = (workbench, snapshot_id, annotation = None))]
    fn retire_snapshot<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        snapshot_id: u64,
        annotation: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let options = SnapshotRetireOptions {
            workbench: parse_workbench(workbench)?,
            selector: parse_snapshot_selector(snapshot_id)?,
            retire_annotation: annotation,
        };
        let client = Arc::clone(&self.client);
        let outcome = py
            .detach(move || client.retire_snapshot_workflow(options))
            .map_err(client_error)?;
        let dict = snapshot_result_to_py(py, &outcome.snapshot)?;
        dict.set_item("retired", outcome.retired)?;
        set_call_metadata(&dict, outcome.commit_version, outcome.replayed)?;
        Ok(dict)
    }

    fn list_snapshots<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        let workbench = parse_workbench(workbench)?;
        let client = Arc::clone(&self.client);
        let snapshots = py
            .detach(move || client.list_all_snapshots(workbench))
            .map_err(client_error)?;
        let list = PyList::empty(py);
        for snapshot in snapshots {
            list.append(snapshot_result_to_py(py, &snapshot)?)?;
        }
        Ok(list)
    }

    /// Search root-wide or within one live workspace.
    #[pyo3(signature = (
        predicates = None,
        projection = None,
        sort = None,
        facets = None,
        workbench = None,
        cursor = None,
        limit = 100
    ))]
    #[allow(clippy::too_many_arguments)]
    fn search<'py>(
        &self,
        py: Python<'py>,
        predicates: Option<Vec<PythonPredicateSpec>>,
        projection: Option<Vec<String>>,
        sort: Option<Vec<PythonSortSpec>>,
        facets: Option<Vec<String>>,
        workbench: Option<&str>,
        cursor: Option<Vec<u8>>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyDict>> {
        let request = SearchRequest {
            profile: QueryProfile::ArtifactV1,
            scope: parse_query_scope(workbench)?,
            predicates: parse_predicates(predicates.unwrap_or_default())?,
            projection: projection.unwrap_or_default(),
            sort: parse_sort(sort.unwrap_or_default())?,
            facets: facets.unwrap_or_default(),
            page: PageRequest { cursor, limit },
        };
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || client.search(request))
            .map_err(runtime_error)?;
        let dict = search_result_to_py(py, &result.value)?;
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    /// Aggregate root-wide or within one live workspace.
    #[pyo3(signature = (
        aggregates,
        predicates = None,
        group_by = None,
        sort = None,
        workbench = None,
        cursor = None,
        limit = 100
    ))]
    #[allow(clippy::too_many_arguments)]
    fn aggregate<'py>(
        &self,
        py: Python<'py>,
        aggregates: Vec<PythonAggregateSpec>,
        predicates: Option<Vec<PythonPredicateSpec>>,
        group_by: Option<Vec<String>>,
        sort: Option<Vec<PythonSortSpec>>,
        workbench: Option<&str>,
        cursor: Option<Vec<u8>>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyDict>> {
        let request = AggregateRequest {
            profile: QueryProfile::ArtifactV1,
            scope: parse_query_scope(workbench)?,
            predicates: parse_predicates(predicates.unwrap_or_default())?,
            group_by: group_by.unwrap_or_default(),
            aggregates: parse_aggregates(aggregates)?,
            sort: parse_sort(sort.unwrap_or_default())?,
            page: PageRequest { cursor, limit },
        };
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || client.aggregate(request))
            .map_err(runtime_error)?;
        let dict = aggregate_result_to_py(py, &result.value)?;
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    #[pyo3(signature = (field_prefix = None, cursor = None, limit = 100))]
    fn catalog<'py>(
        &self,
        py: Python<'py>,
        field_prefix: Option<String>,
        cursor: Option<Vec<u8>>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyDict>> {
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || {
                client.catalog(CatalogRequest {
                    profile: QueryProfile::ArtifactV1,
                    scope: QueryScope::Root { path_prefix: None },
                    path_match: nokv_protocol::CatalogPathMatch::Prefix,
                    field_prefix,
                    // The direct Python catalog ABI exposes fields, not facet
                    // buckets, so requesting them would silently discard data.
                    include_facets: false,
                    page: PageRequest { cursor, limit },
                })
            })
            .map_err(runtime_error)?;
        let dict = catalog_result_to_py(py, &result.value)?;
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    #[pyo3(signature = (
        committed_only = false,
        cursor = None,
        limit = 100
    ))]
    fn find_workspaces<'py>(
        &self,
        py: Python<'py>,
        committed_only: bool,
        cursor: Option<Vec<u8>>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyDict>> {
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || {
                client.find_workspaces(FindWorkspacesRequest {
                    committed_only,
                    page: PageRequest { cursor, limit },
                })
            })
            .map_err(runtime_error)?;
        let dict = find_workspaces_result_to_py(py, &result.value)?;
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    /// Materialize one live workspace prefix into a new local directory tree.
    /// Existing local files are never overwritten and symlinks are rejected.
    #[pyo3(signature = (workbench, local_directory, prefix = None, snapshot_id = None))]
    fn materialize<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        local_directory: &str,
        prefix: Option<&str>,
        snapshot_id: Option<u64>,
    ) -> PyResult<Bound<'py, PyList>> {
        let workbench = parse_workbench(workbench)?;
        let prefix = parse_optional_relative_path(prefix)?;
        let view = parse_read_view(snapshot_id)?;
        let local_directory = PathBuf::from(local_directory);
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let files = py
            .detach(move || {
                materialize_workspace(
                    client.as_ref(),
                    objects.as_ref(),
                    workbench,
                    prefix,
                    view,
                    &local_directory,
                )
            })
            .map_err(PyRuntimeError::new_err)?;
        let list = PyList::empty(py);
        for file in files {
            let item = PyDict::new(py);
            item.set_item("remote_path", file.remote_path)?;
            item.set_item("local_path", file.local_path.to_string_lossy().as_ref())?;
            item.set_item("bytes", file.bytes)?;
            item.set_item("generation", file.generation)?;
            list.append(item)?;
        }
        Ok(list)
    }

    /// Collect regular files below a local directory as independent
    /// create-only workspace artifacts. Symlinks are never followed.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        local_directory,
        workbench,
        prefix = None,
        content_type = "application/octet-stream",
        producer = None,
        block_size = 4_194_304
    ))]
    fn collect<'py>(
        &self,
        py: Python<'py>,
        local_directory: &str,
        workbench: &str,
        prefix: Option<&str>,
        content_type: &str,
        producer: Option<String>,
        block_size: usize,
    ) -> PyResult<Bound<'py, PyList>> {
        let local_directory = PathBuf::from(local_directory);
        let workbench = parse_workbench(workbench)?;
        let prefix = parse_optional_relative_path(prefix)?;
        let content_type = ContentType::new(content_type.to_owned()).map_err(value_error)?;
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let files = py
            .detach(move || {
                collect_workspace(
                    client.as_ref(),
                    objects.as_ref(),
                    &local_directory,
                    workbench,
                    prefix,
                    content_type,
                    producer,
                    block_size,
                )
            })
            .map_err(PyRuntimeError::new_err)?;
        let list = PyList::empty(py);
        for file in files {
            let item = publish_outcome_to_py(py, &file.outcome)?;
            item.set_item("local_path", file.local_path.to_string_lossy().as_ref())?;
            list.append(item)?;
        }
        Ok(list)
    }
}

impl PythonWorkspaceClient {
    fn lifecycle_workbench_path(&self, workbench_id: &WorkbenchId) -> PyResult<String> {
        let root = self.workbench_root.as_deref().ok_or_else(|| {
            value_error(
                "lifecycle calls need the workbench_root this deployment uses \
                 (for example \"/agents/<agent-id>/wb\"); construct the client with it, \
                 because the root is hashed into the commit operation and guessing it \
                 would make this commit a different operation from the CLI's",
            )
        })?;
        Ok(format!("{root}/{}", workbench_id.as_str()))
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_options(
        &self,
        workbench: &str,
        path: &str,
        content_type: &str,
        expected_generation: Option<u64>,
        producer: Option<String>,
        manifest_identity: Option<String>,
        index_fields: Vec<PythonFieldSpec>,
        block_size: usize,
        operation_id: Option<&str>,
        artifact_revision_id: Option<&str>,
    ) -> PyResult<ArtifactPublishOptions> {
        let operation_id = match operation_id {
            Some(identity) => OperationIdentity(parse_fixed_hex("operation_id", identity)?),
            None => OperationIdentity(self.client.new_request_id().0),
        };
        let artifact_revision_id = match artifact_revision_id {
            Some(identity) => {
                ArtifactRevisionIdentity(parse_fixed_hex("artifact_revision_id", identity)?)
            }
            None => ArtifactRevisionIdentity(self.client.new_request_id().0),
        };
        let condition = match expected_generation {
            Some(expected_generation) => PublishCondition::ReplaceOnly {
                expected_generation,
            },
            None => PublishCondition::CreateOnly,
        };
        let mut options = ArtifactPublishOptions::new(
            operation_id,
            artifact_revision_id,
            parse_workspace_path(workbench, path)?,
            condition,
            ContentType::new(content_type.to_owned()).map_err(value_error)?,
        )
        .with_block_size(block_size)
        .with_index_fields(parse_field_specs(index_fields)?);
        if let Some(producer) = producer {
            options = options.with_producer(producer);
        }
        if let Some(manifest_identity) = manifest_identity {
            options = options.with_manifest_identity(manifest_identity);
        }
        Ok(options)
    }
}

/// The presentation root a lifecycle call records in the durable manifest.
/// Refused rather than guessed: see `PythonWorkspaceClient::workbench_root`.
fn parse_workbench_id(raw: &str) -> PyResult<nokv_types::WorkbenchId> {
    WorkbenchId::new(raw.to_owned()).map_err(value_error)
}

fn parse_workbench(raw: &str) -> PyResult<WorkbenchName> {
    WorkbenchName::new(raw.to_owned()).map_err(value_error)
}

fn parse_relative_path(raw: &str) -> PyResult<RelativePath> {
    RelativePath::new(raw.to_owned()).map_err(value_error)
}

fn parse_optional_relative_path(raw: Option<&str>) -> PyResult<Option<RelativePath>> {
    raw.map(parse_relative_path).transpose()
}

fn parse_snapshot_selector(snapshot_id: u64) -> PyResult<SnapshotSelector> {
    if snapshot_id == 0 {
        return Err(value_error("snapshot_id must be greater than zero"));
    }
    Ok(SnapshotSelector::Id(snapshot_id))
}

fn parse_read_view(snapshot_id: Option<u64>) -> PyResult<WorkspaceReadView> {
    snapshot_id
        .map(parse_snapshot_selector)
        .transpose()
        .map(|selector| selector.map_or(WorkspaceReadView::Live, WorkspaceReadView::Snapshot))
}

fn snapshot_deadline_ms(lease_ttl_seconds: u64) -> PyResult<u64> {
    if !(1..=MAX_SNAPSHOT_LEASE_SECONDS).contains(&lease_ttl_seconds) {
        return Err(value_error(format!(
            "lease_ttl_seconds must be between 1 and {MAX_SNAPSHOT_LEASE_SECONDS}"
        )));
    }
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(value_error)?
        .as_millis();
    let now_ms = u64::try_from(now_ms).map_err(value_error)?;
    now_ms
        .checked_add(lease_ttl_seconds.saturating_mul(1_000))
        .ok_or_else(|| value_error("snapshot lease deadline overflows u64"))
}

fn parse_workspace_path(workbench: &str, path: &str) -> PyResult<WorkspacePath> {
    Ok(WorkspacePath {
        workbench: parse_workbench(workbench)?,
        path: parse_relative_path(path)?,
    })
}

fn parse_query_scope(workbench: Option<&str>) -> PyResult<QueryScope> {
    match workbench {
        Some(workbench) => Ok(QueryScope::Workspace {
            workbench: parse_workbench(workbench)?,
            path_prefix: None,
        }),
        None => Ok(QueryScope::Root { path_prefix: None }),
    }
}

fn set_call_metadata(
    dict: &Bound<'_, PyDict>,
    commit_version: Option<u64>,
    replayed: bool,
) -> PyResult<()> {
    dict.set_item("commit_version", commit_version)?;
    dict.set_item("replayed", replayed)?;
    Ok(())
}

fn read_regular_file(path: &Path) -> Result<Vec<u8>, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect local file {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "local publish file {} is a symlink; symlinks are not followed",
            path.display()
        ));
    }
    if !metadata.is_file() {
        return Err(format!(
            "local publish path {} is not a regular file",
            path.display()
        ));
    }
    fs::read(path).map_err(|error| format!("cannot read local file {}: {error}", path.display()))
}

fn list_all_paths(
    client: &RustWorkspaceClient,
    workbench: WorkbenchName,
    prefix: Option<RelativePath>,
    view: WorkspaceReadView,
) -> Result<Vec<PathMetadata>, String> {
    list_all_paths_with(workbench, prefix, view, |request| {
        client.list_paths(request).map(|call| call.value)
    })
}

fn list_all_paths_with(
    workbench: WorkbenchName,
    prefix: Option<RelativePath>,
    view: WorkspaceReadView,
    mut list_page: impl FnMut(nokv_protocol::ListPathsRequest) -> Result<PathPage, ClientError>,
) -> Result<Vec<PathMetadata>, String> {
    'attempt: for attempt in 1..=3 {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut read_version = None;
        let mut entries = Vec::new();
        loop {
            let page = match list_page(nokv_protocol::ListPathsRequest {
                workbench: workbench.clone(),
                prefix: prefix.clone(),
                recursive: true,
                view: view.clone(),
                expected_read_version: read_version,
                workspace_continuation_fence: None,
                page: PageRequest {
                    cursor: cursor.clone(),
                    limit: PageRequest::MAX_LIMIT,
                },
            }) {
                Ok(page) => page,
                Err(error) if attempt < 3 && is_read_version_conflict(&error) => {
                    continue 'attempt;
                }
                Err(error) => return Err(error.to_string()),
            };
            if read_version.is_some_and(|expected| expected != page.read_version) {
                return Err(
                    "workspace listing returned a page outside its read-version fence".to_owned(),
                );
            }
            read_version.get_or_insert(page.read_version);
            for entry in page.entries {
                match entry {
                    PathListEntry::Artifact(metadata) => entries.push(metadata),
                    PathListEntry::Prefix(path) => {
                        return Err(format!(
                            "recursive workspace listing returned implicit prefix {}",
                            path.path.as_str()
                        ));
                    }
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(entries);
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err("workspace listing returned a cursor loop".to_owned());
            }
            cursor = Some(next_cursor);
        }
    }

    unreachable!("consistent list attempt count is non-zero")
}

fn is_read_version_conflict(error: &ClientError) -> bool {
    match error {
        ClientError::Rpc(failure) => {
            failure.code == nokv_protocol::ErrorCode::PreconditionFailed
                && failure.conflict == Some(nokv_protocol::ConflictKind::ReadVersion)
        }
        ClientError::RetryExhausted { last_error, .. } => is_read_version_conflict(last_error),
        _ => false,
    }
}

fn materialize_workspace(
    client: &RustWorkspaceClient,
    objects: &dyn ArtifactObjectStore,
    workbench: WorkbenchName,
    prefix: Option<RelativePath>,
    view: WorkspaceReadView,
    local_directory: &Path,
) -> Result<Vec<MaterializedFile>, String> {
    let root = prepare_materialize_root(local_directory)?;
    let entries = list_all_paths(client, workbench, prefix.clone(), view.clone())?;
    let mut destinations = BTreeSet::new();
    let mut plan = Vec::with_capacity(entries.len());
    for metadata in entries {
        let relative = materialized_relative_path(&metadata.path.path, prefix.as_ref())?;
        if !destinations.insert(relative.as_str().to_owned()) {
            return Err(format!(
                "multiple workspace paths map to local target {:?}",
                relative.as_str()
            ));
        }
        plan.push((metadata, relative));
    }

    let mut created = Vec::new();
    let result = (|| {
        let mut materialized = Vec::with_capacity(plan.len());
        for (expected, relative) in plan {
            let read = client
                .read_artifact(objects, None, expected.path.clone(), view.clone())
                .map_err(|error| error.to_string())?;
            if read.metadata != expected {
                return Err(format!(
                    "workspace path {:?} changed while it was being materialized",
                    expected.path.path.as_str()
                ));
            }
            let local_path = create_materialized_file(&root, &relative, &read.bytes)?;
            created.push(local_path.clone());
            materialized.push(MaterializedFile {
                remote_path: expected.path.path.as_str().to_owned(),
                local_path,
                bytes: read.bytes.len() as u64,
                generation: expected.generation,
            });
        }
        Ok(materialized)
    })();
    if result.is_err() {
        for path in created.into_iter().rev() {
            let _ = fs::remove_file(path);
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn collect_workspace(
    client: &RustWorkspaceClient,
    objects: &dyn ArtifactObjectStore,
    local_directory: &Path,
    workbench: WorkbenchName,
    prefix: Option<RelativePath>,
    content_type: ContentType,
    producer: Option<String>,
    block_size: usize,
) -> Result<Vec<CollectedFile>, String> {
    let files = collect_local_files(local_directory)?;
    let mut collected = Vec::with_capacity(files.len());
    for file in files {
        let remote_path = join_remote_path(prefix.as_ref(), &file.relative_path)?;
        let bytes = read_regular_file(&file.absolute_path)?;
        let mut options = ArtifactPublishOptions::new(
            OperationIdentity(client.new_request_id().0),
            ArtifactRevisionIdentity(client.new_request_id().0),
            WorkspacePath {
                workbench: workbench.clone(),
                path: remote_path,
            },
            PublishCondition::CreateOnly,
            content_type.clone(),
        )
        .with_block_size(block_size);
        if let Some(producer) = &producer {
            options = options.with_producer(producer.clone());
        }
        let outcome = client
            .publish_artifact(objects, options, &bytes)
            .map_err(|error| error.to_string())?;
        collected.push(CollectedFile {
            local_path: file.absolute_path,
            outcome,
        });
    }
    Ok(collected)
}

/// Bound for the run-manifest read inside the lifecycle facade. Manifests are
/// small; this only refuses a pathologically large one.
const LIFECYCLE_MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

/// Marshals a Python mapping into the JSON object the commit manifest is.
/// Values are restricted to what a provenance manifest can carry, so an
/// unserialisable object fails here rather than at the digest.
fn json_object_from_py(value: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    let mapping = value
        .cast::<PyDict>()
        .map_err(|_| value_error("manifest must be a dict"))?;
    let mut object = JsonMap::new();
    for (key, item) in mapping.iter() {
        let key: String = key
            .extract()
            .map_err(|_| value_error("manifest keys must be strings"))?;
        object.insert(key, json_scalar_from_py(&item)?);
    }
    Ok(JsonValue::Object(object))
}

fn json_scalar_from_py(value: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if value.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(flag) = value.extract::<bool>() {
        return Ok(JsonValue::Bool(flag));
    }
    if let Ok(number) = value.extract::<i64>() {
        return Ok(JsonValue::from(number));
    }
    if let Ok(number) = value.extract::<f64>() {
        return serde_json::Number::from_f64(number)
            .map(JsonValue::Number)
            .ok_or_else(|| value_error("manifest numbers must be finite"));
    }
    if let Ok(text) = value.extract::<String>() {
        return Ok(JsonValue::String(text));
    }
    if value.cast::<PyDict>().is_ok() {
        return json_object_from_py(value);
    }
    if let Ok(items) = value.cast::<PyList>() {
        let mut out = Vec::with_capacity(items.len());
        for item in items.iter() {
            out.push(json_scalar_from_py(&item)?);
        }
        return Ok(JsonValue::Array(out));
    }
    Err(value_error(
        "manifest values must be strings, numbers, booleans, null, lists, or dicts",
    ))
}

fn lifecycle_error<E: std::fmt::Display>(error: E) -> String {
    error.to_string()
}

fn value_error(error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn validate_list_page_fence(
    cursor: Option<&[u8]>,
    expected_read_version: Option<u64>,
) -> PyResult<()> {
    if cursor.is_some() && expected_read_version.is_none() {
        return Err(value_error(
            "expected_read_version is required when continuing from a list cursor",
        ));
    }
    Ok(())
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn client_error(error: ClientError) -> PyErr {
    match client_error_code(&error) {
        Some(nokv_protocol::ErrorCode::NotFound) => PyFileNotFoundError::new_err(error.to_string()),
        Some(nokv_protocol::ErrorCode::AlreadyExists) => {
            PyFileExistsError::new_err(error.to_string())
        }
        _ => PyRuntimeError::new_err(error.to_string()),
    }
}

fn client_error_code(error: &ClientError) -> Option<nokv_protocol::ErrorCode> {
    match error {
        ClientError::Discovery(failure) | ClientError::Rpc(failure) => Some(failure.code),
        ClientError::ArtifactPublishFailed { source, .. }
        | ClientError::RetryExhausted {
            last_error: source, ..
        } => client_error_code(source),
        ClientError::InvalidOptions(_)
        | ClientError::InvalidRoute(_)
        | ClientError::Transport(_)
        | ClientError::Protocol(_)
        | ClientError::ResponseMismatch(_)
        | ClientError::MissingCapabilities(_)
        | ClientError::ArtifactIntegrity(_)
        | ClientError::ArtifactReadFenceChanged
        | ClientError::Object(_)
        | ClientError::ArtifactUpload(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    fn read_version_failure(conflict: Option<nokv_protocol::ConflictKind>) -> ClientError {
        ClientError::Rpc(nokv_protocol::RpcFailure {
            code: nokv_protocol::ErrorCode::PreconditionFailed,
            message: "read version changed".to_owned(),
            retryable: false,
            conflict,
            current_generation: None,
            route_hint: None,
        })
    }

    #[test]
    fn workspace_paths_use_the_protocol_normalizer() {
        assert!(parse_workspace_path("run-42", "outputs/result.bin").is_ok());
        assert!(parse_workspace_path("run-42", "/outputs/result.bin").is_err());
        assert!(parse_workspace_path("run-42", "outputs/../secret").is_err());
        assert!(parse_workspace_path("run-42", "outputs\\secret").is_err());
    }

    #[test]
    fn publish_file_rejects_symlinks_and_directories() {
        let root = tempfile::tempdir().unwrap();
        assert!(read_regular_file(root.path())
            .unwrap_err()
            .contains("regular file"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let source = root.path().join("source.bin");
            fs::write(&source, b"bytes").unwrap();
            let link = root.path().join("link.bin");
            symlink(&source, &link).unwrap();
            assert!(read_regular_file(&link).unwrap_err().contains("symlink"));
        }
    }

    #[test]
    fn full_path_listing_fences_followup_pages_and_restarts_the_whole_attempt() {
        let mut responses = VecDeque::from([
            Ok(PathPage {
                entries: Vec::new(),
                next_cursor: Some(b"page-2".to_vec()),
                read_version: 7,
            }),
            Err(read_version_failure(Some(
                nokv_protocol::ConflictKind::ReadVersion,
            ))),
            Ok(PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 8,
            }),
        ]);
        let mut requests = Vec::new();
        let entries = list_all_paths_with(
            WorkbenchName::new("run-42").unwrap(),
            None,
            WorkspaceReadView::Live,
            |request| {
                requests.push(request);
                responses.pop_front().expect("one scripted list response")
            },
        )
        .unwrap();

        assert!(entries.is_empty());
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].expected_read_version, None);
        assert_eq!(requests[1].expected_read_version, Some(7));
        assert_eq!(
            requests[1].page.cursor.as_deref(),
            Some(b"page-2".as_slice())
        );
        assert_eq!(requests[2].expected_read_version, None);
        assert!(requests[2].page.cursor.is_none());
    }

    #[test]
    fn full_path_listing_does_not_retry_other_preconditions() {
        let mut calls = 0;
        let error = list_all_paths_with(
            WorkbenchName::new("run-42").unwrap(),
            None,
            WorkspaceReadView::Live,
            |_| {
                calls += 1;
                Err(read_version_failure(None))
            },
        )
        .unwrap_err();

        assert_eq!(calls, 1);
        assert!(error.contains("read version changed"));
    }

    #[test]
    fn public_list_cursor_requires_the_previous_page_read_version() {
        Python::initialize();
        let error = validate_list_page_fence(Some(b"page-2"), None).unwrap_err();
        assert!(error
            .to_string()
            .contains("expected_read_version is required"));
        validate_list_page_fence(Some(b"page-2"), Some(7)).unwrap();
        validate_list_page_fence(None, None).unwrap();
        validate_list_page_fence(None, Some(7)).unwrap();
    }

    #[test]
    fn snapshot_read_views_reject_zero_and_preserve_the_exact_selector() {
        assert_eq!(parse_read_view(None).unwrap(), WorkspaceReadView::Live);
        assert_eq!(
            parse_read_view(Some(17)).unwrap(),
            WorkspaceReadView::Snapshot(SnapshotSelector::Id(17))
        );
        assert!(parse_read_view(Some(0))
            .unwrap_err()
            .to_string()
            .contains("greater than zero"));
    }

    #[test]
    fn snapshot_deadlines_enforce_the_workbench_lease_bound() {
        assert!(snapshot_deadline_ms(1).is_ok());
        assert!(snapshot_deadline_ms(DEFAULT_SNAPSHOT_LEASE_SECONDS).is_ok());
        assert!(snapshot_deadline_ms(0).is_err());
        assert!(snapshot_deadline_ms(MAX_SNAPSHOT_LEASE_SECONDS + 1).is_err());
    }
}
