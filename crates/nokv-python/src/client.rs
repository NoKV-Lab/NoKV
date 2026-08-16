/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use nokv_client::{
    ArtifactPublishOptions, ArtifactPublishOutcome, ClientError, ClientOptions, FramedTcpOptions,
    FramedTcpTransport, RouteResolver, WorkspaceClient,
};
use nokv_object::ArtifactObjectStore;
use nokv_protocol::{
    AggregateRequest, ArtifactRevisionIdentity, CatalogRequest, ContentType,
    CreateWorkspaceRequest, FindWorkspacesRequest, GetPathRequest, OperationIdentity, PageRequest,
    PathListEntry, PathMetadata, PathPage, PublishCondition, QueryScope, RelativePath,
    RemovePathRequest, RootIdentity, SearchRequest, WorkbenchName, WorkspaceIdentity,
    WorkspacePath, WorkspaceReadView,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::local_adapter::{
    collect_local_files, create_materialized_file, join_remote_path, materialized_relative_path,
    prepare_materialize_root,
};
use crate::object_store::{ConfiguredObjectStore, PythonObjectStoreConfig};
use crate::python_value::{
    aggregate_result_to_py, catalog_result_to_py, find_workspaces_result_to_py, hex,
    parse_aggregates, parse_field_specs, parse_fixed_hex, parse_predicates, parse_sort,
    path_metadata_to_py, path_page_to_py, publish_outcome_to_py, read_outcome_to_py,
    search_result_to_py, workspace_summary_to_py, PythonAggregateSpec, PythonFieldSpec,
    PythonPredicateSpec, PythonSortSpec,
};
use crate::routing::PythonRoutingConfig;

type RustWorkspaceClient = WorkspaceClient<FramedTcpTransport, Arc<dyn RouteResolver>>;

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
        handshake_timeout_ms = 5_000
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
        let objects = object_store.build().map_err(value_error)?;
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
        })
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

    fn stat<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        path: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        let target = parse_workspace_path(workbench, path)?;
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || {
                client.get_path(GetPathRequest {
                    target,
                    view: WorkspaceReadView::Live,
                    range: None,
                    plan_page: None,
                    if_none_match: None,
                })
            })
            .map_err(runtime_error)?;
        let metadata = result.value.metadata.ok_or_else(|| {
            PyRuntimeError::new_err("metadata response omitted the requested path")
        })?;
        let dict = path_metadata_to_py(py, &metadata)?;
        set_call_metadata(&dict, result.commit_version, result.replayed)?;
        Ok(dict)
    }

    #[pyo3(signature = (
        workbench,
        prefix = None,
        recursive = false,
        cursor = None,
        limit = 1_000,
        expected_read_version = None
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
    ) -> PyResult<Bound<'py, PyDict>> {
        validate_list_page_fence(cursor.as_deref(), expected_read_version)?;
        let workbench = parse_workbench(workbench)?;
        let prefix = parse_optional_relative_path(prefix)?;
        let client = Arc::clone(&self.client);
        let result = py
            .detach(move || {
                client.list_paths(nokv_protocol::ListPathsRequest {
                    workbench,
                    prefix,
                    recursive,
                    view: WorkspaceReadView::Live,
                    expected_read_version,
                    workspace_continuation_fence: None,
                    page: PageRequest { cursor, limit },
                })
            })
            .map_err(runtime_error)?;
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
            .map_err(runtime_error)?;
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
            .map_err(runtime_error)?;
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
        let outcome = py
            .detach(move || {
                let bytes = read_regular_file(&local_file)?;
                client
                    .publish_artifact(objects.as_ref(), options, &bytes)
                    .map_err(|error| error.to_string())
            })
            .map_err(PyRuntimeError::new_err)?;
        publish_outcome_to_py(py, &outcome)
    }

    fn read<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        path: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        let target = parse_workspace_path(workbench, path)?;
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let outcome = py
            .detach(move || {
                client.read_artifact(objects.as_ref(), None, target, WorkspaceReadView::Live)
            })
            .map_err(runtime_error)?;
        read_outcome_to_py(py, &outcome)
    }

    fn read_range<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        path: &str,
        offset: u64,
        length: usize,
    ) -> PyResult<Bound<'py, PyDict>> {
        let target = parse_workspace_path(workbench, path)?;
        let client = Arc::clone(&self.client);
        let objects = Arc::clone(&self.objects);
        let outcome = py
            .detach(move || {
                client.read_artifact_range(
                    objects.as_ref(),
                    None,
                    target,
                    WorkspaceReadView::Live,
                    offset,
                    length,
                )
            })
            .map_err(runtime_error)?;
        read_outcome_to_py(py, &outcome)
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
                    scope: QueryScope::Root { path_prefix: None },
                    field_prefix,
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
    #[pyo3(signature = (workbench, local_directory, prefix = None))]
    fn materialize<'py>(
        &self,
        py: Python<'py>,
        workbench: &str,
        local_directory: &str,
        prefix: Option<&str>,
    ) -> PyResult<Bound<'py, PyList>> {
        let workbench = parse_workbench(workbench)?;
        let prefix = parse_optional_relative_path(prefix)?;
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

fn parse_workbench(raw: &str) -> PyResult<WorkbenchName> {
    WorkbenchName::new(raw.to_owned()).map_err(value_error)
}

fn parse_relative_path(raw: &str) -> PyResult<RelativePath> {
    RelativePath::new(raw.to_owned()).map_err(value_error)
}

fn parse_optional_relative_path(raw: Option<&str>) -> PyResult<Option<RelativePath>> {
    raw.map(parse_relative_path).transpose()
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
) -> Result<Vec<PathMetadata>, String> {
    list_all_paths_with(workbench, prefix, |request| {
        client.list_paths(request).map(|call| call.value)
    })
}

fn list_all_paths_with(
    workbench: WorkbenchName,
    prefix: Option<RelativePath>,
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
                view: WorkspaceReadView::Live,
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
    local_directory: &Path,
) -> Result<Vec<MaterializedFile>, String> {
    let root = prepare_materialize_root(local_directory)?;
    let entries = list_all_paths(client, workbench, prefix.clone())?;
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
                .read_artifact(
                    objects,
                    None,
                    expected.path.clone(),
                    WorkspaceReadView::Live,
                )
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
        let entries = list_all_paths_with(WorkbenchName::new("run-42").unwrap(), None, |request| {
            requests.push(request);
            responses.pop_front().expect("one scripted list response")
        })
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
        let error = list_all_paths_with(WorkbenchName::new("run-42").unwrap(), None, |_| {
            calls += 1;
            Err(read_version_failure(None))
        })
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
}
