use nokv_meta::{
    DentryWithAttr, MetadError, NamespaceAggregateGroup, NamespaceAggregateMeasure,
    NamespaceAggregateOp, NamespaceAggregateOutputMeasure, NamespaceAggregateRequest,
    NamespaceAggregateResult, NamespaceAggregateSample, NamespaceAggregateSort,
    NamespaceAggregateValue, NamespaceCard, NamespaceCardKind, NamespaceFacetSummary,
    NamespaceFacetValue, NamespaceFieldSource, NamespaceFieldSourceKind, NamespaceFieldValue,
    NamespaceFilterCapability, NamespaceFindField, NamespaceFindRequest, NamespaceFindResult,
    NamespaceGrepMatch, NamespaceGrepRequest, NamespaceGrepResult, NamespaceInclude,
    NamespaceIndexValue, NamespaceListPage, NamespacePredicate, NamespacePredicateOp,
    NamespacePredicateValue, NamespaceQueryCatalog, NamespaceReadFormat, NamespaceReadOptions,
    NamespaceReadPage, NamespaceRecordCount, NamespaceRecordType, NamespaceSchema, NamespaceSort,
    NamespaceSortDirection, NamespaceSortField, PreparedArtifact, RecordCountProvenance,
    SubtreeDelta, SubtreeDeltaKind, UpdateAttr, XattrSetMode,
};
use nokv_object::{ObjectKey, ObjectReadBlock, StagedObject, StagedObjectSet};
use nokv_protocol::{
    MetadataProtocolError, MetadataRpcEnvelope, MetadataRpcResult, WireAdvisoryLock,
    WireBodyReadPlan, WireDentryWithAttr, WireMetadataError, WireNamespaceAggregateGroup,
    WireNamespaceAggregateMeasure, WireNamespaceAggregateOp, WireNamespaceAggregateOutputMeasure,
    WireNamespaceAggregateRequest, WireNamespaceAggregateResult, WireNamespaceAggregateSample,
    WireNamespaceAggregateSort, WireNamespaceAggregateValue, WireNamespaceCard,
    WireNamespaceCardKind, WireNamespaceFacetSummary, WireNamespaceFacetValue,
    WireNamespaceFieldSource, WireNamespaceFieldSourceKind, WireNamespaceFieldValue,
    WireNamespaceFilterCapability, WireNamespaceFindField, WireNamespaceFindRequest,
    WireNamespaceFindResult, WireNamespaceGrepMatch, WireNamespaceGrepRequest,
    WireNamespaceGrepResult, WireNamespaceInclude, WireNamespaceIndexValue, WireNamespaceListPage,
    WireNamespacePredicate, WireNamespacePredicateOp, WireNamespacePredicateValue,
    WireNamespaceQueryCatalog, WireNamespaceReadFormat, WireNamespaceReadItem,
    WireNamespaceReadOptions, WireNamespaceReadPage, WireNamespaceRecordCount,
    WireNamespaceRecordType, WireNamespaceSchema, WireNamespaceSort, WireNamespaceSortDirection,
    WireNamespaceSortField, WireObjectReadBlock, WireOpenPathReadPlan, WirePathMetadata,
    WirePreparedArtifact, WireReadLease, WireRecordCountProvenance, WireStagedObjectSet,
    WireSubtreeDelta, WireSubtreeDeltaKind, WireUpdateAttr, WireXattrSetMode,
};
use nokv_types::{DentryName, InodeId, MountId};

use crate::server::ServerError;

pub(super) fn ok_envelope(result: MetadataRpcResult) -> MetadataRpcEnvelope {
    MetadataRpcEnvelope {
        ok: true,
        result: Some(result),
        error: None,
        error_kind: None,
    }
}

pub(super) fn err_envelope(err: ServerError) -> MetadataRpcEnvelope {
    let error_kind = wire_server_error(&err);
    MetadataRpcEnvelope {
        ok: false,
        result: None,
        error: Some(err.to_string()),
        error_kind: Some(error_kind),
    }
}

pub(super) fn wire_server_error(err: &ServerError) -> WireMetadataError {
    match err {
        ServerError::Io(err) => WireMetadataError::Io {
            message: err.to_string(),
        },
        ServerError::Object(err) => WireMetadataError::Object {
            message: err.to_string(),
        },
        ServerError::Control(err) => WireMetadataError::Metadata {
            message: err.to_string(),
        },
        ServerError::NotOwner { shard_id, endpoint } => WireMetadataError::NotOwner {
            shard_id: shard_id.clone(),
            endpoint: endpoint.clone(),
        },
        ServerError::Metadata(err) => wire_metad_error(err),
    }
}

fn wire_metad_error(err: &MetadError) -> WireMetadataError {
    match err {
        MetadError::Metadata(nokv_meta::MetadataError::PredicateFailed) => {
            WireMetadataError::PredicateFailed
        }
        MetadError::Metadata(err) => WireMetadataError::Metadata {
            message: err.to_string(),
        },
        MetadError::Object(err) => WireMetadataError::Object {
            message: err.to_string(),
        },
        MetadError::PublishArtifactFailed { source, .. } => wire_metad_error(source),
        MetadError::StaleBodyGeneration { expected, current } => {
            WireMetadataError::StaleBodyGeneration {
                expected: *expected,
                current: *current,
            }
        }
        MetadError::StaleOwnerEpoch {
            owner_epoch,
            required_epoch,
        } => WireMetadataError::StaleOwnerEpoch {
            owner_epoch: *owner_epoch,
            required_epoch: *required_epoch,
        },
        MetadError::LeaseExpired {
            now_ms,
            deadline_ms,
        } => WireMetadataError::LeaseExpired {
            now_ms: *now_ms,
            deadline_ms: *deadline_ms,
        },
        MetadError::SyncLogArchiveFailed { committed, message } => {
            WireMetadataError::SyncLogArchiveFailed {
                committed: *committed,
                message: message.clone(),
            }
        }
        MetadError::LockConflict(lock) => WireMetadataError::LockConflict {
            lock: WireAdvisoryLock::from_advisory_lock(lock),
        },
        MetadError::InvalidPath(message) => WireMetadataError::InvalidPath {
            message: message.clone(),
        },
        MetadError::InvalidQuery(message) => WireMetadataError::InvalidQuery {
            message: message.clone(),
        },
        MetadError::NotFound => WireMetadataError::NotFound,
        MetadError::NotFile => WireMetadataError::NotFile,
        MetadError::NotDirectory => WireMetadataError::NotDirectory,
        MetadError::DirectoryNotEmpty => WireMetadataError::DirectoryNotEmpty,
        MetadError::MissingBodyDescriptor => WireMetadataError::MissingBodyDescriptor,
        MetadError::CrossShard {
            source_shard,
            dest_shard,
        } => WireMetadataError::CrossShard {
            source_shard: *source_shard,
            dest_shard: *dest_shard,
        },
        MetadError::GraftPoint => WireMetadataError::GraftPoint,
        other => WireMetadataError::Metadata {
            message: other.to_string(),
        },
    }
}

pub(super) fn inode_id(raw: u64) -> Result<InodeId, MetadError> {
    InodeId::new(raw).map_err(Into::into)
}

pub(super) fn dentry_name(name: String) -> Result<DentryName, MetadError> {
    DentryName::new(name.into_bytes()).map_err(|err| MetadError::Codec(err.to_string()))
}

pub(super) fn update_attr(wire: WireUpdateAttr) -> UpdateAttr {
    UpdateAttr {
        mode: wire.mode,
        uid: wire.uid,
        gid: wire.gid,
        size: wire.size,
        mtime_ms: wire.mtime_ms,
        ctime_ms: wire.ctime_ms,
    }
}

pub(super) fn xattr_set_mode(wire: WireXattrSetMode) -> XattrSetMode {
    match wire {
        WireXattrSetMode::Any => XattrSetMode::Any,
        WireXattrSetMode::Create => XattrSetMode::Create,
        WireXattrSetMode::Replace => XattrSetMode::Replace,
    }
}

pub(super) fn wire_dentry(entry: &DentryWithAttr) -> WireDentryWithAttr {
    WireDentryWithAttr {
        dentry: nokv_protocol::WireDentryRecord::from_dentry_record(&entry.dentry),
        attr: nokv_protocol::WireInodeAttr::from_inode_attr(&entry.attr),
        body: entry
            .body
            .as_ref()
            .map(nokv_protocol::WireBodyDescriptor::from_body_descriptor),
    }
}

pub(super) fn wire_subtree_delta(delta: &SubtreeDelta) -> WireSubtreeDelta {
    WireSubtreeDelta {
        path: delta.path.clone(),
        kind: match delta.kind {
            SubtreeDeltaKind::Added => WireSubtreeDeltaKind::Added,
            SubtreeDeltaKind::Removed => WireSubtreeDeltaKind::Removed,
            SubtreeDeltaKind::Modified => WireSubtreeDeltaKind::Modified,
        },
        digest: delta.digest.clone(),
        size_delta: delta.size_delta,
    }
}

pub(super) fn wire_prepared_artifact(
    mount: MountId,
    prepared: &PreparedArtifact,
) -> WirePreparedArtifact {
    WirePreparedArtifact {
        mount: mount.get(),
        parent: prepared.parent.get(),
        name: String::from_utf8(prepared.name.as_bytes().to_vec())
            .expect("metadata prepared artifact names are utf-8"),
        path: prepared.path.clone(),
        inode: prepared.inode.get(),
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    }
}

pub(super) fn prepared_artifact(
    prepared: WirePreparedArtifact,
) -> Result<PreparedArtifact, MetadError> {
    MountId::new(prepared.mount)?;
    Ok(PreparedArtifact {
        parent: inode_id(prepared.parent)?,
        name: dentry_name(prepared.name)?,
        path: prepared.path,
        inode: inode_id(prepared.inode)?,
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    })
}

pub(super) fn staged_object_set(
    staged: WireStagedObjectSet,
) -> Result<StagedObjectSet, MetadError> {
    staged
        .objects
        .into_iter()
        .map(|object| {
            Ok(StagedObject {
                key: ObjectKey::new(object.key)?,
                size: object.size,
            })
        })
        .collect::<Result<Vec<_>, MetadError>>()
        .map(StagedObjectSet::new)
}

pub(super) fn wire_body_read_plan(plan: &nokv_meta::BodyReadPlan) -> WireBodyReadPlan {
    WireBodyReadPlan {
        output_len: plan.output_len as u64,
        blocks: plan.blocks.iter().map(wire_object_read_block).collect(),
    }
}

pub(super) fn wire_open_path_read_plan(open: &nokv_meta::OpenPathReadPlan) -> WireOpenPathReadPlan {
    WireOpenPathReadPlan {
        metadata: WirePathMetadata::from_path_metadata(&open.metadata),
        lease: WireReadLease::from_read_lease(&open.lease),
        plan: wire_body_read_plan(&open.plan),
    }
}

pub(super) fn wire_namespace_card(card: &NamespaceCard) -> WireNamespaceCard {
    WireNamespaceCard {
        path: card.path.clone(),
        name: card.name.clone(),
        kind: wire_namespace_card_kind(&card.kind),
        evidence: card.evidence.clone(),
        snapshot_id: card.snapshot_id,
        inode: card.inode.get(),
        generation: card.generation,
        size_bytes: card.size_bytes,
        entry_count: card.entry_count.map(|count| count as u64),
        record_count: card.record_count.as_ref().map(wire_namespace_record_count),
        schema: card.schema.as_ref().map(wire_namespace_schema),
        sample: card.sample.clone(),
        body: card
            .body
            .as_ref()
            .map(|body| nokv_protocol::WireBodyDescriptor {
                producer: body.producer.clone(),
                digest_uri: body.digest_uri.clone(),
                size: body.size,
                content_type: body.content_type.clone(),
                manifest_id: body.manifest_id.clone(),
                generation: body.generation,
                // Namespace cards are an informational projection and do not
                // carry the data-plane fallthrough chain; reads resolve it
                // server-side, never from a card.
                base_generation: 0,
                chunk_size: body.chunk_size,
                block_size: body.block_size,
            }),
        catalog: wire_namespace_query_catalog(&card.catalog),
        indexed_values: card
            .indexed_values
            .iter()
            .map(wire_namespace_index_value)
            .collect(),
    }
}

fn wire_namespace_card_kind(kind: &NamespaceCardKind) -> WireNamespaceCardKind {
    match kind {
        NamespaceCardKind::File => WireNamespaceCardKind::File,
        NamespaceCardKind::Directory => WireNamespaceCardKind::Directory,
        NamespaceCardKind::Symlink => WireNamespaceCardKind::Symlink,
        NamespaceCardKind::Special => WireNamespaceCardKind::Special,
    }
}

fn wire_namespace_record_count(count: &NamespaceRecordCount) -> WireNamespaceRecordCount {
    WireNamespaceRecordCount {
        count: count.count as u64,
        provenance: match count.provenance {
            RecordCountProvenance::LiveNamespace => WireRecordCountProvenance::LiveNamespace,
            RecordCountProvenance::StructuredBody => WireRecordCountProvenance::StructuredBody,
            RecordCountProvenance::MaterializedIndex => {
                WireRecordCountProvenance::MaterializedIndex
            }
            RecordCountProvenance::Approximate => WireRecordCountProvenance::Approximate,
        },
    }
}

fn wire_namespace_schema(schema: &NamespaceSchema) -> WireNamespaceSchema {
    WireNamespaceSchema {
        record_type: wire_namespace_record_type(&schema.record_type),
        fields: schema.fields.clone(),
    }
}

fn wire_namespace_record_type(record_type: &NamespaceRecordType) -> WireNamespaceRecordType {
    match record_type {
        NamespaceRecordType::DirectoryEntries => WireNamespaceRecordType::DirectoryEntries,
        NamespaceRecordType::JsonArray => WireNamespaceRecordType::JsonArray,
        NamespaceRecordType::JsonObject => WireNamespaceRecordType::JsonObject,
        NamespaceRecordType::YamlMapping => WireNamespaceRecordType::YamlMapping,
        NamespaceRecordType::TextLines => WireNamespaceRecordType::TextLines,
    }
}

fn wire_namespace_query_catalog(catalog: &NamespaceQueryCatalog) -> WireNamespaceQueryCatalog {
    WireNamespaceQueryCatalog {
        filterable: catalog
            .filterable
            .iter()
            .map(wire_namespace_filter_capability)
            .collect(),
        sortable: catalog
            .sortable
            .iter()
            .map(wire_namespace_sort_field)
            .collect(),
        facetable: catalog
            .facetable
            .iter()
            .map(wire_namespace_find_field)
            .collect(),
        facets: catalog
            .facets
            .iter()
            .map(wire_namespace_facet_summary)
            .collect(),
        projections: catalog
            .projections
            .iter()
            .map(wire_namespace_include)
            .collect(),
    }
}

fn wire_namespace_facet_summary(facet: &NamespaceFacetSummary) -> WireNamespaceFacetSummary {
    WireNamespaceFacetSummary {
        field: wire_namespace_find_field(&facet.field),
        values: facet
            .values
            .iter()
            .map(wire_namespace_facet_value)
            .collect(),
        distinct_count: facet.distinct_count as u64,
        truncated: facet.truncated,
    }
}

fn wire_namespace_facet_value(value: &NamespaceFacetValue) -> WireNamespaceFacetValue {
    WireNamespaceFacetValue {
        value: wire_namespace_predicate_value(&value.value),
        count: value.count as u64,
    }
}

fn wire_namespace_filter_capability(
    capability: &NamespaceFilterCapability,
) -> WireNamespaceFilterCapability {
    WireNamespaceFilterCapability {
        field: wire_namespace_find_field(&capability.field),
        operators: capability
            .operators
            .iter()
            .map(wire_namespace_predicate_op)
            .collect(),
    }
}

fn wire_namespace_include(include: &NamespaceInclude) -> WireNamespaceInclude {
    match include {
        NamespaceInclude::Body => WireNamespaceInclude::Body,
        NamespaceInclude::Schema => WireNamespaceInclude::Schema,
        NamespaceInclude::Sample => WireNamespaceInclude::Sample,
        NamespaceInclude::Catalog => WireNamespaceInclude::Catalog,
    }
}

fn wire_namespace_find_field(field: &NamespaceFindField) -> WireNamespaceFindField {
    WireNamespaceFindField {
        id: field.id.clone(),
    }
}

fn wire_namespace_predicate_op(op: &NamespacePredicateOp) -> WireNamespacePredicateOp {
    match op {
        NamespacePredicateOp::Eq => WireNamespacePredicateOp::Eq,
        NamespacePredicateOp::NotEqual => WireNamespacePredicateOp::NotEqual,
        NamespacePredicateOp::In => WireNamespacePredicateOp::In,
        NamespacePredicateOp::Prefix => WireNamespacePredicateOp::Prefix,
        NamespacePredicateOp::Suffix => WireNamespacePredicateOp::Suffix,
        NamespacePredicateOp::Contains => WireNamespacePredicateOp::Contains,
        NamespacePredicateOp::GreaterThan => WireNamespacePredicateOp::GreaterThan,
        NamespacePredicateOp::GreaterThanOrEqual => WireNamespacePredicateOp::GreaterThanOrEqual,
        NamespacePredicateOp::LessThan => WireNamespacePredicateOp::LessThan,
        NamespacePredicateOp::LessThanOrEqual => WireNamespacePredicateOp::LessThanOrEqual,
        NamespacePredicateOp::Exists => WireNamespacePredicateOp::Exists,
        NamespacePredicateOp::NotExists => WireNamespacePredicateOp::NotExists,
    }
}

fn wire_namespace_predicate(predicate: &NamespacePredicate) -> WireNamespacePredicate {
    WireNamespacePredicate {
        field: wire_namespace_find_field(&predicate.field),
        op: wire_namespace_predicate_op(&predicate.op),
        value: predicate.value.as_ref().map(wire_namespace_predicate_value),
    }
}

fn wire_namespace_sort_field(field: &NamespaceSortField) -> WireNamespaceSortField {
    WireNamespaceSortField {
        id: field.id.clone(),
    }
}

fn wire_namespace_index_value(value: &NamespaceIndexValue) -> WireNamespaceIndexValue {
    WireNamespaceIndexValue {
        field: wire_namespace_find_field(&value.field),
        value: wire_namespace_predicate_value(&value.value),
    }
}

fn wire_namespace_predicate_value(value: &NamespacePredicateValue) -> WireNamespacePredicateValue {
    match value {
        NamespacePredicateValue::String(value) => {
            WireNamespacePredicateValue::String(value.clone())
        }
        NamespacePredicateValue::U64(value) => WireNamespacePredicateValue::U64(*value),
        NamespacePredicateValue::F64(value) => WireNamespacePredicateValue::F64(*value),
        NamespacePredicateValue::List(values) => WireNamespacePredicateValue::List(
            values.iter().map(wire_namespace_predicate_value).collect(),
        ),
    }
}

pub(super) fn wire_namespace_list_page(
    page: &NamespaceListPage,
) -> Result<WireNamespaceListPage, ServerError> {
    Ok(WireNamespaceListPage {
        path: page.path.clone(),
        evidence: page.evidence.clone(),
        snapshot_id: page.snapshot_id,
        entry_count: page.entry_count as u64,
        entries: page.entries.iter().map(wire_namespace_card).collect(),
        next_cursor: page.next_cursor.clone(),
        truncated: page.truncated,
    })
}

pub(super) fn wire_namespace_find_result(
    result: &NamespaceFindResult,
) -> Result<WireNamespaceFindResult, ServerError> {
    Ok(WireNamespaceFindResult {
        path: result.path.clone(),
        evidence: result.evidence.clone(),
        snapshot_id: result.snapshot_id,
        match_count: result.match_count as u64,
        matches: result.matches.iter().map(wire_namespace_card).collect(),
        facets: result
            .facets
            .iter()
            .map(wire_namespace_facet_summary)
            .collect(),
        next_cursor: result.next_cursor.clone(),
        truncated: result.truncated,
        scanned_entries: result.scanned_entries as u64,
    })
}

pub(super) fn wire_namespace_aggregate_result(
    result: &NamespaceAggregateResult,
) -> WireNamespaceAggregateResult {
    WireNamespaceAggregateResult {
        path: result.path.clone(),
        evidence: result.evidence.clone(),
        snapshot_id: result.snapshot_id,
        predicates: result
            .predicates
            .iter()
            .map(wire_namespace_predicate)
            .collect(),
        input_match_count: result.input_match_count as u64,
        row_count: result.row_count as u64,
        group_count: result.group_count as u64,
        groups: result
            .groups
            .iter()
            .map(wire_namespace_aggregate_group)
            .collect(),
        truncated: result.truncated,
        scanned_entries: result.scanned_entries as u64,
    }
}

fn wire_namespace_aggregate_group(group: &NamespaceAggregateGroup) -> WireNamespaceAggregateGroup {
    WireNamespaceAggregateGroup {
        key: group.key.iter().map(wire_namespace_field_value).collect(),
        measures: group
            .measures
            .iter()
            .map(wire_namespace_aggregate_output_measure)
            .collect(),
        evidence: group.evidence.clone(),
        sample_matches: group
            .sample_matches
            .iter()
            .map(wire_namespace_aggregate_sample)
            .collect(),
    }
}

fn wire_namespace_field_value(value: &NamespaceFieldValue) -> WireNamespaceFieldValue {
    WireNamespaceFieldValue {
        field: wire_namespace_find_field(&value.field),
        value: wire_namespace_predicate_value(&value.value),
        source: wire_namespace_field_source(&value.source),
    }
}

fn wire_namespace_field_source(source: &NamespaceFieldSource) -> WireNamespaceFieldSource {
    WireNamespaceFieldSource {
        evidence: source.evidence.clone(),
        source_path: source.source_path.clone(),
        source_kind: match source.source_kind {
            NamespaceFieldSourceKind::Namespace => WireNamespaceFieldSourceKind::Namespace,
            NamespaceFieldSourceKind::MaterializedIndex => {
                WireNamespaceFieldSourceKind::MaterializedIndex
            }
        },
    }
}

fn wire_namespace_aggregate_output_measure(
    measure: &NamespaceAggregateOutputMeasure,
) -> WireNamespaceAggregateOutputMeasure {
    WireNamespaceAggregateOutputMeasure {
        name: measure.name.clone(),
        op: wire_namespace_aggregate_op(&measure.op),
        field: measure.field.as_ref().map(wire_namespace_find_field),
        value: wire_namespace_aggregate_value(&measure.value),
    }
}

fn wire_namespace_aggregate_sample(
    sample: &NamespaceAggregateSample,
) -> WireNamespaceAggregateSample {
    WireNamespaceAggregateSample {
        path: sample.path.clone(),
        evidence: sample.evidence.clone(),
        generation: sample.generation,
    }
}

fn wire_namespace_aggregate_value(value: &NamespaceAggregateValue) -> WireNamespaceAggregateValue {
    match value {
        NamespaceAggregateValue::U64(value) => WireNamespaceAggregateValue::U64(*value),
        NamespaceAggregateValue::F64(value) => WireNamespaceAggregateValue::F64(*value),
        NamespaceAggregateValue::Null => WireNamespaceAggregateValue::Null,
    }
}

fn wire_namespace_aggregate_op(op: &NamespaceAggregateOp) -> WireNamespaceAggregateOp {
    match op {
        NamespaceAggregateOp::Count => WireNamespaceAggregateOp::Count,
        NamespaceAggregateOp::Sum => WireNamespaceAggregateOp::Sum,
        NamespaceAggregateOp::Avg => WireNamespaceAggregateOp::Avg,
        NamespaceAggregateOp::Min => WireNamespaceAggregateOp::Min,
        NamespaceAggregateOp::Max => WireNamespaceAggregateOp::Max,
    }
}

pub(super) fn wire_namespace_grep_result(
    result: &NamespaceGrepResult,
) -> Result<WireNamespaceGrepResult, ServerError> {
    Ok(WireNamespaceGrepResult {
        path: result.path.clone(),
        pattern: result.pattern.clone(),
        recursive: result.recursive,
        evidence: result.evidence.clone(),
        snapshot_id: result.snapshot_id,
        matches: result
            .matches
            .iter()
            .map(wire_namespace_grep_match)
            .collect(),
        files_scanned: result.files_scanned as u64,
        bytes_read: result.bytes_read as u64,
        next_cursor: result.next_cursor.clone(),
        truncated: result.truncated,
    })
}

fn wire_namespace_grep_match(match_: &NamespaceGrepMatch) -> WireNamespaceGrepMatch {
    WireNamespaceGrepMatch {
        path: match_.path.clone(),
        line_number: match_.line_number as u64,
        snippet: match_.snippet.clone(),
        evidence: match_.evidence.clone(),
        generation: match_.generation,
    }
}

pub(super) fn wire_namespace_read_page(
    page: &NamespaceReadPage,
) -> Result<WireNamespaceReadPage, ServerError> {
    Ok(WireNamespaceReadPage {
        path: page.path.clone(),
        evidence: page.evidence.clone(),
        snapshot_id: page.snapshot_id,
        generation: page.generation,
        total_size_bytes: page.total_size_bytes,
        format: match page.format {
            NamespaceReadFormat::Structured => WireNamespaceReadFormat::Structured,
            NamespaceReadFormat::Bytes => WireNamespaceReadFormat::Bytes,
        },
        record_type: page.record_type.as_ref().map(wire_namespace_record_type),
        record_count: page.record_count.map(|count| count as u64),
        cursor: page.cursor.clone(),
        next_cursor: page.next_cursor.clone(),
        truncated: page.truncated,
        items: page
            .items
            .iter()
            .map(|item| WireNamespaceReadItem {
                index: item.index as u64,
                value_json: item.value_json.clone(),
                evidence: item.evidence.clone(),
            })
            .collect(),
        bytes: page.bytes.clone(),
    })
}

pub(super) fn namespace_find_request(
    request: WireNamespaceFindRequest,
) -> Result<NamespaceFindRequest, ServerError> {
    Ok(NamespaceFindRequest {
        path: request.path,
        predicates: request
            .predicates
            .into_iter()
            .map(namespace_predicate)
            .collect::<Result<Vec<_>, _>>()?,
        sort: request
            .sort
            .into_iter()
            .map(namespace_sort)
            .collect::<Vec<_>>(),
        include: request
            .include
            .into_iter()
            .map(namespace_include)
            .collect::<Vec<_>>(),
        facets: request
            .facets
            .into_iter()
            .map(namespace_find_field)
            .collect::<Vec<_>>(),
        cursor: request.cursor,
        limit: usize::try_from(request.limit).map_err(|_| {
            ServerError::Metadata(MetadError::InvalidQuery(
                "namespace find limit exceeds platform limit".to_owned(),
            ))
        })?,
    })
}

pub(super) fn namespace_aggregate_request(
    request: WireNamespaceAggregateRequest,
) -> Result<NamespaceAggregateRequest, ServerError> {
    Ok(NamespaceAggregateRequest {
        path: request.path,
        predicates: request
            .predicates
            .into_iter()
            .map(namespace_predicate)
            .collect::<Result<Vec<_>, _>>()?,
        group_by: request
            .group_by
            .into_iter()
            .map(namespace_find_field)
            .collect(),
        measures: request
            .measures
            .into_iter()
            .map(namespace_aggregate_measure)
            .collect(),
        sort: request
            .sort
            .into_iter()
            .map(namespace_aggregate_sort)
            .collect(),
        limit: usize::try_from(request.limit).map_err(|_| {
            ServerError::Metadata(MetadError::InvalidQuery(
                "namespace aggregate limit exceeds platform limit".to_owned(),
            ))
        })?,
    })
}

pub(super) fn namespace_grep_request(
    request: WireNamespaceGrepRequest,
) -> Result<NamespaceGrepRequest, ServerError> {
    Ok(NamespaceGrepRequest {
        path: request.path,
        pattern: request.pattern,
        recursive: request.recursive,
        cursor: request.cursor,
        limit: usize::try_from(request.limit).map_err(|_| {
            ServerError::Metadata(MetadError::InvalidQuery(
                "namespace grep limit exceeds platform limit".to_owned(),
            ))
        })?,
        max_files: request
            .max_files
            .map(|value| {
                usize::try_from(value).map_err(|_| {
                    ServerError::Metadata(MetadError::InvalidQuery(
                        "namespace grep max_files exceeds platform limit".to_owned(),
                    ))
                })
            })
            .transpose()?,
        max_bytes: request
            .max_bytes
            .map(|value| {
                usize::try_from(value).map_err(|_| {
                    ServerError::Metadata(MetadError::InvalidQuery(
                        "namespace grep max_bytes exceeds platform limit".to_owned(),
                    ))
                })
            })
            .transpose()?,
    })
}

fn namespace_include(include: WireNamespaceInclude) -> NamespaceInclude {
    match include {
        WireNamespaceInclude::Body => NamespaceInclude::Body,
        WireNamespaceInclude::Schema => NamespaceInclude::Schema,
        WireNamespaceInclude::Sample => NamespaceInclude::Sample,
        WireNamespaceInclude::Catalog => NamespaceInclude::Catalog,
    }
}

fn namespace_predicate(
    predicate: WireNamespacePredicate,
) -> Result<NamespacePredicate, ServerError> {
    Ok(NamespacePredicate {
        field: namespace_find_field(predicate.field),
        op: namespace_predicate_op(predicate.op),
        value: predicate.value.map(namespace_predicate_value),
    })
}

fn namespace_predicate_value(value: WireNamespacePredicateValue) -> NamespacePredicateValue {
    match value {
        WireNamespacePredicateValue::String(value) => NamespacePredicateValue::String(value),
        WireNamespacePredicateValue::U64(value) => NamespacePredicateValue::U64(value),
        WireNamespacePredicateValue::F64(value) => NamespacePredicateValue::F64(value),
        WireNamespacePredicateValue::List(values) => NamespacePredicateValue::List(
            values.into_iter().map(namespace_predicate_value).collect(),
        ),
    }
}

fn namespace_aggregate_measure(
    measure: WireNamespaceAggregateMeasure,
) -> NamespaceAggregateMeasure {
    NamespaceAggregateMeasure {
        name: measure.name,
        op: namespace_aggregate_op(measure.op),
        field: measure.field.map(namespace_find_field),
    }
}

fn namespace_aggregate_sort(sort: WireNamespaceAggregateSort) -> NamespaceAggregateSort {
    NamespaceAggregateSort {
        field: sort.field,
        direction: namespace_sort_direction(sort.direction),
    }
}

fn namespace_aggregate_op(op: WireNamespaceAggregateOp) -> NamespaceAggregateOp {
    match op {
        WireNamespaceAggregateOp::Count => NamespaceAggregateOp::Count,
        WireNamespaceAggregateOp::Sum => NamespaceAggregateOp::Sum,
        WireNamespaceAggregateOp::Avg => NamespaceAggregateOp::Avg,
        WireNamespaceAggregateOp::Min => NamespaceAggregateOp::Min,
        WireNamespaceAggregateOp::Max => NamespaceAggregateOp::Max,
    }
}

fn namespace_find_field(field: WireNamespaceFindField) -> NamespaceFindField {
    NamespaceFindField::new(field.id)
}

fn namespace_predicate_op(op: WireNamespacePredicateOp) -> NamespacePredicateOp {
    match op {
        WireNamespacePredicateOp::Eq => NamespacePredicateOp::Eq,
        WireNamespacePredicateOp::NotEqual => NamespacePredicateOp::NotEqual,
        WireNamespacePredicateOp::In => NamespacePredicateOp::In,
        WireNamespacePredicateOp::Prefix => NamespacePredicateOp::Prefix,
        WireNamespacePredicateOp::Suffix => NamespacePredicateOp::Suffix,
        WireNamespacePredicateOp::Contains => NamespacePredicateOp::Contains,
        WireNamespacePredicateOp::GreaterThan => NamespacePredicateOp::GreaterThan,
        WireNamespacePredicateOp::GreaterThanOrEqual => NamespacePredicateOp::GreaterThanOrEqual,
        WireNamespacePredicateOp::LessThan => NamespacePredicateOp::LessThan,
        WireNamespacePredicateOp::LessThanOrEqual => NamespacePredicateOp::LessThanOrEqual,
        WireNamespacePredicateOp::Exists => NamespacePredicateOp::Exists,
        WireNamespacePredicateOp::NotExists => NamespacePredicateOp::NotExists,
    }
}

fn namespace_sort(sort: WireNamespaceSort) -> NamespaceSort {
    NamespaceSort {
        field: NamespaceSortField::new(sort.field.id),
        direction: namespace_sort_direction(sort.direction),
    }
}

fn namespace_sort_direction(direction: WireNamespaceSortDirection) -> NamespaceSortDirection {
    match direction {
        WireNamespaceSortDirection::Asc => NamespaceSortDirection::Asc,
        WireNamespaceSortDirection::Desc => NamespaceSortDirection::Desc,
    }
}

pub(super) fn namespace_read_options(
    options: WireNamespaceReadOptions,
) -> Result<NamespaceReadOptions, ServerError> {
    Ok(NamespaceReadOptions {
        format: match options.format {
            WireNamespaceReadFormat::Structured => NamespaceReadFormat::Structured,
            WireNamespaceReadFormat::Bytes => NamespaceReadFormat::Bytes,
        },
        cursor: options.cursor,
        offset: options.offset,
        limit: usize::try_from(options.limit).map_err(|_| {
            ServerError::Metadata(MetadError::InvalidQuery(
                "namespace read limit exceeds platform limit".to_owned(),
            ))
        })?,
        expected_generation: options.expected_generation,
    })
}

fn wire_object_read_block(block: &ObjectReadBlock) -> WireObjectReadBlock {
    WireObjectReadBlock {
        object_key: block.object_key.clone(),
        digest_uri: block.digest_uri.clone(),
        object_offset: block.object_offset,
        object_len: block.object_len,
        len: block.len as u64,
        output_offset: block.output_offset as u64,
    }
}

pub(super) fn protocol_error(err: MetadataProtocolError) -> MetadError {
    MetadError::Codec(err.to_string())
}
