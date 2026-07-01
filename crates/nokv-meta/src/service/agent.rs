use super::*;
use std::cmp::Ordering as CmpOrdering;

const DEFAULT_PAGE_LIMIT: usize = 100;
const DEFAULT_SAMPLE_LIMIT: usize = 3;
const DEFAULT_FACET_VALUE_LIMIT: usize = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceCardKind {
    File,
    Directory,
    Symlink,
    Special,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceRecordType {
    DirectoryEntries,
    JsonArray,
    JsonObject,
    YamlMapping,
    TextLines,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordCountProvenance {
    LiveNamespace,
    StructuredBody,
    MaterializedIndex,
    Approximate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceRecordCount {
    pub count: usize,
    pub provenance: RecordCountProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceSchema {
    pub record_type: NamespaceRecordType,
    pub fields: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceBodyDescriptor {
    pub producer: String,
    pub digest_uri: String,
    pub size: u64,
    pub content_type: String,
    pub manifest_id: String,
    pub generation: u64,
    pub chunk_size: u64,
    pub block_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceInclude {
    Body,
    Schema,
    Sample,
    Catalog,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFilterCapability {
    pub field: NamespaceFindField,
    pub operators: Vec<NamespacePredicateOp>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceQueryCatalog {
    pub filterable: Vec<NamespaceFilterCapability>,
    pub sortable: Vec<NamespaceSortField>,
    pub facetable: Vec<NamespaceFindField>,
    pub facets: Vec<NamespaceFacetSummary>,
    pub projections: Vec<NamespaceInclude>,
}

impl NamespaceQueryCatalog {
    fn empty() -> Self {
        Self {
            filterable: Vec::new(),
            sortable: Vec::new(),
            facetable: Vec::new(),
            facets: Vec::new(),
            projections: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFacetSummary {
    pub field: NamespaceFindField,
    pub values: Vec<NamespaceFacetValue>,
    pub distinct_count: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFacetValue {
    pub value: NamespacePredicateValue,
    pub count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceCard {
    pub path: String,
    pub name: String,
    pub kind: NamespaceCardKind,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub inode: InodeId,
    pub generation: u64,
    pub size_bytes: Option<u64>,
    pub entry_count: Option<usize>,
    pub record_count: Option<NamespaceRecordCount>,
    pub schema: Option<NamespaceSchema>,
    pub sample: Vec<String>,
    pub body: Option<NamespaceBodyDescriptor>,
    pub catalog: NamespaceQueryCatalog,
    pub indexed_values: Vec<NamespaceIndexValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceListOptions {
    pub cursor: Option<String>,
    pub limit: usize,
}

impl Default for NamespaceListOptions {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: DEFAULT_PAGE_LIMIT,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceListPage {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub entry_count: usize,
    pub entries: Vec<NamespaceCard>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceFindField {
    pub id: String,
}

impl NamespaceFindField {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn path() -> Self {
        Self::new("path")
    }

    pub fn name() -> Self {
        Self::new("name")
    }

    pub fn kind() -> Self {
        Self::new("kind")
    }

    pub fn size_bytes() -> Self {
        Self::new("size_bytes")
    }

    pub fn body_content_type() -> Self {
        Self::new("body.content_type")
    }

    pub fn body_producer() -> Self {
        Self::new("body.producer")
    }

    pub fn body_manifest_id() -> Self {
        Self::new("body.manifest_id")
    }

    fn as_str(&self) -> &str {
        &self.id
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespacePredicateOp {
    Eq,
    NotEqual,
    In,
    Prefix,
    Suffix,
    Contains,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Exists,
    NotExists,
}

#[derive(Clone, Debug)]
pub enum NamespacePredicateValue {
    String(String),
    U64(u64),
    F64(f64),
    List(Vec<NamespacePredicateValue>),
}

impl PartialEq for NamespacePredicateValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}

impl Eq for NamespacePredicateValue {}

impl PartialOrd for NamespacePredicateValue {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for NamespacePredicateValue {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        predicate_value_rank(self)
            .cmp(&predicate_value_rank(other))
            .then_with(|| match (self, other) {
                (Self::String(left), Self::String(right)) => left.cmp(right),
                (Self::U64(left), Self::U64(right)) => left.cmp(right),
                (Self::F64(left), Self::F64(right)) => left.total_cmp(right),
                (Self::List(left), Self::List(right)) => left.cmp(right),
                _ => CmpOrdering::Equal,
            })
    }
}

fn predicate_value_rank(value: &NamespacePredicateValue) -> u8 {
    match value {
        NamespacePredicateValue::String(_) => 0,
        NamespacePredicateValue::U64(_) => 1,
        NamespacePredicateValue::F64(_) => 2,
        NamespacePredicateValue::List(_) => 3,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespacePredicate {
    pub field: NamespaceFindField,
    pub op: NamespacePredicateOp,
    pub value: Option<NamespacePredicateValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceSortField {
    pub id: String,
}

impl NamespaceSortField {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn path() -> Self {
        Self::new("path")
    }

    pub fn name() -> Self {
        Self::new("name")
    }

    pub fn size_bytes() -> Self {
        Self::new("size_bytes")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceSortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceSort {
    pub field: NamespaceSortField,
    pub direction: NamespaceSortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFindRequest {
    pub path: String,
    pub predicates: Vec<NamespacePredicate>,
    pub sort: Vec<NamespaceSort>,
    pub include: Vec<NamespaceInclude>,
    pub facets: Vec<NamespaceFindField>,
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFindResult {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub match_count: usize,
    pub matches: Vec<NamespaceCard>,
    pub facets: Vec<NamespaceFacetSummary>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub scanned_entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceFieldSourceKind {
    Namespace,
    MaterializedIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFieldSource {
    pub evidence: String,
    pub source_path: String,
    pub source_kind: NamespaceFieldSourceKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFieldValue {
    pub field: NamespaceFindField,
    pub value: NamespacePredicateValue,
    pub source: NamespaceFieldSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceAggregateOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceAggregateMeasure {
    pub name: String,
    pub op: NamespaceAggregateOp,
    pub field: Option<NamespaceFindField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceAggregateSort {
    pub field: String,
    pub direction: NamespaceSortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceAggregateRequest {
    pub path: String,
    pub predicates: Vec<NamespacePredicate>,
    pub group_by: Vec<NamespaceFindField>,
    pub measures: Vec<NamespaceAggregateMeasure>,
    pub sort: Vec<NamespaceAggregateSort>,
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub enum NamespaceAggregateValue {
    U64(u64),
    F64(f64),
    Null,
}

impl PartialEq for NamespaceAggregateValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::U64(left), Self::U64(right)) => left == right,
            (Self::F64(left), Self::F64(right)) => left.total_cmp(right) == CmpOrdering::Equal,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl Eq for NamespaceAggregateValue {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceAggregateOutputMeasure {
    pub name: String,
    pub op: NamespaceAggregateOp,
    pub field: Option<NamespaceFindField>,
    pub value: NamespaceAggregateValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceAggregateSample {
    pub path: String,
    pub evidence: String,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceAggregateGroup {
    pub key: Vec<NamespaceFieldValue>,
    pub measures: Vec<NamespaceAggregateOutputMeasure>,
    pub evidence: String,
    pub sample_matches: Vec<NamespaceAggregateSample>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceAggregateResult {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub predicates: Vec<NamespacePredicate>,
    pub input_match_count: usize,
    pub row_count: usize,
    pub group_count: usize,
    pub groups: Vec<NamespaceAggregateGroup>,
    pub truncated: bool,
    pub scanned_entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceGrepRequest {
    pub path: String,
    pub pattern: String,
    pub recursive: bool,
    pub cursor: Option<String>,
    pub limit: usize,
    pub max_files: Option<usize>,
    pub max_bytes: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceGrepMatch {
    pub path: String,
    pub line_number: usize,
    pub snippet: String,
    pub evidence: String,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceGrepResult {
    pub path: String,
    pub pattern: String,
    pub recursive: bool,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub matches: Vec<NamespaceGrepMatch>,
    pub files_scanned: usize,
    pub bytes_read: usize,
    pub next_cursor: Option<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceReadFormat {
    Structured,
    Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceReadOptions {
    pub format: NamespaceReadFormat,
    pub cursor: Option<String>,
    pub offset: u64,
    pub limit: usize,
    pub expected_generation: Option<u64>,
}

impl Default for NamespaceReadOptions {
    fn default() -> Self {
        Self {
            format: NamespaceReadFormat::Structured,
            cursor: None,
            offset: 0,
            limit: DEFAULT_PAGE_LIMIT,
            expected_generation: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceReadItem {
    pub index: usize,
    pub value_json: String,
    pub evidence: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceReadPage {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub generation: u64,
    pub total_size_bytes: u64,
    pub format: NamespaceReadFormat,
    pub record_type: Option<NamespaceRecordType>,
    pub record_count: Option<usize>,
    pub cursor: Option<String>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub items: Vec<NamespaceReadItem>,
    pub bytes: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceIndexField {
    pub field: NamespaceFindField,
    pub operators: Vec<NamespacePredicateOp>,
    pub sortable: bool,
    pub facetable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceIndexValue {
    pub field: NamespaceFindField,
    pub value: NamespacePredicateValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceIndexRow {
    pub path: String,
    pub values: Vec<NamespaceIndexValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceIndexRegistration {
    pub path: String,
    pub fields: Vec<NamespaceIndexField>,
    pub rows: Vec<NamespaceIndexRow>,
}

#[derive(Clone, Debug)]
struct TraversalEntry {
    path: String,
    name: String,
    entry: DentryWithAttr,
}

#[derive(Clone, Debug)]
struct GrepCandidate {
    path: String,
    metadata: PathMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GrepCursor {
    candidate_index: usize,
    line_index: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CardProjection {
    body: bool,
    schema: bool,
    sample: bool,
    catalog: bool,
}

struct CardContext<'a> {
    version: Version,
    projection: CardProjection,
    indexed_values: Vec<NamespaceIndexValue>,
    indexes: Option<&'a LoadedNamespaceIndex>,
}

#[derive(Clone, Debug, Default)]
struct LoadedNamespaceIndex {
    fields: Vec<NamespaceIndexField>,
    rows: BTreeMap<String, Vec<NamespaceIndexValue>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AggregateGroupKey(Vec<NamespacePredicateValue>);

#[derive(Clone, Debug)]
struct AggregateBuilder {
    key: Vec<NamespaceFieldValue>,
    measures: Vec<AggregateAccumulator>,
    sample_matches: Vec<NamespaceAggregateSample>,
}

#[derive(Clone, Debug)]
struct AggregateAccumulator {
    op: NamespaceAggregateOp,
    count: usize,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}

#[derive(Clone, Debug)]
struct StructuredRecords {
    record_type: NamespaceRecordType,
    fields: Vec<String>,
    items: Vec<String>,
}

impl CardProjection {
    fn full() -> Self {
        Self {
            body: true,
            schema: true,
            sample: true,
            catalog: true,
        }
    }

    fn list_entry() -> Self {
        Self {
            body: false,
            schema: false,
            sample: false,
            catalog: false,
        }
    }

    fn find(includes: &[NamespaceInclude]) -> Self {
        Self {
            body: includes.contains(&NamespaceInclude::Body),
            schema: includes.contains(&NamespaceInclude::Schema),
            sample: includes.contains(&NamespaceInclude::Sample),
            catalog: includes.contains(&NamespaceInclude::Catalog),
        }
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, MetadError> {
        let path = normalize_card_path(path)?;
        let version = self.read_version()?;
        let metadata = self.stat_path_from_at_version(InodeId::root(), &path, version)?;
        metadata
            .map(|metadata| {
                let indexes = self.load_namespace_indexes_for_path(&path, version)?;
                self.card_for_metadata(
                    path_name_for_card(&path),
                    &path,
                    metadata,
                    CardContext {
                        version,
                        projection: CardProjection::full(),
                        indexed_values: indexes.rows.get(&path).cloned().unwrap_or_default(),
                        indexes: Some(&indexes),
                    },
                )
            })
            .transpose()
    }

    pub fn list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, MetadError> {
        let path = normalize_card_path(path)?;
        let limit = bounded_limit(options.limit)?;
        let offset = parse_cursor(options.cursor.as_deref())?;
        let version = self.read_version()?;
        let parent = self.resolve_directory_path_at_version(&path, version)?;
        let mut entries = self.read_dir_plus_at_version(parent, version)?;
        entries.sort_by(|left, right| {
            left.dentry
                .name
                .as_bytes()
                .cmp(right.dentry.name.as_bytes())
        });
        let entry_count = entries.len();
        let page_entries = entries
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|entry| {
                let name = String::from_utf8_lossy(entry.dentry.name.as_bytes()).to_string();
                let child_path = join_card_path(&path, &name);
                self.card_for_entry(
                    name,
                    child_path,
                    entry,
                    CardContext {
                        version,
                        projection: CardProjection::list_entry(),
                        indexed_values: Vec::new(),
                        indexes: None,
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next_offset = offset.saturating_add(page_entries.len());
        let truncated = next_offset < entry_count;
        Ok(NamespaceListPage {
            evidence: namespace_evidence(&path, None),
            path,
            snapshot_id: Some(version.get()),
            entry_count,
            entries: page_entries,
            next_cursor: truncated.then(|| next_offset.to_string()),
            truncated,
        })
    }

    pub fn find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, MetadError> {
        validate_find_request(&request)?;
        let root = normalize_card_path(&request.path)?;
        let limit = bounded_limit(request.limit)?;
        let offset = parse_cursor(request.cursor.as_deref())?;
        let version = self.read_version()?;
        let root_inode = self.resolve_directory_path_at_version(&root, version)?;
        let indexes = self.load_namespace_indexes_for_path(&root, version)?;
        let mut entries = Vec::new();
        self.collect_entries(&root, root_inode, version, &mut entries)?;
        let scanned_entries = entries.len();
        let include_body = request.include.contains(&NamespaceInclude::Body);
        let body_facet_requested = request.facets.iter().any(field_needs_body_projection);
        let mut projection = CardProjection::find(&request.include);
        if body_facet_requested {
            projection.body = true;
        }
        let mut cards = entries
            .into_iter()
            .filter_map(|entry| {
                let indexed_values = indexes.rows.get(&entry.path).cloned().unwrap_or_default();
                matches_predicates(&entry, &indexed_values, &request.predicates)
                    .then_some((entry, indexed_values))
            })
            .map(|(entry, indexed_values)| {
                self.card_for_entry(
                    entry.name,
                    entry.path,
                    entry.entry,
                    CardContext {
                        version,
                        projection,
                        indexed_values,
                        indexes: Some(&indexes),
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        apply_sort(&mut cards, &request.sort);
        let facets = filtered_facet_summaries(&cards, &request.facets, DEFAULT_FACET_VALUE_LIMIT);
        if body_facet_requested && !include_body {
            for card in &mut cards {
                card.body = None;
            }
        }
        let total_matches = cards.len();
        let matches = cards
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
        let next_offset = offset.saturating_add(matches.len());
        let truncated = next_offset < total_matches;
        Ok(NamespaceFindResult {
            evidence: namespace_evidence(&root, None),
            path: root,
            snapshot_id: Some(version.get()),
            match_count: total_matches,
            matches,
            facets,
            next_cursor: truncated.then(|| next_offset.to_string()),
            truncated,
            scanned_entries,
        })
    }

    pub fn aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, MetadError> {
        validate_aggregate_request(&request)?;
        let root = normalize_card_path(&request.path)?;
        let limit = bounded_limit(request.limit)?;
        let version = self.read_version()?;
        let root_inode = self.resolve_directory_path_at_version(&root, version)?;
        let indexes = self.load_namespace_indexes_for_path(&root, version)?;
        let mut entries = Vec::new();
        self.collect_entries(&root, root_inode, version, &mut entries)?;
        let scanned_entries = entries.len();
        let mut input_match_count = 0_usize;
        let mut row_count = 0_usize;
        let mut groups = BTreeMap::<AggregateGroupKey, AggregateBuilder>::new();

        for entry in entries {
            let indexed_values = indexes.rows.get(&entry.path).cloned().unwrap_or_default();
            if !matches_predicates(&entry, &indexed_values, &request.predicates) {
                continue;
            }
            input_match_count = input_match_count.saturating_add(1);
            let key_values = request
                .group_by
                .iter()
                .map(|field| first_field_value(&entry, &indexed_values, field))
                .collect::<Vec<_>>();
            if !request.group_by.is_empty() && key_values.iter().any(Option::is_none) {
                continue;
            }
            if request
                .measures
                .iter()
                .any(|measure| aggregate_measure_requires_field(measure, &entry, &indexed_values))
            {
                continue;
            }

            row_count = row_count.saturating_add(1);
            let key = AggregateGroupKey(
                key_values
                    .iter()
                    .filter_map(|value| value.as_ref().map(|value| value.value.clone()))
                    .collect(),
            );
            let group = groups.entry(key).or_insert_with(|| AggregateBuilder {
                key: key_values.into_iter().flatten().collect(),
                measures: request
                    .measures
                    .iter()
                    .map(|measure| AggregateAccumulator::new(measure.op.clone()))
                    .collect(),
                sample_matches: Vec::new(),
            });
            for (measure, accumulator) in request.measures.iter().zip(group.measures.iter_mut()) {
                if measure.op == NamespaceAggregateOp::Count && measure.field.is_none() {
                    accumulator.observe_count();
                    continue;
                }
                let value = measure
                    .field
                    .as_ref()
                    .and_then(|field| first_field_value(&entry, &indexed_values, field))
                    .map(|value| value.value);
                accumulator.observe(value.as_ref());
            }
            if group.sample_matches.len() < DEFAULT_SAMPLE_LIMIT {
                group.sample_matches.push(NamespaceAggregateSample {
                    path: entry.path.clone(),
                    evidence: namespace_evidence(&entry.path, Some(entry.entry.attr.generation)),
                    generation: entry.entry.attr.generation,
                });
            }
        }

        let mut output_groups = groups
            .into_values()
            .map(|group| aggregate_group_result(group, &request.measures, &root))
            .collect::<Vec<_>>();
        sort_namespace_aggregate_groups(&mut output_groups, &request);
        let group_count = output_groups.len();
        let truncated = group_count > limit;
        output_groups.truncate(limit);

        Ok(NamespaceAggregateResult {
            path: root.clone(),
            evidence: namespace_evidence(&root, None),
            snapshot_id: Some(version.get()),
            predicates: request.predicates,
            input_match_count,
            row_count,
            group_count,
            groups: output_groups,
            truncated,
            scanned_entries,
        })
    }

    pub fn grep_paths(
        &self,
        request: NamespaceGrepRequest,
    ) -> Result<NamespaceGrepResult, MetadError> {
        validate_grep_request(&request)?;
        let root = normalize_card_path(&request.path)?;
        let limit = bounded_limit(request.limit)?;
        let cursor = parse_grep_cursor(request.cursor.as_deref())?;
        let version = self.read_version()?;
        let metadata = self
            .stat_path_from_at_version(InodeId::root(), &root, version)?
            .ok_or(MetadError::NotFound)?;
        let candidates = self.grep_candidates(&root, metadata, request.recursive, version)?;
        let pattern_lower = request.pattern.to_lowercase();
        let mut matches = Vec::new();
        let mut files_scanned = 0_usize;
        let mut files_scanned_this_call = 0_usize;
        let mut bytes_read = 0_usize;
        let mut next_cursor = None;

        'candidates: for (candidate_index, candidate) in candidates.iter().enumerate() {
            if candidate_index < cursor.candidate_index {
                continue;
            }
            if let Some(max_files) = request.max_files {
                if files_scanned_this_call >= max_files {
                    next_cursor = Some(grep_cursor(candidate_index, 0));
                    break;
                }
            }
            let file_len = file_len(candidate.metadata.attr.size)?;
            if let Some(max_bytes) = request.max_bytes {
                if bytes_read.saturating_add(file_len) > max_bytes {
                    if bytes_read == 0 {
                        return Err(MetadError::InvalidQuery(format!(
                            "max_bytes {max_bytes} is smaller than candidate file {}",
                            candidate.path
                        )));
                    }
                    next_cursor = Some(grep_cursor(candidate_index, 0));
                    break;
                }
            }
            let bytes = match candidate.metadata.body.as_ref() {
                Some(body) => self.read_file_at_version(
                    candidate.metadata.attr.inode,
                    body,
                    0,
                    file_len,
                    version,
                )?,
                None if file_len == 0 => Vec::new(),
                None => return Err(MetadError::MissingBodyDescriptor),
            };
            bytes_read = bytes_read.saturating_add(bytes.len());
            files_scanned += 1;
            files_scanned_this_call += 1;
            if bytes.contains(&0) {
                continue;
            }

            let start_line = if candidate_index == cursor.candidate_index {
                cursor.line_index
            } else {
                0
            };
            let text = String::from_utf8_lossy(&bytes);
            for (line_index, line) in text.lines().enumerate().skip(start_line) {
                if !line.to_lowercase().contains(&pattern_lower) {
                    continue;
                }
                if matches.len() == limit {
                    next_cursor = Some(grep_cursor(candidate_index, line_index));
                    break 'candidates;
                }
                matches.push(NamespaceGrepMatch {
                    path: candidate.path.clone(),
                    line_number: line_index + 1,
                    snippet: line.chars().take(240).collect(),
                    evidence: format!(
                        "{}#L{}",
                        namespace_evidence(
                            &candidate.path,
                            Some(candidate.metadata.attr.generation)
                        ),
                        line_index + 1
                    ),
                    generation: candidate.metadata.attr.generation,
                });
            }
        }

        Ok(NamespaceGrepResult {
            evidence: namespace_evidence(&root, None),
            path: root,
            pattern: request.pattern,
            recursive: request.recursive,
            snapshot_id: Some(version.get()),
            matches,
            files_scanned,
            bytes_read,
            truncated: next_cursor.is_some(),
            next_cursor,
        })
    }

    pub fn read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, MetadError> {
        let path = normalize_card_path(path)?;
        let version = self.read_version()?;
        let metadata = self
            .stat_path_from_at_version(InodeId::root(), &path, version)?
            .ok_or(MetadError::NotFound)?;
        if metadata.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if let Some(expected) = options.expected_generation {
            if expected != metadata.attr.generation {
                return Err(MetadError::StaleBodyGeneration {
                    expected,
                    current: metadata.attr.generation,
                });
            }
        }
        match options.format {
            NamespaceReadFormat::Bytes => self.read_bytes_page(path, metadata, version, options),
            NamespaceReadFormat::Structured => {
                self.read_structured_page(path, metadata, version, options)
            }
        }
    }

    pub fn register_namespace_index(
        &self,
        registration: NamespaceIndexRegistration,
    ) -> Result<(), MetadError> {
        let path = normalize_card_path(&registration.path)?;
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let mut mutations = Vec::new();
        let catalog_key = path_index_catalog_key(self.mount, &path);
        let catalog_item = self.metadata.get_versioned(
            RecordFamily::PathIndex,
            &catalog_key,
            read_version,
            ReadPurpose::UserStrong,
        )?;
        let predicates = vec![PredicateRef {
            family: RecordFamily::PathIndex,
            key: catalog_key.clone(),
            predicate: catalog_item
                .map(|item| Predicate::VersionEquals(item.version))
                .unwrap_or(Predicate::NotExists),
        }];
        mutations.push(Mutation {
            family: RecordFamily::PathIndex,
            key: catalog_key.clone(),
            op: MutationOp::Put,
            value: Some(Value(encode_path_index_catalog(
                &path_index_catalog_record(&path, &registration),
            ))),
        });

        let old_rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::PathIndex,
            prefix: path_index_row_prefix(self.mount, &path),
            start_after: None,
            version: read_version,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        for row in old_rows {
            mutations.push(delete_mutation(RecordFamily::PathIndex, row.key));
        }

        for row in registration.rows {
            let row_path = normalize_card_path(&row.path)?;
            if !path_is_self_or_descendant(&path, &row_path) {
                return Err(MetadError::InvalidQuery(format!(
                    "index row path {row_path} is outside registered namespace {path}"
                )));
            }
            validate_index_values(&row.values)?;
            let record = path_index_row_record(&row_path, row.values);
            mutations.push(Mutation {
                family: RecordFamily::PathIndex,
                key: path_index_row_key(self.mount, &path, &row_path),
                op: MutationOp::Put,
                value: Some(Value(encode_path_index_row(&record))),
            });
        }

        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"register-namespace-index",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::RegisterNamespaceIndex,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::PathIndex,
            primary_key: catalog_key,
            predicates,
            mutations,
            watch: Vec::new(),
        })?;
        Ok(())
    }

    fn card_for_entry(
        &self,
        name: String,
        path: String,
        entry: DentryWithAttr,
        context: CardContext<'_>,
    ) -> Result<NamespaceCard, MetadError> {
        self.card_for_metadata(
            name,
            &path,
            PathMetadata {
                attr: entry.attr,
                body: entry.body,
            },
            context,
        )
    }

    fn card_for_metadata(
        &self,
        name: String,
        path: &str,
        metadata: PathMetadata,
        context: CardContext<'_>,
    ) -> Result<NamespaceCard, MetadError> {
        let kind = card_kind(metadata.attr.file_type);
        let mut entry_count = None;
        let mut record_count = None;
        let mut schema = None;
        let mut sample = Vec::new();
        if metadata.attr.file_type == FileType::Directory {
            let entries = self.read_dir_plus_at_version(metadata.attr.inode, context.version)?;
            let count = entries.len();
            entry_count = Some(count);
            if context.projection.schema || context.projection.sample || context.projection.catalog
            {
                record_count = Some(NamespaceRecordCount {
                    count,
                    provenance: RecordCountProvenance::LiveNamespace,
                });
            }
            if context.projection.schema {
                schema = Some(NamespaceSchema {
                    record_type: NamespaceRecordType::DirectoryEntries,
                    fields: vec![
                        "path".to_owned(),
                        "file.name".to_owned(),
                        "file.type".to_owned(),
                        "file.size_bytes".to_owned(),
                    ],
                });
            }
            if context.projection.sample {
                sample = entries
                    .iter()
                    .take(DEFAULT_SAMPLE_LIMIT)
                    .map(|entry| String::from_utf8_lossy(entry.dentry.name.as_bytes()).to_string())
                    .collect();
            }
        } else if metadata.attr.file_type == FileType::File
            && (context.projection.schema || context.projection.sample)
            && metadata
                .body
                .as_ref()
                .map(|body| is_structured_path(path, body))
                .unwrap_or(false)
        {
            if let Some(summary) =
                self.structured_summary(path, &metadata, context.version, DEFAULT_SAMPLE_LIMIT)?
            {
                record_count = Some(NamespaceRecordCount {
                    count: summary.count,
                    provenance: RecordCountProvenance::StructuredBody,
                });
                if context.projection.schema {
                    schema = Some(NamespaceSchema {
                        record_type: summary.record_type,
                        fields: summary.fields,
                    });
                }
                if context.projection.sample {
                    sample = summary.sample;
                }
            }
        }
        let body = context
            .projection
            .body
            .then(|| metadata.body.as_ref().map(namespace_body_descriptor))
            .flatten();
        let catalog = if context.projection.catalog {
            let mut catalog = namespace_query_catalog(kind.clone());
            if let Some(indexes) = context.indexes {
                merge_index_catalog(&mut catalog, indexes);
                if !indexes.rows.is_empty() {
                    record_count = Some(NamespaceRecordCount {
                        count: indexes.rows.len(),
                        provenance: RecordCountProvenance::MaterializedIndex,
                    });
                }
            }
            catalog
        } else {
            NamespaceQueryCatalog::empty()
        };
        Ok(NamespaceCard {
            evidence: namespace_evidence(path, Some(metadata.attr.generation)),
            path: path.to_owned(),
            name,
            kind,
            snapshot_id: Some(context.version.get()),
            inode: metadata.attr.inode,
            generation: metadata.attr.generation,
            size_bytes: (metadata.attr.file_type != FileType::Directory)
                .then_some(metadata.attr.size),
            entry_count,
            record_count,
            schema,
            sample,
            body,
            catalog,
            indexed_values: context.indexed_values,
        })
    }

    fn collect_entries(
        &self,
        root_path: &str,
        root_inode: InodeId,
        version: Version,
        out: &mut Vec<TraversalEntry>,
    ) -> Result<(), MetadError> {
        let mut entries = self.read_dir_plus_at_version(root_inode, version)?;
        entries.sort_by(|left, right| {
            left.dentry
                .name
                .as_bytes()
                .cmp(right.dentry.name.as_bytes())
        });
        for entry in entries {
            let name = String::from_utf8_lossy(entry.dentry.name.as_bytes()).to_string();
            let path = join_card_path(root_path, &name);
            if entry.attr.file_type == FileType::Directory {
                self.collect_entries(&path, entry.attr.inode, version, out)?;
            }
            out.push(TraversalEntry { path, name, entry });
        }
        Ok(())
    }

    fn grep_candidates(
        &self,
        root_path: &str,
        metadata: PathMetadata,
        recursive: bool,
        version: Version,
    ) -> Result<Vec<GrepCandidate>, MetadError> {
        match metadata.attr.file_type {
            FileType::File => Ok(vec![GrepCandidate {
                path: root_path.to_owned(),
                metadata,
            }]),
            FileType::Directory => {
                if recursive {
                    let mut entries = Vec::new();
                    self.collect_entries(root_path, metadata.attr.inode, version, &mut entries)?;
                    Ok(entries
                        .into_iter()
                        .filter_map(|entry| {
                            (entry.entry.attr.file_type == FileType::File).then_some(
                                GrepCandidate {
                                    path: entry.path,
                                    metadata: PathMetadata {
                                        attr: entry.entry.attr,
                                        body: entry.entry.body,
                                    },
                                },
                            )
                        })
                        .collect())
                } else {
                    let mut entries =
                        self.read_dir_plus_at_version(metadata.attr.inode, version)?;
                    entries.sort_by(|left, right| {
                        left.dentry
                            .name
                            .as_bytes()
                            .cmp(right.dentry.name.as_bytes())
                    });
                    Ok(entries
                        .into_iter()
                        .filter_map(|entry| {
                            if entry.attr.file_type != FileType::File {
                                return None;
                            }
                            let name =
                                String::from_utf8_lossy(entry.dentry.name.as_bytes()).to_string();
                            Some(GrepCandidate {
                                path: join_card_path(root_path, &name),
                                metadata: PathMetadata {
                                    attr: entry.attr,
                                    body: entry.body,
                                },
                            })
                        })
                        .collect())
                }
            }
            FileType::Symlink
            | FileType::NamedPipe
            | FileType::CharDevice
            | FileType::BlockDevice
            | FileType::Socket => Err(MetadError::NotFile),
        }
    }

    fn resolve_directory_path_at_version(
        &self,
        path: &str,
        version: Version,
    ) -> Result<InodeId, MetadError> {
        let components = parse_absolute_path(path)?;
        self.resolve_components_as_directory_at_version(&components, version)
    }

    fn load_namespace_indexes_for_path(
        &self,
        path: &str,
        version: Version,
    ) -> Result<LoadedNamespaceIndex, MetadError> {
        let Some(catalog) = self.metadata.get(
            RecordFamily::PathIndex,
            &path_index_catalog_key(self.mount, path),
            version,
            ReadPurpose::UserStrong,
        )?
        else {
            return Ok(LoadedNamespaceIndex::default());
        };
        let catalog = decode_path_index_catalog(&catalog.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::PathIndex,
            prefix: path_index_row_prefix(self.mount, path),
            start_after: None,
            version,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        let mut indexed_rows = BTreeMap::new();
        for row in rows {
            let row = decode_path_index_row(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            indexed_rows.insert(
                row.path,
                row.values
                    .into_iter()
                    .map(|(field, value)| NamespaceIndexValue {
                        field: NamespaceFindField::new(field),
                        value: namespace_value_from_record(value),
                    })
                    .collect(),
            );
        }
        Ok(LoadedNamespaceIndex {
            fields: catalog
                .fields
                .into_iter()
                .map(namespace_index_field_from_record)
                .collect(),
            rows: indexed_rows,
        })
    }

    fn structured_summary(
        &self,
        path: &str,
        metadata: &PathMetadata,
        version: Version,
        sample_limit: usize,
    ) -> Result<Option<StructuredSummary>, MetadError> {
        let body = metadata
            .body
            .as_ref()
            .ok_or(MetadError::MissingBodyDescriptor)?;
        let bytes = self.read_file_at_version(
            metadata.attr.inode,
            body,
            0,
            file_len(metadata.attr.size)?,
            version,
        )?;
        let records = structured_records(path, body, &bytes)?;
        Ok(Some(StructuredSummary {
            record_type: records.record_type,
            count: records.items.len(),
            fields: records.fields,
            sample: records.items.into_iter().take(sample_limit).collect(),
        }))
    }

    fn read_bytes_page(
        &self,
        path: String,
        metadata: PathMetadata,
        version: Version,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, MetadError> {
        let limit = bounded_limit(options.limit)?;
        let offset = parse_byte_cursor(options.cursor.as_deref(), options.offset)?;
        let body = metadata
            .body
            .as_ref()
            .ok_or(MetadError::MissingBodyDescriptor)?;
        let bytes = self.read_file_at_version(metadata.attr.inode, body, offset, limit, version)?;
        let next_offset = offset.saturating_add(
            u64::try_from(bytes.len())
                .map_err(|_| MetadError::InvalidQuery("read length overflow".to_owned()))?,
        );
        let truncated = next_offset < metadata.attr.size;
        Ok(NamespaceReadPage {
            evidence: namespace_evidence(&path, Some(metadata.attr.generation)),
            path,
            snapshot_id: Some(version.get()),
            generation: metadata.attr.generation,
            total_size_bytes: metadata.attr.size,
            format: NamespaceReadFormat::Bytes,
            record_type: None,
            record_count: None,
            cursor: options.cursor,
            next_cursor: truncated.then(|| next_offset.to_string()),
            truncated,
            items: Vec::new(),
            bytes: Some(bytes),
        })
    }

    fn read_structured_page(
        &self,
        path: String,
        metadata: PathMetadata,
        version: Version,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, MetadError> {
        let limit = bounded_limit(options.limit)?;
        let offset = parse_cursor(options.cursor.as_deref())?;
        let body = metadata
            .body
            .as_ref()
            .ok_or(MetadError::MissingBodyDescriptor)?;
        let bytes = self.read_file_at_version(
            metadata.attr.inode,
            body,
            0,
            file_len(metadata.attr.size)?,
            version,
        )?;
        let records = structured_records(&path, body, &bytes)?;
        let page_items = records
            .items
            .iter()
            .enumerate()
            .skip(offset)
            .take(limit)
            .map(|(index, value)| {
                Ok(NamespaceReadItem {
                    index,
                    value_json: value.clone(),
                    evidence: format!(
                        "{}#item:{index}",
                        namespace_evidence(&path, Some(metadata.attr.generation))
                    ),
                })
            })
            .collect::<Result<Vec<_>, MetadError>>()?;
        let next_offset = offset.saturating_add(page_items.len());
        let truncated = next_offset < records.items.len();
        Ok(NamespaceReadPage {
            evidence: namespace_evidence(&path, Some(metadata.attr.generation)),
            path,
            snapshot_id: Some(version.get()),
            generation: metadata.attr.generation,
            total_size_bytes: metadata.attr.size,
            format: NamespaceReadFormat::Structured,
            record_type: Some(records.record_type),
            record_count: Some(records.items.len()),
            cursor: options.cursor,
            next_cursor: truncated.then(|| next_offset.to_string()),
            truncated,
            items: page_items,
            bytes: None,
        })
    }
}

struct StructuredSummary {
    record_type: NamespaceRecordType,
    count: usize,
    fields: Vec<String>,
    sample: Vec<String>,
}

fn validate_find_request(request: &NamespaceFindRequest) -> Result<(), MetadError> {
    bounded_limit(request.limit)?;
    for predicate in &request.predicates {
        validate_predicate(predicate)?;
    }
    Ok(())
}

fn validate_aggregate_request(request: &NamespaceAggregateRequest) -> Result<(), MetadError> {
    bounded_limit(request.limit)?;
    for predicate in &request.predicates {
        validate_predicate(predicate)?;
    }
    if request.measures.is_empty() {
        return Err(MetadError::InvalidQuery(
            "aggregate measures must not be empty".to_owned(),
        ));
    }
    for measure in &request.measures {
        if measure.name.is_empty() {
            return Err(MetadError::InvalidQuery(
                "aggregate measure name must not be empty".to_owned(),
            ));
        }
        if measure.op != NamespaceAggregateOp::Count && measure.field.is_none() {
            return Err(MetadError::InvalidQuery(format!(
                "aggregate measure {} requires a field",
                measure.name
            )));
        }
    }
    Ok(())
}

fn validate_index_values(values: &[NamespaceIndexValue]) -> Result<(), MetadError> {
    for value in values {
        match &value.value {
            NamespacePredicateValue::F64(raw) if !raw.is_finite() => {
                return Err(MetadError::InvalidQuery(format!(
                    "index field {} contains non-finite f64",
                    value.field.id
                )));
            }
            NamespacePredicateValue::List(_) => {
                return Err(MetadError::InvalidQuery(format!(
                    "index field {} contains predicate-only list value",
                    value.field.id
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

fn path_is_self_or_descendant(root: &str, path: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| root == "/" || suffix.starts_with('/'))
}

fn validate_grep_request(request: &NamespaceGrepRequest) -> Result<(), MetadError> {
    bounded_limit(request.limit)?;
    if let Some(max_files) = request.max_files {
        if max_files == 0 {
            return Err(MetadError::InvalidQuery(
                "max_files must be greater than zero".to_owned(),
            ));
        }
    }
    if let Some(max_bytes) = request.max_bytes {
        if max_bytes == 0 {
            return Err(MetadError::InvalidQuery(
                "max_bytes must be greater than zero".to_owned(),
            ));
        }
    }
    Ok(())
}

fn namespace_query_catalog(kind: NamespaceCardKind) -> NamespaceQueryCatalog {
    if kind != NamespaceCardKind::Directory {
        return NamespaceQueryCatalog::empty();
    }
    NamespaceQueryCatalog {
        filterable: vec![
            string_filter_capability(NamespaceFindField::path()),
            string_filter_capability(NamespaceFindField::name()),
            string_filter_capability(NamespaceFindField::kind()),
            numeric_filter_capability(NamespaceFindField::size_bytes()),
            string_filter_capability(NamespaceFindField::body_content_type()),
            string_filter_capability(NamespaceFindField::body_producer()),
            string_filter_capability(NamespaceFindField::body_manifest_id()),
        ],
        sortable: vec![
            NamespaceSortField::path(),
            NamespaceSortField::name(),
            NamespaceSortField::size_bytes(),
        ],
        facetable: vec![
            NamespaceFindField::kind(),
            NamespaceFindField::body_content_type(),
            NamespaceFindField::body_producer(),
        ],
        facets: Vec::new(),
        projections: vec![
            NamespaceInclude::Body,
            NamespaceInclude::Schema,
            NamespaceInclude::Sample,
            NamespaceInclude::Catalog,
        ],
    }
}

fn string_filter_capability(field: NamespaceFindField) -> NamespaceFilterCapability {
    NamespaceFilterCapability {
        field,
        operators: vec![
            NamespacePredicateOp::Eq,
            NamespacePredicateOp::NotEqual,
            NamespacePredicateOp::In,
            NamespacePredicateOp::Prefix,
            NamespacePredicateOp::Suffix,
            NamespacePredicateOp::Contains,
            NamespacePredicateOp::Exists,
            NamespacePredicateOp::NotExists,
        ],
    }
}

fn numeric_filter_capability(field: NamespaceFindField) -> NamespaceFilterCapability {
    NamespaceFilterCapability {
        field,
        operators: vec![
            NamespacePredicateOp::Eq,
            NamespacePredicateOp::NotEqual,
            NamespacePredicateOp::In,
            NamespacePredicateOp::GreaterThan,
            NamespacePredicateOp::GreaterThanOrEqual,
            NamespacePredicateOp::LessThan,
            NamespacePredicateOp::LessThanOrEqual,
            NamespacePredicateOp::Exists,
            NamespacePredicateOp::NotExists,
        ],
    }
}

fn validate_predicate(predicate: &NamespacePredicate) -> Result<(), MetadError> {
    match (&predicate.op, &predicate.value) {
        (NamespacePredicateOp::Exists | NamespacePredicateOp::NotExists, _) => Ok(()),
        (NamespacePredicateOp::In, Some(NamespacePredicateValue::List(values))) => {
            if values.is_empty()
                || values
                    .iter()
                    .any(|value| !valid_scalar_predicate_value(value))
            {
                return Err(MetadError::InvalidQuery(format!(
                    "unsupported predicate {:?} {:?} {:?}",
                    predicate.field, predicate.op, predicate.value
                )));
            }
            Ok(())
        }
        (NamespacePredicateOp::Eq | NamespacePredicateOp::NotEqual, Some(value))
            if valid_scalar_predicate_value(value) =>
        {
            Ok(())
        }
        (_, Some(NamespacePredicateValue::String(_))) if string_operator(&predicate.op) => Ok(()),
        (_, Some(value))
            if numeric_operator(&predicate.op) && numeric_predicate_value(value).is_some() =>
        {
            Ok(())
        }
        _ => Err(MetadError::InvalidQuery(format!(
            "unsupported predicate {:?} {:?} {:?}",
            predicate.field, predicate.op, predicate.value
        ))),
    }
}

fn valid_scalar_predicate_value(value: &NamespacePredicateValue) -> bool {
    match value {
        NamespacePredicateValue::F64(value) => value.is_finite(),
        NamespacePredicateValue::List(_) => false,
        NamespacePredicateValue::String(_) | NamespacePredicateValue::U64(_) => true,
    }
}

fn matches_predicates(
    entry: &TraversalEntry,
    indexed_values: &[NamespaceIndexValue],
    predicates: &[NamespacePredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| matches_predicate(entry, indexed_values, predicate))
}

fn matches_predicate(
    entry: &TraversalEntry,
    indexed_values: &[NamespaceIndexValue],
    predicate: &NamespacePredicate,
) -> bool {
    let values = field_values_for_entry(entry, indexed_values, &predicate.field);
    match predicate.op {
        NamespacePredicateOp::Exists => !values.is_empty(),
        NamespacePredicateOp::NotExists => values.is_empty(),
        NamespacePredicateOp::In => {
            let Some(NamespacePredicateValue::List(expected)) = &predicate.value else {
                return false;
            };
            values.iter().any(|actual| {
                expected
                    .iter()
                    .any(|expected| predicate_values_equal(&actual.value, expected))
            })
        }
        NamespacePredicateOp::NotEqual => {
            let Some(expected) = &predicate.value else {
                return false;
            };
            !values.is_empty()
                && values
                    .iter()
                    .all(|actual| !predicate_values_equal(&actual.value, expected))
        }
        NamespacePredicateOp::Eq => {
            let Some(expected) = &predicate.value else {
                return false;
            };
            values
                .iter()
                .any(|actual| predicate_values_equal(&actual.value, expected))
        }
        NamespacePredicateOp::Prefix
        | NamespacePredicateOp::Suffix
        | NamespacePredicateOp::Contains => {
            let Some(NamespacePredicateValue::String(expected)) = &predicate.value else {
                return false;
            };
            values.iter().any(|actual| {
                matches!(
                    &actual.value,
                    NamespacePredicateValue::String(value)
                        if matches_string(value, &predicate.op, expected)
                )
            })
        }
        NamespacePredicateOp::GreaterThan
        | NamespacePredicateOp::GreaterThanOrEqual
        | NamespacePredicateOp::LessThan
        | NamespacePredicateOp::LessThanOrEqual => {
            let Some(expected) = predicate.value.as_ref().and_then(numeric_predicate_value) else {
                return false;
            };
            values.iter().any(|actual| {
                numeric_predicate_value(&actual.value)
                    .map(|actual| matches_number(actual, &predicate.op, expected))
                    .unwrap_or(false)
            })
        }
    }
}

fn matches_string(actual: &str, op: &NamespacePredicateOp, expected: &str) -> bool {
    match op {
        NamespacePredicateOp::Eq => actual == expected,
        NamespacePredicateOp::Prefix => actual.starts_with(expected),
        NamespacePredicateOp::Suffix => actual.ends_with(expected),
        NamespacePredicateOp::Contains => actual.contains(expected),
        _ => false,
    }
}

fn matches_number(actual: f64, op: &NamespacePredicateOp, expected: f64) -> bool {
    match op {
        NamespacePredicateOp::GreaterThan => actual > expected,
        NamespacePredicateOp::GreaterThanOrEqual => actual >= expected,
        NamespacePredicateOp::LessThan => actual < expected,
        NamespacePredicateOp::LessThanOrEqual => actual <= expected,
        _ => false,
    }
}

fn first_field_value(
    entry: &TraversalEntry,
    indexed_values: &[NamespaceIndexValue],
    field: &NamespaceFindField,
) -> Option<NamespaceFieldValue> {
    field_values_for_entry(entry, indexed_values, field)
        .into_iter()
        .next()
}

fn field_values_for_entry(
    entry: &TraversalEntry,
    indexed_values: &[NamespaceIndexValue],
    field: &NamespaceFindField,
) -> Vec<NamespaceFieldValue> {
    let namespace_source = || NamespaceFieldSource {
        evidence: namespace_evidence(&entry.path, Some(entry.entry.attr.generation)),
        source_path: entry.path.clone(),
        source_kind: NamespaceFieldSourceKind::Namespace,
    };
    let materialized_source = || NamespaceFieldSource {
        evidence: namespace_evidence(&entry.path, Some(entry.entry.attr.generation)),
        source_path: entry.path.clone(),
        source_kind: NamespaceFieldSourceKind::MaterializedIndex,
    };
    match field.as_str() {
        "path" => vec![NamespaceFieldValue {
            field: field.clone(),
            value: NamespacePredicateValue::String(entry.path.clone()),
            source: namespace_source(),
        }],
        "name" => vec![NamespaceFieldValue {
            field: field.clone(),
            value: NamespacePredicateValue::String(entry.name.clone()),
            source: namespace_source(),
        }],
        "kind" => vec![NamespaceFieldValue {
            field: field.clone(),
            value: NamespacePredicateValue::String(
                file_type_label(entry.entry.attr.file_type).to_owned(),
            ),
            source: namespace_source(),
        }],
        "size_bytes" => vec![NamespaceFieldValue {
            field: field.clone(),
            value: NamespacePredicateValue::U64(entry.entry.attr.size),
            source: namespace_source(),
        }],
        "body.content_type" => entry
            .entry
            .body
            .as_ref()
            .map(|body| NamespaceFieldValue {
                field: field.clone(),
                value: NamespacePredicateValue::String(body.content_type.clone()),
                source: namespace_source(),
            })
            .into_iter()
            .collect(),
        "body.producer" => entry
            .entry
            .body
            .as_ref()
            .map(|body| NamespaceFieldValue {
                field: field.clone(),
                value: NamespacePredicateValue::String(body.producer.clone()),
                source: namespace_source(),
            })
            .into_iter()
            .collect(),
        "body.manifest_id" => entry
            .entry
            .body
            .as_ref()
            .map(|body| NamespaceFieldValue {
                field: field.clone(),
                value: NamespacePredicateValue::String(body.manifest_id.clone()),
                source: namespace_source(),
            })
            .into_iter()
            .collect(),
        _ => indexed_values
            .iter()
            .filter(|value| value.field == *field)
            .map(|value| NamespaceFieldValue {
                field: value.field.clone(),
                value: value.value.clone(),
                source: materialized_source(),
            })
            .collect(),
    }
}

fn predicate_values_equal(
    actual: &NamespacePredicateValue,
    expected: &NamespacePredicateValue,
) -> bool {
    if let (Some(actual), Some(expected)) = (
        numeric_predicate_value(actual),
        numeric_predicate_value(expected),
    ) {
        return actual.total_cmp(&expected) == CmpOrdering::Equal;
    }
    actual == expected
}

fn numeric_predicate_value(value: &NamespacePredicateValue) -> Option<f64> {
    match value {
        NamespacePredicateValue::U64(value) => Some(*value as f64),
        NamespacePredicateValue::F64(value) if value.is_finite() => Some(*value),
        NamespacePredicateValue::String(value) => parse_number_string(value),
        NamespacePredicateValue::List(_) | NamespacePredicateValue::F64(_) => None,
    }
}

fn parse_number_string(value: &str) -> Option<f64> {
    let number = value.parse::<f64>().ok().or_else(|| {
        serde_json::from_str::<serde_json::Value>(value)
            .ok()
            .and_then(|value| value.as_f64())
    })?;
    number.is_finite().then_some(number)
}

impl AggregateAccumulator {
    fn new(op: NamespaceAggregateOp) -> Self {
        Self {
            op,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
        }
    }

    fn observe_count(&mut self) {
        self.count = self.count.saturating_add(1);
    }

    fn observe(&mut self, value: Option<&NamespacePredicateValue>) {
        match self.op {
            NamespaceAggregateOp::Count => {
                if value.is_some() {
                    self.observe_count();
                }
            }
            NamespaceAggregateOp::Sum
            | NamespaceAggregateOp::Avg
            | NamespaceAggregateOp::Min
            | NamespaceAggregateOp::Max => {
                let Some(value) = value.and_then(numeric_predicate_value) else {
                    return;
                };
                self.count = self.count.saturating_add(1);
                self.sum += value;
                self.min = Some(self.min.map(|current| current.min(value)).unwrap_or(value));
                self.max = Some(self.max.map(|current| current.max(value)).unwrap_or(value));
            }
        }
    }

    fn value(&self) -> NamespaceAggregateValue {
        match self.op {
            NamespaceAggregateOp::Count => NamespaceAggregateValue::U64(self.count as u64),
            NamespaceAggregateOp::Sum => NamespaceAggregateValue::F64(self.sum),
            NamespaceAggregateOp::Avg => {
                if self.count == 0 {
                    NamespaceAggregateValue::Null
                } else {
                    NamespaceAggregateValue::F64(self.sum / self.count as f64)
                }
            }
            NamespaceAggregateOp::Min => self
                .min
                .map(NamespaceAggregateValue::F64)
                .unwrap_or(NamespaceAggregateValue::Null),
            NamespaceAggregateOp::Max => self
                .max
                .map(NamespaceAggregateValue::F64)
                .unwrap_or(NamespaceAggregateValue::Null),
        }
    }
}

fn aggregate_measure_requires_field(
    measure: &NamespaceAggregateMeasure,
    entry: &TraversalEntry,
    indexed_values: &[NamespaceIndexValue],
) -> bool {
    if measure.op == NamespaceAggregateOp::Count {
        return false;
    }
    measure
        .field
        .as_ref()
        .and_then(|field| first_field_value(entry, indexed_values, field))
        .is_none()
}

fn aggregate_group_result(
    group: AggregateBuilder,
    measures: &[NamespaceAggregateMeasure],
    root: &str,
) -> NamespaceAggregateGroup {
    NamespaceAggregateGroup {
        key: group.key,
        measures: measures
            .iter()
            .zip(group.measures)
            .map(|(measure, accumulator)| NamespaceAggregateOutputMeasure {
                name: measure.name.clone(),
                op: measure.op.clone(),
                field: measure.field.clone(),
                value: accumulator.value(),
            })
            .collect(),
        evidence: namespace_evidence(root, None),
        sample_matches: group.sample_matches,
    }
}

fn sort_namespace_aggregate_groups(
    groups: &mut [NamespaceAggregateGroup],
    request: &NamespaceAggregateRequest,
) {
    groups.sort_by(|left, right| {
        for sort in &request.sort {
            let ordering = aggregate_sort_value(left, &sort.field)
                .cmp(&aggregate_sort_value(right, &sort.field));
            let ordering = match sort.direction {
                NamespaceSortDirection::Asc => ordering,
                NamespaceSortDirection::Desc => ordering.reverse(),
            };
            if ordering != CmpOrdering::Equal {
                return ordering;
            }
        }
        left.key
            .iter()
            .map(|value| &value.value)
            .cmp(right.key.iter().map(|value| &value.value))
    });
}

fn aggregate_sort_value(group: &NamespaceAggregateGroup, field: &str) -> AggregateSortValue {
    if let Some(value) = group.key.iter().find(|value| value.field.id == field) {
        return AggregateSortValue::Predicate(value.value.clone());
    }
    if let Some(measure) = group.measures.iter().find(|measure| measure.name == field) {
        return AggregateSortValue::Aggregate(measure.value.clone());
    }
    AggregateSortValue::Null
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AggregateSortValue {
    Predicate(NamespacePredicateValue),
    Aggregate(NamespaceAggregateValue),
    Null,
}

impl Ord for AggregateSortValue {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match (self, other) {
            (Self::Null, Self::Null) => CmpOrdering::Equal,
            (Self::Null, _) => CmpOrdering::Greater,
            (_, Self::Null) => CmpOrdering::Less,
            (Self::Predicate(left), Self::Predicate(right)) => left.cmp(right),
            (Self::Aggregate(left), Self::Aggregate(right)) => {
                compare_aggregate_values(left, right)
            }
            (Self::Predicate(left), Self::Aggregate(right)) => {
                compare_predicate_to_aggregate(left, right)
            }
            (Self::Aggregate(left), Self::Predicate(right)) => {
                compare_predicate_to_aggregate(right, left).reverse()
            }
        }
    }
}

impl PartialOrd for AggregateSortValue {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

fn compare_aggregate_values(
    left: &NamespaceAggregateValue,
    right: &NamespaceAggregateValue,
) -> CmpOrdering {
    match (left, right) {
        (NamespaceAggregateValue::Null, NamespaceAggregateValue::Null) => CmpOrdering::Equal,
        (NamespaceAggregateValue::Null, _) => CmpOrdering::Greater,
        (_, NamespaceAggregateValue::Null) => CmpOrdering::Less,
        (NamespaceAggregateValue::U64(left), NamespaceAggregateValue::U64(right)) => {
            left.cmp(right)
        }
        (NamespaceAggregateValue::F64(left), NamespaceAggregateValue::F64(right)) => {
            left.total_cmp(right)
        }
        (NamespaceAggregateValue::U64(left), NamespaceAggregateValue::F64(right)) => {
            (*left as f64).total_cmp(right)
        }
        (NamespaceAggregateValue::F64(left), NamespaceAggregateValue::U64(right)) => {
            left.total_cmp(&(*right as f64))
        }
    }
}

fn compare_predicate_to_aggregate(
    left: &NamespacePredicateValue,
    right: &NamespaceAggregateValue,
) -> CmpOrdering {
    match (numeric_predicate_value(left), right) {
        (Some(left), NamespaceAggregateValue::F64(right)) => left.total_cmp(right),
        (Some(left), NamespaceAggregateValue::U64(right)) => left.total_cmp(&(*right as f64)),
        (_, NamespaceAggregateValue::Null) => CmpOrdering::Less,
        _ => CmpOrdering::Equal,
    }
}

fn apply_sort(cards: &mut [NamespaceCard], sort: &[NamespaceSort]) {
    for sort_key in sort.iter().rev() {
        cards.sort_by(|left, right| compare_cards_by_sort_key(left, right, sort_key));
    }
}

fn compare_cards_by_sort_key(
    left: &NamespaceCard,
    right: &NamespaceCard,
    sort_key: &NamespaceSort,
) -> std::cmp::Ordering {
    match sort_key.field.id.as_str() {
        "path" => apply_sort_direction(left.path.cmp(&right.path), &sort_key.direction),
        "name" => apply_sort_direction(left.name.cmp(&right.name), &sort_key.direction),
        "size_bytes" => {
            compare_optional_sort_values(&left.size_bytes, &right.size_bytes, &sort_key.direction)
        }
        field => compare_optional_sort_values(
            &indexed_sort_value(left, field),
            &indexed_sort_value(right, field),
            &sort_key.direction,
        ),
    }
}

fn compare_optional_sort_values<T: Ord>(
    left: &Option<T>,
    right: &Option<T>,
    direction: &NamespaceSortDirection,
) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => apply_sort_direction(left.cmp(right), direction),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn apply_sort_direction(
    ordering: std::cmp::Ordering,
    direction: &NamespaceSortDirection,
) -> std::cmp::Ordering {
    match direction {
        NamespaceSortDirection::Asc => ordering,
        NamespaceSortDirection::Desc => ordering.reverse(),
    }
}

fn string_operator(op: &NamespacePredicateOp) -> bool {
    matches!(
        op,
        NamespacePredicateOp::Prefix
            | NamespacePredicateOp::Suffix
            | NamespacePredicateOp::Contains
    )
}

fn numeric_operator(op: &NamespacePredicateOp) -> bool {
    matches!(
        op,
        NamespacePredicateOp::GreaterThan
            | NamespacePredicateOp::GreaterThanOrEqual
            | NamespacePredicateOp::LessThan
            | NamespacePredicateOp::LessThanOrEqual
    )
}

fn indexed_sort_value(card: &NamespaceCard, field: &str) -> Option<NamespacePredicateValue> {
    card.indexed_values
        .iter()
        .find(|value| value.field.as_str() == field)
        .map(|value| value.value.clone())
}

fn merge_index_catalog(catalog: &mut NamespaceQueryCatalog, indexes: &LoadedNamespaceIndex) {
    for field in &indexes.fields {
        if !catalog
            .filterable
            .iter()
            .any(|capability| capability.field == field.field)
        {
            catalog.filterable.push(NamespaceFilterCapability {
                field: field.field.clone(),
                operators: field.operators.clone(),
            });
        }
        if field.sortable {
            let sort_field = NamespaceSortField::new(field.field.id.clone());
            if !catalog.sortable.contains(&sort_field) {
                catalog.sortable.push(sort_field);
            }
        }
        if field.facetable && !catalog.facetable.contains(&field.field) {
            catalog.facetable.push(field.field.clone());
        }
    }
    catalog.facets = facet_summaries(indexes, DEFAULT_FACET_VALUE_LIMIT);
}

fn facet_summaries(indexes: &LoadedNamespaceIndex, limit: usize) -> Vec<NamespaceFacetSummary> {
    indexes
        .fields
        .iter()
        .filter(|field| field.facetable)
        .filter_map(|field| {
            let mut counts = BTreeMap::<NamespacePredicateValue, usize>::new();
            for values in indexes.rows.values() {
                for value in values {
                    if value.field == field.field {
                        *counts.entry(value.value.clone()).or_default() += 1;
                    }
                }
            }
            facet_summary_from_counts(field.field.clone(), counts, limit)
        })
        .collect()
}

fn filtered_facet_summaries(
    cards: &[NamespaceCard],
    fields: &[NamespaceFindField],
    limit: usize,
) -> Vec<NamespaceFacetSummary> {
    fields
        .iter()
        .filter_map(|field| {
            let mut counts = BTreeMap::<NamespacePredicateValue, usize>::new();
            for card in cards {
                for value in card_facet_values(card, field) {
                    *counts.entry(value).or_default() += 1;
                }
            }
            facet_summary_from_counts(field.clone(), counts, limit)
        })
        .collect()
}

fn field_needs_body_projection(field: &NamespaceFindField) -> bool {
    matches!(
        field.as_str(),
        "body.content_type" | "body.producer" | "body.manifest_id"
    )
}

fn facet_summary_from_counts(
    field: NamespaceFindField,
    counts: BTreeMap<NamespacePredicateValue, usize>,
    limit: usize,
) -> Option<NamespaceFacetSummary> {
    if counts.is_empty() {
        return None;
    }
    let distinct_count = counts.len();
    let mut values = counts
        .into_iter()
        .map(|(value, count)| NamespaceFacetValue { value, count })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.value.cmp(&right.value))
    });
    values.truncate(limit);
    Some(NamespaceFacetSummary {
        field,
        values,
        distinct_count,
        truncated: distinct_count > limit,
    })
}

fn card_facet_values(
    card: &NamespaceCard,
    field: &NamespaceFindField,
) -> Vec<NamespacePredicateValue> {
    match field.as_str() {
        "path" => vec![NamespacePredicateValue::String(card.path.clone())],
        "name" => vec![NamespacePredicateValue::String(card.name.clone())],
        "kind" => vec![NamespacePredicateValue::String(
            card_kind_label(&card.kind).to_owned(),
        )],
        "size_bytes" => card
            .size_bytes
            .map(NamespacePredicateValue::U64)
            .into_iter()
            .collect(),
        "body.content_type" => card
            .body
            .as_ref()
            .map(|body| NamespacePredicateValue::String(body.content_type.clone()))
            .into_iter()
            .collect(),
        "body.producer" => card
            .body
            .as_ref()
            .map(|body| NamespacePredicateValue::String(body.producer.clone()))
            .into_iter()
            .collect(),
        "body.manifest_id" => card
            .body
            .as_ref()
            .map(|body| NamespacePredicateValue::String(body.manifest_id.clone()))
            .into_iter()
            .collect(),
        _ => card
            .indexed_values
            .iter()
            .filter_map(|value| (value.field == *field).then_some(value.value.clone()))
            .collect(),
    }
}

fn card_kind_label(kind: &NamespaceCardKind) -> &'static str {
    match kind {
        NamespaceCardKind::Directory => "directory",
        NamespaceCardKind::File => "file",
        NamespaceCardKind::Symlink => "symlink",
        NamespaceCardKind::Special => "special",
    }
}

fn namespace_index_field_from_record(record: PathIndexFieldRecord) -> NamespaceIndexField {
    NamespaceIndexField {
        field: NamespaceFindField::new(record.field),
        operators: record
            .operators
            .into_iter()
            .filter_map(|op| namespace_predicate_op_from_name(&op))
            .collect(),
        sortable: record.sortable,
        facetable: record.facetable,
    }
}

fn namespace_predicate_op_from_name(name: &str) -> Option<NamespacePredicateOp> {
    Some(match name {
        "eq" => NamespacePredicateOp::Eq,
        "ne" | "not_equal" => NamespacePredicateOp::NotEqual,
        "in" => NamespacePredicateOp::In,
        "prefix" => NamespacePredicateOp::Prefix,
        "suffix" => NamespacePredicateOp::Suffix,
        "contains" => NamespacePredicateOp::Contains,
        "greater_than" => NamespacePredicateOp::GreaterThan,
        "greater_than_or_equal" => NamespacePredicateOp::GreaterThanOrEqual,
        "less_than" => NamespacePredicateOp::LessThan,
        "less_than_or_equal" => NamespacePredicateOp::LessThanOrEqual,
        "exists" => NamespacePredicateOp::Exists,
        "not_exists" => NamespacePredicateOp::NotExists,
        _ => return None,
    })
}

fn namespace_predicate_op_name(op: &NamespacePredicateOp) -> String {
    match op {
        NamespacePredicateOp::Eq => "eq",
        NamespacePredicateOp::NotEqual => "ne",
        NamespacePredicateOp::In => "in",
        NamespacePredicateOp::Prefix => "prefix",
        NamespacePredicateOp::Suffix => "suffix",
        NamespacePredicateOp::Contains => "contains",
        NamespacePredicateOp::GreaterThan => "greater_than",
        NamespacePredicateOp::GreaterThanOrEqual => "greater_than_or_equal",
        NamespacePredicateOp::LessThan => "less_than",
        NamespacePredicateOp::LessThanOrEqual => "less_than_or_equal",
        NamespacePredicateOp::Exists => "exists",
        NamespacePredicateOp::NotExists => "not_exists",
    }
    .to_owned()
}

fn path_index_catalog_record(
    path: &str,
    registration: &NamespaceIndexRegistration,
) -> PathIndexCatalogRecord {
    PathIndexCatalogRecord {
        path: path.to_owned(),
        row_count: registration.rows.len() as u64,
        fields: registration
            .fields
            .iter()
            .map(|field| PathIndexFieldRecord {
                field: field.field.id.clone(),
                operators: field
                    .operators
                    .iter()
                    .map(namespace_predicate_op_name)
                    .collect(),
                sortable: field.sortable,
                facetable: field.facetable,
            })
            .collect(),
    }
}

fn path_index_row_record(path: &str, values: Vec<NamespaceIndexValue>) -> PathIndexRowRecord {
    PathIndexRowRecord {
        path: path.to_owned(),
        values: values
            .into_iter()
            .map(|value| (value.field.id, path_index_value_record(value.value)))
            .collect(),
    }
}

fn path_index_value_record(value: NamespacePredicateValue) -> PathIndexValueRecord {
    match value {
        NamespacePredicateValue::String(value) => PathIndexValueRecord::String(value),
        NamespacePredicateValue::U64(value) => PathIndexValueRecord::U64(value),
        NamespacePredicateValue::F64(value) => PathIndexValueRecord::F64(value),
        NamespacePredicateValue::List(_) => {
            unreachable!("list predicate values cannot be registered as index values")
        }
    }
}

fn namespace_value_from_record(value: PathIndexValueRecord) -> NamespacePredicateValue {
    match value {
        PathIndexValueRecord::String(value) => NamespacePredicateValue::String(value),
        PathIndexValueRecord::U64(value) => NamespacePredicateValue::U64(value),
        PathIndexValueRecord::F64(value) => NamespacePredicateValue::F64(value),
    }
}

fn structured_records(
    path: &str,
    body: &BodyDescriptor,
    bytes: &[u8],
) -> Result<StructuredRecords, MetadError> {
    if is_json_path(path, body) {
        return json_records(bytes);
    }
    if is_yaml_path(path, body) {
        return yaml_records(bytes);
    }
    if is_text_path(path, body) {
        return text_records(bytes);
    }
    Err(MetadError::InvalidQuery(format!(
        "structured read does not support content type {} for {path}",
        body.content_type
    )))
}

fn json_records(bytes: &[u8]) -> Result<StructuredRecords, MetadError> {
    let value = serde_json::from_slice::<serde_json::Value>(bytes)
        .map_err(|err| MetadError::InvalidQuery(format!("structured JSON parse failed: {err}")))?;
    match value {
        serde_json::Value::Array(items) => {
            let fields = infer_json_array_fields(&items);
            let items = items
                .iter()
                .map(json_value_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(StructuredRecords {
                record_type: NamespaceRecordType::JsonArray,
                fields,
                items,
            })
        }
        serde_json::Value::Object(map) => {
            let mut entries = map.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let items = entries
                .into_iter()
                .map(|(key, value)| json_key_value_string(key, value))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(StructuredRecords {
                record_type: NamespaceRecordType::JsonObject,
                fields: vec!["key".to_owned(), "value".to_owned()],
                items,
            })
        }
        _ => Err(MetadError::InvalidQuery(
            "structured JSON read supports arrays and objects".to_owned(),
        )),
    }
}

fn yaml_records(bytes: &[u8]) -> Result<StructuredRecords, MetadError> {
    let value = serde_yaml::from_slice::<serde_yaml::Value>(bytes)
        .map_err(|err| MetadError::InvalidQuery(format!("structured YAML parse failed: {err}")))?;
    let serde_yaml::Value::Mapping(map) = value else {
        return Err(MetadError::InvalidQuery(
            "structured YAML read supports mappings".to_owned(),
        ));
    };
    let mut entries = Vec::new();
    for (key, value) in map {
        let Some(key) = key.as_str() else {
            continue;
        };
        entries.push((key.to_owned(), yaml_to_json(value)));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    let items = entries
        .into_iter()
        .map(|(key, value)| json_key_value_string(key, value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StructuredRecords {
        record_type: NamespaceRecordType::YamlMapping,
        fields: vec!["key".to_owned(), "value".to_owned()],
        items,
    })
}

fn text_records(bytes: &[u8]) -> Result<StructuredRecords, MetadError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|err| MetadError::InvalidQuery(format!("structured text parse failed: {err}")))?;
    let items = text
        .lines()
        .enumerate()
        .map(|(index, line)| {
            json_value_string(&serde_json::json!({
                "line": index + 1,
                "text": line,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StructuredRecords {
        record_type: NamespaceRecordType::TextLines,
        fields: vec!["line".to_owned(), "text".to_owned()],
        items,
    })
}

fn yaml_to_json(value: serde_yaml::Value) -> serde_json::Value {
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}

fn json_key_value_string(key: String, value: serde_json::Value) -> Result<String, MetadError> {
    json_value_string(&serde_json::json!({
        "key": key,
        "value": value,
    }))
}

fn bounded_limit(limit: usize) -> Result<usize, MetadError> {
    if limit == 0 {
        return Err(MetadError::InvalidQuery(
            "limit must be greater than zero".to_owned(),
        ));
    }
    Ok(limit.min(1000))
}

fn parse_cursor(cursor: Option<&str>) -> Result<usize, MetadError> {
    cursor
        .unwrap_or("0")
        .parse::<usize>()
        .map_err(|err| MetadError::InvalidQuery(format!("invalid cursor: {err}")))
}

fn parse_byte_cursor(cursor: Option<&str>, offset: u64) -> Result<u64, MetadError> {
    match cursor {
        Some(cursor) => cursor
            .parse::<u64>()
            .map_err(|err| MetadError::InvalidQuery(format!("invalid cursor: {err}"))),
        None => Ok(offset),
    }
}

fn parse_grep_cursor(cursor: Option<&str>) -> Result<GrepCursor, MetadError> {
    let Some(cursor) = cursor else {
        return Ok(GrepCursor {
            candidate_index: 0,
            line_index: 0,
        });
    };
    let Some((candidate, line)) = cursor.split_once(':') else {
        return Err(MetadError::InvalidQuery("invalid grep cursor".to_owned()));
    };
    Ok(GrepCursor {
        candidate_index: candidate
            .parse::<usize>()
            .map_err(|err| MetadError::InvalidQuery(format!("invalid grep cursor: {err}")))?,
        line_index: line
            .parse::<usize>()
            .map_err(|err| MetadError::InvalidQuery(format!("invalid grep cursor: {err}")))?,
    })
}

fn grep_cursor(candidate_index: usize, line_index: usize) -> String {
    format!("{candidate_index}:{line_index}")
}

fn normalize_card_path(path: &str) -> Result<String, MetadError> {
    let components = parse_absolute_path(path)?;
    let mut out = String::from("/");
    out.push_str(
        &components
            .iter()
            .map(|name| String::from_utf8_lossy(name.as_bytes()).to_string())
            .collect::<Vec<_>>()
            .join("/"),
    );
    if out.len() > 1 && out.ends_with('/') {
        out.pop();
    }
    Ok(out)
}

fn path_name_for_card(path: &str) -> String {
    if path == "/" {
        "/".to_owned()
    } else {
        path.rsplit('/').next().unwrap_or(path).to_owned()
    }
}

fn join_card_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn namespace_evidence(path: &str, generation: Option<u64>) -> String {
    match generation {
        Some(generation) => format!("nokv-native://{path}@generation:{generation}"),
        None => format!("nokv-native://{path}"),
    }
}

fn card_kind(file_type: FileType) -> NamespaceCardKind {
    match file_type {
        FileType::File => NamespaceCardKind::File,
        FileType::Directory => NamespaceCardKind::Directory,
        FileType::Symlink => NamespaceCardKind::Symlink,
        FileType::NamedPipe | FileType::CharDevice | FileType::BlockDevice | FileType::Socket => {
            NamespaceCardKind::Special
        }
    }
}

fn namespace_body_descriptor(body: &BodyDescriptor) -> NamespaceBodyDescriptor {
    NamespaceBodyDescriptor {
        producer: body.producer.clone(),
        digest_uri: body.digest_uri.clone(),
        size: body.size,
        content_type: body.content_type.clone(),
        manifest_id: body.manifest_id.clone(),
        generation: body.generation,
        chunk_size: body.chunk_size,
        block_size: body.block_size,
    }
}

fn is_structured_path(path: &str, body: &BodyDescriptor) -> bool {
    is_json_path(path, body) || is_yaml_path(path, body) || is_text_path(path, body)
}

fn is_json_path(path: &str, body: &BodyDescriptor) -> bool {
    body.content_type == "application/json" || path.ends_with(".json")
}

fn is_yaml_path(path: &str, body: &BodyDescriptor) -> bool {
    matches!(
        body.content_type.as_str(),
        "application/yaml" | "application/x-yaml" | "text/yaml"
    ) || path.ends_with(".yaml")
        || path.ends_with(".yml")
}

fn is_text_path(path: &str, body: &BodyDescriptor) -> bool {
    body.content_type.starts_with("text/") || path.ends_with(".txt") || path.ends_with(".log")
}

fn infer_json_array_fields(items: &[serde_json::Value]) -> Vec<String> {
    let mut fields = Vec::new();
    for item in items {
        if let Some(object) = item.as_object() {
            for key in object.keys() {
                if !fields.contains(key) {
                    fields.push(key.clone());
                }
            }
        }
    }
    fields.sort();
    fields
}

fn json_value_string(value: &serde_json::Value) -> Result<String, MetadError> {
    serde_json::to_string(value)
        .map_err(|err| MetadError::InvalidQuery(format!("structured JSON encode failed: {err}")))
}

fn file_len(size: u64) -> Result<usize, MetadError> {
    usize::try_from(size).map_err(|_| {
        MetadError::InvalidQuery(format!("file size {size} does not fit this platform"))
    })
}

fn file_type_label(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "directory",
        FileType::Symlink => "symlink",
        FileType::NamedPipe | FileType::CharDevice | FileType::BlockDevice | FileType::Socket => {
            "special"
        }
    }
}
