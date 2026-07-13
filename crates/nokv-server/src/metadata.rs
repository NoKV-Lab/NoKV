use nokv_meta::command::{
    CommitResult, DelimitedScanItem, DelimitedScanRequest, HistoryPruneOutcome,
    HistoryPruneRequest, KeyScanRequest, MetadataCheckpointStore, MetadataCommand, MetadataError,
    MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider, ReadItem, ReadPurpose, ScanItem,
    ScanRequest,
};
use nokv_meta::holtstore::HoltMetadataStore;
use nokv_types::RecordFamily;

pub(crate) enum ServerMetadataStore {
    Direct(Box<HoltMetadataStore>),
}

impl ServerMetadataStore {
    pub(crate) fn direct(store: HoltMetadataStore) -> Self {
        Self::Direct(Box::new(store))
    }
}

impl MetadataCheckpointStore for ServerMetadataStore {
    fn checkpoint(&self) -> Result<(), MetadataError> {
        match self {
            Self::Direct(store) => store.checkpoint(),
        }
    }

    fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        match self {
            Self::Direct(store) => store.export_checkpoint_image(),
        }
    }

    fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        match self {
            Self::Direct(store) => store.install_checkpoint_image(image),
        }
    }

    fn reclaim_unreachable_storage(&self) -> Result<usize, MetadataError> {
        match self {
            Self::Direct(store) => store.reclaim_unreachable_storage(),
        }
    }
}

impl MetadataStore for ServerMetadataStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: nokv_meta::Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        match self {
            Self::Direct(store) => store.get_versioned(family, key, version, purpose),
        }
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        match self {
            Self::Direct(store) => store.scan(request),
        }
    }

    fn scan_delimited(
        &self,
        request: DelimitedScanRequest,
    ) -> Result<Vec<DelimitedScanItem>, MetadataError> {
        match self {
            Self::Direct(store) => store.scan_delimited(request),
        }
    }

    fn scan_keys(&self, request: KeyScanRequest) -> Result<Vec<Vec<u8>>, MetadataError> {
        match self {
            Self::Direct(store) => store.scan_keys(request),
        }
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        match self {
            Self::Direct(store) => store.commit_metadata(command),
        }
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        match self {
            Self::Direct(store) => store.commit_independent_batch(commands),
        }
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        match self {
            Self::Direct(store) => store.committed_request_result(request_id),
        }
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        match self {
            Self::Direct(store) => store.history_retention_epoch(),
        }
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        match self {
            Self::Direct(store) => store.prune_history(request),
        }
    }
}

impl MetadataStoreStatsProvider for ServerMetadataStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        match self {
            Self::Direct(store) => store.metadata_store_stats(),
        }
    }
}
