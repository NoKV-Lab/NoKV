/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::Path;
use std::sync::{Arc, Mutex};

use nokv_meta_holt::{HoltOptions, HoltStore, TreeBinding};
use nokv_meta_store::{
    Commit, LimitKind, ReadBatch, ReadSnapshot, StoreError, StoreProfile, TxnStore, WriteTxn,
};
use nokv_types::LogicalShardId;

use super::{keyspaces, store_limits, MetaError, MetaShard};

pub(crate) struct CommitCaptureStore {
    inner: Arc<dyn TxnStore>,
    last_commit: Mutex<Option<WriteTxn>>,
}

impl CommitCaptureStore {
    pub(crate) fn with_last_commit<T>(&self, inspect: impl FnOnce(&WriteTxn) -> T) -> T {
        let last_commit = self
            .last_commit
            .lock()
            .expect("capture commit lock must be available");
        let transaction = last_commit
            .as_ref()
            .expect("captured store must contain one commit");
        inspect(transaction)
    }
}

impl TxnStore for CommitCaptureStore {
    fn profile(&self) -> StoreProfile {
        self.inner.profile()
    }

    fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
        self.inner.read(batch)
    }

    fn commit(&self, transaction: WriteTxn) -> Result<Commit, StoreError> {
        *self
            .last_commit
            .lock()
            .expect("capture commit lock must be available") = Some(transaction.clone());
        self.inner.commit(transaction)
    }

    fn ready(&self) -> Result<(), StoreError> {
        self.inner.ready()
    }
}

pub(crate) fn capture_txn_store(
    inner: Arc<dyn TxnStore>,
) -> (Arc<dyn TxnStore>, Arc<CommitCaptureStore>) {
    let capture = Arc::new(CommitCaptureStore {
        inner,
        last_commit: Mutex::new(None),
    });
    (capture.clone(), capture)
}

pub(crate) fn transaction_bytes(transaction: &WriteTxn) -> usize {
    // Reuse the contract validator as the single byte-accounting source of
    // truth. With every other production limit unchanged, a zero byte ceiling
    // returns the exact fully derived transaction size.
    let mut limits = store_limits();
    limits.max_transaction_bytes = 0;
    match transaction.validate(&limits) {
        Err(StoreError::LimitExceeded {
            kind: LimitKind::TransactionBytes,
            actual,
            maximum: 0,
        }) => actual,
        result => panic!("zero transaction budget must report exact bytes, got {result:?}"),
    }
}

pub(crate) fn memory(logical_shard_id: LogicalShardId) -> Result<MetaShard, MetaError> {
    MetaShard::initialize(memory_txn_store()?, logical_shard_id)
}

pub(crate) fn memory_txn_store() -> Result<Arc<dyn TxnStore>, MetaError> {
    HoltStore::memory(catalog(), store_limits())
        .map(|store| Arc::new(store) as Arc<dyn TxnStore>)
        .map_err(|source| store_error("create test metadata store", source))
}

pub(crate) fn initialize_file(
    path: &Path,
    logical_shard_id: LogicalShardId,
) -> Result<MetaShard, MetaError> {
    MetaShard::initialize(initialize_file_txn_store(path)?, logical_shard_id)
}

pub(crate) fn initialize_file_with_holt_store(
    path: &Path,
    logical_shard_id: LogicalShardId,
) -> Result<(MetaShard, Arc<HoltStore>), MetaError> {
    let physical = Arc::new(
        HoltStore::initialize(HoltOptions::file(path, catalog(), store_limits()))
            .map_err(|source| store_error("initialize test metadata store", source))?,
    );
    let store: Arc<dyn TxnStore> = physical.clone();
    let shard = MetaShard::initialize(store, logical_shard_id)?;
    Ok((shard, physical))
}

pub(crate) fn open_file(
    path: &Path,
    logical_shard_id: LogicalShardId,
) -> Result<MetaShard, MetaError> {
    MetaShard::open(open_file_txn_store(path)?, logical_shard_id)
}

pub(crate) fn initialize_file_txn_store(path: &Path) -> Result<Arc<dyn TxnStore>, MetaError> {
    HoltStore::initialize(HoltOptions::file(path, catalog(), store_limits()))
        .map(|store| Arc::new(store) as Arc<dyn TxnStore>)
        .map_err(|source| store_error("initialize test metadata store", source))
}

pub(crate) fn open_file_txn_store(path: &Path) -> Result<Arc<dyn TxnStore>, MetaError> {
    HoltStore::open(HoltOptions::file(path, catalog(), store_limits()))
        .map(|store| Arc::new(store) as Arc<dyn TxnStore>)
        .map_err(|source| store_error("open test metadata store", source))
}

fn catalog() -> Vec<TreeBinding> {
    keyspaces()
        .iter()
        .map(|definition| TreeBinding::new(definition.id, definition.name))
        .collect()
}

fn store_error(operation: &'static str, source: nokv_meta_store::StoreError) -> MetaError {
    MetaError::Store { operation, source }
}
