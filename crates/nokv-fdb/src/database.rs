/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::sync::Arc;

use foundationdb::options::{StreamingMode, TransactionOption};
use foundationdb::{Database, RangeOption, Transaction};
use futures::executor::block_on;

use crate::{FdbConfigError, FdbConnectionOptions, FdbOperationError, FdbRuntime};

/// Database handle that keeps the process-global network alive.
#[derive(Clone)]
pub struct FdbDatabase {
    database: Arc<Database>,
    runtime: FdbRuntime,
    transaction_timeout_millis: i32,
}

impl FdbDatabase {
    pub fn open(
        runtime: &FdbRuntime,
        options: &FdbConnectionOptions,
    ) -> Result<Self, FdbOpenError> {
        options.validate().map_err(FdbOpenError::Config)?;
        let database = Database::from_path(options.cluster_file_str()).map_err(|error| {
            FdbOpenError::Operation(FdbOperationError::from_binding(
                "open FoundationDB database",
                error,
            ))
        })?;
        Ok(Self {
            database: Arc::new(database),
            runtime: runtime.clone(),
            transaction_timeout_millis: options.transaction_timeout_millis(),
        })
    }

    pub fn transaction(&self) -> Result<FdbTransaction, FdbOperationError> {
        let transaction = self.database.create_trx().map_err(|error| {
            FdbOperationError::from_binding("create FoundationDB transaction", error)
        })?;
        transaction
            .set_option(TransactionOption::Timeout(self.transaction_timeout_millis))
            .map_err(|error| {
                FdbOperationError::from_binding("set FoundationDB transaction timeout", error)
            })?;
        Ok(FdbTransaction {
            transaction,
            runtime: self.runtime.clone(),
        })
    }
}

/// One explicit, non-retrying FoundationDB transaction.
pub struct FdbTransaction {
    transaction: Transaction,
    runtime: FdbRuntime,
}

impl FdbTransaction {
    pub fn get(&self, key: &[u8], snapshot: bool) -> Result<Option<Vec<u8>>, FdbOperationError> {
        block_on(self.transaction.get(key, snapshot))
            .map(|value| value.map(|value| value.as_ref().to_vec()))
            .map_err(|error| FdbOperationError::from_binding("read FoundationDB key", error))
    }

    pub fn get_range(&self, request: &FdbRangeRequest) -> Result<FdbRangePage, FdbOperationError> {
        let mut options = RangeOption::from((request.begin.as_slice(), request.end.as_slice()));
        options.limit = request.limit;
        options.target_bytes = request.target_bytes;
        options.mode = StreamingMode::WantAll;
        options.reverse = request.reverse;
        let values = block_on(self.transaction.get_range(
            &options,
            request.iteration,
            request.snapshot,
        ))
        .map_err(|error| FdbOperationError::from_binding("scan FoundationDB range", error))?;
        Ok(FdbRangePage {
            items: values
                .iter()
                .map(|value| FdbKeyValue {
                    key: value.key().to_vec(),
                    value: value.value().to_vec(),
                })
                .collect(),
            more: values.more(),
        })
    }

    pub fn set(&self, key: &[u8], value: &[u8]) {
        self.transaction.set(key, value);
    }

    pub fn clear(&self, key: &[u8]) {
        self.transaction.clear(key);
    }

    pub fn clear_range(&self, begin: &[u8], end: &[u8]) {
        self.transaction.clear_range(begin, end);
    }

    pub fn approximate_size(&self) -> Result<i64, FdbOperationError> {
        block_on(self.transaction.get_approximate_size()).map_err(|error| {
            FdbOperationError::from_binding("measure FoundationDB transaction size", error)
        })
    }

    pub fn read_version(&self) -> Result<i64, FdbOperationError> {
        block_on(self.transaction.get_read_version()).map_err(|error| {
            FdbOperationError::from_binding("obtain FoundationDB read version", error)
        })
    }

    pub fn commit(self) -> Result<(), FdbOperationError> {
        let Self {
            transaction,
            runtime,
        } = self;
        let result = block_on(transaction.commit()).map(|_| ()).map_err(|error| {
            let error: foundationdb::FdbError = error.into();
            FdbOperationError::from_binding("commit FoundationDB transaction", error)
        });
        drop(runtime);
        result
    }
}

/// One low-level range request. No automatic pagination or retry occurs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbRangeRequest {
    pub begin: Vec<u8>,
    pub end: Vec<u8>,
    pub limit: Option<usize>,
    pub target_bytes: usize,
    pub iteration: usize,
    pub snapshot: bool,
    pub reverse: bool,
}

/// Owned FoundationDB key/value pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbKeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// One physical range page and FoundationDB's continuation bit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbRangePage {
    pub items: Vec<FdbKeyValue>,
    pub more: bool,
}

/// Database open failure with configuration kept distinct from availability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FdbOpenError {
    Config(FdbConfigError),
    Operation(FdbOperationError),
}

impl fmt::Display for FdbOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => error.fmt(formatter),
            Self::Operation(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for FdbOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Operation(error) => Some(error),
        }
    }
}
