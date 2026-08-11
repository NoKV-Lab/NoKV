/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(test)]
use std::sync::Mutex;

use holt::{DBView, RangeEntry, RecordVersion, DB};
use nokv_meta_store::{
    AckBoundary, Authority, Check, Commit, Keyspace, Mutation, ReadBatch, ReadOp, ReadResult,
    ReadSnapshot, Scan, ScanItem, ScanPage, StoreError, StoreLimits, StoreProfile, TxnStore,
    UnknownCommit, WriteTxn,
};

use crate::HoltOptions;
#[cfg(any(test, feature = "test-support"))]
use crate::TreeBinding;

/// Embedded Holt transaction store for one local metadata authority.
pub struct HoltStore {
    profile: StoreProfile,
    // Writers and poison transitions are exclusive. Healthy reads share the state.
    state: RwLock<State>,
    #[cfg(feature = "read-stats")]
    pub(crate) read_stats: crate::stats::ReadStatsState,
    #[cfg(test)]
    test_hooks: TestHooks,
}

struct State {
    db: DB,
    trees: BTreeMap<Keyspace, String>,
    poisoned: Option<String>,
}

struct ValueVersion {
    keyspace: Keyspace,
    key: Vec<u8>,
    version: RecordVersion,
}

#[cfg(test)]
struct TestHooks {
    fail_before_atomic: std::sync::atomic::AtomicBool,
    fail_after_atomic: std::sync::atomic::AtomicBool,
    pause_before_poison: Mutex<Option<PoisonPause>>,
    pause_read_after_lock: Mutex<Option<PoisonPause>>,
    read_entered: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
    commit_entered: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
}

#[cfg(test)]
struct PoisonPause {
    entered: std::sync::mpsc::SyncSender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

impl HoltStore {
    /// Initialize a file-backed namespace whose Holt tree registry is empty.
    ///
    /// Discard the namespace if tree creation fails. Initialization does not
    /// resume a partial physical catalog.
    pub fn initialize(options: HoltOptions) -> Result<Self, StoreError> {
        Self::open_inner(options, OpenAction::Initialize, false)
    }

    /// Open a file-backed namespace with the exact configured tree catalog.
    pub fn open(options: HoltOptions) -> Result<Self, StoreError> {
        Self::open_inner(options, OpenAction::Existing, false)
    }

    fn open_inner(
        options: HoltOptions,
        action: OpenAction,
        allow_memory: bool,
    ) -> Result<Self, StoreError> {
        let trees = options.validate(allow_memory)?;
        preflight_location(&options, action)?;
        let db = DB::open(options.config).map_err(map_holt_error)?;
        prepare_trees(&db, &trees, action)?;
        Ok(Self {
            profile: StoreProfile {
                limits: options.limits,
                ack: AckBoundary::LocalSync,
                authority: Authority::Local,
            },
            state: RwLock::new(State {
                db,
                trees,
                poisoned: None,
            }),
            #[cfg(feature = "read-stats")]
            read_stats: crate::stats::ReadStatsState::default(),
            #[cfg(test)]
            test_hooks: TestHooks {
                fail_before_atomic: std::sync::atomic::AtomicBool::new(false),
                fail_after_atomic: std::sync::atomic::AtomicBool::new(false),
                pause_before_poison: Mutex::new(None),
                pause_read_after_lock: Mutex::new(None),
                read_entered: Mutex::new(None),
                commit_entered: Mutex::new(None),
            },
        })
    }

    /// Open an in-memory store for tests.
    #[cfg(any(test, feature = "test-support"))]
    pub fn memory(
        catalog: impl IntoIterator<Item = TreeBinding>,
        limits: StoreLimits,
    ) -> Result<Self, StoreError> {
        Self::open_inner(
            HoltOptions::memory(catalog, limits),
            OpenAction::Initialize,
            true,
        )
    }

    fn read_state(&self) -> Result<RwLockReadGuard<'_, State>, StoreError> {
        self.state
            .read()
            .map_err(|_| StoreError::Unavailable("HoltStore state lock is poisoned".to_owned()))
    }

    fn write_state(&self) -> Result<RwLockWriteGuard<'_, State>, StoreError> {
        self.state
            .write()
            .map_err(|_| StoreError::Unavailable("HoltStore state lock is poisoned".to_owned()))
    }

    #[cfg(feature = "read-stats")]
    pub(crate) fn read_stats_store_key(&self) -> usize {
        std::ptr::from_ref(&self.read_stats) as usize
    }

    fn read_stats_key(&self) -> Option<usize> {
        #[cfg(feature = "read-stats")]
        {
            Some(self.read_stats_store_key())
        }
        #[cfg(not(feature = "read-stats"))]
        {
            None
        }
    }

    #[cfg(feature = "read-stats")]
    pub(crate) fn read_stats_snapshot(&self) -> Result<crate::stats::HoltReadStats, StoreError> {
        let state = self.read_state()?;
        ensure_ready(&state)?;
        Ok(crate::stats::storage_snapshot(&state.db.stats()))
    }

    #[cfg(test)]
    pub(crate) fn fail_next_atomic_not_applied(&self) {
        self.test_hooks
            .fail_before_atomic
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_atomic_after_apply(&self) {
        self.test_hooks
            .fail_after_atomic
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn pause_next_atomic_before_poison(
        &self,
        entered: std::sync::mpsc::SyncSender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    ) {
        self.fail_next_atomic_after_apply();
        *self
            .test_hooks
            .pause_before_poison
            .lock()
            .expect("lock test poison pause") = Some(PoisonPause { entered, resume });
    }

    #[cfg(test)]
    pub(crate) fn signal_next_read_entry(&self, entered: std::sync::mpsc::SyncSender<()>) {
        *self
            .test_hooks
            .read_entered
            .lock()
            .expect("lock test read hook") = Some(entered);
    }

    #[cfg(test)]
    pub(crate) fn pause_next_read_after_lock(
        &self,
        entered: std::sync::mpsc::SyncSender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    ) {
        *self
            .test_hooks
            .pause_read_after_lock
            .lock()
            .expect("lock test read pause") = Some(PoisonPause { entered, resume });
    }

    #[cfg(test)]
    pub(crate) fn signal_next_commit_entry(&self, entered: std::sync::mpsc::SyncSender<()>) {
        *self
            .test_hooks
            .commit_entered
            .lock()
            .expect("lock test commit hook") = Some(entered);
    }

    #[cfg(test)]
    fn signal_read_entry(&self) {
        if let Some(entered) = self
            .test_hooks
            .read_entered
            .lock()
            .expect("lock test read hook")
            .take()
        {
            let _ = entered.send(());
        }
    }

    #[cfg(test)]
    fn signal_commit_entry(&self) {
        if let Some(entered) = self
            .test_hooks
            .commit_entered
            .lock()
            .expect("lock test commit hook")
            .take()
        {
            let _ = entered.send(());
        }
    }

    #[cfg(test)]
    fn pause_read_after_lock(&self) {
        let pause = self
            .test_hooks
            .pause_read_after_lock
            .lock()
            .expect("lock test read pause")
            .take();
        if let Some(pause) = pause {
            let _ = pause.entered.send(());
            let _ = pause.resume.recv();
        }
    }

    #[cfg(test)]
    fn pause_before_poison(&self) {
        let pause = self
            .test_hooks
            .pause_before_poison
            .lock()
            .expect("lock test poison pause")
            .take();
        if let Some(pause) = pause {
            let _ = pause.entered.send(());
            let _ = pause.resume.recv();
        }
    }
}

#[derive(Clone, Copy)]
enum OpenAction {
    Initialize,
    Existing,
}

impl TxnStore for HoltStore {
    fn profile(&self) -> StoreProfile {
        self.profile
    }

    fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
        batch.validate(&self.profile.limits)?;
        #[cfg(test)]
        self.signal_read_entry();
        let state = self.read_state()?;
        ensure_ready(&state)?;
        #[cfg(test)]
        self.pause_read_after_lock();
        validate_keyspaces(&state, batch_keyspaces(&batch))?;

        let owned_scopes = read_scopes(&state, &batch)?;
        let scopes = owned_scopes
            .iter()
            .map(|(name, prefix)| (name.as_str(), prefix.as_slice()))
            .collect::<Vec<_>>();
        let read_stats_key = self.read_stats_key();
        let snapshot = state
            .db
            .view(&scopes, |view| {
                Ok::<_, holt::Error>(build_snapshot(
                    view,
                    &state.trees,
                    &batch,
                    &self.profile.limits,
                    read_stats_key,
                ))
            })
            .map_err(map_holt_error)??;
        snapshot.validate(&batch, &self.profile.limits)?;
        Ok(snapshot)
    }

    fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
        txn.validate(&self.profile.limits)?;
        #[cfg(test)]
        self.signal_commit_entry();
        let mut state = self.write_state()?;
        ensure_ready(&state)?;
        validate_keyspaces(&state, txn_keyspaces(&txn))?;

        let Some(value_versions) = read_value_versions(&state, &txn)? else {
            return Ok(Commit::Conflict);
        };

        #[cfg(test)]
        if self
            .test_hooks
            .fail_before_atomic
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return map_atomic_result(
                &mut state,
                Err(holt::Error::Atomic {
                    kind: holt::AtomicErrorKind::DefinitelyNotApplied,
                    source: Box::new(holt::Error::BlobStoreIo(std::io::Error::other(
                        "injected error before Holt atomic apply",
                    ))),
                }),
            );
        }

        let applied = state.db.atomic(|batch| {
            for guard in &value_versions {
                batch.assert_version(tree_name(&state, guard.keyspace), &guard.key, guard.version);
            }
            for check in &txn.checks {
                match check {
                    Check::Value { .. } => {}
                    Check::Absent { key } => {
                        batch.assert_absent(tree_name(&state, key.keyspace), &key.bytes);
                    }
                    Check::EmptyPrefix { keyspace, prefix } => {
                        batch.assert_prefix_empty(tree_name(&state, *keyspace), prefix);
                    }
                }
            }
            for mutation in &txn.mutations {
                match mutation {
                    Mutation::Put { key, value } => {
                        batch.put(tree_name(&state, key.keyspace), &key.bytes, value);
                    }
                    Mutation::Delete { key } => {
                        batch.delete(tree_name(&state, key.keyspace), &key.bytes);
                    }
                }
            }
        });

        #[cfg(test)]
        let injected = self
            .test_hooks
            .fail_after_atomic
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        #[cfg(not(test))]
        let injected = false;

        if injected && matches!(applied, Ok(true)) {
            #[cfg(test)]
            self.pause_before_poison();
            return map_atomic_result(
                &mut state,
                Err(holt::Error::Atomic {
                    kind: holt::AtomicErrorKind::OutcomeUnknown,
                    source: Box::new(holt::Error::Internal(
                        "injected error after Holt atomic apply",
                    )),
                }),
            );
        }
        map_atomic_result(&mut state, applied)
    }

    fn ready(&self) -> Result<(), StoreError> {
        let state = self.read_state()?;
        ensure_ready(&state)?;
        let actual = state.db.list_trees().map_err(map_holt_error)?;
        let expected = state.trees.values().cloned().collect::<BTreeSet<_>>();
        if actual.into_iter().collect::<BTreeSet<_>>() == expected {
            Ok(())
        } else {
            Err(StoreError::Corrupt(
                "Holt keyspace tree catalog changed after open".to_owned(),
            ))
        }
    }
}

fn prepare_trees(
    db: &DB,
    trees: &BTreeMap<Keyspace, String>,
    action: OpenAction,
) -> Result<(), StoreError> {
    let expected = trees.values().cloned().collect::<BTreeSet<_>>();
    let actual = db
        .list_trees()
        .map_err(map_holt_error)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    match action {
        OpenAction::Initialize if !actual.is_empty() => {
            return Err(StoreError::Corrupt(
                "Holt initialization requires an empty physical tree registry".to_owned(),
            ));
        }
        OpenAction::Initialize => {
            for name in &expected {
                db.create_tree(name).map_err(map_holt_error)?;
            }
        }
        OpenAction::Existing if actual != expected => {
            return Err(StoreError::Corrupt(
                "Holt tree registry does not match the configured physical catalog".to_owned(),
            ));
        }
        OpenAction::Existing => {}
    }
    Ok(())
}

fn preflight_location(options: &HoltOptions, action: OpenAction) -> Result<(), StoreError> {
    let Some(path) = options.file_path()? else {
        return Ok(());
    };
    match action {
        OpenAction::Initialize => require_empty_location(path),
        OpenAction::Existing => require_holt_location(path),
    }
}

fn require_empty_location(path: &std::path::Path) -> Result<(), StoreError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            StoreError::InvalidRequest(format!("{} is not a HoltStore directory", path.display())),
        ),
        Ok(_) => {
            let mut entries = std::fs::read_dir(path).map_err(|error| {
                StoreError::Unavailable(format!(
                    "inspect HoltStore initialization path {}: {error}",
                    path.display()
                ))
            })?;
            match entries.next().transpose().map_err(|error| {
                StoreError::Unavailable(format!(
                    "inspect HoltStore initialization path {}: {error}",
                    path.display()
                ))
            })? {
                Some(_) => Err(StoreError::InvalidRequest(format!(
                    "HoltStore initialization path {} is not empty",
                    path.display()
                ))),
                None => Ok(()),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StoreError::Unavailable(format!(
            "inspect HoltStore initialization path {}: {error}",
            path.display()
        ))),
    }
}

fn require_holt_location(path: &std::path::Path) -> Result<(), StoreError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(StoreError::InvalidRequest(format!(
                "HoltStore directory {} does not exist",
                path.display()
            )));
        }
        Err(error) => {
            return Err(StoreError::Unavailable(format!(
                "inspect HoltStore directory {}: {error}",
                path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StoreError::InvalidRequest(format!(
            "{} is not a HoltStore directory",
            path.display()
        )));
    }

    for name in ["blobs.dat", "journal.wal"] {
        require_regular_holt_file(path, name)?;
    }
    let mut manifest = false;
    for name in ["manifest.bin", "manifest.log"] {
        let file = path.join(name);
        match std::fs::symlink_metadata(&file) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                manifest = true;
            }
            Ok(_) => {
                return Err(StoreError::Corrupt(format!(
                    "{} is not a regular Holt file",
                    file.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StoreError::Unavailable(format!(
                    "cannot inspect {}: {error}",
                    file.display()
                )));
            }
        }
    }
    if !manifest {
        return Err(StoreError::Corrupt(format!(
            "HoltStore directory {} has no durable manifest",
            path.display()
        )));
    }
    Ok(())
}

fn require_regular_holt_file(path: &std::path::Path, name: &str) -> Result<(), StoreError> {
    let file = path.join(name);
    let metadata = match std::fs::symlink_metadata(&file) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(StoreError::Corrupt(format!(
                "HoltStore directory is missing {}",
                file.display()
            )));
        }
        Err(error) => {
            return Err(StoreError::Unavailable(format!(
                "cannot inspect {}: {error}",
                file.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(StoreError::Corrupt(format!(
            "{} is not a regular Holt file",
            file.display()
        )));
    }
    Ok(())
}

fn tree_name(state: &State, keyspace: Keyspace) -> &str {
    state
        .trees
        .get(&keyspace)
        .expect("keyspace was validated before physical dispatch")
}

fn ensure_ready(state: &State) -> Result<(), StoreError> {
    match &state.poisoned {
        Some(reason) => Err(StoreError::Unavailable(format!(
            "HoltStore is poisoned and must be reopened: {reason}"
        ))),
        None => Ok(()),
    }
}

fn validate_keyspaces(
    state: &State,
    keyspaces: impl IntoIterator<Item = Keyspace>,
) -> Result<(), StoreError> {
    for keyspace in keyspaces {
        if !state.trees.contains_key(&keyspace) {
            return Err(StoreError::InvalidRequest(format!(
                "keyspace {:04x} is not configured for this HoltStore",
                keyspace.get()
            )));
        }
    }
    Ok(())
}

fn batch_keyspaces(batch: &ReadBatch) -> impl Iterator<Item = Keyspace> + '_ {
    batch.ops.iter().map(|op| match op {
        ReadOp::Get(key) => key.keyspace,
        ReadOp::Scan(scan) => scan.keyspace,
    })
}

fn txn_keyspaces(txn: &WriteTxn) -> impl Iterator<Item = Keyspace> + '_ {
    txn.checks
        .iter()
        .map(|check| match check {
            Check::Value { key, .. } | Check::Absent { key } => key.keyspace,
            Check::EmptyPrefix { keyspace, .. } => *keyspace,
        })
        .chain(txn.mutations.iter().map(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key.keyspace,
        }))
}

fn read_scopes(state: &State, batch: &ReadBatch) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
    let mut prefixes = BTreeMap::<Keyspace, Vec<u8>>::new();
    for op in &batch.ops {
        let (keyspace, prefix) = match op {
            ReadOp::Get(key) => (key.keyspace, key.bytes.as_slice()),
            ReadOp::Scan(scan) => (scan.keyspace, scan.prefix.as_slice()),
        };
        prefixes
            .entry(keyspace)
            .and_modify(|current| truncate_to_common_prefix(current, prefix))
            .or_insert_with(|| prefix.to_vec());
    }
    prefixes
        .into_iter()
        .map(|(keyspace, prefix)| {
            state
                .trees
                .get(&keyspace)
                .cloned()
                .map(|name| (name, prefix))
                .ok_or_else(|| {
                    StoreError::InvalidRequest(format!(
                        "keyspace {:04x} is not configured for this HoltStore",
                        keyspace.get()
                    ))
                })
        })
        .collect()
}

fn truncate_to_common_prefix(current: &mut Vec<u8>, other: &[u8]) {
    let length = current
        .iter()
        .zip(other)
        .take_while(|(left, right)| left == right)
        .count();
    current.truncate(length);
}

fn build_snapshot(
    view: &DBView,
    trees: &BTreeMap<Keyspace, String>,
    batch: &ReadBatch,
    limits: &StoreLimits,
    read_stats_key: Option<usize>,
) -> Result<ReadSnapshot, StoreError> {
    let mut results = Vec::with_capacity(batch.ops.len());
    for op in &batch.ops {
        match op {
            ReadOp::Get(key) => {
                let tree = captured_tree(view, trees, key.keyspace)?;
                let value = tree.get(&key.bytes).map_err(map_holt_error)?;
                results.push(ReadResult::Get(value));
            }
            ReadOp::Scan(scan) => {
                let tree = captured_tree(view, trees, scan.keyspace)?;
                results.push(ReadResult::Scan(scan_page(
                    tree,
                    scan,
                    limits,
                    read_stats_key,
                )?));
            }
        }
    }
    Ok(ReadSnapshot { results })
}

fn captured_tree<'a>(
    view: &'a DBView,
    trees: &BTreeMap<Keyspace, String>,
    keyspace: Keyspace,
) -> Result<&'a holt::View, StoreError> {
    let name = trees.get(&keyspace).ok_or_else(|| {
        StoreError::InvalidRequest(format!(
            "keyspace {:04x} is not configured for this HoltStore",
            keyspace.get()
        ))
    })?;
    view.tree(name).ok_or_else(|| {
        StoreError::Corrupt(format!(
            "Holt read view omitted configured keyspace {:04x}",
            keyspace.get()
        ))
    })
}

fn scan_page(
    view: &holt::View,
    scan: &Scan,
    limits: &StoreLimits,
    read_stats_key: Option<usize>,
) -> Result<ScanPage, StoreError> {
    let mut range = view.scan(&scan.prefix).map_err(map_holt_error)?;
    if let Some(after) = &scan.after {
        range = range.start_after(after);
    }
    if let Some(delimiter) = scan.delimiter {
        range = range.delimiter(delimiter);
    }

    let mut items = Vec::with_capacity(scan.limit);
    let mut bytes = 0_usize;
    let mut more = false;
    let mut iterator = range.into_iter();
    for entry in iterator.by_ref() {
        let item = match entry.map_err(map_holt_error)? {
            RangeEntry::Key { key, value, .. } => ScanItem::Row { key, value },
            RangeEntry::CommonPrefix(prefix) => ScanItem::CommonPrefix(prefix),
            _ => {
                return Err(StoreError::Corrupt(
                    "Holt returned an unsupported range entry".to_owned(),
                ));
            }
        };
        if scan
            .after
            .as_deref()
            .is_some_and(|after| item.key() <= after)
        {
            continue;
        }
        validate_physical_item(&item, limits)?;
        let item_bytes = scan_item_bytes(&item)?;
        let next_bytes = bytes.checked_add(item_bytes).ok_or_else(|| {
            StoreError::Corrupt("Holt scan result byte count overflows usize".to_owned())
        })?;
        if items.len() == scan.limit || next_bytes > scan.max_bytes {
            more = true;
            break;
        }
        bytes = next_bytes;
        items.push(item);
    }
    if more && items.is_empty() {
        return Err(StoreError::Corrupt(
            "Holt row exceeds the advertised maximum key or value size".to_owned(),
        ));
    }
    #[cfg(feature = "read-stats")]
    if let Some(read_stats_key) = read_stats_key {
        crate::stats::record_scan(read_stats_key, iterator.stats());
    }
    #[cfg(not(feature = "read-stats"))]
    debug_assert!(read_stats_key.is_none());
    Ok(ScanPage { items, more })
}

fn validate_physical_item(item: &ScanItem, limits: &StoreLimits) -> Result<(), StoreError> {
    let key_bytes = item.key().len();
    if key_bytes > limits.max_key_bytes {
        return Err(StoreError::Corrupt(format!(
            "Holt row has {key_bytes} key bytes, maximum {}",
            limits.max_key_bytes
        )));
    }
    if let ScanItem::Row { value, .. } = item {
        if value.len() > limits.max_value_bytes {
            return Err(StoreError::Corrupt(format!(
                "Holt row has {} value bytes, maximum {}",
                value.len(),
                limits.max_value_bytes
            )));
        }
    }
    Ok(())
}

fn scan_item_bytes(item: &ScanItem) -> Result<usize, StoreError> {
    match item {
        ScanItem::Row { key, value } => key.len().checked_add(value.len()).ok_or_else(|| {
            StoreError::Corrupt("Holt scan row byte count overflows usize".to_owned())
        }),
        ScanItem::CommonPrefix(prefix) => Ok(prefix.len()),
    }
}

fn read_value_versions(
    state: &State,
    txn: &WriteTxn,
) -> Result<Option<Vec<ValueVersion>>, StoreError> {
    let value_checks = txn
        .checks
        .iter()
        .filter_map(|check| match check {
            Check::Value { key, expected } => Some((key, expected)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if value_checks.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut scope_prefixes = BTreeMap::<Keyspace, Vec<u8>>::new();
    for (key, _) in &value_checks {
        scope_prefixes
            .entry(key.keyspace)
            .and_modify(|current| truncate_to_common_prefix(current, &key.bytes))
            .or_insert_with(|| key.bytes.clone());
    }
    let owned_scopes = scope_prefixes
        .into_iter()
        .map(|(keyspace, prefix)| (tree_name(state, keyspace).to_owned(), prefix))
        .collect::<Vec<_>>();
    let scopes = owned_scopes
        .iter()
        .map(|(name, prefix)| (name.as_str(), prefix.as_slice()))
        .collect::<Vec<_>>();
    state
        .db
        .view(&scopes, |view| {
            let mut versions = Vec::with_capacity(value_checks.len());
            for (key, expected) in &value_checks {
                let Some(record) = view
                    .tree(tree_name(state, key.keyspace))
                    .ok_or_else(|| holt::Error::TreeNotFound {
                        name: tree_name(state, key.keyspace).to_owned(),
                    })?
                    .get_record(&key.bytes)?
                else {
                    return Ok(None);
                };
                if record.value != **expected {
                    return Ok(None);
                }
                versions.push(ValueVersion {
                    keyspace: key.keyspace,
                    key: key.bytes.clone(),
                    version: record.version,
                });
            }
            Ok(Some(versions))
        })
        .map_err(map_holt_error)
}

fn poison(state: &mut State, reason: impl Into<String>) -> StoreError {
    let reason = reason.into();
    state.poisoned = Some(reason.clone());
    StoreError::OutcomeUnknown {
        state: UnknownCommit::Poisoned,
        reason,
    }
}

fn map_atomic_result(
    state: &mut State,
    applied: Result<bool, holt::Error>,
) -> Result<Commit, StoreError> {
    match applied {
        Ok(true) => Ok(Commit::Applied),
        Ok(false) => Ok(Commit::Conflict),
        Err(holt::Error::Atomic {
            kind: holt::AtomicErrorKind::DefinitelyNotApplied,
            source,
        }) => Err(map_holt_error(*source)),
        Err(error) => Err(poison(state, error.to_string())),
    }
}

fn map_holt_error(error: holt::Error) -> StoreError {
    let reason = error.to_string();
    match error {
        holt::Error::NodeCorrupt { .. }
        | holt::Error::ReplaySanityFailed { .. }
        | holt::Error::Internal(_)
        | holt::Error::TreeNotFound { .. }
        | holt::Error::TreeDropped => StoreError::Corrupt(reason),
        _ => StoreError::Unavailable(reason),
    }
}
