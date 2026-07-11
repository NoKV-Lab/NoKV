use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn get_attr(&self, inode: InodeId) -> Result<Option<InodeAttr>, MetadError> {
        let version = self.read_version()?;
        self.get_attr_at_version(inode, version)
    }

    pub(super) fn get_attr_at_version(
        &self,
        inode: InodeId,
        version: Version,
    ) -> Result<Option<InodeAttr>, MetadError> {
        self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::UserStrong)
    }

    pub(super) fn get_attr_at_version_for_purpose(
        &self,
        inode: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<InodeAttr>, MetadError> {
        let Some(value) = self.metadata.get(
            RecordFamily::Inode,
            &inode_key(self.mount, inode),
            version,
            purpose,
        )?
        else {
            return Ok(None);
        };
        decode_inode_attr(&value.0)
            .map(Some)
            .map_err(|err| MetadError::Codec(err.to_string()))
    }

    pub fn lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        self.lookup_plus_versioned(parent, name)
            .map(|entry| entry.map(|(entry, _)| entry))
    }

    /// Current record version of the `(parent, name)` dentry, or `None` when it
    /// does not exist. This is the value an artifact-replace publish must guard
    /// against; an open write handle that prepared a replace earlier reads it
    /// again just before publishing so an intervening `setattr`/`update_attrs`
    /// (which advances the dentry version) does not strand the handle's CAS.
    pub fn current_dentry_version(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<u64>, MetadError> {
        self.lookup_plus_versioned(parent, name)
            .map(|entry| entry.map(|(_, version)| version.get()))
    }

    pub fn lookup_path(&self, path: &str) -> Result<Option<DentryWithAttr>, MetadError> {
        self.lookup_path_from_at_version_for_purpose_with_index(
            InodeId::root(),
            path,
            self.read_version()?,
            ReadPurpose::UserStrong,
            false,
        )
        .map(|entry| entry.map(|(entry, _)| entry))
    }

    pub(super) fn lookup_plus_versioned(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let version = self.read_version()?;
        self.lookup_plus_at_version(parent, name, version)
    }

    pub(super) fn lookup_plus_for_write_plan(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let version = self.read_version()?;
        self.lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::WritePlanLocal)
    }

    pub(super) fn lookup_plus_at_version(
        &self,
        parent: InodeId,
        name: &DentryName,
        version: Version,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        self.lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::UserStrong)
    }

    pub(super) fn lookup_plus_at_version_for_purpose(
        &self,
        parent: InodeId,
        name: &DentryName,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let key = dentry_key(self.mount, parent, name);
        let Some(item) =
            self.metadata
                .get_versioned(RecordFamily::Dentry, &key, version, purpose)?
        else {
            return Ok(None);
        };
        Ok(Some((
            crate::layout::decode_dentry_projection(&item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?
                .into(),
            item.version,
        )))
    }

    pub fn read_dir_plus(&self, parent: InodeId) -> Result<Vec<DentryWithAttr>, MetadError> {
        let version = self.read_version()?;
        self.read_dir_plus_at_version(parent, version)
    }

    pub fn read_dir_plus_path(&self, path: &str) -> Result<Vec<DentryWithAttr>, MetadError> {
        let parent = self.resolve_directory_path(path)?;
        self.read_dir_plus(parent)
    }

    pub fn read_dir_plus_page(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let version = self.read_version()?;
        self.read_dir_plus_page_at_version(parent, after, limit, version)
    }

    pub fn read_dir_plus_path_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let parent = self.resolve_directory_path(path)?;
        self.read_dir_plus_page(parent, after, limit)
    }

    pub fn list_indexed_path_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let version = self.read_version()?;
        let components = parse_absolute_path(path)?;
        let parent = self.resolve_components_as_directory_at_version(&components, version)?;
        self.list_indexed_components_page(parent, &components, after, limit, version)
    }

    pub fn stat_path(&self, path: &str) -> Result<Option<PathMetadata>, MetadError> {
        self.stat_path_from_at_version(InodeId::root(), path, self.read_version()?)
    }

    pub(super) fn read_dir_plus_at_version(
        &self,
        parent: InodeId,
        version: Version,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        self.read_dir_plus_at_version_for_purpose(parent, version, ReadPurpose::UserStrong)
    }

    pub(super) fn read_dir_plus_at_version_for_purpose(
        &self,
        parent: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_prefix(self.mount, parent),
            start_after: None,
            version,
            limit: 0,
            purpose,
        })?;
        self.read_dir_plus_total.fetch_add(1, Ordering::Relaxed);
        self.read_dir_plus_entry_total
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        let mut entries = Vec::with_capacity(rows.len());
        let mut projection_hits = 0_u64;
        for item in rows {
            let projection = crate::layout::decode_dentry_projection(&item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            projection_hits += 1;
            entries.push(projection.into());
        }
        self.read_dir_plus_projection_hit_total
            .fetch_add(projection_hits, Ordering::Relaxed);
        Ok(entries)
    }

    pub(super) fn read_dir_plus_page_at_version(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        limit: usize,
        version: Version,
    ) -> Result<ReadDirPlusPage, MetadError> {
        self.read_dir_plus_page_at_version_for_purpose(
            parent,
            after,
            limit,
            version,
            ReadPurpose::UserStrong,
        )
    }

    pub(super) fn read_dir_plus_page_at_version_for_purpose(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        limit: usize,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let requested = limit.max(1);
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_prefix(self.mount, parent),
            start_after: after.map(|name| dentry_key(self.mount, parent, name)),
            version,
            limit: requested.saturating_add(1),
            purpose,
        })?;
        self.read_dir_plus_total.fetch_add(1, Ordering::Relaxed);
        let has_more = rows.len() > requested;
        let returned = rows.len().min(requested);
        self.read_dir_plus_entry_total
            .fetch_add(returned as u64, Ordering::Relaxed);
        let mut entries = Vec::<DentryWithAttr>::with_capacity(returned);
        let mut projection_hits = 0_u64;
        for item in rows.into_iter().take(returned) {
            let projection = crate::layout::decode_dentry_projection(&item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            projection_hits += 1;
            entries.push(projection.into());
        }
        self.read_dir_plus_projection_hit_total
            .fetch_add(projection_hits, Ordering::Relaxed);
        let next_cursor = if has_more {
            entries.last().map(|entry| entry.dentry.name.clone())
        } else {
            None
        };
        Ok(ReadDirPlusPage {
            entries,
            next_cursor,
        })
    }

    fn list_indexed_components_page(
        &self,
        parent: InodeId,
        components: &[DentryName],
        after: Option<&DentryName>,
        limit: usize,
        version: Version,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let requested = limit.max(1);
        let prefix = path_index_prefix(self.mount, components);
        let cache_epoch = self.path_cache_epoch.load(Ordering::Acquire);
        let (live, mut stale_rows) = self.collect_live_indexed_children(
            parent,
            &prefix,
            after,
            requested,
            version,
            cache_epoch,
        )?;
        let mut merged = live
            .into_iter()
            .map(|entry| (entry.dentry.name.as_bytes().to_vec(), entry))
            .collect::<BTreeMap<_, _>>();
        let (shadow, shadow_stale) =
            self.collect_restore_indexed_children(parent, after, requested, version)?;
        stale_rows = stale_rows.saturating_add(shadow_stale);
        for entry in shadow {
            merged
                .entry(entry.dentry.name.as_bytes().to_vec())
                .or_insert(entry);
        }
        let mut entries = merged.into_values().collect::<Vec<_>>();
        let next_cursor = if entries.len() > requested {
            entries.truncate(requested);
            entries.last().map(|entry| entry.dentry.name.clone())
        } else {
            None
        };
        self.read_dir_plus_total.fetch_add(1, Ordering::Relaxed);
        self.read_dir_plus_entry_total
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.read_dir_plus_projection_hit_total
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.path_index_scan_stale_total
            .fetch_add(stale_rows, Ordering::Relaxed);
        Ok(ReadDirPlusPage {
            entries,
            next_cursor,
        })
    }

    fn collect_live_indexed_children(
        &self,
        parent: InodeId,
        prefix: &[u8],
        after: Option<&DentryName>,
        requested: usize,
        version: Version,
        cache_epoch: u64,
    ) -> Result<(Vec<DentryWithAttr>, u64), MetadError> {
        let mut start_after = after.map(|name| delimited_child_marker(prefix, name));
        let scan_limit = requested.saturating_add(1);
        let mut entries = Vec::<DentryWithAttr>::with_capacity(scan_limit);
        let mut stale_rows = 0_u64;
        loop {
            let rows = self.metadata.scan_delimited(DelimitedScanRequest {
                family: RecordFamily::PathIndex,
                prefix: prefix.to_vec(),
                start_after: start_after.clone(),
                delimiter: PATH_INDEX_DELIMITER,
                version,
                limit: scan_limit,
                purpose: ReadPurpose::UserStrong,
            })?;
            if rows.is_empty() {
                break;
            }
            let exhausted = rows.len() < scan_limit;
            let mut last_marker = None;
            for item in rows {
                last_marker = Some(delimited_scan_marker(&item));
                let Some(entry) =
                    self.indexed_path_child(parent, prefix, item, version, cache_epoch)?
                else {
                    stale_rows += 1;
                    continue;
                };
                entries.push(entry);
                if entries.len() > requested {
                    break;
                }
            }
            if entries.len() > requested || exhausted {
                break;
            }
            let Some(marker) = last_marker else {
                break;
            };
            start_after = Some(marker);
        }
        Ok((entries, stale_rows))
    }

    fn collect_restore_indexed_children(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        requested: usize,
        version: Version,
    ) -> Result<(Vec<DentryWithAttr>, u64), MetadError> {
        let prefix = fork_shadow_prefix(self.mount, parent);
        let mut start_after = after.map(|name| fork_shadow_key(self.mount, parent, name));
        let scan_limit = requested.saturating_add(1);
        let mut entries = Vec::with_capacity(scan_limit);
        let mut stale_rows = 0_u64;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::ForkShadow,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: scan_limit,
                purpose: ReadPurpose::UserStrong,
            })?;
            if rows.is_empty() {
                break;
            }
            let exhausted = rows.len() < scan_limit;
            let mut last_marker = None;
            for item in rows {
                last_marker = Some(item.key.clone());
                let Some(entry) =
                    self.restore_indexed_path_child(parent, &prefix, &item, version)?
                else {
                    stale_rows += 1;
                    continue;
                };
                entries.push(entry);
                if entries.len() > requested {
                    break;
                }
            }
            if entries.len() > requested || exhausted {
                break;
            }
            let Some(marker) = last_marker else {
                break;
            };
            start_after = Some(marker);
        }
        Ok((entries, stale_rows))
    }

    pub(super) fn restore_shadow_key_for_entry(
        &self,
        components: &[DentryName],
        parent: InodeId,
        name: &DentryName,
        entry: &DentryWithAttr,
        version: Version,
    ) -> Result<Option<(Vec<u8>, u64)>, MetadError> {
        let Some((path_name, parent_components)) = components.split_last() else {
            return Ok(None);
        };
        if path_name != name {
            return Err(MetadError::InvalidPath(
                "path-index entry name does not match path context".to_owned(),
            ));
        }
        let _ = parent_components;
        self.restore_shadow_key_for_inode_entry(parent, name, entry, version)
    }

    pub(super) fn restore_shadow_key_for_inode_entry(
        &self,
        parent: InodeId,
        name: &DentryName,
        entry: &DentryWithAttr,
        version: Version,
    ) -> Result<Option<(Vec<u8>, u64)>, MetadError> {
        let inverse_key = fork_shadow_key(self.mount, parent, name);
        let Some(value) = self.metadata.get(
            RecordFamily::ForkShadow,
            &inverse_key,
            version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(None);
        };
        let (base_ref_set_id, marker) = decode_restore_shadow_inverse(&value.0)?;
        Ok((marker.attr.inode == entry.attr.inode
            && marker.dentry.parent == parent
            && marker.dentry.name == *name)
            .then(|| {
                (
                    restore_path_index_key(self.mount, base_ref_set_id, parent, name),
                    base_ref_set_id,
                )
            }))
    }

    pub(super) fn restore_shadow_destination_key(
        &self,
        components: &[DentryName],
        base_ref_set_id: u64,
        parent: InodeId,
        name: &DentryName,
        version: Version,
    ) -> Result<Option<Vec<u8>>, MetadError> {
        let Some((path_name, parent_components)) = components.split_last() else {
            return Ok(None);
        };
        if path_name != name {
            return Err(MetadError::InvalidPath(
                "path-index destination name does not match path context".to_owned(),
            ));
        }
        let _ = (parent_components, version);
        Ok(Some(restore_path_index_key(
            self.mount,
            base_ref_set_id,
            parent,
            name,
        )))
    }

    pub(super) fn restore_shadow_destination_key_for_parent(
        &self,
        base_ref_set_id: u64,
        parent: InodeId,
        name: &DentryName,
        version: Version,
    ) -> Result<Option<Vec<u8>>, MetadError> {
        let _ = version;
        Ok(Some(restore_path_index_key(
            self.mount,
            base_ref_set_id,
            parent,
            name,
        )))
    }

    fn restore_indexed_path_child(
        &self,
        parent: InodeId,
        prefix: &[u8],
        item: &ScanItem,
        version: Version,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let name = DentryName::new(
            item.key
                .strip_prefix(prefix)
                .ok_or_else(|| {
                    MetadError::Codec("restore shadow escaped parent prefix".to_owned())
                })?
                .to_vec(),
        )
        .map_err(|err| MetadError::InvalidPath(err.to_string()))?;
        let (_, indexed): (u64, DentryProjection) = decode_restore_shadow_inverse(&item.value.0)?;
        let Some((canonical, _)) = self.lookup_plus_at_version(parent, &name, version)? else {
            return Ok(None);
        };
        if indexed.attr.inode != canonical.attr.inode
            || indexed.dentry.parent != parent
            || indexed.dentry.name != name
        {
            return Ok(None);
        }
        if canonical.attr.file_type == FileType::Directory
            && !self.restore_shadow_subtree_has_index(
                canonical.attr.inode,
                version,
                &mut HashSet::new(),
            )?
        {
            return Ok(None);
        }
        Ok(Some(canonical))
    }

    fn restore_shadow_subtree_has_index(
        &self,
        parent: InodeId,
        version: Version,
        visited: &mut HashSet<InodeId>,
    ) -> Result<bool, MetadError> {
        if !visited.insert(parent) {
            return Ok(false);
        }
        let prefix = fork_shadow_prefix(self.mount, parent);
        let mut start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::ForkShadow,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: 128,
                purpose: ReadPurpose::UserStrong,
            })?;
            if rows.is_empty() {
                break;
            }
            let exhausted = rows.len() < 128;
            start_after = rows.last().map(|row| row.key.clone());
            for row in rows {
                let (_, marker) = decode_restore_shadow_inverse(&row.value.0)?;
                let Some((canonical, _)) =
                    self.lookup_plus_at_version(parent, &marker.dentry.name, version)?
                else {
                    continue;
                };
                if canonical.attr.inode != marker.attr.inode {
                    continue;
                }
                if canonical.attr.file_type != FileType::Directory
                    || self.restore_shadow_subtree_has_index(
                        canonical.attr.inode,
                        version,
                        visited,
                    )?
                {
                    return Ok(true);
                }
            }
            if exhausted {
                break;
            }
        }
        Ok(false)
    }

    fn indexed_path_child(
        &self,
        parent: InodeId,
        prefix: &[u8],
        item: DelimitedScanItem,
        version: Version,
        cache_epoch: u64,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        match item {
            DelimitedScanItem::Key(item) => {
                let name = path_index_child_name(prefix, &item.key, false)?;
                if let Some(cached) =
                    self.cached_validated_path_index(&item.key, item.version, version)?
                {
                    if cached.dentry.parent == parent && cached.dentry.name == name {
                        return Ok(Some(cached));
                    }
                }
                let indexed: DentryWithAttr =
                    crate::layout::decode_dentry_projection(&item.value.0)
                        .map_err(|err| MetadError::Codec(err.to_string()))?
                        .into();
                let Some((canonical, canonical_version)) =
                    self.lookup_plus_at_version(parent, &name, version)?
                else {
                    return Ok(None);
                };
                if canonical_version == item.version && canonical == indexed {
                    self.remember_path_index_lookup(
                        &item.key,
                        version,
                        &canonical,
                        item.version,
                        cache_epoch,
                    )?;
                    self.remember_validated_path_index(
                        &item.key,
                        item.version,
                        version,
                        &canonical,
                        cache_epoch,
                    )?;
                    Ok(Some(canonical))
                } else {
                    Ok(None)
                }
            }
            DelimitedScanItem::CommonPrefix(common) => {
                let name = path_index_child_name(prefix, &common, true)?;
                Ok(self
                    .lookup_plus_at_version(parent, &name, version)?
                    .map(|(entry, _)| entry))
            }
        }
    }

    pub(super) fn resolve_parent_path(
        &self,
        path: &str,
    ) -> Result<(InodeId, DentryName), MetadError> {
        let mut components = parse_absolute_path(path)?;
        let name = components
            .pop()
            .ok_or_else(|| MetadError::InvalidPath("root has no parent".to_owned()))?;
        let parent = self.resolve_components_as_directory(&components)?;
        Ok((parent, name))
    }

    pub(super) fn resolve_directory_path(&self, path: &str) -> Result<InodeId, MetadError> {
        let components = parse_absolute_path(path)?;
        self.resolve_components_as_directory(&components)
    }

    pub(super) fn resolve_components_as_directory(
        &self,
        components: &[DentryName],
    ) -> Result<InodeId, MetadError> {
        self.resolve_components_as_directory_at_version(components, self.read_version()?)
    }

    pub(super) fn resolve_components_as_directory_at_version(
        &self,
        components: &[DentryName],
        version: Version,
    ) -> Result<InodeId, MetadError> {
        self.resolve_components_as_directory_from_at_version(InodeId::root(), components, version)
    }

    pub(super) fn resolve_components_as_directory_from_at_version(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
    ) -> Result<InodeId, MetadError> {
        self.resolve_components_as_directory_from_at_version_for_purpose(
            root,
            components,
            version,
            ReadPurpose::UserStrong,
        )
    }

    pub(super) fn resolve_components_as_directory_from_at_version_for_purpose(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<InodeId, MetadError> {
        if components.is_empty() {
            return Ok(root);
        }
        if let Some(cached) = self.cached_path_resolution(root, components, version)? {
            return Ok(cached);
        }
        let cache_epoch = self.path_cache_epoch.load(Ordering::Acquire);
        let mut current = root;
        for index in 0..components.len() {
            let prefix = &components[..=index];
            if let Some(cached) = self.cached_path_resolution(root, prefix, version)? {
                current = cached;
                continue;
            }
            let name = &components[index];
            let entry = self
                .lookup_plus_at_version_for_purpose(current, name, version, purpose)?
                .map(|(entry, _)| entry)
                .ok_or(MetadError::NotFound)?;
            if entry.attr.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
            current = entry.attr.inode;
            self.remember_path_resolution(root, prefix, version, current, cache_epoch)?;
        }
        Ok(current)
    }

    fn cached_path_resolution(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
    ) -> Result<Option<InodeId>, MetadError> {
        let key = self.path_resolution_cache_key(root, components, version);
        let shard_index = path_cache_shard_index(&key);
        let cache = self.path_resolution_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!("metadata path resolution cache poisoned: {err}"))
            })?;
        Ok(cache.get(&key).copied())
    }

    fn remember_path_resolution(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
        inode: InodeId,
        cache_epoch: u64,
    ) -> Result<(), MetadError> {
        let key = self.path_resolution_cache_key(root, components, version);
        let shard_index = path_cache_shard_index(&key);
        let mut cache = self.path_resolution_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!("metadata path resolution cache poisoned: {err}"))
            })?;
        if !self.path_cache_fill_epoch_current(cache_epoch) {
            return Ok(());
        }
        if cache.len() >= PATH_RESOLUTION_CACHE_MAX_ENTRIES_PER_SHARD {
            cache.clear();
        }
        cache.insert(key, inode);
        Ok(())
    }

    fn path_resolution_cache_key(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
    ) -> PathResolutionCacheKey {
        PathResolutionCacheKey {
            root: root.get(),
            version: version.get(),
            components_key: path_index_key(self.mount, components),
        }
    }

    fn cached_validated_path_index(
        &self,
        index_key: &[u8],
        index_version: Version,
        read_version: Version,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let key = self.path_index_validation_cache_key(index_key, index_version, read_version);
        let shard_index = path_cache_shard_index(&key);
        let cache = self.path_index_validation_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!(
                    "metadata path-index validation cache poisoned: {err}"
                ))
            })?;
        Ok(cache.get(&key).cloned())
    }

    fn cached_path_index_lookup(
        &self,
        index_key: &[u8],
        read_version: Version,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let key = self.path_index_lookup_cache_key(index_key, read_version);
        let shard_index = path_cache_shard_index(&key);
        let cache = self.path_index_lookup_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!("metadata path-index lookup cache poisoned: {err}"))
            })?;
        Ok(cache
            .get(&key)
            .map(|value| (value.entry.clone(), value.dentry_version)))
    }

    fn remember_path_index_lookup(
        &self,
        index_key: &[u8],
        read_version: Version,
        entry: &DentryWithAttr,
        dentry_version: Version,
        cache_epoch: u64,
    ) -> Result<(), MetadError> {
        let key = self.path_index_lookup_cache_key(index_key, read_version);
        let shard_index = path_cache_shard_index(&key);
        let mut cache = self.path_index_lookup_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!("metadata path-index lookup cache poisoned: {err}"))
            })?;
        if !self.path_cache_fill_epoch_current(cache_epoch) {
            return Ok(());
        }
        if cache.len() >= PATH_INDEX_LOOKUP_CACHE_MAX_ENTRIES_PER_SHARD {
            cache.clear();
        }
        cache.insert(
            key,
            PathIndexLookupCacheValue {
                entry: entry.clone(),
                dentry_version,
            },
        );
        Ok(())
    }

    fn remember_validated_path_index(
        &self,
        index_key: &[u8],
        index_version: Version,
        read_version: Version,
        entry: &DentryWithAttr,
        cache_epoch: u64,
    ) -> Result<(), MetadError> {
        let key = self.path_index_validation_cache_key(index_key, index_version, read_version);
        let shard_index = path_cache_shard_index(&key);
        let mut cache = self.path_index_validation_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!(
                    "metadata path-index validation cache poisoned: {err}"
                ))
            })?;
        if !self.path_cache_fill_epoch_current(cache_epoch) {
            return Ok(());
        }
        if cache.len() >= PATH_INDEX_VALIDATION_CACHE_MAX_ENTRIES_PER_SHARD {
            cache.clear();
        }
        cache.insert(key, entry.clone());
        Ok(())
    }

    /// True when no write applied since the caller snapshotted the epoch (before
    /// its engine reads). Callers hold their target shard's lock across this
    /// check and the insert: the purger bumps the epoch before clearing any
    /// shard, so a fill that raced a purge either loses the check here or lands
    /// before the clear and is wiped by it — a stale entry can never survive.
    ///
    /// Correctness rests on the shard `Mutex`, not the atomic ordering: a fill
    /// that outlives a purge's clear must have acquired the shard lock *after*
    /// the purge released it, and that release→acquire edge orders the purge's
    /// (sequenced-earlier) epoch bump before this load, so coherence forces the
    /// load to observe it and the check fails. The `Acquire`/`Release` pairing
    /// on the epoch itself is belt-and-suspenders: it makes that ordering hold
    /// without leaning on the lock argument, so a future reader need not derive
    /// it to trust the invariant.
    fn path_cache_fill_epoch_current(&self, cache_epoch: u64) -> bool {
        self.path_cache_epoch.load(Ordering::Acquire) == cache_epoch
    }

    /// Drop every path-cache entry after a metadata write applied. Commit
    /// versions are pre-allocated (possibly an RPC earlier, e.g. prepared
    /// artifacts), so an entry cached at `read_version >= commit_version` may
    /// hold pre-commit state that no later clock bump would ever shadow; a
    /// commit never advances the clock, so exact-version lookups would serve it
    /// for the process lifetime. Infallible on purpose: the commit is already
    /// durably applied, so a poisoned cache mutex must not fail the write —
    /// clearing a poisoned map is safe.
    ///
    /// The bump is `AcqRel` and pairs with the `Acquire` snapshot/guard loads so
    /// a fill that reads a stale epoch can never insert a survivor (see
    /// `path_cache_fill_epoch_current`). It runs before the clears so any fill
    /// still holding a pre-bump snapshot is either dropped by its guard or wiped
    /// by the clear.
    pub(super) fn purge_path_caches_after_write(&self) {
        self.path_cache_epoch.fetch_add(1, Ordering::AcqRel);
        for shard in &self.path_resolution_cache {
            shard.lock().unwrap_or_else(|err| err.into_inner()).clear();
        }
        for shard in &self.path_index_lookup_cache {
            shard.lock().unwrap_or_else(|err| err.into_inner()).clear();
        }
        for shard in &self.path_index_validation_cache {
            shard.lock().unwrap_or_else(|err| err.into_inner()).clear();
        }
    }

    fn path_index_lookup_cache_key(
        &self,
        index_key: &[u8],
        read_version: Version,
    ) -> PathIndexLookupCacheKey {
        PathIndexLookupCacheKey {
            read_version: read_version.get(),
            index_key: index_key.to_vec(),
        }
    }

    fn path_index_validation_cache_key(
        &self,
        index_key: &[u8],
        index_version: Version,
        read_version: Version,
    ) -> PathIndexValidationCacheKey {
        PathIndexValidationCacheKey {
            read_version: read_version.get(),
            index_version: index_version.get(),
            index_key: index_key.to_vec(),
        }
    }

    #[cfg(test)]
    pub(super) fn clear_read_path_caches_for_test(&self) {
        for shard in &self.path_resolution_cache {
            shard.lock().unwrap().clear();
        }
        for shard in &self.path_index_lookup_cache {
            shard.lock().unwrap().clear();
        }
        for shard in &self.path_index_validation_cache {
            shard.lock().unwrap().clear();
        }
    }

    pub(super) fn lookup_path_from_at_version_for_purpose(
        &self,
        root: InodeId,
        path: &str,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        self.lookup_path_from_at_version_for_purpose_with_index(root, path, version, purpose, true)
    }

    fn lookup_path_from_at_version_for_purpose_with_index(
        &self,
        root: InodeId,
        path: &str,
        version: Version,
        purpose: ReadPurpose,
        probe_path_index: bool,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let mut components = parse_absolute_path(path)?;
        if probe_path_index && root == InodeId::root() && components.len() > 1 {
            if let Some(indexed) =
                self.lookup_path_index_components_at_version(&components, version, purpose)?
            {
                return Ok(Some(indexed));
            }
            self.path_index_fallback_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let Some(name) = components.pop() else {
            return Ok(None);
        };
        let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
            root,
            &components,
            version,
            purpose,
        )?;
        self.lookup_plus_at_version_for_purpose(parent, &name, version, purpose)
    }

    fn lookup_path_index_components_at_version(
        &self,
        components: &[DentryName],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let Some((name, parent_components)) = components.split_last() else {
            return Ok(None);
        };
        self.path_index_lookup_total.fetch_add(1, Ordering::Relaxed);
        let key = path_index_key(self.mount, components);
        if let Some(cached) = self.cached_path_index_lookup(&key, version)? {
            self.path_index_hit_total.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(cached));
        }
        let cache_epoch = self.path_cache_epoch.load(Ordering::Acquire);
        let Some(item) =
            self.metadata
                .get_versioned(RecordFamily::PathIndex, &key, version, purpose)?
        else {
            self.path_index_miss_total.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        if let Some(cached) = self.cached_validated_path_index(&key, item.version, version)? {
            self.path_index_hit_total.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((cached, item.version)));
        }
        let indexed: DentryWithAttr = crate::layout::decode_dentry_projection(&item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?
            .into();
        let parent = match self.resolve_components_as_directory_from_at_version_for_purpose(
            InodeId::root(),
            parent_components,
            version,
            purpose,
        ) {
            Ok(parent) => parent,
            Err(MetadError::NotFound | MetadError::NotDirectory) => {
                self.path_index_stale_total.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            Err(err) => return Err(err),
        };
        if parent != indexed.dentry.parent || *name != indexed.dentry.name {
            self.path_index_stale_total.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let Some((canonical, canonical_version)) =
            self.lookup_plus_at_version_for_purpose(parent, name, version, purpose)?
        else {
            self.path_index_stale_total.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        if canonical_version == item.version && canonical == indexed {
            self.remember_path_index_lookup(
                &key,
                version,
                &canonical,
                canonical_version,
                cache_epoch,
            )?;
            self.remember_validated_path_index(
                &key,
                item.version,
                version,
                &canonical,
                cache_epoch,
            )?;
            self.path_index_hit_total.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((canonical, canonical_version)));
        }
        self.path_index_stale_total.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    pub(super) fn stat_path_from_at_version(
        &self,
        root: InodeId,
        path: &str,
        version: Version,
    ) -> Result<Option<PathMetadata>, MetadError> {
        self.stat_path_from_at_version_for_purpose(root, path, version, ReadPurpose::UserStrong)
    }

    pub(super) fn stat_path_from_at_version_for_purpose(
        &self,
        root: InodeId,
        path: &str,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<PathMetadata>, MetadError> {
        let components = parse_absolute_path(path)?;
        if components.is_empty() {
            let Some(attr) = self.get_attr_at_version_for_purpose(root, version, purpose)? else {
                return Ok(None);
            };
            if attr.file_type == FileType::File {
                let body = self.body_descriptor_at_version_for_purpose(
                    root,
                    attr.generation,
                    version,
                    purpose,
                )?;
                return Ok(Some(PathMetadata { attr, body }));
            }
            return Ok(Some(PathMetadata { attr, body: None }));
        }
        let Some((entry, _)) =
            self.lookup_path_from_at_version_for_purpose(root, path, version, purpose)?
        else {
            return Ok(None);
        };
        Ok(Some(PathMetadata {
            attr: entry.attr,
            body: entry.body,
        }))
    }

    pub fn read_artifact(&self, parent: InodeId, name: &DentryName) -> Result<Vec<u8>, MetadError> {
        let version = self.read_version()?;
        let entry = self
            .lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::UserStrong)?
            .map(|(entry, _)| entry)
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version(entry.attr.inode, &body, 0, body.size as usize, version)
    }

    pub fn body_descriptor(&self, inode: InodeId) -> Result<Option<BodyDescriptor>, MetadError> {
        let Some(attr) = self.get_attr(inode)? else {
            return Ok(None);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        self.body_descriptor_at_version(inode, attr.generation, self.read_version()?)
    }

    pub(super) fn body_descriptor_at_version(
        &self,
        inode: InodeId,
        generation: u64,
        version: Version,
    ) -> Result<Option<BodyDescriptor>, MetadError> {
        self.body_descriptor_at_version_for_purpose(
            inode,
            generation,
            version,
            ReadPurpose::UserStrong,
        )
    }

    pub(super) fn body_descriptor_at_version_for_purpose(
        &self,
        inode: InodeId,
        generation: u64,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<BodyDescriptor>, MetadError> {
        let summary_key =
            chunk_manifest_key(self.mount, inode, generation, BODY_SUMMARY_CHUNK_INDEX);
        let Some(value) =
            self.metadata
                .get(RecordFamily::ChunkManifest, &summary_key, version, purpose)?
        else {
            return Err(MetadError::MissingBodyDescriptor);
        };
        decode_body_descriptor(&value.0)
            .map(Some)
            .map_err(|err| MetadError::Codec(err.to_string()))
    }

    pub fn read_file(
        &self,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let version = self.read_version()?;
        let Some(attr) =
            self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::UserStrong)?
        else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if offset >= attr.size {
            return Ok(Vec::new());
        }
        let body = self
            .body_descriptor_at_version_for_purpose(
                inode,
                attr.generation,
                version,
                ReadPurpose::UserStrong,
            )?
            .ok_or(MetadError::NotFound)?;
        self.read_file_at_version(inode, &body, offset, len, version)
    }

    pub fn read_symlink(&self, inode: InodeId) -> Result<Vec<u8>, MetadError> {
        let Some(attr) = self.get_attr(inode)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::Symlink {
            return Err(MetadError::NotFile);
        }
        let version = self.read_version()?;
        let body = self
            .body_descriptor_at_version(inode, attr.generation, version)?
            .ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version(inode, &body, 0, body.size as usize, version)
    }

    /// Open `inode` for reading: the formal `open()` boundary for the read path.
    ///
    /// Returns a [`ReadLease`] naming the file's current `(generation,
    /// read_version)` **without writing any metadata** — read-mode open creates
    /// zero state. The caller then issues range reads via [`read_file_plan`]
    /// carrying `lease.generation`; each read validates that generation against
    /// the live attr, so a concurrent rewrite surfaces as `StaleBodyGeneration`
    /// instead of a silent stale read. The generation freezes the immutable block
    /// layout the reader sees — the substrate for reshard-on-read (arbitrary range
    /// reads from a differently-parallelized consumer over one consistent view).
    ///
    /// This lease holds **no** durable pin, so it cannot keep a *superseded*
    /// generation alive against GC; it is sound only because the live generation's
    /// blocks are never reclaimed. To read a historical generation, take a durable
    /// snapshot pin ([`snapshot_subtree`](Self::snapshot_subtree)) instead.
    pub fn open_read(&self, inode: InodeId) -> Result<ReadLease, MetadError> {
        self.open_read_expecting(inode, None)
    }

    /// [`open_read`](Self::open_read) that additionally fails with
    /// `StaleBodyGeneration` unless the file is currently at `expected_generation`
    /// — for a caller re-opening to confirm it still holds the same artifact.
    pub fn open_read_expecting(
        &self,
        inode: InodeId,
        expected_generation: Option<u64>,
    ) -> Result<ReadLease, MetadError> {
        let version = self.read_version()?;
        let Some(attr) =
            self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::UserStrong)?
        else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if let Some(expected) = expected_generation {
            if attr.generation != expected {
                return Err(MetadError::StaleBodyGeneration {
                    expected,
                    current: attr.generation,
                });
            }
        }
        Ok(read_lease_for_generation(inode, attr.generation, version))
    }

    pub fn read_file_plan(
        &self,
        inode: InodeId,
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<BodyReadPlan, MetadError> {
        if len == 0 {
            return Ok(BodyReadPlan {
                output_len: 0,
                blocks: Vec::new(),
            });
        }
        let version = self.read_version()?;
        let Some(attr) =
            self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::UserStrong)?
        else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if attr.generation != generation {
            return Err(MetadError::StaleBodyGeneration {
                expected: generation,
                current: attr.generation,
            });
        }
        self.body_read_plan_at_version(inode, &attr, offset, len, version)
    }

    pub fn open_path_read_plan(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<OpenPathReadPlan, MetadError> {
        let version = self.read_version()?;
        let path_plan =
            self.path_read_plan_at_version(path, offset, len, expected_generation, version)?;
        let lease = read_lease_for_generation(
            path_plan.metadata.attr.inode,
            path_plan.metadata.attr.generation,
            version,
        );
        Ok(OpenPathReadPlan {
            metadata: path_plan.metadata,
            lease,
            plan: path_plan.plan,
        })
    }

    pub fn open_path_read_plan_batch(
        &self,
        requests: &[super::OpenPathReadPlanRequest],
    ) -> Result<Vec<OpenPathReadPlan>, MetadError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.read_version()?;
        requests
            .iter()
            .map(|request| {
                let path_plan = self.path_read_plan_at_version(
                    &request.path,
                    request.offset,
                    request.len,
                    request.expected_generation,
                    version,
                )?;
                let lease = read_lease_for_generation(
                    path_plan.metadata.attr.inode,
                    path_plan.metadata.attr.generation,
                    version,
                );
                Ok(OpenPathReadPlan {
                    metadata: path_plan.metadata,
                    lease,
                    plan: path_plan.plan,
                })
            })
            .collect()
    }

    fn body_read_plan_at_version(
        &self,
        inode: InodeId,
        attr: &InodeAttr,
        offset: u64,
        len: usize,
        version: Version,
    ) -> Result<BodyReadPlan, MetadError> {
        if len == 0 || offset >= attr.size {
            return Ok(BodyReadPlan {
                output_len: 0,
                blocks: Vec::new(),
            });
        }
        let body = self
            .body_descriptor_at_version_for_purpose(
                inode,
                attr.generation,
                version,
                ReadPurpose::UserStrong,
            )?
            .ok_or(MetadError::MissingBodyDescriptor)?;
        if body.size != attr.size {
            return Err(MetadError::BodySizeMismatch {
                descriptor: body.size,
                bytes: attr.size,
            });
        }
        let output_len = len.min((attr.size - offset) as usize);
        Ok(BodyReadPlan {
            output_len,
            blocks: self.read_plan(inode, &body, offset, output_len, version)?,
        })
    }

    fn path_read_plan_at_version(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
        version: Version,
    ) -> Result<PathReadPlan, MetadError> {
        let entry = self
            .lookup_path_from_at_version_for_purpose(
                InodeId::root(),
                path,
                version,
                ReadPurpose::UserStrong,
            )?
            .map(|(entry, _)| entry)
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if let Some(expected) = expected_generation {
            if entry.attr.generation != expected {
                return Err(MetadError::StaleBodyGeneration {
                    expected,
                    current: entry.attr.generation,
                });
            }
        }
        let body = entry
            .body
            .clone()
            .ok_or(MetadError::MissingBodyDescriptor)?;
        let output_len = if offset >= entry.attr.size {
            0
        } else {
            len.min((entry.attr.size - offset) as usize)
        };
        let blocks = if output_len == 0 {
            Vec::new()
        } else {
            self.read_plan_for_purpose(
                entry.attr.inode,
                &body,
                offset,
                output_len,
                version,
                ReadPurpose::UserStrong,
            )?
        };
        Ok(PathReadPlan {
            metadata: PathMetadata {
                attr: entry.attr,
                body: Some(body),
            },
            plan: BodyReadPlan { output_len, blocks },
        })
    }

    pub(super) fn read_file_at_version(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
    ) -> Result<Vec<u8>, MetadError> {
        self.read_file_at_version_for_purpose(
            inode,
            body,
            offset,
            len,
            version,
            ReadPurpose::UserStrong,
        )
    }

    pub(super) fn read_file_at_version_for_purpose(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        if offset >= body.size {
            return Ok(Vec::new());
        }
        let len = len.min((body.size - offset) as usize);
        let plan = self.read_plan_for_purpose(inode, body, offset, len, version, purpose)?;
        let cache = if self.block_cache_enabled() {
            Some(&self.block_cache)
        } else {
            None
        };
        let outcome = self.objects.read_blocks_with_options(
            cache,
            len,
            &plan,
            BlockReadOptions::default(),
        )?;
        self.object_gets
            .fetch_add(outcome.object_gets as u64, Ordering::Relaxed);
        self.object_get_bytes
            .fetch_add(outcome.object_get_bytes, Ordering::Relaxed);
        self.coalesced_gets
            .fetch_add(outcome.coalesced_gets as u64, Ordering::Relaxed);
        self.coalesced_get_bytes
            .fetch_add(outcome.coalesced_get_bytes, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(outcome.cache_hits as u64, Ordering::Relaxed);
        self.cache_hit_bytes
            .fetch_add(outcome.cache_hit_bytes, Ordering::Relaxed);
        Ok(outcome.bytes)
    }

    pub fn read_session_object_blocks(
        &self,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> Result<Vec<u8>, MetadError> {
        let cache = self.block_cache_enabled().then_some(&self.block_cache);
        let outcome = self.objects.read_blocks_with_options(
            cache,
            output_len,
            blocks,
            BlockReadOptions::default(),
        )?;
        self.object_gets
            .fetch_add(outcome.object_gets as u64, Ordering::Relaxed);
        self.object_get_bytes
            .fetch_add(outcome.object_get_bytes, Ordering::Relaxed);
        self.coalesced_gets
            .fetch_add(outcome.coalesced_gets as u64, Ordering::Relaxed);
        self.coalesced_get_bytes
            .fetch_add(outcome.coalesced_get_bytes, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(outcome.cache_hits as u64, Ordering::Relaxed);
        self.cache_hit_bytes
            .fetch_add(outcome.cache_hit_bytes, Ordering::Relaxed);
        Ok(outcome.bytes)
    }

    pub(super) fn read_plan(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
    ) -> Result<Vec<ObjectReadBlock>, MetadError> {
        self.read_plan_for_purpose(inode, body, offset, len, version, ReadPurpose::UserStrong)
    }

    pub(super) fn read_plan_for_purpose(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<ObjectReadBlock>, MetadError> {
        if body.chunk_size == 0 || body.block_size == 0 {
            return Err(ObjectError::InvalidChunkLayout.into());
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or(ObjectError::InvalidRange)?
            .min(body.size);
        if end <= offset {
            return Ok(Vec::new());
        }

        let start_chunk = offset / body.chunk_size;
        let end_chunk = (end - 1) / body.chunk_size;
        // A sparse generation stores manifests only for the chunks it rewrote;
        // untouched chunks fall through to `base_generation`. Resolve the
        // newest-first generation chain once, then take each chunk's manifest
        // from the newest generation that holds it. For a self-contained body
        // (`base_generation == 0`) the chain is a single element and this is
        // identical to a direct per-chunk lookup.
        let chain = self.resolve_generation_chain(inode, body, version, purpose)?;
        let mut manifests = Vec::with_capacity((end_chunk - start_chunk + 1) as usize);
        for chunk_index in start_chunk..=end_chunk {
            manifests.push(self.resolve_chunk_manifest(
                inode,
                &chain,
                chunk_index,
                version,
                purpose,
            )?);
        }
        let slice_plan = plan_chunk_manifest_reads(&manifests, offset, len)?;
        Ok(slice_plan.blocks)
    }

    /// Newest-first list of generations to consult for `body`: the body's own
    /// generation followed by each `base_generation` it falls through to,
    /// ending at a self-contained generation. A fresh or compacted body yields
    /// a single element and performs no extra metadata reads.
    pub(super) fn resolve_generation_chain(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<u64>, MetadError> {
        // Compaction bounds the live chain far below this; the cap only guards
        // against a corrupt/cyclic pointer.
        const MAX_GENERATION_CHAIN_DEPTH: usize = 64;
        let mut chain = vec![body.generation];
        let mut base = body.base_generation;
        while base != 0 {
            if chain.len() >= MAX_GENERATION_CHAIN_DEPTH {
                return Err(MetadError::Codec(
                    "metadata generation chain exceeds maximum depth".to_owned(),
                ));
            }
            chain.push(base);
            base = self
                .body_descriptor_at_version_for_purpose(inode, base, version, purpose)?
                .ok_or(MetadError::MissingBodyDescriptor)?
                .base_generation;
        }
        Ok(chain)
    }

    /// Resolve a single chunk's manifest from the newest generation in `chain`
    /// that stored one, or `None` if no generation holds it (a hole — e.g. a
    /// chunk appended past the base's EOF). Each generation's chunk manifest is
    /// self-contained for the chunk it rewrote, so the first (newest) hit is
    /// authoritative.
    pub(super) fn chain_chunk_manifest(
        &self,
        inode: InodeId,
        chain: &[u64],
        chunk_index: u64,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ChunkManifest>, MetadError> {
        for &generation in chain {
            let key = chunk_manifest_key(self.mount, inode, generation, chunk_index);
            if let Some(value) =
                self.metadata
                    .get(RecordFamily::ChunkManifest, &key, version, purpose)?
            {
                return decode_chunk_manifest(&value.0)
                    .map(Some)
                    .map_err(|err| MetadError::Codec(err.to_string()));
            }
        }
        Ok(None)
    }

    /// Strict variant for reads: a chunk covered by the body's size must
    /// resolve to a manifest somewhere in the chain.
    fn resolve_chunk_manifest(
        &self,
        inode: InodeId,
        chain: &[u64],
        chunk_index: u64,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<ChunkManifest, MetadError> {
        self.chain_chunk_manifest(inode, chain, chunk_index, version, purpose)?
            .ok_or(MetadError::MissingBodyDescriptor)
    }

    pub(super) fn chunk_manifests_for_body_at_version(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<ChunkManifest>, MetadError> {
        if body.chunk_size == 0 || body.block_size == 0 {
            return Err(ObjectError::InvalidChunkLayout.into());
        }
        if body.size == 0 {
            return Ok(Vec::new());
        }
        let end_chunk = (body.size - 1) / body.chunk_size;
        // Resolve each chunk through the generation chain so a sparse body
        // yields its *effective* full manifest set (inherited chunks included),
        // which clone/rollback/inheritance depend on. Identity for a
        // self-contained body (chain of length 1).
        let chain = self.resolve_generation_chain(inode, body, version, purpose)?;
        let mut manifests = Vec::new();
        for chunk_index in 0..=end_chunk {
            if let Some(manifest) =
                self.chain_chunk_manifest(inode, &chain, chunk_index, version, purpose)?
            {
                manifests.push(manifest);
            }
        }
        Ok(manifests)
    }
}

fn read_lease_for_generation(inode: InodeId, generation: u64, version: Version) -> ReadLease {
    ReadLease {
        inode,
        generation,
        read_version: version.get(),
        lease_expires_unix_ms: current_time_ms().saturating_add(DEFAULT_READ_LEASE_MS),
    }
}

fn path_index_child_name(
    prefix: &[u8],
    key: &[u8],
    common_prefix: bool,
) -> Result<DentryName, MetadError> {
    let mut suffix = key.strip_prefix(prefix).ok_or_else(|| {
        MetadError::Codec("path index scan returned a key outside the requested prefix".to_owned())
    })?;
    if common_prefix {
        suffix = suffix
            .strip_suffix(&[PATH_INDEX_DELIMITER])
            .ok_or_else(|| {
                MetadError::Codec("path index common prefix is missing delimiter".to_owned())
            })?;
    }
    if suffix.is_empty() || suffix.contains(&PATH_INDEX_DELIMITER) {
        return Err(MetadError::Codec(
            "path index scan returned a malformed child component".to_owned(),
        ));
    }
    DentryName::new(suffix.to_vec()).map_err(|err| MetadError::Codec(err.to_string()))
}

fn delimited_scan_marker(item: &DelimitedScanItem) -> Vec<u8> {
    match item {
        DelimitedScanItem::Key(item) => item.key.clone(),
        DelimitedScanItem::CommonPrefix(prefix) => prefix.clone(),
    }
}

fn delimited_child_marker(prefix: &[u8], name: &DentryName) -> Vec<u8> {
    let mut marker = Vec::with_capacity(prefix.len() + name.as_bytes().len() + 1);
    marker.extend_from_slice(prefix);
    marker.extend_from_slice(name.as_bytes());
    marker.push(PATH_INDEX_DELIMITER);
    marker
}
