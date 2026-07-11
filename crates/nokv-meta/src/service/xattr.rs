use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn set_xattr(
        &self,
        inode: InodeId,
        name: &[u8],
        value: Vec<u8>,
        mode: XattrSetMode,
    ) -> Result<(), MetadError> {
        validate_xattr_name(name)?;
        let version = self.next_version()?;
        let key = xattr_key(self.mount, inode, name);
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, inode),
            predicate: Predicate::Exists,
        }];
        match mode {
            XattrSetMode::Any => {}
            XattrSetMode::Create => predicates.push(PredicateRef {
                family: RecordFamily::Xattr,
                key: key.clone(),
                predicate: Predicate::NotExists,
            }),
            XattrSetMode::Replace => predicates.push(PredicateRef {
                family: RecordFamily::Xattr,
                key: key.clone(),
                predicate: Predicate::Exists,
            }),
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"set-xattr", self.mount, inode, version),
            kind: CommandKind::SetXattr,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Xattr,
            primary_key: key.clone(),
            predicates,
            mutations: vec![Mutation {
                family: RecordFamily::Xattr,
                key,
                op: MutationOp::Put,
                value: Some(Value(value)),
            }],
            watch: self
                .watch_projection(
                    InodeId::root(),
                    WatchEvent {
                        kind: WatchEventKind::UpdateAttr,
                        parent: None,
                        name: None,
                        inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        })?;
        Ok(())
    }

    pub fn get_xattr(&self, inode: InodeId, name: &[u8]) -> Result<Option<Vec<u8>>, MetadError> {
        validate_xattr_name(name)?;
        if self.get_attr(inode)?.is_none() {
            return Err(MetadError::NotFound);
        }
        let version = self.read_version()?;
        self.metadata
            .get(
                RecordFamily::Xattr,
                &xattr_key(self.mount, inode, name),
                version,
                ReadPurpose::UserStrong,
            )
            .map(|value| value.map(|value| value.0))
            .map_err(Into::into)
    }

    pub fn list_xattr(&self, inode: InodeId) -> Result<Vec<Vec<u8>>, MetadError> {
        if self.get_attr(inode)?.is_none() {
            return Err(MetadError::NotFound);
        }
        let prefix = xattr_prefix(self.mount, inode);
        let keys = self.metadata.scan_keys(KeyScanRequest {
            family: RecordFamily::Xattr,
            prefix: prefix.clone(),
            start_after: None,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        Ok(keys
            .into_iter()
            .filter_map(|key| key.strip_prefix(prefix.as_slice()).map(<[u8]>::to_vec))
            .collect())
    }

    pub fn remove_xattr(&self, inode: InodeId, name: &[u8]) -> Result<(), MetadError> {
        validate_xattr_name(name)?;
        let version = self.next_version()?;
        let key = xattr_key(self.mount, inode, name);
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"remove-xattr", self.mount, inode, version),
            kind: CommandKind::RemoveXattr,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Xattr,
            primary_key: key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, inode),
                    predicate: Predicate::Exists,
                },
                PredicateRef {
                    family: RecordFamily::Xattr,
                    key: key.clone(),
                    predicate: Predicate::Exists,
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::Xattr,
                key,
                op: MutationOp::Delete,
                value: None,
            }],
            watch: self
                .watch_projection(
                    InodeId::root(),
                    WatchEvent {
                        kind: WatchEventKind::UpdateAttr,
                        parent: None,
                        name: None,
                        inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        })?;
        Ok(())
    }
}

fn validate_xattr_name(name: &[u8]) -> Result<(), MetadError> {
    if name.is_empty() || name.contains(&0) {
        return Err(MetadError::InvalidPath(
            "xattr name must be non-empty and must not contain NUL".to_owned(),
        ));
    }
    Ok(())
}
