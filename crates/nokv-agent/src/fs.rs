use crate::codec::{decode_json, encode_json};
use crate::key::{
    body_key, catalog_key, child_key, child_prefix, node_key, node_prefix, row_key, row_prefix,
};
use crate::{
    AgentBatch, AgentId, AgentIndexError, AgentIndexField, AgentIndexRegistration,
    AgentIndexResult, AgentIndexRow, AgentNode, AgentNodeKind, AgentScanItem, AgentStore,
    ScanDirection,
};

#[derive(Clone)]
pub struct AgentFs<S> {
    agent_id: AgentId,
    store: S,
}

impl<S> AgentFs<S>
where
    S: AgentStore,
{
    pub fn new(agent_id: AgentId, store: S) -> Self {
        Self { agent_id, store }
    }

    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn bootstrap(&self) -> AgentIndexResult<()> {
        if self.node("/")?.is_some() {
            return Ok(());
        }
        let mut batch = AgentBatch::new();
        put_node(
            &mut batch,
            &self.agent_id,
            AgentNode {
                path: "/".to_owned(),
                name: "/".to_owned(),
                kind: AgentNodeKind::Directory,
                size_bytes: None,
                content_type: None,
            },
        )?;
        self.store.apply(batch)?;
        Ok(())
    }

    pub fn create_dir_all(&self, path: &str) -> AgentIndexResult<()> {
        let path = normalize_path(path)?;
        let mut batch = AgentBatch::new();
        self.add_directory_ancestors(&mut batch, &path)?;
        self.store.apply(batch)?;
        Ok(())
    }

    pub fn put_file(
        &self,
        path: &str,
        bytes: Vec<u8>,
        content_type: impl Into<Option<String>>,
    ) -> AgentIndexResult<()> {
        let path = normalize_path(path)?;
        if let Some(existing) = self.node(&path)? {
            if existing.kind != AgentNodeKind::File {
                return Err(AgentIndexError::InvalidArgument(format!(
                    "path exists but is not a file: {path}"
                )));
            }
        }
        let parent = parent_path(&path)?;
        let name = path_name(&path)?;
        let mut batch = AgentBatch::new();
        self.add_directory_ancestors(&mut batch, &parent)?;
        put_node(
            &mut batch,
            &self.agent_id,
            AgentNode {
                path: path.clone(),
                name: name.clone(),
                kind: AgentNodeKind::File,
                size_bytes: Some(bytes.len() as u64),
                content_type: content_type.into(),
            },
        )?;
        batch.put(child_key(&self.agent_id, &parent, &name), path.as_bytes());
        batch.put(body_key(&self.agent_id, &path), bytes);
        self.store.apply(batch)?;
        Ok(())
    }

    pub fn register_index(&self, registration: AgentIndexRegistration) -> AgentIndexResult<()> {
        let root = normalize_path(&registration.path)?;
        let mut batch = AgentBatch::new();
        batch.put(
            catalog_key(&self.agent_id, &root),
            encode_json(&registration.fields)?,
        );
        for row in registration.rows {
            let path = normalize_path(&row.path)?;
            batch.put(row_key(&self.agent_id, &root, &path), encode_json(&row)?);
        }
        self.store.apply(batch)?;
        Ok(())
    }

    pub fn node(&self, path: &str) -> AgentIndexResult<Option<AgentNode>> {
        let path = normalize_path(path)?;
        self.store
            .get(&node_key(&self.agent_id, &path))?
            .map(|bytes| decode_json(&bytes))
            .transpose()
    }

    pub fn read_file(&self, path: &str) -> AgentIndexResult<Vec<u8>> {
        let path = normalize_path(path)?;
        let Some(node) = self.node(&path)? else {
            return Err(AgentIndexError::NotFound(path));
        };
        if node.kind != AgentNodeKind::File {
            return Err(AgentIndexError::InvalidArgument(format!(
                "{path} is not a file"
            )));
        }
        self.store
            .get(&body_key(&self.agent_id, &path))?
            .ok_or(AgentIndexError::NotFound(path))
    }

    pub fn list(
        &self,
        path: &str,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> AgentIndexResult<(Vec<AgentNode>, Option<Vec<u8>>, bool)> {
        let path = normalize_path(path)?;
        let Some(node) = self.node(&path)? else {
            return Err(AgentIndexError::NotFound(path));
        };
        if node.kind != AgentNodeKind::Directory {
            return Err(AgentIndexError::InvalidArgument(format!(
                "{path} is not a directory"
            )));
        }
        let page = self.store.scan(
            &child_prefix(&self.agent_id, &path),
            cursor,
            limit,
            ScanDirection::Asc,
        )?;
        let mut nodes = Vec::new();
        for item in &page.items {
            let child_path = String::from_utf8(item.value.clone())
                .map_err(|err| AgentIndexError::Codec(err.to_string()))?;
            if let Some(child) = self.node(&child_path)? {
                nodes.push(child);
            }
        }
        Ok((nodes, page.next_cursor, page.truncated))
    }

    pub fn catalog(&self, root: &str) -> AgentIndexResult<Vec<AgentIndexField>> {
        let root = normalize_path(root)?;
        Ok(self
            .store
            .get(&catalog_key(&self.agent_id, &root))?
            .map(|bytes| decode_json(&bytes))
            .transpose()?
            .unwrap_or_default())
    }

    pub fn index_rows(&self, root: &str) -> AgentIndexResult<Vec<AgentIndexRow>> {
        let root = normalize_path(root)?;
        let page = self.scan_all(&row_prefix(&self.agent_id, &root))?;
        page.into_iter()
            .map(|item| decode_json(&item.value))
            .collect()
    }

    pub fn files_under(&self, path: &str, recursive: bool) -> AgentIndexResult<Vec<AgentNode>> {
        let path = normalize_path(path)?;
        let Some(node) = self.node(&path)? else {
            return Err(AgentIndexError::NotFound(path));
        };
        if node.kind == AgentNodeKind::File {
            return Ok(vec![node]);
        }
        if !recursive {
            return Ok(self
                .list(&path, None, usize::MAX)?
                .0
                .into_iter()
                .filter(|node| node.kind == AgentNodeKind::File)
                .collect());
        }
        let child_prefix = if path == "/" {
            "/".to_owned()
        } else {
            format!("{path}/")
        };
        Ok(self
            .scan_all(&node_prefix(&self.agent_id))?
            .into_iter()
            .filter_map(|item| decode_json::<AgentNode>(&item.value).ok())
            .filter(|node| {
                node.kind == AgentNodeKind::File
                    && (node.path == path || node.path.starts_with(child_prefix.as_str()))
            })
            .collect())
    }

    /// Stage directory nodes for every missing ancestor of `path`.
    ///
    /// Components that already exist as directories are left untouched;
    /// a component that exists as anything else fails the whole call so a
    /// file is never silently converted into a directory. Checks read
    /// committed store state: the store is single-writer, so a staged
    /// batch cannot race its own precondition.
    fn add_directory_ancestors(&self, batch: &mut AgentBatch, path: &str) -> AgentIndexResult<()> {
        let path = normalize_path(path)?;
        let mut current = String::from("/");
        if self.node(&current)?.is_none() {
            put_node(
                batch,
                &self.agent_id,
                AgentNode {
                    path: current.clone(),
                    name: "/".to_owned(),
                    kind: AgentNodeKind::Directory,
                    size_bytes: None,
                    content_type: None,
                },
            )?;
        }
        for part in path
            .trim_matches('/')
            .split('/')
            .filter(|part| !part.is_empty())
        {
            let parent = current.clone();
            current = if current == "/" {
                format!("/{part}")
            } else {
                format!("{current}/{part}")
            };
            match self.node(&current)? {
                Some(existing) if existing.kind == AgentNodeKind::Directory => {}
                Some(_) => {
                    return Err(AgentIndexError::InvalidArgument(format!(
                        "path exists but is not a directory: {current}"
                    )))
                }
                None => {
                    put_node(
                        batch,
                        &self.agent_id,
                        AgentNode {
                            path: current.clone(),
                            name: part.to_owned(),
                            kind: AgentNodeKind::Directory,
                            size_bytes: None,
                            content_type: None,
                        },
                    )?;
                    batch.put(child_key(&self.agent_id, &parent, part), current.as_bytes());
                }
            }
        }
        Ok(())
    }

    fn scan_all(&self, prefix: &[u8]) -> AgentIndexResult<Vec<AgentScanItem>> {
        let mut cursor = None;
        let mut out = Vec::new();
        loop {
            let page = self
                .store
                .scan(prefix, cursor.as_deref(), 512, ScanDirection::Asc)?;
            out.extend(page.items);
            if !page.truncated {
                return Ok(out);
            }
            cursor = page.next_cursor;
        }
    }
}

fn put_node(batch: &mut AgentBatch, agent_id: &AgentId, node: AgentNode) -> AgentIndexResult<()> {
    batch.put(node_key(agent_id, &node.path), encode_json(&node)?);
    Ok(())
}

pub(crate) fn normalize_path(path: &str) -> AgentIndexResult<String> {
    if path.is_empty() || !path.starts_with('/') {
        return Err(AgentIndexError::InvalidArgument(format!(
            "agent path must be absolute: {path}"
        )));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        if part.is_empty() {
            continue;
        }
        if part == "." || part == ".." {
            return Err(AgentIndexError::InvalidArgument(format!(
                "agent path must not contain relative components: {path}"
            )));
        }
        parts.push(part);
    }
    if parts.is_empty() {
        Ok("/".to_owned())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

pub(crate) fn parent_path(path: &str) -> AgentIndexResult<String> {
    let path = normalize_path(path)?;
    if path == "/" {
        return Err(AgentIndexError::InvalidArgument(
            "root path has no parent".to_owned(),
        ));
    }
    match path.rsplit_once('/') {
        Some(("", _)) => Ok("/".to_owned()),
        Some((parent, _)) => Ok(parent.to_owned()),
        None => Err(AgentIndexError::InvalidArgument(format!(
            "invalid path {path}"
        ))),
    }
}

pub(crate) fn path_name(path: &str) -> AgentIndexResult<String> {
    let path = normalize_path(path)?;
    if path == "/" {
        return Ok("/".to_owned());
    }
    path.rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| AgentIndexError::InvalidArgument(format!("invalid path {path}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HoltAgentStore;

    fn fs() -> AgentFs<HoltAgentStore> {
        let fs = AgentFs::new(
            AgentId::new("fs-test"),
            HoltAgentStore::open_memory().unwrap(),
        );
        fs.bootstrap().unwrap();
        fs
    }

    #[test]
    fn put_file_rejects_existing_directory_target() {
        let fs = fs();
        fs.create_dir_all("/w/input/dir").unwrap();
        fs.put_file("/w/input/dir/child.txt", b"child".to_vec(), None)
            .unwrap();

        let err = fs
            .put_file("/w/input/dir", b"x".to_vec(), None)
            .unwrap_err();

        assert!(
            matches!(err, AgentIndexError::InvalidArgument(ref message) if message.contains("is not a file")),
            "unexpected error: {err:?}"
        );
        let (children, _, _) = fs.list("/w/input/dir", None, 10).unwrap();
        assert_eq!(children.len(), 1, "directory subtree must stay intact");
    }

    #[test]
    fn put_file_rejects_file_ancestor() {
        let fs = fs();
        fs.put_file("/w/input/a", b"body".to_vec(), None).unwrap();

        let err = fs
            .put_file("/w/input/a/b.txt", b"x".to_vec(), None)
            .unwrap_err();

        assert!(
            matches!(err, AgentIndexError::InvalidArgument(ref message) if message.contains("is not a directory")),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            fs.read_file("/w/input/a").unwrap(),
            b"body".to_vec(),
            "original file body must stay readable"
        );
    }

    #[test]
    fn create_dir_all_rejects_file_component() {
        let fs = fs();
        fs.put_file("/w/f", b"body".to_vec(), None).unwrap();

        let nested = fs.create_dir_all("/w/f/sub").unwrap_err();
        assert!(
            matches!(nested, AgentIndexError::InvalidArgument(ref message) if message.contains("is not a directory")),
            "unexpected error: {nested:?}"
        );
        let direct = fs.create_dir_all("/w/f").unwrap_err();
        assert!(
            matches!(direct, AgentIndexError::InvalidArgument(ref message) if message.contains("is not a directory")),
            "unexpected error: {direct:?}"
        );
        assert_eq!(fs.read_file("/w/f").unwrap(), b"body".to_vec());
    }

    #[test]
    fn put_file_replaces_existing_file() {
        let fs = fs();
        fs.put_file("/w/f", b"old".to_vec(), None).unwrap();
        fs.put_file("/w/f", b"new".to_vec(), None).unwrap();
        assert_eq!(fs.read_file("/w/f").unwrap(), b"new".to_vec());
    }

    #[test]
    fn files_under_root_recursive_finds_files() {
        let fs = fs();
        fs.put_file("/a/b.txt", b"1".to_vec(), None).unwrap();
        fs.put_file("/c.txt", b"2".to_vec(), None).unwrap();

        let mut paths = fs
            .files_under("/", true)
            .unwrap()
            .into_iter()
            .map(|node| node.path)
            .collect::<Vec<_>>();
        paths.sort();

        assert_eq!(paths, vec!["/a/b.txt".to_owned(), "/c.txt".to_owned()]);
    }
}
