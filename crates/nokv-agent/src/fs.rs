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
        add_directory_ancestors(&mut batch, &self.agent_id, &path)?;
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
        let parent = parent_path(&path)?;
        let name = path_name(&path)?;
        let mut batch = AgentBatch::new();
        add_directory_ancestors(&mut batch, &self.agent_id, &parent)?;
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
        Ok(self
            .scan_all(&node_prefix(&self.agent_id))?
            .into_iter()
            .filter_map(|item| decode_json::<AgentNode>(&item.value).ok())
            .filter(|node| {
                node.kind == AgentNodeKind::File
                    && (node.path == path || node.path.starts_with(&format!("{path}/")))
            })
            .collect())
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

fn add_directory_ancestors(
    batch: &mut AgentBatch,
    agent_id: &AgentId,
    path: &str,
) -> AgentIndexResult<()> {
    let path = normalize_path(path)?;
    let mut current = String::from("/");
    put_node(
        batch,
        agent_id,
        AgentNode {
            path: current.clone(),
            name: "/".to_owned(),
            kind: AgentNodeKind::Directory,
            size_bytes: None,
            content_type: None,
        },
    )?;
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
        put_node(
            batch,
            agent_id,
            AgentNode {
                path: current.clone(),
                name: part.to_owned(),
                kind: AgentNodeKind::Directory,
                size_bytes: None,
                content_type: None,
            },
        )?;
        batch.put(child_key(agent_id, &parent, part), current.as_bytes());
    }
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
