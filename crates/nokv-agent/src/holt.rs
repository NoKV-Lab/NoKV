use std::path::Path;

use holt::{RangeEntry, Tree, TreeConfig};

use crate::store::validate_scan_limit;
use crate::{
    AgentBatch, AgentCommit, AgentIndexError, AgentIndexResult, AgentMutation, AgentScanItem,
    AgentScanPage, AgentStore, ScanDirection,
};

#[derive(Clone)]
pub struct HoltAgentStore {
    tree: Tree,
}

impl HoltAgentStore {
    pub fn open(path: impl AsRef<Path>) -> AgentIndexResult<Self> {
        Self::open_with_config(TreeConfig::new(path.as_ref()))
    }

    pub fn open_with_config(config: TreeConfig) -> AgentIndexResult<Self> {
        let tree = Tree::open(config).map_err(store_error)?;
        Ok(Self { tree })
    }

    #[cfg(test)]
    pub(crate) fn open_memory() -> AgentIndexResult<Self> {
        Self::open_with_config(TreeConfig::memory())
    }

    pub fn checkpoint(&self) -> AgentIndexResult<()> {
        self.tree.checkpoint().map_err(store_error)
    }
}

impl AgentStore for HoltAgentStore {
    fn get(&self, key: &[u8]) -> AgentIndexResult<Option<Vec<u8>>> {
        self.tree.get(key).map_err(store_error)
    }

    fn scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
        direction: ScanDirection,
    ) -> AgentIndexResult<AgentScanPage> {
        validate_scan_limit(limit)?;
        match direction {
            ScanDirection::Asc => self.scan_asc(prefix, cursor, limit),
            ScanDirection::Desc => self.scan_desc(prefix, cursor, limit),
        }
    }

    fn apply(&self, batch: AgentBatch) -> AgentIndexResult<AgentCommit> {
        let mutation_count = batch.len();
        let committed = self
            .tree
            .atomic(|holt_batch| {
                for mutation in batch.into_mutations() {
                    match mutation {
                        AgentMutation::Put { key, value } => holt_batch.put(&key, &value),
                        AgentMutation::Delete { key } => holt_batch.delete(&key),
                    }
                }
            })
            .map_err(store_error)?;
        if !committed {
            return Err(AgentIndexError::Conflict(
                "holt rejected agent index batch".to_owned(),
            ));
        }
        Ok(AgentCommit { mutation_count })
    }
}

impl HoltAgentStore {
    fn scan_asc(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> AgentIndexResult<AgentScanPage> {
        let mut builder = self.tree.scan(prefix);
        if let Some(cursor) = cursor {
            builder = builder.start_after(cursor);
        }
        let mut items = Vec::new();
        let mut next_cursor = None;
        for entry in builder {
            let RangeEntry::Key { key, value, .. } = entry.map_err(store_error)? else {
                continue;
            };
            if items.len() == limit {
                next_cursor = items.last().map(|item: &AgentScanItem| item.key.clone());
                break;
            }
            items.push(AgentScanItem { key, value });
        }
        Ok(AgentScanPage {
            truncated: next_cursor.is_some(),
            items,
            next_cursor,
        })
    }

    fn scan_desc(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> AgentIndexResult<AgentScanPage> {
        let mut all = Vec::new();
        for entry in self.tree.scan(prefix) {
            let RangeEntry::Key { key, value, .. } = entry.map_err(store_error)? else {
                continue;
            };
            all.push(AgentScanItem { key, value });
        }

        let mut items = Vec::new();
        let mut next_cursor = None;
        for item in all.into_iter().rev() {
            if cursor.is_some_and(|cursor| item.key.as_slice() >= cursor) {
                continue;
            }
            if items.len() == limit {
                next_cursor = items.last().map(|item: &AgentScanItem| item.key.clone());
                break;
            }
            items.push(item);
        }
        Ok(AgentScanPage {
            truncated: next_cursor.is_some(),
            items,
            next_cursor,
        })
    }
}

fn store_error(err: holt::Error) -> AgentIndexError {
    AgentIndexError::Store(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(page: &AgentScanPage) -> Vec<String> {
        page.items
            .iter()
            .map(|item| String::from_utf8_lossy(&item.key).to_string())
            .collect()
    }

    #[test]
    fn scans_prefix_in_both_directions() {
        let store = HoltAgentStore::open_memory().unwrap();
        let mut batch = AgentBatch::new();
        batch.put(b"a/001".to_vec(), b"one".to_vec());
        batch.put(b"a/002".to_vec(), b"two".to_vec());
        batch.put(b"b/001".to_vec(), b"other".to_vec());
        store.apply(batch).unwrap();

        let asc = store.scan(b"a/", None, 10, ScanDirection::Asc).unwrap();
        assert_eq!(keys(&asc), vec!["a/001", "a/002"]);

        let desc = store.scan(b"a/", None, 10, ScanDirection::Desc).unwrap();
        assert_eq!(keys(&desc), vec!["a/002", "a/001"]);
    }

    #[test]
    fn persists_committed_batch_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-index");
        {
            let store = HoltAgentStore::open(&path).unwrap();
            let mut batch = AgentBatch::new();
            batch.put(b"event/a/00000000000000000001".to_vec(), b"one".to_vec());
            batch.put(b"event/a/00000000000000000002".to_vec(), b"two".to_vec());
            assert_eq!(store.apply(batch).unwrap().mutation_count, 2);
            store.checkpoint().unwrap();
        }

        let reopened = HoltAgentStore::open(&path).unwrap();
        assert_eq!(
            reopened
                .get(b"event/a/00000000000000000001")
                .unwrap()
                .as_deref(),
            Some(b"one".as_slice())
        );
    }
}
