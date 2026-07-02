use crate::{AgentIndexError, AgentIndexResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentMutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentBatch {
    mutations: Vec<AgentMutation>,
}

impl AgentBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.mutations.push(AgentMutation::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.mutations
            .push(AgentMutation::Delete { key: key.into() });
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    pub fn into_mutations(self) -> Vec<AgentMutation> {
        self.mutations
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentCommit {
    pub mutation_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentScanItem {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentScanPage {
    pub items: Vec<AgentScanItem>,
    pub next_cursor: Option<Vec<u8>>,
    pub truncated: bool,
}

pub trait AgentStore: Clone + Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> AgentIndexResult<Option<Vec<u8>>>;

    fn scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
        direction: ScanDirection,
    ) -> AgentIndexResult<AgentScanPage>;

    fn apply(&self, batch: AgentBatch) -> AgentIndexResult<AgentCommit>;
}

pub(crate) fn validate_scan_limit(limit: usize) -> AgentIndexResult<()> {
    if limit == 0 {
        return Err(AgentIndexError::InvalidArgument(
            "scan limit must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}
