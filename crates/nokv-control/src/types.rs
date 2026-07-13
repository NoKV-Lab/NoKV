use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ShardId(String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardState {
    Unassigned,
    Recovering,
    Serving,
    Draining,
    ReadOnly,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub object_key: String,
    pub lsn: u64,
    pub image_bytes: u64,
    /// SHA-256 identity of the exact checkpoint image. Empty only when decoding
    /// a pre-fence control record; recovery must reject those legacy refs.
    #[serde(default)]
    pub image_digest: String,
    /// Digest of the logical metadata log at `lsn` (not the image digest).
    pub digest: String,
}

/// A single archived logical-log segment in the shared-log chain.
///
/// The shared log is a *chain* of segments above the latest checkpoint, not a
/// single object. Failover replays every segment whose `last_lsn` is above the
/// checkpoint LSN, in order, so the chain must be enumerable from the control
/// record — a single pointer would silently drop all but the newest segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogSegmentRef {
    pub segment_key: String,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRef {
    /// Ordered (oldest first) segment chain above the latest checkpoint.
    pub segments: Vec<LogSegmentRef>,
    pub durable_lsn: u64,
    pub digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRecord {
    pub shard_id: ShardId,
    pub owner: Option<NodeId>,
    pub epoch: u64,
    pub lease_id: u64,
    pub state: ShardState,
    pub checkpoint: Option<CheckpointRef>,
    pub log: Option<LogRef>,
    pub durable_lsn: u64,
    /// Durable proof that this shard has completed `mark_serving` at least once.
    /// A never-served shard may safely retry Fresh acquisition after a startup
    /// failure; every legacy record defaults to `true` on decode so historical
    /// data is never mistaken for an empty, resurrectable shard.
    #[serde(default = "legacy_record_ever_served")]
    pub ever_served: bool,
    /// Reachable endpoint (host:port) of the current owner, so a client can
    /// route to it. `None` when unowned. (In this deployment the `NodeId` is the
    /// bind address, so this mirrors `owner` while owned.)
    #[serde(default)]
    pub endpoint: Option<String>,
    /// The absolute path prefix this shard owns (derived from `shard_id`). Lets
    /// clients build the longest-prefix routing map from `list_shards`.
    #[serde(default = "default_shard_prefix")]
    pub prefix: String,
    /// Stable shard index, encoded in the high bits of every inode this shard
    /// mints (see `nokv_types::InodeId::shard_index`). The default/root shard is
    /// index 0.
    #[serde(default)]
    pub shard_index: u16,
    /// For a subtree shard, the inode of its namespace subtree root (the dir
    /// `mkdir`-ed at the shard's prefix on this shard during graft registration).
    /// This is the DURABLE graft target: recording it here is the single,
    /// atomic registration point, from which the parent shard's graft dentry can
    /// be (re)created idempotently by `reconcile_grafts`. `None` until a graft is
    /// registered for this shard (and on the default/root shard, which is never a
    /// graft child).
    #[serde(default)]
    pub subtree_root_inode: Option<u64>,
}

fn default_shard_prefix() -> String {
    "/".to_owned()
}

fn legacy_record_ever_served() -> bool {
    true
}

/// Derive the path prefix from a `mount-<n>:<path>` shard id, defaulting to `/`.
fn shard_prefix_from_id(shard_id: &ShardId) -> String {
    shard_id
        .as_str()
        .split_once(':')
        .map(|(_, path)| path)
        .filter(|path| path.starts_with('/'))
        .unwrap_or("/")
        .to_owned()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardLease {
    pub shard_id: ShardId,
    pub owner: NodeId,
    pub epoch: u64,
    pub lease_id: u64,
}

impl ShardId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl NodeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl ShardRecord {
    pub fn unassigned(shard_id: ShardId) -> Self {
        let prefix = shard_prefix_from_id(&shard_id);
        Self {
            shard_id,
            owner: None,
            epoch: 0,
            lease_id: 0,
            state: ShardState::Unassigned,
            checkpoint: None,
            log: None,
            durable_lsn: 0,
            ever_served: false,
            endpoint: None,
            prefix,
            shard_index: 0,
            subtree_root_inode: None,
        }
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
