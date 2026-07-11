//! Metadata family <-> Holt tree-name mapping, split out of holtstore.rs.

use nokv_types::RecordFamily;

pub(super) const SYSTEM_CURRENT_TREE: &str = "system_current";
pub(super) const MOUNT_CURRENT_TREE: &str = "mount_current";
pub(super) const INODE_CURRENT_TREE: &str = "inode_current";
pub(super) const DENTRY_CURRENT_TREE: &str = "dentry_current";
pub(super) const PARENT_CURRENT_TREE: &str = "parent_current";
pub(super) const XATTR_CURRENT_TREE: &str = "xattr_current";
pub(super) const CHUNK_MANIFEST_CURRENT_TREE: &str = "chunk_manifest_current";
pub(super) const SESSION_CURRENT_TREE: &str = "session_current";
pub(super) const PATH_INDEX_CURRENT_TREE: &str = "path_index_current";
pub(super) const WATCH_CURRENT_TREE: &str = "watch_current";
pub(super) const SNAPSHOT_CURRENT_TREE: &str = "snapshot_current";
pub(super) const GC_CURRENT_TREE: &str = "gc_current";
pub(super) const COMMAND_DEDUPE_CURRENT_TREE: &str = "command_dedupe_current";
pub(super) const FORK_BINDING_CURRENT_TREE: &str = "fork_binding_current";
pub(super) const FORK_SHADOW_CURRENT_TREE: &str = "fork_shadow_current";
pub(super) const HISTORY_TREE: &str = "history";
pub(super) const HISTORY_KEY_INDEX_TREE: &str = "history_key_index";
pub(super) const METADATA_TREE_NAMES: &[&str] = &[
    SYSTEM_CURRENT_TREE,
    MOUNT_CURRENT_TREE,
    INODE_CURRENT_TREE,
    DENTRY_CURRENT_TREE,
    PARENT_CURRENT_TREE,
    XATTR_CURRENT_TREE,
    CHUNK_MANIFEST_CURRENT_TREE,
    SESSION_CURRENT_TREE,
    PATH_INDEX_CURRENT_TREE,
    WATCH_CURRENT_TREE,
    SNAPSHOT_CURRENT_TREE,
    GC_CURRENT_TREE,
    COMMAND_DEDUPE_CURRENT_TREE,
    FORK_BINDING_CURRENT_TREE,
    FORK_SHADOW_CURRENT_TREE,
    HISTORY_TREE,
    HISTORY_KEY_INDEX_TREE,
];

pub(super) fn current_tree_name(family: RecordFamily) -> &'static str {
    match family {
        RecordFamily::System => SYSTEM_CURRENT_TREE,
        RecordFamily::Mount => MOUNT_CURRENT_TREE,
        RecordFamily::Inode => INODE_CURRENT_TREE,
        RecordFamily::Dentry => DENTRY_CURRENT_TREE,
        RecordFamily::Parent => PARENT_CURRENT_TREE,
        RecordFamily::Xattr => XATTR_CURRENT_TREE,
        RecordFamily::ChunkManifest => CHUNK_MANIFEST_CURRENT_TREE,
        RecordFamily::Session => SESSION_CURRENT_TREE,
        RecordFamily::PathIndex => PATH_INDEX_CURRENT_TREE,
        RecordFamily::Watch => WATCH_CURRENT_TREE,
        RecordFamily::Snapshot => SNAPSHOT_CURRENT_TREE,
        RecordFamily::Gc => GC_CURRENT_TREE,
        RecordFamily::CommandDedupe => COMMAND_DEDUPE_CURRENT_TREE,
        RecordFamily::ForkBinding => FORK_BINDING_CURRENT_TREE,
        RecordFamily::ForkShadow => FORK_SHADOW_CURRENT_TREE,
        RecordFamily::History => HISTORY_TREE,
    }
}

pub(super) fn family_requires_history(family: RecordFamily) -> bool {
    !matches!(
        family,
        RecordFamily::System
            | RecordFamily::CommandDedupe
            | RecordFamily::Watch
            | RecordFamily::Gc
            | RecordFamily::ForkBinding
            | RecordFamily::ForkShadow
    )
}
