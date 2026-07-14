//! Holt-friendly key layout for NoKV metadata.
//!
//! This crate owns ordered keys and family-local prefixes. It does not own
//! namespace semantics, metadata execution, Holt tree handles, Raft state, or
//! object-store references.

mod codec;

use nokv_types::{DentryName, InodeId, MountId, RecordFamily};

pub const U64_WIDTH: usize = 8;
const U32_WIDTH: usize = 4;
pub const PATH_INDEX_DELIMITER: u8 = b'/';

pub use codec::{
    decode_allocator_state, decode_body_descriptor, decode_chunk_manifest,
    decode_dentry_projection, decode_fork_binding, decode_fork_shadow, decode_inode_attr,
    decode_object_gc_record, decode_path_index_catalog, decode_path_index_row, decode_snapshot_pin,
    decode_watch_event, encode_allocator_state, encode_body_descriptor, encode_chunk_manifest,
    encode_dentry_projection, encode_fork_binding, encode_fork_shadow, encode_inode_attr,
    encode_object_gc_record, encode_path_index_catalog, encode_path_index_row, encode_snapshot_pin,
    encode_watch_event, CodecError, PathIndexCatalogRecord, PathIndexFieldRecord,
    PathIndexRowRecord, PathIndexValueRecord,
};

pub fn allocator_key(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 9);
    push_u64(&mut out, mount.get());
    out.extend_from_slice(b"allocator");
    out
}

pub fn object_gc_claim_key(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 16);
    push_u64(&mut out, mount.get());
    out.extend_from_slice(b"object-gc-claim\0");
    out
}

pub fn object_gc_scan_cursor_key(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 22);
    push_u64(&mut out, mount.get());
    out.extend_from_slice(b"object-gc-scan-cursor\0");
    out
}

pub fn failover_durability_required_key(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 29);
    push_u64(&mut out, mount.get());
    out.extend_from_slice(b"failover-durability-required\0");
    out
}

pub fn object_gc_quarantine_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 21);
    push_u64(&mut out, mount.get());
    out.extend_from_slice(b"object-gc-quarantine\0");
    out
}

pub fn object_gc_quarantine_key(mount: MountId, record_digest: &[u8; 32]) -> Vec<u8> {
    let mut out = object_gc_quarantine_prefix(mount);
    out.extend_from_slice(record_digest);
    out
}

pub fn inode_key(mount: MountId, inode: InodeId) -> Vec<u8> {
    let mut out = inode_prefix(mount);
    push_u64(&mut out, inode.get());
    out
}

pub fn inode_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 2);
    push_u64(&mut out, mount.get());
    out
}

pub fn dentry_prefix(mount: MountId, parent: InodeId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 2);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, parent.get());
    out
}

pub fn dentry_mount_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH);
    push_u64(&mut out, mount.get());
    out
}

pub fn dentry_key(mount: MountId, parent: InodeId, name: &DentryName) -> Vec<u8> {
    let mut out = dentry_prefix(mount, parent);
    out.extend_from_slice(name.as_bytes());
    out
}

pub fn path_index_catalog_key(mount: MountId, path: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 9 + path.len());
    push_u64(&mut out, mount.get());
    out.extend_from_slice(b"catalog\0");
    out.extend_from_slice(path.as_bytes());
    out
}

pub fn path_index_row_prefix(mount: MountId, root_path: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 5 + root_path.len() + 1);
    push_u64(&mut out, mount.get());
    out.extend_from_slice(b"row\0");
    out.extend_from_slice(root_path.as_bytes());
    out.push(0);
    out
}

pub fn path_index_row_key(mount: MountId, root_path: &str, row_path: &str) -> Vec<u8> {
    let mut out = path_index_row_prefix(mount, root_path);
    out.extend_from_slice(row_path.as_bytes());
    out
}

pub fn parent_key(mount: MountId, child: InodeId, parent: InodeId, name: &DentryName) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 3 + name.as_bytes().len());
    push_u64(&mut out, mount.get());
    push_u64(&mut out, child.get());
    push_u64(&mut out, parent.get());
    out.extend_from_slice(name.as_bytes());
    out
}

pub fn xattr_prefix(mount: MountId, inode: InodeId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 2);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, inode.get());
    out
}

pub fn xattr_key(mount: MountId, inode: InodeId, name: &[u8]) -> Vec<u8> {
    let mut out = xattr_prefix(mount, inode);
    out.extend_from_slice(name);
    out
}

pub fn path_index_key(mount: MountId, components: &[DentryName]) -> Vec<u8> {
    let mut out = path_index_root_prefix(mount);
    for (index, component) in components.iter().enumerate() {
        if index > 0 {
            out.push(PATH_INDEX_DELIMITER);
        }
        out.extend_from_slice(component.as_bytes());
    }
    out
}

pub fn path_index_prefix(mount: MountId, components: &[DentryName]) -> Vec<u8> {
    let mut out = path_index_key(mount, components);
    if !out.ends_with(&[PATH_INDEX_DELIMITER]) {
        out.push(PATH_INDEX_DELIMITER);
    }
    out
}

fn path_index_root_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 1);
    push_u64(&mut out, mount.get());
    out.push(PATH_INDEX_DELIMITER);
    out
}

pub fn chunk_manifest_prefix(mount: MountId, inode: InodeId, generation: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 3);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, inode.get());
    push_u64(&mut out, generation);
    out
}

pub fn chunk_manifest_key(
    mount: MountId,
    inode: InodeId,
    generation: u64,
    chunk_index: u64,
) -> Vec<u8> {
    let mut out = chunk_manifest_prefix(mount, inode, generation);
    push_u64(&mut out, chunk_index);
    out
}

pub fn watch_log_key(mount: MountId, scope: InodeId, apply_index: u64, event_id: u64) -> Vec<u8> {
    let mut out = watch_log_prefix(mount, scope);
    push_u64(&mut out, apply_index);
    push_u64(&mut out, event_id);
    out
}

pub fn watch_log_prefix(mount: MountId, scope: InodeId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 2);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, scope.get());
    out
}

pub fn snapshot_pin_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH);
    push_u64(&mut out, mount.get());
    out
}

pub fn snapshot_pin_key(mount: MountId, snapshot_id: u64) -> Vec<u8> {
    let mut out = snapshot_pin_prefix(mount);
    push_u64(&mut out, snapshot_id);
    out
}

pub fn fork_binding_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH);
    push_u64(&mut out, mount.get());
    out
}

pub fn fork_binding_key(mount: MountId, fork_root: InodeId) -> Vec<u8> {
    let mut out = fork_binding_prefix(mount);
    push_u64(&mut out, fork_root.get());
    out
}

pub fn fork_shadow_key(mount: MountId, fork_inode: InodeId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 2);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, fork_inode.get());
    out
}

pub fn gc_queue_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH);
    push_u64(&mut out, mount.get());
    out
}

pub fn gc_object_key(
    mount: MountId,
    enqueue_version: u64,
    inode: InodeId,
    generation: u64,
    chunk_index: u64,
    block_index: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 6);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, enqueue_version);
    push_u64(&mut out, inode.get());
    push_u64(&mut out, generation);
    push_u64(&mut out, chunk_index);
    push_u64(&mut out, block_index);
    out
}

pub fn history_key(family: RecordFamily, user_key: &[u8], commit_version: u64) -> Vec<u8> {
    let mut out = history_prefix(family, user_key);
    push_u64(&mut out, u64::MAX - commit_version);
    out
}

pub fn history_prefix(family: RecordFamily, user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + U32_WIDTH + user_key.len());
    out.push(family_tag(family));
    out.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
    out.extend_from_slice(user_key);
    out
}

/// Key in the derived history candidate index.
///
/// Unlike [`history_key`], this key deliberately omits the user-key length and
/// commit version. The dedicated index tree already separates it from history
/// records, so `[family_tag][user_key]` preserves the user's lexical ordering
/// and permits efficient prefix scans for snapshot directory enumeration.
pub fn history_index_key(family: RecordFamily, user_key: &[u8]) -> Vec<u8> {
    history_index_prefix(family, user_key)
}

pub fn history_index_prefix(family: RecordFamily, user_key_prefix: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + user_key_prefix.len());
    out.push(family_tag(family));
    out.extend_from_slice(user_key_prefix);
    out
}

pub fn family_tag(family: RecordFamily) -> u8 {
    match family {
        RecordFamily::Mount => 1,
        RecordFamily::Inode => 2,
        RecordFamily::Dentry => 3,
        RecordFamily::Parent => 4,
        RecordFamily::Xattr => 5,
        RecordFamily::ChunkManifest => 6,
        RecordFamily::Session => 7,
        RecordFamily::PathIndex => 8,
        RecordFamily::Watch => 9,
        RecordFamily::Snapshot => 10,
        RecordFamily::Gc => 11,
        RecordFamily::CommandDedupe => 12,
        RecordFamily::History => 13,
        RecordFamily::System => 14,
        RecordFamily::ForkBinding => 15,
        RecordFamily::ForkShadow => 16,
    }
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_types::{WatchEvent, WatchEventKind};

    fn mount() -> MountId {
        MountId::new(7).unwrap()
    }

    fn inode(id: u64) -> InodeId {
        InodeId::new(id).unwrap()
    }

    fn name(raw: &[u8]) -> DentryName {
        DentryName::new(raw.to_vec()).unwrap()
    }

    #[test]
    fn allocator_key_is_mount_scoped() {
        let key = allocator_key(mount());
        assert!(key.starts_with(&mount().get().to_be_bytes()));
        assert_ne!(key, allocator_key(MountId::new(8).unwrap()));
    }

    #[test]
    fn allocator_state_codec_round_trips_with_epoch() {
        let encoded = encode_allocator_state(42, 99, 7);
        assert_eq!(encoded.len(), U64_WIDTH * 3);
        assert_eq!(decode_allocator_state(&encoded).unwrap(), (42, 99, 7));

        let mut trailing = encoded;
        trailing.push(1);
        assert_eq!(
            decode_allocator_state(&trailing).unwrap_err(),
            CodecError::TrailingBytes
        );
    }

    #[test]
    fn allocator_state_codec_rejects_missing_epoch() {
        let mut missing_epoch = Vec::new();
        missing_epoch.extend_from_slice(&42u64.to_be_bytes());
        missing_epoch.extend_from_slice(&99u64.to_be_bytes());
        assert_eq!(missing_epoch.len(), U64_WIDTH * 2);
        assert_eq!(
            decode_allocator_state(&missing_epoch).unwrap_err(),
            CodecError::Truncated
        );

        let mut truncated = missing_epoch;
        truncated.extend_from_slice(&[0u8; 4]);
        assert_eq!(
            decode_allocator_state(&truncated).unwrap_err(),
            CodecError::Truncated
        );
    }

    #[test]
    fn watch_event_codec_preserves_typed_event_fields() {
        let event = WatchEvent {
            kind: WatchEventKind::PublishArtifact,
            parent: Some(inode(9)),
            name: Some(name(b"checkpoint.bin")),
            inode: inode(10),
            version: 42,
        };
        let encoded = encode_watch_event(&event);
        assert_eq!(decode_watch_event(&encoded).unwrap(), event);
    }

    #[test]
    fn dentry_keys_for_one_parent_share_a_contiguous_prefix() {
        let prefix = dentry_prefix(mount(), inode(9));
        let a = dentry_key(mount(), inode(9), &name(b"a"));
        let b = dentry_key(mount(), inode(9), &name(b"b"));
        let other_parent = dentry_key(mount(), inode(10), &name(b"a"));

        assert!(a.starts_with(&prefix));
        assert!(b.starts_with(&prefix));
        assert!(!other_parent.starts_with(&prefix));
        assert!(a < b);
    }

    #[test]
    fn big_endian_ids_keep_numeric_order() {
        assert!(inode_key(mount(), inode(2)) < inode_key(mount(), inode(10)));
    }

    #[test]
    fn path_index_keys_are_component_prefix_safe() {
        let parent = path_index_prefix(mount(), &[name(b"runs")]);
        let parent_exact = path_index_key(mount(), &[name(b"runs")]);
        let child = path_index_key(mount(), &[name(b"runs"), name(b"ckpt")]);
        let sibling_prefix = path_index_key(mount(), &[name(b"runs-long")]);

        assert!(child.starts_with(&parent));
        assert!(!parent_exact.ends_with(&[PATH_INDEX_DELIMITER]));
        assert!(!sibling_prefix.starts_with(&parent));
    }

    #[test]
    fn xattr_keys_for_one_inode_share_a_contiguous_prefix() {
        let prefix = xattr_prefix(mount(), inode(9));
        let a = xattr_key(mount(), inode(9), b"user.a");
        let b = xattr_key(mount(), inode(9), b"user.b");
        let other_inode = xattr_key(mount(), inode(10), b"user.a");

        assert!(a.starts_with(&prefix));
        assert!(b.starts_with(&prefix));
        assert!(!other_inode.starts_with(&prefix));
        assert!(a < b);
    }

    #[test]
    fn inode_keys_for_one_mount_share_a_prefix() {
        let prefix = inode_prefix(mount());
        let key = inode_key(mount(), inode(42));
        let other_mount = inode_key(MountId::new(8).unwrap(), inode(42));
        assert!(key.starts_with(&prefix));
        assert!(!other_mount.starts_with(&prefix));
    }

    #[test]
    fn history_key_orders_newer_versions_first_for_same_user_key() {
        let key = inode_key(mount(), inode(2));
        let newer = history_key(RecordFamily::Inode, &key, 100);
        let older = history_key(RecordFamily::Inode, &key, 90);
        assert!(newer < older);
    }

    #[test]
    fn gc_keys_are_mount_and_version_ordered() {
        let key = gc_object_key(mount(), 10, inode(2), 3, 4, 5);
        let other_mount = gc_object_key(MountId::new(8).unwrap(), 10, inode(2), 3, 4, 5);
        let later = gc_object_key(mount(), 11, inode(2), 3, 4, 5);

        assert!(key.starts_with(&gc_queue_prefix(mount())));
        assert!(!other_mount.starts_with(&gc_queue_prefix(mount())));
        assert!(key < later);
    }

    #[test]
    fn object_gc_control_keys_are_mount_scoped_and_distinct() {
        let claim = object_gc_claim_key(mount());
        let cursor = object_gc_scan_cursor_key(mount());
        let failover = failover_durability_required_key(mount());
        let quarantine = object_gc_quarantine_key(mount(), &[7; 32]);
        let other_claim = object_gc_claim_key(MountId::new(8).unwrap());

        assert_ne!(claim, cursor);
        assert_ne!(claim, failover);
        assert_ne!(cursor, failover);
        assert_ne!(claim, other_claim);
        assert!(quarantine.starts_with(&object_gc_quarantine_prefix(mount())));
    }

    #[test]
    fn snapshot_pin_keys_are_mount_scoped() {
        let key = snapshot_pin_key(mount(), 10);
        let other_mount = snapshot_pin_key(MountId::new(8).unwrap(), 10);
        let later = snapshot_pin_key(mount(), 11);

        assert!(key.starts_with(&snapshot_pin_prefix(mount())));
        assert!(!other_mount.starts_with(&snapshot_pin_prefix(mount())));
        assert!(key < later);
    }

    #[test]
    fn watch_log_keys_are_scope_and_cursor_ordered() {
        let key = watch_log_key(mount(), inode(2), 10, 0);
        let later = watch_log_key(mount(), inode(2), 10, 1);
        let other_scope = watch_log_key(mount(), inode(3), 10, 0);

        assert!(key.starts_with(&watch_log_prefix(mount(), inode(2))));
        assert!(!other_scope.starts_with(&watch_log_prefix(mount(), inode(2))));
        assert!(key < later);
    }

    #[test]
    fn history_prefix_is_exact_for_user_key() {
        let a = history_prefix(RecordFamily::Dentry, b"a");
        let aa = history_prefix(RecordFamily::Dentry, b"aa");
        assert!(!aa.starts_with(&a));
    }

    #[test]
    fn history_index_key_preserves_family_local_user_prefix_order() {
        let prefix = history_index_prefix(RecordFamily::Dentry, b"dir/");
        let a = history_index_key(RecordFamily::Dentry, b"dir/a");
        let b = history_index_key(RecordFamily::Dentry, b"dir/b");
        let other = history_index_key(RecordFamily::Inode, b"dir/a");

        assert!(a.starts_with(&prefix));
        assert!(b.starts_with(&prefix));
        assert!(a < b);
        assert!(!other.starts_with(&prefix));
    }
}
