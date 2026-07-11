use fuser::Errno;
use nokv_meta::MetadError;

use crate::backend::FuseBackendError;

pub(super) fn errno(err: impl Into<FuseBackendError>) -> Errno {
    classify(&err.into())
}

fn classify(err: &FuseBackendError) -> Errno {
    match err {
        FuseBackendError::Metadata(e) => metadata_errno(e),
        FuseBackendError::Client(nokv_client::ClientError::Metadata(e)) => metadata_errno(e),
        FuseBackendError::Client(nokv_client::ClientError::NotFound(_)) => Errno::ENOENT,
        FuseBackendError::Client(nokv_client::ClientError::LockConflict(_)) => Errno::EAGAIN,
        // `EIO` is the lossy bucket: object-upload failures and raw IO collapse
        // into it and otherwise reach userspace as an opaque `Input/output
        // error`. Surface the real cause on the mount's stderr (captured in the
        // mount log) so write-path failures are diagnosable instead of silent.
        FuseBackendError::Client(nokv_client::ClientError::Object(_))
        | FuseBackendError::Client(nokv_client::ClientError::Io(_))
        | FuseBackendError::Client(nokv_client::ClientError::Protocol(_))
        | FuseBackendError::Client(nokv_client::ClientError::EmptyPath)
        | FuseBackendError::Client(nokv_client::ClientError::RelativePath)
        | FuseBackendError::Client(nokv_client::ClientError::ParentTraversal)
        | FuseBackendError::Client(nokv_client::ClientError::InvalidArtifactPath(_))
        | FuseBackendError::Client(nokv_client::ClientError::ArtifactIsDirectory(_))
        | FuseBackendError::Client(nokv_client::ClientError::ArtifactIsFile(_))
        | FuseBackendError::Client(nokv_client::ClientError::InvalidName(_))
        | FuseBackendError::Client(nokv_client::ClientError::RootHasNoParent)
        | FuseBackendError::Object(_) => {
            eprintln!("nokv-fuse: operation failed -> EIO: {err:?}");
            Errno::EIO
        }
    }
}

fn metadata_errno(err: &MetadError) -> Errno {
    match err {
        MetadError::Model(_) => Errno::EINVAL,
        MetadError::InvalidPath(_) => Errno::EINVAL,
        MetadError::InvalidQuery(_) => Errno::EINVAL,
        MetadError::NotFound => Errno::ENOENT,
        MetadError::NotFile => Errno::EISDIR,
        MetadError::NotDirectory => Errno::ENOTDIR,
        MetadError::DirectoryNotEmpty => Errno::ENOTEMPTY,
        MetadError::CannotRemoveRoot => Errno::EBUSY,
        MetadError::StaleBodyGeneration { .. } => Errno::ESTALE,
        MetadError::StaleOwnerEpoch { .. } => Errno::ESTALE,
        // The owner self-fenced on its lease deadline; the client should
        // re-resolve the shard owner, same as a stale epoch.
        MetadError::LeaseExpired { .. } => Errno::ESTALE,
        // The addressed shard moved off this node; re-resolve the owner and
        // retry, same client-side handling as a stale epoch.
        MetadError::NotOwner { .. } => Errno::ESTALE,
        // A rename/hardlink/clone crossed a shard boundary. POSIX cross-device
        // semantics: userspace falls back to copy+unlink on EXDEV.
        MetadError::CrossShard { .. } => Errno::EXDEV,
        // Attempted to remove/rename a cross-shard graft point. EBUSY (the entry
        // is a live mount point), not EXDEV — there is no copy+unlink fallback
        // that would correctly tear down the graft.
        MetadError::GraftPoint => Errno::EBUSY,
        MetadError::SnapshotLeaseExpired { .. } | MetadError::SnapshotRootMismatch { .. } => {
            Errno::ESTALE
        }
        MetadError::SnapshotBindingChanged { .. } | MetadError::SnapshotRenewContended { .. } => {
            Errno::EAGAIN
        }
        MetadError::LockConflict(_) => Errno::EAGAIN,
        // See the note above: surface the underlying metadata/object/allocator
        // failure before it collapses into an opaque `EIO`.
        MetadError::MissingBodyDescriptor
        | MetadError::Metadata(_)
        | MetadError::Object(_)
        | MetadError::PublishArtifactFailed { .. }
        | MetadError::Codec(_)
        | MetadError::BodySizeMismatch { .. }
        | MetadError::InvalidPreparedArtifact(_)
        | MetadError::InvalidOwnerEpoch
        | MetadError::SyncLogArchiveFailed { .. }
        | MetadError::AllocatorExhausted => {
            eprintln!("nokv-fuse: metadata operation failed -> EIO: {err:?}");
            Errno::EIO
        }
        // MetadError is intentionally non-exhaustive across crate boundaries.
        // Feature-specific errors stay fail-closed until their own FUSE change
        // defines the correct POSIX mapping.
        _ => {
            eprintln!("nokv-fuse: unsupported metadata error -> EIO: {err:?}");
            Errno::EIO
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_shard_maps_to_exdev() {
        // A cross-shard rename/hardlink/clone surfaces as EXDEV so userspace can
        // fall back to copy+unlink, matching POSIX cross-device semantics.
        let err = MetadError::CrossShard {
            source_shard: 1,
            dest_shard: 0,
        };
        // `Errno` is not `PartialEq`; compare the raw codes.
        assert_eq!(i32::from(metadata_errno(&err)), i32::from(Errno::EXDEV));
    }

    #[test]
    fn graft_point_maps_to_ebusy() {
        // Remove/rename of a cross-shard graft point surfaces as EBUSY (a live
        // mount point), distinct from CrossShard's EXDEV: there is no correct
        // copy+unlink fallback, so userspace must not attempt one.
        assert_eq!(
            i32::from(metadata_errno(&MetadError::GraftPoint)),
            i32::from(Errno::EBUSY)
        );
    }
}
