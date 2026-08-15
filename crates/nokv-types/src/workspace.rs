//! Storage-neutral identifiers and path validation for NoKV workspaces.
//!
//! This module owns the caller-visible identity and normalized-path contract. It
//! deliberately does not own metadata keys, Holt families, routing, or wire
//! encoding.

use std::fmt;
use std::num::NonZeroU64;

/// Width of every fixed-size workspace storage identifier.
pub const FIXED_ID_BYTES: usize = 16;
/// Width of SHA-256-backed identities and digests.
pub const SHA256_BYTES: usize = 32;

/// Globally unique routing identity for one Agent root.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootId([u8; FIXED_ID_BYTES]);

impl RootId {
    pub fn from_bytes(bytes: [u8; FIXED_ID_BYTES]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; FIXED_ID_BYTES] {
        &self.0
    }
}

/// Never-reused identity for one visible or retired workbench incarnation.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceIncarnationId([u8; FIXED_ID_BYTES]);

impl WorkspaceIncarnationId {
    pub fn from_bytes(bytes: [u8; FIXED_ID_BYTES]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; FIXED_ID_BYTES] {
        &self.0
    }
}

/// Immutable artifact-revision identity, unique within one [`RootId`].
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArtifactRevisionId([u8; FIXED_ID_BYTES]);

impl ArtifactRevisionId {
    pub fn from_bytes(bytes: [u8; FIXED_ID_BYTES]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; FIXED_ID_BYTES] {
        &self.0
    }
}

macro_rules! fixed_bytes_type {
    ($(#[$meta:meta])* $name:ident, $width:expr) => {
        $(#[$meta])*
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; $width]);

        impl $name {
            pub const BYTE_WIDTH: usize = $width;

            pub const fn from_bytes(bytes: [u8; $width]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; $width] {
                &self.0
            }

            pub const fn into_bytes(self) -> [u8; $width] {
                self.0
            }
        }
    };
}

fixed_bytes_type!(
    /// Globally unique identity for one persisted logical metadata shard.
    LogicalShardId,
    FIXED_ID_BYTES
);
fixed_bytes_type!(
    /// Never-reused identity for one durable artifact-object namespace.
    ///
    /// Endpoints and credentials are deployment details. This identity names
    /// the durable bucket/prefix contents they resolve to.
    ObjectNamespaceId,
    FIXED_ID_BYTES
);
fixed_bytes_type!(
    /// Never-reused identity for one durable lifecycle operation within a root.
    OperationId,
    FIXED_ID_BYTES
);
fixed_bytes_type!(
    /// Stable request identity used for exact command replay.
    RequestId,
    FIXED_ID_BYTES
);
fixed_bytes_type!(
    /// Root-global SHA-256 identity for one immutable commit.
    CommitId,
    SHA256_BYTES
);
fixed_bytes_type!(
    /// SHA-256 digest of the canonical metadata command input.
    CommandDigest,
    SHA256_BYTES
);

/// Error returned when a durable scalar that must be non-zero is constructed
/// from zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ZeroValueError {
    type_name: &'static str,
}

impl ZeroValueError {
    const fn new(type_name: &'static str) -> Self {
        Self { type_name }
    }

    pub const fn type_name(self) -> &'static str {
        self.type_name
    }
}

impl fmt::Display for ZeroValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} must be non-zero", self.type_name)
    }
}

impl std::error::Error for ZeroValueError {}

macro_rules! non_zero_u64_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(NonZeroU64);

        impl $name {
            pub fn new(value: u64) -> Result<Self, ZeroValueError> {
                NonZeroU64::new(value)
                    .map(Self)
                    .ok_or_else(|| ZeroValueError::new(stringify!($name)))
            }

            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }

        impl TryFrom<u64> for $name {
            type Error = ZeroValueError;

            fn try_from(value: u64) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }
    };
}

non_zero_u64_type!(
    /// Generation of a persisted root placement. Zero is never installed.
    PlacementGeneration
);
non_zero_u64_type!(
    /// Epoch of the current physical shard owner. Zero never owns writes.
    OwnerEpoch
);
non_zero_u64_type!(
    /// Durable MVCC read version. The engine's zero sentinel is not readable.
    ReadVersion
);
non_zero_u64_type!(
    /// Durable version assigned to one committed metadata command.
    CommitVersion
);
non_zero_u64_type!(
    /// Generation of a published path, head, tag, or alias.
    Generation
);

macro_rules! zero_allowed_u64_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            pub const ZERO: Self = Self(0);

            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self::new(value)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }
    };
}

zero_allowed_u64_type!(
    /// Non-negative numeric snapshot id exposed by the Workbench facade.
    SnapshotId
);
zero_allowed_u64_type!(
    /// Workspace-wide mutation revision. A newly-created workspace starts at zero.
    WorkspaceRevision
);
zero_allowed_u64_type!(
    /// Strong-reference epoch. Zero is the initial no-reference epoch.
    ReferenceEpoch
);
zero_allowed_u64_type!(
    /// Commit or snapshot consumer epoch. Zero is the initial no-consumer epoch.
    ConsumerEpoch
);

/// External, case-sensitive workbench name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkbenchId(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkbenchIdError {
    Empty,
    TooLong { bytes: usize, max: usize },
    InvalidStart { byte: u8 },
    InvalidCharacter { index: usize, byte: u8 },
}

impl WorkbenchId {
    pub const MAX_BYTES: usize = 128;

    pub fn new(value: impl Into<String>) -> Result<Self, WorkbenchIdError> {
        let value = value.into();
        let bytes = value.as_bytes();
        let Some(first) = bytes.first().copied() else {
            return Err(WorkbenchIdError::Empty);
        };
        if bytes.len() > Self::MAX_BYTES {
            return Err(WorkbenchIdError::TooLong {
                bytes: bytes.len(),
                max: Self::MAX_BYTES,
            });
        }
        if !first.is_ascii_alphanumeric() {
            return Err(WorkbenchIdError::InvalidStart { byte: first });
        }
        if let Some((index, byte)) = bytes
            .iter()
            .copied()
            .enumerate()
            .find(|(_, byte)| !byte.is_ascii_alphanumeric() && !matches!(byte, b'_' | b'-'))
        {
            return Err(WorkbenchIdError::InvalidCharacter { index, byte });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl AsRef<str> for WorkbenchId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for WorkbenchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for WorkbenchIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "workbench id is empty"),
            Self::TooLong { bytes, max } => {
                write!(f, "workbench id is {bytes} bytes, maximum is {max}")
            }
            Self::InvalidStart { byte } => write!(
                f,
                "workbench id must start with an ASCII letter or digit, found byte 0x{byte:02x}"
            ),
            Self::InvalidCharacter { index, byte } => write!(
                f,
                "workbench id contains invalid byte 0x{byte:02x} at offset {index}"
            ),
        }
    }
}

impl std::error::Error for WorkbenchIdError {}

/// Validation failure for a durable tag or snapshot-alias name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurableNameError {
    InvalidUtf8,
    ContainsNul { index: usize },
    TooLong { bytes: usize, max: usize },
}

impl fmt::Display for DurableNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUtf8 => formatter.write_str("durable name is not valid UTF-8"),
            Self::ContainsNul { index } => {
                write!(
                    formatter,
                    "durable name contains NUL at byte offset {index}"
                )
            }
            Self::TooLong { bytes, max } => {
                write!(formatter, "durable name is {bytes} bytes, maximum is {max}")
            }
        }
    }
}

impl std::error::Error for DurableNameError {}

fn validate_durable_name(bytes: &[u8]) -> Result<&str, DurableNameError> {
    if bytes.len() > TagName::MAX_BYTES {
        return Err(DurableNameError::TooLong {
            bytes: bytes.len(),
            max: TagName::MAX_BYTES,
        });
    }
    let value = std::str::from_utf8(bytes).map_err(|_| DurableNameError::InvalidUtf8)?;
    if let Some(index) = bytes.iter().position(|byte| *byte == 0) {
        return Err(DurableNameError::ContainsNul { index });
    }
    Ok(value)
}

macro_rules! durable_name_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub const MAX_BYTES: usize = 128;

            pub fn new(value: impl Into<String>) -> Result<Self, DurableNameError> {
                let value = value.into();
                validate_durable_name(value.as_bytes())?;
                Ok(Self(value))
            }

            pub fn from_bytes(value: &[u8]) -> Result<Self, DurableNameError> {
                validate_durable_name(value).map(|value| Self(value.to_owned()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl TryFrom<String> for $name {
            type Error = DurableNameError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = DurableNameError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&[u8]> for $name {
            type Error = DurableNameError;

            fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
                Self::from_bytes(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

durable_name_type!(
    /// Case-sensitive UTF-8 name for one durable commit tag.
    TagName
);
durable_name_type!(
    /// Case-sensitive UTF-8 name for the current alias of a leased snapshot.
    SnapshotAliasName
);

/// Fail-closed error for an unknown format-v1 durable enum discriminant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnknownDurableDiscriminant {
    type_name: &'static str,
    value: u8,
}

impl UnknownDurableDiscriminant {
    const fn new(type_name: &'static str, value: u8) -> Self {
        Self { type_name, value }
    }

    pub const fn type_name(self) -> &'static str {
        self.type_name
    }

    pub const fn value(self) -> u8 {
        self.value
    }
}

impl fmt::Display for UnknownDurableDiscriminant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown {} durable discriminant {}",
            self.type_name, self.value
        )
    }
}

impl std::error::Error for UnknownDurableDiscriminant {}

macro_rules! durable_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $value:literal
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[repr(u8)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $(
                $(#[$variant_meta])*
                $variant = $value,
            )+
        }

        impl TryFrom<u8> for $name {
            type Error = UnknownDurableDiscriminant;

            fn try_from(value: u8) -> Result<Self, Self::Error> {
                match value {
                    $(
                        $value => Ok(Self::$variant),
                    )+
                    value => Err(UnknownDurableDiscriminant::new(stringify!($name), value)),
                }
            }
        }

        impl From<$name> for u8 {
            fn from(value: $name) -> Self {
                value as u8
            }
        }
    };
}

durable_enum! {
    /// Durable visibility state for one workspace incarnation.
    pub enum WorkspaceState {
        Staging = 1,
        Visible = 2,
        Retired = 3,
    }
}

durable_enum! {
    /// Durable object-lifetime state for one immutable artifact revision.
    pub enum RevisionState {
        Available = 1,
        Deleting = 2,
        Deleted = 3,
        Quarantined = 4,
    }
}

durable_enum! {
    /// Durable lifecycle of a published immutable commit.
    pub enum CommitState {
        Sealed = 1,
        Retiring = 2,
        Retired = 3,
    }
}

durable_enum! {
    /// Durable lifecycle of a leased snapshot.
    pub enum SnapshotState {
        Active = 1,
        ReapClaimed = 2,
        Reaped = 3,
        Retired = 4,
    }
}

durable_enum! {
    /// Owner kind for one strong artifact-revision reference.
    pub enum ReferenceKind {
        Path = 1,
        Commit = 2,
        RevisionDependency = 3,
    }
}

durable_enum! {
    /// Owner kind for a metadata-history retention hold.
    pub enum HistoryHoldKind {
        Snapshot = 1,
        BuildCommit = 2,
        Restore = 3,
    }
}

durable_enum! {
    /// Durable release state for a metadata-history retention hold.
    pub enum HistoryHoldState {
        Active = 1,
        Releasing = 2,
    }
}

durable_enum! {
    /// Shard-local activation state of one installed root fence.
    pub enum RootActivationState {
        Installing = 1,
        Active = 2,
        Draining = 3,
        Fenced = 4,
    }
}

durable_enum! {
    /// Control-plane lifecycle of one persisted root placement.
    pub enum RootPlacementLifecycle {
        Provisioning = 1,
        Active = 2,
        Draining = 3,
        Retired = 4,
    }
}

durable_enum! {
    /// Owner kind for one exact durable commit consumer.
    pub enum CommitConsumerKind {
        WorkbenchHead = 1,
        Tag = 2,
        Lease = 3,
        ChildCommit = 4,
    }
}

durable_enum! {
    /// Frozen source kind for one restore operation.
    pub enum RestoreSourceKind {
        Snapshot = 1,
        Commit = 2,
    }
}

durable_enum! {
    /// Kind discriminator for one durable lifecycle operation.
    pub enum OperationKind {
        Publish = 1,
        BuildCommit = 2,
        Restore = 3,
        CommitRetire = 4,
        Gc = 5,
    }
}

durable_enum! {
    /// Durable phase of an object-first artifact publication.
    pub enum PublishPhase {
        Uploading = 1,
        Finalizing = 2,
        Published = 3,
        Aborting = 4,
        Cleaning = 5,
        Cleaned = 6,
        Quarantined = 7,
    }
}

durable_enum! {
    /// Durable phase of an immutable commit build.
    pub enum BuildCommitPhase {
        Building = 1,
        Sealing = 2,
        Complete = 3,
        Aborting = 4,
        Cleaning = 5,
        Cleaned = 6,
        Quarantined = 7,
    }
}

durable_enum! {
    /// Durable phase of a destination-creating restore.
    pub enum RestorePhase {
        Preparing = 1,
        Copying = 2,
        SourceSealed = 3,
        Ready = 4,
        Complete = 5,
        Aborting = 6,
        Cleaning = 7,
        Cleaned = 8,
        Quarantined = 9,
    }
}

durable_enum! {
    /// Durable phase of bounded commit retirement.
    pub enum CommitRetirePhase {
        Claiming = 1,
        Releasing = 2,
        Complete = 3,
        Quarantined = 4,
    }
}

durable_enum! {
    /// Durable phase of revision garbage collection.
    pub enum GcPhase {
        Queued = 1,
        Claimed = 2,
        Deleting = 3,
        Deleted = 4,
        Quarantined = 5,
    }
}

durable_enum! {
    /// Provider-observed state for one staged immutable object.
    pub enum StagedProviderState {
        Planned = 1,
        Uploading = 2,
        Uploaded = 3,
        AbortPending = 4,
        Aborted = 5,
        Ambiguous = 6,
    }
}

durable_enum! {
    /// Cleanup ownership state for one staged immutable object.
    pub enum StagedCleanupState {
        Owned = 1,
        DeletePending = 2,
        Deleted = 3,
        Quarantined = 4,
    }
}

durable_enum! {
    /// Claim state of one epoch-keyed revision GC candidate.
    pub enum GcClaimState {
        Candidate = 1,
        Claimed = 2,
        Complete = 3,
        Quarantined = 4,
    }
}

/// Validated, case-sensitive, slash-joined relative Agent path.
///
/// The stored string preserves its exact UTF-8 bytes. `/` is only a component
/// separator; repeated, leading, and trailing separators are rejected instead
/// of being normalized away.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NormalizedRelativePath(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NormalizedRelativePathError {
    Empty,
    Absolute,
    EmptyComponent { index: usize },
    DotComponent { index: usize },
    ParentComponent { index: usize },
    ContainsBackslash { component: usize },
    ContainsNul { component: usize },
    TooLong { bytes: usize, max: usize },
    TooManyComponents { components: usize, max: usize },
}

impl NormalizedRelativePath {
    pub const MAX_BYTES: usize = 4096;
    pub const MAX_COMPONENTS: usize = 64;

    pub fn new(value: impl Into<String>) -> Result<Self, NormalizedRelativePathError> {
        let value = value.into();
        if value.is_empty() {
            return Err(NormalizedRelativePathError::Empty);
        }
        if value.starts_with('/') {
            return Err(NormalizedRelativePathError::Absolute);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(NormalizedRelativePathError::TooLong {
                bytes: value.len(),
                max: Self::MAX_BYTES,
            });
        }

        let mut component_count = 0;
        for (index, component) in value.split('/').enumerate() {
            component_count += 1;
            if component_count > Self::MAX_COMPONENTS {
                return Err(NormalizedRelativePathError::TooManyComponents {
                    components: component_count,
                    max: Self::MAX_COMPONENTS,
                });
            }
            match component {
                "" => return Err(NormalizedRelativePathError::EmptyComponent { index }),
                "." => return Err(NormalizedRelativePathError::DotComponent { index }),
                ".." => return Err(NormalizedRelativePathError::ParentComponent { index }),
                _ => {}
            }
            if component.contains('\\') {
                return Err(NormalizedRelativePathError::ContainsBackslash { component: index });
            }
            if component.contains('\0') {
                return Err(NormalizedRelativePathError::ContainsNul { component: index });
            }
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn components(&self) -> impl Iterator<Item = &str> {
        self.0.split('/')
    }

    pub fn byte_len(&self) -> usize {
        self.0.len()
    }

    pub fn component_count(&self) -> usize {
        self.components().count()
    }
}

impl AsRef<str> for NormalizedRelativePath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for NormalizedRelativePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for NormalizedRelativePathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "relative path is empty"),
            Self::Absolute => write!(f, "relative path must not start with '/'"),
            Self::EmptyComponent { index } => {
                write!(f, "relative path component {index} is empty")
            }
            Self::DotComponent { index } => {
                write!(f, "relative path component {index} is '.'")
            }
            Self::ParentComponent { index } => {
                write!(f, "relative path component {index} is '..'")
            }
            Self::ContainsBackslash { component } => {
                write!(f, "relative path component {component} contains backslash")
            }
            Self::ContainsNul { component } => {
                write!(f, "relative path component {component} contains NUL")
            }
            Self::TooLong { bytes, max } => {
                write!(f, "relative path is {bytes} bytes, maximum is {max}")
            }
            Self::TooManyComponents { components, max } => write!(
                f,
                "relative path has {components} components, maximum is {max}"
            ),
        }
    }
}

impl std::error::Error for NormalizedRelativePathError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_durable_registry<T>(expected: &[(T, u8)])
    where
        T: Copy + fmt::Debug + PartialEq + TryFrom<u8, Error = UnknownDurableDiscriminant>,
        u8: From<T>,
    {
        for (variant, discriminant) in expected {
            assert_eq!(u8::from(*variant), *discriminant);
            assert_eq!(T::try_from(*discriminant).unwrap(), *variant);
        }

        let type_name = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .expect("Rust type names contain one segment");
        for discriminant in u8::MIN..=u8::MAX {
            let expected_variant = expected
                .iter()
                .find_map(|(variant, expected)| (*expected == discriminant).then_some(*variant));
            match (T::try_from(discriminant), expected_variant) {
                (Ok(actual), Some(expected)) => assert_eq!(actual, expected),
                (Err(error), None) => {
                    assert_eq!(error.type_name(), type_name);
                    assert_eq!(error.value(), discriminant);
                }
                (actual, expected) => panic!(
                    "registry mismatch for {type_name} discriminant {discriminant}: \
                     actual={actual:?}, expected={expected:?}"
                ),
            }
        }
    }

    #[test]
    fn fixed_width_ids_round_trip_exact_bytes() {
        let bytes = std::array::from_fn(|index| index as u8);
        let digest_bytes = std::array::from_fn(|index| (index as u8).wrapping_mul(3));

        let root = RootId::from_bytes(bytes);
        let shard = LogicalShardId::from_bytes(bytes);
        let object_namespace = ObjectNamespaceId::from_bytes(bytes);
        let workspace = WorkspaceIncarnationId::from_bytes(bytes);
        let revision = ArtifactRevisionId::from_bytes(bytes);
        let operation = OperationId::from_bytes(bytes);
        let request = RequestId::from_bytes(bytes);
        let commit = CommitId::from_bytes(digest_bytes);
        let command_digest = CommandDigest::from_bytes(digest_bytes);

        assert_eq!(root.as_bytes(), &bytes);
        assert_eq!(shard.as_bytes(), &bytes);
        assert_eq!(object_namespace.as_bytes(), &bytes);
        assert_eq!(workspace.as_bytes(), &bytes);
        assert_eq!(revision.as_bytes(), &bytes);
        assert_eq!(operation.as_bytes(), &bytes);
        assert_eq!(request.as_bytes(), &bytes);
        assert_eq!(commit.as_bytes(), &digest_bytes);
        assert_eq!(command_digest.as_bytes(), &digest_bytes);
        assert_eq!(shard.into_bytes(), bytes);
        assert_eq!(object_namespace.into_bytes(), bytes);
        assert_eq!(commit.into_bytes(), digest_bytes);
        assert_eq!(std::mem::size_of::<RootId>(), FIXED_ID_BYTES);
        assert_eq!(std::mem::size_of::<LogicalShardId>(), FIXED_ID_BYTES);
        assert_eq!(
            std::mem::size_of::<WorkspaceIncarnationId>(),
            FIXED_ID_BYTES
        );
        assert_eq!(std::mem::size_of::<ArtifactRevisionId>(), FIXED_ID_BYTES);
        assert_eq!(std::mem::size_of::<OperationId>(), FIXED_ID_BYTES);
        assert_eq!(std::mem::size_of::<RequestId>(), FIXED_ID_BYTES);
        assert_eq!(std::mem::size_of::<CommitId>(), SHA256_BYTES);
        assert_eq!(std::mem::size_of::<CommandDigest>(), SHA256_BYTES);
        assert_eq!(LogicalShardId::BYTE_WIDTH, FIXED_ID_BYTES);
        assert_eq!(CommitId::BYTE_WIDTH, SHA256_BYTES);
    }

    #[test]
    fn typed_u64_wrappers_freeze_zero_policy_and_width() {
        macro_rules! assert_non_zero {
            ($type:ty) => {
                assert_eq!(<$type>::new(0), Err(ZeroValueError::new(stringify!($type))));
                assert_eq!(<$type>::new(1).unwrap().get(), 1);
                assert_eq!(<$type>::try_from(u64::MAX).unwrap().get(), u64::MAX);
                assert_eq!(std::mem::size_of::<$type>(), std::mem::size_of::<u64>());
            };
        }

        assert_non_zero!(PlacementGeneration);
        assert_non_zero!(OwnerEpoch);
        assert_non_zero!(ReadVersion);
        assert_non_zero!(CommitVersion);
        assert_non_zero!(Generation);

        macro_rules! assert_zero_allowed {
            ($type:ty) => {
                assert_eq!(<$type>::ZERO.get(), 0);
                assert_eq!(<$type>::new(0).get(), 0);
                assert_eq!(<$type>::from(u64::MAX).get(), u64::MAX);
                assert_eq!(u64::from(<$type>::new(42)), 42);
                assert_eq!(std::mem::size_of::<$type>(), std::mem::size_of::<u64>());
            };
        }

        assert_zero_allowed!(SnapshotId);
        assert_zero_allowed!(WorkspaceRevision);
        assert_zero_allowed!(ReferenceEpoch);
        assert_zero_allowed!(ConsumerEpoch);
    }

    #[test]
    fn durable_names_share_strict_utf8_nul_and_byte_envelope() {
        let maximum = "é".repeat(64);
        let tag = TagName::new(maximum.clone()).unwrap();
        let alias = SnapshotAliasName::from_bytes(maximum.as_bytes()).unwrap();
        assert_eq!(tag.as_bytes(), maximum.as_bytes());
        assert_eq!(alias.as_str(), maximum);
        assert_eq!(tag.as_bytes().len(), TagName::MAX_BYTES);
        assert_eq!(SnapshotAliasName::MAX_BYTES, TagName::MAX_BYTES);

        // The format contract permits a zero-byte name; optionality is carried
        // by the enclosing record rather than by a sentinel string.
        assert_eq!(TagName::new("").unwrap().as_str(), "");
        assert_eq!(SnapshotAliasName::new("").unwrap().as_str(), "");

        let too_long = format!("{maximum}x");
        assert_eq!(
            TagName::new(too_long),
            Err(DurableNameError::TooLong {
                bytes: TagName::MAX_BYTES + 1,
                max: TagName::MAX_BYTES,
            })
        );
        assert_eq!(
            SnapshotAliasName::from_bytes(b"name\0tail"),
            Err(DurableNameError::ContainsNul { index: 4 })
        );
        assert_eq!(
            TagName::from_bytes(&[0xff]),
            Err(DurableNameError::InvalidUtf8)
        );

        let composed = SnapshotAliasName::new("Å").unwrap();
        let decomposed = SnapshotAliasName::new("A\u{30a}").unwrap();
        assert_ne!(composed, decomposed);
        assert_ne!(
            TagName::new("release").unwrap(),
            TagName::new("Release").unwrap()
        );
    }

    #[test]
    fn durable_enum_registry_round_trips_and_rejects_every_unknown_byte() {
        assert_durable_registry(&[
            (WorkspaceState::Staging, 1),
            (WorkspaceState::Visible, 2),
            (WorkspaceState::Retired, 3),
        ]);
        assert_durable_registry(&[
            (RevisionState::Available, 1),
            (RevisionState::Deleting, 2),
            (RevisionState::Deleted, 3),
            (RevisionState::Quarantined, 4),
        ]);
        assert_durable_registry(&[
            (CommitState::Sealed, 1),
            (CommitState::Retiring, 2),
            (CommitState::Retired, 3),
        ]);
        assert_durable_registry(&[
            (SnapshotState::Active, 1),
            (SnapshotState::ReapClaimed, 2),
            (SnapshotState::Reaped, 3),
            (SnapshotState::Retired, 4),
        ]);
        assert_durable_registry(&[
            (ReferenceKind::Path, 1),
            (ReferenceKind::Commit, 2),
            (ReferenceKind::RevisionDependency, 3),
        ]);
        assert_durable_registry(&[
            (HistoryHoldKind::Snapshot, 1),
            (HistoryHoldKind::BuildCommit, 2),
            (HistoryHoldKind::Restore, 3),
        ]);
        assert_durable_registry(&[
            (HistoryHoldState::Active, 1),
            (HistoryHoldState::Releasing, 2),
        ]);
        assert_durable_registry(&[
            (RootActivationState::Installing, 1),
            (RootActivationState::Active, 2),
            (RootActivationState::Draining, 3),
            (RootActivationState::Fenced, 4),
        ]);
        assert_durable_registry(&[
            (RootPlacementLifecycle::Provisioning, 1),
            (RootPlacementLifecycle::Active, 2),
            (RootPlacementLifecycle::Draining, 3),
            (RootPlacementLifecycle::Retired, 4),
        ]);
        assert_durable_registry(&[
            (CommitConsumerKind::WorkbenchHead, 1),
            (CommitConsumerKind::Tag, 2),
            (CommitConsumerKind::Lease, 3),
            (CommitConsumerKind::ChildCommit, 4),
        ]);
        assert_durable_registry(&[
            (RestoreSourceKind::Snapshot, 1),
            (RestoreSourceKind::Commit, 2),
        ]);
        assert_durable_registry(&[
            (OperationKind::Publish, 1),
            (OperationKind::BuildCommit, 2),
            (OperationKind::Restore, 3),
            (OperationKind::CommitRetire, 4),
            (OperationKind::Gc, 5),
        ]);
        assert_durable_registry(&[
            (PublishPhase::Uploading, 1),
            (PublishPhase::Finalizing, 2),
            (PublishPhase::Published, 3),
            (PublishPhase::Aborting, 4),
            (PublishPhase::Cleaning, 5),
            (PublishPhase::Cleaned, 6),
            (PublishPhase::Quarantined, 7),
        ]);
        assert_durable_registry(&[
            (BuildCommitPhase::Building, 1),
            (BuildCommitPhase::Sealing, 2),
            (BuildCommitPhase::Complete, 3),
            (BuildCommitPhase::Aborting, 4),
            (BuildCommitPhase::Cleaning, 5),
            (BuildCommitPhase::Cleaned, 6),
            (BuildCommitPhase::Quarantined, 7),
        ]);
        assert_durable_registry(&[
            (RestorePhase::Preparing, 1),
            (RestorePhase::Copying, 2),
            (RestorePhase::SourceSealed, 3),
            (RestorePhase::Ready, 4),
            (RestorePhase::Complete, 5),
            (RestorePhase::Aborting, 6),
            (RestorePhase::Cleaning, 7),
            (RestorePhase::Cleaned, 8),
            (RestorePhase::Quarantined, 9),
        ]);
        assert_durable_registry(&[
            (CommitRetirePhase::Claiming, 1),
            (CommitRetirePhase::Releasing, 2),
            (CommitRetirePhase::Complete, 3),
            (CommitRetirePhase::Quarantined, 4),
        ]);
        assert_durable_registry(&[
            (GcPhase::Queued, 1),
            (GcPhase::Claimed, 2),
            (GcPhase::Deleting, 3),
            (GcPhase::Deleted, 4),
            (GcPhase::Quarantined, 5),
        ]);
        assert_durable_registry(&[
            (StagedProviderState::Planned, 1),
            (StagedProviderState::Uploading, 2),
            (StagedProviderState::Uploaded, 3),
            (StagedProviderState::AbortPending, 4),
            (StagedProviderState::Aborted, 5),
            (StagedProviderState::Ambiguous, 6),
        ]);
        assert_durable_registry(&[
            (StagedCleanupState::Owned, 1),
            (StagedCleanupState::DeletePending, 2),
            (StagedCleanupState::Deleted, 3),
            (StagedCleanupState::Quarantined, 4),
        ]);
        assert_durable_registry(&[
            (GcClaimState::Candidate, 1),
            (GcClaimState::Claimed, 2),
            (GcClaimState::Complete, 3),
            (GcClaimState::Quarantined, 4),
        ]);
    }

    #[test]
    fn workbench_id_enforces_frozen_ascii_contract() {
        let longest = format!("a{}", "_".repeat(WorkbenchId::MAX_BYTES - 1));
        let id = WorkbenchId::new(longest.clone()).unwrap();
        assert_eq!(id.as_str(), longest);
        assert_eq!(id.as_bytes(), longest.as_bytes());

        assert_eq!(WorkbenchId::new(""), Err(WorkbenchIdError::Empty));
        assert_eq!(
            WorkbenchId::new("_hidden"),
            Err(WorkbenchIdError::InvalidStart { byte: b'_' })
        );
        assert_eq!(
            WorkbenchId::new("run.name"),
            Err(WorkbenchIdError::InvalidCharacter {
                index: 3,
                byte: b'.',
            })
        );
        assert_eq!(
            WorkbenchId::new(format!("a{}", "x".repeat(WorkbenchId::MAX_BYTES))),
            Err(WorkbenchIdError::TooLong {
                bytes: WorkbenchId::MAX_BYTES + 1,
                max: WorkbenchId::MAX_BYTES,
            })
        );
    }

    #[test]
    fn relative_path_rejects_invalid_components_without_cleanup() {
        assert_eq!(
            NormalizedRelativePath::new(""),
            Err(NormalizedRelativePathError::Empty)
        );
        assert_eq!(
            NormalizedRelativePath::new("/input/file"),
            Err(NormalizedRelativePathError::Absolute)
        );
        assert_eq!(
            NormalizedRelativePath::new("input//file"),
            Err(NormalizedRelativePathError::EmptyComponent { index: 1 })
        );
        assert_eq!(
            NormalizedRelativePath::new("input/"),
            Err(NormalizedRelativePathError::EmptyComponent { index: 1 })
        );
        assert_eq!(
            NormalizedRelativePath::new("./file"),
            Err(NormalizedRelativePathError::DotComponent { index: 0 })
        );
        assert_eq!(
            NormalizedRelativePath::new("input/../file"),
            Err(NormalizedRelativePathError::ParentComponent { index: 1 })
        );
        assert_eq!(
            NormalizedRelativePath::new(r"input\file"),
            Err(NormalizedRelativePathError::ContainsBackslash { component: 0 })
        );
        assert_eq!(
            NormalizedRelativePath::new("input/\0file"),
            Err(NormalizedRelativePathError::ContainsNul { component: 1 })
        );
    }

    #[test]
    fn relative_path_preserves_component_boundaries_for_a_and_ab() {
        let a = NormalizedRelativePath::new("a").unwrap();
        let ab = NormalizedRelativePath::new("ab").unwrap();
        let child = NormalizedRelativePath::new("a/b").unwrap();

        assert_ne!(a, ab);
        assert_eq!(a.components().collect::<Vec<_>>(), vec!["a"]);
        assert_eq!(ab.components().collect::<Vec<_>>(), vec!["ab"]);
        assert_eq!(child.components().collect::<Vec<_>>(), vec!["a", "b"]);
        assert_eq!(child.component_count(), 2);
    }

    #[test]
    fn relative_path_preserves_unicode_bytes_without_normalization() {
        let composed = NormalizedRelativePath::new("input/café.txt").unwrap();
        let decomposed = NormalizedRelativePath::new("input/cafe\u{301}.txt").unwrap();

        assert_eq!(composed.as_str().as_bytes(), "input/café.txt".as_bytes());
        assert_eq!(
            decomposed.as_str().as_bytes(),
            "input/cafe\u{301}.txt".as_bytes()
        );
        assert_ne!(composed, decomposed);
        assert_eq!(composed.byte_len(), "input/café.txt".len());
        assert_eq!(decomposed.byte_len(), "input/cafe\u{301}.txt".len());
    }

    #[test]
    fn relative_path_enforces_byte_and_component_envelopes() {
        let maximum_bytes = "x".repeat(NormalizedRelativePath::MAX_BYTES);
        assert_eq!(
            NormalizedRelativePath::new(maximum_bytes)
                .unwrap()
                .byte_len(),
            NormalizedRelativePath::MAX_BYTES
        );
        assert_eq!(
            NormalizedRelativePath::new("x".repeat(NormalizedRelativePath::MAX_BYTES + 1)),
            Err(NormalizedRelativePathError::TooLong {
                bytes: NormalizedRelativePath::MAX_BYTES + 1,
                max: NormalizedRelativePath::MAX_BYTES,
            })
        );

        let maximum_components = vec!["x"; NormalizedRelativePath::MAX_COMPONENTS].join("/");
        assert_eq!(
            NormalizedRelativePath::new(maximum_components)
                .unwrap()
                .component_count(),
            NormalizedRelativePath::MAX_COMPONENTS
        );
        let too_many_components = vec!["x"; NormalizedRelativePath::MAX_COMPONENTS + 1].join("/");
        assert_eq!(
            NormalizedRelativePath::new(too_many_components),
            Err(NormalizedRelativePathError::TooManyComponents {
                components: NormalizedRelativePath::MAX_COMPONENTS + 1,
                max: NormalizedRelativePath::MAX_COMPONENTS,
            })
        );
    }
}
