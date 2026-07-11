use std::fmt;

use nokv_types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName, DentryProjection, DentryRecord,
    FileType, ForkBinding, ForkBindingState, InodeAttr, InodeId, ObjectGcRecord, SliceManifest,
    SnapshotPin, WatchEvent, WatchEventKind,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodecError {
    Truncated,
    InvalidMagic(&'static str),
    UnsupportedVersion { codec: &'static str, version: u8 },
    InvalidFileType(u8),
    InvalidWatchEventKind(u8),
    InvalidOptionTag(u8),
    InvalidInodeId(u64),
    InvalidName(String),
    InvalidUtf8,
    TrailingBytes,
    InvalidFloat,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathIndexCatalogRecord {
    pub path: String,
    pub fields: Vec<PathIndexFieldRecord>,
    pub row_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathIndexFieldRecord {
    pub field: String,
    pub operators: Vec<String>,
    pub sortable: bool,
    pub facetable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathIndexRowRecord {
    pub path: String,
    pub values: Vec<(String, PathIndexValueRecord)>,
}

#[derive(Clone, Debug)]
pub enum PathIndexValueRecord {
    String(String),
    U64(u64),
    F64(f64),
}

impl PartialEq for PathIndexValueRecord {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(left), Self::String(right)) => left == right,
            (Self::U64(left), Self::U64(right)) => left == right,
            (Self::F64(left), Self::F64(right)) => {
                left.total_cmp(right) == std::cmp::Ordering::Equal
            }
            _ => false,
        }
    }
}

impl Eq for PathIndexValueRecord {}

/// Durable allocator high-water reservation: `(last_commit_version, next_inode,
/// epoch)`. `epoch` is the monotonic identity of the allocation authority,
/// bumped only when a control plane transfers ownership of an inode range so a
/// fenced owner reservation can be rejected.
pub fn encode_allocator_state(last_commit_version: u64, next_inode: u64, epoch: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    push_u64(&mut out, last_commit_version);
    push_u64(&mut out, next_inode);
    push_u64(&mut out, epoch);
    out
}

pub fn decode_allocator_state(bytes: &[u8]) -> Result<(u64, u64, u64), CodecError> {
    let mut input = Decoder::new(bytes);
    let last_commit_version = input.u64()?;
    let next_inode = input.u64()?;
    let epoch = input.u64()?;
    input.finish()?;
    Ok((last_commit_version, next_inode, epoch))
}

pub fn encode_inode_attr(attr: &InodeAttr) -> Vec<u8> {
    let mut out = Vec::with_capacity(69);
    push_u64(&mut out, attr.inode.get());
    out.push(file_type_tag(attr.file_type));
    push_u32(&mut out, attr.mode);
    push_u32(&mut out, attr.uid);
    push_u32(&mut out, attr.gid);
    push_u32(&mut out, attr.rdev);
    push_u32(&mut out, attr.nlink);
    push_u64(&mut out, attr.size);
    push_u64(&mut out, attr.generation);
    push_u64(&mut out, attr.mtime_ms);
    push_u64(&mut out, attr.ctime_ms);
    out
}

pub fn decode_inode_attr(bytes: &[u8]) -> Result<InodeAttr, CodecError> {
    let mut input = Decoder::new(bytes);
    let attr = decode_inode_attr_from(&mut input)?;
    input.finish()?;
    Ok(attr)
}

pub fn encode_dentry_projection(projection: &DentryProjection) -> Vec<u8> {
    let mut out = Vec::new();
    push_u64(&mut out, projection.dentry.parent.get());
    put_bytes(&mut out, projection.dentry.name.as_bytes());
    push_u64(&mut out, projection.dentry.child.get());
    out.push(file_type_tag(projection.dentry.child_type));
    push_u64(&mut out, projection.dentry.attr_generation);
    put_bytes(&mut out, &encode_inode_attr(&projection.attr));
    match &projection.body {
        Some(body) => {
            out.push(1);
            put_string(&mut out, &body.producer);
            put_string(&mut out, &body.digest_uri);
            push_u64(&mut out, body.size);
            put_string(&mut out, &body.content_type);
            put_string(&mut out, &body.manifest_id);
            push_u64(&mut out, body.generation);
            push_u64(&mut out, body.base_generation);
            push_u64(&mut out, body.chunk_size);
            push_u64(&mut out, body.block_size);
        }
        None => out.push(0),
    }
    out
}

pub fn decode_dentry_projection(bytes: &[u8]) -> Result<DentryProjection, CodecError> {
    let mut input = Decoder::new(bytes);
    let parent = inode(input.u64()?)?;
    let name = DentryName::new(input.bytes()?.to_vec())
        .map_err(|err| CodecError::InvalidName(err.to_string()))?;
    let child = inode(input.u64()?)?;
    let child_type = file_type(input.u8()?)?;
    let attr_generation = input.u64()?;
    let attr_bytes = input.bytes()?;
    let attr = decode_inode_attr(attr_bytes)?;
    let body = match input.u8()? {
        0 => None,
        1 => Some(BodyDescriptor {
            producer: input.string()?,
            digest_uri: input.string()?,
            size: input.u64()?,
            content_type: input.string()?,
            manifest_id: input.string()?,
            generation: input.u64()?,
            base_generation: input.u64()?,
            chunk_size: input.u64()?,
            block_size: input.u64()?,
        }),
        tag => return Err(CodecError::InvalidFileType(tag)),
    };
    input.finish()?;
    Ok(DentryProjection {
        dentry: DentryRecord {
            parent,
            name,
            child,
            child_type,
            attr_generation,
        },
        attr,
        body,
    })
}

pub fn encode_body_descriptor(body: &BodyDescriptor) -> Vec<u8> {
    let mut out = Vec::new();
    put_string(&mut out, &body.producer);
    put_string(&mut out, &body.digest_uri);
    push_u64(&mut out, body.size);
    put_string(&mut out, &body.content_type);
    put_string(&mut out, &body.manifest_id);
    push_u64(&mut out, body.generation);
    push_u64(&mut out, body.base_generation);
    push_u64(&mut out, body.chunk_size);
    push_u64(&mut out, body.block_size);
    out
}

pub fn decode_body_descriptor(bytes: &[u8]) -> Result<BodyDescriptor, CodecError> {
    let mut input = Decoder::new(bytes);
    let body = BodyDescriptor {
        producer: input.string()?,
        digest_uri: input.string()?,
        size: input.u64()?,
        content_type: input.string()?,
        manifest_id: input.string()?,
        generation: input.u64()?,
        base_generation: input.u64()?,
        chunk_size: input.u64()?,
        block_size: input.u64()?,
    };
    input.finish()?;
    Ok(body)
}

pub fn encode_chunk_manifest(manifest: &ChunkManifest) -> Vec<u8> {
    let mut out = Vec::new();
    push_u64(&mut out, manifest.chunk_index);
    push_u64(&mut out, manifest.logical_offset);
    push_u64(&mut out, manifest.len);
    push_u32(&mut out, manifest.slices.len() as u32);
    for slice in &manifest.slices {
        push_u64(&mut out, slice.slice_id);
        push_u64(&mut out, slice.logical_offset);
        push_u64(&mut out, slice.len);
        push_u32(&mut out, slice.blocks.len() as u32);
        for block in &slice.blocks {
            put_string(&mut out, &block.object_key);
            push_u64(&mut out, block.logical_offset);
            push_u64(&mut out, block.object_offset);
            push_u64(&mut out, block.len);
            put_string(&mut out, &block.digest_uri);
        }
    }
    out
}

pub fn decode_chunk_manifest(bytes: &[u8]) -> Result<ChunkManifest, CodecError> {
    let mut input = Decoder::new(bytes);
    let chunk_index = input.u64()?;
    let logical_offset = input.u64()?;
    let len = input.u64()?;
    let slice_count = input.u32()? as usize;
    let mut slices = Vec::with_capacity(slice_count);
    for _ in 0..slice_count {
        let slice_id = input.u64()?;
        let logical_offset = input.u64()?;
        let len = input.u64()?;
        let block_count = input.u32()? as usize;
        let mut blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            blocks.push(BlockDescriptor {
                object_key: input.string()?,
                logical_offset: input.u64()?,
                object_offset: input.u64()?,
                len: input.u64()?,
                digest_uri: input.string()?,
            });
        }
        slices.push(SliceManifest {
            slice_id,
            logical_offset,
            len,
            blocks,
        });
    }
    input.finish()?;
    Ok(ChunkManifest {
        chunk_index,
        logical_offset,
        len,
        slices,
    })
}

pub fn encode_object_gc_record(record: &ObjectGcRecord) -> Vec<u8> {
    let mut out = Vec::new();
    push_u64(&mut out, record.inode.get());
    push_u64(&mut out, record.generation);
    put_string(&mut out, &record.object_key);
    push_u64(&mut out, record.size);
    put_string(&mut out, &record.digest_uri);
    push_u64(&mut out, record.enqueue_version);
    push_u64(&mut out, record.enqueue_unix_ms);
    out
}

pub fn decode_object_gc_record(bytes: &[u8]) -> Result<ObjectGcRecord, CodecError> {
    let mut input = Decoder::new(bytes);
    let record = ObjectGcRecord {
        inode: inode(input.u64()?)?,
        generation: input.u64()?,
        object_key: input.string()?,
        size: input.u64()?,
        digest_uri: input.string()?,
        enqueue_version: input.u64()?,
        enqueue_unix_ms: input.u64()?,
    };
    input.finish()?;
    Ok(record)
}

pub fn encode_snapshot_pin(pin: &SnapshotPin) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    push_u64(&mut out, pin.snapshot_id);
    push_u64(&mut out, pin.root.get());
    push_u64(&mut out, pin.read_version);
    push_u64(&mut out, pin.created_version);
    push_u64(&mut out, pin.lease_expires_unix_ms);
    out
}

pub fn decode_snapshot_pin(bytes: &[u8]) -> Result<SnapshotPin, CodecError> {
    let mut input = Decoder::new(bytes);
    let pin = SnapshotPin {
        snapshot_id: input.u64()?,
        root: inode(input.u64()?)?,
        read_version: input.u64()?,
        created_version: input.u64()?,
        lease_expires_unix_ms: input.u64()?,
    };
    input.finish()?;
    Ok(pin)
}

const FORK_BINDING_MAGIC: &[u8; 4] = b"NKVF";
const FORK_BINDING_CODEC_VERSION: u8 = 1;

pub fn encode_fork_binding(binding: &ForkBinding) -> Vec<u8> {
    let mut out = Vec::with_capacity(122 + binding.destination_path.len());
    out.extend_from_slice(FORK_BINDING_MAGIC);
    out.push(FORK_BINDING_CODEC_VERSION);
    push_u64(&mut out, binding.fork_root.get());
    push_u64(&mut out, binding.source_root.get());
    push_u64(&mut out, binding.pinned_read_version);
    push_u64(&mut out, binding.snapshot_id);
    push_u64(&mut out, binding.created_version);
    push_u64(&mut out, binding.base_ref_set_id);
    out.extend_from_slice(&binding.operation_digest);
    out.extend_from_slice(&binding.initialization_digest);
    out.push(match binding.state {
        ForkBindingState::Preparing => 1,
        ForkBindingState::Complete => 2,
        ForkBindingState::Releasing => 3,
    });
    put_string(&mut out, &binding.destination_path);
    out
}

pub fn decode_fork_binding(bytes: &[u8]) -> Result<ForkBinding, CodecError> {
    let mut input = Decoder::new(bytes);
    if input.fixed::<4>()? != *FORK_BINDING_MAGIC {
        return Err(CodecError::InvalidMagic("fork binding"));
    }
    let version = input.u8()?;
    if version != FORK_BINDING_CODEC_VERSION {
        return Err(CodecError::UnsupportedVersion {
            codec: "fork binding",
            version,
        });
    }
    let fork_root = inode(input.u64()?)?;
    let source_root = inode(input.u64()?)?;
    let pinned_read_version = input.u64()?;
    let snapshot_id = input.u64()?;
    let created_version = input.u64()?;
    let base_ref_set_id = input.u64()?;
    let operation_digest = input.fixed::<32>()?;
    let initialization_digest = input.fixed::<32>()?;
    let state = match input.u8()? {
        1 => ForkBindingState::Preparing,
        2 => ForkBindingState::Complete,
        3 => ForkBindingState::Releasing,
        tag => return Err(CodecError::InvalidOptionTag(tag)),
    };
    let destination_path = input.string()?;
    let binding = ForkBinding {
        fork_root,
        source_root,
        pinned_read_version,
        snapshot_id,
        created_version,
        base_ref_set_id,
        operation_digest,
        initialization_digest,
        state,
        destination_path,
    };
    input.finish()?;
    Ok(binding)
}

pub fn encode_fork_shadow(source_inode: InodeId) -> Vec<u8> {
    let mut out = Vec::with_capacity(8);
    push_u64(&mut out, source_inode.get());
    out
}

pub fn decode_fork_shadow(bytes: &[u8]) -> Result<InodeId, CodecError> {
    let mut input = Decoder::new(bytes);
    let source = inode(input.u64()?)?;
    input.finish()?;
    Ok(source)
}

pub fn encode_watch_event(event: &WatchEvent) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(watch_event_kind_tag(event.kind));
    put_optional_inode(&mut out, event.parent);
    match &event.name {
        Some(name) => {
            out.push(1);
            put_bytes(&mut out, name.as_bytes());
        }
        None => out.push(0),
    }
    push_u64(&mut out, event.inode.get());
    push_u64(&mut out, event.version);
    out
}

pub fn decode_watch_event(bytes: &[u8]) -> Result<WatchEvent, CodecError> {
    let mut input = Decoder::new(bytes);
    let kind = watch_event_kind(input.u8()?)?;
    let parent = match input.u8()? {
        0 => None,
        1 => Some(inode(input.u64()?)?),
        tag => return Err(CodecError::InvalidOptionTag(tag)),
    };
    let name = match input.u8()? {
        0 => None,
        1 => Some(
            DentryName::new(input.bytes()?.to_vec())
                .map_err(|err| CodecError::InvalidName(err.to_string()))?,
        ),
        tag => return Err(CodecError::InvalidOptionTag(tag)),
    };
    let event = WatchEvent {
        kind,
        parent,
        name,
        inode: inode(input.u64()?)?,
        version: input.u64()?,
    };
    input.finish()?;
    Ok(event)
}

pub fn encode_path_index_catalog(record: &PathIndexCatalogRecord) -> Vec<u8> {
    let mut out = Vec::new();
    put_string(&mut out, &record.path);
    push_u64(&mut out, record.row_count);
    push_u32(&mut out, record.fields.len() as u32);
    for field in &record.fields {
        put_string(&mut out, &field.field);
        push_u32(&mut out, field.operators.len() as u32);
        for op in &field.operators {
            put_string(&mut out, op);
        }
        out.push(u8::from(field.sortable));
        out.push(u8::from(field.facetable));
    }
    out
}

pub fn decode_path_index_catalog(bytes: &[u8]) -> Result<PathIndexCatalogRecord, CodecError> {
    let mut input = Decoder::new(bytes);
    let path = input.string()?;
    let row_count = input.u64()?;
    let field_count = input.u32()? as usize;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let field = input.string()?;
        let operator_count = input.u32()? as usize;
        let mut operators = Vec::with_capacity(operator_count);
        for _ in 0..operator_count {
            operators.push(input.string()?);
        }
        let sortable = input.bool()?;
        let facetable = input.bool()?;
        fields.push(PathIndexFieldRecord {
            field,
            operators,
            sortable,
            facetable,
        });
    }
    input.finish()?;
    Ok(PathIndexCatalogRecord {
        path,
        fields,
        row_count,
    })
}

pub fn encode_path_index_row(record: &PathIndexRowRecord) -> Vec<u8> {
    let mut out = Vec::new();
    put_string(&mut out, &record.path);
    push_u32(&mut out, record.values.len() as u32);
    for (field, value) in &record.values {
        put_string(&mut out, field);
        match value {
            PathIndexValueRecord::String(value) => {
                out.push(1);
                put_string(&mut out, value);
            }
            PathIndexValueRecord::U64(value) => {
                out.push(2);
                push_u64(&mut out, *value);
            }
            PathIndexValueRecord::F64(value) => {
                out.push(3);
                push_u64(&mut out, value.to_bits());
            }
        }
    }
    out
}

pub fn decode_path_index_row(bytes: &[u8]) -> Result<PathIndexRowRecord, CodecError> {
    let mut input = Decoder::new(bytes);
    let path = input.string()?;
    let value_count = input.u32()? as usize;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        let field = input.string()?;
        let value = match input.u8()? {
            1 => PathIndexValueRecord::String(input.string()?),
            2 => PathIndexValueRecord::U64(input.u64()?),
            3 => {
                let value = f64::from_bits(input.u64()?);
                if !value.is_finite() {
                    return Err(CodecError::InvalidFloat);
                }
                PathIndexValueRecord::F64(value)
            }
            tag => return Err(CodecError::InvalidOptionTag(tag)),
        };
        values.push((field, value));
    }
    input.finish()?;
    Ok(PathIndexRowRecord { path, values })
}

fn decode_inode_attr_from(input: &mut Decoder<'_>) -> Result<InodeAttr, CodecError> {
    Ok(InodeAttr {
        inode: inode(input.u64()?)?,
        file_type: file_type(input.u8()?)?,
        mode: input.u32()?,
        uid: input.u32()?,
        gid: input.u32()?,
        rdev: input.u32()?,
        nlink: input.u32()?,
        size: input.u64()?,
        generation: input.u64()?,
        mtime_ms: input.u64()?,
        ctime_ms: input.u64()?,
    })
}

fn file_type_tag(file_type: FileType) -> u8 {
    match file_type {
        FileType::File => 1,
        FileType::Directory => 2,
        FileType::Symlink => 3,
        FileType::NamedPipe => 4,
        FileType::CharDevice => 5,
        FileType::BlockDevice => 6,
        FileType::Socket => 7,
    }
}

fn file_type(tag: u8) -> Result<FileType, CodecError> {
    match tag {
        1 => Ok(FileType::File),
        2 => Ok(FileType::Directory),
        3 => Ok(FileType::Symlink),
        4 => Ok(FileType::NamedPipe),
        5 => Ok(FileType::CharDevice),
        6 => Ok(FileType::BlockDevice),
        7 => Ok(FileType::Socket),
        _ => Err(CodecError::InvalidFileType(tag)),
    }
}

fn watch_event_kind_tag(kind: WatchEventKind) -> u8 {
    match kind {
        WatchEventKind::Create => 1,
        WatchEventKind::Remove => 2,
        WatchEventKind::Rename => 3,
        WatchEventKind::UpdateAttr => 4,
        WatchEventKind::PublishArtifact => 5,
    }
}

fn watch_event_kind(tag: u8) -> Result<WatchEventKind, CodecError> {
    match tag {
        1 => Ok(WatchEventKind::Create),
        2 => Ok(WatchEventKind::Remove),
        3 => Ok(WatchEventKind::Rename),
        4 => Ok(WatchEventKind::UpdateAttr),
        5 => Ok(WatchEventKind::PublishArtifact),
        _ => Err(CodecError::InvalidWatchEventKind(tag)),
    }
}

fn inode(raw: u64) -> Result<InodeId, CodecError> {
    InodeId::new(raw).map_err(|_| CodecError::InvalidInodeId(raw))
}

fn put_optional_inode(out: &mut Vec<u8>, inode: Option<InodeId>) {
    match inode {
        Some(inode) => {
            out.push(1);
            push_u64(out, inode.get());
        }
        None => out.push(0),
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        if self.offset >= self.bytes.len() {
            return Err(CodecError::Truncated);
        }
        let value = self.bytes[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn bool(&mut self) -> Result<bool, CodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(CodecError::InvalidOptionTag(tag)),
        }
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        Ok(self.take(N)?.try_into().expect("fixed decoder width"))
    }

    fn bytes(&mut self) -> Result<&'a [u8], CodecError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn string(&mut self) -> Result<String, CodecError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| CodecError::InvalidUtf8)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self.offset.checked_add(len).ok_or(CodecError::Truncated)?;
        if end > self.bytes.len() {
            return Err(CodecError::Truncated);
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes)
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "encoded metadata value is truncated"),
            Self::InvalidMagic(codec) => write!(f, "invalid {codec} codec magic"),
            Self::UnsupportedVersion { codec, version } => {
                write!(f, "unsupported {codec} codec version {version}")
            }
            Self::InvalidFileType(tag) => write!(f, "invalid file type tag {tag}"),
            Self::InvalidWatchEventKind(tag) => write!(f, "invalid watch event kind tag {tag}"),
            Self::InvalidOptionTag(tag) => write!(f, "invalid optional value tag {tag}"),
            Self::InvalidInodeId(id) => write!(f, "invalid inode id {id}"),
            Self::InvalidName(err) => write!(f, "invalid dentry name: {err}"),
            Self::InvalidUtf8 => write!(f, "encoded metadata string is not UTF-8"),
            Self::TrailingBytes => write!(f, "encoded metadata value has trailing bytes"),
            Self::InvalidFloat => write!(f, "encoded metadata float is not finite"),
        }
    }
}

impl std::error::Error for CodecError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_binding_round_trips() {
        let binding = ForkBinding {
            fork_root: InodeId::new(42).unwrap(),
            source_root: InodeId::new(7).unwrap(),
            pinned_read_version: 1234,
            snapshot_id: 5678,
            created_version: 5679,
            base_ref_set_id: 5680,
            operation_digest: [7; 32],
            initialization_digest: [8; 32],
            state: ForkBindingState::Complete,
            destination_path: "/restored".to_owned(),
        };
        assert_eq!(
            decode_fork_binding(&encode_fork_binding(&binding)).unwrap(),
            binding
        );
        assert!(decode_fork_binding(b"short").is_err());
        let mut extra = encode_fork_binding(&binding);
        extra.push(0);
        assert!(decode_fork_binding(&extra).is_err());
    }

    #[test]
    fn fork_binding_rejects_legacy_unversioned_layout() {
        let mut legacy = Vec::new();
        push_u64(&mut legacy, 42);
        push_u64(&mut legacy, 7);
        push_u64(&mut legacy, 1234);
        push_u64(&mut legacy, 5678);
        push_u64(&mut legacy, 5679);
        push_u64(&mut legacy, 5680);
        legacy.extend_from_slice(&[7; 32]);
        legacy.extend_from_slice(&[8; 32]);
        legacy.push(2);
        put_string(&mut legacy, "/restored");

        assert_eq!(
            decode_fork_binding(&legacy),
            Err(CodecError::InvalidMagic("fork binding"))
        );
    }

    #[test]
    fn fork_binding_rejects_unknown_codec_version() {
        let binding = ForkBinding {
            fork_root: InodeId::new(42).unwrap(),
            source_root: InodeId::new(7).unwrap(),
            pinned_read_version: 1234,
            snapshot_id: 5678,
            created_version: 5679,
            base_ref_set_id: 5680,
            operation_digest: [7; 32],
            initialization_digest: [8; 32],
            state: ForkBindingState::Complete,
            destination_path: "/restored".to_owned(),
        };
        let mut encoded = encode_fork_binding(&binding);
        encoded[FORK_BINDING_MAGIC.len()] = 99;

        assert_eq!(
            decode_fork_binding(&encoded),
            Err(CodecError::UnsupportedVersion {
                codec: "fork binding",
                version: 99,
            })
        );
    }

    #[test]
    fn fork_shadow_round_trips() {
        let source = InodeId::new(99).unwrap();
        assert_eq!(
            decode_fork_shadow(&encode_fork_shadow(source)).unwrap(),
            source
        );
        assert!(decode_fork_shadow(b"").is_err());
    }

    #[test]
    fn body_descriptor_round_trips_base_generation() {
        // Distinct generation/base_generation so a dropped or swapped field fails.
        let body = BodyDescriptor {
            producer: "producer".to_owned(),
            digest_uri: "sha256:body".to_owned(),
            size: 4096,
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "manifest-12".to_owned(),
            generation: 12,
            base_generation: 11,
            chunk_size: 64 * 1024 * 1024,
            block_size: 4 * 1024 * 1024,
        };
        let decoded = decode_body_descriptor(&encode_body_descriptor(&body)).unwrap();
        assert_eq!(decoded, body);
        assert_eq!(decoded.base_generation, 11);
        assert_ne!(decoded.base_generation, decoded.generation);
    }
}
