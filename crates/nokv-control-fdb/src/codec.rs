/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_control::{
    CatalogEntryState, ControlError, HeartbeatSequence, LogicalShardId, NodeId, ObjectNamespaceId,
    OwnerEpoch, OwnerHeartbeat, OwnerSession, PlacementGeneration, RootCatalogEntry, RootId,
    RpcEndpoint, SessionGeneration, ShardCatalogEntry, ShardRoute, ShardRouteState, StoreId,
    StoreManifest, StoreProvider,
};

const RECORD_MAGIC: &[u8] = b"\x16nokv-control\0";
const RECORD_VERSION: u8 = 1;

#[repr(u8)]
#[derive(Clone, Copy)]
enum RecordKind {
    Manifest = 1,
    RootCatalog = 2,
    ShardCatalog = 3,
    Route = 4,
    Session = 5,
    Heartbeat = 6,
}

pub(crate) fn encode_manifest(manifest: &StoreManifest) -> Result<Vec<u8>, ControlError> {
    let mut encoder = Encoder::new(RecordKind::Manifest);
    encoder.fixed(manifest.store_id().as_bytes());
    encoder.byte(manifest.provider() as u8);
    encoder.u32(manifest.workspace_format_version());
    encoder.byte(manifest.physical_encoding_version());
    encoder.fixed(manifest.provider_namespace_digest());
    encoder.string(manifest.created_by_version(), "created-by version")?;
    Ok(encoder.finish())
}

pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<StoreManifest, ControlError> {
    let mut decoder = Decoder::new(bytes, RecordKind::Manifest, "store manifest")?;
    let store_id = StoreId::from_bytes(decoder.fixed("store id")?);
    let provider = StoreProvider::try_from(decoder.byte("provider")?)?;
    let workspace_format_version = decoder.u32("workspace format version")?;
    let physical_encoding_version = decoder.byte("physical encoding version")?;
    let provider_namespace_digest = decoder.fixed("provider namespace digest")?;
    let created_by_version = decoder.string("created-by version")?;
    decoder.finish()?;
    StoreManifest::new(
        store_id,
        provider,
        workspace_format_version,
        physical_encoding_version,
        provider_namespace_digest,
        created_by_version,
    )
}

pub(crate) fn encode_root_catalog(entry: &RootCatalogEntry) -> Vec<u8> {
    let mut encoder = Encoder::new(RecordKind::RootCatalog);
    encoder.fixed(entry.root_id().as_bytes());
    encoder.fixed(entry.object_namespace_id().as_bytes());
    encoder.fixed(entry.logical_shard_id().as_bytes());
    encoder.u64(entry.placement_generation().get());
    encoder.byte(entry.state() as u8);
    encoder.finish()
}

pub(crate) fn decode_root_catalog(bytes: &[u8]) -> Result<RootCatalogEntry, ControlError> {
    let mut decoder = Decoder::new(bytes, RecordKind::RootCatalog, "root catalog")?;
    let root_id = RootId::from_bytes(decoder.fixed("root id")?);
    let object_namespace_id = ObjectNamespaceId::from_bytes(decoder.fixed("object namespace id")?);
    let logical_shard_id = LogicalShardId::from_bytes(decoder.fixed("logical shard id")?);
    let placement_generation = PlacementGeneration::new(decoder.u64("placement generation")?)
        .map_err(|error| ControlError::InvalidRecord(error.to_string()))?;
    let state = CatalogEntryState::try_from(decoder.byte("catalog state")?)?;
    decoder.finish()?;
    Ok(RootCatalogEntry::new(
        root_id,
        object_namespace_id,
        logical_shard_id,
        placement_generation,
        state,
    ))
}

pub(crate) fn encode_shard_catalog(entry: &ShardCatalogEntry) -> Vec<u8> {
    let mut encoder = Encoder::new(RecordKind::ShardCatalog);
    encoder.fixed(entry.logical_shard_id().as_bytes());
    encoder.byte(entry.state() as u8);
    encoder.finish()
}

pub(crate) fn decode_shard_catalog(bytes: &[u8]) -> Result<ShardCatalogEntry, ControlError> {
    let mut decoder = Decoder::new(bytes, RecordKind::ShardCatalog, "shard catalog")?;
    let logical_shard_id = LogicalShardId::from_bytes(decoder.fixed("logical shard id")?);
    let state = CatalogEntryState::try_from(decoder.byte("catalog state")?)?;
    decoder.finish()?;
    Ok(ShardCatalogEntry::new(logical_shard_id, state))
}

pub(crate) fn encode_route(route: &ShardRoute) -> Result<Vec<u8>, ControlError> {
    let mut encoder = Encoder::new(RecordKind::Route);
    encoder.fixed(route.logical_shard_id().as_bytes());
    encoder.byte(route.state() as u8);
    match (
        route.owner(),
        route.owner_epoch(),
        route.session_generation(),
    ) {
        (Some(owner), Some(owner_epoch), Some(session_generation)) => {
            encoder.byte(1);
            encoder.string(owner.as_str(), "route owner")?;
            encoder.u64(owner_epoch.get());
            encoder.u64(session_generation.get());
        }
        (None, None, None) => encoder.byte(0),
        _ => {
            return Err(ControlError::InvalidRecord(
                "route token fields are only partially present".to_owned(),
            ));
        }
    }
    match route.endpoint() {
        Some(endpoint) => {
            encoder.byte(1);
            encoder.string(endpoint.as_str(), "route endpoint")?;
        }
        None => encoder.byte(0),
    }
    Ok(encoder.finish())
}

pub(crate) fn decode_route(bytes: &[u8]) -> Result<ShardRoute, ControlError> {
    let mut decoder = Decoder::new(bytes, RecordKind::Route, "shard route")?;
    let logical_shard_id = LogicalShardId::from_bytes(decoder.fixed("logical shard id")?);
    let state = ShardRouteState::try_from(decoder.byte("route state")?)?;
    let (owner, owner_epoch, session_generation) = match decoder.flag("route token")? {
        true => (
            Some(
                NodeId::new(decoder.string("route owner")?).map_err(|error| {
                    ControlError::InvalidRecord(format!("invalid route owner: {error}"))
                })?,
            ),
            Some(
                OwnerEpoch::new(decoder.u64("owner epoch")?)
                    .map_err(|error| ControlError::InvalidRecord(error.to_string()))?,
            ),
            Some(SessionGeneration::new(decoder.u64("session generation")?)?),
        ),
        false => (None, None, None),
    };
    let endpoint = match decoder.flag("route endpoint")? {
        true => Some(RpcEndpoint::new(decoder.string("route endpoint")?)?),
        false => None,
    };
    decoder.finish()?;
    ShardRoute::new(
        logical_shard_id,
        state,
        owner,
        endpoint,
        owner_epoch,
        session_generation,
    )
}

pub(crate) fn encode_session(session: &OwnerSession) -> Result<Vec<u8>, ControlError> {
    let mut encoder = Encoder::new(RecordKind::Session);
    encode_session_fields(&mut encoder, session)?;
    Ok(encoder.finish())
}

pub(crate) fn decode_session(bytes: &[u8]) -> Result<OwnerSession, ControlError> {
    let mut decoder = Decoder::new(bytes, RecordKind::Session, "owner session")?;
    let session = decode_session_fields(&mut decoder)?;
    decoder.finish()?;
    Ok(session)
}

pub(crate) fn encode_heartbeat(heartbeat: &OwnerHeartbeat) -> Result<Vec<u8>, ControlError> {
    let mut encoder = Encoder::new(RecordKind::Heartbeat);
    encode_session_fields(&mut encoder, heartbeat.session())?;
    encoder.u64(heartbeat.sequence().get());
    Ok(encoder.finish())
}

pub(crate) fn decode_heartbeat(bytes: &[u8]) -> Result<OwnerHeartbeat, ControlError> {
    let mut decoder = Decoder::new(bytes, RecordKind::Heartbeat, "owner heartbeat")?;
    let session = decode_session_fields(&mut decoder)?;
    let sequence = HeartbeatSequence::new(decoder.u64("heartbeat sequence")?)?;
    decoder.finish()?;
    Ok(OwnerHeartbeat::new(session, sequence))
}

fn encode_session_fields(
    encoder: &mut Encoder,
    session: &OwnerSession,
) -> Result<(), ControlError> {
    encoder.fixed(session.logical_shard_id().as_bytes());
    encoder.string(session.owner().as_str(), "session owner")?;
    encoder.u64(session.owner_epoch().get());
    encoder.u64(session.session_generation().get());
    Ok(())
}

fn decode_session_fields(decoder: &mut Decoder<'_>) -> Result<OwnerSession, ControlError> {
    let logical_shard_id = LogicalShardId::from_bytes(decoder.fixed("logical shard id")?);
    let owner = NodeId::new(decoder.string("session owner")?)
        .map_err(|error| ControlError::InvalidRecord(format!("invalid session owner: {error}")))?;
    let owner_epoch = OwnerEpoch::new(decoder.u64("owner epoch")?)
        .map_err(|error| ControlError::InvalidRecord(error.to_string()))?;
    let session_generation = SessionGeneration::new(decoder.u64("session generation")?)?;
    Ok(OwnerSession::new(
        logical_shard_id,
        owner,
        owner_epoch,
        session_generation,
    ))
}

struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn new(kind: RecordKind) -> Self {
        let mut bytes = Vec::with_capacity(128);
        bytes.extend_from_slice(RECORD_MAGIC);
        bytes.push(RECORD_VERSION);
        bytes.push(kind as u8);
        Self { bytes }
    }

    fn byte(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn fixed(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn string(&mut self, value: &str, field: &'static str) -> Result<(), ControlError> {
        let length = u16::try_from(value.len()).map_err(|_| {
            ControlError::InvalidRecord(format!("{field} exceeds {} bytes", u16::MAX))
        })?;
        self.bytes.extend_from_slice(&length.to_be_bytes());
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
    record: &'static str,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8], kind: RecordKind, record: &'static str) -> Result<Self, ControlError> {
        let minimum = RECORD_MAGIC.len() + 2;
        if bytes.len() < minimum || !bytes.starts_with(RECORD_MAGIC) {
            return Err(ControlError::InvalidRecord(format!(
                "{record} has an invalid physical envelope"
            )));
        }
        let version = bytes[RECORD_MAGIC.len()];
        if version != RECORD_VERSION {
            return Err(ControlError::UnsupportedRecordVersion {
                record,
                version,
                supported: RECORD_VERSION,
            });
        }
        if bytes[RECORD_MAGIC.len() + 1] != kind as u8 {
            return Err(ControlError::InvalidRecord(format!(
                "{record} has the wrong record kind"
            )));
        }
        Ok(Self {
            bytes,
            offset: minimum,
            record,
        })
    }

    fn byte(&mut self, field: &'static str) -> Result<u8, ControlError> {
        Ok(self.take(1, field)?[0])
    }

    fn flag(&mut self, field: &'static str) -> Result<bool, ControlError> {
        match self.byte(field)? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ControlError::InvalidRecord(format!(
                "{} {field} flag is {value}, expected 0 or 1",
                self.record
            ))),
        }
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, ControlError> {
        Ok(u32::from_be_bytes(
            self.take(4, field)?
                .try_into()
                .expect("four bytes were requested"),
        ))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, ControlError> {
        Ok(u64::from_be_bytes(
            self.take(8, field)?
                .try_into()
                .expect("eight bytes were requested"),
        ))
    }

    fn fixed<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], ControlError> {
        Ok(self
            .take(N, field)?
            .try_into()
            .expect("the requested fixed width was checked"))
    }

    fn string(&mut self, field: &'static str) -> Result<String, ControlError> {
        let length = u16::from_be_bytes(
            self.take(2, field)?
                .try_into()
                .expect("two bytes were requested"),
        ) as usize;
        let value = self.take(length, field)?;
        String::from_utf8(value.to_vec()).map_err(|_| {
            ControlError::InvalidRecord(format!("{} {field} is not UTF-8", self.record))
        })
    }

    fn take(&mut self, length: usize, field: &'static str) -> Result<&'a [u8], ControlError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            ControlError::InvalidRecord(format!("{} {field} length overflows", self.record))
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            ControlError::InvalidRecord(format!("{} truncates {field}", self.record))
        })?;
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), ControlError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ControlError::InvalidRecord(format!(
                "{} has {} trailing bytes",
                self.record,
                self.bytes.len() - self.offset
            )))
        }
    }
}
