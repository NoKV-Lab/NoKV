use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{AgentIndexError, AgentIndexResult};

pub(crate) fn encode_json<T: Serialize>(value: &T) -> AgentIndexResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|err| AgentIndexError::Codec(err.to_string()))
}

pub(crate) fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> AgentIndexResult<T> {
    serde_json::from_slice(bytes).map_err(|err| AgentIndexError::Codec(err.to_string()))
}

pub(crate) fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

pub(crate) fn decode_u64(bytes: &[u8]) -> AgentIndexResult<u64> {
    let raw: [u8; 8] = bytes
        .try_into()
        .map_err(|_| AgentIndexError::Codec("invalid u64 byte length".to_owned()))?;
    Ok(u64::from_be_bytes(raw))
}
