/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::num::NonZeroU64;

use crate::{ControlError, LogicalShardId, NodeId, OwnerEpoch};

pub const MAX_RPC_ENDPOINT_BYTES: usize = 512;

macro_rules! non_zero_generation {
    ($name:ident, $label:literal) => {
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(NonZeroU64);

        impl $name {
            pub fn new(value: u64) -> Result<Self, ControlError> {
                NonZeroU64::new(value).map(Self).ok_or_else(|| {
                    ControlError::InvalidRecord(concat!($label, " must be nonzero").to_owned())
                })
            }

            pub const fn get(self) -> u64 {
                self.0.get()
            }

            pub fn checked_next(
                self,
                logical_shard_id: LogicalShardId,
            ) -> Result<Self, ControlError> {
                self.get()
                    .checked_add(1)
                    .and_then(NonZeroU64::new)
                    .map(Self)
                    .ok_or(ControlError::OwnershipCounterExhausted {
                        logical_shard_id,
                        counter: $label,
                    })
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }
    };
}

non_zero_generation!(SessionGeneration, "session generation");
non_zero_generation!(HeartbeatSequence, "heartbeat sequence");

/// Canonical RPC endpoint advertised by a serving logical-shard owner.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RpcEndpoint(String);

impl RpcEndpoint {
    pub fn new(value: impl Into<String>) -> Result<Self, ControlError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_RPC_ENDPOINT_BYTES
            || value.trim() != value
            || value.chars().any(char::is_control)
            || value.chars().any(char::is_whitespace)
            || value.contains('/')
        {
            return Err(ControlError::InvalidEndpoint(value));
        }
        let (host, port) =
            split_host_port(&value).ok_or_else(|| ControlError::InvalidEndpoint(value.clone()))?;
        if host.is_empty()
            || (!value.starts_with('[') && host.contains(':'))
            || (value.starts_with('[') && host.parse::<std::net::Ipv6Addr>().is_err())
            || (!value.starts_with('[')
                && !host
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_')))
            || port.parse::<u16>().ok().filter(|port| *port != 0).is_none()
        {
            return Err(ControlError::InvalidEndpoint(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn split_host_port(endpoint: &str) -> Option<(&str, &str)> {
    if let Some(rest) = endpoint.strip_prefix('[') {
        let close = rest.find(']')?;
        let host = &rest[..close];
        let port = rest[close + 1..].strip_prefix(':')?;
        return Some((host, port));
    }
    endpoint.rsplit_once(':')
}

/// Discovery-visible state of one logical shard.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ShardRouteState {
    Unassigned = 1,
    Activating = 2,
    Serving = 3,
    FailClosed = 4,
}

impl TryFrom<u8> for ShardRouteState {
    type Error = ControlError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Unassigned),
            2 => Ok(Self::Activating),
            3 => Ok(Self::Serving),
            4 => Ok(Self::FailClosed),
            value => Err(ControlError::InvalidRecord(format!(
                "unknown shard route state discriminant {value}"
            ))),
        }
    }
}

/// Durable route record. Unassigned routes retain the last token so counters
/// never reset, but they carry no usable endpoint and have no live session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardRoute {
    logical_shard_id: LogicalShardId,
    state: ShardRouteState,
    owner: Option<NodeId>,
    endpoint: Option<RpcEndpoint>,
    owner_epoch: Option<OwnerEpoch>,
    session_generation: Option<SessionGeneration>,
}

impl ShardRoute {
    pub fn unassigned(logical_shard_id: LogicalShardId) -> Self {
        Self {
            logical_shard_id,
            state: ShardRouteState::Unassigned,
            owner: None,
            endpoint: None,
            owner_epoch: None,
            session_generation: None,
        }
    }

    pub fn new(
        logical_shard_id: LogicalShardId,
        state: ShardRouteState,
        owner: Option<NodeId>,
        endpoint: Option<RpcEndpoint>,
        owner_epoch: Option<OwnerEpoch>,
        session_generation: Option<SessionGeneration>,
    ) -> Result<Self, ControlError> {
        let token_fields = [
            owner.is_some(),
            owner_epoch.is_some(),
            session_generation.is_some(),
        ];
        if token_fields.iter().any(|present| *present)
            && token_fields.iter().any(|present| !present)
        {
            return Err(ControlError::InvalidRecord(
                "shard route owner and token fields must be all present or all absent".to_owned(),
            ));
        }
        match state {
            ShardRouteState::Unassigned => {
                if endpoint.is_some() {
                    return Err(ControlError::InvalidRecord(
                        "unassigned shard route must not advertise an endpoint".to_owned(),
                    ));
                }
            }
            ShardRouteState::Activating | ShardRouteState::Serving => {
                if owner.is_none() || endpoint.is_none() {
                    return Err(ControlError::InvalidRecord(
                        "activating or serving shard route requires owner, token, and endpoint"
                            .to_owned(),
                    ));
                }
            }
            ShardRouteState::FailClosed => {
                if owner.is_none() || endpoint.is_none() {
                    return Err(ControlError::InvalidRecord(
                        "fail-closed shard route retains its owner token and fenced endpoint"
                            .to_owned(),
                    ));
                }
            }
        }
        Ok(Self {
            logical_shard_id,
            state,
            owner,
            endpoint,
            owner_epoch,
            session_generation,
        })
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub const fn state(&self) -> ShardRouteState {
        self.state
    }

    pub fn owner(&self) -> Option<&NodeId> {
        self.owner.as_ref()
    }

    pub fn endpoint(&self) -> Option<&RpcEndpoint> {
        self.endpoint.as_ref()
    }

    pub const fn owner_epoch(&self) -> Option<OwnerEpoch> {
        self.owner_epoch
    }

    pub const fn session_generation(&self) -> Option<SessionGeneration> {
        self.session_generation
    }

    fn session_projection(&self) -> Option<OwnerSession> {
        Some(OwnerSession {
            logical_shard_id: self.logical_shard_id,
            owner: self.owner.clone()?,
            owner_epoch: self.owner_epoch?,
            session_generation: self.session_generation?,
        })
    }
}

/// Stable ownership fence checked by owner-required metadata transactions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerSession {
    logical_shard_id: LogicalShardId,
    owner: NodeId,
    owner_epoch: OwnerEpoch,
    session_generation: SessionGeneration,
}

impl OwnerSession {
    pub fn new(
        logical_shard_id: LogicalShardId,
        owner: NodeId,
        owner_epoch: OwnerEpoch,
        session_generation: SessionGeneration,
    ) -> Self {
        Self {
            logical_shard_id,
            owner,
            owner_epoch,
            session_generation,
        }
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub fn owner(&self) -> &NodeId {
        &self.owner
    }

    pub const fn owner_epoch(&self) -> OwnerEpoch {
        self.owner_epoch
    }

    pub const fn session_generation(&self) -> SessionGeneration {
        self.session_generation
    }
}

/// Independently renewed liveness record for one exact owner session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerHeartbeat {
    session: OwnerSession,
    sequence: HeartbeatSequence,
}

impl OwnerHeartbeat {
    pub fn new(session: OwnerSession, sequence: HeartbeatSequence) -> Self {
        Self { session, sequence }
    }

    pub fn session(&self) -> &OwnerSession {
        &self.session
    }

    pub const fn sequence(&self) -> HeartbeatSequence {
        self.sequence
    }
}

/// One consistent control-plane read used for local TTL observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnershipSnapshot {
    route: ShardRoute,
    session: Option<OwnerSession>,
    heartbeat: Option<OwnerHeartbeat>,
}

impl OwnershipSnapshot {
    pub fn new(
        route: ShardRoute,
        session: Option<OwnerSession>,
        heartbeat: Option<OwnerHeartbeat>,
    ) -> Result<Self, ControlError> {
        let snapshot = Self {
            route,
            session,
            heartbeat,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn route(&self) -> &ShardRoute {
        &self.route
    }

    pub fn session(&self) -> Option<&OwnerSession> {
        self.session.as_ref()
    }

    pub fn heartbeat(&self) -> Option<&OwnerHeartbeat> {
        self.heartbeat.as_ref()
    }

    pub fn validate(&self) -> Result<(), ControlError> {
        let shard = self.route.logical_shard_id;
        if self
            .session
            .as_ref()
            .is_some_and(|session| session.logical_shard_id != shard)
            || self
                .heartbeat
                .as_ref()
                .is_some_and(|heartbeat| heartbeat.session.logical_shard_id != shard)
        {
            return ownership_invalid(shard, "route, session, and heartbeat shard ids differ");
        }
        if let Some(heartbeat) = &self.heartbeat {
            if self.route.session_projection().as_ref() != Some(heartbeat.session()) {
                return ownership_invalid(shard, "heartbeat token differs from the route token");
            }
        }
        match self.route.state {
            ShardRouteState::Unassigned => {
                if self.session.is_some() {
                    return ownership_invalid(shard, "unassigned route has a live session");
                }
                if self.route.session_projection().is_none() && self.heartbeat.is_some() {
                    return ownership_invalid(shard, "never-owned route has a heartbeat");
                }
            }
            ShardRouteState::Activating
            | ShardRouteState::Serving
            | ShardRouteState::FailClosed => {
                let route_session = self.route.session_projection();
                if route_session.as_ref() != self.session.as_ref() {
                    return ownership_invalid(shard, "live route token differs from session key");
                }
                if self.heartbeat.is_none() {
                    return ownership_invalid(shard, "live route has no heartbeat");
                }
            }
        }
        Ok(())
    }
}

/// Complete ownership mutation planned before one provider transaction writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnershipUpdate {
    route: ShardRoute,
    session: Option<OwnerSession>,
    heartbeat: OwnerHeartbeat,
}

impl OwnershipUpdate {
    pub fn route(&self) -> &ShardRoute {
        &self.route
    }

    pub fn session(&self) -> Option<&OwnerSession> {
        self.session.as_ref()
    }

    pub fn heartbeat(&self) -> &OwnerHeartbeat {
        &self.heartbeat
    }

    pub fn snapshot(&self) -> Result<OwnershipSnapshot, ControlError> {
        OwnershipSnapshot::new(
            self.route.clone(),
            self.session.clone(),
            Some(self.heartbeat.clone()),
        )
    }
}

pub fn plan_owner_acquisition(
    current: &OwnershipSnapshot,
    owner: NodeId,
    endpoint: RpcEndpoint,
) -> Result<OwnershipUpdate, ControlError> {
    current.validate()?;
    let shard = current.route.logical_shard_id;
    let owner_epoch = match current.route.owner_epoch {
        Some(epoch) => OwnerEpoch::new(
            epoch
                .get()
                .checked_add(1)
                .ok_or(ControlError::OwnerEpochExhausted(shard))?,
        )
        .map_err(|_| ControlError::OwnerEpochExhausted(shard))?,
        None => OwnerEpoch::new(1).expect("one is a valid owner epoch"),
    };
    let session_generation = match current.route.session_generation {
        Some(generation) => generation.checked_next(shard)?,
        None => SessionGeneration::new(1).expect("one is a valid session generation"),
    };
    let heartbeat_sequence = match &current.heartbeat {
        Some(heartbeat) => heartbeat.sequence.checked_next(shard)?,
        None => HeartbeatSequence::new(1).expect("one is a valid heartbeat sequence"),
    };
    let session = OwnerSession::new(shard, owner.clone(), owner_epoch, session_generation);
    let route = ShardRoute::new(
        shard,
        ShardRouteState::Activating,
        Some(owner),
        Some(endpoint),
        Some(owner_epoch),
        Some(session_generation),
    )?;
    let update = OwnershipUpdate {
        route,
        session: Some(session.clone()),
        heartbeat: OwnerHeartbeat::new(session, heartbeat_sequence),
    };
    update.snapshot()?;
    Ok(update)
}

pub fn plan_heartbeat_renewal(
    current: &OwnershipSnapshot,
    expected: &OwnerSession,
) -> Result<OwnershipUpdate, ControlError> {
    ensure_expected_session(current, expected)?;
    let heartbeat = current
        .heartbeat
        .as_ref()
        .expect("validated live ownership has a heartbeat");
    Ok(OwnershipUpdate {
        route: current.route.clone(),
        session: current.session.clone(),
        heartbeat: OwnerHeartbeat::new(
            expected.clone(),
            heartbeat.sequence.checked_next(expected.logical_shard_id)?,
        ),
    })
}

pub fn plan_route_activation(
    current: &OwnershipSnapshot,
    expected: &OwnerSession,
) -> Result<OwnershipUpdate, ControlError> {
    ensure_expected_session(current, expected)?;
    if current.route.state == ShardRouteState::Serving {
        return Ok(OwnershipUpdate {
            route: current.route.clone(),
            session: current.session.clone(),
            heartbeat: current
                .heartbeat
                .clone()
                .expect("validated live ownership has a heartbeat"),
        });
    }
    if !matches!(
        current.route.state,
        ShardRouteState::Activating | ShardRouteState::FailClosed
    ) {
        return ownership_invalid(
            expected.logical_shard_id,
            "only an activating or reconciled fail-closed route can become serving",
        );
    }
    let route = ShardRoute::new(
        current.route.logical_shard_id,
        ShardRouteState::Serving,
        current.route.owner.clone(),
        current.route.endpoint.clone(),
        current.route.owner_epoch,
        current.route.session_generation,
    )?;
    Ok(OwnershipUpdate {
        route,
        session: current.session.clone(),
        heartbeat: current
            .heartbeat
            .clone()
            .expect("validated live ownership has a heartbeat"),
    })
}

pub fn plan_fail_closed(
    current: &OwnershipSnapshot,
    expected: &OwnerSession,
) -> Result<OwnershipUpdate, ControlError> {
    ensure_expected_session(current, expected)?;
    let route = ShardRoute::new(
        current.route.logical_shard_id,
        ShardRouteState::FailClosed,
        current.route.owner.clone(),
        current.route.endpoint.clone(),
        current.route.owner_epoch,
        current.route.session_generation,
    )?;
    Ok(OwnershipUpdate {
        route,
        session: current.session.clone(),
        heartbeat: current
            .heartbeat
            .clone()
            .expect("validated live ownership has a heartbeat"),
    })
}

pub fn plan_owner_release(
    current: &OwnershipSnapshot,
    expected: &OwnerSession,
) -> Result<OwnershipUpdate, ControlError> {
    current.validate()?;
    if current.route.state == ShardRouteState::Unassigned
        && current.session.is_none()
        && current.route.session_projection().as_ref() == Some(expected)
        && current
            .heartbeat
            .as_ref()
            .is_some_and(|heartbeat| heartbeat.session() == expected)
    {
        return Ok(OwnershipUpdate {
            route: current.route.clone(),
            session: None,
            heartbeat: current
                .heartbeat
                .clone()
                .expect("a previously owned route retains its heartbeat"),
        });
    }
    ensure_expected_session(current, expected)?;
    let heartbeat = current
        .heartbeat
        .as_ref()
        .expect("validated live ownership has a heartbeat");
    let route = ShardRoute::new(
        current.route.logical_shard_id,
        ShardRouteState::Unassigned,
        current.route.owner.clone(),
        None,
        current.route.owner_epoch,
        current.route.session_generation,
    )?;
    let update = OwnershipUpdate {
        route,
        session: None,
        heartbeat: OwnerHeartbeat::new(
            expected.clone(),
            heartbeat.sequence.checked_next(expected.logical_shard_id)?,
        ),
    };
    update.snapshot()?;
    Ok(update)
}

fn ensure_expected_session(
    current: &OwnershipSnapshot,
    expected: &OwnerSession,
) -> Result<(), ControlError> {
    current.validate()?;
    if current.session.as_ref() != Some(expected) {
        return Err(ControlError::NotOwner {
            logical_shard_id: expected.logical_shard_id,
        });
    }
    Ok(())
}

fn ownership_invalid<T>(
    logical_shard_id: LogicalShardId,
    reason: impl Into<String>,
) -> Result<T, ControlError> {
    Err(ControlError::OwnershipStateConflict {
        logical_shard_id,
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([7; 16])
    }

    #[test]
    fn endpoint_requires_a_canonical_nonzero_host_port() {
        assert!(RpcEndpoint::new("node-a.example:7750").is_ok());
        assert!(RpcEndpoint::new("[::1]:7750").is_ok());
        for invalid in [
            "",
            "node-a",
            "node-a:0",
            " node-a:7750",
            "bad?host:7750",
            "node:a:7750",
            "http://node-a:7750",
        ] {
            assert!(RpcEndpoint::new(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn ownership_flow_keeps_session_and_heartbeat_separate_and_monotonic() {
        let initial = OwnershipSnapshot::new(ShardRoute::unassigned(shard()), None, None).unwrap();
        let first = plan_owner_acquisition(
            &initial,
            NodeId::new("node-a").unwrap(),
            RpcEndpoint::new("node-a.example:7750").unwrap(),
        )
        .unwrap();
        let first_session = first.session().unwrap().clone();
        assert_eq!(first.route().state(), ShardRouteState::Activating);
        assert_eq!(first_session.owner_epoch().get(), 1);
        assert_eq!(first_session.session_generation().get(), 1);
        assert_eq!(first.heartbeat().sequence().get(), 1);

        let serving = plan_route_activation(&first.snapshot().unwrap(), &first_session).unwrap();
        assert_eq!(serving.route().state(), ShardRouteState::Serving);
        assert_eq!(
            plan_route_activation(&serving.snapshot().unwrap(), &first_session).unwrap(),
            serving
        );
        let renewed = plan_heartbeat_renewal(&serving.snapshot().unwrap(), &first_session).unwrap();
        assert_eq!(renewed.heartbeat().sequence().get(), 2);
        assert_eq!(renewed.session(), Some(&first_session));

        let closed = plan_fail_closed(&renewed.snapshot().unwrap(), &first_session).unwrap();
        assert_eq!(closed.route().state(), ShardRouteState::FailClosed);
        assert!(closed.route().endpoint().is_some());

        let reconciled =
            plan_route_activation(&closed.snapshot().unwrap(), &first_session).unwrap();
        assert_eq!(reconciled.route().state(), ShardRouteState::Serving);

        let released = plan_owner_release(&reconciled.snapshot().unwrap(), &first_session).unwrap();
        assert_eq!(released.route().state(), ShardRouteState::Unassigned);
        assert!(released.session().is_none());
        assert_eq!(released.heartbeat().sequence().get(), 3);
        assert_eq!(
            plan_owner_release(&released.snapshot().unwrap(), &first_session).unwrap(),
            released
        );

        let successor = plan_owner_acquisition(
            &released.snapshot().unwrap(),
            NodeId::new("node-b").unwrap(),
            RpcEndpoint::new("node-b.example:7750").unwrap(),
        )
        .unwrap();
        assert_eq!(successor.session().unwrap().owner_epoch().get(), 2);
        assert_eq!(successor.session().unwrap().session_generation().get(), 2);
        assert_eq!(successor.heartbeat().sequence().get(), 4);
    }

    #[test]
    fn ownership_snapshot_rejects_mixed_tokens() {
        let session = OwnerSession::new(
            shard(),
            NodeId::new("node-a").unwrap(),
            OwnerEpoch::new(1).unwrap(),
            SessionGeneration::new(1).unwrap(),
        );
        let route = ShardRoute::new(
            shard(),
            ShardRouteState::Serving,
            Some(NodeId::new("node-a").unwrap()),
            Some(RpcEndpoint::new("node-a.example:7750").unwrap()),
            Some(OwnerEpoch::new(1).unwrap()),
            Some(SessionGeneration::new(1).unwrap()),
        )
        .unwrap();
        let wrong = OwnerHeartbeat::new(
            OwnerSession::new(
                shard(),
                NodeId::new("node-b").unwrap(),
                OwnerEpoch::new(1).unwrap(),
                SessionGeneration::new(1).unwrap(),
            ),
            HeartbeatSequence::new(1).unwrap(),
        );
        assert!(OwnershipSnapshot::new(route, Some(session), Some(wrong)).is_err());
    }
}
