/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Idempotent control-plane provisioning for one immutable root affinity.

use std::fmt;

use nokv_control::{ControlError, ControlStore, LogicalShardId, RootId, RootPlacement};
use nokv_types::{PlacementGeneration, RootPlacementLifecycle};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionOutcome {
    pub placement: RootPlacement,
    pub logical_shard_preexisting: bool,
    pub placement_preexisting: bool,
    pub activation_required: bool,
}

#[derive(Debug)]
pub enum ProvisionError {
    Control(ControlError),
    PlacementGenerationExhausted(RootId),
    PlacementNotActivatable {
        root_id: RootId,
        lifecycle: RootPlacementLifecycle,
    },
    MissingAfterConflict(&'static str),
}

impl fmt::Display for ProvisionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::PlacementGenerationExhausted(root_id) => {
                write!(formatter, "root placement {root_id:?} generation is exhausted")
            }
            Self::PlacementNotActivatable { root_id, lifecycle } => write!(
                formatter,
                "root placement {root_id:?} is {lifecycle:?} and cannot be provisioned or reactivated"
            ),
            Self::MissingAfterConflict(record) => {
                write!(formatter, "{record} disappeared after a create/CAS conflict")
            }
        }
    }
}

impl std::error::Error for ProvisionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ControlError> for ProvisionError {
    fn from(error: ControlError) -> Self {
        Self::Control(error)
    }
}

/// Create the logical shard, bind the root to it exactly once, and advance the
/// initial placement from `Provisioning` to `Active` by CAS.
///
/// Re-running this operation after any completed step is safe. An existing
/// affinity to another logical shard and a draining/retired placement both
/// fail closed instead of being rewritten.
pub fn provision_and_activate(
    control: &dyn ControlStore,
    root_id: RootId,
    logical_shard_id: LogicalShardId,
) -> Result<ProvisionOutcome, ProvisionError> {
    let logical_shard_preexisting = control.get_logical_shard(&logical_shard_id)?.is_some();
    if !logical_shard_preexisting {
        match control.create_logical_shard(logical_shard_id) {
            Ok(_) => {}
            Err(ControlError::LogicalShardAlreadyExists(existing))
                if existing == logical_shard_id =>
            {
                control
                    .get_logical_shard(&logical_shard_id)?
                    .ok_or(ProvisionError::MissingAfterConflict("logical shard"))?;
            }
            Err(error) => return Err(error.into()),
        }
    }

    let desired = RootPlacement {
        root_id,
        logical_shard_id,
        placement_generation: PlacementGeneration::new(1)
            .expect("one is a valid placement generation"),
        lifecycle: RootPlacementLifecycle::Provisioning,
    };
    let (placement, placement_preexisting) = match control.get_root_placement(&root_id)? {
        Some(placement) => (placement, true),
        None => match control.create_root_placement(desired) {
            Ok(placement) => (placement, false),
            Err(ControlError::RootPlacementAlreadyExists(existing)) if existing == root_id => (
                control
                    .get_root_placement(&root_id)?
                    .ok_or(ProvisionError::MissingAfterConflict("root placement"))?,
                true,
            ),
            Err(error) => return Err(error.into()),
        },
    };
    require_affinity(&placement, logical_shard_id)?;

    let activation_required = placement.lifecycle == RootPlacementLifecycle::Provisioning;
    let placement = match placement.lifecycle {
        RootPlacementLifecycle::Active => placement,
        RootPlacementLifecycle::Provisioning => {
            let generation = placement
                .placement_generation
                .get()
                .checked_add(1)
                .and_then(|value| PlacementGeneration::new(value).ok())
                .ok_or(ProvisionError::PlacementGenerationExhausted(root_id))?;
            let next = RootPlacement {
                placement_generation: generation,
                lifecycle: RootPlacementLifecycle::Active,
                ..placement.clone()
            };
            match control.compare_and_set_root_placement(&placement, next) {
                Ok(active) => active,
                Err(ControlError::RootPlacementCasConflict { .. }) => {
                    let current = control
                        .get_root_placement(&root_id)?
                        .ok_or(ProvisionError::MissingAfterConflict("root placement"))?;
                    require_affinity(&current, logical_shard_id)?;
                    if current.lifecycle != RootPlacementLifecycle::Active {
                        return Err(ProvisionError::PlacementNotActivatable {
                            root_id,
                            lifecycle: current.lifecycle,
                        });
                    }
                    current
                }
                Err(error) => return Err(error.into()),
            }
        }
        lifecycle @ (RootPlacementLifecycle::Draining | RootPlacementLifecycle::Retired) => {
            return Err(ProvisionError::PlacementNotActivatable { root_id, lifecycle });
        }
    };

    Ok(ProvisionOutcome {
        placement,
        logical_shard_preexisting,
        placement_preexisting,
        activation_required,
    })
}

fn require_affinity(
    placement: &RootPlacement,
    requested: LogicalShardId,
) -> Result<(), ProvisionError> {
    if placement.logical_shard_id == requested {
        return Ok(());
    }
    Err(ControlError::ImmutableShardAffinity {
        root_id: placement.root_id,
        existing: placement.logical_shard_id,
        requested,
    }
    .into())
}

#[cfg(test)]
mod tests {
    use nokv_control::InMemoryControlStore;

    use super::*;

    fn root(byte: u8) -> RootId {
        RootId::from_bytes([byte; 16])
    }

    fn shard(byte: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([byte; 16])
    }

    #[test]
    fn provision_is_idempotent_and_finishes_active() {
        let control = InMemoryControlStore::new();
        let first = provision_and_activate(&control, root(1), shard(2)).unwrap();
        assert!(!first.logical_shard_preexisting);
        assert!(!first.placement_preexisting);
        assert!(first.activation_required);
        assert_eq!(first.placement.lifecycle, RootPlacementLifecycle::Active);
        assert_eq!(first.placement.placement_generation.get(), 2);

        let replay = provision_and_activate(&control, root(1), shard(2)).unwrap();
        assert!(replay.logical_shard_preexisting);
        assert!(replay.placement_preexisting);
        assert!(!replay.activation_required);
        assert_eq!(replay.placement, first.placement);
    }

    #[test]
    fn provision_never_rebinds_an_existing_root() {
        let control = InMemoryControlStore::new();
        provision_and_activate(&control, root(1), shard(2)).unwrap();
        let error = provision_and_activate(&control, root(1), shard(3)).unwrap_err();
        assert!(matches!(
            error,
            ProvisionError::Control(ControlError::ImmutableShardAffinity { .. })
        ));
        assert_eq!(
            control
                .get_root_placement(&root(1))
                .unwrap()
                .unwrap()
                .logical_shard_id,
            shard(2)
        );
    }
}
