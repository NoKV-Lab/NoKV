/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Idempotent control-plane provisioning for one immutable root affinity.

use std::fmt;

use nokv_control::{
    ControlError, ControlStore, FreshRootProvisioningDisposition, LogicalShardId, RootId,
    RootPlacement,
};
use nokv_server::{RuntimeDescriptor, RuntimeQualification, ServerError};
use nokv_types::{
    PlacementGeneration, RootLayoutGeneration, RootLayoutProfile, RootPartitionId,
    RootPlacementLifecycle,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionOutcome {
    pub placement: RootPlacement,
    pub logical_shard_preexisting: bool,
    pub metadata_authority_preexisting: bool,
    pub placement_preexisting: bool,
    pub activation_required: bool,
}

#[derive(Debug)]
pub enum ProvisionError {
    Control(ControlError),
    Runtime(ServerError),
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
            Self::Runtime(error) => error.fmt(formatter),
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
            Self::Runtime(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ControlError> for ProvisionError {
    fn from(error: ControlError) -> Self {
        Self::Control(error)
    }
}

impl From<ServerError> for ProvisionError {
    fn from(error: ServerError) -> Self {
        Self::Runtime(error)
    }
}

/// Atomically create the logical shard, its generation-one metadata authority,
/// and the root binding, then advance the initial placement from
/// `Provisioning` to `Active` by CAS.
///
/// Re-running this operation after any completed step is safe. A partial or
/// different pre-existing bundle, an affinity to another logical shard, and a
/// draining/retired placement all fail closed instead of being adopted or
/// rewritten.
pub fn provision_and_activate(
    control: &dyn ControlStore,
    root_id: RootId,
    logical_shard_id: LogicalShardId,
    metadata_runtime: &RuntimeDescriptor,
    root_layout: RootLayoutProfile,
) -> Result<ProvisionOutcome, ProvisionError> {
    // This admission must remain before every control-plane mutation. In
    // particular, foundationdb-v1 cannot leave a partial provisioning bundle
    // while its complete MetadataCommand surface remains NOT QUALIFIED.
    if let RuntimeQualification::NotQualified(code) = metadata_runtime.qualification() {
        return Err(ServerError::InvalidOptions(format!(
            "metadata runtime is not qualified for provisioning ({code:?})"
        ))
        .into());
    }
    if root_layout != RootLayoutProfile::SingleShardRoot {
        return Err(ControlError::RootLayoutNotQualified {
            root_id,
            profile: root_layout,
        }
        .into());
    }
    let desired = RootPlacement {
        root_id,
        layout_profile: root_layout,
        layout_generation: RootLayoutGeneration::new(1)
            .expect("one is a valid root layout generation"),
        partition_id: RootPartitionId::SINGLE_SHARD,
        logical_shard_id,
        placement_generation: PlacementGeneration::new(1)
            .expect("one is a valid placement generation"),
        lifecycle: RootPlacementLifecycle::Provisioning,
    };
    let initial_authority = metadata_runtime.initial_authority(logical_shard_id);
    let provisioned = control.provision_fresh_root(desired.clone(), initial_authority.clone())?;
    if provisioned.metadata_authority != initial_authority {
        return Err(ProvisionError::Control(
            ControlError::FreshRootProvisioningConflict {
                root_id,
                logical_shard_id,
                reason: "atomic provisioning returned a different metadata authority".to_owned(),
            },
        ));
    }
    let preexisting = provisioned.disposition == FreshRootProvisioningDisposition::Replayed;
    let placement = provisioned.root_placement;
    require_affinity(&placement, &desired)?;

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
                    require_affinity(&current, &desired)?;
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
        logical_shard_preexisting: preexisting,
        metadata_authority_preexisting: preexisting,
        placement_preexisting: preexisting,
        activation_required,
    })
}

fn require_affinity(
    placement: &RootPlacement,
    requested: &RootPlacement,
) -> Result<(), ProvisionError> {
    if placement.root_id == requested.root_id
        && placement.layout_profile == requested.layout_profile
        && placement.layout_generation == requested.layout_generation
        && placement.partition_id == requested.partition_id
        && placement.logical_shard_id == requested.logical_shard_id
    {
        return Ok(());
    }
    Err(ControlError::FreshRootProvisioningConflict {
        root_id: requested.root_id,
        logical_shard_id: requested.logical_shard_id,
        reason: format!(
            "stored root binding differs: expected {:?}, actual {:?}",
            requested.layout_fence(),
            placement.layout_fence()
        ),
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

    fn runtime() -> RuntimeDescriptor {
        nokv_server::holt_runtime_descriptor().unwrap()
    }

    #[test]
    fn provision_is_idempotent_and_finishes_active() {
        let control = InMemoryControlStore::new();
        let first = provision_and_activate(
            &control,
            root(1),
            shard(2),
            &runtime(),
            RootLayoutProfile::SingleShardRoot,
        )
        .unwrap();
        assert!(!first.logical_shard_preexisting);
        assert!(!first.metadata_authority_preexisting);
        assert!(!first.placement_preexisting);
        assert!(first.activation_required);
        assert_eq!(first.placement.lifecycle, RootPlacementLifecycle::Active);
        assert_eq!(first.placement.placement_generation.get(), 2);

        let replay = provision_and_activate(
            &control,
            root(1),
            shard(2),
            &runtime(),
            RootLayoutProfile::SingleShardRoot,
        )
        .unwrap();
        assert!(replay.logical_shard_preexisting);
        assert!(replay.metadata_authority_preexisting);
        assert!(replay.placement_preexisting);
        assert!(!replay.activation_required);
        assert_eq!(replay.placement, first.placement);
    }

    #[test]
    fn provision_never_rebinds_an_existing_root() {
        let control = InMemoryControlStore::new();
        provision_and_activate(
            &control,
            root(1),
            shard(2),
            &runtime(),
            RootLayoutProfile::SingleShardRoot,
        )
        .unwrap();
        let error = provision_and_activate(
            &control,
            root(1),
            shard(3),
            &runtime(),
            RootLayoutProfile::SingleShardRoot,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProvisionError::Control(ControlError::FreshRootProvisioningConflict { .. })
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

    #[test]
    fn provision_rejects_an_existing_shard_without_authority_or_placement() {
        let control = InMemoryControlStore::new();
        control.create_logical_shard(shard(2)).unwrap();

        let error = provision_and_activate(
            &control,
            root(1),
            shard(2),
            &runtime(),
            RootLayoutProfile::SingleShardRoot,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ProvisionError::Control(ControlError::FreshRootProvisioningConflict { .. })
        ));
        assert!(control.get_metadata_authority(&shard(2)).unwrap().is_none());
        assert!(control.get_root_placement(&root(1)).unwrap().is_none());
    }

    #[test]
    fn provision_rejects_a_partial_bundle_instead_of_adopting_it() {
        let control = InMemoryControlStore::new();
        control.create_logical_shard(shard(2)).unwrap();
        control
            .create_metadata_authority(runtime().initial_authority(shard(2)))
            .unwrap();

        let error = provision_and_activate(
            &control,
            root(1),
            shard(2),
            &runtime(),
            RootLayoutProfile::SingleShardRoot,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ProvisionError::Control(ControlError::FreshRootProvisioningConflict { .. })
        ));
        assert!(control.get_root_placement(&root(1)).unwrap().is_none());
    }

    #[test]
    fn partitioned_root_provisioning_is_explicitly_not_qualified() {
        let control = InMemoryControlStore::new();
        let error = provision_and_activate(
            &control,
            root(1),
            shard(2),
            &runtime(),
            RootLayoutProfile::PartitionedRoot,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProvisionError::Control(ControlError::RootLayoutNotQualified {
                profile: RootLayoutProfile::PartitionedRoot,
                ..
            })
        ));
        assert!(control.get_logical_shard(&shard(2)).unwrap().is_none());
        assert!(control.get_root_placement(&root(1)).unwrap().is_none());
    }

    #[cfg(feature = "foundationdb-provider")]
    #[test]
    fn unqualified_foundationdb_runtime_cannot_mutate_control_provisioning() {
        use nokv_server::{
            foundationdb_runtime_descriptor, FoundationDbRuntimeConfig,
            FoundationDbTransactionPolicy,
        };

        let directory = tempfile::tempdir().unwrap();
        let cluster_file = directory.path().join("fdb.cluster");
        std::fs::write(&cluster_file, b"test:test@127.0.0.1:4500\n").unwrap();
        let config = FoundationDbRuntimeConfig::from_cluster_file(
            &cluster_file,
            "nokv-test",
            FoundationDbTransactionPolicy::default(),
        )
        .unwrap();
        let runtime = foundationdb_runtime_descriptor(&config).unwrap();
        let control = InMemoryControlStore::new();
        let error = provision_and_activate(
            &control,
            root(1),
            shard(2),
            &runtime,
            RootLayoutProfile::SingleShardRoot,
        )
        .unwrap_err();
        assert!(matches!(error, ProvisionError::Runtime(_)));
        assert!(error.to_string().contains("not qualified"));
        assert!(control.get_logical_shard(&shard(2)).unwrap().is_none());
        assert!(control.get_metadata_authority(&shard(2)).unwrap().is_none());
        assert!(control.get_root_placement(&root(1)).unwrap().is_none());
    }
}
