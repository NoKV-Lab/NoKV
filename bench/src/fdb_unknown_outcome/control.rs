/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! FoundationDB distributed-control mutation-family scenarios.

use nokv_control::{
    plan_fail_closed, plan_heartbeat_renewal, plan_owner_acquisition, plan_owner_release,
    plan_route_activation, CatalogEntryState, ControlError, CreateOutcome, DistributedControlStore,
    OwnershipSnapshot, ShardCatalogEntry, ShardRoute, ShardRouteState,
};
use nokv_control_fdb::{FdbControlKeys, FdbControlStore};
use nokv_fdb::{FdbRuntime, FdbStorePrefix};

use super::evidence::{ChildResult, Scenario};
use super::ScenarioContext;

pub(crate) fn setup(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    scenario: Scenario,
) -> Result<(), String> {
    if scenario.is_metadata() {
        return Err("metadata scenario was routed to control setup".to_owned());
    }
    if scenario == Scenario::ControlManifestFormat {
        return Ok(());
    }
    format_store(runtime, context)?;
    let store = open_store(runtime, context)?;
    match scenario {
        Scenario::ControlShardCreate => {}
        Scenario::ControlRootCreate => {
            require_created(store.create_shard_catalog(context.identity.shard_id))?;
        }
        Scenario::ControlRootReadyCas => {
            require_created(store.create_shard_catalog(context.identity.shard_id))?;
            require_created(
                store.create_root_catalog(context.root_entry(CatalogEntryState::Provisioning)),
            )?;
        }
        Scenario::ControlShardReadyCas => {
            require_created(store.create_shard_catalog(context.identity.shard_id))?;
        }
        Scenario::ControlProvisioningAcquire => {
            require_created(store.create_shard_catalog(context.identity.shard_id))?;
        }
        Scenario::ControlServingAcquire => {
            setup_ready_shard(&store, context)?;
        }
        Scenario::ControlRenew
        | Scenario::ControlActivate
        | Scenario::ControlFailClose
        | Scenario::ControlRelease => {
            setup_ready_shard(&store, context)?;
            let session = store
                .acquire_owner(
                    &context.identity.shard_id,
                    context.identity.owner_a.clone(),
                    context.identity.endpoint_a.clone(),
                )
                .map_err(|error| error.to_string())?;
            if scenario == Scenario::ControlFailClose {
                store
                    .activate_route(&session)
                    .map_err(|error| error.to_string())?;
            }
        }
        Scenario::ControlManifestFormat => unreachable!("handled above"),
        _ => return Err(format!("unsupported control setup scenario {scenario}")),
    }
    Ok(())
}

pub(crate) fn execute_child(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    scenario: Scenario,
) -> Result<ChildResult, String> {
    if scenario == Scenario::ControlManifestFormat {
        let result =
            FdbControlStore::format(runtime, &context.control_options()?, context.manifest()?);
        return control_child_result(scenario, result.map(|_| ()), true);
    }
    let store = open_store(runtime, context)?;
    let result = match scenario {
        Scenario::ControlShardCreate => store
            .create_shard_catalog(context.identity.shard_id)
            .map(|_| ()),
        Scenario::ControlRootCreate => store
            .create_root_catalog(context.root_entry(CatalogEntryState::Provisioning))
            .map(|_| ()),
        Scenario::ControlRootReadyCas => {
            let expected = context.root_entry(CatalogEntryState::Provisioning);
            store
                .compare_and_set_root_catalog(
                    &expected,
                    expected.with_state(CatalogEntryState::Ready),
                )
                .map(|_| ())
        }
        Scenario::ControlShardReadyCas => {
            let expected =
                ShardCatalogEntry::new(context.identity.shard_id, CatalogEntryState::Provisioning);
            store
                .compare_and_set_shard_catalog(
                    &expected,
                    expected.with_state(CatalogEntryState::Ready),
                )
                .map(|_| ())
        }
        Scenario::ControlProvisioningAcquire => store
            .acquire_provisioning_owner(
                &context.identity.shard_id,
                context.identity.owner_a.clone(),
                context.identity.endpoint_a.clone(),
            )
            .map(|_| ()),
        Scenario::ControlServingAcquire => store
            .acquire_owner(
                &context.identity.shard_id,
                context.identity.owner_a.clone(),
                context.identity.endpoint_a.clone(),
            )
            .map(|_| ()),
        Scenario::ControlRenew => current_session(&store, context)
            .and_then(|session| store.renew_owner(&session))
            .map(|_| ()),
        Scenario::ControlActivate => current_session(&store, context)
            .and_then(|session| store.activate_route(&session))
            .map(|_| ()),
        Scenario::ControlFailClose => current_session(&store, context)
            .and_then(|session| store.fail_closed(&session))
            .map(|_| ()),
        Scenario::ControlRelease => current_session(&store, context)
            .and_then(|session| store.release_owner(&session))
            .map(|_| ()),
        _ => return Err(format!("unsupported control child scenario {scenario}")),
    };
    control_child_result(scenario, result, false)
}

pub(crate) fn target_key(context: &ScenarioContext, scenario: Scenario) -> Result<Vec<u8>, String> {
    let prefix =
        FdbStorePrefix::new(context.prefix.as_bytes()).map_err(|error| error.to_string())?;
    let keys = FdbControlKeys::new(&prefix);
    let key = match scenario {
        Scenario::ControlManifestFormat => keys.manifest_key(),
        Scenario::ControlShardCreate | Scenario::ControlShardReadyCas => {
            keys.shard_catalog_key(&context.identity.shard_id)
        }
        Scenario::ControlRootCreate | Scenario::ControlRootReadyCas => {
            keys.root_catalog_key(&context.identity.root_id)
        }
        Scenario::ControlProvisioningAcquire
        | Scenario::ControlServingAcquire
        | Scenario::ControlRelease => keys.session_key(&context.identity.shard_id),
        Scenario::ControlRenew => keys.heartbeat_key(&context.identity.shard_id),
        Scenario::ControlActivate | Scenario::ControlFailClose => {
            keys.route_key(&context.identity.shard_id)
        }
        _ => return Err(format!("unsupported control target scenario {scenario}")),
    };
    Ok(key)
}

pub(crate) fn readback(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    scenario: Scenario,
) -> Result<String, String> {
    if scenario == Scenario::ControlManifestFormat {
        let actual = FdbControlStore::inspect_manifest(runtime, &context.control_options()?)
            .map_err(|error| error.to_string())?;
        if actual != context.manifest()? {
            return Err("manifest readback differs from the complete requested record".to_owned());
        }
        return Ok("complete manifest equals requested value".to_owned());
    }
    let store = open_store(runtime, context)?;
    match scenario {
        Scenario::ControlShardCreate => {
            let expected =
                ShardCatalogEntry::new(context.identity.shard_id, CatalogEntryState::Provisioning);
            if store
                .get_shard_catalog(&context.identity.shard_id)
                .map_err(|error| error.to_string())?
                != Some(expected)
            {
                return Err("created shard catalog differs from requested value".to_owned());
            }
            let snapshot = store
                .observe_ownership(&context.identity.shard_id)
                .map_err(|error| error.to_string())?;
            let expected = OwnershipSnapshot::new(
                ShardRoute::unassigned(context.identity.shard_id),
                None,
                None,
            )
            .map_err(|error| error.to_string())?;
            require_snapshot(&snapshot, &expected)?;
            Ok("Provisioning shard and exact Unassigned ownership tuple".to_owned())
        }
        Scenario::ControlRootCreate | Scenario::ControlRootReadyCas => {
            let state = if scenario == Scenario::ControlRootCreate {
                CatalogEntryState::Provisioning
            } else {
                CatalogEntryState::Ready
            };
            let actual = store
                .get_root_catalog(&context.identity.root_id)
                .map_err(|error| error.to_string())?;
            if actual != Some(context.root_entry(state)) {
                return Err(
                    "root catalog readback differs from complete expected record".to_owned(),
                );
            }
            Ok(format!("complete root catalog is {state:?}"))
        }
        Scenario::ControlShardReadyCas => {
            let expected =
                ShardCatalogEntry::new(context.identity.shard_id, CatalogEntryState::Ready);
            if store
                .get_shard_catalog(&context.identity.shard_id)
                .map_err(|error| error.to_string())?
                != Some(expected)
            {
                return Err("Ready shard CAS readback differs from expected record".to_owned());
            }
            Ok("complete shard catalog is Ready".to_owned())
        }
        Scenario::ControlProvisioningAcquire | Scenario::ControlServingAcquire => {
            let initial = unassigned(context)?;
            let expected = plan_owner_acquisition(
                &initial,
                context.identity.owner_a.clone(),
                context.identity.endpoint_a.clone(),
            )
            .and_then(|update| update.snapshot())
            .map_err(|error| error.to_string())?;
            let actual = store
                .observe_ownership(&context.identity.shard_id)
                .map_err(|error| error.to_string())?;
            require_snapshot(&actual, &expected)?;
            Ok(
                "exact Activating owner, session, endpoint, epoch, generation, heartbeat"
                    .to_owned(),
            )
        }
        Scenario::ControlRenew => {
            let acquired = acquired(context)?;
            let session = acquired
                .session()
                .expect("planned acquisition has a session");
            let expected = plan_heartbeat_renewal(&acquired, session)
                .and_then(|update| update.snapshot())
                .map_err(|error| error.to_string())?;
            let actual = store
                .observe_ownership(&context.identity.shard_id)
                .map_err(|error| error.to_string())?;
            require_snapshot(&actual, &expected)?;
            Ok("exact owner heartbeat sequence advanced once".to_owned())
        }
        Scenario::ControlActivate => {
            let acquired = acquired(context)?;
            let session = acquired
                .session()
                .expect("planned acquisition has a session");
            let expected = plan_route_activation(&acquired, session)
                .and_then(|update| update.snapshot())
                .map_err(|error| error.to_string())?;
            let actual = store
                .observe_ownership(&context.identity.shard_id)
                .map_err(|error| error.to_string())?;
            require_snapshot(&actual, &expected)?;
            Ok("exact Serving route for the acquired session".to_owned())
        }
        Scenario::ControlFailClose => {
            let acquired = acquired(context)?;
            let session = acquired
                .session()
                .expect("planned acquisition has a session");
            let serving = plan_route_activation(&acquired, session)
                .and_then(|update| update.snapshot())
                .map_err(|error| error.to_string())?;
            let expected = plan_fail_closed(&serving, session)
                .and_then(|update| update.snapshot())
                .map_err(|error| error.to_string())?;
            let actual = store
                .observe_ownership(&context.identity.shard_id)
                .map_err(|error| error.to_string())?;
            require_snapshot(&actual, &expected)?;
            Ok("exact FailClosed route retained the same fenced session".to_owned())
        }
        Scenario::ControlRelease => {
            let acquired = acquired(context)?;
            let session = acquired
                .session()
                .expect("planned acquisition has a session");
            let expected = plan_owner_release(&acquired, session)
                .and_then(|update| update.snapshot())
                .map_err(|error| error.to_string())?;
            let actual = store
                .observe_ownership(&context.identity.shard_id)
                .map_err(|error| error.to_string())?;
            require_snapshot(&actual, &expected)?;
            if actual.session().is_some() {
                return Err("released stable session key still exists".to_owned());
            }
            Ok("exact Unassigned route, absent session, retained monotonic counters".to_owned())
        }
        _ => Err(format!("unsupported control readback scenario {scenario}")),
    }
}

pub(crate) fn format_store(runtime: &FdbRuntime, context: &ScenarioContext) -> Result<(), String> {
    match FdbControlStore::format(runtime, &context.control_options()?, context.manifest()?)
        .map_err(|error| error.to_string())?
    {
        CreateOutcome::Created(_) => Ok(()),
        CreateOutcome::Existing(_) => Err("fresh scenario prefix was already formatted".to_owned()),
    }
}

pub(crate) fn open_store(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
) -> Result<FdbControlStore, String> {
    FdbControlStore::open(runtime, context.control_options()?, context.manifest()?)
        .map_err(|error| error.to_string())
}

fn setup_ready_shard(store: &FdbControlStore, context: &ScenarioContext) -> Result<(), String> {
    require_created(store.create_shard_catalog(context.identity.shard_id))?;
    let expected =
        ShardCatalogEntry::new(context.identity.shard_id, CatalogEntryState::Provisioning);
    store
        .compare_and_set_shard_catalog(&expected, expected.with_state(CatalogEntryState::Ready))
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn current_session(
    store: &FdbControlStore,
    context: &ScenarioContext,
) -> Result<nokv_control::OwnerSession, ControlError> {
    store
        .observe_ownership(&context.identity.shard_id)?
        .session()
        .cloned()
        .ok_or(ControlError::NotOwner {
            logical_shard_id: context.identity.shard_id,
        })
}

fn unassigned(context: &ScenarioContext) -> Result<OwnershipSnapshot, String> {
    OwnershipSnapshot::new(
        ShardRoute::unassigned(context.identity.shard_id),
        None,
        None,
    )
    .map_err(|error| error.to_string())
}

fn acquired(context: &ScenarioContext) -> Result<OwnershipSnapshot, String> {
    plan_owner_acquisition(
        &unassigned(context)?,
        context.identity.owner_a.clone(),
        context.identity.endpoint_a.clone(),
    )
    .and_then(|update| update.snapshot())
    .map_err(|error| error.to_string())
}

fn require_snapshot(
    actual: &OwnershipSnapshot,
    expected: &OwnershipSnapshot,
) -> Result<(), String> {
    if actual != expected {
        return Err(format!(
            "ownership readback differs from exact planned tuple: actual {actual:?}, expected {expected:?}"
        ));
    }
    if actual.route().state() == ShardRouteState::Serving
        && expected.route().state() != ShardRouteState::Serving
    {
        return Err("unproved ownership state became Serving".to_owned());
    }
    Ok(())
}

fn require_created<T>(outcome: Result<CreateOutcome<T>, ControlError>) -> Result<(), String> {
    match outcome.map_err(|error| error.to_string())? {
        CreateOutcome::Created(_) => Ok(()),
        CreateOutcome::Existing(_) => {
            Err("fresh setup unexpectedly observed existing state".to_owned())
        }
    }
}

fn control_child_result(
    scenario: Scenario,
    result: Result<(), ControlError>,
    reconcile_expected: bool,
) -> Result<ChildResult, String> {
    match result {
        Ok(()) => Ok(ChildResult {
            scenario,
            outcome: if reconcile_expected {
                "reconciled_success"
            } else {
                "operation_success"
            }
            .to_owned(),
            typed_error: None,
        }),
        Err(error @ ControlError::CommitOutcomeUnknown { .. }) => Ok(ChildResult {
            scenario,
            outcome: "commit_outcome_unknown".to_owned(),
            typed_error: Some(error.to_string()),
        }),
        Err(error) => Err(format!(
            "{scenario} returned unexpected control error: {error}"
        )),
    }
}
