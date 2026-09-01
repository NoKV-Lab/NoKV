/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Session-fenced FoundationDB metadata mutation-family scenarios.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use nokv_control::{
    CatalogEntryState, CreateOutcome, DistributedControlStore, OwnerSession, ShardCatalogEntry,
};
use nokv_control_fdb::{FdbControlStore, FdbSessionFence};
use nokv_fdb::{FdbRuntime, FdbStorePrefix, FdbSubspaceKind};
use nokv_meta::workspace::{
    CommandMutation, CommandPredicate, MetaError, MetaShard, MetadataCommand, MetadataFamily,
    RootFence, RootFenceAction, SCHEMA_ID,
};
use nokv_meta_fdb::{FdbMetadataSessionFence, FdbOptions, FdbStore};
use nokv_meta_store::StoreError;
use nokv_types::{ReadVersion, RequestId, RootActivationState};

use super::control;
use super::evidence::{ChildResult, Scenario};
use super::{owner_epoch, zero_digest, ScenarioContext, LEASE_TTL_MILLIS};

const SYSTEM_KEYSPACE: u16 = 0x0101;
const ROOT_FENCE_KEYSPACE: u16 = 0x0102;
const WORKSPACE_CURRENT_KEYSPACE: u16 = 0x0202;
const SYSTEM_SCHEMA_KEY: &[u8] = b"schema";
const SYSTEM_OWNER_FENCE_KEY: &[u8] = b"owner_fence";
const SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY: &[u8] = b"lease_clock_high_water";
const LEASE_CLOCK_OBSERVATION: u64 = 424_242;
const ORDINARY_VALUE: &[u8] = b"gate2-applied-once";
const ORDINARY_RESULT: &[u8] = b"gate2-deterministic-result";

pub(crate) fn setup(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    scenario: Scenario,
) -> Result<(), String> {
    if !scenario.is_metadata() {
        return Err("control scenario was routed to metadata setup".to_owned());
    }
    let session = setup_control_session(runtime, context)?;
    if scenario == Scenario::MetadataInitialize {
        return Ok(());
    }
    let shard = initialize_metadata(runtime, context, &session)?;
    if scenario == Scenario::MetadataOwnerEpoch {
        return Ok(());
    }
    shard
        .advance_owner_epoch(None, owner_epoch(1))
        .map_err(|error| error.to_string())?;
    if scenario == Scenario::MetadataRootFenceInstall {
        return Ok(());
    }
    execute_exact(
        &shard,
        fence_command(&shard, context, FenceCommand::Install)?,
    )?;
    if scenario == Scenario::MetadataRootFenceActivate {
        return Ok(());
    }
    execute_exact(
        &shard,
        fence_command(&shard, context, FenceCommand::Activate)?,
    )?;
    Ok(())
}

pub(crate) fn execute_child(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    scenario: Scenario,
) -> Result<ChildResult, String> {
    let control = control::open_store(runtime, context)?;
    let session = control
        .observe_ownership(&context.identity.shard_id)
        .map_err(|error| error.to_string())?
        .session()
        .cloned()
        .ok_or_else(|| "metadata child has no exact owner session".to_owned())?;
    let store = metadata_store(runtime, context, &control, &session)?;
    if scenario == Scenario::MetadataInitialize {
        return metadata_child_result(
            scenario,
            MetaShard::initialize(store, context.identity.shard_id).map(|_| ()),
            false,
        );
    }
    let shard =
        MetaShard::open(store, context.identity.shard_id).map_err(|error| error.to_string())?;
    let result = match scenario {
        Scenario::MetadataOwnerEpoch => shard.advance_owner_epoch(None, owner_epoch(1)).map(|_| ()),
        Scenario::MetadataRootFenceInstall => {
            let command = fence_command(&shard, context, FenceCommand::Install)?;
            shard.execute(&command).map(|_| ())
        }
        Scenario::MetadataRootFenceActivate => {
            let command = fence_command(&shard, context, FenceCommand::Activate)?;
            shard.execute(&command).map(|_| ())
        }
        Scenario::MetadataOrdinaryCommand => {
            let command = ordinary_command(&shard, context)?;
            shard.execute(&command).map(|_| ())
        }
        Scenario::MetadataLeaseClock => shard
            .observe_lease_clock(
                context.identity.root_id,
                context.identity.placement_generation,
                owner_epoch(1),
                LEASE_CLOCK_OBSERVATION,
            )
            .map(|_| ()),
        _ => return Err(format!("unsupported metadata child scenario {scenario}")),
    };
    metadata_child_result(
        scenario,
        result,
        matches!(
            scenario,
            Scenario::MetadataOwnerEpoch | Scenario::MetadataLeaseClock
        ),
    )
}

pub(crate) fn target_key(context: &ScenarioContext, scenario: Scenario) -> Result<Vec<u8>, String> {
    let (keyspace, logical_key) = match scenario {
        Scenario::MetadataInitialize => (SYSTEM_KEYSPACE, SYSTEM_SCHEMA_KEY.to_vec()),
        Scenario::MetadataOwnerEpoch => (SYSTEM_KEYSPACE, SYSTEM_OWNER_FENCE_KEY.to_vec()),
        Scenario::MetadataRootFenceInstall | Scenario::MetadataRootFenceActivate => (
            ROOT_FENCE_KEYSPACE,
            context.identity.root_id.as_bytes().to_vec(),
        ),
        Scenario::MetadataOrdinaryCommand => (WORKSPACE_CURRENT_KEYSPACE, ordinary_key(context)),
        Scenario::MetadataLeaseClock => {
            (SYSTEM_KEYSPACE, SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY.to_vec())
        }
        _ => return Err(format!("unsupported metadata target scenario {scenario}")),
    };
    physical_metadata_key(context, keyspace, &logical_key)
}

pub(crate) fn readback(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    scenario: Scenario,
) -> Result<String, String> {
    let control = control::open_store(runtime, context)?;
    let session_a = control
        .observe_ownership(&context.identity.shard_id)
        .map_err(|error| error.to_string())?
        .session()
        .cloned()
        .ok_or_else(|| "metadata readback has no owner A session".to_owned())?;
    let store_a = metadata_store(runtime, context, &control, &session_a)?;
    let shard_a =
        MetaShard::open(store_a, context.identity.shard_id).map_err(|error| error.to_string())?;
    match scenario {
        Scenario::MetadataInitialize => {
            if shard_a
                .current_owner_epoch()
                .map_err(|error| error.to_string())?
                .is_some()
                || shard_a
                    .lease_clock_high_water()
                    .map_err(|error| error.to_string())?
                    != 0
                || shard_a
                    .current_read_version()
                    .map_err(|error| error.to_string())?
                    .get()
                    != 1
            {
                return Err(
                    "metadata initialization rows are not the exact bootstrap state".to_owned(),
                );
            }
            Ok("schema, shard identity, owner zero, commit clock one, lease clock zero".to_owned())
        }
        Scenario::MetadataOwnerEpoch => {
            if shard_a
                .current_owner_epoch()
                .map_err(|error| error.to_string())?
                != Some(owner_epoch(1))
            {
                return Err("owner epoch did not advance exactly to one".to_owned());
            }
            Ok("owner epoch equals exact acquired epoch once".to_owned())
        }
        Scenario::MetadataRootFenceInstall => {
            require_root_fence(&shard_a, context, RootActivationState::Installing)?;
            Ok("complete root fence is Installing with exact immutable placement".to_owned())
        }
        Scenario::MetadataRootFenceActivate => {
            require_root_fence(&shard_a, context, RootActivationState::Active)?;
            Ok("complete root fence is Active with exact immutable placement".to_owned())
        }
        Scenario::MetadataLeaseClock => {
            require_root_fence(&shard_a, context, RootActivationState::Active)?;
            let actual = shard_a
                .lease_clock_high_water()
                .map_err(|error| error.to_string())?;
            if actual != LEASE_CLOCK_OBSERVATION {
                return Err(format!(
                    "lease-clock readback is {actual}, expected {LEASE_CLOCK_OBSERVATION}"
                ));
            }
            Ok("lease-clock high-water equals the isolated observation once".to_owned())
        }
        Scenario::MetadataOrdinaryCommand => {
            ordinary_command_readback(runtime, context, control, shard_a, session_a)
        }
        _ => Err(format!("unsupported metadata readback scenario {scenario}")),
    }
}

fn setup_control_session(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
) -> Result<OwnerSession, String> {
    control::format_store(runtime, context)?;
    let control = control::open_store(runtime, context)?;
    match control
        .create_shard_catalog(context.identity.shard_id)
        .map_err(|error| error.to_string())?
    {
        CreateOutcome::Created(_) => {}
        CreateOutcome::Existing(_) => {
            return Err("fresh metadata setup observed an existing shard".to_owned())
        }
    }
    let expected =
        ShardCatalogEntry::new(context.identity.shard_id, CatalogEntryState::Provisioning);
    control
        .compare_and_set_shard_catalog(&expected, expected.with_state(CatalogEntryState::Ready))
        .map_err(|error| error.to_string())?;
    control
        .acquire_owner(
            &context.identity.shard_id,
            context.identity.owner_a.clone(),
            context.identity.endpoint_a.clone(),
        )
        .map_err(|error| error.to_string())
}

fn initialize_metadata(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    session: &OwnerSession,
) -> Result<MetaShard, String> {
    let control = control::open_store(runtime, context)?;
    MetaShard::initialize(
        metadata_store(runtime, context, &control, session)?,
        context.identity.shard_id,
    )
    .map_err(|error| error.to_string())
}

fn metadata_store(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    control: &FdbControlStore,
    session: &OwnerSession,
) -> Result<Arc<FdbStore>, String> {
    let fence =
        FdbSessionFence::new(control.keys(), session.clone()).map_err(|error| error.to_string())?;
    let metadata_fence = FdbMetadataSessionFence::new(
        fence.key(),
        fence.expected_value(),
        session.owner_epoch().get(),
        session.session_generation().get(),
    )
    .map_err(|error| error.to_string())?;
    FdbStore::open(
        runtime,
        FdbOptions::new(
            &context.cluster_file,
            context.prefix.as_bytes().to_vec(),
            metadata_fence,
        ),
    )
    .map(Arc::new)
    .map_err(|error| error.to_string())
}

#[derive(Clone, Copy)]
enum FenceCommand {
    Install,
    Activate,
}

fn fence_command(
    shard: &MetaShard,
    context: &ScenarioContext,
    action: FenceCommand,
) -> Result<MetadataCommand, String> {
    let (request_tag, root_fence_action, result) = match action {
        FenceCommand::Install => (
            1,
            RootFenceAction::Install,
            b"gate2-root-fence-installed".as_slice(),
        ),
        FenceCommand::Activate => (
            2,
            RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
            b"gate2-root-fence-active".as_slice(),
        ),
    };
    base_command(
        context,
        tagged_request(context.identity.request_id, request_tag),
        shard
            .current_read_version()
            .map_err(|error| error.to_string())?,
        root_fence_action,
        Vec::new(),
        result.to_vec(),
    )
}

fn ordinary_command(
    shard: &MetaShard,
    context: &ScenarioContext,
) -> Result<MetadataCommand, String> {
    ordinary_command_at(
        context,
        shard
            .current_read_version()
            .map_err(|error| error.to_string())?,
    )
}

fn ordinary_command_at(
    context: &ScenarioContext,
    read_version: ReadVersion,
) -> Result<MetadataCommand, String> {
    let key = ordinary_key(context);
    let mut command = base_command(
        context,
        tagged_request(context.identity.request_id, 3),
        read_version,
        RootFenceAction::RequireActive,
        vec![CommandMutation::Put {
            family: MetadataFamily::WorkspaceCurrent,
            key: key.clone(),
            value: ORDINARY_VALUE.to_vec(),
        }],
        ORDINARY_RESULT.to_vec(),
    )?;
    command.predicates.push(CommandPredicate::Value {
        family: MetadataFamily::WorkspaceCurrent,
        key,
        expected: None,
    });
    Ok(command.seal())
}

fn base_command(
    context: &ScenarioContext,
    request_id: RequestId,
    read_version: ReadVersion,
    root_fence_action: RootFenceAction,
    mutations: Vec<CommandMutation>,
    deterministic_result: Vec<u8>,
) -> Result<MetadataCommand, String> {
    Ok(MetadataCommand {
        schema_id: SCHEMA_ID.to_owned(),
        root_id: context.identity.root_id,
        logical_shard_id: context.identity.shard_id,
        object_namespace_id: Some(context.identity.object_namespace_id),
        placement_generation: context.identity.placement_generation,
        owner_epoch: owner_epoch(1),
        request_id,
        command_digest: zero_digest(),
        read_version,
        root_fence_action,
        predicates: Vec::new(),
        mutations,
        history_projection: Vec::new(),
        event_projection: Vec::new(),
        deterministic_result,
    }
    .seal())
}

fn execute_exact(shard: &MetaShard, command: MetadataCommand) -> Result<(), String> {
    shard
        .execute(&command)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn ordinary_command_readback(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    control: FdbControlStore,
    shard_a: MetaShard,
    session_a: OwnerSession,
) -> Result<String, String> {
    require_root_fence(&shard_a, context, RootActivationState::Active)?;
    let after_apply = shard_a
        .current_read_version()
        .map_err(|error| error.to_string())?;
    let original_version = after_apply
        .get()
        .checked_sub(1)
        .and_then(|value| ReadVersion::new(value).ok())
        .ok_or_else(|| "ordinary command read-version underflow".to_owned())?;
    let command = ordinary_command_at(context, original_version)?;
    let applied_value = shard_a
        .read_at(
            context.identity.root_id,
            context.identity.placement_generation,
            owner_epoch(1),
            MetadataFamily::WorkspaceCurrent,
            &ordinary_key(context),
            after_apply,
        )
        .map_err(|error| error.to_string())?;
    if applied_value.as_deref() != Some(ORDINARY_VALUE) {
        return Err("ordinary command durable value differs after lost acknowledgement".to_owned());
    }

    control
        .fail_closed(&session_a)
        .map_err(|error| error.to_string())?;
    let closed = control
        .observe_ownership(&context.identity.shard_id)
        .map_err(|error| error.to_string())?;
    if closed.route().state() != nokv_control::ShardRouteState::FailClosed {
        return Err("owner A did not fail closed before takeover".to_owned());
    }
    thread::sleep(Duration::from_millis(LEASE_TTL_MILLIS * 3));
    let session_b = control
        .acquire_owner(
            &context.identity.shard_id,
            context.identity.owner_b.clone(),
            context.identity.endpoint_b.clone(),
        )
        .map_err(|error| error.to_string())?;
    if session_b.owner_epoch().get() != 2 || session_b.session_generation().get() != 2 {
        return Err("successor session did not advance both fencing counters".to_owned());
    }
    let shard_b = MetaShard::open(
        metadata_store(runtime, context, &control, &session_b)?,
        context.identity.shard_id,
    )
    .map_err(|error| error.to_string())?;
    shard_b
        .advance_owner_epoch(Some(owner_epoch(1)), owner_epoch(2))
        .map_err(|error| error.to_string())?;
    let replay = shard_b
        .execute(&command)
        .map_err(|error| error.to_string())?;
    if !replay.replayed
        || replay.commit_version.get() != after_apply.get()
        || replay.deterministic_result != ORDINARY_RESULT
        || shard_b
            .current_read_version()
            .map_err(|error| error.to_string())?
            != after_apply
    {
        return Err("successor did not return the exact durable dedupe result".to_owned());
    }
    let replayed_value = shard_b
        .read_at(
            context.identity.root_id,
            context.identity.placement_generation,
            owner_epoch(2),
            MetadataFamily::WorkspaceCurrent,
            &ordinary_key(context),
            after_apply,
        )
        .map_err(|error| error.to_string())?;
    if replayed_value.as_deref() != Some(ORDINARY_VALUE) {
        return Err("successor readback differs from the one applied mutation".to_owned());
    }
    Ok(
        "A failed closed; B advanced epoch and generation; byte-identical request replayed once"
            .to_owned(),
    )
}

fn require_root_fence(
    shard: &MetaShard,
    context: &ScenarioContext,
    activation_state: RootActivationState,
) -> Result<(), String> {
    let expected = RootFence {
        logical_shard_id: context.identity.shard_id,
        object_namespace_id: Some(context.identity.object_namespace_id),
        placement_generation: context.identity.placement_generation,
        activation_state,
    };
    let actual = shard
        .root_fence(context.identity.root_id)
        .map_err(|error| error.to_string())?;
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(format!(
            "root-fence readback differs from exact expected record: {actual:?}"
        ))
    }
}

fn ordinary_key(context: &ScenarioContext) -> Vec<u8> {
    [
        context.identity.root_id.as_bytes().as_slice(),
        b"/gate2/ordinary-command".as_slice(),
    ]
    .concat()
}

fn physical_metadata_key(
    context: &ScenarioContext,
    keyspace: u16,
    logical_key: &[u8],
) -> Result<Vec<u8>, String> {
    let prefix =
        FdbStorePrefix::new(context.prefix.as_bytes()).map_err(|error| error.to_string())?;
    let subspace = prefix
        .subspace(FdbSubspaceKind::Metadata)
        .component(&keyspace.to_be_bytes())
        .map_err(|error| error.to_string())?;
    Ok(subspace.key(logical_key))
}

fn tagged_request(request: RequestId, tag: u8) -> RequestId {
    let mut bytes = *request.as_bytes();
    bytes[15] ^= tag;
    RequestId::from_bytes(bytes)
}

fn metadata_child_result(
    scenario: Scenario,
    result: Result<(), MetaError>,
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
        Err(
            error @ MetaError::Store {
                source: StoreError::OutcomeUnknown { .. },
                ..
            },
        ) => Ok(ChildResult {
            scenario,
            outcome: "commit_outcome_unknown".to_owned(),
            typed_error: Some(error.to_string()),
        }),
        Err(error) => Err(format!(
            "{scenario} returned unexpected metadata error: {error}"
        )),
    }
}
