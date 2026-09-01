/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Gate 2 scenario names and retained-evidence schemas.

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub(crate) const REQUIRED_REPETITIONS: u8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Scenario {
    MetadataInitialize,
    MetadataOwnerEpoch,
    MetadataRootFenceInstall,
    MetadataRootFenceActivate,
    MetadataOrdinaryCommand,
    MetadataLeaseClock,
    ControlManifestFormat,
    ControlShardCreate,
    ControlRootCreate,
    ControlRootReadyCas,
    ControlShardReadyCas,
    ControlProvisioningAcquire,
    ControlServingAcquire,
    ControlRenew,
    ControlActivate,
    ControlFailClose,
    ControlRelease,
}

impl Scenario {
    pub(crate) const ALL: [Self; 17] = [
        Self::MetadataInitialize,
        Self::MetadataOwnerEpoch,
        Self::MetadataRootFenceInstall,
        Self::MetadataRootFenceActivate,
        Self::MetadataOrdinaryCommand,
        Self::MetadataLeaseClock,
        Self::ControlManifestFormat,
        Self::ControlShardCreate,
        Self::ControlRootCreate,
        Self::ControlRootReadyCas,
        Self::ControlShardReadyCas,
        Self::ControlProvisioningAcquire,
        Self::ControlServingAcquire,
        Self::ControlRenew,
        Self::ControlActivate,
        Self::ControlFailClose,
        Self::ControlRelease,
    ];

    pub(crate) const fn slug(self) -> &'static str {
        match self {
            Self::MetadataInitialize => "metadata-initialize",
            Self::MetadataOwnerEpoch => "metadata-owner-epoch",
            Self::MetadataRootFenceInstall => "metadata-root-fence-install",
            Self::MetadataRootFenceActivate => "metadata-root-fence-activate",
            Self::MetadataOrdinaryCommand => "metadata-ordinary-command",
            Self::MetadataLeaseClock => "metadata-lease-clock",
            Self::ControlManifestFormat => "control-manifest-format",
            Self::ControlShardCreate => "control-shard-create",
            Self::ControlRootCreate => "control-root-create",
            Self::ControlRootReadyCas => "control-root-ready-cas",
            Self::ControlShardReadyCas => "control-shard-ready-cas",
            Self::ControlProvisioningAcquire => "control-provisioning-acquire",
            Self::ControlServingAcquire => "control-serving-acquire",
            Self::ControlRenew => "control-renew",
            Self::ControlActivate => "control-activate",
            Self::ControlFailClose => "control-fail-close",
            Self::ControlRelease => "control-release",
        }
    }

    pub(crate) const fn is_metadata(self) -> bool {
        matches!(
            self,
            Self::MetadataInitialize
                | Self::MetadataOwnerEpoch
                | Self::MetadataRootFenceInstall
                | Self::MetadataRootFenceActivate
                | Self::MetadataOrdinaryCommand
                | Self::MetadataLeaseClock
        )
    }

    pub(crate) const fn selector(self) -> Selector {
        match self {
            Self::MetadataInitialize
            | Self::MetadataOwnerEpoch
            | Self::MetadataRootFenceInstall
            | Self::MetadataRootFenceActivate
            | Self::MetadataOrdinaryCommand
            | Self::MetadataLeaseClock
            | Self::ControlManifestFormat
            | Self::ControlShardCreate
            | Self::ControlRootCreate
            | Self::ControlRootReadyCas
            | Self::ControlShardReadyCas => Selector::Ordinal,
            Self::ControlProvisioningAcquire
            | Self::ControlServingAcquire
            | Self::ControlRenew
            | Self::ControlActivate
            | Self::ControlFailClose
            | Self::ControlRelease => Selector::Armed,
        }
    }

    pub(crate) const fn mutation_kind(self) -> MutationKind {
        if matches!(self, Self::ControlRelease) {
            MutationKind::Clear
        } else {
            MutationKind::Set
        }
    }
}

impl fmt::Display for Scenario {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.slug())
    }
}

impl FromStr for Scenario {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|scenario| scenario.slug() == value)
            .ok_or_else(|| format!("unknown Gate 2 scenario {value:?}"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Selector {
    Ordinal,
    Armed,
}

impl Selector {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ordinal => "ordinal",
            Self::Armed => "armed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MutationKind {
    Set,
    Clear,
}

impl MutationKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Set => "set",
            Self::Clear => "clear",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct InjectorEvent {
    pub(crate) version: u8,
    pub(crate) event: String,
    pub(crate) nonce: String,
    pub(crate) pid: u64,
    pub(crate) tid: u64,
    pub(crate) selector_sha256: String,
    pub(crate) target_key_sha256: String,
    pub(crate) kind: String,
    pub(crate) mode: String,
    pub(crate) matching_mutations: u64,
    pub(crate) prearm_matches: u64,
    pub(crate) selected_transactions: u64,
    pub(crate) target_commits: u64,
    pub(crate) substitutions: u64,
    pub(crate) duplicate_matches: u64,
    pub(crate) arm_messages: u64,
    pub(crate) event_writes_before: u64,
    pub(crate) real_result: i32,
    pub(crate) substituted_result: i32,
    pub(crate) invalid: bool,
}

pub(crate) fn validate_injector_events(
    events: &[InjectorEvent],
    nonce: &str,
    target_key_sha256: &str,
    scenario: Scenario,
) -> Result<(), String> {
    validate_injector_events_exact(
        events,
        nonce,
        target_key_sha256,
        scenario.mutation_kind().as_str(),
        scenario.selector().as_str(),
    )
}

pub(crate) fn validate_injector_events_exact(
    events: &[InjectorEvent],
    nonce: &str,
    target_key_sha256: &str,
    mutation_kind: &str,
    selector_mode: &str,
) -> Result<(), String> {
    if events.len() != 2 {
        return Err(format!(
            "injector must emit one substitution and one summary, observed {} events",
            events.len()
        ));
    }
    let substitution = &events[0];
    let summary = &events[1];
    if substitution.event != "substitution" || summary.event != "summary" {
        return Err(format!(
            "injector event order is {:?}, {:?}",
            substitution.event, summary.event
        ));
    }
    for event in events {
        if event.version != 1
            || event.nonce != nonce
            || event.target_key_sha256 != target_key_sha256
            || event.kind != mutation_kind
            || event.mode != selector_mode
            || event.invalid
        {
            return Err(format!("injector event failed exact validation: {event:?}"));
        }
    }
    if substitution.real_result != 0
        || substitution.substituted_result != 1021
        || summary.selected_transactions != 1
        || summary.target_commits != 1
        || summary.substitutions != 1
        || summary.duplicate_matches != 0
        || (selector_mode == "armed" && summary.arm_messages != 1)
    {
        return Err(format!(
            "injector did not prove one successful commit acknowledgement substitution: {summary:?}"
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ChildResult {
    pub(crate) scenario: Scenario,
    pub(crate) outcome: String,
    pub(crate) typed_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ScenarioEvidence {
    pub(crate) phase: String,
    pub(crate) repetition: u8,
    pub(crate) scenario: Scenario,
    pub(crate) prefix_sha256: String,
    pub(crate) target_key_sha256: String,
    pub(crate) selector: Selector,
    pub(crate) mutation_kind: MutationKind,
    pub(crate) child: ChildResult,
    pub(crate) exact_readback: String,
    pub(crate) cleanup_verified: bool,
    pub(crate) status: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct EnvironmentEvidence {
    pub(crate) source_revision: String,
    pub(crate) source_dirty: bool,
    pub(crate) candidate_sha256: String,
    pub(crate) qualification_sha256: String,
    pub(crate) injector_sha256: String,
    pub(crate) fdb_cluster_file_sha256: String,
    pub(crate) fdb_client_sha256: String,
    pub(crate) rustfs_service_identity: String,
    pub(crate) rustfs_health_url: String,
    pub(crate) object_binding_sha256: String,
    pub(crate) owner_endpoints: [SocketAddr; 2],
    pub(crate) required_repetitions: u8,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CandidateEvidence {
    pub(crate) case: String,
    pub(crate) prefix_sha256: String,
    pub(crate) target_key_sha256: String,
    pub(crate) mutation_kind: String,
    pub(crate) ordinal: u64,
    pub(crate) candidate_outcome: String,
    pub(crate) exact_readback: String,
    pub(crate) cleanup_verified: bool,
    pub(crate) status: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CandidateOrdinaryEvidence {
    pub(crate) prefix_sha256: String,
    pub(crate) request_sha256: String,
    pub(crate) target_key_sha256: String,
    pub(crate) owner_a_epoch: u64,
    pub(crate) owner_a_generation: u64,
    pub(crate) owner_b_epoch: u64,
    pub(crate) owner_b_generation: u64,
    pub(crate) first_outcome: String,
    pub(crate) replayed: bool,
    pub(crate) commit_version: u64,
    pub(crate) seed_failover: String,
    pub(crate) cleanup_verified: bool,
    pub(crate) status: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TerminalResult {
    pub(crate) status: &'static str,
    pub(crate) source_revision: String,
    pub(crate) completed_scenarios: usize,
    pub(crate) required_scenarios: usize,
    pub(crate) candidate_cases_complete: bool,
    pub(crate) failure: Option<String>,
    pub(crate) inventory_sha256: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_catalog_is_complete_and_round_trips() {
        assert_eq!(Scenario::ALL.len(), 17);
        for scenario in Scenario::ALL {
            assert_eq!(scenario.slug().parse::<Scenario>(), Ok(scenario));
        }
    }

    #[test]
    fn lifecycle_selectors_are_armed() {
        for scenario in [
            Scenario::ControlProvisioningAcquire,
            Scenario::ControlServingAcquire,
            Scenario::ControlRenew,
            Scenario::ControlActivate,
            Scenario::ControlFailClose,
            Scenario::ControlRelease,
        ] {
            assert_eq!(scenario.selector(), Selector::Armed);
        }
    }
}
