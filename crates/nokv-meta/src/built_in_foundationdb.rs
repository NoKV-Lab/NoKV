/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Configured factory entry point for the built-in FoundationDB adapter.
//!
//! FoundationDB runtime and transaction policy are resolved before entering
//! provider SPI v1. Generic create and reopen requests therefore contain only
//! the engine-owned schema and expected durable store identity.

use std::sync::Arc;

use crate::provider::v1::{MetadataProviderFactoryV1, ProviderContractOfferV1, ProviderError};
use crate::workspace::provider::FoundationDbProviderFactory;

pub use crate::workspace::provider::{
    FoundationDbProviderConfig, FoundationDbProviderConfigError, FoundationDbRuntime,
    FoundationDbRuntimeError,
};

/// Return the exact configured FoundationDB offer without starting the process
/// runtime or opening a database/namespace.
pub fn contract_offer_v1(
    config: FoundationDbProviderConfig,
) -> Result<ProviderContractOfferV1, ProviderError> {
    FoundationDbProviderFactory::contract_offer_for_config(config)
}

/// Capture one process runtime and validated transaction policy in an SPI v1
/// factory without opening a metadata namespace.
pub fn provider_factory_v1(
    runtime: FoundationDbRuntime,
    config: FoundationDbProviderConfig,
) -> Result<Arc<dyn MetadataProviderFactoryV1>, ProviderError> {
    Ok(Arc::new(FoundationDbProviderFactory::new(runtime, config)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::admission::{admit_provider_offer_v1, ProviderAdmissionCode};

    #[test]
    fn public_factory_entrypoint_has_the_provider_v1_shape() {
        let _: fn(
            FoundationDbRuntime,
            FoundationDbProviderConfig,
        ) -> Result<Arc<dyn MetadataProviderFactoryV1>, ProviderError> = provider_factory_v1;
    }

    #[test]
    fn contract_offer_is_pure_and_reports_the_exact_unqualified_limits() {
        let offer = contract_offer_v1(FoundationDbProviderConfig::default()).unwrap();
        let report =
            admit_provider_offer_v1(&crate::workspace::canonical_provider_schema_v1(), &offer);
        assert_eq!(
            report.rejection_codes,
            vec![
                ProviderAdmissionCode::AmbiguousCommitMayRemainInFlight,
                ProviderAdmissionCode::AtomicOperationLimitTooSmall,
                ProviderAdmissionCode::LogicalPlanLimitTooSmall,
                ProviderAdmissionCode::ReadViewLifetimeBounded,
            ]
        );
    }
}
