//! Built-in provider bindings for the public provider v1 contract.

#[cfg(feature = "foundationdb-provider")]
mod foundationdb;
mod holt;

#[cfg(feature = "foundationdb-provider")]
pub(crate) use foundationdb::FoundationDbProviderFactory;
#[cfg(feature = "foundationdb-provider")]
pub use foundationdb::{
    FoundationDbProviderConfig, FoundationDbProviderConfigError, FoundationDbRuntime,
    FoundationDbRuntimeError,
};
#[cfg(test)]
pub(crate) use holt::HoltProvider;
pub(crate) use holt::HoltProviderFactory;

pub(crate) use crate::provider::v1::{
    AtomicCommitOutcome, AtomicOp, AtomicPlan, MetadataProvider, MetadataReadView,
    MetadataTransaction, OrderedSpaceId, ProviderCapabilities, ProviderContractOfferV1,
    ProviderCreateRequestV1, ProviderError, ProviderErrorKind, ProviderInstanceToken,
    ProviderOperationV1, ProviderRecord, ProviderReopenRequestV1, ProviderScan, ProviderScanItem,
    ProviderScanPage, ProviderScanStats, ProviderSchemaV1, ProviderTransactionModel,
    ProviderVersionModel, ReadScope, ReadWitness,
};

pub(super) use super::provider_catalog::all_ordered_spaces;

#[cfg(test)]
mod tests;
