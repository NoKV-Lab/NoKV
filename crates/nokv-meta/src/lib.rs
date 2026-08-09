//! Provider-neutral workspace metadata for NoKV Agent infrastructure.
//!
//! The [`workspace`] module is the complete durable schema and execution
//! surface. Provider bindings lower its ordered read views and atomic plans;
//! they do not own workspace commands, recovery, or authority semantics.

#[cfg(feature = "foundationdb-provider")]
pub mod built_in_foundationdb;
pub mod built_in_holt;
pub mod provider;
pub mod workspace;
