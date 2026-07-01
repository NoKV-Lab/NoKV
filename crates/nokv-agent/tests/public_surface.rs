//! Black-box characterization of `nokv-agent`'s public surface.
//!
//! This is the TDD gate for the lift: it pins the two contracts the downstream
//! consumers (the bench harness, the `nokv-client` re-export) rely on — the set
//! of read tools, and that the error type is a real `std::error::Error` (the
//! bench's `from_nokv(err: impl Error)` funnel depends on this). It must fail to
//! compile until the agent tooling layer is lifted into this crate.

use nokv_agent::{agent_tool_definitions, AgentError};

#[test]
fn exposes_exactly_the_seven_read_tools() {
    let mut names: Vec<&str> = agent_tool_definitions().iter().map(|t| t.name).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        ["aggregate", "catalog", "find", "grep", "ls", "read", "stat"]
    );
}

#[test]
fn agent_error_implements_std_error() {
    fn assert_is_error<E: std::error::Error>() {}
    assert_is_error::<AgentError>();
}
