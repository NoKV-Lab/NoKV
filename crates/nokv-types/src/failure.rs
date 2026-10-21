/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Caller-facing failure classes and the action each one permits.
//!
//! A class states what a failure means for durable state, independent of the
//! transport that reported it. Every surface derives its retry guidance from
//! the class and from how the failed command identifies its effect, so a
//! caller never has to infer idempotency from a boolean.

/// What one failed request means for durable state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// The request is malformed or not valid for this command. Nothing ran.
    InvalidInput,
    /// The named workbench, path, snapshot, or operation does not exist.
    NotFound,
    /// A precondition no longer holds: create-only, expected generation,
    /// workspace incarnation, commit head, or lifecycle state. Nothing was
    /// applied; the caller must read current state before deciding again.
    PreconditionFailed,
    /// The request failed before it could take effect. Repeating it is safe.
    Transient,
    /// The request may or may not have taken effect.
    OutcomeUnknown,
    /// The identified operation is still running.
    InProgress,
    /// The identity was already used for other inputs. Nothing was applied.
    IdentityConflict,
    /// The client and owner do not share a protocol version or capability.
    Incompatible,
    /// Durable or control state needs operator action.
    OperatorRequired,
    /// An unexpected failure.
    Internal,
}

impl ErrorClass {
    pub const ALL: [Self; 10] = [
        Self::InvalidInput,
        Self::NotFound,
        Self::PreconditionFailed,
        Self::Transient,
        Self::OutcomeUnknown,
        Self::InProgress,
        Self::IdentityConflict,
        Self::Incompatible,
        Self::OperatorRequired,
        Self::Internal,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::NotFound => "not_found",
            Self::PreconditionFailed => "precondition_failed",
            Self::Transient => "transient",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::InProgress => "in_progress",
            Self::IdentityConflict => "identity_conflict",
            Self::Incompatible => "incompatible",
            Self::OperatorRequired => "operator_required",
            Self::Internal => "internal",
        }
    }

    /// The class a read observes: a read changes nothing, so a failure whose
    /// outcome would be unknown for a mutation is only transient for it.
    pub const fn for_read(self) -> Self {
        match self {
            Self::OutcomeUnknown | Self::InProgress => Self::Transient,
            other => other,
        }
    }

    /// The action a caller may take next, given how the failed command
    /// identifies its effect.
    pub const fn next_action(self, scope: IdempotencyScope) -> NextAction {
        match self {
            Self::InvalidInput => NextAction::FixInput,
            Self::NotFound => match scope {
                IdempotencyScope::ReadOnly => NextAction::DoNotRetry,
                _ => NextAction::RereadThenRetry,
            },
            Self::PreconditionFailed => NextAction::RereadThenRetry,
            Self::Transient => NextAction::ResubmitSame,
            Self::OutcomeUnknown => match scope {
                IdempotencyScope::CallerOperation => NextAction::QuerySame,
                IdempotencyScope::Unidentified => NextAction::RereadThenRetry,
                IdempotencyScope::CallerRequest
                | IdempotencyScope::ContentDerived
                | IdempotencyScope::Convergent
                | IdempotencyScope::ReadOnly => NextAction::ResubmitSame,
            },
            Self::InProgress => NextAction::Poll,
            Self::IdentityConflict | Self::Incompatible | Self::Internal => NextAction::DoNotRetry,
            Self::OperatorRequired => NextAction::Operator,
        }
    }
}

/// How a command identifies its effect, which decides how a caller resolves
/// an unknown outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IdempotencyScope {
    /// A caller-chosen operation id: the outcome can be queried by that id,
    /// and resubmitting the same id and inputs replays it.
    CallerOperation,
    /// A caller-chosen request id: resubmitting the same id and inputs
    /// replays it.
    CallerRequest,
    /// An identity derived from the content: resubmitting the same content
    /// replays it.
    ContentDerived,
    /// Repeating the command converges on the same state.
    Convergent,
    /// Each invocation mints a fresh identity: a repeat may apply twice.
    Unidentified,
    /// The command changes nothing.
    ReadOnly,
}

/// The action a caller may take after a failure or an observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NextAction {
    /// Nothing remains to do.
    None,
    /// Wait, then observe the same identity again.
    Poll,
    /// Observe the same identity before deciding.
    QuerySame,
    /// Submit the same command with the same identity and inputs.
    ResubmitSame,
    /// Ask the owner to retry cleanup of a quarantined operation.
    RetryCleanup,
    /// Retry the same cleanup request with its saved state digest.
    RetrySameCleanup,
    /// Read current state, then decide again under a new identity.
    RereadThenRetry,
    /// Change the request; repeating it unchanged fails the same way.
    FixInput,
    /// Stop; repeating the command cannot succeed.
    DoNotRetry,
    /// Stop and hand the state to an operator.
    Operator,
}

impl NextAction {
    pub const ALL: [Self; 10] = [
        Self::None,
        Self::Poll,
        Self::QuerySame,
        Self::ResubmitSame,
        Self::RetryCleanup,
        Self::RetrySameCleanup,
        Self::RereadThenRetry,
        Self::FixInput,
        Self::DoNotRetry,
        Self::Operator,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Poll => "poll",
            Self::QuerySame => "query_same",
            Self::ResubmitSame => "resubmit_same",
            Self::RetryCleanup => "retry_cleanup",
            Self::RetrySameCleanup => "retry_same_cleanup",
            Self::RereadThenRetry => "reread_then_retry",
            Self::FixInput => "fix_input",
            Self::DoNotRetry => "do_not_retry",
            Self::Operator => "operator",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn wire_names_are_distinct_snake_case() {
        let classes: BTreeSet<_> = ErrorClass::ALL.iter().map(|class| class.as_str()).collect();
        assert_eq!(classes.len(), ErrorClass::ALL.len());
        let actions: BTreeSet<_> = NextAction::ALL
            .iter()
            .map(|action| action.as_str())
            .collect();
        assert_eq!(actions.len(), NextAction::ALL.len());
        for name in classes.iter().chain(actions.iter()) {
            assert!(name
                .chars()
                .all(|character| character.is_ascii_lowercase() || character == '_'));
        }
    }

    #[test]
    fn an_unknown_outcome_is_resolved_through_the_commands_identity() {
        let class = ErrorClass::OutcomeUnknown;
        assert_eq!(
            class.next_action(IdempotencyScope::CallerOperation),
            NextAction::QuerySame
        );
        assert_eq!(
            class.next_action(IdempotencyScope::CallerRequest),
            NextAction::ResubmitSame
        );
        assert_eq!(
            class.next_action(IdempotencyScope::ContentDerived),
            NextAction::ResubmitSame
        );
        // Without an identity a repeat may apply twice: read first.
        assert_eq!(
            class.next_action(IdempotencyScope::Unidentified),
            NextAction::RereadThenRetry
        );
    }

    #[test]
    fn a_read_never_reports_an_unknown_outcome() {
        for class in ErrorClass::ALL {
            let read = class.for_read();
            assert_ne!(read, ErrorClass::OutcomeUnknown);
            assert_ne!(read, ErrorClass::InProgress);
        }
        assert_eq!(ErrorClass::OutcomeUnknown.for_read(), ErrorClass::Transient);
    }

    #[test]
    fn only_a_transient_failure_or_a_known_identity_permits_a_blind_repeat() {
        for class in ErrorClass::ALL {
            for scope in [
                IdempotencyScope::CallerOperation,
                IdempotencyScope::CallerRequest,
                IdempotencyScope::ContentDerived,
                IdempotencyScope::Convergent,
                IdempotencyScope::Unidentified,
                IdempotencyScope::ReadOnly,
            ] {
                if class.next_action(scope) == NextAction::ResubmitSame {
                    assert!(
                        class == ErrorClass::Transient
                            || (class == ErrorClass::OutcomeUnknown
                                && scope != IdempotencyScope::Unidentified
                                && scope != IdempotencyScope::CallerOperation),
                        "{class:?} under {scope:?}"
                    );
                }
            }
        }
    }
}
