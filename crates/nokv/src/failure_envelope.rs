/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The CLI failure contract.
//!
//! A failed client command writes exactly one JSON line to stderr, nothing to
//! stdout, and exits with the code of its failure class. The line is the
//! stable Agent error envelope plus `class` and `next_action`.

use nokv_agent::AgentError;
use nokv_types::{ErrorClass, IdempotencyScope, NextAction};
use serde_json::{json, Value};

/// Version of the CLI's observable contract, reported by `nokv version --json`.
pub(crate) const CLI_CONTRACT: &str = "nokv.cli.contract.v1";

/// The exit code of a failure class. Success is `0` and a Rust panic `101`;
/// both stay outside this table.
pub(crate) const fn exit_code(class: ErrorClass) -> u8 {
    match class {
        ErrorClass::Internal => 1,
        ErrorClass::InvalidInput => 2,
        ErrorClass::NotFound => 3,
        ErrorClass::PreconditionFailed => 4,
        ErrorClass::Transient => 5,
        ErrorClass::OutcomeUnknown | ErrorClass::InProgress => 6,
        ErrorClass::IdentityConflict => 7,
        ErrorClass::Incompatible => 8,
        ErrorClass::OperatorRequired => 9,
    }
}

/// One failed CLI command.
#[derive(Debug)]
pub(crate) struct CliFailure {
    class: ErrorClass,
    next_action: NextAction,
    envelope: Value,
}

impl CliFailure {
    /// A failure the CLI itself raises for a command of `scope`.
    pub(crate) fn new(
        code: &str,
        message: impl Into<String>,
        class: ErrorClass,
        scope: IdempotencyScope,
    ) -> Self {
        Self::from_agent(
            &AgentError::backend(code, message, false, json!({})).with_class(class),
            scope,
        )
    }

    /// The command line or its arguments are invalid; nothing ran.
    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(
            "InvalidArguments",
            message,
            ErrorClass::InvalidInput,
            IdempotencyScope::ReadOnly,
        )
    }

    /// An Agent tool or SDK error, classified for a command of `scope`.
    pub(crate) fn from_agent(error: &AgentError, scope: IdempotencyScope) -> Self {
        let class = match scope {
            IdempotencyScope::ReadOnly => error.class().for_read(),
            _ => error.class(),
        };
        Self {
            class,
            next_action: class.next_action(scope),
            envelope: error.as_value(),
        }
    }

    pub(crate) fn with_next_action(mut self, next_action: NextAction) -> Self {
        self.next_action = next_action;
        self
    }

    pub(crate) fn details_mut(&mut self) -> &mut Value {
        &mut self.envelope["details"]
    }

    /// The human message, for operator commands that still report text.
    pub(crate) fn message(&self) -> String {
        self.envelope["message"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }

    pub(crate) const fn class(&self) -> ErrorClass {
        self.class
    }

    pub(crate) const fn next_action(&self) -> NextAction {
        self.next_action
    }

    pub(crate) const fn exit_code(&self) -> u8 {
        exit_code(self.class)
    }

    /// The one stderr line.
    pub(crate) fn to_line(&self) -> String {
        let mut value = self.envelope.clone();
        value["class"] = json!(self.class.as_str());
        value["next_action"] = json!(self.next_action.as_str());
        serde_json::to_string(&value).expect("a failure envelope contains only JSON values")
    }
}

impl From<String> for CliFailure {
    /// A failure no typed source classified.
    fn from(message: String) -> Self {
        Self::new(
            "Internal",
            message,
            ErrorClass::Internal,
            IdempotencyScope::ReadOnly,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn table_rows<'a>(document: &'a str, name: &str) -> Vec<&'a str> {
        let begin = format!("<!-- {name}:begin -->");
        let end = format!("<!-- {name}:end -->");
        let start = document.find(&begin).expect("table start marker") + begin.len();
        let stop = start + document[start..].find(&end).expect("table end marker");
        document[start..stop]
            .lines()
            .filter(|line| line.starts_with("| ") && !line.starts_with("| ---"))
            .skip(1)
            .collect()
    }

    #[test]
    fn the_published_contract_tables_match_the_code() {
        let document = include_str!("../../../docs/cli-contract.md");
        assert!(document.contains(CLI_CONTRACT));
        let exits = table_rows(document, "exit-codes");
        // One success row and one row per class.
        assert_eq!(exits.len(), ErrorClass::ALL.len() + 1);
        for class in ErrorClass::ALL {
            let row = format!("| {} | `{}` |", exit_code(class), class.as_str());
            assert!(exits.iter().any(|line| line.starts_with(&row)), "{row}");
        }
        let actions = table_rows(document, "next-actions");
        assert_eq!(actions.len(), NextAction::ALL.len());
        for action in NextAction::ALL {
            let row = format!("| `{}` |", action.as_str());
            assert!(actions.iter().any(|line| line.starts_with(&row)), "{row}");
        }
    }

    #[test]
    fn every_class_has_one_exit_code_outside_success_and_panic() {
        let codes: BTreeSet<_> = ErrorClass::ALL
            .iter()
            .filter(|class| **class != ErrorClass::InProgress)
            .map(|class| exit_code(*class))
            .collect();
        assert_eq!(codes.len(), ErrorClass::ALL.len() - 1);
        assert!(!codes.contains(&0) && !codes.contains(&101));
        // A running operation and an unknown outcome are both resolved by
        // observing the same identity.
        assert_eq!(
            exit_code(ErrorClass::InProgress),
            exit_code(ErrorClass::OutcomeUnknown)
        );
    }

    #[test]
    fn a_failure_is_one_line_of_the_agent_envelope_plus_its_class() {
        let failure = CliFailure::from_agent(
            &AgentError::backend(
                "Conflict",
                "path generation mismatch",
                false,
                json!({"current_generation": 2}),
            )
            .with_class(ErrorClass::PreconditionFailed),
            IdempotencyScope::CallerRequest,
        );
        let line = failure.to_line();
        assert!(!line.contains('\n'));
        let value: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["status"], "error");
        assert_eq!(value["code"], "Conflict");
        assert_eq!(value["retryable"], false);
        assert_eq!(value["details"]["current_generation"], 2);
        assert_eq!(value["class"], "precondition_failed");
        assert_eq!(value["next_action"], "reread_then_retry");
        assert_eq!(failure.exit_code(), 4);
    }

    #[test]
    fn a_read_reports_a_lost_reply_as_transient() {
        let lost = AgentError::backend("Transport", "connection reset", true, json!({}))
            .with_class(ErrorClass::OutcomeUnknown);
        let read = CliFailure::from_agent(&lost, IdempotencyScope::ReadOnly);
        assert_eq!(read.class(), ErrorClass::Transient);
        assert_eq!(read.next_action(), NextAction::ResubmitSame);
        let write = CliFailure::from_agent(&lost, IdempotencyScope::Unidentified);
        assert_eq!(write.class(), ErrorClass::OutcomeUnknown);
        assert_eq!(write.next_action(), NextAction::RereadThenRetry);
    }
}
