/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

/// Stable identity of one physical metadata-shard process.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeIdError {
    Empty,
    NonCanonical,
}

impl NodeId {
    pub fn new(value: impl Into<String>) -> Result<Self, NodeIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(NodeIdError::Empty);
        }
        if value.trim() != value || value.chars().any(char::is_control) {
            return Err(NodeIdError::NonCanonical);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for NodeId {
    type Error = NodeIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for NodeId {
    type Error = NodeIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for NodeIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("node id must not be empty"),
            Self::NonCanonical => formatter
                .write_str("node id must not contain surrounding whitespace or control characters"),
        }
    }
}

impl std::error::Error for NodeIdError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_is_nonempty_and_canonical() {
        assert_eq!(NodeId::new("node-a").unwrap().as_str(), "node-a");
        assert_eq!(NodeId::new(""), Err(NodeIdError::Empty));
        assert_eq!(NodeId::new(" node-a"), Err(NodeIdError::NonCanonical));
        assert_eq!(NodeId::new("node\na"), Err(NodeIdError::NonCanonical));
    }
}
