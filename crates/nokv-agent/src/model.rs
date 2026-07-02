use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentNodeKind {
    Directory,
    File,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentNode {
    pub path: String,
    pub name: String,
    pub kind: AgentNodeKind,
    pub size_bytes: Option<u64>,
    pub content_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentFindField {
    pub id: String,
}

impl AgentFindField {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentPredicateOp {
    Eq,
    NotEqual,
    In,
    Prefix,
    Suffix,
    Contains,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Exists,
    NotExists,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AgentPredicateValue {
    String(String),
    U64(u64),
    F64(f64),
    List(Vec<AgentPredicateValue>),
}

impl AgentPredicateValue {
    pub fn from_json(value: &Value) -> Option<Self> {
        match value {
            Value::String(value) => Some(Self::String(value.clone())),
            Value::Number(value) => value
                .as_u64()
                .map(Self::U64)
                .or_else(|| value.as_f64().map(Self::F64)),
            Value::Bool(value) => Some(Self::U64(u64::from(*value))),
            Value::Array(values) => values
                .iter()
                .map(Self::from_json)
                .collect::<Option<Vec<_>>>()
                .map(Self::List),
            Value::Null | Value::Object(_) => None,
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            Self::String(value) => Value::String(value.clone()),
            Self::U64(value) => Value::Number((*value).into()),
            Self::F64(value) if value.is_finite() => serde_json::Number::from_f64(*value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Self::F64(_) => Value::Null,
            Self::List(values) => Value::Array(values.iter().map(Self::to_json).collect()),
        }
    }

    pub(crate) fn as_f64(&self) -> Option<f64> {
        match self {
            Self::U64(value) => Some(*value as f64),
            Self::F64(value) if value.is_finite() => Some(*value),
            _ => None,
        }
    }

    pub(crate) fn as_sort_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentIndexValue {
    pub field: AgentFindField,
    pub value: AgentPredicateValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentIndexField {
    pub field: AgentFindField,
    pub operators: Vec<AgentPredicateOp>,
    pub sortable: bool,
    pub facetable: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentIndexRow {
    pub path: String,
    pub values: Vec<AgentIndexValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentIndexRegistration {
    pub path: String,
    pub fields: Vec<AgentIndexField>,
    pub rows: Vec<AgentIndexRow>,
}
