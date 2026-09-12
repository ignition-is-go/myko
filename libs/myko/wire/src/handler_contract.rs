//! Generated payload evidence from an activated application, not an execution permit.

use myko_federation::{HandlerKind, NodeId, ServiceId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::HandlerRequest;

/// One handler open and its optional inspected-contract precondition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandlerOpenRequest {
    pub request: HandlerRequest,
    pub observed_contract: Option<Box<HandlerContract>>,
}

/// A JSON Schema document root. Keyword validity and compatibility need separate checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SchemaDocument {
    Boolean(bool),
    Object(Map<String, Value>),
}

impl TryFrom<Value> for SchemaDocument {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(value) => Ok(Self::Boolean(value)),
            Value::Object(value) => Ok(Self::Object(value)),
            _ => Err("JSON Schema document must have an object or boolean root".to_owned()),
        }
    }
}

/// Distinct generated contracts for emitted and accepted values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeSchemaPair {
    pub serialization: SchemaDocument,
    pub deserialization: SchemaDocument,
}

/// Application payload shape inside the framework's result envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "schema", rename_all = "snake_case")]
pub enum HandlerResultContract {
    Value(TypeSchemaPair),
    Rows(TypeSchemaPair),
}

/// Metadata observed at a serving application. No assignment or readiness is implied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandlerContract {
    pub serving_node: NodeId,
    /// `None` identifies the explicit global-handler namespace.
    pub service_id: Option<ServiceId>,
    pub kind: HandlerKind,
    pub handler_id: String,
    pub arguments: TypeSchemaPair,
    pub result: HandlerResultContract,
}

impl HandlerContract {
    /// Validate response identity and framework result shape, not schema compatibility.
    ///
    /// # Errors
    /// Rejects a different handler, routed node, or result envelope.
    pub fn validate_for(
        &self,
        request: &HandlerRequest,
        destination: Option<NodeId>,
    ) -> Result<(), String> {
        if self.service_id != request.service_id
            || self.kind != request.kind
            || self.handler_id != request.handler_id
            || destination.is_some_and(|node| node != self.serving_node)
        {
            return Err(
                "handler contract does not match the requested executor and handler".to_owned(),
            );
        }
        let expected_rows = match request.kind {
            HandlerKind::Query | HandlerKind::View => true,
            HandlerKind::Report => false,
            HandlerKind::Command => {
                return Err("commands have no reactive handler contract".to_owned());
            }
        };
        if expected_rows != matches!(self.result, HandlerResultContract::Rows(_)) {
            return Err("handler contract has the wrong result envelope".to_owned());
        }
        Ok(())
    }
}
