//! Serialized application-handler requests and updates.
//!
//! These shapes belong to the canonical node protocol. Application handlers
//! remain typed until the session converts their pending updates into these
//! transport-facing values.

use myko_federation::{HandlerKind, NodeId, ScopeId, ServiceId, SubscriptionLiveness};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Monotonic position of one application-handler stream.
///
/// Reopening a handler creates a new epoch. Within that epoch every state or
/// keyed delta advances the sequence exactly once, allowing clients to detect
/// dropped, duplicated, or reordered revisions before mutating local state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerStreamRevision {
    pub epoch: u64,
    pub sequence: u64,
}

/// Transport-neutral request for one registered reactive handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerRequest {
    pub kind: HandlerKind,
    /// Expected registered owner. The serving application validates this identity.
    pub service_id: Option<ServiceId>,
    pub handler_id: String,
    pub source_node: Option<NodeId>,
    pub scope_id: Option<ScopeId>,
    pub params: Value,
}

/// Type-erased lifecycle state used only at transport boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasedHandlerState {
    /// An omitted field carries no value; explicit null is a published nullable result.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_value"
    )]
    pub value: Option<Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_value"
    )]
    pub through: Option<Value>,
    pub liveness: SubscriptionLiveness,
    /// Stable row identities for a keyed collection snapshot.
    ///
    /// Scalar reports omit this field. Collection handlers include one key
    /// for each value in the same order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_keys: Option<Vec<String>>,
}

/// One keyed row crossing a transport boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasedKeyedValue {
    pub key: String,
    pub value: Value,
}

/// One incremental update to a keyed application view.
///
/// A handler stream starts with an [`ErasedHandlerState`] snapshot. Later
/// revisions send only changed rows while retaining the authoritative cursor,
/// liveness, and ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasedViewDelta {
    pub upserts: Vec<ErasedKeyedValue>,
    pub deletes: Vec<String>,
    /// Replacement row ordering when membership or order changed.
    ///
    /// `None` retains the preceding order, so a content-only update does not
    /// repeat every row ID.
    pub order: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_value"
    )]
    pub through: Option<Value>,
    pub liveness: SubscriptionLiveness,
}

fn deserialize_present_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_state_preserves_absent_and_present_null_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        for value in [
            Some(Value::Null),
            None,
            Some(serde_json::json!({"count": 1})),
        ] {
            let state = ErasedHandlerState {
                value: value.clone(),
                through: value,
                liveness: SubscriptionLiveness::Current,
                row_keys: None,
            };
            let encoded = serde_json::to_value(&state)?;
            if serde_json::from_value::<ErasedHandlerState>(encoded.clone())? != state
                || encoded.get("value").is_some() != state.value.is_some()
                || encoded.get("through").is_some() != state.through.is_some()
            {
                return Err("handler state lost absent/present-null distinctions".into());
            }
        }
        Ok(())
    }

    #[test]
    fn view_delta_preserves_absent_and_present_null_cursors()
    -> Result<(), Box<dyn std::error::Error>> {
        for through in [
            Some(Value::Null),
            None,
            Some(serde_json::json!({"offset": 1})),
        ] {
            let delta = ErasedViewDelta {
                upserts: Vec::new(),
                deletes: Vec::new(),
                order: None,
                through,
                liveness: SubscriptionLiveness::Current,
            };
            let encoded = serde_json::to_value(&delta)?;
            if serde_json::from_value::<ErasedViewDelta>(encoded.clone())? != delta
                || encoded.get("through").is_some() != delta.through.is_some()
            {
                return Err("view delta lost absent/present-null cursor distinctions".into());
            }
        }
        Ok(())
    }

    #[test]
    fn handler_request_round_trips_without_application_types() {
        let request = HandlerRequest {
            kind: HandlerKind::View,
            service_id: Some(ServiceId::new("messages")),
            handler_id: "conversation_messages".to_owned(),
            source_node: Some(NodeId::new()),
            scope_id: None,
            params: serde_json::json!({"conversation_id": "conversation-1"}),
        };

        let encoded = serde_json::to_vec(&request);
        assert!(encoded.is_ok(), "handler request did not encode");
        let Some(encoded) = encoded.ok() else {
            return;
        };
        let decoded = serde_json::from_slice::<HandlerRequest>(&encoded);
        assert!(decoded.is_ok(), "handler request did not decode");
        let Some(decoded) = decoded.ok() else {
            return;
        };

        assert_eq!(request, decoded);
    }

    #[test]
    fn keyed_view_delta_round_trips_without_repeating_order() {
        let delta = ErasedViewDelta {
            upserts: vec![ErasedKeyedValue {
                key: "message-2".to_owned(),
                value: serde_json::json!({"body": "updated"}),
            }],
            deletes: Vec::new(),
            order: None,
            through: Some(serde_json::json!({"offset": 42})),
            liveness: SubscriptionLiveness::Current,
        };

        let encoded = serde_json::to_vec(&delta);
        assert!(encoded.is_ok(), "handler delta did not encode");
        let Some(encoded) = encoded.ok() else {
            return;
        };
        let decoded = serde_json::from_slice::<ErasedViewDelta>(&encoded);
        assert!(decoded.is_ok(), "handler delta did not decode");
        let Some(decoded) = decoded.ok() else {
            return;
        };

        assert_eq!(delta, decoded);
        assert!(decoded.order.is_none());
    }
}
