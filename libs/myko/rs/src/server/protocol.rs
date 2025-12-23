//! WebSocket protocol types
//!
//! Re-exports and helpers for the existing wire protocol.

pub use crate::wire::{
    CancelSubscription, MykoMessage, QueryError, QueryResponse, ReportError, ReportResponse,
};

/// Serialize a MykoMessage to MessagePack bytes.
pub fn message_to_msgpack(msg: &MykoMessage) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec(msg)
}

/// Serialize a MykoMessage to JSON.
pub fn message_to_json(msg: &MykoMessage) -> Result<String, serde_json::Error> {
    serde_json::to_string(msg)
}
