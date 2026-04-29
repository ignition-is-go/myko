//! WebSocket protocol types
//!
//! Re-exports and helpers for the existing wire protocol.

pub use crate::wire::MykoMessage;

/// Serialize a MykoMessage to CBOR bytes.
pub fn message_to_cbor(msg: &MykoMessage) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(msg, &mut bytes)?;
    Ok(bytes)
}

/// Serialize a MykoMessage to JSON.
pub fn message_to_json(msg: &MykoMessage) -> Result<String, serde_json::Error> {
    serde_json::to_string(msg)
}
