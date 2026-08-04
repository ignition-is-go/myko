//! Round-trip regression tests for the wire protocol.
//!
//! These tests exist because rmp_serde was found to silently corrupt
//! MykoMessage::ReportResponse on round-trip (see commit history). They
//! must pass for ciborium and serve as the gate for the binary path.

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::wire::{MykoMessage, ReportResponse};

    fn sample_report_response() -> MykoMessage {
        MykoMessage::ReportResponse(ReportResponse {
            response: json!({
                "rows": [
                    { "id": "row-1", "value": 42, "label": "alpha" },
                    { "id": "row-2", "value": -17, "label": "beta" },
                ],
                "total": 2,
                "metadata": {
                    "duration_ms": 12.5,
                    "cached": false,
                }
            }),
            tx: "tx-abc-123".to_string(),
        })
    }

    /// Gate test: ciborium must round-trip MykoMessage::ReportResponse cleanly.
    /// If this fails, the CBOR migration halts and the spec is reopened.
    #[test]
    fn report_response_roundtrip_cbor() {
        let original = sample_report_response();

        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&original, &mut bytes).expect("ciborium encode");

        let roundtripped: MykoMessage =
            ciborium::de::from_reader(bytes.as_slice()).expect("ciborium decode");

        let original_json = serde_json::to_value(&original).unwrap();
        let roundtripped_json = serde_json::to_value(&roundtripped).unwrap();
        assert_eq!(
            original_json, roundtripped_json,
            "ciborium roundtrip should preserve ReportResponse"
        );
    }

    fn assert_roundtrip(msg: MykoMessage) {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&msg, &mut bytes).expect("ciborium encode");
        let roundtripped: MykoMessage =
            ciborium::de::from_reader(bytes.as_slice()).expect("ciborium decode");
        assert_eq!(
            serde_json::to_value(&msg).unwrap(),
            serde_json::to_value(&roundtripped).unwrap(),
            "roundtrip mismatch for {:?}",
            msg,
        );
    }

    #[test]
    fn ping_roundtrip_cbor() {
        assert_roundtrip(MykoMessage::Ping(crate::wire::PingData {
            id: "ping-1".into(),
            timestamp: 1_700_000_000_000,
        }));
    }

    #[test]
    fn query_cancel_roundtrip_cbor() {
        assert_roundtrip(MykoMessage::QueryCancel(crate::wire::CancelSubscription {
            tx: "tx-cancel-1".into(),
        }));
    }

    #[test]
    fn command_error_roundtrip_cbor() {
        assert_roundtrip(MykoMessage::CommandError(crate::wire::CommandError::new(
            "tx-cmd-1",
            "MyCommand",
            "validation failed: name is required",
        )));
    }
}
