//! Round-trip regression tests for the CBOR wire protocol.
//!
//! These verify that MykoMessage variants round-trip cleanly through ciborium
//! and that the binary-frame -> serde_json::Value decode path the client
//! dispatches over produces a Value::Object with the "event" tag intact.

#[cfg(test)]
mod tests {
    use crate::wire::{
        CancelSubscription, CommandError, MykoMessage, PingData, ReportResponse,
    };
    use serde_json::json;

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
    ///
    /// This test exercises the `bytes -> MykoMessage` decode path. The
    /// `report_response_dispatch_via_cbor_preserves_event_tag` test below
    /// covers the `bytes -> serde_json::Value` decode path used by the
    /// client dispatcher and is the more critical of the two.
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

    /// Gate test for the client dispatch path.
    ///
    /// The client decodes incoming binary frames as `serde_json::Value`
    /// (see client/mod.rs WsFrame::Binary arm) and dispatches via
    /// `value.get("event")`. The `MykoMessage → bytes → Value` path must
    /// produce a `Value::Object` with the "event" and "data" keys intact.
    #[test]
    fn report_response_dispatch_via_cbor_preserves_event_tag() {
        let original = sample_report_response();

        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&original, &mut bytes).expect("ciborium encode");

        let value: serde_json::Value =
            ciborium::de::from_reader(bytes.as_slice()).expect("decode as Value");

        assert!(
            value.is_object(),
            "ciborium should produce Value::Object for tagged-enum encoding; got: {:?}",
            value,
        );

        let event_tag = value.get("event").and_then(|v| v.as_str());
        assert_eq!(
            event_tag,
            Some("ws:m:report-response"),
            "ciborium should preserve the 'event' key for dispatch; got value: {:?}",
            value,
        );

        // Content fidelity of the `data` payload is covered by the round-trip
        // test above; here we only need the key to be present so the dispatcher
        // can route the message.
        assert!(
            value.get("data").is_some(),
            "ciborium should preserve the 'data' key alongside 'event'; got value: {:?}",
            value,
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
        assert_roundtrip(MykoMessage::Ping(PingData {
            id: "ping-1".into(),
            timestamp: 1_700_000_000_000,
        }));
    }

    #[test]
    fn query_cancel_roundtrip_cbor() {
        assert_roundtrip(MykoMessage::QueryCancel(CancelSubscription {
            tx: "tx-cancel-1".into(),
        }));
    }

    #[test]
    fn command_error_roundtrip_cbor() {
        assert_roundtrip(MykoMessage::CommandError(CommandError {
            tx: "tx-cmd-1".into(),
            command_id: "MyCommand".into(),
            message: "validation failed: name is required".into(),
        }));
    }
}
