//! Round-trip regression tests for the wire protocol.
//!
//! These tests exist because rmp_serde was found to silently corrupt
//! MykoMessage::ReportResponse on round-trip (see commit history). They
//! must pass for ciborium and serve as the gate for the binary path.

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

    /// Documents the failure mode that motivated this migration.
    ///
    /// This test passes by design today: it asserts the rmp_serde array-encoding
    /// failure is present, so it serves as a historical record of the bug, not as
    /// a regression alarm. It will be deleted along with the rmp_serde dependency
    /// in Task 18 of the migration plan.
    ///
    /// Observed failure (diagnosed 2026-04-24): the direct MykoMessage→msgpack→MykoMessage
    /// roundtrip via rmp_serde actually PASSES. The real failure is in the client dispatch
    /// path (client/mod.rs line 726): the client decodes binary frames as
    /// `rmp_serde::from_slice::<serde_json::Value>` before dispatching by event tag.
    /// rmp_serde::to_vec uses compact array-based encoding by default, so an
    /// adjacently-tagged MykoMessage (`#[serde(tag = "event", content = "data")]`)
    /// is serialized to `["ws:m:report-response", [{...response...}, "tx-abc-123"]]`
    /// — a JSON array — rather than `{"event": "ws:m:report-response", "data": {...}}`.
    /// The client's `value.get("event")` returns None, the report-response handler is
    /// silently never invoked, and the response field content is also scrambled (the
    /// response object and tx string are packed into an inner array instead of an object
    /// with `response`/`tx` keys). This is the silent data-loss bug that forced JSON mode.
    #[test]
    #[ignore = "documents the rmp_serde dispatch-path failure that motivated CBOR migration"]
    fn report_response_dispatch_via_msgpack_documents_failure() {
        let original = sample_report_response();

        // Encode through the same path the server uses (rmp_serde::to_vec).
        let bytes = rmp_serde::to_vec(&original).expect("encode should succeed");

        // Decode as serde_json::Value, mirroring the client dispatch path
        // at libs/myko/core/src/client/mod.rs (the WsFrame::Binary arm,
        // which uses rmp_serde::from_slice::<Value>(...)).
        let value: serde_json::Value =
            rmp_serde::from_slice(&bytes).expect("decode as Value should succeed");

        // The dispatcher reads value.get("event") to route messages.
        // rmp_serde's default array encoding produces a Value::Array,
        // not a Value::Object, so this lookup returns None and the
        // message is silently dropped.
        let event_tag = value.get("event");
        assert!(
            event_tag.is_none(),
            "rmp_serde unexpectedly produced an object with 'event' key — \
             the dispatch-path bug may have been fixed elsewhere; got: {:?}",
            value,
        );

        // Also assert the shape: it should be a Value::Array.
        assert!(
            value.is_array(),
            "rmp_serde should produce a Value::Array (compact tagged-enum encoding); \
             got: {:?}",
            value,
        );
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

    /// Gate test for the actual rmp_serde dispatch-path bug diagnosed in
    /// `report_response_dispatch_via_msgpack_documents_failure`.
    ///
    /// The client decodes incoming binary frames as `serde_json::Value`
    /// (see client/mod.rs WsFrame::Binary arm) and dispatches via
    /// `value.get("event")`. For ciborium to be a viable replacement, the
    /// `MykoMessage → bytes → Value` path must produce a `Value::Object`
    /// with the "event" and "data" keys intact, NOT a `Value::Array` like
    /// rmp_serde's compact tagged-enum encoding produces.
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
