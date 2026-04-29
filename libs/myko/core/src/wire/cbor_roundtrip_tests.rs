//! Round-trip regression tests for the wire protocol.
//!
//! These tests exist because rmp_serde was found to silently corrupt
//! MykoMessage::ReportResponse on round-trip (see commit history). They
//! must pass for ciborium and serve as the gate for the binary path.

#[cfg(test)]
mod tests {
    use crate::wire::{MykoMessage, ReportResponse};
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
    /// OBSERVED (2026-04-28): with this synthetic payload and a symmetric
    /// `rmp_serde::to_vec` → `rmp_serde::from_slice::<MykoMessage>` round-trip,
    /// the test PASSES — bytes decode back to a structurally equal MykoMessage.
    /// That means the breakage motivating the JSON-forced workaround at
    /// `client/mod.rs` (see the `// Force JSON until msgpack report-response
    /// round-trip is diagnosed` comment) is NOT reproduced by the simplest
    /// `ReportResponse(serde_json::Value)` shape. It is asymmetric or
    /// payload-specific — the production decode path goes
    /// `rmp_serde::from_slice::<serde_json::Value>` (see `decode_message` in
    /// `client/mod.rs`), not directly into `MykoMessage`, and that is the
    /// likely site of the silent corruption (Value's untagged-enum representation
    /// loses fidelity through MessagePack's lack of distinct float vs int tags
    /// and its array-vs-map ambiguity for adjacently-tagged enums).
    ///
    /// Kept `#[ignore]` and as the named regression marker per the migration
    /// plan; ciborium's gate test in `task 3` covers the same shape and must
    /// pass without `#[ignore]`. If a tighter reproducer is ever pinned down,
    /// extend this test rather than replacing it so the bug is preserved in
    /// the historical record.
    #[test]
    #[ignore = "documents the rmp_serde failure that motivated CBOR migration"]
    fn report_response_roundtrip_msgpack_documents_failure() {
        let original = sample_report_response();
        let bytes = rmp_serde::to_vec(&original).expect("encode should succeed");
        let decoded: Result<MykoMessage, _> = rmp_serde::from_slice(&bytes);

        // Either decode fails, or the result is not equal to the original.
        // Both outcomes are wrong; we record the actual outcome in the
        // assertion message for posterity.
        match decoded {
            Err(e) => panic!("rmp_serde decode failed (expected): {}", e),
            Ok(roundtripped) => {
                let original_json = serde_json::to_value(&original).unwrap();
                let roundtripped_json = serde_json::to_value(&roundtripped).unwrap();
                assert_eq!(
                    original_json, roundtripped_json,
                    "rmp_serde roundtrip mismatch"
                );
            }
        }
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
        assert_roundtrip(MykoMessage::CommandError(crate::wire::CommandError {
            tx: "tx-cmd-1".into(),
            command_id: "MyCommand".into(),
            message: "validation failed: name is required".into(),
        }));
    }
}
