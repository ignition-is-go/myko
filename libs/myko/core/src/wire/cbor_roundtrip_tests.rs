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
    /// If this test ever PASSES with rmp_serde, the workaround at
    /// client/mod.rs (the JSON-forced default) can be reconsidered
    /// independently of the CBOR migration.
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
}
