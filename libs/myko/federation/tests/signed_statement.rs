use std::error::Error;

use myko_federation::{RetainedHistoryStatement, SignedRetainedHistoryStatement};

type TestResult = Result<(), Box<dyn Error>>;

fn statement() -> Result<RetainedHistoryStatement, Box<dyn Error>> {
    let encoded = serde_json::json!({
        "holder": "01010101-0101-0101-0101-010101010101",
        "storage_incarnation": "02020202-0202-0202-0202-020202020202",
        "obligation": {
            "node_id": "03030303-0303-0303-0303-030303030303",
            "sequence": 7
        },
        "selection": {"type": "exact", "scope_id": "signed:test"},
        "commitment": {"digest": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                       "event_count": 0}
    });
    Ok(serde_json::from_value(encoded)?)
}

#[test]
fn signed_statement_round_trips_exact_unverified_bytes() -> TestResult {
    let statement = statement()?;
    let signed =
        SignedRetainedHistoryStatement::from_signature(statement.clone(), [4; 32], [5; 64]);
    let decoded: SignedRetainedHistoryStatement =
        serde_json::from_slice(&serde_json::to_vec(&signed)?)?;
    if decoded.statement() != &statement
        || decoded.signer() != &[4; 32]
        || decoded.signature() != &[5; 64]
    {
        return Err("signed statement bytes changed during round trip".into());
    }
    Ok(())
}

#[test]
fn signed_statement_rejects_wrong_signature_lengths() -> TestResult {
    let signed = SignedRetainedHistoryStatement::from_signature(statement()?, [4; 32], [5; 64]);
    let mut encoded = serde_json::to_value(signed)?;
    let signature = encoded
        .get_mut("signature")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("serialized signature was not a byte array")?;
    signature.pop();
    if serde_json::from_value::<SignedRetainedHistoryStatement>(encoded).is_ok() {
        return Err("63-byte signature was accepted".into());
    }
    Ok(())
}

#[test]
fn signed_statement_rejects_wrong_signer_length_and_unknown_fields() -> TestResult {
    let signed = SignedRetainedHistoryStatement::from_signature(statement()?, [4; 32], [5; 64]);
    let mut short_key = serde_json::to_value(&signed)?;
    short_key
        .get_mut("signer")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("signer was not a byte array")?
        .pop();
    if serde_json::from_value::<SignedRetainedHistoryStatement>(short_key).is_ok() {
        return Err("31-byte verification key was accepted".into());
    }
    let mut unknown = serde_json::to_value(&signed)?;
    unknown
        .as_object_mut()
        .ok_or("signed statement was not an object")?
        .insert("trusted".to_owned(), serde_json::Value::Bool(true));
    if serde_json::from_value::<SignedRetainedHistoryStatement>(unknown).is_ok() {
        return Err("unrecognized signed-envelope fields were accepted".into());
    }
    let mut future = serde_json::to_value(&signed)?;
    future
        .as_object_mut()
        .ok_or("signed statement was not an object")?
        .insert(
            "format".to_owned(),
            serde_json::json!("ed25519_statement_v2"),
        );
    if serde_json::from_value::<SignedRetainedHistoryStatement>(future).is_ok() {
        return Err("an unsupported signature format was accepted".into());
    }
    Ok(())
}
