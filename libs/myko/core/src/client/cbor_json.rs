use std::fmt;

use serde::{Deserialize, de};
use serde_json::{Map, Number, Value};

pub(super) fn from_slice(bytes: &[u8]) -> Result<Value, ciborium::de::Error<std::io::Error>> {
    ciborium::de::from_reader::<JsonValue, _>(bytes).map(|value| value.0)
}

struct JsonValue(Value);

impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonValueVisitor)
    }
}

struct JsonValueVisitor;

impl<'de> de::Visitor<'de> for JsonValueVisitor {
    type Value = JsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a CBOR value representable as JSON")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Number(value.into())))
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        i64::try_from(value)
            .map_err(E::custom)
            .and_then(|value| self.visit_i64(value))
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        u64::try_from(value)
            .map_err(E::custom)
            .and_then(|value| self.visit_u64(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .map(JsonValue)
            .ok_or_else(|| E::custom("non-finite CBOR float is not representable as JSON"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::String(value)))
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E> {
        Ok(JsonValue(bytes_to_json(value)))
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E> {
        Ok(JsonValue(bytes_to_json(&value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        JsonValue::deserialize(deserializer)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Null))
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        JsonValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or_default());
        while let Some(value) = sequence.next_element::<JsonValue>()? {
            values.push(value.0);
        }
        Ok(JsonValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut values = Map::with_capacity(map.size_hint().unwrap_or_default());
        while let Some((key, value)) = map.next_entry::<String, JsonValue>()? {
            values.insert(key, value.0);
        }
        Ok(JsonValue(Value::Object(values)))
    }
}

fn bytes_to_json(bytes: &[u8]) -> Value {
    // UUID's non-human-readable serde representation is an untagged 16-byte
    // string, so length is the only information available when restoring the
    // textual representation expected by JSON-deserialized entity types.
    if let Ok(uuid) = uuid::Uuid::from_slice(bytes) {
        return Value::String(uuid.to_string());
    }

    Value::Array(
        bytes
            .iter()
            .copied()
            .map(|byte| Value::Number(byte.into()))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use serde::Serialize;
    use serde_json::json;
    use uuid::Uuid;

    use super::from_slice;

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct WrappedItem {
        item: TestItem,
        item_type: &'static str,
    }

    #[derive(Serialize)]
    struct TestItem {
        id: Uuid,
        bytes: Vec<u8>,
    }

    #[derive(Serialize)]
    struct QueryResponse {
        tx: &'static str,
        sequence: u64,
        deletes: Vec<String>,
        upserts: Vec<WrappedItem>,
    }

    #[derive(Serialize)]
    #[serde(tag = "event", content = "data")]
    enum Message {
        #[serde(rename = "ws:m:query-response")]
        QueryResponse(QueryResponse),
    }

    #[test]
    fn query_response_uuid_bytes_decode_as_json_strings() {
        let id = Uuid::parse_str("b6e72873-9b84-4be5-a84b-a5707883c346");
        assert!(id.is_ok(), "valid UUID fixture");
        let Ok(id) = id else {
            return;
        };
        let message = Message::QueryResponse(QueryResponse {
            tx: "tx-1",
            sequence: 0,
            deletes: Vec::new(),
            upserts: vec![WrappedItem {
                item: TestItem {
                    id,
                    bytes: vec![1, 2, 3],
                },
                item_type: "TestItem",
            }],
        });

        let mut bytes = Vec::new();
        assert!(ciborium::ser::into_writer(&message, &mut bytes).is_ok());

        let decoded = from_slice(&bytes);
        assert!(decoded.is_ok(), "CBOR query response should decode");
        let Ok(decoded) = decoded else {
            return;
        };
        assert_eq!(decoded.pointer("/data/upserts/0/item/id"), Some(&json!(id.to_string())));
        assert_eq!(
            decoded.pointer("/data/upserts/0/item/bytes"),
            Some(&json!([1, 2, 3]))
        );
    }

    #[test]
    fn non_uuid_byte_strings_decode_as_json_byte_arrays() {
        let value = ciborium::value::Value::Bytes(vec![1, 2, 3]);
        let mut bytes = Vec::new();
        assert!(ciborium::ser::into_writer(&value, &mut bytes).is_ok());

        assert_eq!(from_slice(&bytes).ok(), Some(json!([1, 2, 3])));
    }
}
