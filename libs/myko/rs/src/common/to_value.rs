use serde_json::Value;

pub trait ToValue {
    fn to_value(&self) -> Value;
}
