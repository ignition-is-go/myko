use serde::Serialize;
use serde_json::Value;

pub trait ToValue {
    fn to_value(&self) -> Value;
}

/// Blanket implementation for all types that implement Serialize
impl<T: Serialize> ToValue for T {
    fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}
