use serde::Serialize;
use serde_json::Value;

pub trait ToValue {
    fn to_value(&self) -> Value;
}

/// Blanket implementation for Option<T> where T: Serialize
impl<T: Serialize> ToValue for Option<T> {
    fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// Blanket implementation for Vec<T> where T: Serialize
impl<T: Serialize> ToValue for Vec<T> {
    fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}
