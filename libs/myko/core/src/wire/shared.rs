use std::sync::Arc;

use serde::{Serialize, de::Error};
use serde_json::Value;

pub fn value_with_tx<T: Serialize + Clone>(
    tx: Arc<str>,
    value: &T,
) -> Result<Value, serde_json::Error> {
    let mut json = serde_json::to_value(value.clone())?;
    let Some(obj) = json.as_object_mut() else {
        return Err(serde_json::Error::custom("Could not convert to object"));
    };
    obj.insert("tx".to_string(), tx.to_string().into());
    drop(tx);
    Ok(json)
}
