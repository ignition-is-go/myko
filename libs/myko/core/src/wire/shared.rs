use std::sync::Arc;

use serde::{Serialize, de::Error};
use serde_json::Value;

pub(crate) fn value_with_tx<T: Serialize + Clone>(
    tx: Arc<str>,
    value: &T,
) -> Result<Value, serde_json::Error> {
    let mut json = serde_json::to_value(value.clone())?;
    let obj_mut = json.as_object_mut();
    if obj_mut.is_none() {
        return Err(serde_json::Error::custom("Could not convert to object"));
    }
    let obj = obj_mut.unwrap();
    obj.insert("tx".to_string(), tx.to_string().into());
    Ok(json)
}
