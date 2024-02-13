use myko_wasm::item::Eventable;
use serde_json::Value;
use std::collections::HashMap;

pub fn mask_filter(filter: &Value, candidate: &Value) -> bool {
    match (filter, candidate) {
        (Value::Object(map_a), Value::Object(map_b)) => {
            for (key, value_a) in map_a {
                match map_b.get(key) {
                    Some(value_b) => {
                        if !mask_filter(value_a, value_b) {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        }
        _ => filter == candidate,
    }
}

pub fn remove_whitespace(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

pub fn matches<T: Eventable<T, PT> + PartialEq, PT: Clone>(item: &T, query: &PT) -> bool {
    let mut after = item.clone();

    let q = query.clone();

    after.apply_some(q);

    after == *item
}

pub fn filter_query<T: Eventable<T, PT> + PartialEq, PT: Clone>(
    state: &HashMap<String, T>,
    query: &PT,
) -> HashMap<String, T> {
    state
        .iter()
        .filter(|(_, v)| matches(*v, query))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
