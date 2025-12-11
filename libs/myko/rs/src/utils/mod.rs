pub mod signal_stream;

use std::collections::HashMap;
use std::hash::Hash;

use serde_json::Value;

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

pub fn assert_default_for_key<K, V>(hash_map: &mut HashMap<K, V>, key: &K)
where
    K: Hash + Eq + Clone,
    V: Default,
{
    match hash_map.contains_key(key) {
        true => (),
        false => {
            hash_map.insert(key.clone(), V::default());
        }
    }
}
