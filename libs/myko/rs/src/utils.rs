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

// tests

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use serde::{de, Deserialize, Serialize};
    use serde_json::json;

    use super::*;
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]

    struct C {
        d: i32,
        e: i32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Data {
        a: i32,
        b: i32,
        c: C,
    }

    fn generate_objects(num: i32) -> Vec<(Value, Value)> {
        let mut objects = Vec::new();
        for i in 0..num {
            let filter = json!({
                "a": i,
                "b": i,
                "c": {
                    "d": i,
                    "e": i,
                },
            });
            let c = if i % 2 == 0 { i } else { 0 };
            let candidate = json!({
                "a": i,
                "b": c,
                "c": {
                    "d": i,
                    "e": i,
                },
            });
            objects.push((filter, candidate));
        }
        objects
    }

    fn generate_data(num: i32) -> Vec<(Data, Data)> {
        let mut objects = Vec::new();
        for i in 0..num {
            let filter = Data {
                a: i,
                b: i,
                c: C { d: i, e: i },
            };
            let c = if i % 2 == 0 { i } else { 0 };
            let candidate = Data {
                a: i,
                b: c,
                c: C { d: i, e: i },
            };
            objects.push((filter, candidate));
        }
        objects
    }

    #[test]
    fn test_mask_filter() {
        let num = 1000000;

        eprintln!("Genertating objects: {}", num);
        let objs = generate_objects(num);
        let with_deser = Instant::now();
        let deser = objs
            .iter()
            .map(|(filter, candidate)| {
                let filter: Data = serde_json::from_value(filter.clone()).unwrap();
                let candidate: Data = serde_json::from_value(candidate.clone()).unwrap();
                (filter, candidate)
            })
            .collect::<Vec<_>>();
        eprintln!("withDeser: {:?}", with_deser.elapsed());

        eprintln!("Filtering objects");
        let now = Instant::now();
        let filtered = deser
            .iter()
            .filter(|(filter, candidate)| filter == candidate);

        println!("num: {}", num / 2);
        let count = filtered.count();
        println!("count: {}", count);
        eprintln!("Elapsed: {:?}", now.elapsed());
    }
}

pub fn remove_whitespace(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}
