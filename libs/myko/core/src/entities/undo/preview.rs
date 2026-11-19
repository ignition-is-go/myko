//! Bounded display data, never inverse plans or persistence metadata.
use crate::{prelude::*, server::HistoryEntityKey};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    sync::Arc,
};

pub const UNDO_DIFF_ENTITIES: usize = 32;
pub const UNDO_DIFF_BYTES: usize = 32 * 1024;
const RAW_BYTES: usize = 16 * 1024;
const VALUE_BYTES: usize = 512;
const FIELDS: usize = 128;

#[myko_report_output]
pub struct UndoFieldDiff {
    /// JSON Pointer: missing values are None; explicit null is the string "null".
    pub path: String,
    pub before: Option<String>,
    pub after: Option<String>,
    pub truncated: bool,
}
#[myko_report_output]
pub struct UndoEntityDiff {
    pub entity_type: Arc<str>,
    pub entity_id: Arc<str>,
    pub label: Option<String>,
    pub kind: String,
    pub fields: Vec<UndoFieldDiff>,
    pub truncated: bool,
}

/// One cached transaction's display data, subscribed to only while expanded.
#[myko_report_output]
pub struct UndoDiffSnapshot {
    pub entity_diffs: Arc<Vec<UndoEntityDiff>>,
    pub complete: bool,
    pub available: bool,
    pub error: Option<String>,
}

#[derive(Default)]
struct Limited {
    bytes: Vec<u8>,
    limit: usize,
    truncated: bool,
}
impl Write for Limited {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let room = self.limit.saturating_sub(self.bytes.len());
        if bytes.len() > room {
            self.bytes
                .extend_from_slice(bytes.get(..room).unwrap_or_default());
            self.truncated = true;
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encoded(value: &Value, limit: usize) -> (String, bool) {
    let mut writer = Limited {
        limit,
        ..Limited::default()
    };
    let _ = serde_json::to_writer(&mut writer, value);
    let mut bytes = writer.bytes;
    while std::str::from_utf8(&bytes).is_err() {
        bytes.pop();
    }
    (
        String::from_utf8(bytes).unwrap_or_default(),
        writer.truncated,
    )
}
fn size(diff: &UndoEntityDiff) -> usize {
    diff.fields.iter().fold(128, |bytes, field| {
        bytes
            .saturating_add(field.path.len())
            .saturating_add(field.before.as_ref().map_or(0, String::len))
            .saturating_add(field.after.as_ref().map_or(0, String::len))
    })
}

struct Walker {
    fields: Vec<UndoFieldDiff>,
    remaining: usize,
    bytes: usize,
    truncated: bool,
}
impl Walker {
    #[allow(clippy::too_many_lines)]
    fn visit(&mut self, path: &str, before: Option<&Value>, after: Option<&Value>, depth: usize) {
        if self.remaining == 0
            || self.fields.len() >= FIELDS
            || self.bytes >= UNDO_DIFF_BYTES
            || path.len() > 512
        {
            self.truncated = true;
            return;
        }
        self.remaining = self.remaining.saturating_sub(1);
        if before == after {
            return;
        }
        if depth < 32 {
            let object = |value: Option<&Value>| value.is_none_or(Value::is_object);
            if object(before) && object(after) {
                let candidates = before
                    .and_then(Value::as_object)
                    .into_iter()
                    .flat_map(|map| map.keys())
                    .chain(
                        after
                            .and_then(Value::as_object)
                            .into_iter()
                            .flat_map(|map| map.keys()),
                    );
                let mut keys = BTreeSet::new();
                for key in candidates {
                    if keys.len() >= 4096 && !keys.contains(key) {
                        self.truncated = true;
                        break;
                    }
                    keys.insert(key);
                }
                let empty = keys.is_empty();
                for key in keys {
                    if key.len() > 512 {
                        self.truncated = true;
                        continue;
                    }
                    let escaped = key.replace('~', "~0").replace('/', "~1");
                    self.visit(
                        &format!("{path}/{escaped}"),
                        before.and_then(|value| value.get(key)),
                        after.and_then(|value| value.get(key)),
                        depth.saturating_add(1),
                    );
                    if self.truncated
                        && (self.remaining == 0
                            || self.fields.len() >= FIELDS
                            || self.bytes >= UNDO_DIFF_BYTES)
                    {
                        break;
                    }
                }
                if !empty {
                    return;
                }
            }
            let array = |value: Option<&Value>| value.is_none_or(Value::is_array);
            if array(before) && array(after) {
                let count = before
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len)
                    .max(after.and_then(Value::as_array).map_or(0, Vec::len));
                for index in 0..count {
                    self.visit(
                        &format!("{path}/{index}"),
                        before.and_then(|value| value.get(index)),
                        after.and_then(|value| value.get(index)),
                        depth.saturating_add(1),
                    );
                    if self.truncated
                        && (self.remaining == 0
                            || self.fields.len() >= FIELDS
                            || self.bytes >= UNDO_DIFF_BYTES)
                    {
                        break;
                    }
                }
                if count > 0 {
                    return;
                }
            }
        }
        self.truncated |= depth >= 32
            && before
                .into_iter()
                .chain(after)
                .any(|value| value.is_object() || value.is_array());
        let (before, bt) = before.map_or((None, false), |value| {
            let (text, cut) = encoded(value, VALUE_BYTES);
            (Some(text), cut)
        });
        let (after, at) = after.map_or((None, false), |value| {
            let (text, cut) = encoded(value, VALUE_BYTES);
            (Some(text), cut)
        });
        let field = UndoFieldDiff {
            path: path.to_owned(),
            before,
            after,
            truncated: bt || at,
        };
        let bytes = field
            .path
            .len()
            .saturating_add(field.before.as_ref().map_or(0, String::len))
            .saturating_add(field.after.as_ref().map_or(0, String::len));
        if self.bytes.saturating_add(bytes) > UNDO_DIFF_BYTES {
            self.truncated = true;
            return;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.truncated |= field.truncated;
        self.fields.push(field);
    }
}
fn diff(key: &HistoryEntityKey, before: Option<&Value>, after: Option<&Value>) -> UndoEntityDiff {
    let mut walker = Walker {
        fields: Vec::new(),
        remaining: 4096,
        bytes: 0,
        truncated: false,
    };
    walker.visit("", before, after, 0);
    UndoEntityDiff {
        entity_type: key.item_type.clone(),
        entity_id: key.item_id.clone(),
        label: after
            .or(before)
            .and_then(|value| value.get("name"))
            .and_then(Value::as_str)
            .map(|text| text.chars().take(160).collect()),
        kind: if before.is_none() {
            "Created"
        } else if after.is_none() {
            "Deleted"
        } else {
            "Updated"
        }
        .into(),
        fields: walker.fields,
        truncated: walker.truncated,
    }
}

/// Transient handoff from mutation to successful commit. Never serialized.
#[derive(Clone, Debug)]
pub struct UndoPreviewInput {
    before: Option<Value>,
    after: Option<Value>,
    complete: bool,
    bytes: usize,
    display: UndoEntityDiff,
}
impl UndoPreviewInput {
    #[must_use]
    pub fn capture(key: &HistoryEntityKey, before: Option<&Value>, after: Option<&Value>) -> Self {
        let display = diff(key, before, after);
        let (before_bytes, bt) =
            before.map_or((String::new(), false), |value| encoded(value, RAW_BYTES));
        let (after_bytes, at) =
            after.map_or((String::new(), false), |value| encoded(value, RAW_BYTES));
        let complete = !(bt || at);
        let bytes = size(&display).saturating_add(if complete {
            before_bytes.len().saturating_add(after_bytes.len())
        } else {
            0
        });
        Self {
            before: complete.then(|| before.cloned()).flatten(),
            after: complete.then(|| after.cloned()).flatten(),
            complete,
            bytes,
            display,
        }
    }
}
#[derive(Default)]
pub struct UndoPreviewAccumulator {
    items: BTreeMap<HistoryEntityKey, UndoPreviewInput>,
    bytes: usize,
    incomplete: bool,
}
impl UndoPreviewAccumulator {
    pub fn record(&mut self, key: &HistoryEntityKey, input: Option<&UndoPreviewInput>) {
        let Some(input) = input else {
            self.incomplete = true;
            return;
        };
        if let Some(previous) = self.items.get_mut(key) {
            if previous.complete && input.complete {
                let after_bytes = input
                    .after
                    .as_ref()
                    .map_or(0, |value| encoded(value, RAW_BYTES).0.len());
                if self.bytes.saturating_add(after_bytes) > UNDO_DIFF_BYTES.saturating_mul(2) {
                    self.incomplete = true;
                    previous.complete = false;
                    return;
                }
                let old_after = previous
                    .after
                    .as_ref()
                    .map_or(0, |value| encoded(value, RAW_BYTES).0.len());
                self.bytes = self
                    .bytes
                    .saturating_sub(old_after)
                    .saturating_add(after_bytes);
                previous.after.clone_from(&input.after);
            } else {
                previous.complete = false;
                self.incomplete = true;
            }
            return;
        }
        if self.items.len() >= UNDO_DIFF_ENTITIES
            || self.bytes.saturating_add(input.bytes) > UNDO_DIFF_BYTES.saturating_mul(2)
        {
            self.incomplete = true;
            return;
        }
        self.bytes = self.bytes.saturating_add(input.bytes);
        self.items.insert(key.clone(), input.clone());
    }
    #[must_use]
    pub fn finish(self) -> (Vec<UndoEntityDiff>, bool) {
        let mut output = Vec::new();
        let mut complete = !self.incomplete;
        let mut bytes = 0_usize;
        let mut fields = 0_usize;
        for (key, input) in self.items {
            let mut display = if input.complete {
                diff(&key, input.before.as_ref(), input.after.as_ref())
            } else {
                let mut display = input.display;
                display.truncated = true;
                display
            };
            let mut retained = Vec::new();
            for field in display.fields {
                let cost = field
                    .path
                    .len()
                    .saturating_add(field.before.as_ref().map_or(0, String::len))
                    .saturating_add(field.after.as_ref().map_or(0, String::len));
                if bytes.saturating_add(cost) > UNDO_DIFF_BYTES || fields >= FIELDS {
                    display.truncated = true;
                    break;
                }
                bytes = bytes.saturating_add(cost);
                fields = fields.saturating_add(1);
                retained.push(field);
            }
            display.fields = retained;
            complete &= !display.truncated;
            if !display.fields.is_empty() {
                output.push(display);
            }
        }
        (output, complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn nested_fields_arrays_and_null_are_distinct_from_missing() {
        let key = HistoryEntityKey::new("Node", "a");
        let before = json!({"id":"a","value":{"r":0.1,"items":[1,2],"gone":null}});
        let after = json!({"id":"a","value":{"r":0.8,"items":[1,3],"new":null}});
        let result = diff(&key, Some(&before), Some(&after));
        assert_eq!(
            result
                .fields
                .iter()
                .map(|field| field.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/value/gone", "/value/items/1", "/value/new", "/value/r"]
        );
        let removed = result
            .fields
            .iter()
            .find(|field| field.path == "/value/gone")
            .unwrap();
        assert_eq!(removed.before.as_deref(), Some("null"));
        assert_eq!(removed.after, None);
        let added = result
            .fields
            .iter()
            .find(|field| field.path == "/value/new")
            .unwrap();
        assert_eq!(added.before, None);
        assert_eq!(added.after.as_deref(), Some("null"));
    }
    #[test]
    fn transaction_diff_keeps_first_before_and_last_after_even_across_shape_changes() {
        let key = HistoryEntityKey::new("Node", "a");
        let first = json!({"value":{"x":1,"y":2}});
        let middle = json!({"value":7});
        let last = json!({"value":{"x":3,"y":2}});
        let mut accumulator = UndoPreviewAccumulator::default();
        accumulator.record(
            &key,
            Some(&UndoPreviewInput::capture(
                &key,
                Some(&first),
                Some(&middle),
            )),
        );
        accumulator.record(
            &key,
            Some(&UndoPreviewInput::capture(&key, Some(&middle), Some(&last))),
        );
        let (output, complete) = accumulator.finish();
        assert!(complete);
        assert_eq!(output.len(), 1);
        let field = output.first().unwrap().fields.first().unwrap();
        assert_eq!(field.path, "/value/x");
        assert_eq!(field.before.as_deref(), Some("1"));
        assert_eq!(field.after.as_deref(), Some("3"));
    }
    #[test]
    fn create_delete_and_empty_containers_have_honest_presence() {
        let key = HistoryEntityKey::new("Node", "a");
        let empty = json!({"empty":{},"array":[]});
        let created = diff(&key, None, Some(&empty));
        assert_eq!(created.kind, "Created");
        assert_eq!(created.fields.len(), 2);
        assert!(
            created
                .fields
                .iter()
                .all(|field| field.before.is_none() && field.after.is_some())
        );
        let deleted = diff(&key, Some(&empty), None);
        assert_eq!(deleted.kind, "Deleted");
        assert_eq!(deleted.fields.len(), 2);
        let mut accumulator = UndoPreviewAccumulator::default();
        accumulator.record(
            &key,
            Some(&UndoPreviewInput::capture(&key, None, Some(&empty))),
        );
        accumulator.record(
            &key,
            Some(&UndoPreviewInput::capture(&key, Some(&empty), None)),
        );
        let (output, complete) = accumulator.finish();
        assert!(complete);
        assert!(output.is_empty());
    }
    #[test]
    fn transaction_field_count_is_bounded_across_entities() {
        let mut accumulator = UndoPreviewAccumulator::default();
        for index in 0..32 {
            let key = HistoryEntityKey::new("Node", index.to_string());
            let after = Value::Object(
                (0..20)
                    .map(|field| (format!("f{field}"), Value::from(field)))
                    .collect(),
            );
            accumulator.record(
                &key,
                Some(&UndoPreviewInput::capture(&key, None, Some(&after))),
            );
        }
        let (output, complete) = accumulator.finish();
        assert!(!complete);
        assert!(output.iter().map(|diff| diff.fields.len()).sum::<usize>() <= FIELDS);
    }
    #[test]
    fn large_values_are_bounded_and_missing_previews_never_claim_completeness() {
        let key = HistoryEntityKey::new("Node", "a");
        let large = json!({"value":"🦀".repeat(20_000)});
        let input = UndoPreviewInput::capture(&key, None, Some(&large));
        let mut accumulator = UndoPreviewAccumulator::default();
        accumulator.record(&key, Some(&input));
        let (output, complete) = accumulator.finish();
        assert!(!complete);
        let field = output.first().unwrap().fields.first().unwrap();
        assert!(field.truncated);
        assert!(field.after.as_ref().unwrap().len() <= VALUE_BYTES);
        let mut missing = UndoPreviewAccumulator::default();
        missing.record(&key, None);
        assert!(!missing.finish().1);
    }
}
