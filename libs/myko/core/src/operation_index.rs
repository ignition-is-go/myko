//! Two independent things live in this module:
//!
//! 1. **The MCP "Code Mode" operation index** ([`build_operation_index`],
//!    [`OperationSchema`]) — the reflection surface backing the `search()`
//!    MCP tool and the generated `myko.*` JS bindings `execute()` scripts
//!    call. This is pure in-memory `inventory` iteration: field
//!    names/types/optionality and doc comments are captured directly by the
//!    `#[myko_query]`/`#[myko_view]`/`#[myko_report]`/`#[myko_command]`
//!    macros at compile time (see `crate::reflection::OperationArgField`)
//!    and embedded in the registration structs — no runtime file I/O, no
//!    bindings-directory configuration, and it works identically for types
//!    defined in *any* crate linked into the binary, not just the one whose
//!    ts-rs bindings happen to be configured. (An earlier version of this
//!    module read the same data back out of generated `.ts` binding files;
//!    that broke for multi-crate consumers whose types live in a different
//!    crate — and thus a different ts-rs output directory — than whichever
//!    single path got wired up. Capturing it in the macro avoids the whole
//!    problem.)
//!
//! 2. **Lightweight ts-rs `.ts` binding text parsing**, shared with the
//!    (heavy, `codegen`-feature-gated) TS codegen tooling in
//!    `crate::codegen::typescript` for its *doc generation* pass
//!    (`generate_docs_json_from_bindings`) — a genuinely separate, still
//!    file-based feature for producing the TS SDK's prose docs, unrelated
//!    to MCP. This module intentionally does not depend on
//!    `dprint-plugin-typescript` itself (unlike `crate::codegen::typescript`), so it can
//!    be an always-on dependency of `myko-server` without forcing every
//!    server binary to pull in a full TS formatter.

#[cfg(feature = "codegen-ts")]
use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{
    command::CommandRegistration, query::QueryRegistration, reflection::OperationArgField,
    report::ReportRegistration, view::ViewRegistration,
};

/// One argument of an [`OperationSchema`], rendered from the
/// [`OperationArgField`] the defining macro captured.
#[derive(serde::Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OperationArg {
    pub name: String,
    pub ts_type: String,
    pub optional: bool,
    pub rust_type: String,
    pub aliases: Vec<String>,
    pub flattened: bool,
}

impl From<&OperationArgField> for OperationArg {
    fn from(field: &OperationArgField) -> Self {
        Self {
            name: field.name.to_string(),
            ts_type: rust_type_to_ts(field.rust_type),
            optional: field.optional,
            rust_type: field.rust_type.to_string(),
            aliases: field
                .aliases
                .iter()
                .map(|alias| (*alias).to_string())
                .collect(),
            flattened: field.flattened,
        }
    }
}

/// One entry in the MCP "Code Mode" operation index — the reflection surface
/// backing the `search()` MCP tool and the generated `myko.*` JS bindings
/// consumed by `execute()`.
#[derive(serde::Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OperationSchema {
    pub id: String,
    /// `"query"` | `"view"` | `"report"` | `"command"`
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub args: Vec<OperationArg>,
    pub output_type: String,
}

/// Fields shared by result serialization and on-demand MCP descriptions.
#[derive(Clone, Copy)]
enum ResultField {
    Id,
    TypeName,
    Count,
    Success,
    Payload,
}

pub struct ResultContract(&'static [(&'static str, ResultField)]);

pub const QUERY_RESULT: ResultContract = ResultContract(&[
    ("query_id", ResultField::Id),
    ("item_type", ResultField::TypeName),
    ("count", ResultField::Count),
    ("items", ResultField::Payload),
]);
pub const VIEW_RESULT: ResultContract = ResultContract(&[
    ("view_id", ResultField::Id),
    ("item_type", ResultField::TypeName),
    ("count", ResultField::Count),
    ("items", ResultField::Payload),
]);
pub const REPORT_RESULT: ResultContract = ResultContract(&[
    ("report_id", ResultField::Id),
    ("output_type", ResultField::TypeName),
    ("result", ResultField::Payload),
]);
pub const COMMAND_RESULT: ResultContract = ResultContract(&[
    ("command_id", ResultField::Id),
    ("success", ResultField::Success),
    ("result", ResultField::Payload),
]);

impl ResultContract {
    /// Construct the exact object returned to an `execute` script.
    #[must_use]
    pub fn wrap(
        &self,
        id: &str,
        type_name: &str,
        mut payload: serde_json::Value,
    ) -> serde_json::Value {
        let count = payload.as_array().map_or(0, Vec::len);
        self.0
            .iter()
            .map(|(name, field)| {
                let value = match field {
                    ResultField::Id => serde_json::json!(id),
                    ResultField::TypeName => serde_json::json!(type_name),
                    ResultField::Count => serde_json::json!(count),
                    ResultField::Success => serde_json::json!(true),
                    ResultField::Payload => std::mem::take(&mut payload),
                };
                ((*name).to_string(), value)
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }

    fn describe(&self, payload_type: &str) -> serde_json::Value {
        self.0
            .iter()
            .map(|(name, field)| {
                let value = match field {
                    ResultField::Id | ResultField::TypeName => "string",
                    ResultField::Count => "number",
                    ResultField::Success => "true",
                    ResultField::Payload => payload_type,
                };
                ((*name).to_string(), serde_json::json!(value))
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }
}

impl OperationSchema {
    /// Return a detailed description only when requested, keeping discovery compact.
    #[must_use]
    pub fn describe(&self) -> serde_json::Value {
        let result_contract = match self.kind.as_str() {
            "query" => &QUERY_RESULT,
            "view" => &VIEW_RESULT,
            "report" => &REPORT_RESULT,
            _ => &COMMAND_RESULT,
        };
        let example_args = self.example_args();
        let example = example_args.as_ref().map(|args| {
            format!(
                "return await myko.{}({}, {});",
                self.kind,
                serde_json::json!(self.id),
                args,
            )
        });
        serde_json::json!({
            "operation": self,
            "inputType": self.args,
            "outputType": result_contract.describe(&self.output_type),
            "payloadType": self.output_type,
            "exampleArgs": example_args,
            "example": example,
            "exampleUnavailable": if example_args.is_none() {
                Some("No valid example is registered for these input types. The application can use register_operation_example! to supply one.")
            } else { None },
        })
    }

    fn example_args(&self) -> Option<serde_json::Value> {
        if let Some(registration) = inventory::iter::<crate::reflection::OperationExample>
            .into_iter()
            .find(|entry| entry.kind == self.kind && entry.id == self.id)
        {
            let args = (registration.example)();
            crate::reflection::validate_operation_input(&self.kind, &self.id, &args).ok()?;
            return Some(args);
        }

        let args = self
            .args
            .iter()
            .filter(|arg| !arg.optional)
            .map(|arg| {
                if arg.flattened {
                    return None;
                }
                let value = example_value(&arg.rust_type)?;
                Some((arg.name.clone(), value))
            })
            .collect::<Option<serde_json::Map<_, _>>>()?;
        let args = serde_json::Value::Object(args);
        crate::reflection::validate_operation_input(&self.kind, &self.id, &args).ok()?;
        Some(args)
    }
}

fn wire_output_type(rust_type: &str) -> String {
    if rust_type.trim() == "()" {
        "null".into()
    } else {
        rust_type_to_ts(rust_type)
    }
}

fn example_value(rust_type: &str) -> Option<serde_json::Value> {
    let canonical = rust_type.replace(' ', "");
    if let Some((outer, inner)) = split_outer_generic(&canonical) {
        return match outer_leaf(outer) {
            "Option" => Some(serde_json::Value::Null),
            "Vec" | "HashSet" | "BTreeSet" => Some(serde_json::json!([])),
            "HashMap" | "BTreeMap" => Some(serde_json::json!({})),
            "Arc" | "Box" => example_value(inner),
            _ => None,
        };
    }
    match outer_leaf(&canonical) {
        "String" | "str" => Some(serde_json::json!("example")),
        "Uuid" => Some(serde_json::json!("00000000-0000-4000-8000-000000000001")),
        "bool" => Some(serde_json::json!(false)),
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64" | "u128"
        | "usize" | "f32" | "f64" => Some(serde_json::json!(0)),
        "Value" | "()" => Some(serde_json::Value::Null),
        _ => None,
    }
}

/// Build the full operation index from `inventory`-registered
///
/// query/view/report/command operations. Pure in-memory, no I/O, no
/// configuration — every operation from every linked-in crate is included
/// with full argument detail, since that detail is embedded in the
/// registration itself (see the module doc).
pub fn build_operation_index() -> Vec<OperationSchema> {
    let mut index = Vec::new();

    for reg in inventory::iter::<QueryRegistration> {
        index.push(OperationSchema {
            id: reg.query_id.to_string(),
            kind: "query".to_string(),
            description: reg.description.map(str::to_string),
            args: reg.args.iter().map(OperationArg::from).collect(),
            output_type: format!("{}[]", rust_type_to_ts(reg.query_item_type)),
        });
    }
    for reg in inventory::iter::<ViewRegistration> {
        index.push(OperationSchema {
            id: reg.view_id.to_string(),
            kind: "view".to_string(),
            description: reg.description.map(str::to_string),
            args: reg.args.iter().map(OperationArg::from).collect(),
            output_type: format!("{}[]", rust_type_to_ts(reg.view_item_type)),
        });
    }
    for reg in inventory::iter::<ReportRegistration> {
        index.push(OperationSchema {
            id: reg.report_id.to_string(),
            kind: "report".to_string(),
            description: reg.description.map(str::to_string),
            args: reg.args.iter().map(OperationArg::from).collect(),
            output_type: wire_output_type(reg.output_type),
        });
    }
    for reg in inventory::iter::<CommandRegistration> {
        index.push(OperationSchema {
            id: reg.command_id.to_string(),
            kind: "command".to_string(),
            description: reg.description.map(str::to_string),
            args: reg.args.iter().map(OperationArg::from).collect(),
            output_type: wire_output_type(reg.result_type),
        });
    }

    index.sort_by(|a, b| a.kind.cmp(&b.kind).then_with(|| a.id.cmp(&b.id)));
    index
}

/// Render a Rust type (as captured verbatim from field syntax, e.g.
/// `"Option < String >"`) into a TS-ish display string for the operation
/// index — `Option<T>` → `T | null`, `Vec<T>` → `T[]`, `Arc<T>` unwraps to
/// `T`, primitives map to their TS equivalents, everything else falls back
/// to its bare (last path segment) type name.
pub(crate) fn rust_type_to_ts(rust_type: &str) -> String {
    let trimmed = rust_type.trim();
    let canonical = trimmed.replace(' ', "");
    let canonical = canonical.as_str();
    if let Some((outer, inner)) = split_outer_generic(canonical) {
        match outer_leaf(outer) {
            // Handle Option<T> -> T | null
            "Option" => {
                let inner_ts = rust_type_to_ts(inner);
                return format!("{inner_ts} | null");
            }
            // Handle Vec<T> -> T[]
            "Vec" => {
                let inner_ts = rust_type_to_ts(inner);
                return format!("{inner_ts}[]");
            }
            // Handle Arc<T> -> unwrap to inner type
            "Arc" => return rust_type_to_ts(inner),
            _ => {}
        }
    }

    // Map Rust primitive types to TypeScript
    match outer_leaf(canonical) {
        "str" | "String" => "string".to_string(),
        "bool" => "boolean".to_string(),
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64" | "u128"
        | "usize" | "f32" | "f64" => "number".to_string(),
        "()" => "void".to_string(),
        // serde_json::Value maps to JsonValue in ts-rs
        "Value" | "serde_json::Value" => "JsonValue".to_string(),
        _ => outer_leaf(canonical).to_string(),
    }
}

pub(crate) fn outer_leaf(path_or_ident: &str) -> &str {
    path_or_ident.rsplit("::").next().unwrap_or(path_or_ident)
}

pub(crate) fn split_outer_generic(s: &str) -> Option<(&str, &str)> {
    let start = s.find('<')?;
    let end = s.rfind('>')?;
    if end <= start {
        return None;
    }
    let outer = s.get(..start)?.trim();
    let inner = s.get(start.saturating_add(1)..end)?.trim();
    if outer.is_empty() || inner.is_empty() {
        return None;
    }
    Some((outer, inner))
}

#[cfg(feature = "codegen-ts")]
pub(crate) fn split_generic_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (idx, ch) in s.char_indices() {
        match ch {
            '<' => depth = depth.saturating_add(1),
            '>' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                let arg = s.get(start..idx).unwrap_or_default().trim();
                if !arg.is_empty() {
                    args.push(arg.to_string());
                }
                start = idx.saturating_add(1);
            }
            _ => {}
        }
    }
    let tail = s.get(start..).unwrap_or_default().trim();
    if !tail.is_empty() {
        args.push(tail.to_string());
    }
    args
}

// ─────────────────────────────────────────────────────────────────────────────
// ts-rs `.ts` binding text parsing — shared with `crate::codegen::typescript`'s docgen.
// ─────────────────────────────────────────────────────────────────────────────

// Only consumed by `crate::codegen::typescript::generate_docs_json_from_bindings`, which
// is gated behind the (non-default) `codegen` feature.
#[cfg(feature = "codegen-ts")]
pub(crate) fn collect_ts_binding_files(bindings_dir: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
    let mut files = Vec::new();
    for entry in fs::read_dir(bindings_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let is_ts = path.extension().is_some_and(|x| x == "ts");
        let is_declaration_file = path
            .file_name()
            .and_then(|x| x.to_str())
            .is_some_and(|x| x.ends_with(".d.ts"));
        if is_ts && !is_declaration_file {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

#[cfg(any(feature = "codegen-ts", test))]
pub(crate) fn extract_exported_object_type_body(content: &str, type_name: &str) -> Option<String> {
    let marker = format!("export type {type_name} =");
    let start = content.find(&marker)?;
    let marker_end = start.checked_add(marker.len())?;
    let rest = content.get(marker_end..)?;
    let brace_start_rel = rest.find('{')?;
    let brace_start = marker_end.checked_add(brace_start_rel)?;

    let mut depth = 0usize;
    let mut end_idx = None;
    for (i, ch) in content.get(brace_start..)?.char_indices() {
        match ch {
            '{' => depth = depth.saturating_add(1),
            '}' => {
                if depth == 0 {
                    return None;
                }
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    end_idx = brace_start.checked_add(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let end_idx = end_idx?;
    content
        .get(brace_start.saturating_add(1)..end_idx)
        .map(str::to_string)
}

/// Returns `(field_name, ts_type, doc_comment, optional)` for each field of
/// a parsed ts-rs object type body.
#[cfg(any(feature = "codegen-ts", test))]
pub(crate) fn parse_object_type_fields(body: &str) -> Vec<(String, String, Option<String>, bool)> {
    let mut fields = Vec::new();
    let mut pending_doc: Option<String> = None;

    for segment in split_top_level_commas(body) {
        let mut segment = segment.trim().to_string();
        if segment.is_empty() {
            continue;
        }

        while let Some(start) = segment.find("/**") {
            let tail = segment.get(start.saturating_add(3)..).unwrap_or_default();
            let Some(end_rel) = tail.find("*/") else {
                break;
            };
            let end = start
                .saturating_add(3)
                .saturating_add(end_rel)
                .saturating_add(2);
            let comment_block = segment.get(start..end).unwrap_or_default();
            let doc = normalize_jsdoc(comment_block);
            if !doc.is_empty() {
                pending_doc = Some(doc);
            }
            segment.replace_range(start..end, "");
        }

        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }

        let Some(colon_idx) = find_top_level_colon(segment) else {
            continue;
        };
        let raw_name = segment.get(..colon_idx).unwrap_or_default().trim();
        let raw_type = segment
            .get(colon_idx.saturating_add(1)..)
            .unwrap_or_default()
            .trim();

        let prop_name = raw_name
            .trim_start_matches("readonly ")
            .trim_end_matches('?')
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        if prop_name.is_empty() {
            pending_doc = None;
            continue;
        }

        let prop_type = raw_type.trim().to_string();
        if prop_type.is_empty() {
            pending_doc = None;
            continue;
        }

        // `field?: T` (serde default/skip_serializing_if) or ts-rs's own
        // `T | null` rendering of `Option<T>` both mean "not required".
        let optional = raw_name.trim_end().ends_with('?') || prop_type.contains("| null");

        fields.push((prop_name, prop_type, pending_doc.take(), optional));
    }

    fields
}

#[cfg(any(feature = "codegen-ts", test))]
fn split_top_level_commas(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut brace = 0usize;
    let mut bracket = 0usize;
    let mut paren = 0usize;
    let mut angle = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_block_comment = false;
    let mut in_line_comment = false;
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let Some(&ch) = chars.get(i) else { break };
        let next = chars.get(i.saturating_add(1)).copied();
        let prev = i.checked_sub(1).and_then(|index| chars.get(index)).copied();
        if in_block_comment {
            if ch == '*' && next == Some('/') {
                in_block_comment = false;
                i = i.saturating_add(2);
                continue;
            }
            i = i.saturating_add(1);
            continue;
        }
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            i = i.saturating_add(1);
            continue;
        }
        if ch == '/' && next == Some('*') {
            in_block_comment = true;
            i = i.saturating_add(2);
            continue;
        }
        if ch == '/' && next == Some('/') {
            in_line_comment = true;
            i = i.saturating_add(2);
            continue;
        }
        if ch == '\'' && !in_double && prev != Some('\\') {
            in_single = !in_single;
        } else if ch == '"' && !in_single && prev != Some('\\') {
            in_double = !in_double;
        } else if !in_single && !in_double {
            match ch {
                '{' => brace = brace.saturating_add(1),
                '}' => brace = brace.saturating_sub(1),
                '[' => bracket = bracket.saturating_add(1),
                ']' => bracket = bracket.saturating_sub(1),
                '(' => paren = paren.saturating_add(1),
                ')' => paren = paren.saturating_sub(1),
                '<' => angle = angle.saturating_add(1),
                '>' => angle = angle.saturating_sub(1),
                ',' if brace == 0 && bracket == 0 && paren == 0 && angle == 0 => {
                    out.push(
                        chars
                            .get(start..i)
                            .unwrap_or_default()
                            .iter()
                            .collect::<String>(),
                    );
                    start = i.saturating_add(1);
                }
                _ => {}
            }
        }
        i = i.saturating_add(1);
    }
    if start < chars.len() {
        out.push(
            chars
                .get(start..)
                .unwrap_or_default()
                .iter()
                .collect::<String>(),
        );
    }
    out
}

#[cfg(any(feature = "codegen-ts", test))]
fn find_top_level_colon(input: &str) -> Option<usize> {
    let chars: Vec<char> = input.chars().collect();
    let mut brace = 0usize;
    let mut bracket = 0usize;
    let mut paren = 0usize;
    let mut angle = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    for (i, ch) in chars.iter().enumerate() {
        let prev = i.checked_sub(1).and_then(|index| chars.get(index)).copied();
        if *ch == '\'' && !in_double && prev != Some('\\') {
            in_single = !in_single;
            continue;
        }
        if *ch == '"' && !in_single && prev != Some('\\') {
            in_double = !in_double;
            continue;
        }
        if in_single || in_double {
            continue;
        }
        match ch {
            '{' => brace = brace.saturating_add(1),
            '}' => brace = brace.saturating_sub(1),
            '[' => bracket = bracket.saturating_add(1),
            ']' => bracket = bracket.saturating_sub(1),
            '(' => paren = paren.saturating_add(1),
            ')' => paren = paren.saturating_sub(1),
            '<' => angle = angle.saturating_add(1),
            '>' => angle = angle.saturating_sub(1),
            ':' if brace == 0 && bracket == 0 && paren == 0 && angle == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

#[cfg(any(feature = "codegen-ts", test))]
fn normalize_jsdoc(block: &str) -> String {
    block
        .replace("/**", "")
        .replace("*/", "")
        .lines()
        .map(|line| line.trim().trim_start_matches('*').trim().to_string())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[crate::myko_command(String)]
    pub struct WireContractFixture {
        #[serde(
            rename(deserialize = "wireName", serialize = "savedName"),
            alias = "legacyName"
        )]
        rust_name: String,
        #[serde(default)]
        batch_limit: u32,
        #[serde(skip_deserializing)]
        ignored: String,
    }

    impl crate::command::CommandHandler for WireContractFixture {
        fn execute(
            self,
            _ctx: crate::command::CommandContext,
        ) -> Result<String, crate::command::CommandError> {
            Ok(self.rust_name)
        }
    }

    #[crate::myko_command(u32)]
    pub struct InvalidExampleFixture {
        value: u32,
    }

    impl crate::command::CommandHandler for InvalidExampleFixture {
        fn execute(
            self,
            _ctx: crate::command::CommandContext,
        ) -> Result<u32, crate::command::CommandError> {
            Ok(self.value)
        }
    }

    crate::register_operation_example!(
        "command",
        InvalidExampleFixture,
        serde_json::json!({"value": "wrong type"})
    );

    #[test]
    #[allow(clippy::panic_in_result_fn)]
    fn invalid_registered_examples_are_not_advertised() -> Result<(), String> {
        let index = build_operation_index();
        let operation = index
            .iter()
            .find(|operation| operation.id == "InvalidExampleFixture")
            .ok_or("fixture missing")?;
        let descriptor = operation.describe();
        assert_eq!(descriptor.get("example"), Some(&serde_json::Value::Null));
        assert!(
            descriptor
                .get("exampleUnavailable")
                .is_some_and(serde_json::Value::is_string)
        );
        Ok(())
    }

    #[test]
    #[allow(clippy::panic_in_result_fn)]
    fn wire_descriptions_follow_serde_and_examples_validate() -> Result<(), String> {
        let index = build_operation_index();
        let operation = index
            .iter()
            .find(|operation| operation.id == "WireContractFixture")
            .ok_or("fixture must be registered")?;
        let descriptor = operation.describe();
        assert_eq!(
            descriptor.pointer("/inputType/0/name"),
            Some(&serde_json::json!("wireName"))
        );
        assert_eq!(
            descriptor.pointer("/inputType/0/aliases"),
            Some(&serde_json::json!(["legacyName"]))
        );
        assert_eq!(
            descriptor.pointer("/inputType/1/name"),
            Some(&serde_json::json!("batchLimit"))
        );
        assert_eq!(
            descriptor.pointer("/inputType/1/optional"),
            Some(&serde_json::json!(true))
        );
        assert_eq!(operation.args.len(), 2, "skipped fields are not arguments");
        let example = descriptor
            .get("exampleArgs")
            .ok_or("missing example args")?;
        assert!(
            crate::reflection::validate_operation_input("command", "WireContractFixture", example)
                .is_ok()
        );
        assert!(
            crate::reflection::validate_operation_input(
                "command",
                "WireContractFixture",
                &serde_json::json!({"legacyName":"legacy"})
            )
            .is_ok()
        );
        assert!(
            crate::reflection::validate_operation_input(
                "command",
                "WireContractFixture",
                &serde_json::json!({"savedName":"wrong direction"})
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn result_wrappers_and_descriptions_share_all_fields() {
        for (contract, payload) in [
            (&QUERY_RESULT, serde_json::json!([1, 2])),
            (&VIEW_RESULT, serde_json::json!([])),
            (&REPORT_RESULT, serde_json::json!(42)),
            (&COMMAND_RESULT, serde_json::Value::Null),
        ] {
            let result = contract.wrap("Example", "Payload", payload);
            let shape = contract.describe("Payload");
            let result_keys = result
                .as_object()
                .map(|object| object.keys().collect::<Vec<_>>());
            let described_keys = shape
                .as_object()
                .map(|object| object.keys().collect::<Vec<_>>());
            assert_eq!(result_keys, described_keys);
        }
        assert_eq!(
            QUERY_RESULT
                .wrap("Example", "number", serde_json::json!([1, 2]))
                .get("count"),
            Some(&serde_json::json!(2))
        );
    }

    #[test]
    fn extracts_object_body_and_fields() {
        let content = "export type Foo = { a: string, b: number | null, };";
        let body = extract_exported_object_type_body(content, "Foo");
        assert!(body.is_some(), "object body");
        let Some(body) = body else {
            return;
        };
        let fields = parse_object_type_fields(&body);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields.first().map(|field| field.0.as_str()), Some("a"));
        assert_eq!(fields.first().map(|field| field.3), Some(false));
        assert_eq!(fields.get(1).map(|field| field.0.as_str()), Some("b"));
        assert_eq!(fields.get(1).map(|field| field.3), Some(true));
    }

    #[test]
    fn type_alias_without_object_body_yields_no_args() {
        let content = "export type Foo = Bar;";
        assert_eq!(extract_exported_object_type_body(content, "Foo"), None);
    }

    #[test]
    fn rust_type_to_ts_handles_option_vec_arc_and_primitives() {
        assert_eq!(rust_type_to_ts("Option < String >"), "string | null");
        assert_eq!(rust_type_to_ts("Vec < u32 >"), "number[]");
        assert_eq!(rust_type_to_ts("Arc < str >"), "string");
        assert_eq!(rust_type_to_ts("bool"), "boolean");
        // Rust module paths are not valid TypeScript type references; custom
        // types use the exported leaf binding name.
        assert_eq!(rust_type_to_ts("MyCrate :: Widget"), "Widget");
    }

    /// Exercises the whole macro → registration → index path against this
    /// crate's own real, compiled-in entities — not synthetic fixtures — to
    /// confirm field metadata actually reaches `search()` results.
    #[test]
    fn builds_against_real_registrations() {
        let index = build_operation_index();
        assert!(!index.is_empty(), "expected registered operations");

        let delete_server = index
            .iter()
            .find(|op| op.kind == "command" && op.id == "DeleteServer");
        assert!(
            delete_server.is_some(),
            "DeleteServer command must be in the index"
        );
        let Some(delete_server) = delete_server else {
            return;
        };
        assert!(
            delete_server.args.iter().any(|a| a.name == "id"),
            "DeleteServer's own `id` field should surface as an arg, got {:?}",
            delete_server.args
        );

        let get_all_servers = index
            .iter()
            .find(|op| op.kind == "query" && op.id == "GetAllServers");
        assert!(
            get_all_servers.is_some(),
            "GetAllServers query must be in the index"
        );
        let Some(get_all_servers) = get_all_servers else {
            return;
        };
        assert_eq!(get_all_servers.output_type, "Server[]");
    }
}
