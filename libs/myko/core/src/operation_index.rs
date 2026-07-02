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
//!    `crate::codegen` for its *doc generation* pass
//!    (`generate_docs_json_from_bindings`) — a genuinely separate, still
//!    file-based feature for producing the TS SDK's prose docs, unrelated
//!    to MCP. This module intentionally does not depend on
//!    `dprint-plugin-typescript` itself (unlike `crate::codegen`), so it can
//!    be an always-on dependency of `myko-server` without forcing every
//!    server binary to pull in a full TS formatter.

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
}

impl From<&OperationArgField> for OperationArg {
    fn from(field: &OperationArgField) -> Self {
        OperationArg {
            name: field.name.to_string(),
            ts_type: rust_type_to_ts(field.rust_type),
            optional: field.optional,
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

/// Build the full operation index from `inventory`-registered
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
            output_type: format!("{}[]", reg.query_item_type),
        });
    }
    for reg in inventory::iter::<ViewRegistration> {
        index.push(OperationSchema {
            id: reg.view_id.to_string(),
            kind: "view".to_string(),
            description: reg.description.map(str::to_string),
            args: reg.args.iter().map(OperationArg::from).collect(),
            output_type: format!("{}[]", reg.view_item_type),
        });
    }
    for reg in inventory::iter::<ReportRegistration> {
        index.push(OperationSchema {
            id: reg.report_id.to_string(),
            kind: "report".to_string(),
            description: reg.description.map(str::to_string),
            args: reg.args.iter().map(OperationArg::from).collect(),
            output_type: reg.output_type.to_string(),
        });
    }
    for reg in inventory::iter::<CommandRegistration> {
        index.push(OperationSchema {
            id: reg.command_id.to_string(),
            kind: "command".to_string(),
            description: reg.description.map(str::to_string),
            args: reg.args.iter().map(OperationArg::from).collect(),
            output_type: reg.result_type.to_string(),
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
        _ => canonical.to_string(),
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
    let outer = s[..start].trim();
    let inner = s[start + 1..end].trim();
    if outer.is_empty() || inner.is_empty() {
        return None;
    }
    Some((outer, inner))
}

#[cfg_attr(not(feature = "codegen"), allow(dead_code))]
pub(crate) fn split_generic_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (idx, ch) in s.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                let arg = s[start..idx].trim();
                if !arg.is_empty() {
                    args.push(arg.to_string());
                }
                start = idx + 1;
            }
            _ => {}
        }
    }
    let tail = s[start..].trim();
    if !tail.is_empty() {
        args.push(tail.to_string());
    }
    args
}

// ─────────────────────────────────────────────────────────────────────────────
// ts-rs `.ts` binding text parsing — shared with `crate::codegen`'s docgen.
// ─────────────────────────────────────────────────────────────────────────────

// Only consumed by `crate::codegen::generate_docs_json_from_bindings`, which
// is gated behind the (non-default) `codegen` feature.
#[cfg_attr(not(feature = "codegen"), allow(dead_code))]
pub(crate) fn collect_ts_binding_files(bindings_dir: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
    let mut files = Vec::new();
    for entry in fs::read_dir(bindings_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let is_ts = path.extension().map(|x| x == "ts").unwrap_or(false);
        let is_dts = path
            .file_name()
            .and_then(|x| x.to_str())
            .map(|x| x.ends_with(".d.ts"))
            .unwrap_or(false);
        if is_ts && !is_dts {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

#[cfg_attr(not(feature = "codegen"), allow(dead_code))]
pub(crate) fn extract_exported_object_type_body(content: &str, type_name: &str) -> Option<String> {
    let marker = format!("export type {type_name} =");
    let start = content.find(&marker)?;
    let rest = &content[start + marker.len()..];
    let brace_start_rel = rest.find('{')?;
    let brace_start = start + marker.len() + brace_start_rel;

    let mut depth = 0usize;
    let mut end_idx = None;
    for (i, ch) in content[brace_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    end_idx = Some(brace_start + i);
                    break;
                }
            }
            _ => {}
        }
    }
    let end_idx = end_idx?;
    Some(content[brace_start + 1..end_idx].to_string())
}

/// Returns `(field_name, ts_type, doc_comment, optional)` for each field of
/// a parsed ts-rs object type body.
#[cfg_attr(not(feature = "codegen"), allow(dead_code))]
pub(crate) fn parse_object_type_fields(body: &str) -> Vec<(String, String, Option<String>, bool)> {
    let mut fields = Vec::new();
    let mut pending_doc: Option<String> = None;

    for segment in split_top_level_commas(body) {
        let mut segment = segment.trim().to_string();
        if segment.is_empty() {
            continue;
        }

        while let Some(start) = segment.find("/**") {
            let tail = &segment[start + 3..];
            let Some(end_rel) = tail.find("*/") else {
                break;
            };
            let end = start + 3 + end_rel + 2;
            let comment_block = &segment[start..end];
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
        let raw_name = segment[..colon_idx].trim();
        let raw_type = segment[colon_idx + 1..].trim();

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

#[cfg_attr(not(feature = "codegen"), allow(dead_code))]
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
        let ch = chars[i];
        let next = if i + 1 < chars.len() {
            Some(chars[i + 1])
        } else {
            None
        };
        let prev = if i > 0 { Some(chars[i - 1]) } else { None };
        if in_block_comment {
            if ch == '*' && next == Some('/') {
                in_block_comment = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if ch == '/' && next == Some('*') {
            in_block_comment = true;
            i += 2;
            continue;
        }
        if ch == '/' && next == Some('/') {
            in_line_comment = true;
            i += 2;
            continue;
        }
        if ch == '\'' && !in_double && prev != Some('\\') {
            in_single = !in_single;
        } else if ch == '"' && !in_single && prev != Some('\\') {
            in_double = !in_double;
        } else if !in_single && !in_double {
            match ch {
                '{' => brace += 1,
                '}' => brace = brace.saturating_sub(1),
                '[' => bracket += 1,
                ']' => bracket = bracket.saturating_sub(1),
                '(' => paren += 1,
                ')' => paren = paren.saturating_sub(1),
                '<' => angle += 1,
                '>' => angle = angle.saturating_sub(1),
                ',' if brace == 0 && bracket == 0 && paren == 0 && angle == 0 => {
                    out.push(chars[start..i].iter().collect::<String>());
                    start = i + 1;
                }
                _ => {}
            }
        }
        i += 1;
    }
    if start < chars.len() {
        out.push(chars[start..].iter().collect::<String>());
    }
    out
}

#[cfg_attr(not(feature = "codegen"), allow(dead_code))]
fn find_top_level_colon(input: &str) -> Option<usize> {
    let chars: Vec<char> = input.chars().collect();
    let mut brace = 0usize;
    let mut bracket = 0usize;
    let mut paren = 0usize;
    let mut angle = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    for (i, ch) in chars.iter().enumerate() {
        let prev = if i > 0 { Some(chars[i - 1]) } else { None };
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
            '{' => brace += 1,
            '}' => brace = brace.saturating_sub(1),
            '[' => bracket += 1,
            ']' => bracket = bracket.saturating_sub(1),
            '(' => paren += 1,
            ')' => paren = paren.saturating_sub(1),
            '<' => angle += 1,
            '>' => angle = angle.saturating_sub(1),
            ':' if brace == 0 && bracket == 0 && paren == 0 && angle == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

#[cfg_attr(not(feature = "codegen"), allow(dead_code))]
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

    #[test]
    fn extracts_object_body_and_fields() {
        let content = "export type Foo = { a: string, b: number | null, };";
        let body = extract_exported_object_type_body(content, "Foo").expect("body");
        let fields = parse_object_type_fields(&body);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "a");
        assert!(!fields[0].3);
        assert_eq!(fields[1].0, "b");
        assert!(fields[1].3, "`T | null` field should be optional");
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
        // Unrecognized (non-primitive) types keep their full canonical path
        // rather than being leaf-truncated — only the primitive-matching
        // arm does that.
        assert_eq!(rust_type_to_ts("MyCrate :: Widget"), "MyCrate::Widget");
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
            .find(|op| op.kind == "command" && op.id == "DeleteServer")
            .expect("DeleteServer command must be in the index");
        assert!(
            delete_server.args.iter().any(|a| a.name == "id"),
            "DeleteServer's own `id` field should surface as an arg, got {:?}",
            delete_server.args
        );

        let get_all_servers = index
            .iter()
            .find(|op| op.kind == "query" && op.id == "GetAllServers")
            .expect("GetAllServers query must be in the index");
        assert_eq!(get_all_servers.output_type, "Server[]");
    }
}
