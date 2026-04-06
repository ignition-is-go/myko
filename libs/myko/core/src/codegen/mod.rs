use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use dprint_plugin_typescript::{
    FormatTextOptions,
    configuration::{ConfigurationBuilder, TrailingCommas},
};

use crate::{
    codegen_types::{TsConstRegistration, TsConstValue, TsExportRegistration},
    command::CommandRegistration,
    core::item::ItemRegistration,
    query::QueryRegistration,
    report::ReportRegistration,
    view::ViewRegistration,
    wire::MessageEventRegistration,
};

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct DocEntry {
    entity_type: String,
    kind: String,
    prop_name: String,
    #[serde(rename = "type")]
    entry_type: String,
    prop_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc_string: Option<String>,
}

/// Export all registered ts-rs types to the bindings directory.
pub fn export_registered_ts_types() -> Result<(), anyhow::Error> {
    let mut success_count = 0;
    let mut error_count = 0;

    for registration in inventory::iter::<TsExportRegistration> {
        match (registration.export_fn)() {
            Ok(()) => {
                println!("  Exported: {}", registration.type_name);
                success_count += 1;
            }
            Err(e) => {
                eprintln!("  Failed to export {}: {}", registration.type_name, e);
                error_count += 1;
            }
        }
    }

    println!(
        "ts-rs export complete: {} succeeded, {} failed",
        success_count, error_count
    );

    if error_count > 0 {
        anyhow::bail!("{} ts-rs exports failed", error_count);
    }

    Ok(())
}

fn collect_binding_types(directory_path: &str) -> Vec<String> {
    let mut types = Vec::new();

    if let Ok(entries) = fs::read_dir(directory_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            let filename = path.file_name().map(|n| n.to_string_lossy().to_string());
            if path.is_file()
                && path.extension().map(|e| e == "ts").unwrap_or(false)
                && let Some(ref fname) = filename
                && !fname.ends_with(".d.ts")
                && let Some(name) = path.file_stem()
            {
                let name = name.to_string_lossy().to_string();
                if name != "index" {
                    types.push(name);
                }
            }
        }
    }

    types.sort();
    types
}

fn collect_subdir_types(directory_path: &str) -> Vec<(String, String)> {
    let mut types = Vec::new();

    if let Ok(entries) = fs::read_dir(directory_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let subdir_name = path.file_name().unwrap().to_string_lossy().to_string();
                if let Ok(subentries) = fs::read_dir(&path) {
                    for subentry in subentries.flatten() {
                        let subpath = subentry.path();
                        let filename = subpath.file_name().map(|n| n.to_string_lossy().to_string());
                        if subpath.is_file()
                            && subpath.extension().map(|e| e == "ts").unwrap_or(false)
                            && let Some(ref fname) = filename
                            && !fname.ends_with(".d.ts")
                            && let Some(name) = subpath.file_stem()
                        {
                            let name = name.to_string_lossy().to_string();
                            types.push((subdir_name.clone(), name));
                        }
                    }
                }
            }
        }
    }

    types
}

pub fn generate_item_types(directory_path: &str) -> Result<(), anyhow::Error> {
    let file_name = "index.ts";

    fs::create_dir_all(directory_path)?;

    println!("Exporting ts-rs types...");
    export_registered_ts_types()?;

    let crate_name = std::env::var("CARGO_PKG_NAME")
        .expect("CARGO_PKG_NAME environment variable not found")
        .replace("-", "_");
    println!("The current crate name is: {}", crate_name);

    let binding_types = collect_binding_types(directory_path);
    let subdir_types = collect_subdir_types(directory_path);

    let items: Vec<_> = inventory::iter::<ItemRegistration>()
        .filter(|x| x.crate_name.contains(&crate_name))
        .collect();
    let queries: Vec<_> = inventory::iter::<QueryRegistration>()
        .filter(|x| x.crate_name.contains(&crate_name))
        .collect();
    let views: Vec<_> = inventory::iter::<ViewRegistration>()
        .filter(|x| x.crate_name.contains(&crate_name))
        .collect();
    let reports: Vec<_> = inventory::iter::<ReportRegistration>()
        .filter(|x| x.crate_name.contains(&crate_name))
        .collect();
    let commands: Vec<_> = inventory::iter::<CommandRegistration>()
        .filter(|x| x.crate_name.contains(&crate_name))
        .collect();

    // Collect query/report/command type names (these will be classes, not re-exported types)
    let class_type_names: HashSet<&str> = queries
        .iter()
        .map(|q| q.query_id)
        .chain(views.iter().map(|v| v.view_id))
        .chain(reports.iter().map(|r| r.report_id))
        .chain(commands.iter().map(|c| c.command_id))
        .collect();

    // Re-export types that are NOT query/report/command classes
    let binding_exports = binding_types
        .iter()
        .filter(|name| !class_type_names.contains(name.as_str()))
        .map(|name| format!("export type {{ {} }} from \"./{}\";", name, name))
        .collect::<Vec<String>>()
        .join("\n");

    // Use `export *` for subdirectory files since they may export types and/or values
    let subdir_exports = subdir_types
        .iter()
        .map(|(subdir, name)| format!("export * from \"./{}/{}\";", subdir, name))
        .collect::<Vec<String>>()
        .join("\n");

    // Collect entity types for imports
    let mut entity_types: HashSet<String> = HashSet::new();
    for item in &items {
        entity_types.insert(item.entity_type.to_string());
    }
    for query in &queries {
        entity_types.insert(query.query_item_type.to_string());
    }
    for view in &views {
        entity_types.insert(view.view_item_type.to_string());
    }
    for report in reports
        .iter()
        .filter(|r| r.output_type_crate.contains(&crate_name))
    {
        for t in extract_importable_types(report.output_type) {
            entity_types.insert(t);
        }
    }
    for command in commands
        .iter()
        .filter(|c| c.result_type_crate.contains(&crate_name) && c.result_type != "()")
    {
        for t in extract_importable_types(command.result_type) {
            entity_types.insert(t);
        }
    }

    // Generate imports for entity types (no alias needed)
    let entity_imports = entity_types
        .iter()
        .filter(|t| !class_type_names.contains(t.as_str()))
        .map(|t| {
            // Handle special import paths
            let import_path = if t == "JsonValue" {
                "./serde_json/JsonValue".to_string()
            } else {
                format!("./{t}")
            };
            format!("import type {{ {} }} from '{}';", t, import_path)
        })
        .collect::<Vec<String>>()
        .join("\n");

    // Generate aliased imports for query/report/command types (to avoid name collision with classes)
    let aliased_imports = class_type_names
        .iter()
        .map(|name| format!("import type {{ {} as _{} }} from './{name}';", name, name))
        .collect::<Vec<String>>()
        .join("\n");

    // Generate query classes
    let query_classes = queries
        .iter()
        .map(|q| generate_query_class(q.query_id, q.query_item_type))
        .collect::<Vec<String>>()
        .join("\n\n");
    let view_classes = views
        .iter()
        .map(|v| generate_view_class(v.view_id, v.view_item_type))
        .collect::<Vec<String>>()
        .join("\n\n");

    // Generate report classes
    let report_classes = reports
        .iter()
        .map(|r| generate_report_class(r.report_id, r.output_type))
        .collect::<Vec<String>>()
        .join("\n\n");

    // Generate command classes
    let command_classes = commands
        .iter()
        .map(|c| generate_command_class(c.command_id, c.result_type))
        .collect::<Vec<String>>()
        .join("\n\n");

    // Generate item constructors (keep as object for now)
    let item_ctors = items
        .iter()
        .map(|i| generate_item_constructor(i.entity_type))
        .collect::<Vec<String>>()
        .join(",\n");
    let item_ctor_obj = format!("export const items = {{\n{}\n}};", item_ctors);

    // Shared constants
    let consts: Vec<_> = inventory::iter::<TsConstRegistration>()
        .filter(|x| x.crate_name.contains(&crate_name))
        .collect();

    // Deduplicate constants by name — allow duplicates with the same value,
    // but bail on conflicting values for the same name.
    let mut seen_consts: HashMap<&str, &TsConstValue> = HashMap::new();
    let mut deduped_consts: Vec<&TsConstRegistration> = Vec::new();
    for c in &consts {
        if let Some(existing) = seen_consts.get(c.name) {
            if !c.value.eq(existing) {
                anyhow::bail!(
                    "Conflicting ts_const values for '{}': {:?} vs {:?}",
                    c.name,
                    existing,
                    c.value
                );
            }
            // Same name + same value — skip duplicate
            continue;
        }
        seen_consts.insert(c.name, &c.value);
        deduped_consts.push(c);
    }

    let const_exports = deduped_consts
        .iter()
        .map(|c| {
            let ts_value = match &c.value {
                TsConstValue::Str(s) => format!("'{}'", s),
                TsConstValue::Int(n) => n.to_string(),
                TsConstValue::Float(f) => f.to_string(),
                TsConstValue::Bool(b) => b.to_string(),
            };
            format!("export const {} = {} as const", c.name, ts_value)
        })
        .collect::<Vec<_>>()
        .join("\n");

    // Message event constants
    let message_event_entries = inventory::iter::<MessageEventRegistration>()
        .map(|r| format!("  {}: '{}',", r.variant_name, r.event_value))
        .collect::<Vec<String>>()
        .join("\n");

    let message_events = format!(
        r#"export const MykoEvent = {{
{}
}} as const;
export type MykoEventType = typeof MykoEvent[keyof typeof MykoEvent];"#,
        message_event_entries
    );

    let code = [
        "// Auto-generated by type_gen - do not edit manually".to_string(),
        "".to_string(),
        "// Core type aliases".to_string(),
        "/** Entity identifier type. In Rust this is Arc<str>, serialized as string. */"
            .to_string(),
        "export type ID = string;".to_string(),
        "".to_string(),
        "// Re-export ts-rs generated types".to_string(),
        binding_exports,
        subdir_exports,
        "".to_string(),
        "// Internal imports".to_string(),
        entity_imports,
        aliased_imports,
        "".to_string(),
        "// Query classes".to_string(),
        query_classes,
        "".to_string(),
        "// View classes".to_string(),
        view_classes,
        "".to_string(),
        "// Report classes".to_string(),
        report_classes,
        "".to_string(),
        "// Command classes".to_string(),
        command_classes,
        "".to_string(),
        "// Item constructors".to_string(),
        item_ctor_obj,
        "".to_string(),
        "// Message events".to_string(),
        message_events,
        "".to_string(),
        "// Shared constants".to_string(),
        const_exports,
    ]
    .join("\n");

    let file_path = Path::new(directory_path).join(file_name);

    let config = ConfigurationBuilder::new()
        .arguments_trailing_commas(TrailingCommas::Always)
        .build();

    let code = dprint_plugin_typescript::format_text(FormatTextOptions {
        path: &file_path,
        extension: None,
        text: code,
        config: &config,
        external_formatter: None,
    })?;

    if code.is_none() {
        anyhow::bail!("Generated code is empty");
    }

    fs::write(&file_path, code.unwrap())?;
    println!("Successfully wrote to file: {}", file_path.display());

    Ok(())
}

/// Generate docs JSON entries from ts-rs binding files.
///
/// This is intentionally separate from `generate_item_types` so callers can
/// run docs generation independently (or in addition to TS type generation).
pub fn generate_docs_json_from_bindings(
    bindings_dir: impl AsRef<Path>,
    output_file: impl AsRef<Path>,
) -> Result<(), anyhow::Error> {
    let bindings_dir = bindings_dir.as_ref();
    let output_file = output_file.as_ref();

    if !bindings_dir.exists() {
        anyhow::bail!(
            "Bindings directory does not exist: {}",
            bindings_dir.display()
        );
    }

    let mut entries = Vec::<DocEntry>::new();
    for file in collect_ts_binding_files(bindings_dir)? {
        let content = fs::read_to_string(&file)?;
        let Some(entity_type) = file.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Some(body) = extract_exported_object_type_body(&content, entity_type) else {
            continue;
        };
        for (prop_name, prop_type, doc_string) in parse_object_type_fields(&body) {
            entries.push(DocEntry {
                entity_type: entity_type.to_string(),
                kind: "prop".to_string(),
                prop_name,
                entry_type: "prop".to_string(),
                prop_type,
                doc_string,
            });
        }
    }

    entries.sort_by(|a, b| {
        a.entity_type
            .cmp(&b.entity_type)
            .then_with(|| a.prop_name.cmp(&b.prop_name))
    });

    if let Some(parent) = output_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(&entries)?;
    fs::write(output_file, json)?;
    println!("Successfully wrote docs JSON: {}", output_file.display());

    Ok(())
}

fn collect_ts_binding_files(bindings_dir: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
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

fn extract_exported_object_type_body(content: &str, type_name: &str) -> Option<String> {
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

fn parse_object_type_fields(body: &str) -> Vec<(String, String, Option<String>)> {
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
        if prop_name.is_empty() || prop_name == "id" || prop_name == "hash" {
            pending_doc = None;
            continue;
        }

        let prop_type = raw_type.trim().to_string();
        if prop_type.is_empty() {
            pending_doc = None;
            continue;
        }

        fields.push((prop_name, prop_type, pending_doc.take()));
    }

    fields
}

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

fn generate_query_class(query_id: &str, query_item_type: &str) -> String {
    format!(
        r#"export class {query_id} {{
  static readonly queryId = "{query_id}" as const;
  static readonly queryItemType = "{query_item_type}" as const;
  readonly queryId = "{query_id}" as const;
  readonly queryItemType = "{query_item_type}" as const;
  readonly query: Omit<_{query_id}, 'tx' | 'createdAt'>;
  declare readonly $res: () => {query_item_type}[];

  constructor(args: Omit<_{query_id}, 'tx' | 'createdAt'>) {{
    this.query = args;
  }}
}}"#
    )
}

fn generate_view_class(view_id: &str, view_item_type: &str) -> String {
    format!(
        r#"export class {view_id} {{
  static readonly viewId = "{view_id}" as const;
  static readonly viewItemType = "{view_item_type}" as const;
  readonly viewId = "{view_id}" as const;
  readonly viewItemType = "{view_item_type}" as const;
  readonly view: Omit<_{view_id}, 'tx' | 'createdAt'>;
  declare readonly $res: () => {view_item_type}[];

  constructor(args: Omit<_{view_id}, 'tx' | 'createdAt'>) {{
    this.view = args;
  }}
}}"#
    )
}

fn generate_report_class(report_id: &str, output_type: &str) -> String {
    let ts_output_type = rust_type_to_ts(output_type);
    format!(
        r#"export class {report_id} {{
  static readonly reportId = "{report_id}" as const;
  readonly reportId = "{report_id}" as const;
  readonly report: Omit<_{report_id}, 'tx'>;
  declare readonly $res: () => {ts_output_type};

  constructor(args: Omit<_{report_id}, 'tx'>) {{
    this.report = args;
  }}
}}"#
    )
}

fn generate_command_class(command_id: &str, result_type: &str) -> String {
    let ts_result_type = if result_type == "()" {
        "void".to_string()
    } else {
        rust_type_to_ts(result_type)
    };
    format!(
        r#"export class {command_id} {{
  static readonly commandId = "{command_id}" as const;
  readonly commandId = "{command_id}" as const;
  readonly command: Omit<_{command_id}, 'tx' | 'createdAt'>;
  declare readonly $res: () => {ts_result_type};

  constructor(args: Omit<_{command_id}, 'tx' | 'createdAt'>) {{
    this.command = args;
  }}
}}"#
    )
}

fn rust_type_to_ts(rust_type: &str) -> String {
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

fn generate_item_constructor(item_name: &str) -> String {
    format!(
        "  {}: (args: {}) => ({{ item: args, itemType: \"{}\" }})",
        item_name, item_name, item_name
    )
}

fn extract_importable_types(rust_type: &str) -> Vec<String> {
    let trimmed = rust_type.trim();
    let canonical = trimmed.replace(' ', "");
    let canonical = canonical.as_str();
    if let Some((outer, inner)) = split_outer_generic(canonical) {
        match outer_leaf(outer) {
            // Handle Option<T>/Vec<T>/Arc<T> - extract inner type
            "Option" | "Vec" | "Arc" => return extract_importable_types(inner),
            // For other generics, collect importables from type arguments
            _ => {
                return split_generic_args(inner)
                    .into_iter()
                    .flat_map(|arg| extract_importable_types(&arg))
                    .collect();
            }
        }
    }

    // Filter out Rust primitive types that don't need imports
    let primitives = [
        "str", "String", "bool", "()", "i8", "i16", "i32", "i64", "i128", "isize", "u8", "u16",
        "u32", "u64", "u128", "usize", "f32", "f64",
    ];

    if primitives.contains(&outer_leaf(canonical)) {
        return vec![];
    }

    // Map serde_json::Value to JsonValue
    if trimmed == "Value" || trimmed == "serde_json::Value" {
        return vec!["JsonValue".to_string()];
    }

    let clean_type = outer_leaf(canonical).to_string();
    vec![clean_type]
}

fn outer_leaf(path_or_ident: &str) -> &str {
    path_or_ident.rsplit("::").next().unwrap_or(path_or_ident)
}

fn split_outer_generic(s: &str) -> Option<(&str, &str)> {
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

fn split_generic_args(s: &str) -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_index() {
        generate_item_types("bindings").expect("Failed to generate index.ts");
    }
}
