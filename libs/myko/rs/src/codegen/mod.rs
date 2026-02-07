use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

use dprint_plugin_typescript::{
    FormatTextOptions,
    configuration::{ConfigurationBuilder, TrailingCommas},
};

use crate::{
    command::CommandRegistration, core::item::ItemRegistration, query::QueryRegistration,
    report::ReportRegistration, wire::MessageEventRegistration,
};

// ============================================================================
// TypeScript Constant Registration
// ============================================================================

/// Value types for TypeScript constant generation.
#[derive(Debug, PartialEq)]
pub enum TsConstValue {
    Str(&'static str),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// Registration for a Rust constant to be exported as a TypeScript constant.
pub struct TsConstRegistration {
    pub name: &'static str,
    pub value: TsConstValue,
    pub crate_name: &'static str,
}

inventory::collect!(TsConstRegistration);

/// Define a Rust constant and register it for TypeScript export.
///
/// Usage:
/// ```ignore
/// myko_rs::ts_const!(pub MY_CONST: &str = "value");
/// myko_rs::ts_const!(MY_PRIVATE: i64 = 42);
/// myko_rs::ts_const!(pub IS_ENABLED: bool = true);
/// ```
#[macro_export]
macro_rules! ts_const {
    (pub $name:ident : &str = $value:expr) => {
        pub const $name: &str = $value;
        $crate::inventory::submit! {
            $crate::codegen::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen::TsConstValue::Str($value),
                crate_name: module_path!(),
            }
        }
    };
    ($name:ident : &str = $value:expr) => {
        const $name: &str = $value;
        $crate::inventory::submit! {
            $crate::codegen::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen::TsConstValue::Str($value),
                crate_name: module_path!(),
            }
        }
    };
    (pub $name:ident : i64 = $value:expr) => {
        pub const $name: i64 = $value;
        $crate::inventory::submit! {
            $crate::codegen::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen::TsConstValue::Int($value),
                crate_name: module_path!(),
            }
        }
    };
    ($name:ident : i64 = $value:expr) => {
        const $name: i64 = $value;
        $crate::inventory::submit! {
            $crate::codegen::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen::TsConstValue::Int($value),
                crate_name: module_path!(),
            }
        }
    };
    (pub $name:ident : bool = $value:expr) => {
        pub const $name: bool = $value;
        $crate::inventory::submit! {
            $crate::codegen::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen::TsConstValue::Bool($value),
                crate_name: module_path!(),
            }
        }
    };
    ($name:ident : bool = $value:expr) => {
        const $name: bool = $value;
        $crate::inventory::submit! {
            $crate::codegen::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen::TsConstValue::Bool($value),
                crate_name: module_path!(),
            }
        }
    };
}

// ============================================================================
// ts-rs Export Registration
// ============================================================================

/// Registration for ts-rs type exports.
/// Allows types with `#[derive(TS)]` to be exported without running cargo test.
pub struct TsExportRegistration {
    /// Name of the type being exported (for logging)
    pub type_name: &'static str,
    /// Function that calls `T::export()` for the registered type
    pub export_fn: fn() -> Result<(), ts_rs::ExportError>,
}

inventory::collect!(TsExportRegistration);

/// Register a type for ts-rs export.
#[macro_export]
macro_rules! register_ts_export {
    ($ty:ty) => {
        $crate::inventory::submit! {
            $crate::codegen::TsExportRegistration {
                type_name: stringify!($ty),
                export_fn: || <$ty as $crate::ts_rs::TS>::export(),
            }
        }
    };
    ($($ty:ty),+ $(,)?) => {
        $(
            $crate::register_ts_export!($ty);
        )+
    };
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

pub fn generate_item_types() -> Result<(), anyhow::Error> {
    let directory_path = "bindings";
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

    // Handle Option<T> -> T | null
    if trimmed.starts_with("Option")
        && let Some(start) = trimmed.find('<')
        && let Some(end) = trimmed.rfind('>')
    {
        let inner = trimmed[start + 1..end].trim();
        let inner_ts = rust_type_to_ts(inner);
        return format!("{} | null", inner_ts);
    }

    // Handle Vec<T> -> T[]
    if trimmed.starts_with("Vec")
        && let Some(start) = trimmed.find('<')
        && let Some(end) = trimmed.rfind('>')
    {
        let inner = trimmed[start + 1..end].trim();
        let inner_ts = rust_type_to_ts(inner);
        return format!("{}[]", inner_ts);
    }

    // Handle Arc<T> -> unwrap to inner type
    if trimmed.starts_with("Arc")
        && let Some(start) = trimmed.find('<')
        && let Some(end) = trimmed.rfind('>')
    {
        let inner = trimmed[start + 1..end].trim();
        return rust_type_to_ts(inner);
    }

    // Map Rust primitive types to TypeScript
    match trimmed {
        "str" | "String" => "string".to_string(),
        "bool" => "boolean".to_string(),
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64" | "u128"
        | "usize" | "f32" | "f64" => "number".to_string(),
        "()" => "void".to_string(),
        // serde_json::Value maps to JsonValue in ts-rs
        "Value" | "serde_json::Value" => "JsonValue".to_string(),
        _ => trimmed.to_string(),
    }
}

fn generate_item_constructor(item_name: &str) -> String {
    format!(
        "  {}: (args: Omit<{}, 'hash'>) => ({{ item: args, itemType: \"{}\" }})",
        item_name, item_name, item_name
    )
}

fn extract_importable_types(rust_type: &str) -> Vec<String> {
    let trimmed = rust_type.trim();

    // Handle Option<T> - extract inner type
    if trimmed.starts_with("Option")
        && let Some(start) = trimmed.find('<')
        && let Some(end) = trimmed.rfind('>')
    {
        let inner = trimmed[start + 1..end].trim();
        return extract_importable_types(inner);
    }

    // Handle Vec<T> - extract inner type
    if trimmed.starts_with("Vec")
        && let Some(start) = trimmed.find('<')
        && let Some(end) = trimmed.rfind('>')
    {
        let inner = trimmed[start + 1..end].trim();
        return extract_importable_types(inner);
    }

    // Handle Arc<T> - extract inner type
    if trimmed.starts_with("Arc")
        && let Some(start) = trimmed.find('<')
        && let Some(end) = trimmed.rfind('>')
    {
        let inner = trimmed[start + 1..end].trim();
        return extract_importable_types(inner);
    }

    // Filter out Rust primitive types that don't need imports
    let primitives = [
        "str", "String", "bool", "()", "i8", "i16", "i32", "i64", "i128", "isize", "u8", "u16",
        "u32", "u64", "u128", "usize", "f32", "f64",
    ];

    if primitives.contains(&trimmed) {
        return vec![];
    }

    // Map serde_json::Value to JsonValue
    if trimmed == "Value" || trimmed == "serde_json::Value" {
        return vec!["JsonValue".to_string()];
    }

    let clean_type = trimmed.replace(" ", "");
    vec![clean_type]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_index() {
        generate_item_types().expect("Failed to generate index.ts");
    }
}
