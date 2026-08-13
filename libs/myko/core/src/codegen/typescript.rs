use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

use anyhow::Context;
use dprint_plugin_typescript::{
    FormatTextOptions,
    configuration::{ConfigurationBuilder, TrailingCommas},
};

#[path = "typegen_typescript.rs"]
mod typegen_typescript;

use crate::{
    codegen_types::{TypegenCatalog, TypegenConstValue},
    operation_index::{
        collect_ts_binding_files, extract_exported_object_type_body, parse_object_type_fields,
    },
    typegen_typescript::TypeExportRegistration,
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

fn typescript_adapters_for_catalog<'a>(
    catalog: &TypegenCatalog,
    adapters: impl IntoIterator<Item = &'a TypeExportRegistration>,
) -> Vec<&'a TypeExportRegistration> {
    let type_ids = catalog.type_ids();
    adapters
        .into_iter()
        .filter(|adapter| type_ids.contains(adapter.type_id))
        .collect()
}

/// Export all registered ts-rs types to the bindings directory.
///
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
fn export_registered_ts_types_for_catalog(catalog: &TypegenCatalog) -> Result<(), anyhow::Error> {
    let mut success_count = 0_u64;
    let mut error_count = 0_u64;

    for registration in
        typescript_adapters_for_catalog(catalog, inventory::iter::<TypeExportRegistration>)
    {
        match (registration.export_fn)() {
            Ok(()) => {
                println!("  Exported: {}", registration.type_name);
                success_count = success_count.saturating_add(1);
            }
            Err(e) => {
                eprintln!("  Failed to export {}: {}", registration.type_name, e);
                error_count = error_count.saturating_add(1);
            }
        }
    }

    println!("ts-rs export complete: {success_count} succeeded, {error_count} failed");

    if error_count > 0 {
        anyhow::bail!("{error_count} ts-rs exports failed");
    }

    Ok(())
}

/// Export the current crate's registered `ts-rs` types.
///
/// # Errors
///
/// Returns an error when the crate name is unavailable or an adapter export fails.
pub fn export_registered_ts_types() -> Result<(), anyhow::Error> {
    let crate_name = std::env::var("CARGO_PKG_NAME")
        .context("CARGO_PKG_NAME environment variable not found")?
        .replace('-', "_");
    export_registered_ts_types_for_catalog(&TypegenCatalog::collect(&crate_name))
}

fn collect_binding_types(directory_path: &str) -> Vec<String> {
    let mut types = Vec::new();

    if let Ok(entries) = fs::read_dir(directory_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            let filename = path.file_name().map(|n| n.to_string_lossy().to_string());
            if path.is_file()
                && path.extension().is_some_and(|e| e == "ts")
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
                let Some(subdir_name) = path
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
                else {
                    continue;
                };
                if let Ok(subentries) = fs::read_dir(&path) {
                    for subentry in subentries.flatten() {
                        let subpath = subentry.path();
                        let filename = subpath.file_name().map(|n| n.to_string_lossy().to_string());
                        if subpath.is_file()
                            && subpath.extension().is_some_and(|e| e == "ts")
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

fn registration_crate_root(path: &str) -> Option<&str> {
    path.split("::").next()
}

fn catalog_crate_roots(catalog: &TypegenCatalog) -> HashSet<&str> {
    catalog
        .types
        .iter()
        .filter_map(|entry| registration_crate_root(entry.crate_path))
        .chain(
            catalog
                .constants
                .iter()
                .filter_map(|entry| registration_crate_root(entry.crate_path)),
        )
        .chain(
            catalog
                .modules
                .iter()
                .filter_map(|entry| registration_crate_root(entry.crate_path)),
        )
        .chain(
            catalog
                .items
                .iter()
                .filter_map(|entry| registration_crate_root(entry.crate_name)),
        )
        .chain(
            catalog
                .queries
                .iter()
                .filter_map(|entry| registration_crate_root(entry.crate_name)),
        )
        .chain(
            catalog
                .views
                .iter()
                .filter_map(|entry| registration_crate_root(entry.crate_name)),
        )
        .chain(
            catalog
                .reports
                .iter()
                .filter_map(|entry| registration_crate_root(entry.crate_name)),
        )
        .chain(
            catalog
                .commands
                .iter()
                .filter_map(|entry| registration_crate_root(entry.crate_name)),
        )
        .collect()
}

fn generate_import_sections(
    directory_path: &str,
    catalog: &TypegenCatalog,
) -> (String, String, String, String) {
    let selected_crates = catalog_crate_roots(catalog);
    let class_type_names: HashSet<&str> = catalog
        .queries
        .iter()
        .map(|query| query.query_id)
        .chain(catalog.views.iter().map(|view| view.view_id))
        .chain(catalog.reports.iter().map(|report| report.report_id))
        .chain(catalog.commands.iter().map(|command| command.command_id))
        .collect();
    let binding_exports = collect_binding_types(directory_path)
        .iter()
        .filter(|name| !class_type_names.contains(name.as_str()))
        .map(|name| format!("export type {{ {name} }} from \"./{name}\";"))
        .collect::<Vec<_>>()
        .join("\n");
    let subdir_exports = collect_subdir_types(directory_path)
        .iter()
        .map(|(subdir, name)| format!("export * from \"./{subdir}/{name}\";"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut entity_types: HashSet<String> = catalog
        .items
        .iter()
        .map(|item| item.entity_type.to_string())
        .chain(
            catalog
                .queries
                .iter()
                .map(|query| query.query_item_type.to_string()),
        )
        .chain(
            catalog
                .views
                .iter()
                .map(|view| view.view_item_type.to_string()),
        )
        .collect();
    for report in catalog.reports.iter().filter(|report| {
        registration_crate_root(report.output_type_crate)
            .is_some_and(|name| selected_crates.contains(name))
    }) {
        entity_types.extend(extract_importable_types(report.output_type));
    }
    for command in catalog.commands.iter().filter(|command| {
        registration_crate_root(command.result_type_crate)
            .is_some_and(|name| selected_crates.contains(name))
            && command.result_type != "()"
    }) {
        entity_types.extend(extract_importable_types(command.result_type));
    }
    let entity_imports = entity_types
        .iter()
        .filter(|name| !class_type_names.contains(name.as_str()))
        .map(|name| {
            let path = if name == "JsonValue" {
                "./serde_json/JsonValue".to_string()
            } else {
                format!("./{name}")
            };
            format!("import type {{ {name} }} from '{path}';")
        })
        .collect::<Vec<_>>()
        .join("\n");
    let aliased_imports = class_type_names
        .iter()
        .map(|name| format!("import type {{ {name} as _{name} }} from './{name}';"))
        .collect::<Vec<_>>()
        .join("\n");
    (
        binding_exports,
        subdir_exports,
        entity_imports,
        aliased_imports,
    )
}

fn generate_class_sections(catalog: &TypegenCatalog) -> [String; 5] {
    let query_classes = catalog
        .queries
        .iter()
        .map(|query| generate_query_class(query.query_id, query.query_item_type))
        .collect::<Vec<_>>()
        .join("\n\n");
    let view_classes = catalog
        .views
        .iter()
        .map(|view| generate_view_class(view.view_id, view.view_item_type))
        .collect::<Vec<_>>()
        .join("\n\n");
    let report_classes = catalog
        .reports
        .iter()
        .map(|report| generate_report_class(report.report_id, report.output_type))
        .collect::<Vec<_>>()
        .join("\n\n");
    let command_classes = catalog
        .commands
        .iter()
        .map(|command| generate_command_class(command.command_id, command.result_type))
        .collect::<Vec<_>>()
        .join("\n\n");
    let item_constructors = catalog
        .items
        .iter()
        .map(|item| generate_item_constructor(item.entity_type))
        .collect::<Vec<_>>()
        .join(",\n");
    [
        query_classes,
        view_classes,
        report_classes,
        command_classes,
        format!("export const items = {{\n{item_constructors}\n}};"),
    ]
}

fn generate_const_exports(catalog: &TypegenCatalog) -> Result<String, anyhow::Error> {
    let mut seen: HashMap<&str, &TypegenConstValue> = HashMap::new();
    let mut registrations = Vec::new();
    for registration in &catalog.constants {
        if let Some(existing) = seen.get(registration.name) {
            if !registration.value.eq(existing) {
                anyhow::bail!(
                    "Conflicting typegen constant values for '{}': {:?} vs {:?}",
                    registration.name,
                    existing,
                    registration.value
                );
            }
        } else {
            seen.insert(registration.name, &registration.value);
            registrations.push(*registration);
        }
    }
    Ok(registrations
        .iter()
        .map(|registration| {
            let value = match &registration.value {
                TypegenConstValue::Str(value) => format!("'{value}'"),
                TypegenConstValue::Int(value) => value.to_string(),
                TypegenConstValue::Float(value) => value.to_string(),
                TypegenConstValue::Bool(value) => value.to_string(),
            };
            format!("export const {} = {} as const", registration.name, value)
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

fn generate_message_events() -> String {
    let entries = inventory::iter::<MessageEventRegistration>()
        .map(|registration| {
            format!(
                "  {}: '{}',",
                registration.variant_name, registration.event_value
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r"export const MykoEvent = {{
{entries}
}} as const;
export type MykoEventType = typeof MykoEvent[keyof typeof MykoEvent];"
    )
}

/// Generate TypeScript bindings for registrations owned by the current crate.
///
/// # Errors
///
/// Returns an error when the crate name is unavailable or generation fails.
pub fn generate_item_types(directory_path: &str) -> Result<(), anyhow::Error> {
    let crate_name = std::env::var("CARGO_PKG_NAME")
        .context("CARGO_PKG_NAME environment variable not found")?
        .replace('-', "_");
    println!("The current crate name is: {crate_name}");
    generate_item_types_for_catalog(directory_path, &TypegenCatalog::collect(&crate_name))
}

/// Generate TypeScript bindings for an explicitly collected catalog.
///
/// Aggregate typegen binaries can build the catalog with
/// [`TypegenCatalog::collect_crates`] or [`TypegenCatalog::collect_crate_family`].
///
/// # Errors
///
/// Returns an error when bindings cannot be generated or written.
pub fn generate_item_types_for_catalog(
    directory_path: &str,
    catalog: &TypegenCatalog,
) -> Result<(), anyhow::Error> {
    let file_name = "index.ts";

    // Wipe before regenerating: collect_binding_types/collect_subdir_types
    // (below) rebuild index.ts's barrel by SCANNING this directory's .ts
    // files, not from the current type registry — a type that gets
    // renamed or deleted (e.g. myko 5.0's PartialX -> XQuery) leaves its
    // old file behind, and that stale file gets perpetually re-exported by
    // every future run until physically removed. Every file this function
    // writes is machine-generated (confirmed: every .ts file under
    // directory_path carries the ts-rs "generated by" header, or is
    // index.ts itself, freshly rewritten below) — safe to remove wholesale.
    if Path::new(directory_path).exists() {
        fs::remove_dir_all(directory_path)?;
    }
    fs::create_dir_all(directory_path)?;

    // The renderer owns its backend configuration; callers only provide the
    // language-neutral output directory. Type generation runs single-threaded
    // before any application initialization, so updating this process-global
    // backend setting cannot race another exporter.
    // SAFETY: this function is invoked by the single-threaded typegen binary.
    unsafe { std::env::set_var("TS_RS_EXPORT_DIR", directory_path) };

    println!("Exporting registered generated binding types...");
    export_registered_ts_types_for_catalog(catalog)?;

    // Additional typegen modules are rendered before scanning bindings so their
    // generated barrels participate in the root index.
    typegen_typescript::export_registered_typegen_modules(
        Path::new(directory_path),
        &catalog.modules,
    )?;

    let (binding_exports, subdir_exports, entity_imports, aliased_imports) =
        generate_import_sections(directory_path, catalog);
    let [
        query_classes,
        view_classes,
        report_classes,
        command_classes,
        item_ctor_obj,
    ] = generate_class_sections(catalog);
    let const_exports = generate_const_exports(catalog)?;
    let message_events = generate_message_events();

    let code = [
        "// Auto-generated by type_gen - do not edit manually".to_string(),
        String::new(),
        "// Core type aliases".to_string(),
        "/** Entity identifier type. In Rust this is Arc<str>, serialized as string. */"
            .to_string(),
        "export type ID = string;".to_string(),
        String::new(),
        "// Re-export ts-rs generated types".to_string(),
        binding_exports,
        subdir_exports,
        String::new(),
        "// Internal imports".to_string(),
        entity_imports,
        aliased_imports,
        String::new(),
        "// Query classes".to_string(),
        query_classes,
        String::new(),
        "// View classes".to_string(),
        view_classes,
        String::new(),
        "// Report classes".to_string(),
        report_classes,
        String::new(),
        "// Command classes".to_string(),
        command_classes,
        String::new(),
        "// Item constructors".to_string(),
        item_ctor_obj,
        String::new(),
        "// Message events".to_string(),
        message_events,
        String::new(),
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

    let Some(code) = code else {
        anyhow::bail!("Generated code is empty");
    };

    fs::write(&file_path, code)?;
    println!("Successfully wrote to file: {}", file_path.display());

    Ok(())
}

/// Generate docs JSON entries from ts-rs binding files.
///
/// This is intentionally separate from `generate_item_types` so callers can
/// run docs generation independently (or in addition to TS type generation).
///
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
        for (prop_name, prop_type, doc_string, _optional) in parse_object_type_fields(&body) {
            // `id`/`hash` are auto-added by `#[myko_item]` on every entity;
            // they're not meaningful documentation targets, so docgen omits
            // them here. Operation-argument structs (which legitimately use
            // `id` as their one real field, e.g. `DeleteServerArgs`) go
            // through `operation_index::build_operation_index` instead,
            // which does not apply this filter.
            if prop_name == "id" || prop_name == "hash" {
                continue;
            }
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
    let ts_output_type = crate::operation_index::rust_type_to_ts(output_type);
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
        crate::operation_index::rust_type_to_ts(result_type)
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

fn generate_item_constructor(item_name: &str) -> String {
    format!("  {item_name}: (args: {item_name}) => ({{ item: args, itemType: \"{item_name}\" }})")
}

fn extract_importable_types(rust_type: &str) -> Vec<String> {
    use crate::operation_index::{outer_leaf, split_generic_args, split_outer_generic};

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::query::{IdFilter, StringFilter};

    #[allow(dead_code)]
    #[derive(crate::TS)]
    struct AggregateDownstreamQuery {
        id: Option<IdFilter<Arc<str>>>,
        name: Option<StringFilter>,
    }

    const AGGREGATE_QUERY_TYPE_ID: &str = "downstream_entities::AggregateDownstreamQuery";

    inventory::submit! {
        crate::codegen_types::TypegenTypeRegistration {
            id: AGGREGATE_QUERY_TYPE_ID,
            type_name: "AggregateDownstreamQuery",
            crate_path: "downstream_entities::query",
        }
    }

    inventory::submit! {
        crate::typegen_typescript::TypeExportRegistration {
            type_id: AGGREGATE_QUERY_TYPE_ID,
            type_name: "AggregateDownstreamQuery",
            export_fn: || <AggregateDownstreamQuery as crate::TS>::export(
                &crate::ts_rs::Config::from_env()
            ),
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn adapter_export_ok() -> Result<(), ts_rs::ExportError> {
        Ok(())
    }

    #[test]
    fn typescript_adapters_are_selected_from_the_neutral_catalog() {
        static OWN_TYPE: crate::codegen_types::TypegenTypeRegistration =
            crate::codegen_types::TypegenTypeRegistration {
                id: "rship::Own",
                type_name: "Own",
                crate_path: "rship",
            };
        static OWN_ADAPTER: TypeExportRegistration = TypeExportRegistration {
            type_id: "rship::Own",
            type_name: "Own",
            export_fn: adapter_export_ok,
        };
        static FOREIGN_ADAPTER: TypeExportRegistration = TypeExportRegistration {
            type_id: "rship_core::Foreign",
            type_name: "Foreign",
            export_fn: adapter_export_ok,
        };

        let catalog = TypegenCatalog {
            types: vec![&OWN_TYPE],
            constants: Vec::new(),
            modules: Vec::new(),
            items: Vec::new(),
            queries: Vec::new(),
            views: Vec::new(),
            reports: Vec::new(),
            commands: Vec::new(),
        };
        let selected = typescript_adapters_for_catalog(&catalog, [&OWN_ADAPTER, &FOREIGN_ADAPTER]);

        assert_eq!(selected.len(), 1);
        assert_eq!(
            selected.first().map(|adapter| adapter.type_name),
            Some("Own")
        );
    }

    /// A process- and call-unique scratch directory under the system temp
    /// dir. Each codegen test gets its own, so `generate_item_types`' wipe +
    /// regenerate can never race another test or a parallel `cargo flux`
    /// process. The tests used to share a fixed `./bindings` path, which
    /// raced across processes under `cargo flux run test` and failed with
    /// `ENOTEMPTY` ("Directory not empty") mid-wipe.
    fn typegen_test_serial() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn unique_bindings_dir(label: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "myko-codegen-test-{}-{label}-{n}",
            std::process::id()
        ))
    }

    #[test]
    fn aggregate_catalog_exports_framework_filter_dependencies() {
        let _serial = typegen_test_serial();
        let dir = unique_bindings_dir("aggregate-framework-types");
        let Some(dir_str) = dir.to_str() else {
            panic!("temp dir path is valid UTF-8");
        };
        let catalog = TypegenCatalog::collect_crate_family("downstream_entities")
            .merge(TypegenCatalog::collect_framework_types());

        assert!(generate_item_types_for_catalog(dir_str, &catalog).is_ok());
        assert!(dir.join("AggregateDownstreamQuery.ts").exists());
        assert!(dir.join("IdFilter.ts").exists());
        assert!(dir.join("StringFilter.ts").exists());
        let query = fs::read_to_string(dir.join("AggregateDownstreamQuery.ts"))
            .expect("aggregate query binding should be readable");
        assert!(query.contains("./IdFilter"));
        assert!(query.contains("./StringFilter"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn generate_index() {
        let _serial = typegen_test_serial();
        let dir = unique_bindings_dir("generate_index");
        let dir_str = dir.to_str();
        assert!(dir_str.is_some(), "temp dir path is valid UTF-8");
        let Some(dir_str) = dir_str else {
            return;
        };
        let catalog = TypegenCatalog::collect(env!("CARGO_CRATE_NAME"));
        assert!(generate_item_types_for_catalog(dir_str, &catalog).is_ok());
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test for the myko 5.0 stale-generated-file bug: a type
    /// renamed or deleted on the Rust side (e.g. `PartialX` -> `XQuery`) used
    /// to leave its old .ts file behind, and `collect_binding_types` picked
    /// it up from the directory listing and kept re-exporting it from
    /// index.ts forever, since nothing ever cleared the directory first.
    #[test]
    fn generate_item_types_removes_stale_files_from_a_previous_run() {
        let _serial = typegen_test_serial();
        let dir = unique_bindings_dir("stale_files");
        let dir_str = dir.to_str();
        assert!(dir_str.is_some(), "temp dir path is valid UTF-8");
        let Some(dir_str) = dir_str else {
            return;
        };
        assert!(fs::create_dir_all(&dir).is_ok());
        let stale_path = dir.join("StaleTestArtifact.ts");
        assert!(
            fs::write(
                &stale_path,
                "// This file was generated by [ts-rs]\nexport type StaleTestArtifact = string;\n",
            )
            .is_ok()
        );
        assert!(stale_path.exists(), "precondition: stale file exists");

        assert!(generate_item_types(dir_str).is_ok());

        assert!(
            !stale_path.exists(),
            "generate_item_types must wipe stale files from a previous run, \
             not just add/overwrite current ones"
        );
        let index_contents = fs::read_to_string(dir.join("index.ts"));
        assert!(index_contents.is_ok(), "index.ts must exist");
        let Ok(index_contents) = index_contents else {
            return;
        };
        assert!(
            !index_contents.contains("StaleTestArtifact"),
            "index.ts must not re-export a type whose file no longer exists"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
