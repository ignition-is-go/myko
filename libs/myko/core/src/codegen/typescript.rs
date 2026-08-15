use std::{
    collections::{BTreeSet, HashMap, HashSet},
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
    graph::{EndpointRequirement, GraphSchemaCatalog},
    operation_index::{
        collect_ts_binding_files, extract_exported_object_type_body, parse_object_type_fields,
    },
    typegen_typescript::TypeExportRegistration,
    wire::MessageEventRegistration,
};

fn ts_literal<T: serde::Serialize + ?Sized>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|error| format!("\"<serialization error: {error}>\""))
}

fn endpoint_address_type(requirement: EndpointRequirement, qualified: bool) -> String {
    let entity = match requirement {
        EndpointRequirement::Concrete(entity_type) => format!("__MykoGraph{entity_type}Id"),
        EndpointRequirement::OneOf(_)
        | EndpointRequirement::Category(_)
        | EndpointRequirement::AnyRegisteredItem => "__MykoGraphEntityRef".to_string(),
    };
    if qualified {
        format!("{{ entity: {entity}; qualifier: unknown }}")
    } else {
        entity
    }
}

fn endpoint_requirement_literal(requirement: EndpointRequirement) -> String {
    match requirement {
        EndpointRequirement::Concrete(entity_type) => format!(
            "{{ kind: \"concrete\", entityType: {} }}",
            ts_literal(entity_type)
        ),
        EndpointRequirement::OneOf(entity_types) => format!(
            "{{ kind: \"oneOf\", entityTypes: {} }}",
            ts_literal(entity_types)
        ),
        EndpointRequirement::Category(category) => format!(
            "{{ kind: \"category\", category: {} }}",
            ts_literal(category)
        ),
        EndpointRequirement::AnyRegisteredItem => "{ kind: \"anyRegisteredItem\" }".to_string(),
    }
}

fn generate_graph_query_helpers(edge: &crate::graph::EdgeRegistration) -> String {
    let a = &edge.endpoints[0];
    let b = &edge.endpoints[1];
    let a_type = endpoint_address_type((a.requirement)(), (a.qualifier_type)().is_some());
    let b_type = endpoint_address_type((b.requirement)(), (b.qualifier_type)().is_some());
    format!(
        r#"export type {edge}AAddress = {a_type};
export type {edge}BAddress = {b_type};
export class {edge}GraphFrom {{
  static readonly queryId = "{edge}GraphFrom" as const;
  static readonly queryItemType = "{edge}" as const;
  readonly queryId = "{edge}GraphFrom" as const;
  readonly queryItemType = "{edge}" as const;
  readonly query: {{ endpoint: {edge}AAddress }};
  declare readonly $res: () => {edge}[];
  constructor(endpoint: {edge}AAddress) {{ this.query = {{ endpoint }}; }}
}}
export class {edge}GraphTo {{
  static readonly queryId = "{edge}GraphTo" as const;
  static readonly queryItemType = "{edge}" as const;
  readonly queryId = "{edge}GraphTo" as const;
  readonly queryItemType = "{edge}" as const;
  readonly query: {{ endpoint: {edge}BAddress }};
  declare readonly $res: () => {edge}[];
  constructor(endpoint: {edge}BAddress) {{ this.query = {{ endpoint }}; }}
}}
export class {edge}GraphBetween {{
  static readonly queryId = "{edge}GraphBetween" as const;
  static readonly queryItemType = "{edge}" as const;
  readonly queryId = "{edge}GraphBetween" as const;
  readonly queryItemType = "{edge}" as const;
  readonly query: {{ a: {edge}AAddress; b: {edge}BAddress }};
  declare readonly $res: () => {edge}[];
  constructor(a: {edge}AAddress, b: {edge}BAddress) {{ this.query = {{ a, b }}; }}
}}
export const {edge}Graph = {{
  from: (endpoint: {edge}AAddress) => new {edge}GraphFrom(endpoint),
  to: (endpoint: {edge}BAddress) => new {edge}GraphTo(endpoint),
  between: (a: {edge}AAddress, b: {edge}BAddress) => new {edge}GraphBetween(a, b),
  connect: (edge: {edge}) => new Connect{edge}({{ edge }}),
  connectMany: (edges: {edge}[]) => new Connect{edge}s({{ edges }}),
  ensure: (edge: {edge}) => new Ensure{edge}({{ edge }}),
  disconnect: (id: __MykoGraph{edge}Id) => new Delete{edge}({{ id }}),
  disconnectMany: (ids: __MykoGraph{edge}Id[]) => new Delete{edge}s({{ ids }}),
}} as const;"#,
        edge = edge.edge_type,
    )
}

fn generate_graph_schema(catalog: &GraphSchemaCatalog) -> String {
    if catalog.entity_categories.is_empty()
        && catalog.item_categories.is_empty()
        && catalog.edges.is_empty()
    {
        return "export const graphSchema = { categories: {}, memberships: [], edges: {} } as const;\nexport type GraphEdgeType = never;".to_string();
    }

    let endpoint_imports = generate_graph_endpoint_imports(catalog);

    let categories = catalog
        .entity_categories
        .iter()
        .map(|category| {
            format!(
                "{}: {{ id: {}, name: {} }}",
                ts_literal(category.name),
                ts_literal(category.id),
                ts_literal(category.name)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let memberships = catalog
        .item_categories
        .iter()
        .map(|membership| {
            format!(
                "{{ entityType: {}, category: {} }}",
                ts_literal(membership.item_type),
                ts_literal(membership.entity_category_id)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let edges = catalog
        .edges
        .iter()
        .map(|edge| {
            let a = &edge.endpoints[0];
            let b = &edge.endpoints[1];
            let [a_adjacency, b_adjacency] = edge.endpoint_adjacency();
            format!(
                "{}: {{ shape: {}, pairPolicy: {}, pairProjection: {}, adjacency: {}, aAdjacency: {}, bAdjacency: {}, selfLoops: {}, endpoints: {{ a: {{ requirement: {}, qualifierType: {} }}, b: {{ requirement: {}, qualifierType: {} }} }} }}",
                ts_literal(edge.edge_type),
                ts_literal(&edge.shape),
                ts_literal(&edge.pair_policy),
                ts_literal(&edge.pair_projection),
                ts_literal(&edge.adjacency),
                ts_literal(&a_adjacency),
                ts_literal(&b_adjacency),
                ts_literal(&edge.self_loops),
                endpoint_requirement_literal((a.requirement)()),
                ts_literal(&(a.qualifier_type)()),
                endpoint_requirement_literal((b.requirement)()),
                ts_literal(&(b.qualifier_type)()),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let helpers = catalog
        .edges
        .iter()
        .map(|edge| generate_graph_query_helpers(edge))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "{endpoint_imports}\nexport const graphSchema = {{ categories: {{ {categories} }}, memberships: [{memberships}], edges: {{ {edges} }} }} as const;\nexport type GraphEdgeType = keyof typeof graphSchema.edges;\n{helpers}"
    )
}

fn generate_graph_endpoint_imports(catalog: &GraphSchemaCatalog) -> String {
    let concrete_endpoint_types = catalog
        .edges
        .iter()
        .flat_map(|edge| edge.endpoints.iter())
        .filter_map(|endpoint| match (endpoint.requirement)() {
            EndpointRequirement::Concrete(entity_type) => Some(entity_type),
            EndpointRequirement::OneOf(_)
            | EndpointRequirement::Category(_)
            | EndpointRequirement::AnyRegisteredItem => None,
        })
        .collect::<BTreeSet<_>>();
    let uses_entity_ref = catalog
        .edges
        .iter()
        .flat_map(|edge| edge.endpoints.iter())
        .any(|endpoint| {
            matches!(
                (endpoint.requirement)(),
                EndpointRequirement::OneOf(_)
                    | EndpointRequirement::Category(_)
                    | EndpointRequirement::AnyRegisteredItem
            )
        });
    let mut endpoint_imports = concrete_endpoint_types
        .into_iter()
        .map(|entity_type| {
            format!(
                "import type {{ {entity_type}Id as __MykoGraph{entity_type}Id }} from \"./{entity_type}Id\";"
            )
        })
        .collect::<Vec<_>>();
    endpoint_imports.extend(catalog.edges.iter().map(|edge| {
        format!(
            "import type {{ {edge}Id as __MykoGraph{edge}Id }} from \"./{edge}Id\";",
            edge = edge.edge_type,
        )
    }));
    if uses_entity_ref {
        endpoint_imports.push(
            "import type { EntityRef as __MykoGraphEntityRef } from \"./EntityRef\";".to_string(),
        );
    }
    endpoint_imports.join("\n")
}

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
    generate_item_types_for_catalogs(
        directory_path,
        &TypegenCatalog::collect(&crate_name),
        &GraphSchemaCatalog::collect(&crate_name),
    )
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
    let crates = catalog_crate_roots(catalog);
    let graph_catalog = GraphSchemaCatalog::collect_crates(crates);
    generate_item_types_for_catalogs(directory_path, catalog, &graph_catalog)
}

/// Generate TypeScript bindings from separate public type and graph catalogs.
///
/// Keeping these inputs separate preserves the public shape of
/// [`TypegenCatalog`] while allowing aggregate applications to select graph
/// metadata explicitly.
///
/// # Errors
///
/// Returns an error when bindings cannot be generated or written.
pub fn generate_item_types_for_catalogs(
    directory_path: &str,
    catalog: &TypegenCatalog,
    graph_catalog: &GraphSchemaCatalog,
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
    let graph_schema = generate_graph_schema(graph_catalog);

    let code = [
        "// Auto-generated by type_gen - do not edit manually".to_string(),
        String::new(),
        "// Core type aliases".to_string(),
        "/** Entity identifier type. In Rust this is Arc<str>, serialized as string. */"
            .to_string(),
        "export type ID = string;".to_string(),
        String::new(),
        "// Graph schema and typed endpoint helpers".to_string(),
        graph_schema,
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
    struct SchemaRef(String);

    #[allow(dead_code)]
    #[derive(crate::TS)]
    struct ServerId(String);

    #[allow(dead_code)]
    #[derive(crate::TS)]
    struct MEventType(String);

    #[allow(dead_code)]
    #[derive(crate::TS)]
    struct AggregateDownstreamQuery {
        id: Option<IdFilter<Arc<str>>>,
        name: Option<StringFilter>,
        schema: SchemaRef,
        server_id: ServerId,
        event_type: MEventType,
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
            rust_type_id: || std::any::TypeId::of::<AggregateDownstreamQuery>(),
            generated_name: |config| <AggregateDownstreamQuery as crate::TS>::ident(config),
            output_path: || <AggregateDownstreamQuery as crate::TS>::output_path(),
            export_fn: || <AggregateDownstreamQuery as crate::TS>::export_all(
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
            rust_type_id: || std::any::TypeId::of::<u8>(),
            generated_name: |_| "Own".into(),
            output_path: || Some("Own.ts".into()),
            export_fn: adapter_export_ok,
        };
        static FOREIGN_ADAPTER: TypeExportRegistration = TypeExportRegistration {
            type_id: "rship_core::Foreign",
            type_name: "Foreign",
            rust_type_id: || std::any::TypeId::of::<u16>(),
            generated_name: |_| "Foreign".into(),
            output_path: || Some("Foreign.ts".into()),
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
        let dir_string = dir.to_string_lossy();
        let catalog = TypegenCatalog::collect_crate_family("downstream_entities");

        let generated = generate_item_types_for_catalog(&dir_string, &catalog);
        assert!(generated.is_ok(), "aggregate catalog should render");
        let Ok(()) = generated else {
            return;
        };
        assert!(dir.join("AggregateDownstreamQuery.ts").exists());
        assert!(dir.join("IdFilter.ts").exists());
        assert!(dir.join("StringFilter.ts").exists());
        assert!(dir.join("SchemaRef.ts").exists());
        assert!(dir.join("ServerId.ts").exists());
        assert!(dir.join("MEventType.ts").exists());
        let query = fs::read_to_string(dir.join("AggregateDownstreamQuery.ts"));
        assert!(query.is_ok(), "aggregate query binding should be readable");
        let Ok(query) = query else {
            return;
        };
        assert!(query.contains("./IdFilter"));
        assert!(query.contains("./StringFilter"));
        assert!(query.contains("./SchemaRef"));
        assert!(query.contains("./ServerId"));
        assert!(query.contains("./MEventType"));
        let index = fs::read_to_string(dir.join("index.ts"));
        assert!(index.is_ok(), "aggregate index should be readable");
        let Ok(index) = index else {
            return;
        };
        for dependency in [
            "SchemaRef",
            "ServerId",
            "MEventType",
            "IdFilter",
            "StringFilter",
        ] {
            assert!(index.contains(dependency), "index omitted {dependency}");
        }
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

    #[test]
    fn graph_catalog_renders_separately_from_typegen_catalog() {
        let graph = GraphSchemaCatalog::collect(env!("CARGO_CRATE_NAME"));
        let rendered = generate_graph_schema(&graph);
        assert!(rendered.contains("export const graphSchema"));
        assert!(rendered.contains("TagAssignment"));
        assert!(rendered.contains("TagAssignmentAAddress"));
        assert!(rendered.contains("TagId as __MykoGraphTagId"));
        assert!(rendered.contains("EntityRef as __MykoGraphEntityRef"));
        assert!(rendered.contains("export class TagAssignmentGraphFrom"));
        assert!(rendered.contains("queryId = \"TagAssignmentGraphBetween\""));
        assert!(rendered.contains("connect: (edge: TagAssignment) => new ConnectTagAssignment"));
        assert!(rendered.contains("ensure: (edge: TagAssignment) => new EnsureTagAssignment"));
        assert!(
            rendered.contains(
                "disconnect: (id: __MykoGraphTagAssignmentId) => new DeleteTagAssignment"
            )
        );
        assert!(rendered.contains("from: (endpoint: TagAssignmentAAddress)"));
        assert!(rendered.contains("new TagAssignmentGraphFrom(endpoint)"));
        assert!(rendered.contains("pairPolicy"));
        assert!(rendered.contains("aAdjacency"));
        assert!(rendered.contains("bAdjacency"));
        assert!(rendered.contains("category"));
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
