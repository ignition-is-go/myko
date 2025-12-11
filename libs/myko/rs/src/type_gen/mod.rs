use std::{collections::HashSet, fs, path::Path};

use dprint_plugin_typescript::{
    FormatTextOptions,
    configuration::{ConfigurationBuilder, TrailingCommas},
};

use crate::{command::CommandRegistration, item::ItemRegistration, message::MessageEventRegistration, query::QueryRegistration, report::ReportRegistration};

/// Collect all .ts files in the bindings directory (excluding index.ts, .d.ts files, and subdirectories)
fn collect_binding_types(directory_path: &str) -> Vec<String> {
    let mut types = Vec::new();

    if let Ok(entries) = fs::read_dir(directory_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            let filename = path.file_name().map(|n| n.to_string_lossy().to_string());
            if path.is_file()
                && path.extension().map(|e| e == "ts").unwrap_or(false)
                && let Some(ref fname) = filename
                && !fname.ends_with(".d.ts") // Exclude .d.ts declaration files
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

/// Collect types from subdirectories (e.g., serde_json/JsonValue), excluding .d.ts files
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
                            && !fname.ends_with(".d.ts") // Exclude .d.ts declaration files
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
    let directory_path = "bindings"; // Specify the target directory
    let file_name = "index.ts";

    // Create the directory if it doesn't exist
    fs::create_dir_all(directory_path)?;

    let crate_name = std::env::var("CARGO_PKG_NAME")
        .expect("CARGO_PKG_NAME environment variable not found")
        .replace("-", "_");
    println!("The current crate name is: {}", crate_name);

    // Collect all ts-rs generated types
    let binding_types = collect_binding_types(directory_path);
    let subdir_types = collect_subdir_types(directory_path);

    // Generate re-exports for ts-rs types
    let binding_exports = binding_types
        .iter()
        .map(|name| format!("export type {{ {} }} from \"./{}\";", name, name))
        .collect::<Vec<String>>()
        .join("\n");

    // Generate re-exports for subdirectory types
    let subdir_exports = subdir_types
        .iter()
        .map(|(subdir, name)| format!("export type {{ {} }} from \"./{}/{}\";", name, subdir, name))
        .collect::<Vec<String>>()
        .join("\n");

    let query_return_type = "export type QueryReturn<U> = { query: Record<string, unknown>; queryId: string; queryItemType: string; $res?: () => U[] }".to_string();

    let report_return_type = "export type ReportReturn<U> = { report: Record<string, unknown>; reportId: string; $res?: () => U }".to_string();

    let command_return_type = "export type CommandReturn<U> = { command: Record<string, unknown>; commandId: string; $res?: () => U }".to_string();

    let items =
        inventory::iter::<ItemRegistration>().filter(|x| x.crate_name.contains(&crate_name));

    let queries =
        inventory::iter::<QueryRegistration>().filter(|x| x.crate_name.contains(&crate_name));

    let reports =
        inventory::iter::<ReportRegistration>().filter(|x| x.crate_name.contains(&crate_name));

    let commands =
        inventory::iter::<CommandRegistration>().filter(|x| x.crate_name.contains(&crate_name));

    let item_imports = items
        .clone()
        .map(|i| gen_import(i.entity_type))
        .collect::<Vec<String>>()
        .join(";");

    let query_imports = queries
        .clone()
        .map(|i| gen_import(i.query_id))
        .collect::<Vec<String>>()
        .join(";");

    let report_imports = reports
        .clone()
        .map(|i| gen_import(i.report_id))
        .collect::<Vec<String>>()
        .join(";");

    let command_imports = commands
        .clone()
        .map(|i| gen_import(i.command_id))
        .collect::<Vec<String>>()
        .join(";");

    // Import output types for reports, filtered by crate and deduplicated
    let report_output_imports = reports
        .clone()
        .filter(|r| r.output_type_crate.contains(&crate_name))
        .map(|i| i.output_type)
        .collect::<HashSet<_>>()
        .into_iter()
        .map(gen_type_import)
        .collect::<Vec<String>>()
        .join(";");

    // Import result types for commands, filtered by crate and deduplicated
    let command_result_imports = commands
        .clone()
        .filter(|c| c.result_type_crate.contains(&crate_name) && c.result_type != "()")
        .map(|i| i.result_type)
        .collect::<HashSet<_>>()
        .into_iter()
        .map(gen_type_import)
        .collect::<Vec<String>>()
        .join(";");

    let item_ctors = items
        .clone()
        .map(|i| generate_item_constructor(i.entity_type))
        .collect::<Vec<String>>()
        .join(",\n");

    let query_ctors = queries
        .clone()
        .map(|i| generate_query_constructor(i.query_id, i.query_item_type))
        .collect::<Vec<String>>()
        .join(",\n");

    let report_ctors = reports
        .clone()
        .map(|i| generate_report_constructor(i.report_id, i.output_type))
        .collect::<Vec<String>>()
        .join(",\n");

    let command_ctors = commands
        .clone()
        .map(|i| generate_command_constructor(i.command_id, i.result_type))
        .collect::<Vec<String>>()
        .join(",\n");

    let item_ctor_obj = ["export const items = {", &item_ctors, "}"].join("");

    let query_ctor_obj = ["export const queries = {", &query_ctors, "}"].join("");

    let report_ctor_obj = ["export const reports = {", &report_ctors, "}"].join("");

    let command_ctor_obj = ["export const commands = {", &command_ctors, "}"].join("");

    // Message event constants - generated from inventory
    let message_event_entries = inventory::iter::<MessageEventRegistration>()
        .map(|r| format!("  {}: '{}',", r.variant_name, r.event_value))
        .collect::<Vec<String>>()
        .join("\n");

    let message_events = format!(
        r#"
// Message event constants
export const MykoEvent = {{
{}
}} as const;
export type MykoEventType = typeof MykoEvent[keyof typeof MykoEvent];
"#,
        message_event_entries
    );

    let code = [
        "// Auto-generated by type_gen - do not edit manually".to_string(),
        "".to_string(),
        "// Re-export all ts-rs generated types".to_string(),
        binding_exports,
        subdir_exports,
        "".to_string(),
        "// Entity, query, report, and command utilities".to_string(),
        item_imports,
        query_imports,
        report_imports,
        command_imports,
        report_output_imports,
        command_result_imports,
        query_return_type,
        report_return_type,
        command_return_type,
        item_ctor_obj,
        query_ctor_obj,
        report_ctor_obj,
        command_ctor_obj,
        message_events.to_string(),
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

    let code = code.unwrap();

    fs::write(&file_path, &code)?;

    println!("Successfully wrote to file: {}", file_path.display());

    Ok(())
}

fn generate_query_constructor(query_id: &str, query_item_type: &str) -> String {
    format!(
        "{}: {}\n",
        query_id,
        func_obj(
            &format!("args: Omit<{}, 'tx' | 'createdAt'>", query_id),
            &format!(
                "({}) as unknown as QueryReturn<{}[]>",
                scope(
                    &[
                        kv("query", "args"),
                        kv("queryId", &format!("\"{}\"", query_id)),
                        kv("queryItemType", &format!("\"{}\"", query_item_type))
                    ]
                    .join(",")
                ),
                query_item_type
            )
        )
    )
}

fn generate_report_constructor(report_id: &str, output_type: &str) -> String {
    format!(
        "{}: {}\n",
        report_id,
        func_obj(
            &format!("args: Omit<{}, 'tx'>", report_id),
            &format!(
                "({}) as unknown as ReportReturn<{}>",
                scope(
                    &[
                        kv("report", "args"),
                        kv("reportId", &format!("\"{}\"", report_id)),
                    ]
                    .join(",")
                ),
                output_type
            )
        )
    )
}

fn generate_command_constructor(command_id: &str, result_type: &str) -> String {
    // Handle () result type by using void in TypeScript
    let ts_result_type = if result_type == "()" {
        "void"
    } else {
        result_type
    };

    format!(
        "{}: {}\n",
        command_id,
        func_obj(
            &format!("args: Omit<{}, 'tx' | 'createdAt'>", command_id),
            &format!(
                "({}) as unknown as CommandReturn<{}>",
                scope(
                    &[
                        kv("command", "args"),
                        kv("commandId", &format!("\"{}\"", command_id)),
                    ]
                    .join(",")
                ),
                ts_result_type
            )
        )
    )
}

fn generate_item_constructor(item_name: &str) -> String {
    format!(
        "{}: {}\n",
        item_name,
        func_obj(
            &format!("args: Omit<{}, 'hash'>", item_name),
            &format!(
                "({})",
                scope(
                    &[
                        kv("item", "args"),
                        kv("itemType", &format!("\"{}\"", item_name))
                    ]
                    .join(",")
                )
            )
        )
    )
}

fn gen_import(item_name: &str) -> String {
    format!("import type {{ {} }} from './{}';", item_name, item_name)
}

fn gen_type_import(type_name: &str) -> String {
    format!("import type {{ {} }} from './{}';", type_name, type_name)
}

fn scope(body: &str) -> String {
    format!("{}{}{}", "{", body, "}")
}

fn kv(key: &str, value: &str) -> String {
    format!("{}: {}", key, value)
}

fn func_obj(args: &str, body: &str) -> String {
    format!("({}) => ({})", args, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_index() {
        generate_item_types().expect("Failed to generate index.ts");
    }
}
