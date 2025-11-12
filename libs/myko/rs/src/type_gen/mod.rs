use std::{fs, path::Path};

use dprint_plugin_typescript::{
    FormatTextOptions,
    configuration::{ConfigurationBuilder, TrailingCommas},
};

use crate::{item::ItemRegistration, query::QueryRegistration};

pub fn generate_item_types() -> Result<(), anyhow::Error> {
    let directory_path = "bindings"; // Specify the target directory
    let file_name = "index.ts";

    // Create the directory if it doesn't exist
    fs::create_dir_all(directory_path)?;

    let crate_name = std::env::var("CARGO_PKG_NAME")
        .expect("CARGO_PKG_NAME environment variable not found")
        .replace("-", "_");
    println!("The current crate name is: {}", crate_name);

    let query_return_type = "type QueryReturn<U> = { $res: () => U[]}".to_string();

    let items =
        inventory::iter::<ItemRegistration>().filter(|x| x.crate_name.contains(&crate_name));

    let queries =
        inventory::iter::<QueryRegistration>().filter(|x| x.crate_name.contains(&crate_name));

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

    let item_ctor_obj = ["export const items = {", &item_ctors, "}"].join("");

    let query_ctor_obj = ["export const queries = {", &query_ctors, "}"].join("");

    let code = [
        item_imports,
        query_imports,
        query_return_type,
        item_ctor_obj,
        query_ctor_obj,
    ]
    .join(";");

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

    fs::write(&file_path, code)?;

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
    format!("import {{ {} }} from './{}';", item_name, item_name)
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
