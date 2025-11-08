use std::{fs, path::Path};

use crate::item::ItemRegistration;

pub fn generate_item_types() -> Result<(), std::io::Error> {
    let directory_path = "bindings"; // Specify the target directory
    let file_name = "index.ts";

    // Create the directory if it doesn't exist
    fs::create_dir_all(directory_path)?;

    let mut code = String::new();

    let crate_name = std::env::var("CARGO_PKG_NAME")
        .expect("CARGO_PKG_NAME environment variable not found")
        .replace("-", "_");
    println!("The current crate name is: {}", crate_name);

    let items =
        inventory::iter::<ItemRegistration>().filter(|x| x.crate_name.contains(&crate_name));
    
    let imports = items.clone().iter().map(|i| gen_import(s, item_name))

    for item in items.clone() {
        gen_import(&mut code, item.entity_type);
    }

    let ctors = items
        .clone()
        .map(|i| generate_item_constructor(i.entity_type))
        .collect::<Vec<String>>()
        .join(",\n");

    let ctor_obj = ["export const items = {", &ctors, "}"].join("");

    code.push_str(&ctor_obj);

    // Construct the full file path
    let file_path = Path::new(directory_path).join(file_name);

    // Write the contents to the file
    fs::write(&file_path, code)?;

    println!("Successfully wrote to file: {}", file_path.display());

    Ok(())
}

fn generate_item_constructor(item_name: &str) -> String {
    format!(
        "{}: {}\n",
        item_name,
        func_obj(
            &format!("args: {}", item_name),
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

fn gen_import(s: &mut String, item_name: &str) {
    s.push_str("import { ");
    s.push_str(item_name);
    s.push_str(" } from './");
    // s.push_str(&scope(&format!("itemType: \"{}\",\n", item_name)));
    s.push_str(item_name);
    s.push_str("';\n");
}

fn scope(body: &str) -> String {
    format!("{}{}{}", "{", body, "}")
}

fn kv(key: &str, value: &str) -> String {
    format!("{}: {}\n", key, value)
}

fn func_obj(args: &str, body: &str) -> String {
    format!("({}) => ({})", args, body)
}
