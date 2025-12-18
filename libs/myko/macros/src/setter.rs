//! Generate setter commands from field attributes.
//!
//! Supports:
//! - `#[myko_rename]` on name fields - generates `Rename{Entity} { id, name }`
//! - `#[myko_setter]` on any field - generates `Set{Entity}{Field} { id, field }`

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Field, ItemStruct};

/// Information about a field that needs a setter command
pub struct SetterField {
    pub field_name: syn::Ident,
    pub field_type: syn::Type,
    pub is_rename: bool, // true for #[myko_rename], false for #[myko_setter]
}

/// Collect fields marked with #[myko_rename] or #[myko_setter]
pub fn collect_setter_fields(input: &ItemStruct) -> Vec<SetterField> {
    let mut setters = Vec::new();

    if let syn::Fields::Named(fields) = &input.fields {
        for field in &fields.named {
            let field_name = field.ident.clone().unwrap();
            let field_type = field.ty.clone();

            for attr in &field.attrs {
                if attr.path().is_ident("myko_rename") {
                    setters.push(SetterField {
                        field_name,
                        field_type,
                        is_rename: true,
                    });
                    break;
                } else if attr.path().is_ident("myko_setter") {
                    setters.push(SetterField {
                        field_name,
                        field_type,
                        is_rename: false,
                    });
                    break;
                }
            }
        }
    }

    setters
}

/// Strip #[myko_rename] and #[myko_setter] attributes from a field
pub fn strip_setter_attrs(field: &mut Field) {
    field.attrs.retain(|attr| {
        !attr.path().is_ident("myko_rename") && !attr.path().is_ident("myko_setter")
    });
}

/// Convert snake_case to PascalCase
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().chain(chars).collect(),
                None => String::new(),
            }
        })
        .collect()
}

/// Generate setter commands for all annotated fields
pub fn generate_setter_commands(entity_name: &str, setters: &[SetterField]) -> TokenStream {
    let entity_ident = format_ident!("{}", entity_name);
    let get_by_ids_ident = format_ident!("Get{}sByIds", entity_name);

    let commands: Vec<TokenStream> = setters
        .iter()
        .map(|setter| {
            let field_name = &setter.field_name;
            let field_type = &setter.field_type;

            // Generate command name
            let (command_name, handler_name, param_name) = if setter.is_rename {
                // #[myko_rename] generates Rename{Entity}
                (
                    format_ident!("Rename{}", entity_name),
                    format_ident!("Rename{}Handler", entity_name),
                    format_ident!("name"),
                )
            } else {
                // #[myko_setter] generates Set{Entity}{Field}
                let field_pascal = to_pascal_case(&field_name.to_string());
                (
                    format_ident!("Set{}{}", entity_name, field_pascal),
                    format_ident!("Set{}{}Handler", entity_name, field_pascal),
                    field_name.clone(),
                )
            };

            // For rename, the param is always "name" but field might be different
            let field_assignment = if setter.is_rename {
                quote! { #field_name: cmd.name.to_string() }
            } else {
                // Handle the field type - if it's String, convert from Arc<str>
                let type_str = quote!(#field_type).to_string();
                if type_str.contains("String") && !type_str.contains("Option") {
                    quote! { #field_name: cmd.#param_name.to_string() }
                } else if type_str.contains("Option") && type_str.contains("String") {
                    quote! { #field_name: cmd.#param_name.map(|s| s.to_string()) }
                } else {
                    quote! { #field_name: cmd.#param_name.clone() }
                }
            };

            // Determine the command param type
            // For String fields, use Arc<str> in the command for efficiency
            let param_type = {
                let type_str = quote!(#field_type).to_string();
                if type_str == "String" {
                    quote! { std::sync::Arc<str> }
                } else if type_str.contains("Option < String >") || type_str.contains("Option<String>") {
                    quote! { Option<std::sync::Arc<str>> }
                } else {
                    quote! { #field_type }
                }
            };

            // For rename commands, param is always "name"
            let param_field = if setter.is_rename {
                quote! { pub name: std::sync::Arc<str> }
            } else {
                quote! { pub #param_name: #param_type }
            };

            quote! {
                /// Auto-generated setter command
                #[myko_macros::myko_command]
                pub struct #command_name {
                    pub id: std::sync::Arc<str>,
                    #param_field,
                }

                impl #handler_name {
                    pub async fn execute(
                        cmd: #command_name,
                        ctx: myko_rs::prelude::CommandContext,
                    ) -> Result<(), myko_rs::prelude::CommandError> {
                        let query = #get_by_ids_ident { ids: vec![cmd.id.clone()] };
                        let entity = ctx.query_one(&query).await?.ok_or_else(|| {
                            myko_rs::prelude::CommandError {
                                tx: ctx.tx().to_string(),
                                message: format!("{} {} not found", stringify!(#entity_ident), cmd.id),
                            }
                        })?;

                        let updated = #entity_ident {
                            #field_assignment,
                            ..entity
                        };

                        ctx.emit_set(&updated)?;
                        Ok(())
                    }
                }
            }
        })
        .collect();

    quote! {
        #(#commands)*
    }
}
