//! Generate setter commands from field attributes.
//!
//! Supports:
//! - `#[myko_rename]` on name fields - generates `Rename{Entity} { id, name }`
//! - `#[myko_setter]` on any field - generates `Set{Entity}{Field} { id, field }`
//! - `#[myko_setter("CustomName")]` - generates `CustomName { id, field }` with custom command name

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Field, ItemStruct, Lit, Meta};

/// Information about a field that needs a setter command
pub struct SetterField {
    pub field_name: syn::Ident,
    pub field_type: syn::Type,
    pub is_rename: bool, // true for #[myko_rename], false for #[myko_setter]
    pub command_name_override: Option<String>, // Optional custom command name
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
                    // Check for optional name override: #[myko_rename("CustomName")]
                    let command_name_override = parse_string_arg(attr);
                    setters.push(SetterField {
                        field_name,
                        field_type,
                        is_rename: true,
                        command_name_override,
                    });
                    break;
                } else if attr.path().is_ident("myko_setter") {
                    // Check for optional name override: #[myko_setter("CustomName")]
                    let command_name_override = parse_string_arg(attr);
                    setters.push(SetterField {
                        field_name,
                        field_type,
                        is_rename: false,
                        command_name_override,
                    });
                    break;
                }
            }
        }
    }

    setters
}

/// Parse optional string argument from attribute: #[attr("value")]
fn parse_string_arg(attr: &syn::Attribute) -> Option<String> {
    if let Meta::List(meta_list) = &attr.meta
        && let Ok(Lit::Str(lit_str)) = meta_list.parse_args::<Lit>()
    {
        return Some(lit_str.value());
    }
    None
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
    let id_type_ident = format_ident!("{}Id", entity_name);
    let get_by_id_ident = format_ident!("Get{}ById", entity_name);
    let krate = crate::myko_path();

    let commands: Vec<TokenStream> = setters
    .iter()
    .map(|setter| {
      let field_name = &setter.field_name;
      let field_type = &setter.field_type;

      // Generate command name (with optional override)
      let (command_name, param_name) = if setter.is_rename {
        // #[myko_rename] or #[myko_rename("CustomName")]
        let cmd_name = setter
          .command_name_override
          .as_ref()
          .map(|s| format_ident!("{}", s))
          .unwrap_or_else(|| format_ident!("Rename{}", entity_name));
        (cmd_name, format_ident!("name"))
      } else {
        // #[myko_setter] or #[myko_setter("CustomName")]
        let cmd_name = setter
          .command_name_override
          .as_ref()
          .map(|s| format_ident!("{}", s))
          .unwrap_or_else(|| {
            let field_pascal = to_pascal_case(&field_name.to_string());
            format_ident!("Set{}{}", entity_name, field_pascal)
          });
        (cmd_name, field_name.clone())
      };

      // For rename, the param is always "name" but field might be different
      let field_assignment = if setter.is_rename {
        quote! { #field_name: self.name.to_string() }
      } else {
        // Handle the field type - if it's String, convert from Arc<str>
        let type_str = quote!(#field_type).to_string();
        if type_str.contains("Vec") && type_str.contains("String") {
          // Vec<String> - convert from Vec<Arc<str>>
          quote! { #field_name: self.#param_name.iter().map(|s| s.to_string()).collect() }
        } else if type_str.contains("String") && !type_str.contains("Option") {
          quote! { #field_name: self.#param_name.to_string() }
        } else if type_str.contains("Option") && type_str.contains("String") {
          quote! { #field_name: self.#param_name.as_ref().map(|s| s.to_string()) }
        } else {
          quote! { #field_name: self.#param_name.clone() }
        }
      };

      // Determine the command param type
      // For String fields, use Arc<str> in the command for efficiency
      let param_type = {
        let type_str = quote!(#field_type).to_string();
        if type_str.contains("Vec < String >") || type_str.contains("Vec<String>") {
          quote! { Vec<std::sync::Arc<str>> }
        } else if type_str == "String" {
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
          #[#krate::myko_command]
          pub struct #command_name {
              pub id: #id_type_ident,
              #param_field,
          }

          impl #krate::command::CommandHandler for #command_name {
              fn execute(
                  self,
                  ctx: #krate::prelude::CommandContext,
              ) -> Result<(), #krate::prelude::CommandError> {
                  let report = #get_by_id_ident { id: self.id.clone() };
                  let entity = ctx.exec_report(report)?.ok_or_else(|| {
                      #krate::prelude::CommandError {
                          tx: ctx.tx().to_string(),
                          command_id: ctx.command_id.to_string(),
                          message: format!("{} {} not found", stringify!(#entity_ident), self.id),
                      }
                  })?;

                  let updated = #entity_ident {
                      #field_assignment,
                      ..entity.as_ref().clone()
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
