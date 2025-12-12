//! Relationship attribute parsing helpers for myko_item macro

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Field, ItemStruct, Path};

/// Information about a belongs_to relationship on a field
#[derive(Debug)]
pub struct BelongsToInfo {
    /// Field name in Rust (snake_case)
    pub field_name: String,
    /// Field name in JSON (camelCase)
    pub field_name_json: String,
    /// Foreign entity type name
    pub foreign_type: String,
}

/// Information about an owns_many relationship on a field
#[derive(Debug)]
pub struct OwnsManyInfo {
    /// Field name in Rust (snake_case)
    pub field_name: String,
    /// Field name in JSON (camelCase)
    pub field_name_json: String,
    /// Owned entity type name
    pub foreign_type: String,
}

/// Information about an ensure_for relationship on the struct
#[derive(Debug)]
pub struct EnsureForInfo {
    /// Dependencies (foreign_type, local_key, local_key_json)
    pub dependencies: Vec<(String, String, String)>,
}

/// Information about a default_value on a field
#[derive(Debug)]
pub struct DefaultValueInfo {
    #[allow(dead_code)]
    pub field_name: String,
    pub field_name_json: String,
    pub value_tokens: TokenStream,
}

/// Convert snake_case to camelCase
pub fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = false;
    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }
    result
}

/// Check if an attribute is a relationship attribute that should be stripped
pub fn is_relationship_attr(attr: &Attribute) -> bool {
    let path = attr.path();
    path.is_ident("belongs_to")
        || path.is_ident("owns_many")
        || path.is_ident("ensure_for")
        || path.is_ident("default_value")
}

/// Parse belongs_to attribute from a field
pub fn parse_belongs_to(field: &Field) -> Option<BelongsToInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);

    for attr in &field.attrs {
        if attr.path().is_ident("belongs_to")
            && let Ok(path) = attr.parse_args::<Path>()
        {
            let foreign_type = path.segments.last()?.ident.to_string();
            return Some(BelongsToInfo {
                field_name,
                field_name_json,
                foreign_type,
            });
        }
    }
    None
}

/// Parse owns_many attribute from a field
pub fn parse_owns_many(field: &Field) -> Option<OwnsManyInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);

    for attr in &field.attrs {
        if attr.path().is_ident("owns_many")
            && let Ok(path) = attr.parse_args::<Path>()
        {
            let foreign_type = path.segments.last()?.ident.to_string();
            return Some(OwnsManyInfo {
                field_name,
                field_name_json,
                foreign_type,
            });
        }
    }
    None
}

/// Parse default_value attribute from a field
pub fn parse_default_value(field: &Field) -> Option<DefaultValueInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);

    for attr in &field.attrs {
        if attr.path().is_ident("default_value") {
            // Parse the literal or expression inside the attribute
            if let Ok(lit) = attr.parse_args::<syn::Lit>() {
                let value_tokens = quote! { #lit };
                return Some(DefaultValueInfo {
                    field_name,
                    field_name_json,
                    value_tokens,
                });
            }
            // Also try parsing as an expression for more complex defaults
            if let Ok(expr) = attr.parse_args::<syn::Expr>() {
                let value_tokens = quote! { #expr };
                return Some(DefaultValueInfo {
                    field_name,
                    field_name_json,
                    value_tokens,
                });
            }
        }
    }
    None
}

/// Parse ensure_for attributes from struct-level attributes
pub fn parse_ensure_for(input: &ItemStruct) -> Option<EnsureForInfo> {
    let mut dependencies = Vec::new();

    for attr in &input.attrs {
        if attr.path().is_ident("ensure_for") {
            // Parse comma-separated list of types: #[ensure_for(Session, Bundle)]
            if let Ok(paths) = attr.parse_args_with(
                syn::punctuated::Punctuated::<Path, syn::Token![,]>::parse_terminated,
            ) {
                for path in paths {
                    if let Some(segment) = path.segments.last() {
                        let foreign_type = segment.ident.to_string();
                        // The local key is derived from foreign type: Session -> session_id
                        let local_key = format!("{}_id", to_snake_case(&foreign_type));
                        let local_key_json = to_camel_case(&local_key);
                        dependencies.push((foreign_type, local_key, local_key_json));
                    }
                }
            }
        }
    }

    if dependencies.is_empty() {
        None
    } else {
        Some(EnsureForInfo { dependencies })
    }
}

/// Convert PascalCase to snake_case
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }
    result
}

/// Strip relationship attributes from a field's attributes
pub fn strip_relationship_attrs(field: &mut Field) {
    field.attrs.retain(|attr| !is_relationship_attr(attr));
}

/// Strip ensure_for attributes from struct-level attributes
pub fn strip_ensure_for_attrs(input: &mut ItemStruct) {
    input.attrs.retain(|attr| !attr.path().is_ident("ensure_for"));
}

/// Collected relationship information from an item
#[derive(Debug, Default)]
pub struct RelationshipInfo {
    pub belongs_to: Vec<BelongsToInfo>,
    pub owns_many: Vec<OwnsManyInfo>,
    pub ensure_for: Option<EnsureForInfo>,
    pub default_values: Vec<DefaultValueInfo>,
}

/// Collect all relationship information from an item struct
pub fn collect_relationships(input: &ItemStruct) -> RelationshipInfo {
    let mut info = RelationshipInfo {
        ensure_for: parse_ensure_for(input),
        ..Default::default()
    };

    // Collect field-level relationships
    if let syn::Fields::Named(ref fields) = input.fields {
        for field in &fields.named {
            if let Some(bt) = parse_belongs_to(field) {
                info.belongs_to.push(bt);
            }
            if let Some(om) = parse_owns_many(field) {
                info.owns_many.push(om);
            }
            if let Some(dv) = parse_default_value(field) {
                info.default_values.push(dv);
            }
        }
    }

    info
}

/// Generate relationship registration code
pub fn generate_registrations(local_type: &str, info: &RelationshipInfo) -> TokenStream {
    let mut registrations = Vec::new();

    // Generate BelongsTo registrations
    for bt in &info.belongs_to {
        let local_key = &bt.field_name;
        let local_key_json = &bt.field_name_json;
        let foreign_type = &bt.foreign_type;

        registrations.push(quote! {
            myko_rs::submit! {
                myko_rs::relationship::RelationRegistration {
                    relation: myko_rs::relationship::Relation::BelongsTo {
                        local_type: #local_type,
                        local_key: #local_key,
                        local_key_json: #local_key_json,
                        foreign_type: #foreign_type,
                    }
                }
            }
        });
    }

    // Generate OwnsMany registrations
    for om in &info.owns_many {
        let local_key = &om.field_name;
        let local_key_json = &om.field_name_json;
        let foreign_type = &om.foreign_type;

        registrations.push(quote! {
            myko_rs::submit! {
                myko_rs::relationship::RelationRegistration {
                    relation: myko_rs::relationship::Relation::OwnsMany {
                        local_type: #local_type,
                        local_key: #local_key,
                        local_key_json: #local_key_json,
                        foreign_type: #foreign_type,
                    }
                }
            }
        });
    }

    // Generate EnsureFor registration if present
    if let Some(ref ef) = info.ensure_for {
        let deps: Vec<_> = ef
            .dependencies
            .iter()
            .map(|(ft, lk, lkj)| {
                quote! {
                    myko_rs::relationship::EnsureForDependency {
                        foreign_type: #ft,
                        local_key: #lk,
                        local_key_json: #lkj,
                    }
                }
            })
            .collect();

        // Generate make_default function based on default_values
        let default_fields: Vec<_> = info
            .default_values
            .iter()
            .map(|dv| {
                let field_json = &dv.field_name_json;
                let value = &dv.value_tokens;
                quote! {
                    obj.insert(#field_json.to_string(), serde_json::json!(#value));
                }
            })
            .collect();

        registrations.push(quote! {
            myko_rs::submit! {
                myko_rs::relationship::RelationRegistration {
                    relation: myko_rs::relationship::Relation::EnsureFor {
                        local_type: #local_type,
                        dependencies: &[#(#deps),*],
                        make_default: || {
                            let mut obj = serde_json::Map::new();
                            #(#default_fields)*
                            serde_json::Value::Object(obj)
                        },
                    }
                }
            }
        });
    }

    quote! {
        #(#registrations)*
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("scope_id"), "scopeId");
        assert_eq!(to_camel_case("node_ids"), "nodeIds");
        assert_eq!(to_camel_case("name"), "name");
        assert_eq!(to_camel_case("my_long_field_name"), "myLongFieldName");
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("Session"), "session");
        assert_eq!(to_snake_case("BindingNode"), "binding_node");
        assert_eq!(to_snake_case("Name"), "name");
    }
}
