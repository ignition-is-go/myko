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

/// Information about a single ensure_for dependency on a field
#[derive(Debug)]
pub struct EnsureForFieldInfo {
    /// Field name in Rust (snake_case)
    pub field_name: String,
    /// Field name in JSON (camelCase)
    pub field_name_json: String,
    /// Foreign entity type name (the dependency)
    pub foreign_type: String,
}

/// Information about ensure_for relationships on the struct (collected from fields)
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

/// Information about a myko_client_id attribute on a field.
/// When present, the server will auto-populate this field with the client_id
/// of the WebSocket connection that sent the event.
#[derive(Debug)]
pub struct ClientIdFieldInfo {
    /// Field name in JSON (camelCase)
    pub field_name_json: String,
}

/// Information about a searchable field for full-text search indexing.
#[derive(Debug)]
pub struct SearchableFieldInfo {
    /// Field name in JSON (camelCase) - used for indexing
    pub field_name_json: String,
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
        || path.is_ident("myko_client_id")
        || path.is_ident("searchable")
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

/// Parse ensure_for attribute from a field.
///
/// `#[ensure_for(Type)]` on a field indicates this entity should be auto-created
/// for each instance of the dependency type. Multiple ensure_for attributes on
/// different fields create a Cartesian product.
///
/// # Example
///
/// ```rust,ignore
/// #[myko_item]
/// pub struct BundleStatus {
///     #[ensure_for(Session)]
///     pub session_id: Arc<str>,
///     #[ensure_for(Bundle)]
///     pub bundle_id: Arc<str>,
/// }
/// // Creates one BundleStatus per Session×Bundle combination
/// ```
pub fn parse_ensure_for_field(field: &Field) -> Option<EnsureForFieldInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);

    for attr in &field.attrs {
        if attr.path().is_ident("ensure_for")
            && let Ok(path) = attr.parse_args::<Path>()
        {
            let foreign_type = path.segments.last()?.ident.to_string();
            return Some(EnsureForFieldInfo {
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

/// Parse myko_client_id attribute from a field.
///
/// When present, the server will auto-populate this field with the client_id
/// of the WebSocket connection that sent the event.
///
/// # Example
///
/// ```rust,ignore
/// #[myko_item]
/// pub struct Instance {
///     #[myko_client_id]
///     pub client_id: Option<String>,
/// }
/// ```
pub fn parse_client_id(field: &Field) -> Option<ClientIdFieldInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);

    for attr in &field.attrs {
        if attr.path().is_ident("myko_client_id") {
            return Some(ClientIdFieldInfo { field_name_json });
        }
    }
    None
}

/// Parse searchable attribute from a field.
///
/// When present, this field will be included in full-text search indexing.
///
/// # Example
///
/// ```rust,ignore
/// #[myko_item]
/// pub struct Target {
///     #[searchable]
///     pub name: String,
///     #[searchable]
///     pub category: String,
///     pub service_id: Arc<str>,  // not searchable
/// }
/// ```
pub fn parse_searchable(field: &Field) -> Option<SearchableFieldInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);

    for attr in &field.attrs {
        if attr.path().is_ident("searchable") {
            return Some(SearchableFieldInfo { field_name_json });
        }
    }
    None
}

/// Strip relationship attributes from a field's attributes
pub fn strip_relationship_attrs(field: &mut Field) {
    field.attrs.retain(|attr| !is_relationship_attr(attr));
}

/// Collected relationship information from an item
#[derive(Debug, Default)]
pub struct RelationshipInfo {
    pub belongs_to: Vec<BelongsToInfo>,
    pub owns_many: Vec<OwnsManyInfo>,
    pub ensure_for_fields: Vec<EnsureForFieldInfo>,
    pub default_values: Vec<DefaultValueInfo>,
    pub client_id_field: Option<ClientIdFieldInfo>,
    pub searchable_fields: Vec<SearchableFieldInfo>,
}

impl RelationshipInfo {
    /// Convert ensure_for_fields to EnsureForInfo for registration
    pub fn ensure_for(&self) -> Option<EnsureForInfo> {
        if self.ensure_for_fields.is_empty() {
            None
        } else {
            Some(EnsureForInfo {
                dependencies: self
                    .ensure_for_fields
                    .iter()
                    .map(|ef| {
                        (
                            ef.foreign_type.clone(),
                            ef.field_name.clone(),
                            ef.field_name_json.clone(),
                        )
                    })
                    .collect(),
            })
        }
    }
}

/// Collect all relationship information from an item struct
pub fn collect_relationships(input: &ItemStruct) -> RelationshipInfo {
    let mut info = RelationshipInfo::default();

    // Collect field-level relationships
    if let syn::Fields::Named(ref fields) = input.fields {
        for field in &fields.named {
            if let Some(bt) = parse_belongs_to(field) {
                info.belongs_to.push(bt);
            }
            if let Some(om) = parse_owns_many(field) {
                info.owns_many.push(om);
            }
            if let Some(ef) = parse_ensure_for_field(field) {
                info.ensure_for_fields.push(ef);
            }
            if let Some(dv) = parse_default_value(field) {
                info.default_values.push(dv);
            }
            if let Some(ci) = parse_client_id(field) {
                info.client_id_field = Some(ci);
            }
            if let Some(sf) = parse_searchable(field) {
                info.searchable_fields.push(sf);
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
    if let Some(ref ef) = info.ensure_for() {
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

    // Generate ClientId registration if present
    if let Some(ref ci) = info.client_id_field {
        let field_name_json = &ci.field_name_json;

        registrations.push(quote! {
            myko_rs::submit! {
                myko_rs::relationship::ClientIdRegistration {
                    entity_type: #local_type,
                    field_name_json: #field_name_json,
                }
            }
        });
    }

    // Generate Searchable registration if any fields are marked searchable
    if !info.searchable_fields.is_empty() {
        let fields: Vec<_> = info
            .searchable_fields
            .iter()
            .map(|sf| {
                let field = &sf.field_name_json;
                quote! { #field }
            })
            .collect();

        registrations.push(quote! {
            myko_rs::submit! {
                myko_rs::search::SearchableRegistration {
                    entity_type: #local_type,
                    fields: &[#(#fields),*],
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
}
