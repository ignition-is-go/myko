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
    /// Whether the field type is Option<T>
    pub is_optional: bool,
    /// If true, exclude this child from entity tree exports
    pub exclude_from_tree: bool,
}

/// Information about an owns_many relationship on a field
#[derive(Debug)]
pub struct OwnsManyInfo {
    /// Field name in Rust (snake_case)
    pub field_name: String,
    /// Field name in JSON (camelCase) - reserved for future use
    #[allow(dead_code)]
    pub field_name_json: String,
    /// Owned entity type name
    pub foreign_type: String,
    /// If true, exclude this child from entity tree exports
    pub exclude_from_tree: bool,
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
    /// If true, exclude this child from entity tree exports
    pub exclude_from_tree: bool,
}

/// Information about ensure_for relationships on the struct (collected from fields)
#[derive(Debug)]
pub struct EnsureForInfo {
    /// Dependencies (foreign_type, local_key, local_key_json, exclude_from_tree)
    pub dependencies: Vec<(String, String, String, bool)>,
}

/// Information about a default_value on a field
#[derive(Debug)]
pub struct DefaultValueInfo {
    #[allow(dead_code)]
    pub field_name: String,
    /// Field name in JSON (camelCase) - reserved for future use
    #[allow(dead_code)]
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

/// Information about a fallback_to_id attribute on a field.
/// When present, the server will auto-populate this field with the entity's `id`
/// if the value is null or missing at ingest time.
#[derive(Debug)]
pub struct FallbackToIdFieldInfo {
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
        || path.is_ident("fallback_to_id")
        || path.is_ident("searchable")
        || path.is_ident("exclude_from_tree")
}

/// Check if a type is Option<T>
fn is_option_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "Option";
    }
    false
}

/// Parse belongs_to attribute from a field
pub fn parse_belongs_to(field: &Field) -> Option<BelongsToInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);
    let is_optional = is_option_type(&field.ty);
    let exclude_from_tree = field
        .attrs
        .iter()
        .any(|a| a.path().is_ident("exclude_from_tree"));

    for attr in &field.attrs {
        if attr.path().is_ident("belongs_to")
            && let Ok(path) = attr.parse_args::<Path>()
        {
            let foreign_type = path.segments.last()?.ident.to_string();
            return Some(BelongsToInfo {
                field_name,
                field_name_json,
                foreign_type,
                is_optional,
                exclude_from_tree,
            });
        }
    }
    None
}

/// Parse owns_many attribute from a field
pub fn parse_owns_many(field: &Field) -> Option<OwnsManyInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);
    let exclude_from_tree = field
        .attrs
        .iter()
        .any(|a| a.path().is_ident("exclude_from_tree"));

    for attr in &field.attrs {
        if attr.path().is_ident("owns_many")
            && let Ok(path) = attr.parse_args::<Path>()
        {
            let foreign_type = path.segments.last()?.ident.to_string();
            return Some(OwnsManyInfo {
                field_name,
                field_name_json,
                foreign_type,
                exclude_from_tree,
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
    let exclude_from_tree = field
        .attrs
        .iter()
        .any(|a| a.path().is_ident("exclude_from_tree"));

    for attr in &field.attrs {
        if attr.path().is_ident("ensure_for")
            && let Ok(path) = attr.parse_args::<Path>()
        {
            let foreign_type = path.segments.last()?.ident.to_string();
            return Some(EnsureForFieldInfo {
                field_name,
                field_name_json,
                foreign_type,
                exclude_from_tree,
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

/// Parse fallback_to_id attribute from a field.
///
/// When present, the server will auto-populate this field with the entity's own `id`
/// if the value is null or missing at ingest time. Useful for optional fields that
/// should default to the entity's ID (e.g., `cluster_id` defaulting to `instance_id`).
///
/// # Example
///
/// ```rust,ignore
/// #[myko_item]
/// pub struct Instance {
///     #[fallback_to_id]
///     pub cluster_id: Option<String>,
/// }
/// ```
pub fn parse_fallback_to_id(field: &Field) -> Option<FallbackToIdFieldInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);

    for attr in &field.attrs {
        if attr.path().is_ident("fallback_to_id") {
            return Some(FallbackToIdFieldInfo { field_name_json });
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
    pub fallback_to_id_fields: Vec<FallbackToIdFieldInfo>,
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
                            ef.exclude_from_tree,
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
            if let Some(fi) = parse_fallback_to_id(field) {
                info.fallback_to_id_fields.push(fi);
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
    let local_type_ident = syn::Ident::new(local_type, proc_macro2::Span::call_site());
    let krate = crate::myko_rs_path();

    // Generate BelongsTo registrations
    for bt in &info.belongs_to {
        let field_ident = syn::Ident::new(&bt.field_name, proc_macro2::Span::call_site());
        let foreign_type = &bt.foreign_type;

        // Generate different extract_fk code for optional vs non-optional fields
        let extract_fk = if bt.is_optional {
            quote! {
                |item: &dyn std::any::Any| -> Option<std::sync::Arc<str>> {
                    item.downcast_ref::<#local_type_ident>()
                        .and_then(|e| e.#field_ident.as_ref().map(|s| std::sync::Arc::<str>::from(&**s)))
                }
            }
        } else {
            quote! {
                |item: &dyn std::any::Any| -> Option<std::sync::Arc<str>> {
                    item.downcast_ref::<#local_type_ident>()
                        .map(|e| std::sync::Arc::<str>::from(&*e.#field_ident))
                }
            }
        };

        let exclude_from_tree = bt.exclude_from_tree;
        let fk_field_json = &bt.field_name_json;
        registrations.push(quote! {
            #krate::submit! {
                #krate::relationship::RelationRegistration {
                    relation: #krate::relationship::Relation::BelongsTo {
                        local_type: #local_type,
                        foreign_type: #foreign_type,
                        fk_field_json: #fk_field_json,
                        extract_fk: #extract_fk,
                        exclude_from_tree: #exclude_from_tree,
                    }
                }
            }
        });
    }

    // Generate OwnsMany registrations
    for om in &info.owns_many {
        let field_ident = syn::Ident::new(&om.field_name, proc_macro2::Span::call_site());
        let foreign_type = &om.foreign_type;
        let exclude_from_tree = om.exclude_from_tree;

        registrations.push(quote! {
            #krate::submit! {
                #krate::relationship::RelationRegistration {
                    relation: #krate::relationship::Relation::OwnsMany {
                        local_type: #local_type,
                        foreign_type: #foreign_type,
                        extract_ids: |item: &dyn std::any::Any| -> Option<Vec<std::sync::Arc<str>>> {
                            item.downcast_ref::<#local_type_ident>()
                                .map(|e| e.#field_ident.iter().map(|id| std::sync::Arc::<str>::from(&**id)).collect())
                        },
                        remove_id: |item: &dyn std::any::Any, id_to_remove: &str| -> Option<std::sync::Arc<dyn #krate::item::AnyItem>> {
                            item.downcast_ref::<#local_type_ident>().map(|e| {
                                let mut updated = e.clone();
                                updated.#field_ident.retain(|id| &**id != id_to_remove);
                                std::sync::Arc::new(updated) as std::sync::Arc<dyn #krate::item::AnyItem>
                            })
                        },
                        exclude_from_tree: #exclude_from_tree,
                    }
                }
            }
        });
    }

    // Generate EnsureFor registration if present
    if let Some(ref ef) = info.ensure_for() {
        let exclude_from_tree = ef.dependencies.iter().any(|(_, _, _, ex)| *ex);
        let deps: Vec<_> = ef
            .dependencies
            .iter()
            .map(|(ft, lk, _lkj, _ex)| {
                let field_ident = syn::Ident::new(lk, proc_macro2::Span::call_site());
                quote! {
                    #krate::relationship::EnsureForDependency {
                        foreign_type: #ft,
                        extract_fk: |item: &dyn std::any::Any| -> Option<std::sync::Arc<str>> {
                            item.downcast_ref::<#local_type_ident>()
                                .map(|e| std::sync::Arc::<str>::from(&*e.#field_ident))
                        },
                    }
                }
            })
            .collect();

        // Generate make_entity function that creates entity with dependency IDs populated
        // The function takes &[Arc<str>] with IDs in the same order as dependencies.
        // Assign via Into so typed ID wrappers and Arc<str>/String all work.
        let fk_field_assignments: Vec<_> = ef
            .dependencies
            .iter()
            .enumerate()
            .map(|(i, (_, lk, _lkj, _ex))| {
                let field_ident = syn::Ident::new(lk, proc_macro2::Span::call_site());
                let idx = syn::Index::from(i);
                quote! {
                    entity.#field_ident = dep_ids[#idx].clone().into();
                }
            })
            .collect();

        // Generate default value assignments
        let default_assignments: Vec<_> = info
            .default_values
            .iter()
            .map(|dv| {
                let field_ident = syn::Ident::new(&dv.field_name, proc_macro2::Span::call_site());
                let value = &dv.value_tokens;
                quote! {
                    entity.#field_ident = #value.into();
                }
            })
            .collect();

        registrations.push(quote! {
            #krate::submit! {
                #krate::relationship::RelationRegistration {
                    relation: #krate::relationship::Relation::EnsureFor {
                        local_type: #local_type,
                        dependencies: &[#(#deps),*],
                        exclude_from_tree: #exclude_from_tree,
                        make_entity: |dep_ids: &[std::sync::Arc<str>]| {
                            let mut entity = #local_type_ident::default();
                            entity.id = uuid::Uuid::new_v4().to_string().into();
                            #(#fk_field_assignments)*
                            #(#default_assignments)*
                            std::sync::Arc::new(entity) as std::sync::Arc<dyn #krate::item::AnyItem>
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
            #krate::submit! {
                #krate::relationship::ClientIdRegistration {
                    entity_type: #local_type,
                    field_name_json: #field_name_json,
                }
            }
        });
    }

    // Generate FallbackToId registrations
    for fi in &info.fallback_to_id_fields {
        let field_name_json = &fi.field_name_json;

        registrations.push(quote! {
            #krate::submit! {
                #krate::relationship::FallbackToIdRegistration {
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
            #[cfg(not(target_arch = "wasm32"))]
            #krate::submit! {
                #krate::search::SearchableRegistration {
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
