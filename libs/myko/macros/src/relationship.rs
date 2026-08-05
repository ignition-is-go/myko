//! Relationship attribute parsing helpers for `myko_item` macro

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Field, ItemStruct, Path};

/// Information about a `belongs_to` relationship on a field
#[derive(Debug)]
pub struct BelongsToInfo {
    /// Field name in Rust (`snake_case`)
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

/// Information about an `owns_many` relationship on a field
#[derive(Debug)]
pub struct OwnsManyInfo {
    /// Field name in Rust (`snake_case`)
    pub field_name: String,
    /// Owned entity type name
    pub foreign_type: String,
    /// If true, exclude this child from entity tree exports
    pub exclude_from_tree: bool,
}

/// Information about a single `ensure_for` dependency on a field
#[derive(Debug)]
pub struct EnsureForFieldInfo {
    /// Field name in Rust (`snake_case`)
    pub field_name: String,
    /// Field name in JSON (camelCase)
    pub field_name_json: String,
    /// Foreign entity type name (the dependency)
    pub foreign_type: String,
    /// If true, exclude this child from entity tree exports
    pub exclude_from_tree: bool,
}

/// Information about `ensure_for` relationships on the struct (collected from fields)
#[derive(Debug)]
pub struct EnsureForInfo {
    /// Dependencies (`foreign_type`, `local_key`, `local_key_json`, `exclude_from_tree`)
    pub dependencies: Vec<(String, String, String, bool)>,
}

/// Information about a `default_value` on a field
#[derive(Debug)]
pub struct DefaultValueInfo {
    pub field_name: String,
    pub value_tokens: TokenStream,
}

/// Information about a `myko_client_id` attribute on a field.
/// When present, the server will auto-populate this field with the `client_id`
/// of the WebSocket connection that sent the event.
#[derive(Debug)]
pub struct ClientIdFieldInfo {
    /// Field name in JSON (camelCase)
    pub field_name_json: String,
}

/// Information about a `fallback_to_id` attribute on a field.
/// When present, the server will auto-populate this field with the entity's `id`
/// if the value is null or missing at ingest time.
#[derive(Debug)]
pub struct FallbackToIdFieldInfo {
    /// Field name in JSON (camelCase)
    pub field_name_json: String,
}

/// Information about a `server_owned` attribute on a field.
/// When present, the framework auto-manages this `ServerId` field —
/// populating on creation and redistributing on peer death.
#[derive(Debug)]
pub struct ServerOwnedFieldInfo {
    /// Field name in Rust (`snake_case`)
    pub field_name: String,
    /// Field name in JSON (camelCase)
    pub field_name_json: String,
}

/// Information about a searchable field for full-text search indexing.
#[derive(Debug)]
pub struct SearchableFieldInfo {
    /// Field name in Rust (`snake_case`) — used to reference `self.<field>`
    /// in the generated `Searchable::extract_searchable` body.
    pub field_name: String,
    /// Field name in JSON (camelCase) - used for indexing
    pub field_name_json: String,
    /// Whether the field is `Option<_>`. Optional string-like fields index
    /// their inner value when `Some` and contribute nothing when `None`.
    pub is_optional: bool,
}

/// Convert `snake_case` to camelCase
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
        || path.is_ident("server_owned")
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

/// Parse `belongs_to` attribute from a field
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

/// Parse `owns_many` attribute from a field
pub fn parse_owns_many(field: &Field) -> Option<OwnsManyInfo> {
    let field_name = field.ident.as_ref()?.to_string();
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
                foreign_type,
                exclude_from_tree,
            });
        }
    }
    None
}

/// Parse `ensure_for` attribute from a field.
///
/// `#[ensure_for(Type)]` on a field indicates this entity should be auto-created
/// for each instance of the dependency type. Multiple `ensure_for` attributes on
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

/// Parse `default_value` attribute from a field
pub fn parse_default_value(field: &Field) -> Option<DefaultValueInfo> {
    let field_name = field.ident.as_ref()?.to_string();

    for attr in &field.attrs {
        if attr.path().is_ident("default_value") {
            // Parse the literal or expression inside the attribute
            if let Ok(lit) = attr.parse_args::<syn::Lit>() {
                let value_tokens = quote! { #lit };
                return Some(DefaultValueInfo {
                    field_name,
                    value_tokens,
                });
            }
            // Also try parsing as an expression for more complex defaults
            if let Ok(expr) = attr.parse_args::<syn::Expr>() {
                let value_tokens = quote! { #expr };
                return Some(DefaultValueInfo {
                    field_name,
                    value_tokens,
                });
            }
        }
    }
    None
}

/// Parse `myko_client_id` attribute from a field.
///
/// When present, the server will auto-populate this field with the `client_id`
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

/// Parse `fallback_to_id` attribute from a field.
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

/// Parse `server_owned` attribute from a field.
///
/// When present, the framework auto-manages this `ServerId` field —
/// populating on creation and redistributing on peer death.
///
/// # Example
///
/// ```rust,ignore
/// #[myko_item]
/// pub struct Instance {
///     #[server_owned]
///     pub server_id: Option<Arc<str>>,
/// }
/// ```
pub fn parse_server_owned(field: &Field) -> Option<ServerOwnedFieldInfo> {
    let field_name = field.ident.as_ref()?.to_string();
    let field_name_json = to_camel_case(&field_name);

    for attr in &field.attrs {
        if attr.path().is_ident("server_owned") {
            return Some(ServerOwnedFieldInfo {
                field_name,
                field_name_json,
            });
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
            return Some(SearchableFieldInfo {
                field_name,
                field_name_json,
                is_optional: is_option_type(&field.ty),
            });
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
    pub server_owned_field: Option<ServerOwnedFieldInfo>,
}

impl RelationshipInfo {
    /// Convert `ensure_for_fields` to `EnsureForInfo` for registration
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
            if let Some(so) = parse_server_owned(field) {
                info.server_owned_field = Some(so);
            }
        }
    }

    info
}

fn generate_belongs_to_registrations(
    local_type: &str,
    info: &RelationshipInfo,
) -> Vec<TokenStream> {
    let local_type_ident = syn::Ident::new(local_type, proc_macro2::Span::call_site());
    let krate = crate::myko_path();
    info.belongs_to
        .iter()
        .map(|bt| {
        let field_ident = syn::Ident::new(&bt.field_name, proc_macro2::Span::call_site());
        let foreign_type = &bt.foreign_type;
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
        quote! {
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
        }
    })
    .collect()
}

fn generate_owns_many_registrations(
    local_type: &str,
    info: &RelationshipInfo,
) -> Vec<TokenStream> {
    let local_type_ident = syn::Ident::new(local_type, proc_macro2::Span::call_site());
    let krate = crate::myko_path();
    info.owns_many
        .iter()
        .map(|om| {
        let field_ident = syn::Ident::new(&om.field_name, proc_macro2::Span::call_site());
        let foreign_type = &om.foreign_type;
        let exclude_from_tree = om.exclude_from_tree;
        quote! {
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
        }
    })
    .collect()
}

fn generate_ensure_for_registration(
    local_type: &str,
    info: &RelationshipInfo,
) -> Option<TokenStream> {
    let local_type_ident = syn::Ident::new(local_type, proc_macro2::Span::call_site());
    let krate = crate::myko_path();
    info.ensure_for().map(|ef| {
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

        let fk_field_assignments: Vec<_> = ef
            .dependencies
            .iter()
            .enumerate()
            .map(|(i, (_, lk, _lkj, _ex))| {
                let field_ident = syn::Ident::new(lk, proc_macro2::Span::call_site());
                let idx = syn::Index::from(i);
                quote! {
                    if let Some(dep_id) = dep_ids.get(#idx) {
                        entity.#field_ident = dep_id.clone().into();
                    }
                }
            })
            .collect();

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

        quote! {
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
        }
    })
}

fn generate_marker_registrations(local_type: &str, info: &RelationshipInfo) -> Vec<TokenStream> {
    let krate = crate::myko_path();
    let mut registrations = Vec::new();

    if let Some(ci) = &info.client_id_field {
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
    if let Some(so) = &info.server_owned_field {
        let field_name_json = &so.field_name_json;
        registrations.push(quote! {
            #[cfg(not(target_arch = "wasm32"))]
            #krate::submit! {
                #krate::relationship::ServerOwnedRegistration {
                    entity_type: #local_type,
                    field_name_json: #field_name_json,
                }
            }
        });
    }
    registrations
}

fn generate_search_registration(local_type: &str, info: &RelationshipInfo) -> TokenStream {
    let local_type_ident = syn::Ident::new(local_type, proc_macro2::Span::call_site());
    let krate = crate::myko_path();
    let json_fields = info.searchable_fields.iter().map(|field| {
        let field = &field.field_name_json;
        quote! { #field }
    });

    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #krate::search::SearchableRegistration {
                entity_type: #local_type,
                fields: &[#(#json_fields),*],
                register_typed: ::std::option::Option::Some(
                    |reg: &mut #krate::search::typed::SearchRegistry|
                        reg.register::<#local_type_ident>(#local_type),
                ),
            }
        }
    }
}

fn generate_searchable_impl(local_type: &str, info: &RelationshipInfo) -> TokenStream {
    let local_type_ident = syn::Ident::new(local_type, proc_macro2::Span::call_site());
    let krate = crate::myko_path();
    let push_calls = info.searchable_fields.iter().map(|field| {
        let ident = syn::Ident::new(&field.field_name, proc_macro2::Span::call_site());
        if field.is_optional {
            quote! {
                match &self.#ident {
                    ::std::option::Option::Some(__v) => {
                        extractor.push_field(::std::convert::AsRef::<str>::as_ref(__v));
                    }
                    ::std::option::Option::None => {
                        extractor.push_field("");
                    }
                }
            }
        } else {
            quote! { extractor.push_field(::std::convert::AsRef::<str>::as_ref(&self.#ident)); }
        }
    });
    let name_strs = info.searchable_fields.iter().map(|field| {
        let name = &field.field_name_json;
        quote! { #name }
    });

    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        impl #krate::search::typed::Searchable for #local_type_ident {
            fn extract_searchable(
                &self,
                extractor: &mut #krate::search::typed::SearchableExtractor<'_>,
            ) {
                #(#push_calls)*
            }

            fn searchable_field_names() -> &'static [&'static str] {
                &[#(#name_strs),*]
            }
        }
    }
}

fn generate_search_report(local_type: &str) -> TokenStream {
    let krate = crate::myko_path();
    let id_type_ident = syn::Ident::new(&format!("{local_type}Id"), proc_macro2::Span::call_site());
    let search_result_ident = syn::Ident::new(
        &format!("Search{local_type}Result"),
        proc_macro2::Span::call_site(),
    );
    let search_report_ident = syn::Ident::new(
        &format!("Search{local_type}"),
        proc_macro2::Span::call_site(),
    );
    let default_limit_fn = syn::Ident::new(
        &format!("__myko_search_default_limit_{local_type}"),
        proc_macro2::Span::call_site(),
    );
    let default_limit_fn_str = default_limit_fn.to_string();

    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        #[#krate::myko_report_output]
        pub struct #search_result_ident {
            pub ids: ::std::vec::Vec<#id_type_ident>,
        }

        #[cfg(not(target_arch = "wasm32"))]
        #[doc(hidden)]
        pub fn #default_limit_fn() -> usize {
            #krate::search::default_search_limit()
        }

        /// Search this entity type by query string.
        #[cfg(not(target_arch = "wasm32"))]
        #[#krate::myko_report(#search_result_ident)]
        pub struct #search_report_ident {
            pub query: ::std::string::String,
            #[serde(default = #default_limit_fn_str)]
            pub limit: usize,
        }

        #[cfg(not(target_arch = "wasm32"))]
        impl #krate::prelude::ReportHandler for #search_report_ident {
            type Output = #search_result_ident;

            fn compute(
                &self,
                ctx: #krate::prelude::ReportContext,
            ) -> impl #krate::prelude::Materialize<::std::sync::Arc<Self::Output>, #krate::prelude::Definite> {
                let arc_ids = ctx.search(#local_type, &self.query, self.limit);
                let ids: ::std::vec::Vec<#id_type_ident> = arc_ids
                    .into_iter()
                    .map(<#id_type_ident as ::std::convert::From<::std::sync::Arc<str>>>::from)
                    .collect();
                #krate::hyphae::Cell::new(::std::sync::Arc::new(#search_result_ident { ids })).lock()
            }
        }
    }
}

fn generate_search_registrations(local_type: &str, info: &RelationshipInfo) -> Vec<TokenStream> {
    if info.searchable_fields.is_empty() {
        Vec::new()
    } else {
        vec![
            generate_search_registration(local_type, info),
            generate_searchable_impl(local_type, info),
            generate_search_report(local_type),
        ]
    }
}

/// Generate relationship registration code.
pub fn generate_registrations(local_type: &str, info: &RelationshipInfo) -> TokenStream {
    let mut registrations = generate_belongs_to_registrations(local_type, info);
    registrations.extend(generate_owns_many_registrations(local_type, info));
    registrations.extend(generate_ensure_for_registration(local_type, info));
    registrations.extend(generate_marker_registrations(local_type, info));
    registrations.extend(generate_search_registrations(local_type, info));

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
