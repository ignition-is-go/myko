use std::collections::{BTreeSet, HashMap};

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    ExprPath, Field, FieldsNamed, ItemStruct, LitBool, LitInt, Path, Result, Token,
    parse::{Parse, ParseStream},
};

use crate::{DeriveCtx, relationship, setter};

#[derive(Default)]
pub struct ItemArgs {
    pub ingest_buffer_ms: Option<u64>,
    pub post_deserialize: Option<ExprPath>,
    pub service: Option<Path>,
    pub scope_root: bool,
    pub scoped_by: Option<Path>,
    pub filters: Option<bool>,
    pub deletes: Option<bool>,
}

impl Parse for ItemArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut args = Self::default();

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            if ident == "ingest_buffer_ms" {
                input.parse::<Token![=]>()?;
                let value: LitInt = input.parse()?;
                args.ingest_buffer_ms = Some(value.base10_parse()?);
            } else if ident == "post_deserialize" {
                input.parse::<Token![=]>()?;
                let value: ExprPath = input.parse()?;
                args.post_deserialize = Some(value);
            } else if ident == "service" {
                input.parse::<Token![=]>()?;
                if args.service.is_some() {
                    return Err(syn::Error::new(ident.span(), "duplicate `service`"));
                }
                args.service = Some(input.parse()?);
            } else if ident == "scope_root" {
                if args.scope_root {
                    return Err(syn::Error::new(ident.span(), "duplicate `scope_root`"));
                }
                args.scope_root = true;
            } else if ident == "scoped_by" {
                input.parse::<Token![=]>()?;
                if args.scoped_by.is_some() {
                    return Err(syn::Error::new(ident.span(), "duplicate `scoped_by`"));
                }
                args.scoped_by = Some(input.parse()?);
            } else if ident == "filters" {
                input.parse::<Token![=]>()?;
                if args.filters.is_some() {
                    return Err(syn::Error::new(ident.span(), "duplicate `filters`"));
                }
                args.filters = Some(input.parse::<LitBool>()?.value);
            } else if ident == "deletes" {
                input.parse::<Token![=]>()?;
                if args.deletes.is_some() {
                    return Err(syn::Error::new(ident.span(), "duplicate `deletes`"));
                }
                args.deletes = Some(input.parse::<LitBool>()?.value);
            } else {
                return Err(syn::Error::new(
                    ident.span(),
                    "unsupported myko_item option",
                ));
            }

            if input.is_empty() {
                break;
            }

            input.parse::<Token![,]>()?;
        }

        Ok(args)
    }
}

/// If `ty` is `Option<Inner>`, returns `Inner`. Used by the advanced-query
/// filter codegen: an optional entity field's filter still targets the
/// inner type (`Filterable` is implemented on `Option<T>` as a passthrough
/// to `T::Filter` — see `core::query::filter` — so the *type* resolves
/// either way), but `matches` needs the inner type specifically to compare
/// against, since a `None` field must never satisfy any filter regardless
/// of what the filter says (spec §1: "a `None` field matches no filter").
fn to_pascal_case(value: &str) -> String {
    value
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(chars).collect()
            })
        })
        .collect()
}

fn option_inner_type(ty: &syn::Type) -> Option<&syn::Type> {
    let syn::Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

fn generate_get_all_query(
    name: &syn::Ident,
    query_ident: &syn::Ident,
    krate: &syn::Path,
) -> TokenStream {
    quote! {
        #[derive(PartialEq, Eq)]
        #[#krate::myko_query(#name, service = #name)]
        pub struct #query_ident {}

        impl #krate::prelude::QueryHandler for #query_ident {
            fn test_entity(_ctx: #krate::prelude::QueryTestContext<Self>) -> bool {
                true
            }

            #[cfg(not(target_arch = "wasm32"))]
            fn build_view(
                ctx: #krate::prelude::QueryBuildArgs<Self>,
            ) -> Option<impl #krate::prelude::MapQuery<
                Key = std::sync::Arc<str>,
                Value = std::sync::Arc<dyn #krate::prelude::AnyItem>,
            >>
            where
                Self: std::marker::Send + std::marker::Sync + 'static,
            {
                use #krate::prelude::RegistryScoped as _;
                // Registry stores are already partitioned by entity type, so returning
                // the raw map avoids installing a no-op `select(|_| true)` runtime.
                Some(
                    ctx.query_context
                        .registry()
                        .get_or_create(<#name as #krate::prelude::Eventable>::ENTITY_NAME_STATIC)
                        .as_ref()
                        .clone()
                        .lock(),
                )
            }
        }
    }
}

fn generate_get_by_ids_query(
    name: &syn::Ident,
    name_str: &str,
    id_type_ident: &syn::Ident,
    query_ident: &syn::Ident,
    krate: &syn::Path,
) -> TokenStream {
    quote! {
        #[derive(PartialEq, Eq)]
        #[#krate::myko_query(#name, service = #name)]
        pub struct #query_ident {
            pub ids: Vec<#id_type_ident>,
        }

        impl #krate::prelude::QueryHandler for #query_ident {
            fn test_entity(ctx: #krate::prelude::QueryTestContext<Self>) -> bool {
                ctx.query.ids.contains(&ctx.item.id.clone().into())
            }

            #[cfg(not(target_arch = "wasm32"))]
            fn build_view(
                ctx: #krate::prelude::QueryBuildArgs<Self>,
            ) -> Option<impl #krate::prelude::MapQuery<
                Key = std::sync::Arc<str>,
                Value = std::sync::Arc<dyn #krate::prelude::AnyItem>,
            >>
            where
                Self: std::marker::Send + std::marker::Sync + 'static,
            {
                use #krate::prelude::RegistryScoped as _;
                let ids: Vec<std::sync::Arc<str>> = ctx.query.ids.iter()
                    .map(|id| std::sync::Arc::<str>::from(id.clone()))
                    .collect();
                let store = ctx.query_context.registry().get_or_create(#name_str);
                Some(#krate::query::build_ids_source_map(&store, &ids))
            }
        }
    }
}

fn generate_filter_struct(
    name: &syn::Ident,
    filter_ident: &syn::Ident,
    filter_fields: &[(syn::Ident, syn::Type)],
    ctx: &DeriveCtx,
) -> TokenStream {
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));
    let fields = filter_fields.iter().map(|(field_ident, field_ty)| {
        quote! {
            #[serde(default, skip_serializing_if = "Option::is_none")]
            #[ts(optional = nullable)]
            pub #field_ident: Option<<#field_ty as #krate::query::Filterable>::Filter>
        }
    });
    let matches = filter_fields.iter().map(|(field_ident, field_ty)| {
        if option_inner_type(field_ty).is_some() {
            quote! {
                self.#field_ident.as_ref().is_none_or(|f| {
                    item.#field_ident.as_ref().is_some_and(|v| #krate::query::Filter::matches(f, v))
                })
            }
        } else {
            quote! {
                self.#field_ident.as_ref().is_none_or(|f| #krate::query::Filter::matches(f, &item.#field_ident))
            }
        }
    }).reduce(|acc, term| quote! { (#acc) && (#term) }).unwrap_or_else(|| quote! { true });
    let canonical_fields = filter_fields.iter().map(|(field_ident, _)| {
        quote! {
            #field_ident: self.#field_ident.map(#krate::query::CanonicalFilter::canonicalize)
        }
    });
    let equal_fields = filter_fields
        .iter()
        .map(|(field_ident, _)| quote! { self.#field_ident == other.#field_ident })
        .reduce(|acc, term| quote! { (#acc) && (#term) })
        .unwrap_or_else(|| quote! { true });

    quote! {
        #[derive(Clone, Default, Debug, #serde_path::Serialize, #serde_path::Deserialize)]
        #[derive(#krate::TS)]
        #[ts(crate = "myko::ts_rs")]
        #serde_rename_attr
        pub struct #filter_ident { #(#fields),* }

        impl PartialEq for #filter_ident {
            fn eq(&self, other: &Self) -> bool { #equal_fields }
        }

        impl #filter_ident {
            pub fn matches(&self, item: &#name) -> bool { #matches }

            pub fn canonicalize(self) -> Self {
                Self { #(#canonical_fields),* }
            }
        }

        #krate::register_typegen_type!(#filter_ident);
    }
}

fn generate_route_arms(
    name: &syn::Ident,
    belongs_to: &[&relationship::BelongsToInfo],
    krate: &syn::Path,
) -> Vec<TokenStream> {
    let count = belongs_to.len();
    let Some(mask_limit) = 1u32.checked_shl(u32::try_from(count).unwrap_or(u32::MAX)) else {
        return Vec::new();
    };
    let mut masks: Vec<u32> = (1..mask_limit).collect();
    masks.sort_by(|a, b| b.count_ones().cmp(&a.count_ones()).then(a.cmp(b)));
    masks
        .into_iter()
        .map(|mask| {
            let selected: Vec<_> = (0..count)
                .filter(|index| {
                    mask & 1u32
                        .checked_shl(u32::try_from(*index).unwrap_or(u32::MAX))
                        .unwrap_or(0)
                        != 0
                })
                .filter_map(|index| belongs_to.get(index).copied())
                .collect();
            let field_idents: Vec<_> = selected
                .iter()
                .map(|item| format_ident!("{}", item.field_name))
                .collect();
            let field_names: Vec<_> = selected
                .iter()
                .map(|item| item.field_name.clone())
                .collect();
            let filters: Vec<_> = (0..selected.len())
                .map(|index| format_ident!("filter{index}"))
                .collect();
            let refs: Vec<_> = field_idents
                .iter()
                .map(|field| quote! { self.#field.as_ref() })
                .collect();
            let condition = if selected.len() == 1 {
                filters.first().zip(refs.first()).map_or_else(
                    || quote! { if false },
                    |(filter, field)| quote! { if let Some(#filter) = #field },
                )
            } else {
                quote! { if let (#(Some(#filters)),*) = (#(#refs),*) }
            };
            let extracted = field_idents
                .iter()
                .map(|field| quote! { std::sync::Arc::<str>::from(e.#field.clone()) });
            let values = filters.iter().map(|filter| quote! {
            #filter.key_values().into_iter().map(std::sync::Arc::<str>::from).collect::<Vec<_>>()
        });
            quote! {
                #condition {
                    let keys = #krate::query::cartesian_product(vec![#(#values),*]);
                    return Some(#krate::query::BelongsToRoute {
                        field_names: &[#(#field_names),*], keys,
                        extract_fk: |item: &dyn std::any::Any| {
                            item.downcast_ref::<#name>()
                                .map(|e| #krate::query::CompoundKey::from_iter([#(#extracted),*]))
                        },
                    });
                }
            }
        })
        .collect()
}

fn generate_route_impl(
    name: &syn::Ident,
    name_str: &str,
    filter_ident: &syn::Ident,
    route_arms: &[TokenStream],
    krate: &syn::Path,
) -> TokenStream {
    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        impl #filter_ident {
            pub fn belongs_to_route(&self) -> Option<#krate::query::BelongsToRoute> {
                #(#route_arms)*
                None
            }

            pub fn query_route(&self) -> Option<#krate::query::QueryRoute> {
                if let Some(id_filter) = self.id.as_ref() {
                    return Some(#krate::query::QueryRoute::Ids(
                        id_filter.key_values().into_iter().map(std::sync::Arc::<str>::from).collect(),
                    ));
                }
                self.belongs_to_route().map(#krate::query::QueryRoute::BelongsTo)
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        impl #krate::query::LiveFilterQuery for #filter_ident {
            type Item = #name;
            fn entity_type() -> &'static str { #name_str }
            fn matches(&self, item: &Self::Item) -> bool { #filter_ident::matches(self, item) }
            fn query_route(&self) -> Option<#krate::query::QueryRoute> { #filter_ident::query_route(self) }
        }

        #[cfg(target_arch = "wasm32")]
        impl #krate::query::LiveFilterQuery for #filter_ident {
            type Item = #name;
            fn entity_type() -> &'static str { #name_str }
            fn matches(&self, _item: &Self::Item) -> bool {
                unreachable!("live queries execute on the server")
            }
            fn query_route(&self) -> Option<#krate::query::QueryRoute> {
                unreachable!("live queries execute on the server")
            }
        }
    }
}

fn generate_filter_query(
    name: &syn::Ident,
    name_str: &str,
    filter_ident: &syn::Ident,
    query_ident: &syn::Ident,
    krate: &syn::Path,
) -> TokenStream {
    quote! {
        #[#krate::myko_non_hash_cache_key]
        #[#krate::myko_manual_cache_key]
        #[#krate::myko_query(#name, service = #name)]
        pub struct #query_ident(pub #filter_ident);

        impl #krate::prelude::CacheKey for #query_ident {
            fn cache_key(&self, state: &mut dyn std::hash::Hasher) {
                #krate::cache::write_serde_cache_key(&self.0.clone().canonicalize(), state);
            }
        }

        impl #krate::prelude::QueryHandler for #query_ident {
            fn test_entity(ctx: #krate::prelude::QueryTestContext<Self>) -> bool {
                ctx.query.0.matches(&ctx.item)
            }

            #[cfg(not(target_arch = "wasm32"))]
            fn build_view(ctx: #krate::prelude::QueryBuildArgs<Self>)
                -> Option<impl #krate::prelude::MapQuery<
                    Key = std::sync::Arc<str>,
                    Value = std::sync::Arc<dyn #krate::prelude::AnyItem>,
                >>
            where Self: std::marker::Send + std::marker::Sync + 'static,
            {
                use #krate::prelude::RegistryScoped as _;
                let source = match ctx.query.0.query_route()? {
                    #krate::query::QueryRoute::Ids(ids) => {
                        let store = ctx.query_context.registry().get_or_create(#name_str);
                        #krate::query::build_ids_source_map(&store, &ids)
                    }
                    #krate::query::QueryRoute::BelongsTo(route) => {
                        #krate::query::build_belongs_to_union_source_map(
                            ctx.query_context.registry(),
                            ctx.query_context.query_context.req.host_id,
                            #name_str, route.field_names, route.extract_fk, route.keys,
                        )
                    }
                };
                Some(#krate::query::filter_query_over_source::<#query_ident>(
                    source, ctx.query.clone(), ctx.query_context.query_context.clone(),
                ))
            }
        }
    }
}

fn generate_count_all_report(
    name: &syn::Ident,
    query_ident: &syn::Ident,
    result_ident: &syn::Ident,
    report_ident: &syn::Ident,
    krate: &syn::Path,
) -> TokenStream {
    quote! {
        #[#krate::myko_report_output]
        pub struct #result_ident { pub count: usize }

        #[#krate::myko_report(#result_ident, item = #name)]
        pub struct #report_ident {}

        impl #krate::prelude::ReportHandler for #report_ident {
            type Output = #result_ident;
            fn compute(&self, ctx: #krate::prelude::ReportContext)
                -> impl #krate::prelude::Materialize<std::sync::Arc<Self::Output>, #krate::prelude::Definite>
            {
                use #krate::prelude::{MapExt as _, Querying as _};
                let query = #query_ident {};
                let source = ctx.query_map_by_str(query);
                source.size().map(move |count| {
                    std::sync::Arc::new(#result_ident { count: *count })
                })
            }
        }
    }
}

fn generate_count_report(
    name: &syn::Ident,
    filter_ident: &syn::Ident,
    query_ident: &syn::Ident,
    result_ident: &syn::Ident,
    report_ident: &syn::Ident,
    krate: &syn::Path,
) -> TokenStream {
    quote! {
        #[#krate::myko_non_hash_cache_key]
        #[#krate::myko_manual_cache_key]
        #[#krate::myko_report(#result_ident, item = #name)]
        pub struct #report_ident(pub #filter_ident);

        impl #krate::prelude::CacheKey for #report_ident {
            fn cache_key(&self, state: &mut dyn std::hash::Hasher) {
                #krate::cache::write_serde_cache_key(&self.0.clone().canonicalize(), state);
            }
        }

        impl #krate::prelude::ReportHandler for #report_ident {
            type Output = #result_ident;
            fn compute(&self, ctx: #krate::prelude::ReportContext)
                -> impl #krate::prelude::Materialize<std::sync::Arc<Self::Output>, #krate::prelude::Definite>
            {
                use #krate::prelude::{MapExt as _, Querying as _};
                let source = ctx.query_map_by_str(#query_ident(self.0.clone()));
                source.size().map(move |count| {
                    std::sync::Arc::new(#result_ident { count: *count })
                })
            }
        }
    }
}

fn generate_get_by_id_report(
    name: &syn::Ident,
    id_type_ident: &syn::Ident,
    report_ident: &syn::Ident,
    krate: &syn::Path,
) -> TokenStream {
    quote! {
        #[#krate::myko_report(Option<std::sync::Arc<#name>>, item = #name)]
        pub struct #report_ident { pub id: #id_type_ident }

        impl #krate::prelude::ReportHandler for #report_ident {
            type Output = Option<std::sync::Arc<#name>>;
            fn compute(&self, ctx: #krate::prelude::ReportContext)
                -> impl #krate::prelude::Materialize<std::sync::Arc<Self::Output>, #krate::prelude::Definite>
            {
                use #krate::prelude::{Eventable as _, MapExt as _, RegistryScoped as _};
                let id: std::sync::Arc<str> = self.id.clone().into();
                let store = ctx.registry().get_or_create(#name::ENTITY_NAME_STATIC);
                store.get(&id).map(move |item| std::sync::Arc::new(
                    item.as_ref().and_then(|item| item.as_any().downcast_ref::<#name>())
                        .map(|typed| std::sync::Arc::new(typed.clone()))
                ))
            }
        }
    }
}

struct DeleteGeneration<'a> {
    name_str: &'a str,
    id_type_ident: &'a syn::Ident,
    get_by_id_ident: &'a syn::Ident,
    get_by_ids_ident: &'a syn::Ident,
    ctx: &'a DeriveCtx,
}

fn generate_delete_commands(input: &DeleteGeneration<'_>) -> TokenStream {
    let krate = &input.ctx.krate;
    let serde_path = &input.ctx.serde_path;
    let serde_attr = input.ctx.serde_attr(&quote!(rename_all = "camelCase"));
    let delete_ident = format_ident!("Delete{}", input.name_str);
    let delete_result = format_ident!("Delete{}Result", input.name_str);
    let delete_many_ident = format_ident!("Delete{}s", input.name_str);
    let delete_many_result = format_ident!("Delete{}sResult", input.name_str);
    let name_str = input.name_str;
    let id = input.id_type_ident;
    let get_one = input.get_by_id_ident;
    let get_many = input.get_by_ids_ident;
    quote! {
        #[derive(Clone, PartialEq, Eq, #serde_path::Serialize, #serde_path::Deserialize, Debug, #krate::TS)]
        #[ts(crate = "myko::ts_rs")]
        #serde_attr
        pub struct #delete_result { pub deleted: bool }
        #krate::register_typegen_type!(#delete_result);

        #[#krate::myko_command(#delete_result)]
        pub struct #delete_ident { pub id: #id }
        impl #krate::command::CommandHandler for #delete_ident {
            fn execute(self, ctx: #krate::prelude::CommandContext) -> Result<#delete_result, #krate::prelude::CommandError> {
                use #krate::prelude::{EventPublishing as _, RequestScoped as _};
                match ctx.exec_report(#get_one { id: self.id.clone() })? {
                    Some(entity) => { ctx.emit_del(entity)?; Ok(#delete_result { deleted: true }) }
                    None => Err(#krate::prelude::CommandError::new(
                        ctx.tx(), ctx.command_id.to_string(), format!("{} not found: {}", #name_str, self.id),
                    )),
                }
            }
        }

        #[derive(Clone, PartialEq, Eq, #serde_path::Serialize, #serde_path::Deserialize, Debug, #krate::TS)]
        #[ts(crate = "myko::ts_rs")]
        #serde_attr
        pub struct #delete_many_result { pub deleted_count: usize }
        #krate::register_typegen_type!(#delete_many_result);

        #[#krate::myko_command(#delete_many_result)]
        pub struct #delete_many_ident { pub ids: Vec<#id> }
        impl #krate::command::CommandHandler for #delete_many_ident {
            fn execute(self, ctx: #krate::prelude::CommandContext) -> Result<#delete_many_result, #krate::prelude::CommandError> {
                use #krate::prelude::EventPublishing as _;
                let entities = ctx.exec_query(#get_many { ids: self.ids.clone() })?;
                let deleted_count = entities.len();
                ctx.emit_del_batch(entities.iter().map(|entity| entity.as_ref()))?;
                Ok(#delete_many_result { deleted_count })
            }
        }
    }
}

struct PreparedItem {
    input_struct: ItemStruct,
    relationships: relationship::RelationshipInfo,
    setters: Vec<setter::SetterField>,
    name: syn::Ident,
    name_str: String,
    id_type_ident: syn::Ident,
    ctx: DeriveCtx,
    filter_fields: Vec<(syn::Ident, syn::Type)>,
    derives: TokenStream,
    partial_eq_impl: TokenStream,
    post_deserialize: Option<TokenStream>,
    ingest_registration: Option<TokenStream>,
}

// Preparation deliberately centralizes mutations to the parsed struct before
// any expansion reads it, so relationship and generated-field metadata agree.
#[allow(clippy::too_many_lines)]
fn prepare_item(args: &ItemArgs, mut input_struct: ItemStruct) -> Result<PreparedItem> {
    let name = input_struct.ident.clone();
    let name_str = name.to_string();
    let id_type_ident = format_ident!("{}Id", name_str);
    let ctx = DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    if let Some(parent) = &args.scoped_by {
        let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields else {
            return Err(syn::Error::new_spanned(
                &name,
                "scoped Myko items require named fields",
            ));
        };
        let parent_name = parent
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .ok_or_else(|| syn::Error::new_spanned(parent, "scoped parent type is empty"))?;
        let parent_id = format_ident!("{}_id", snake_case(&parent_name));
        if named
            .iter()
            .any(|field| field.ident.as_ref() == Some(&parent_id))
        {
            return Err(syn::Error::new_spanned(
                parent_id,
                "the scoped parent ID field is generated by `scoped_by`",
            ));
        }
        named.push(syn::parse_quote! {
            #[belongs_to(#parent)]
            pub #parent_id: <#parent as #krate::MykoItem>::Id
        });
    }
    let relationships = relationship::collect_relationships(&input_struct);
    let setters = setter::collect_setter_fields(&input_struct);
    let ingest_registration = args.ingest_buffer_ms.map(|window_ms| quote! {
        #krate::submit! {
            #krate::prelude::IngestBufferRegistration {
                entity_type: #name_str,
                policy: #krate::prelude::IngestBufferPolicy::TimeWindow { window_ms: #window_ms },
            }
        }
    });
    crate::gate_ts_attrs(&mut input_struct.attrs);
    let filter_fields =
        if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
            for field in named.iter_mut() {
                relationship::strip_relationship_attrs(field);
                setter::strip_setter_attrs(field);
                crate::prepare_typegen_field(field);
            }
            let id_field: Field = syn::parse_quote! { pub id: #id_type_ident };
            named.push(id_field);
            if args.filters.unwrap_or(true) {
                named
                    .iter()
                    .filter_map(|field| Some((field.ident.clone()?, field.ty.clone())))
                    .collect()
            } else {
                let parent_id = args.scoped_by.as_ref().and_then(|parent| {
                    parent.segments.last().map(|segment| {
                        format_ident!("{}_id", snake_case(&segment.ident.to_string()))
                    })
                });
                named
                    .iter()
                    .filter(|field| {
                        field
                            .ident
                            .as_ref()
                            .is_some_and(|ident| ident == "id" || parent_id.as_ref() == Some(ident))
                    })
                    .filter_map(|field| Some((field.ident.clone()?, field.ty.clone())))
                    .collect()
            }
        } else {
            Vec::new()
        };
    let serde_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));
    let deserialize = args
        .post_deserialize
        .as_ref()
        .map_or_else(|| quote!(#serde_path::Deserialize,), |_| quote!());
    let default = (!relationships.ensure_for_fields.is_empty()).then(|| quote!(Default,));
    let derives = quote! {
        #[derive(#default Clone, #serde_path::Serialize, #deserialize Debug, #krate::TS)]
        #[ts(crate = "myko::ts_rs")]
        #serde_attr
    };
    let equal_fields = input_struct
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let member = field.ident.clone().map_or_else(
                || syn::Member::Unnamed(syn::Index::from(index)),
                syn::Member::Named,
            );
            quote! { self.#member == other.#member }
        })
        .reduce(|acc, term| quote! { (#acc) && (#term) })
        .unwrap_or_else(|| quote! { true });
    let partial_eq_impl = quote! {
        impl PartialEq for #name {
            fn eq(&self, other: &Self) -> bool { #equal_fields }
        }
    };
    let post_deserialize = args.post_deserialize.as_ref().map(|callback| {
        let helper_ident = format_ident!("{}DeserializeHelper", name_str);
        let mut helper_struct = input_struct.clone();
        helper_struct.ident = helper_ident.clone();
        let helper_fields = match &input_struct.fields {
            syn::Fields::Named(FieldsNamed { named, .. }) => named.iter().filter_map(|field| field.ident.clone()).collect::<Vec<_>>(),
            _ => Vec::new(),
        };
        quote! {
            #[derive(#serde_path::Deserialize, #krate::TS)]
            #[ts(crate = "myko::ts_rs")]
            #serde_attr
            #helper_struct
            impl<'de> #serde_path::Deserialize<'de> for #name {
                fn deserialize<D: #serde_path::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    let #helper_ident { #(#helper_fields),* } = #helper_ident::deserialize(deserializer)?;
                    let mut value = #name { #(#helper_fields),* };
                    #callback(&mut value);
                    Ok(value)
                }
            }
        }
    });
    Ok(PreparedItem {
        input_struct,
        relationships,
        setters,
        name,
        name_str,
        id_type_ident,
        ctx,
        filter_fields,
        derives,
        partial_eq_impl,
        post_deserialize,
        ingest_registration,
    })
}

fn snake_case(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();
    let mut previous = None;
    while let Some(character) = characters.next() {
        let next_is_lower = characters.peek().is_some_and(char::is_ascii_lowercase);
        let starts_word = previous.is_some_and(|previous: char| {
            character.is_ascii_uppercase()
                && (previous.is_ascii_lowercase() || previous.is_ascii_digit() || next_is_lower)
        });
        if starts_word {
            output.push('_');
        }
        output.extend(character.to_lowercase());
        previous = Some(character);
    }
    output
}

fn generate_foreign_key_impls(
    name: &syn::Ident,
    input_struct: &ItemStruct,
    relationships: &relationship::RelationshipInfo,
    krate: &syn::Path,
) -> Vec<TokenStream> {
    let field_types = match &input_struct.fields {
        syn::Fields::Named(FieldsNamed { named, .. }) => named
            .iter()
            .filter_map(|field| Some((field.ident.as_ref()?.to_string(), field.ty.clone())))
            .collect::<HashMap<_, _>>(),
        _ => HashMap::new(),
    };
    let fields = relationships
        .belongs_to
        .iter()
        .map(|item| (&item.field_name, &item.foreign_type, &item.foreign_path))
        .chain(
            relationships
                .ensure_for_fields
                .iter()
                .map(|item| (&item.field_name, &item.foreign_type, &item.foreign_path)),
        );
    let mut relations = BTreeSet::new();
    fields
        .filter_map(|(field_name, foreign_type, foreign_path)| {
            if !relations.insert((field_name.clone(), foreign_type.clone())) {
                return None;
            }
            let field_ty = field_types.get(field_name)?;
            let field_ident = format_ident!("{field_name}");
            let field_pascal = to_pascal_case(field_name);
            let relation_ident = format_ident!("{name}{field_pascal}Relation");
            let (foreign_key_ty, foreign_key) = option_inner_type(field_ty).map_or_else(
                || (field_ty, quote! { Some(child.#field_ident.clone()) }),
                |inner| (inner, quote! { child.#field_ident.clone() }),
            );
            Some(quote! {
                pub struct #relation_ident;

                impl #krate::hyphae::ForeignKeyRelation for #relation_ident
                where #foreign_key_ty: #krate::hyphae::IdFor<#foreign_path>,
                {
                    type Parent = #foreign_path;
                    type Child = std::sync::Arc<#name>;
                    type ForeignKey = #foreign_key_ty;

                    fn foreign_key(child: &Self::Child) -> Option<Self::ForeignKey> {
                        #foreign_key
                    }
                }
            })
        })
        .collect()
}

fn generate_id_type(name: &syn::Ident, id: &syn::Ident, ctx: &DeriveCtx) -> TokenStream {
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    quote! {
        #[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, #serde_path::Serialize, #serde_path::Deserialize, Debug, #krate::TS)]
        #[ts(crate = "myko::ts_rs")]
        #[ts(type = "string")]
        pub struct #id(pub std::sync::Arc<str>);
        impl #id {
            #[must_use]
            pub fn new(value: impl Into<std::sync::Arc<str>>) -> Self { Self(value.into()) }
        }
        impl std::ops::Deref for #id { type Target = str; fn deref(&self) -> &str { self.0.as_ref() } }
        impl std::fmt::Display for #id {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { std::fmt::Display::fmt(self.0.as_ref(), f) }
        }
        impl AsRef<str> for #id { fn as_ref(&self) -> &str { self.0.as_ref() } }
        impl From<std::sync::Arc<str>> for #id { fn from(value: std::sync::Arc<str>) -> Self { Self(value) } }
        impl From<#id> for std::sync::Arc<str> { fn from(value: #id) -> Self { value.0 } }
        impl From<String> for #id { fn from(value: String) -> Self { Self(value.into()) } }
        impl From<&str> for #id { fn from(value: &str) -> Self { Self(value.into()) } }
        impl #krate::hyphae::IdFor<#name> for #id {
            type MapKey = std::sync::Arc<str>;
            fn map_key(&self) -> Self::MapKey { self.0.clone() }
        }
        impl #krate::hyphae::IdType for #id { type Parent = #name; }
        impl #krate::ItemId for #id {}
        impl #krate::query::Filterable for #id { type Filter = #krate::query::IdFilter<#id>; }
    }
}

struct ItemExpansion {
    name: syn::Ident,
    name_str: String,
    id_type_ident: syn::Ident,
    ctx: DeriveCtx,
    input_struct: ItemStruct,
    derives: TokenStream,
    partial_eq_impl: TokenStream,
    post_deserialize: Option<TokenStream>,
    ingest_registration: Option<TokenStream>,
    item_registration: TokenStream,
    server_owned_impls: TokenStream,
    foreign_key_impls: Vec<TokenStream>,
    federated_item_impls: TokenStream,
    generated_items: Vec<TokenStream>,
}

fn expand_item(input: ItemExpansion) -> TokenStream {
    let ItemExpansion {
        name,
        name_str,
        id_type_ident,
        ctx,
        input_struct,
        derives,
        partial_eq_impl,
        post_deserialize,
        ingest_registration,
        item_registration,
        server_owned_impls,
        foreign_key_impls,
        federated_item_impls,
        generated_items,
    } = input;
    let krate = &ctx.krate;
    let id_type = generate_id_type(&name, &id_type_ident, &ctx);
    quote! {
        use #krate::prelude::Query as _;
        use #krate::hyphae::MapExt as _;
        #id_type
        #derives
        #input_struct
        #partial_eq_impl
        #post_deserialize
        #krate::register_typegen_type!(#id_type_ident, #name);
        #krate::submit! { #item_registration }
        #ingest_registration

        impl #krate::item::Eventable for #name {
            const ENTITY_NAME_STATIC: &'static str = #name_str;
        }
        impl #krate::prelude::AnyItem for #name {
            fn as_any(&self) -> &dyn std::any::Any { self }
            fn entity_type(&self) -> &'static str { #name_str }
            fn equals(&self, other: &dyn #krate::prelude::AnyItem) -> bool {
                other.as_any().downcast_ref::<Self>().is_some_and(|typed| self == typed)
            }
            #server_owned_impls
        }
        impl #krate::prelude::WithId for #name {
            fn id(&self) -> std::sync::Arc<str> { self.id.clone().into() }
        }
        impl #krate::common::with_id::WithTypedId for #name {
            type Id = #id_type_ident;
            fn typed_id(&self) -> Self::Id { self.id.clone().into() }
        }
        #federated_item_impls
        #(#foreign_key_impls)*
        #(#generated_items)*
    }
}

// The item contract is emitted as one block so its service, scope, queries,
// and parent relationship cannot drift across separate expansion branches.
#[allow(clippy::too_many_lines)]
fn generate_federated_item_impls(
    args: &ItemArgs,
    name: &syn::Ident,
    name_str: &str,
    id: &syn::Ident,
    get_all: &syn::Ident,
    get_by_ids: &syn::Ident,
    krate: &syn::Path,
) -> TokenStream {
    let (service, service_impl) = args.service.as_ref().map_or_else(
        || {
            (
                quote!(#name),
                quote! {
                    impl #krate::MykoService for #name {
                        type Items = (#name,);
                        const SERVICE_ID: #krate::ServiceTypeId = #krate::ServiceTypeId::new(
                            concat!(module_path!(), "::", stringify!(#name)),
                        );
                    }
                },
            )
        },
        |service| (quote!(#service), quote!()),
    );
    let (scope_type, scope, scope_id, belongs_to_method, belongs_to_impl) =
        args.scoped_by.as_ref().map_or_else(
            || {
                let scope = if args.scope_root {
                    quote!(#krate::ItemScope::Root)
                } else {
                    quote!(#krate::ItemScope::Unscoped)
                };
                (quote!(#name), scope, quote!(&self.id), quote!(), quote!())
            },
            |parent| {
                let parent_name = parent
                    .segments
                    .last()
                    .map(|segment| segment.ident.to_string())
                    .unwrap_or_default();
                let parent_id = format_ident!("{}_id", snake_case(&parent_name));
                let belongs_to_method = quote! {
                    fn belongs_to(&self) -> Option<#krate::FederatedEntityRef> {
                        Some(<Self as #krate::BelongsTo>::parent_ref(self))
                    }
                };
                let belongs_to_impl = quote! {
                    impl #krate::BelongsTo for #name {
                        type Parent = #parent;

                        fn parent_id(&self) -> &<#parent as #krate::MykoItem>::Id {
                            &self.#parent_id
                        }
                    }
                };
                if args.scope_root {
                    (
                        quote!(#name),
                        quote!(#krate::ItemScope::RootScopedBy {
                            service_id: <#parent as #krate::MykoItem>::SERVICE_ID,
                            item_type: <#parent as #krate::MykoItem>::ITEM_TYPE,
                        }),
                        quote!(&self.id),
                        belongs_to_method,
                        belongs_to_impl,
                    )
                } else {
                    (
                        quote!(#parent),
                        quote!(#krate::ItemScope::ScopedBy {
                            service_id: <#parent as #krate::MykoItem>::SERVICE_ID,
                            item_type: <#parent as #krate::MykoItem>::ITEM_TYPE,
                        }),
                        quote!(&self.#parent_id),
                        belongs_to_method,
                        belongs_to_impl,
                    )
                }
            },
        );

    quote! {
        #service_impl

        impl #krate::ItemQuery for #get_all {
            type Item = #name;
        }

        impl #krate::GeneratedItemQuery for #get_all {}

        impl #krate::ItemQuery for #get_by_ids {
            type Item = #name;

            fn selected_item_ids(&self) -> Option<Vec<#id>> {
                Some(self.ids.clone())
            }
        }

        impl #krate::GeneratedItemQuery for #get_by_ids {}

        impl #krate::MykoItem for #name {
            type Id = #id;
            type Service = #service;
            type Scope = #scope_type;
            type GetAllQuery = #get_all;
            type GetByIdQuery = #get_by_ids;
            type GetByIdsQuery = #get_by_ids;

            const ITEM_TYPE: &'static str = #name_str;
            const SCOPE: #krate::ItemScope = #scope;

            fn item_id(&self) -> &Self::Id {
                &self.id
            }

            fn scope_id(&self) -> &<Self::Scope as #krate::MykoItem>::Id {
                #scope_id
            }

            #belongs_to_method
        }

        #belongs_to_impl
    }
}

fn generate_item_registration(name: &syn::Ident, name_str: &str, krate: &syn::Path) -> TokenStream {
    quote! {
        #krate::prelude::ItemRegistration {
            entity_type: #name_str,
            service_id: Some(<#name as #krate::MykoItem>::SERVICE_ID),
            crate_name: module_path!(),
            parse: <#name as #krate::item::Eventable>::parse,
            parse_bytes: <#name as #krate::item::Eventable>::parse_bytes,
            serialize_json: |any| {
                let typed = any.as_any().downcast_ref::<#name>().ok_or_else(|| {
                    #krate::serde_json::Error::io(std::io::Error::other(
                        "ItemRegistration::serialize_json: entity_type/type mismatch",
                    ))
                })?;
                #krate::serde_json::value::to_raw_value(typed)
            },
        }
    }
}

fn generate_server_owned_impls(
    relationships: &relationship::RelationshipInfo,
    krate: &syn::Path,
) -> TokenStream {
    relationships.server_owned_field.as_ref().map_or_else(|| quote! {}, |field| {
        let field_ident = format_ident!("{}", field.field_name);
        quote! {
            fn server_owner(&self) -> Option<&str> {
                let owner: &str = &self.#field_ident;
                (!owner.is_empty()).then_some(owner)
            }
            fn bake_server_owner(&self, server_id: &str) -> Option<std::sync::Arc<dyn #krate::prelude::AnyItem>> {
                let mut patched = self.clone();
                patched.#field_ident = server_id.to_string().into();
                Some(std::sync::Arc::new(patched))
            }
        }
    })
}

fn required_belongs_to(
    rel_info: &relationship::RelationshipInfo,
) -> Vec<&relationship::BelongsToInfo> {
    rel_info
        .belongs_to
        .iter()
        .filter(|item| !item.is_optional)
        .collect()
}

// This is the single assembly point for the established item macro fragments;
// splitting it would add indirection without reducing generated complexity.
#[allow(clippy::too_many_lines)]
pub fn myko_item_impl(args: &ItemArgs, input_struct: ItemStruct) -> Result<TokenStream> {
    let PreparedItem {
        input_struct,
        relationships: rel_info,
        setters: setter_fields,
        name,
        name_str,
        id_type_ident,
        ctx,
        filter_fields,
        derives,
        partial_eq_impl,
        post_deserialize: post_deserialize_impl,
        ingest_registration: ingest_buffer_registration,
    } = prepare_item(args, input_struct)?;
    let name = &name;
    let krate = &ctx.krate;

    let get_all_query_ident = format_ident!("GetAll{}s", name_str);
    let get_all_query = generate_get_all_query(name, &get_all_query_ident, krate);

    let get_by_ids_query_ident = format_ident!("Get{}sByIds", name_str);
    let get_by_ids_query = generate_get_by_ids_query(
        name,
        &name_str,
        &id_type_ident,
        &get_by_ids_query_ident,
        krate,
    );

    // Route by the intersection of every belongs_to field the query
    // actually pins, not just the first one declared on the struct. For an
    // entity with N required belongs_to fields there are 2^N - 1 non-empty
    // subsets of "which fields happen to be Some at runtime" — generate an
    // if-let block per subset, tried from most fields-pinned (most
    // selective) down to a single field, so a query pinning e.g. both
    // node_id and session_id always routes on that exact pair rather than
    // silently collapsing onto whichever field was declared first. Each
    // subset gets its own BelongsToSourceIndex (keyed by the field-NAME SET,
    // see build_belongs_to_source_map), so different subsets never share a
    // bucket even when they overlap on one field.
    let required_belongs_to = required_belongs_to(&rel_info);

    // ─────────────────────────────────────────────────────────────────
    // Query (myko 5.0, docs/superpowers/specs/2026-07-14-myko-5-query-
    // api.md). XQuery mirrors the entity field-for-field, but each field is
    // Option<<FieldType as Filterable>::Filter> instead of
    // Option<FieldType> — the per-type filter (IdFilter/NumericFilter/
    // StringFilter/EqFilter/bool) the compiler resolves via Filterable.
    // This is now the ONLY per-entity query-parameter type — there is no
    // separate Partial-based query anymore.
    // ─────────────────────────────────────────────────────────────────

    let filter_ident = format_ident!("{}Query", name_str);
    let get_by_filter_ident = format_ident!("Get{}sByQuery", name_str);

    // Fields are optional on the wire and in TS (`field?: Filter | null`):
    // a query pins a handful of fields, so callers — especially TS
    // constructors, which would otherwise have to name every entity field —
    // only write the pinned ones. `default` tolerates omitted fields on
    // deserialize; `skip_serializing_if` keeps unpinned fields off the wire
    // (and out of the serde-derived cache key, which only sees pinned
    // fields either way since it hashes the canonicalized value).
    let filter_struct = generate_filter_struct(name, &filter_ident, &filter_fields, &ctx);

    // K-bucket union routing for In/Eq on #[belongs_to] fields — the spec
    // §4 hard requirement. Subset enumeration (largest/most-selective
    // combination first): instead of a single Eq foreign_id, extracts each
    // matched field's IdFilter key_values() (the Eq value, or the whole In
    // set — id filters only ever express Eq/In, so this is always
    // index-servable, never a scan) and unions the cartesian product of
    // every field's value set. A field with an In set of size N and
    // another of size M yields N*M compound keys, each backed by its own
    // bucket in the union.
    //
    // Emitted as a `belongs_to_route()` METHOD (returning the routing
    // decision as data, not directly building/wrapping a source map)
    // rather than inlined into build_view directly, so query_live's
    // incremental per-tick diffing can call the exact same decision logic
    // build_view uses for the one-shot value-based query — one routing
    // rule, two consumers.
    let filter_belongs_to_route_arms = generate_route_arms(name, &required_belongs_to, krate);

    // BelongsToRoute is a plain data type visible on every target (needed so
    // ReportContext::query_live's wasm32 stub can name LiveFilterQuery in its
    // bound), but this impl's body calls cartesian_product, which lives in
    // the server-only core::query::registration engine — gate the impl the
    // same way build_view already is, rather than the individual method.
    let filter_belongs_to_route_impl = generate_route_impl(
        name,
        &name_str,
        &filter_ident,
        &filter_belongs_to_route_arms,
        krate,
    );

    let get_by_filter_query =
        generate_filter_query(name, &name_str, &filter_ident, &get_by_filter_ident, krate);

    // Generate per-entity count result type (e.g., TargetCount, InstanceCount)
    // This avoids the shared CountResult type which causes duplicate imports in TypeScript
    let count_result_ident = format_ident!("{}Count", name_str);

    let count_all_report_ident = format_ident!("CountAll{}s", name_str);
    let count_all_report = generate_count_all_report(
        name,
        &get_all_query_ident,
        &count_result_ident,
        &count_all_report_ident,
        krate,
    );

    // Generate Count report, filtered the same way GetXsByQuery is.
    let count_report_ident = format_ident!("Count{}s", name_str);

    let count_report = generate_count_report(
        name,
        &filter_ident,
        &get_by_filter_ident,
        &count_result_ident,
        &count_report_ident,
        krate,
    );

    // Generate Get{Entity}ById report that returns Option<Arc<Entity>>
    let get_by_id_report_ident = format_ident!("Get{}ById", name_str);

    let get_by_id_report =
        generate_get_by_id_report(name, &id_type_ident, &get_by_id_report_ident, krate);
    let delete_commands = if args.deletes.unwrap_or(true) {
        generate_delete_commands(&DeleteGeneration {
            name_str: &name_str,
            id_type_ident: &id_type_ident,
            get_by_id_ident: &get_by_id_report_ident,
            get_by_ids_ident: &get_by_ids_query_ident,
            ctx: &ctx,
        })
    } else {
        TokenStream::new()
    };

    let item_registration = generate_item_registration(name, &name_str, krate);

    // Generate relationship registrations
    let relationship_registrations = relationship::generate_registrations(&name_str, &rel_info);

    let has_foreign_key_impls = generate_foreign_key_impls(name, &input_struct, &rel_info, krate);

    // Generate setter commands for fields with #[myko_rename] or #[myko_setter]
    let setter_commands = setter::generate_setter_commands(&name_str, &setter_fields);

    let server_owned_impls = generate_server_owned_impls(&rel_info, krate);
    let federated_item_impls = generate_federated_item_impls(
        args,
        name,
        &name_str,
        &id_type_ident,
        &get_all_query_ident,
        &get_by_ids_query_ident,
        krate,
    );
    Ok(expand_item(ItemExpansion {
        name: name.clone(),
        name_str,
        id_type_ident,
        ctx,
        input_struct,
        derives,
        partial_eq_impl,
        post_deserialize: post_deserialize_impl,
        ingest_registration: ingest_buffer_registration,
        item_registration,
        server_owned_impls,
        foreign_key_impls: has_foreign_key_impls,
        federated_item_impls,
        generated_items: vec![
            get_all_query,
            get_by_ids_query,
            filter_struct,
            filter_belongs_to_route_impl,
            get_by_filter_query,
            count_all_report,
            count_report,
            get_by_id_report,
            delete_commands,
            setter_commands,
            relationship_registrations,
        ],
    }))
}
