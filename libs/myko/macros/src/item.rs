use std::collections::{BTreeSet, HashMap};

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    ExprPath, Field, FieldsNamed, ItemStruct, LitInt, Result, Token,
    parse::{Parse, ParseStream},
};

use crate::{DeriveCtx, relationship, setter};

#[derive(Default)]
pub struct ItemArgs {
    pub ingest_buffer_ms: Option<u64>,
    pub post_deserialize: Option<ExprPath>,
}

impl Parse for ItemArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut args = ItemArgs::default();

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            if ident == "ingest_buffer_ms" {
                let value: LitInt = input.parse()?;
                args.ingest_buffer_ms = Some(value.base10_parse()?);
            } else if ident == "post_deserialize" {
                let value: ExprPath = input.parse()?;
                args.post_deserialize = Some(value);
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

pub fn myko_item_impl(args: ItemArgs, mut input_struct: ItemStruct) -> TokenStream {
    // Collect relationship information BEFORE stripping attributes
    let rel_info = relationship::collect_relationships(&input_struct);

    // Collect setter fields BEFORE stripping attributes
    let setter_fields = setter::collect_setter_fields(&input_struct);

    let name = &input_struct.ident;
    let name_str = name.to_string();
    let id_type_ident = format_ident!("{}Id", name_str);

    let ctx = DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let partially_path = &ctx.partially_path;
    let ingest_buffer_registration = args.ingest_buffer_ms.map(|window_ms| {
        quote! {
            #krate::submit! {
                #krate::prelude::IngestBufferRegistration {
                    entity_type: #name_str,
                    policy: #krate::prelude::IngestBufferPolicy::TimeWindow { window_ms: #window_ms },
                }
            }
        }
    });

    // Rewrite any `#[ts(...)]` attrs on the container/fields so they only
    // apply when the consuming crate has `ts-export` on — the ts-rs TS
    // derive we're about to emit conditionally would otherwise leave these
    // orphaned and break compilation when the feature is off.
    crate::gate_ts_attrs(&mut input_struct.attrs);

    let mut filter_fields: Vec<(syn::Ident, syn::Type)> = Vec::new();
    if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
        // Strip relationship and setter attributes from each field
        for field in named.iter_mut() {
            relationship::strip_relationship_attrs(field);
            setter::strip_setter_attrs(field);
            crate::gate_ts_attrs(&mut field.attrs);
        }

        let id = quote! { id };
        let pub_viz = quote! { pub };

        let id_field: Field = syn::parse_quote! {
            #pub_viz #id: #id_type_ident
        };

        named.push(id_field);

        // Snapshot (name, type) for every field — including the id field
        // just pushed — for XFilter codegen (see below). Mirrors exactly
        // the field set `partially::Partial` uses to build PartialX.
        filter_fields = named
            .iter()
            .filter_map(|field| Some((field.ident.clone()?, field.ty.clone())))
            .collect();
    };

    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    let partially_crate_attr = match &ctx.partially_crate_attr {
        Some(s) => quote!(crate = #s,),
        None => quote!(),
    };

    let deserialize_derive = if args.post_deserialize.is_some() {
        quote!()
    } else {
        quote!(#serde_path::Deserialize,)
    };

    // Add Default derive if entity has ensure_for relationships (needed for make_entity)
    // NOTE(ts): `partially` forwards all container attributes (including #[serde(crate = ...)])
    // from the main struct to the Partial struct automatically, so we must NOT also add
    // `attribute(serde(crate = ...))` — that would cause "duplicate serde attribute `crate`".
    // `myko::TS` is the no-op `TsNoop` derive unless myko's own `ts-export` feature is on,
    // so emit it unconditionally on both the item and the generated Partial — no
    // consumer-side feature gate. A single `partially(...)` form always carries TS;
    // `TsNoop` claims the `ts(optional_fields)` attr when typegen is off.
    let derives = if !rel_info.ensure_for_fields.is_empty() {
        quote! {
            #[derive(Default, #partially_path::Partial, PartialEq, Clone, #serde_path::Serialize, #deserialize_derive Debug, #krate::TS)]
            #serde_rename_attr
            #[partially(#partially_crate_attr derive(Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, Default, #krate::PartialMatches, #krate::TS), attribute(ts(optional_fields)))]
        }
    } else {
        quote! {
            #[derive(#partially_path::Partial, PartialEq, Clone, #serde_path::Serialize, #deserialize_derive Debug, #krate::TS)]
            #serde_rename_attr
            #[partially(#partially_crate_attr derive(Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, Default, #krate::PartialMatches, #krate::TS), attribute(ts(optional_fields)))]
        }
    };

    let post_deserialize_impl = args.post_deserialize.as_ref().map(|post_deserialize| {
        let helper_ident = format_ident!("{}DeserializeHelper", name_str);
        let mut helper_struct = input_struct.clone();
        helper_struct.ident = helper_ident.clone();
        let helper_fields =
            if let syn::Fields::Named(FieldsNamed { named, .. }) = &input_struct.fields {
                named
                    .iter()
                    .filter_map(|field| field.ident.clone())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };

        quote! {
            #[derive(#serde_path::Deserialize)]
            #[derive(#krate::TS)]
            #serde_rename_attr
            #helper_struct

            impl<'de> #serde_path::Deserialize<'de> for #name {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: #serde_path::Deserializer<'de>,
                {
                    let #helper_ident {
                        #(#helper_fields),*
                    } = #helper_ident::deserialize(deserializer)?;
                    let mut value = #name {
                        #(#helper_fields),*
                    };
                    #post_deserialize(&mut value);
                    Ok(value)
                }
            }
        }
    });

    let get_all_query_ident = format_ident!("GetAll{}s", name_str);

    let get_all_query = quote! {

        #[#krate::myko_query(#name)]
        pub struct #get_all_query_ident {}

        impl #krate::prelude::QueryHandler for #get_all_query_ident {
            fn test_entity(ctx: #krate::prelude::QueryTestCtx<Self>) -> bool {
                true
            }
        }

    };

    let get_by_ids_query_ident = format_ident!("Get{}sByIds", name_str);

    let get_by_ids_query = quote! {
        #[#krate::myko_query(#name)]
        pub struct #get_by_ids_query_ident {
            pub ids: Vec<#id_type_ident>,
        }

        impl #krate::prelude::QueryHandler for #get_by_ids_query_ident {
            fn test_entity(ctx: #krate::prelude::QueryTestCtx<Self>) -> bool {
                ctx.query.ids.contains(&ctx.item.id.clone().into())
            }

            // Skip the O(N) test_entity scan over every item in the store
            // during initial-list construction. We know the exact ids we
            // care about, so subscribe to those per-key cells directly.
            // `test_entity` semantics still hold because the resulting map
            // only ever contains keys from `ctx.query.ids`.
            #[cfg(not(target_arch = "wasm32"))]
            fn build_view(
                ctx: #krate::prelude::QueryBuildCellCtx<Self>,
            ) -> Option<#krate::prelude::FilteredCellMap>
            where
                Self: std::marker::Send + std::marker::Sync + 'static,
            {
                let ids: Vec<std::sync::Arc<str>> = ctx
                    .query
                    .ids
                    .iter()
                    .map(|id| std::sync::Arc::<str>::from(id.as_ref()))
                    .collect();
                let store = ctx
                    .query_context
                    .registry()
                    .get_or_create(#name_str);
                Some(#krate::query::build_ids_source_map(&store, &ids))
            }
        }

    };

    let get_by_partial_ident = format_ident!("Get{}sByQuery", name_str);
    let partial_ident = format_ident!("Partial{}", name_str);

    // Route by the intersection of every belongs_to field the query's
    // Partial actually pins, not just the first one declared on the struct.
    // For an entity with N required belongs_to fields there are 2^N - 1
    // non-empty subsets of "which fields happen to be Some at runtime" —
    // generate an if-let block per subset, tried from most fields-pinned
    // (most selective) down to a single field, so a query pinning e.g. both
    // node_id and session_id always routes on that exact pair rather than
    // silently collapsing onto whichever field was declared first. Each
    // subset gets its own BelongsToSourceIndex (keyed by the field-NAME SET,
    // see build_belongs_to_source_map), so different subsets never share a
    // bucket even when they overlap on one field.
    let required_belongs_to: Vec<&relationship::BelongsToInfo> = rel_info
        .belongs_to
        .iter()
        .filter(|bt| !bt.is_optional)
        .collect();

    let belongs_to_fast_paths: Vec<TokenStream> = {
        let n = required_belongs_to.len();
        let mut masks: Vec<u32> = (1u32..(1u32 << n)).collect();
        masks.sort_by(|a, b| b.count_ones().cmp(&a.count_ones()).then(a.cmp(b)));

        masks
            .into_iter()
            .map(|mask| {
                let selected: Vec<&relationship::BelongsToInfo> = (0..n)
                    .filter(|i| mask & (1 << i) != 0)
                    .map(|i| required_belongs_to[i])
                    .collect();

                let field_idents: Vec<_> = selected
                    .iter()
                    .map(|bt| format_ident!("{}", bt.field_name))
                    .collect();
                let field_names: Vec<String> =
                    selected.iter().map(|bt| bt.field_name.clone()).collect();
                let fk_idents: Vec<_> = (0..selected.len())
                    .map(|i| format_ident!("fk{}", i))
                    .collect();
                let field_clone_exprs: Vec<TokenStream> = field_idents
                    .iter()
                    .map(|field_ident| quote! { ctx.query.0.#field_ident.clone() })
                    .collect();

                let if_let_expr = if selected.len() == 1 {
                    let fk0 = &fk_idents[0];
                    let clone_expr = &field_clone_exprs[0];
                    quote! { if let Some(#fk0) = #clone_expr }
                } else {
                    quote! { if let (#(Some(#fk_idents)),*) = (#(#field_clone_exprs),*) }
                };

                let extractor_field_exprs: Vec<TokenStream> = field_idents
                    .iter()
                    .map(|field_ident| {
                        quote! { std::sync::Arc::<str>::from(e.#field_ident.as_ref()) }
                    })
                    .collect();
                let foreign_id_exprs: Vec<TokenStream> = fk_idents
                    .iter()
                    .map(|fk| quote! { std::sync::Arc::<str>::from(#fk.as_ref()) })
                    .collect();

                quote! {
                    #if_let_expr {
                        let source = #krate::query::build_belongs_to_source_map(
                            ctx.query_context.registry(),
                            ctx.query_context.request_ctx.host_id,
                            #name_str,
                            &[#(#field_names),*],
                            |item: &dyn std::any::Any| -> Option<Vec<std::sync::Arc<str>>> {
                                item.downcast_ref::<#name>()
                                    .map(|e| vec![#(#extractor_field_exprs),*])
                            },
                            vec![#(#foreign_id_exprs),*],
                        );
                        return Some(#krate::query::filter_query_over_source::<#get_by_partial_ident>(
                            source,
                            ctx.query.clone(),
                            ctx.query_context.query_context.clone(),
                        ));
                    }
                }
            })
            .collect()
    };

    let get_by_partial_query = quote! {
        #[#krate::myko_non_hash_cache_key]
        #[#krate::myko_query(#name)]
         pub struct #get_by_partial_ident(pub #partial_ident);

         impl #krate::prelude::QueryHandler for #get_by_partial_ident {
             fn test_entity(ctx: #krate::prelude::QueryTestCtx<Self>) -> bool {
                 ctx.query.0.matches(&ctx.item)
             }

             #[cfg(not(target_arch = "wasm32"))]
             fn build_view(
                ctx: #krate::prelude::QueryBuildCellCtx<Self>,
             ) -> Option<#krate::prelude::FilteredCellMap>
             where
                Self: std::marker::Send + std::marker::Sync + 'static,
             {
                #(#belongs_to_fast_paths)*
                None
             }
         }

    };

    // ─────────────────────────────────────────────────────────────────
    // Advanced query filters (docs/superpowers/specs/2026-07-13-advanced-
    // query-design.md). XFilter mirrors PartialX field-for-field, but each
    // field is Option<<FieldType as Filterable>::Filter> instead of
    // Option<FieldType> — the per-type filter (IdFilter/NumericFilter/
    // StringFilter/EqFilter/bool) the compiler resolves via Filterable.
    // Purely additive: PartialX/GetXsByQuery above are untouched.
    // ─────────────────────────────────────────────────────────────────

    let filter_ident = format_ident!("{}Filter", name_str);
    let get_by_filter_ident = format_ident!("Get{}sByFilter", name_str);

    let filter_struct_fields: Vec<TokenStream> = filter_fields
        .iter()
        .map(|(field_ident, field_ty)| {
            quote! {
                pub #field_ident: Option<<#field_ty as #krate::query::Filterable>::Filter>
            }
        })
        .collect();

    let filter_match_terms: Vec<TokenStream> = filter_fields
        .iter()
        .map(|(field_ident, field_ty)| {
            if option_inner_type(field_ty).is_some() {
                // Optional entity field: a filter only matches items whose
                // field is Some AND whose inner value satisfies it — None
                // never matches, regardless of what the filter says.
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
        })
        .collect();
    let filter_matches_body = filter_match_terms
        .into_iter()
        .reduce(|acc, term| quote! { (#acc) && (#term) })
        .unwrap_or_else(|| quote! { true });

    let filter_canonicalize_terms: Vec<TokenStream> = filter_fields
        .iter()
        .map(|(field_ident, _field_ty)| {
            quote! {
                #field_ident: self.#field_ident.map(#krate::query::CanonicalFilter::canonicalize)
            }
        })
        .collect();

    let filter_struct = quote! {
        #[derive(Clone, Default, PartialEq, Debug, #serde_path::Serialize, #serde_path::Deserialize)]
        #[derive(#krate::TS)]
        #serde_rename_attr
        pub struct #filter_ident {
            #(#filter_struct_fields),*
        }

        impl #filter_ident {
            pub fn matches(&self, item: &#name) -> bool {
                #filter_matches_body
            }

            /// Normalize every field to canonical form (sorted+deduped `In`,
            /// `In([x])` -> `Eq(x)`, `Range{a,a}` -> `Eq(a)`) so equivalent
            /// filters hash/compare equal for query-cache identity — see
            /// `Get{name}sByFilter`'s manually-written `CacheKey` impl below.
            pub fn canonicalize(self) -> Self {
                Self {
                    #(#filter_canonicalize_terms),*
                }
            }
        }

        #krate::register_ts_export!(#filter_ident);
    };

    let get_by_filter_query = quote! {
        // non_hash_cache_key: skips deriving Hash on the generated struct —
        // XFilter can contain floats/Vec, neither Hash-able, so the default
        // #[derive(Hash)] path myko_query would otherwise take can't apply.
        // manual_cache_key: skips the auto-generated CacheKey impl entirely,
        // since the key must be computed from the CANONICALIZED filter, not
        // the as-constructed one — so e.g. In([b,a]) and In([a,b]) share one
        // query cell (spec §1 canonicalization requirement / acceptance
        // criterion 4) regardless of which order a caller built the filter in.
        #[#krate::myko_non_hash_cache_key]
        #[#krate::myko_manual_cache_key]
        #[#krate::myko_query(#name)]
        pub struct #get_by_filter_ident(pub #filter_ident);

        impl #krate::prelude::CacheKey for #get_by_filter_ident {
            fn cache_key(&self, state: &mut dyn std::hash::Hasher) {
                #krate::cache::write_serde_cache_key(&self.0.clone().canonicalize(), state);
            }
        }

        impl #krate::prelude::QueryHandler for #get_by_filter_ident {
            fn test_entity(ctx: #krate::prelude::QueryTestCtx<Self>) -> bool {
                ctx.query.0.matches(&ctx.item)
            }

            #[cfg(not(target_arch = "wasm32"))]
            fn build_view(
                ctx: #krate::prelude::QueryBuildCellCtx<Self>,
            ) -> Option<#krate::prelude::FilteredCellMap>
            where
                Self: std::marker::Send + std::marker::Sync + 'static,
            {
                // K-bucket union routing for In/Eq on #[belongs_to] fields
                // lands in a follow-up commit (BelongsToSourceIndex union
                // support) — scan fallback here is correct but unindexed.
                None
            }
        }
    };

    // Generate per-entity count result type (e.g., TargetCount, InstanceCount)
    // This avoids the shared CountResult type which causes duplicate imports in TypeScript
    let count_result_ident = format_ident!("{}Count", name_str);

    let count_result_type = quote! {
        #[#krate::myko_report_output]
        pub struct #count_result_ident {
            pub count: usize,
        }
    };

    // Generate CountAll report
    let count_all_report_ident = format_ident!("CountAll{}s", name_str);

    let count_all_report = quote! {
        #[#krate::myko_report(#count_result_ident)]
        pub struct #count_all_report_ident {}

        impl #krate::prelude::ReportHandler for #count_all_report_ident {
            type Output = #count_result_ident;

            fn compute(
                &self,
                ctx: #krate::prelude::ReportContext,
            ) -> impl #krate::prelude::MaterializeDefinite<std::sync::Arc<Self::Output>>
                 {
                use #krate::prelude::MapExt;

                // Query all items and count them. `.size()` returns a bare
                // clone of the map's internal len_cell — unlike entries()/
                // items()/subscribe_diffs, it does NOT keep the map's own
                // CellMapInner (and therefore its store subscription) alive.
                // Capture `source` in the closure below so the returned
                // count cell keeps the whole chain reachable for as long as
                // it's subscribed to — otherwise the source map (and its
                // subscription) can be dropped out from under len_cell,
                // freezing the count.
                let query = #get_all_query_ident {};
                let source = ctx.query_map_by_str(query);
                source.size().map(move |count| {
                    let _keepalive = &source;
                    std::sync::Arc::new(#count_result_ident { count: *count })
                })
            }
        }
    };

    // Generate Count report with partial filter
    let count_report_ident = format_ident!("Count{}s", name_str);

    let count_report = quote! {
        #[#krate::myko_non_hash_cache_key]
        #[#krate::myko_report(#count_result_ident)]
        pub struct #count_report_ident(pub #partial_ident);

        impl #krate::prelude::ReportHandler for #count_report_ident {
            type Output = #count_result_ident;

            fn compute(
                &self,
                ctx: #krate::prelude::ReportContext,
            ) -> impl #krate::prelude::MaterializeDefinite<std::sync::Arc<Self::Output>>
                 {
                use #krate::prelude::MapExt;

                // Query by partial filter and count results. See
                // CountAll's compute (above) for why `source` must be
                // captured here rather than dropped after `.size()`.
                let query = #get_by_partial_ident(self.0.clone());
                let source = ctx.query_map_by_str(query);
                source.size().map(move |count| {
                    let _keepalive = &source;
                    std::sync::Arc::new(#count_result_ident { count: *count })
                })
            }
        }
    };

    // Generate Get{Entity}ById report that returns Option<Arc<Entity>>
    let get_by_id_report_ident = format_ident!("Get{}ById", name_str);

    let get_by_id_report = quote! {
        #[#krate::myko_report(Option<std::sync::Arc<#name>>)]
        pub struct #get_by_id_report_ident {
            pub id: #id_type_ident,
        }

        impl #krate::prelude::ReportHandler for #get_by_id_report_ident {
            type Output = Option<std::sync::Arc<#name>>;

            fn compute(
                &self,
                ctx: #krate::prelude::ReportContext,
            ) -> impl #krate::prelude::MaterializeDefinite<std::sync::Arc<Self::Output>>
                 {
                use #krate::prelude::{MapExt, Eventable};

                let id: std::sync::Arc<str> = self.id.clone().into();
                let store = ctx.registry().get_or_create(#name::ENTITY_NAME_STATIC);
                store.get(&id).map(move |opt| {
                    std::sync::Arc::new(opt.as_ref().map(|any_item| {
                        std::sync::Arc::new(any_item.as_any().downcast_ref::<#name>().expect(
                            concat!("downcast failed in ", stringify!(#get_by_id_report_ident))
                        ).clone())
                    }))
                })
            }
        }
    };

    // Generate Delete{Entity} command (single ID)
    let delete_command_ident = format_ident!("Delete{}", name_str);
    let delete_result_ident = format_ident!("Delete{}Result", name_str);

    let delete_serde_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    let delete_command = quote! {
        /// Result type for Delete command
        #[derive(Clone, PartialEq, #serde_path::Serialize, #serde_path::Deserialize, Debug)]
        #[derive(#krate::TS)]
        #delete_serde_attr
        pub struct #delete_result_ident {
            pub deleted: bool,
        }

        #krate::register_ts_export!(#delete_result_ident);

        /// Command to delete a single entity by ID
        #[#krate::myko_command(#delete_result_ident)]
        pub struct #delete_command_ident {
            pub id: #id_type_ident,
        }

        impl #krate::command::CommandHandler for #delete_command_ident {
            fn execute(
                self,
                ctx: #krate::prelude::CommandContext,
            ) -> Result<#delete_result_ident, #krate::prelude::CommandError> {

                let report = #get_by_id_report_ident { id: self.id.clone() };

                let entity = ctx.exec_report(report)?;

                match entity {
                    Some(e) => {
                        ctx.emit_del(e)?;
                        Ok(#delete_result_ident { deleted: true })
                    }
                    None => Err(#krate::prelude::CommandError {
                        tx: ctx.tx().to_string(),
                        command_id: ctx.command_id.to_string(),
                        message: format!("{} not found: {}", #name_str, self.id),
                    }),
                }
            }
        }
    };

    // Generate Delete{Entity}s command (multiple IDs)
    let delete_many_command_ident = format_ident!("Delete{}s", name_str);
    let delete_many_result_ident = format_ident!("Delete{}sResult", name_str);

    let delete_many_command = quote! {
        /// Result type for bulk Delete command
        #[derive(Clone, PartialEq, #serde_path::Serialize, #serde_path::Deserialize, Debug)]
        #[derive(#krate::TS)]
        #delete_serde_attr
        pub struct #delete_many_result_ident {
            pub deleted_count: usize,
        }

        #krate::register_ts_export!(#delete_many_result_ident);

        /// Command to delete multiple entities by ID
        #[#krate::myko_command(#delete_many_result_ident)]
        pub struct #delete_many_command_ident {
            pub ids: Vec<#id_type_ident>,
        }

        impl #krate::command::CommandHandler for #delete_many_command_ident {
            fn execute(
                self,
                ctx: #krate::prelude::CommandContext,
            ) -> Result<#delete_many_result_ident, #krate::prelude::CommandError> {
                let q = #get_by_ids_query_ident { ids: self.ids.clone() };
                let entities = ctx.exec_query(q)?;
                let deleted_count = entities.len();
                ctx.emit_del_batch(entities.iter().map(|entity| entity.as_ref()))?;

                Ok(#delete_many_result_ident { deleted_count })
            }
        }
    };

    let item_registration = quote! {
        #krate::prelude::ItemRegistration {
            entity_type: #name_str,
            crate_name: module_path!(),
            parse: <#name as #krate::item::Eventable>::parse,
            parse_bytes: <#name as #krate::item::Eventable>::parse_bytes,
            // Typed JSON serialize shim: downcast once, then call typed
            // `to_raw_value` so the inner field-by-field serialize is
            // monomorphized — no erased_serde on the JSON hot path.
            serialize_json: |any| {
                let typed = any
                    .as_any()
                    .downcast_ref::<#name>()
                    .expect("ItemRegistration::serialize_json: entity_type/type mismatch");
                #krate::serde_json::value::to_raw_value(typed)
            },
        }
    };

    // Generate relationship registrations
    let relationship_registrations = relationship::generate_registrations(&name_str, &rel_info);

    let mut field_types = HashMap::<String, syn::Type>::new();
    if let syn::Fields::Named(FieldsNamed { named, .. }) = &input_struct.fields {
        for field in named {
            if let Some(field_ident) = &field.ident {
                field_types.insert(field_ident.to_string(), field.ty.clone());
            }
        }
    }

    // Generate HasForeignKey impls from #[belongs_to] / #[ensure_for] fields.
    // Deduplicate by foreign parent type so a field tagged with both
    // #[belongs_to(X)] and #[ensure_for(X)] yields a single impl.
    let mut fk_impl_parents = BTreeSet::<String>::new();
    let mut has_foreign_key_impls: Vec<TokenStream> = Vec::new();

    for bt in &rel_info.belongs_to {
        let Some(field_ty) = field_types.get(&bt.field_name) else {
            continue;
        };
        let field_ident = format_ident!("{}", bt.field_name);
        let foreign_ty_ident = format_ident!("{}", bt.foreign_type);
        if fk_impl_parents.insert(bt.foreign_type.clone()) {
            has_foreign_key_impls.push(quote! {
                impl #krate::hyphae::HasForeignKey<#foreign_ty_ident> for #name
                where
                    #field_ty: #krate::hyphae::IdFor<#foreign_ty_ident>,
                {
                    type ForeignKey = #field_ty;

                    fn fk(&self) -> Self::ForeignKey {
                        self.#field_ident.clone()
                    }
                }
            });
        }
    }

    for ef in &rel_info.ensure_for_fields {
        let Some(field_ty) = field_types.get(&ef.field_name) else {
            continue;
        };
        let field_ident = format_ident!("{}", ef.field_name);
        let foreign_ty_ident = format_ident!("{}", ef.foreign_type);
        if fk_impl_parents.insert(ef.foreign_type.clone()) {
            has_foreign_key_impls.push(quote! {
                impl #krate::hyphae::HasForeignKey<#foreign_ty_ident> for #name
                where
                    #field_ty: #krate::hyphae::IdFor<#foreign_ty_ident>,
                {
                    type ForeignKey = #field_ty;

                    fn fk(&self) -> Self::ForeignKey {
                        self.#field_ident.clone()
                    }
                }
            });
        }
    }

    // Generate setter commands for fields with #[myko_rename] or #[myko_setter]
    let setter_commands = setter::generate_setter_commands(&name_str, &setter_fields);

    // Generate server_owner / bake_server_owner overrides for #[server_owned] fields
    let server_owned_impls = if let Some(ref so) = rel_info.server_owned_field {
        let field_ident = format_ident!("{}", so.field_name);
        quote! {
            fn server_owner(&self) -> Option<&str> {
                let s: &str = &self.#field_ident;
                if s.is_empty() { None } else { Some(s) }
            }

            fn bake_server_owner(&self, server_id: &str) -> Option<std::sync::Arc<dyn #krate::prelude::AnyItem>> {
                let mut patched = self.clone();
                patched.#field_ident = server_id.to_string().into();
                Some(std::sync::Arc::new(patched))
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {

        use #krate::prelude::Query;
        use #krate::hyphae::MapExt as _HyphaMapExt;

        #[derive(
            Clone,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            #serde_path::Serialize,
            #serde_path::Deserialize,
            Debug,
        )]
        #[derive(#krate::TS)]
        #[ts(type = "string")]
        pub struct #id_type_ident(pub std::sync::Arc<str>);

        impl std::ops::Deref for #id_type_ident {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                self.0.as_ref()
            }
        }

        impl std::fmt::Display for #id_type_ident {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(self.0.as_ref(), f)
            }
        }

        impl AsRef<str> for #id_type_ident {
            fn as_ref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl From<std::sync::Arc<str>> for #id_type_ident {
            fn from(value: std::sync::Arc<str>) -> Self {
                Self(value)
            }
        }

        impl From<#id_type_ident> for std::sync::Arc<str> {
            fn from(value: #id_type_ident) -> Self {
                value.0
            }
        }

        impl From<String> for #id_type_ident {
            fn from(value: String) -> Self {
                Self(std::sync::Arc::<str>::from(value))
            }
        }

        impl From<&str> for #id_type_ident {
            fn from(value: &str) -> Self {
                Self(std::sync::Arc::<str>::from(value))
            }
        }

        impl #krate::hyphae::IdFor<#name> for #id_type_ident {
            type MapKey = std::sync::Arc<str>;

            fn map_key(&self) -> Self::MapKey {
                self.0.clone()
            }
        }
        impl #krate::hyphae::IdType for #id_type_ident {
            type Parent = #name;
        }
        impl #krate::query::Filterable for #id_type_ident {
            type Filter = #krate::query::IdFilter<#id_type_ident>;
        }

        #derives
        #input_struct
        #post_deserialize_impl

        // Register for ts-rs export
        #krate::register_ts_export!(#id_type_ident, #name, #partial_ident);

        #krate::submit! {
            #item_registration
        }
        #ingest_buffer_registration

        impl #krate::item::Eventable for #name {
            const ENTITY_NAME_STATIC: &'static str = #name_str;
        }

        impl #krate::prelude::AnyItem for #name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn entity_type(&self) -> &'static str {
                #name_str
            }

            fn equals(&self, other: &dyn #krate::prelude::AnyItem) -> bool {
                other
                    .as_any()
                    .downcast_ref::<Self>()
                    .map(|typed| self == typed)
                    .unwrap_or(false)
            }

            #server_owned_impls
        }

        impl #krate::prelude::WithId for #name {
            fn id(&self) -> std::sync::Arc<str> {
                self.id.clone().into()
            }
        }

        impl #krate::common::with_id::WithTypedId for #name {
            type Id = #id_type_ident;

            fn typed_id(&self) -> Self::Id {
                self.id.clone().into()
            }
        }

        #(#has_foreign_key_impls)*

        // ToValue is implemented via blanket impl for all Serialize types

        #get_all_query

        #get_by_ids_query

        #get_by_partial_query

        #filter_struct

        #get_by_filter_query

        #count_result_type

        #count_all_report

        #count_report

        #get_by_id_report

        #delete_command

        #delete_many_command

        // Setter commands (from #[myko_rename] and #[myko_setter] field attributes)
        #setter_commands

        // Relationship registrations (belongs_to, owns_many, ensure_for)
        #relationship_registrations

        // Note: Auto-generated queries (GetAll*, Get*sByIds, Get*sByQuery) and reports
        // (CountAll*, Count*, Get*ById) are registered via their #[myko_query] and
        // #[myko_report] macro attributes which emit inventory registrations.

    };

    expanded
}
