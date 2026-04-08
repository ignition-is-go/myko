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

    if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
        // Strip relationship and setter attributes from each field
        for field in named.iter_mut() {
            relationship::strip_relationship_attrs(field);
            setter::strip_setter_attrs(field);
        }

        let id = quote! { id };
        let pub_viz = quote! { pub };

        let id_field: Field = syn::parse_quote! {
            #pub_viz #id: #id_type_ident
        };

        named.push(id_field);
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
    let derives = if !rel_info.ensure_for_fields.is_empty() {
        quote! {
            #[derive(Default, #partially_path::Partial, PartialEq, Clone, #serde_path::Serialize, #deserialize_derive Debug, #krate::TS)]
            #serde_rename_attr
            #[partially(#partially_crate_attr derive(Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, Default, myko_macros::PartialMatches, #krate::TS), attribute(ts(optional_fields)))]
        }
    } else {
        quote! {
            #[derive(#partially_path::Partial, PartialEq, Clone, #serde_path::Serialize, #deserialize_derive Debug, #krate::TS)]
            #serde_rename_attr
            #[partially(#partially_crate_attr derive(Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, Default, myko_macros::PartialMatches, #krate::TS), attribute(ts(optional_fields)))]
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
            #[derive(#serde_path::Deserialize, #krate::TS)]
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

        #[myko_macros::myko_query(#name)]
        pub struct #get_all_query_ident {}

        impl #krate::prelude::QueryHandler for #get_all_query_ident {
            fn test_entity(ctx: #krate::prelude::QueryTestCtx<Self>) -> bool {
                true
            }
        }

    };

    let get_by_ids_query_ident = format_ident!("Get{}sByIds", name_str);

    let get_by_ids_query = quote! {
        #[myko_macros::myko_query(#name)]
        pub struct #get_by_ids_query_ident {
            pub ids: Vec<#id_type_ident>,
        }

        impl #krate::prelude::QueryHandler for #get_by_ids_query_ident {
            fn test_entity(ctx: #krate::prelude::QueryTestCtx<Self>) -> bool {
                ctx.query.ids.contains(&ctx.item.id.clone().into())
            }
        }

    };

    let get_by_partial_ident = format_ident!("Get{}sByQuery", name_str);
    let partial_ident = format_ident!("Partial{}", name_str);

    let belongs_to_fast_paths: Vec<TokenStream> = rel_info
        .belongs_to
        .iter()
        .filter(|bt| !bt.is_optional)
        .map(|bt| {
            let field_ident = format_ident!("{}", bt.field_name);
            let field_name = bt.field_name.clone();
            quote! {
                if let Some(fk) = ctx.query.0.#field_ident.clone() {
                    let source = #krate::query::build_belongs_to_source_map(
                        ctx.query_context.registry(),
                        ctx.query_context.request_ctx.host_id,
                        #name_str,
                        #field_name,
                        |item: &dyn std::any::Any| -> Option<std::sync::Arc<str>> {
                            item.downcast_ref::<#name>()
                                .map(|e| std::sync::Arc::<str>::from(e.#field_ident.as_ref()))
                        },
                        std::sync::Arc::<str>::from(fk.as_ref()),
                    );
                    return Some(#krate::query::filter_query_over_source::<#get_by_partial_ident>(
                        source,
                        ctx.query.clone(),
                        ctx.query_context.query_context.clone(),
                    ));
                }
            }
        })
        .collect();

    let get_by_partial_query = quote! {
        #[myko_macros::myko_non_hash_cache_key]
        #[myko_macros::myko_query(#name)]
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

    // Generate per-entity count result type (e.g., TargetCount, InstanceCount)
    // This avoids the shared CountResult type which causes duplicate imports in TypeScript
    let count_result_ident = format_ident!("{}Count", name_str);

    let count_result_type = quote! {
        #[myko_macros::myko_report_output]
        pub struct #count_result_ident {
            pub count: usize,
        }
    };

    // Generate CountAll report
    let count_all_report_ident = format_ident!("CountAll{}s", name_str);

    let count_all_report = quote! {
        #[myko_macros::myko_report(#count_result_ident)]
        pub struct #count_all_report_ident {}

        impl #krate::prelude::ReportHandler for #count_all_report_ident {
            type Output = #count_result_ident;

            fn compute(&self, ctx: #krate::prelude::ReportContext) -> #krate::prelude::Cell<std::sync::Arc<Self::Output>, #krate::prelude::CellImmutable> {
                use #krate::prelude::MapExt;

                // Query all items and count them
                let query = #get_all_query_ident {};
                ctx.query_map_by_str(query)
                    .size()
                    .map(|count| std::sync::Arc::new(#count_result_ident { count: *count }))
            }
        }
    };

    // Generate Count report with partial filter
    let count_report_ident = format_ident!("Count{}s", name_str);

    let count_report = quote! {
        #[myko_macros::myko_non_hash_cache_key]
        #[myko_macros::myko_report(#count_result_ident)]
        pub struct #count_report_ident(pub #partial_ident);

        impl #krate::prelude::ReportHandler for #count_report_ident {
            type Output = #count_result_ident;

            fn compute(&self, ctx: #krate::prelude::ReportContext) -> #krate::prelude::Cell<std::sync::Arc<Self::Output>, #krate::prelude::CellImmutable> {
                use #krate::prelude::MapExt;

                // Query by partial filter and count results
                let query = #get_by_partial_ident(self.0.clone());
                ctx.query_map_by_str(query)
                    .size()
                    .map(|count| std::sync::Arc::new(#count_result_ident { count: *count }))
            }
        }
    };

    // Generate Get{Entity}ById report that returns Option<Arc<Entity>>
    let get_by_id_report_ident = format_ident!("Get{}ById", name_str);

    let get_by_id_report = quote! {
        #[myko_macros::myko_report(Option<std::sync::Arc<#name>>)]
        pub struct #get_by_id_report_ident {
            pub id: #id_type_ident,
        }

        impl #krate::prelude::ReportHandler for #get_by_id_report_ident {
            type Output = Option<std::sync::Arc<#name>>;

            fn compute(&self, ctx: #krate::prelude::ReportContext) -> #krate::prelude::Cell<std::sync::Arc<Self::Output>, #krate::prelude::CellImmutable> {
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
        #[derive(Clone, PartialEq, #serde_path::Serialize, #serde_path::Deserialize, Debug, #krate::TS)]
        #delete_serde_attr
        pub struct #delete_result_ident {
            pub deleted: bool,
        }

        #krate::register_ts_export!(#delete_result_ident);

        /// Command to delete a single entity by ID
        #[myko_macros::myko_command(#delete_result_ident)]
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
        #[derive(Clone, PartialEq, #serde_path::Serialize, #serde_path::Deserialize, Debug, #krate::TS)]
        #delete_serde_attr
        pub struct #delete_many_result_ident {
            pub deleted_count: usize,
        }

        #krate::register_ts_export!(#delete_many_result_ident);

        /// Command to delete multiple entities by ID
        #[myko_macros::myko_command(#delete_many_result_ident)]
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
            #krate::TS
        )]
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
