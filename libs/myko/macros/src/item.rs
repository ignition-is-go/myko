use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Field, FieldsNamed, ItemStruct};

use crate::{DeriveCtx, relationship, setter};

pub fn myko_item_impl(mut input_struct: ItemStruct) -> TokenStream {
    // Collect relationship information BEFORE stripping attributes
    let rel_info = relationship::collect_relationships(&input_struct);

    // Collect setter fields BEFORE stripping attributes
    let setter_fields = setter::collect_setter_fields(&input_struct);

    let name = &input_struct.ident;
    let name_str = name.to_string();

    let ctx = DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let partially_path = &ctx.partially_path;

    if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
        // Strip relationship and setter attributes from each field
        for field in named.iter_mut() {
            relationship::strip_relationship_attrs(field);
            setter::strip_setter_attrs(field);
        }

        let id = quote! { id };
        let arc_str = quote! { std::sync::Arc<str> };
        let pub_viz = quote! { pub };

        let hash = quote! { hash };

        let id_field: Field = syn::parse_quote! {
            #pub_viz #id: #arc_str
        };

        let mut hash_field: Field = syn::parse_quote! {
            #pub_viz #hash: #arc_str
        };

        hash_field.attrs.push(syn::parse_quote! {
            #[serde(default)]
        });

        named.push(id_field);
        named.push(hash_field);
    };

    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    let partially_crate_attr = match &ctx.partially_crate_attr {
        Some(s) => quote!(crate = #s,),
        None => quote!(),
    };

    // Add Default derive if entity has ensure_for relationships (needed for make_entity)
    // NOTE(ts): `partially` forwards all container attributes (including #[serde(crate = ...)])
    // from the main struct to the Partial struct automatically, so we must NOT also add
    // `attribute(serde(crate = ...))` — that would cause "duplicate serde attribute `crate`".
    let derives = if !rel_info.ensure_for_fields.is_empty() {
        quote! {
            #[derive(Default, #partially_path::Partial, PartialEq, Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, #krate::TS)]
            #serde_rename_attr
            #[partially(#partially_crate_attr derive(Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, Default, myko_macros::PartialMatches, #krate::TS), attribute(ts(optional_fields)))]
        }
    } else {
        quote! {
            #[derive(#partially_path::Partial, PartialEq, Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, #krate::TS)]
            #serde_rename_attr
            #[partially(#partially_crate_attr derive(Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, Default, myko_macros::PartialMatches, #krate::TS), attribute(ts(optional_fields)))]
        }
    };

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
            pub ids: Vec<std::sync::Arc<str>>,
        }

        impl #krate::prelude::QueryHandler for #get_by_ids_query_ident {
            fn test_entity(ctx: #krate::prelude::QueryTestCtx<Self>) -> bool {
                ctx.query.ids.contains(&ctx.item.id)
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

            fn compute(&self, ctx: #krate::prelude::ReportContext) -> #krate::prelude::Cell<Self::Output, #krate::prelude::CellImmutable> {
                use #krate::prelude::MapExt;

                // Query all items and count them
                let query = #get_all_query_ident {};
                ctx.query(query).map(|items| #count_result_ident { count: items.len() })
            }
        }
    };

    // Generate Count report with partial filter
    let count_report_ident = format_ident!("Count{}s", name_str);

    let count_report = quote! {
        #[myko_macros::myko_report(#count_result_ident)]
        pub struct #count_report_ident(pub #partial_ident);

        impl #krate::prelude::ReportHandler for #count_report_ident {
            type Output = #count_result_ident;

            fn compute(&self, ctx: #krate::prelude::ReportContext) -> #krate::prelude::Cell<Self::Output, #krate::prelude::CellImmutable> {
                use #krate::prelude::MapExt;

                // Query by partial filter and count results
                let query = #get_by_partial_ident(self.0.clone());
                ctx.query(query).map(|items| #count_result_ident { count: items.len() })
            }
        }
    };

    // Generate Get{Entity}ById report that returns Option<Entity>
    let get_by_id_report_ident = format_ident!("Get{}ById", name_str);

    let get_by_id_report = quote! {
        #[myko_macros::myko_report(Option<#name>)]
        pub struct #get_by_id_report_ident {
            pub id: std::sync::Arc<str>,
        }

        impl #krate::prelude::ReportHandler for #get_by_id_report_ident {
            type Output = Option<#name>;

            fn compute(&self, ctx: #krate::prelude::ReportContext) -> #krate::prelude::Cell<Self::Output, #krate::prelude::CellImmutable> {
                use #krate::prelude::MapExt;

                let id = self.id.clone();

                // Query by ID and return the first match (clone from reference)
                let query = #get_by_ids_query_ident { ids: vec![id] };
                ctx.query(query).map(|items| items.first().cloned())
            }
        }
    };

    // Generate Delete{Entity} command (single ID)
    let delete_command_ident = format_ident!("Delete{}", name_str);
    let delete_result_ident = format_ident!("Delete{}Result", name_str);

    let delete_serde_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    let delete_command = quote! {
        /// Result type for Delete command
        #[derive(Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, #krate::TS)]
        #delete_serde_attr
        pub struct #delete_result_ident {
            pub deleted: bool,
        }

        #krate::register_ts_export!(#delete_result_ident);

        /// Command to delete a single entity by ID
        #[myko_macros::myko_command(#delete_result_ident)]
        pub struct #delete_command_ident {
            pub id: std::sync::Arc<str>,
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
                        ctx.emit_del(&e)?;
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
        #[derive(Clone, #serde_path::Serialize, #serde_path::Deserialize, Debug, #krate::TS)]
        #delete_serde_attr
        pub struct #delete_many_result_ident {
            pub deleted_count: usize,
        }

        #krate::register_ts_export!(#delete_many_result_ident);

        /// Command to delete multiple entities by ID
        #[myko_macros::myko_command(#delete_many_result_ident)]
        pub struct #delete_many_command_ident {
            pub ids: Vec<std::sync::Arc<str>>,
        }

        impl #krate::command::CommandHandler for #delete_many_command_ident {
            fn execute(
                self,
                ctx: #krate::prelude::CommandContext,
            ) -> Result<#delete_many_result_ident, #krate::prelude::CommandError> {
                let mut deleted_count = 0;


                let q = #get_by_ids_query_ident { ids: self.ids.clone() };

                let entities = ctx.exec_query(q)?;

                for entity in entities {
                    ctx.emit_del(&entity)?;
                    deleted_count += 1;
                }

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

    // Generate setter commands for fields with #[myko_rename] or #[myko_setter]
    let setter_commands = setter::generate_setter_commands(&name_str, &setter_fields);

    let expanded = quote! {

        use #krate::prelude::Query;
        use #krate::hypha::MapExt as _HyphaMapExt;

        #derives
        #input_struct

        // Register for ts-rs export
        #krate::register_ts_export!(#name, #partial_ident);

        #krate::submit! {
            #item_registration
        }

        impl #krate::item::Eventable for #name {
            fn entity_name_static() -> &'static str {
                #name_str
            }
        }

        impl #krate::prelude::AnyItem for #name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn entity_type(&self) -> &'static str {
                #name_str
            }
        }

        impl #krate::prelude::WithId for #name {
            fn id(&self) -> std::sync::Arc<str> {
                self.id.clone()
            }
        }

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
