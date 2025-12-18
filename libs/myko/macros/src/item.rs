use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Field, FieldsNamed, ItemStruct};

use crate::relationship;

pub fn myko_item_impl(mut input_struct: ItemStruct) -> TokenStream {
    // Collect relationship information BEFORE stripping attributes
    let rel_info = relationship::collect_relationships(&input_struct);

    let name = &input_struct.ident;
    let name_str = name.to_string();

    if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
        // Strip relationship attributes from each field
        for field in named.iter_mut() {
            relationship::strip_relationship_attrs(field);
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

    let derives = quote! {
        #[derive(partially::Partial, PartialEq, Clone, serde::Serialize, serde::Deserialize, Debug, myko_rs::TS)]
        #[serde(rename_all = "camelCase")]
        #[partially(derive(Clone, serde::Serialize, serde::Deserialize, Debug, Default, myko_macros::PartialMatches, myko_rs::TS), attribute(ts(optional_fields)))]
    };

    let get_all_query_ident = format_ident!("GetAll{}s", name_str);

    let get_all_query = quote! {

        #[myko_macros::myko_query(#name)]
        pub struct #get_all_query_ident {}


        impl myko_rs::prelude::QueryHandler for #get_all_query_ident {
            fn test_entity(ctx: myko_rs::prelude::QueryHandlerCtx<Self>) -> bool {
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


        impl myko_rs::prelude::QueryHandler for #get_by_ids_query_ident {
            fn test_entity(ctx: myko_rs::prelude::QueryHandlerCtx<Self>) -> bool {
                ctx.query.ids.contains(&ctx.item.id)
            }
        }
    };

    let get_by_partial_ident = format_ident!("Get{}sByQuery", name_str);
    let partial_ident = format_ident!("Partial{}", name_str);

    let get_by_partial_query = quote! {
        #[myko_macros::myko_query(#name)]
         pub struct #get_by_partial_ident(pub #partial_ident);

         impl myko_rs::prelude::QueryHandler for #get_by_partial_ident {
             fn test_entity(ctx: myko_rs::prelude::QueryHandlerCtx<Self>) -> bool {
                 ctx.query.0.matches(&ctx.item)
             }
         }

    };

    // Generate per-entity count result type (e.g., TargetCount, InstanceCount)
    // This avoids the shared CountResult type which causes duplicate imports in TypeScript
    let count_result_ident = format_ident!("{}Count", name_str);

    let count_result_type = quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, myko_rs::TS)]
        #[serde(rename_all = "camelCase")]
        pub struct #count_result_ident {
            pub count: usize,
        }

        myko_rs::register_ts_export!(#count_result_ident);
    };

    // Generate CountAll report
    let count_all_report_ident = format_ident!("CountAll{}s", name_str);

    let count_all_report = quote! {
        #[myko_macros::myko_report(#count_result_ident)]
        pub struct #count_all_report_ident {}

        impl myko_rs::prelude::ReportHandler for #count_all_report_ident {
            type Output = #count_result_ident;

            fn compute(ctx: myko_rs::prelude::ReportContext) -> std::pin::Pin<Box<dyn futures::Stream<Item = Self::Output> + Send>> {
                use futures::StreamExt;

                // Use bare query params - ctx.query() wraps them automatically
                let query = #get_all_query_ident {};
                let stream = ctx.query(query);

                Box::pin(stream.map(|items| #count_result_ident { count: items.len() }))
            }
        }
    };

    // Generate Count report with partial filter
    let count_report_ident = format_ident!("Count{}s", name_str);

    let count_report = quote! {
        #[myko_macros::myko_report(#count_result_ident)]
        pub struct #count_report_ident(pub #partial_ident);

        impl myko_rs::prelude::ReportHandler for #count_report_ident {
            type Output = #count_result_ident;

            fn compute(ctx: myko_rs::prelude::ReportContext) -> std::pin::Pin<Box<dyn futures::Stream<Item = Self::Output> + Send>> {
                use futures::StreamExt;

                // Parse the report params (which are the args now - no separate Args type)
                let params: #count_report_ident = ctx.args().expect("Failed to parse count report params");

                // Use bare query params - ctx.query() wraps them automatically
                let query = #get_by_partial_ident(params.0);
                let stream = ctx.query(query);

                Box::pin(stream.map(|items| #count_result_ident { count: items.len() }))
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

        impl myko_rs::prelude::ReportHandler for #get_by_id_report_ident {
            type Output = Option<#name>;

            fn compute(ctx: myko_rs::prelude::ReportContext) -> std::pin::Pin<Box<dyn futures::Stream<Item = Self::Output> + Send>> {
                use futures::StreamExt;

                // Parse the report params (which are the args now - no separate Args type)
                let params: #get_by_id_report_ident = ctx.args().expect("Failed to parse get by id report params");
                let id = params.id;

                // Use bare query params - ctx.query() wraps them automatically
                let query = #get_by_ids_query_ident { ids: vec![id] };
                let stream = ctx.query(query);

                Box::pin(stream.map(|items| items.into_iter().next()))
            }
        }
    };

    // Generate Delete{Entity} command (single ID)
    let delete_command_ident = format_ident!("Delete{}", name_str);
    let delete_command_handler_ident = format_ident!("Delete{}Handler", name_str);
    let delete_result_ident = format_ident!("Delete{}Result", name_str);

    let delete_command = quote! {
        /// Result type for Delete command
        #[derive(Clone, serde::Serialize, serde::Deserialize, Debug, myko_rs::TS)]
        #[serde(rename_all = "camelCase")]
        pub struct #delete_result_ident {
            pub deleted: bool,
        }

        myko_rs::register_ts_export!(#delete_result_ident);

        /// Command to delete a single entity by ID
        #[myko_macros::myko_command(#delete_result_ident)]
        pub struct #delete_command_ident {
            pub id: std::sync::Arc<str>,
        }

        impl #delete_command_handler_ident {
            pub async fn execute(
                cmd: #delete_command_ident,
                ctx: myko_rs::prelude::CommandContext,
            ) -> Result<#delete_result_ident, myko_rs::prelude::CommandError> {
                // Use bare query params - ctx.query_one wraps them automatically
                let query = #get_by_ids_query_ident { ids: vec![cmd.id.clone()] };

                let entity = ctx.query_one(&query).await?;

                match entity {
                    Some(e) => {
                        ctx.emit_del(&e)?;
                        Ok(#delete_result_ident { deleted: true })
                    }
                    None => Err(myko_rs::prelude::CommandError {
                        tx: ctx.tx().to_string(),
                        message: format!("{} not found: {}", #name_str, cmd.id),
                    }),
                }
            }
        }
    };

    // Generate Delete{Entity}s command (multiple IDs)
    let delete_many_command_ident = format_ident!("Delete{}s", name_str);
    let delete_many_command_handler_ident = format_ident!("Delete{}sHandler", name_str);
    let delete_many_result_ident = format_ident!("Delete{}sResult", name_str);

    let delete_many_command = quote! {
        /// Result type for bulk Delete command
        #[derive(Clone, serde::Serialize, serde::Deserialize, Debug, myko_rs::TS)]
        #[serde(rename_all = "camelCase")]
        pub struct #delete_many_result_ident {
            pub deleted_count: usize,
        }

        myko_rs::register_ts_export!(#delete_many_result_ident);

        /// Command to delete multiple entities by ID
        #[myko_macros::myko_command(#delete_many_result_ident)]
        pub struct #delete_many_command_ident {
            pub ids: Vec<std::sync::Arc<str>>,
        }

        impl #delete_many_command_handler_ident {
            pub async fn execute(
                cmd: #delete_many_command_ident,
                ctx: myko_rs::prelude::CommandContext,
            ) -> Result<#delete_many_result_ident, myko_rs::prelude::CommandError> {
                let mut deleted_count = 0;

                // For bulk delete, we iterate through the provided IDs and delete each found entity
                for id in &cmd.ids {
                    // Use bare query params - ctx.query_one wraps them automatically
                    let single_query = #get_by_ids_query_ident { ids: vec![id.clone()] };

                    if let Some(entity) = ctx.query_one(&single_query).await? {
                        ctx.emit_del(&entity)?;
                        deleted_count += 1;
                    }
                }

                Ok(#delete_many_result_ident { deleted_count })
            }
        }
    };

    let item_registration = quote! {
        myko_rs::prelude::ItemRegistration {
            entity_type: #name_str,
            crate_name: module_path!(),
            factory: || -> myko_rs::item::RegisterItemData {
                <#name as myko_rs::item::Eventable>::create_registration()
            },
        }
    };

    // Generate relationship registrations
    let relationship_registrations = relationship::generate_registrations(&name_str, &rel_info);

    let expanded = quote! {

        use myko_rs::prelude::Query;

        #derives
        #input_struct

        // Register for ts-rs export
        myko_rs::register_ts_export!(#name, #partial_ident);

        myko_rs::submit! {
            #item_registration
        }

        impl myko_rs::item::Eventable for #name {
            fn entity_name(&self) -> String {
                #name_str.to_string()
            }

            fn entity_name_static() -> String {
                #name_str.to_string()
            }
        }

        impl myko_rs::prelude::AnyItem for #name {}

        impl myko_rs::prelude::WithId for #name {
            fn id(&self) -> std::sync::Arc<str> {
                self.id.clone()
            }
        }

        impl myko_rs::prelude::ToValue for #name {
            fn to_value(&self) -> serde_json::Value {
                serde_json::to_value(self).expect("Failed to serialize")
            }
        }

        #get_all_query

        #get_by_ids_query

        #get_by_partial_query

        #count_result_type

        #count_all_report

        #count_report

        #get_by_id_report

        #delete_command

        #delete_many_command

        // Relationship registrations (belongs_to, owns_many, ensure_for)
        #relationship_registrations

    };

    expanded
}
