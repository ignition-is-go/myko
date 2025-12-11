use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Field, FieldsNamed, ItemStruct};

pub fn myko_item_impl(mut input_struct: ItemStruct) -> TokenStream {
    let name = &input_struct.ident;

    let name_str = name.to_string();

    if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
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
        #[derive(Partial, PartialEq, Clone, Serialize, Deserialize, Debug, myko_rs::TS)]
        #[ts(export)]
        #[serde(rename_all = "camelCase")]
        #[partially(derive(Clone, Serialize, Deserialize, Debug, Default, myko_macros::PartialMatches, myko_rs::TS))]
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
         pub struct #get_by_partial_ident {
             pub partial: #partial_ident
         }

         impl myko_rs::prelude::QueryHandler for #get_by_partial_ident {
             fn test_entity(ctx: myko_rs::prelude::QueryHandlerCtx<Self>) -> bool {
                 ctx.query.partial.matches(&ctx.item)
             }
         }

    };

    // Generate per-entity count result type (e.g., TargetCount, InstanceCount)
    // This avoids the shared CountResult type which causes duplicate imports in TypeScript
    let count_result_ident = format_ident!("{}Count", name_str);

    let count_result_type = quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, myko_rs::TS)]
        #[serde(rename_all = "camelCase")]
        #[ts(export)]
        pub struct #count_result_ident {
            pub count: usize,
        }
    };

    // Generate CountAll report
    let count_all_report_ident = format_ident!("CountAll{}s", name_str);
    let get_all_args_ident = format_ident!("{}Args", get_all_query_ident);

    let count_all_report = quote! {
        #[myko_macros::myko_report(#count_result_ident)]
        pub struct #count_all_report_ident {}

        impl myko_rs::prelude::ReportHandler for #count_all_report_ident {
            type Output = #count_result_ident;

            fn compute(ctx: myko_rs::prelude::ReportContext) -> std::pin::Pin<Box<dyn futures::Stream<Item = Self::Output> + Send>> {
                use futures::StreamExt;

                let query = #get_all_query_ident::new(#get_all_args_ident {});
                let stream = ctx.query(query);

                Box::pin(stream.map(|items| #count_result_ident { count: items.len() }))
            }
        }
    };

    // Generate Count report with partial filter
    let count_report_ident = format_ident!("Count{}s", name_str);
    let count_report_args_ident = format_ident!("Count{}sArgs", name_str);
    let get_by_partial_args_ident = format_ident!("{}Args", get_by_partial_ident);

    let count_report = quote! {
        #[myko_macros::myko_report(#count_result_ident)]
        pub struct #count_report_ident {
            pub partial: #partial_ident,
        }

        impl myko_rs::prelude::ReportHandler for #count_report_ident {
            type Output = #count_result_ident;

            fn compute(ctx: myko_rs::prelude::ReportContext) -> std::pin::Pin<Box<dyn futures::Stream<Item = Self::Output> + Send>> {
                use futures::StreamExt;

                let args: #count_report_args_ident = ctx.args().expect("Failed to parse count report args");
                let partial = args.partial;

                let query = #get_by_partial_ident::new(#get_by_partial_args_ident { partial });
                let stream = ctx.query(query);

                Box::pin(stream.map(|items| #count_result_ident { count: items.len() }))
            }
        }
    };

    // Generate Get{Entity}ById report that returns Option<Entity>
    let get_by_id_report_ident = format_ident!("Get{}ById", name_str);
    let get_by_id_report_args_ident = format_ident!("Get{}ByIdArgs", name_str);
    let get_by_ids_args_ident = format_ident!("{}Args", get_by_ids_query_ident);

    let get_by_id_report = quote! {
        #[myko_macros::myko_report(Option<#name>)]
        pub struct #get_by_id_report_ident {
            pub id: std::sync::Arc<str>,
        }

        impl myko_rs::prelude::ReportHandler for #get_by_id_report_ident {
            type Output = Option<#name>;

            fn compute(ctx: myko_rs::prelude::ReportContext) -> std::pin::Pin<Box<dyn futures::Stream<Item = Self::Output> + Send>> {
                use futures::StreamExt;

                let args: #get_by_id_report_args_ident = ctx.args().expect("Failed to parse get by id report args");
                let id = args.id;

                let query = #get_by_ids_query_ident::new(#get_by_ids_args_ident { ids: vec![id] });
                let stream = ctx.query(query);

                Box::pin(stream.map(|items| items.into_iter().next()))
            }
        }
    };

    let item_registration = quote! {
        myko_rs::prelude::ItemRegistration {
            entity_type: #name_str,
            crate_name: module_path!(),
        }
    };

    let expanded = quote! {

        use myko_rs::prelude::Query;

        #derives
        #input_struct


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

        impl myko_rs::prelude::MykoAutoQueries for #name {
                fn register_auto(server: &std::sync::Arc<myko_rs::prelude::MykoServer>) -> Result<(), anyhow::Error>{

                        #get_all_query_ident::register(&server)?;
                        #get_by_ids_query_ident::register(&server)?;
                        #get_by_partial_ident::register(&server)?;
                     Ok(())
                }
        }

        impl myko_rs::prelude::MykoAutoReports for #name {
                fn register_auto(server: &std::sync::Arc<myko_rs::prelude::MykoServer>) -> Result<(), anyhow::Error>{
                        use myko_rs::prelude::Report;
                        #count_all_report_ident::register(&server)?;
                        #count_report_ident::register(&server)?;
                        #get_by_id_report_ident::register(&server)?;
                     Ok(())
                }
        }

    };

    expanded
}
