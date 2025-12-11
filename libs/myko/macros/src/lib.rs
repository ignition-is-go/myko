use proc_macro::TokenStream;

use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Field, Fields, FieldsNamed, parse_macro_input};

#[proc_macro_derive(PartialMatches)]
pub fn derive_partial_matches(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Extract the base struct name (remove "Partial" prefix)
    let base_name = if let Some(stripped) = name.to_string().strip_prefix("Partial") {
        syn::Ident::new(stripped, name.span())
    } else {
        panic!("PartialMatches can only be derived on structs with 'Partial' prefix");
    };

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("PartialMatches only works on structs with named fields"),
        },
        _ => panic!("PartialMatches only works on structs"),
    };

    // Generate match checks for each field
    let field_checks = fields.iter().map(|f| {
        let field_name = &f.ident;
        quote! {
            if let Some(ref value) = self.#field_name {
                if value != &item.#field_name {
                    return false;
                }
            }
        }
    });

    let expanded = quote! {
        impl #name {
            pub fn matches(&self, item: &#base_name) -> bool {
                #(#field_checks)*
                true
            }
        }
    };

    TokenStream::from(expanded)
}

/// implements a number of traits automatically, as well as adds
///
/// `pub id: Arc<str>`
///
/// `pub hash: Arc<str>`
///
/// Derives:
///
/// `Partial, PartialEq, Clone, Serialize, Deserialize, Debug`
///
/// Derives for Partial:
///
/// `Clone, Serialize, Deserialize, Default`
///
/// all fields added manually must implement at least `Clone, Serialize, Deserialize`
///
#[proc_macro_attribute]
pub fn myko_item(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut input_struct = parse_macro_input!(input as ItemStruct);
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
                     Ok(())
                }
        }

    };

    expanded.into()
}

use syn::{ItemStruct, Path};

#[proc_macro_attribute]
pub fn myko_query(attr: TokenStream, input: TokenStream) -> TokenStream {
    // Parse the single argument (e.g., `File`) from the attribute
    let query_item_type: Path = parse_macro_input!(attr as Path);

    // Parse the input struct
    let mut input_struct = parse_macro_input!(input as ItemStruct);
    let struct_name = &input_struct.ident;

    let args_struct_name = format_ident!("{}Args", struct_name);

    let mut args_struct = input_struct.clone();
    args_struct.ident = args_struct_name.clone();
    // Apply derives directly to args_struct
    args_struct.attrs = vec![
        syn::parse_quote!(#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]),
        syn::parse_quote!(#[ts(export)]),
        syn::parse_quote!(#[serde(rename_all = "camelCase")]),
    ];

    if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
        let tx = quote! { tx };
        let arc_str = quote! { std::sync::Arc<str> };
        let pub_viz = quote! { pub };

        let created_at = quote! { created_at };

        let tx_field: Field = syn::parse_quote! {
            #pub_viz #tx: #arc_str
        };

        let created_at_field: Field = syn::parse_quote! {
            #pub_viz #created_at: #arc_str
        };

        named.push(tx_field);
        named.push(created_at_field);
    };

    let derives = quote! {
         #[derive(Clone, Debug, Serialize, Deserialize, myko_rs::TS)]
         #[ts(export)]
         #[serde(rename_all = "camelCase")]
    };

    let query_registration = quote! {
        myko_rs::prelude::QueryRegistration {
            query_id: stringify!(#struct_name),
            query_item_type: stringify!(#query_item_type),
            crate_name: module_path!(),
        }
    };

    let pairs = args_struct
        .fields
        .iter()
        .map(|f| {
            let f_name = f.ident.as_ref().expect("must be field struct");
            quote! {#f_name: args.#f_name,}
        })
        .collect::<Vec<_>>();

    // Generate the implementation
    let expanded = quote! {
        #derives
        #input_struct

        #args_struct

        impl #struct_name {
            pub fn new(args: #args_struct_name) -> Self {
                let tx: std::sync::Arc<str> = myko_rs::prelude::Uuid::new_v4().to_string().into();
                let created_at: std::sync::Arc<str> = myko_rs::prelude::Utc::now().to_rfc3339().into();
                Self {
                    tx,
                    created_at,
                    #(#pairs)*
                }
            }
        }


        myko_rs::submit! {
            #query_registration
        }

        // Impl MykoQuery
        impl myko_rs::prelude::Query for #struct_name {
            fn watch(&self, client: &myko_rs::prelude::MykoClient) -> impl tokio_stream::Stream<Item = Vec<<Self as myko_rs::prelude::QueryItemType>::Item>> {
                client.watch_query(self)
            }
        }

        impl myko_rs::prelude::WithTransaction for #struct_name {
            fn tx_id(&self) -> std::sync::Arc<str> {
                self.tx.clone()
            }
        }

        // Impl QueryId
        impl myko_rs::prelude::QueryId for #struct_name {
            fn query_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }

        }

        impl myko_rs::prelude::AnyQuery for #struct_name {}

        impl myko_rs::prelude::QueryIdStatic for #struct_name {
            fn query_id_static() -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        // Impl QueryItemType
        impl myko_rs::prelude::QueryItemType for #struct_name {
            type Item = #query_item_type;

            fn query_item_type(&self) -> std::sync::Arc<str> {
                Self::query_item_type_static()
            }

            fn query_item_type_static() -> std::sync::Arc<str> {
                stringify!(#query_item_type).into()
            }
        }

        impl From<myko_rs::prelude::WrappedQuery> for #struct_name {
            fn from(wrapped_query: myko_rs::prelude::WrappedQuery) -> Self {
                serde_json::from_value::<Self>(wrapped_query.query).expect("Failed to deserialize query")
            }
        }



    };

    // Return the generated code
    TokenStream::from(expanded)
}

#[proc_macro_attribute]
/// Generates a reactive report that can depend on queries and other reports.
///
/// # Usage
///
/// ```ignore
/// #[myko_report(Vec<Target>)]
/// pub struct GetParentTargets {
///     pub target_id: String,
///     pub depth: u32,
/// }
///
/// // You must implement the compute method:
/// impl GetParentTargets {
///     pub fn compute(
///         report: std::sync::Arc<Self>,
///         ctx: myko_rs::prelude::ReportContext,
///     ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Vec<Target>> + Send>> {
///         // Use ctx.query() and ctx.report() for reactive dependencies
///         Box::pin(async_stream::stream! {
///             // ... your reactive logic
///         })
///     }
/// }
/// ```
pub fn myko_report(attr: TokenStream, input: TokenStream) -> TokenStream {
    let report_output_type: Path = parse_macro_input!(attr as Path);

    // Parse the input struct
    let mut input_struct = parse_macro_input!(input as ItemStruct);
    let struct_name = &input_struct.ident;

    let args_struct_name = format_ident!("{}Args", struct_name);

    let mut args_struct = input_struct.clone();
    args_struct.ident = args_struct_name.clone();
    // Apply derives directly to args_struct
    args_struct.attrs = vec![
        syn::parse_quote!(#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]),
        syn::parse_quote!(#[ts(export)]),
        syn::parse_quote!(#[serde(rename_all = "camelCase")]),
    ];

    // Add tx field for tracking
    if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
        let tx = quote! { tx };
        let arc_str = quote! { std::sync::Arc<str> };
        let pub_viz = quote! { pub };

        let tx_field: Field = syn::parse_quote! {
            #pub_viz #tx: #arc_str
        };

        named.push(tx_field);
    };

    let derives = quote! {
         #[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]
         #[ts(export)]
         #[serde(rename_all = "camelCase")]
    };

    // Extract just the type name (last segment) from the path for registration
    let output_type_name = report_output_type
        .segments
        .last()
        .map(|seg| &seg.ident)
        .expect("Output type path must have at least one segment");

    let report_registration = quote! {
        myko_rs::prelude::ReportRegistration {
            report_id: stringify!(#struct_name),
            output_type: stringify!(#output_type_name),
            crate_name: module_path!(),
            // Output type crate: use module_path!() since the output type is defined
            // in the same crate as the report (either explicitly or via generated types)
            output_type_crate: module_path!(),
        }
    };

    let pairs = args_struct
        .fields
        .iter()
        .map(|f| {
            let f_name = f.ident.as_ref().expect("must be field struct");
            quote! {#f_name: args.#f_name,}
        })
        .collect::<Vec<_>>();

    // Generate the implementation
    let expanded = quote! {
        #derives
        #input_struct
        #args_struct

        impl #struct_name {
            pub fn new(args: #args_struct_name) -> Self {
                let tx: std::sync::Arc<str> = myko_rs::prelude::Uuid::new_v4().to_string().into();
                Self {
                    tx,
                    #(#pairs)*
                }
            }
        }

        myko_rs::submit! {
            #report_registration
        }

        // Client-side watch (legacy compatibility)
        impl myko_rs::prelude::MykoReport<#report_output_type> for #struct_name {
            fn watch(&self, client: &myko_rs::client::MykoClient) -> impl tokio_stream::Stream<Item = #report_output_type> {
                client.watch_report::<#struct_name, #report_output_type>(self)
            }
        }

        impl myko_rs::prelude::ReportId for #struct_name {
            fn report_id(&self) -> String {
                stringify!(#struct_name).to_string()
            }
        }

        impl myko_rs::prelude::WithTransaction for #struct_name {
            fn tx_id(&self) -> std::sync::Arc<str> {
                self.tx.clone()
            }
        }

        impl myko_rs::prelude::ReportIdStatic for #struct_name {
            fn report_id_static() -> &'static str {
                stringify!(#struct_name)
            }
        }

        impl myko_rs::prelude::ReportOutputType for #struct_name {
            type Output = #report_output_type;
        }

        impl From<myko_rs::prelude::WrappedReport> for #struct_name {
            fn from(wrapped_report: myko_rs::prelude::WrappedReport) -> Self {
                serde_json::from_value::<Self>(wrapped_report.report).expect("Failed to deserialize report")
            }
        }

        // Report trait impl - requires ReportHandler to be implemented by the user
        impl myko_rs::prelude::Report for #struct_name {
            fn watch(&self, client: &myko_rs::client::MykoClient) -> impl tokio_stream::Stream<Item = #report_output_type> {
                client.watch_report::<Self, #report_output_type>(self)
            }
        }
    };

    // Return the generated code
    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn myko_command(_attr: TokenStream, input: TokenStream) -> TokenStream {
    // No attribute args for now. The struct name is the commandId.
    let input_struct = parse_macro_input!(input as ItemStruct);
    let struct_name = &input_struct.ident;

    let expanded = quote! {
        #input_struct

        impl myko_rs::command::CommandId for &#struct_name {
            fn command_id(&self) -> String {
                stringify!(#struct_name).to_string()
            }
        }

        impl myko_rs::command::CommandId for #struct_name {
            fn command_id(&self) -> String {
                stringify!(#struct_name).to_string()
            }
        }

        impl #struct_name {
            pub async fn handle<R: serde::de::DeserializeOwned + Clone + 'static>(
                &self,
                client: &myko_rs::prelude::MykoClient,
            ) -> Result<R, String> {
                client.send_command::<#struct_name, R>(self).await
            }
        }
    };

    TokenStream::from(expanded)
}
