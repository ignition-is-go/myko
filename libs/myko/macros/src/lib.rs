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
///	Derives:
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
         struct #get_by_partial_ident {
             pub partial: #partial_ident
         }

         impl myko_rs::prelude::QueryHandler for #get_by_partial_ident {
             fn test_entity(ctx: myko_rs::prelude::QueryHandlerCtx<Self>) -> bool {
                 ctx.query.partial.matches(&ctx.item)
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

        #get_all_query

        #get_by_ids_query

        #get_by_partial_query

        impl myko_rs::prelude::MykoAutoQueries for #name {
                fn register_auto(server: &std::sync::Arc<myko_rs::prelude::MykoServer>) -> Result<(), anyhow::Error>{

                        #get_all_query_ident::register(&server)?;
                        #get_by_ids_query_ident::register(&server)?;
                        #get_by_partial_ident::register(&server)?;
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

    // Generate the implementation
    let expanded = quote! {
        #derives
        #input_struct

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
pub fn myko_report(attr: TokenStream, input: TokenStream) -> TokenStream {
    let report_item_type: Path = parse_macro_input!(attr as Path);

    // Parse the input struct
    let input_struct = parse_macro_input!(input as ItemStruct);
    let struct_name = &input_struct.ident;

    // Generate the implementation
    let expanded = quote! {
        #input_struct

        impl myko_rs::prelude::MykoReport<#report_item_type> for &#struct_name {
            fn watch(&self, client: &myko_rs::client::MykoClient) -> impl tokio_stream::Stream<Item = #report_item_type> {
                client.watch_report::<&#struct_name, #report_item_type>(self)
            }
        }

        impl myko_rs::prelude::MykoReport<#report_item_type> for #struct_name {
            fn watch(&self, client: &myko_rs::client::MykoClient) -> impl tokio_stream::Stream<Item = #report_item_type> {
                client.watch_report::<#struct_name, #report_item_type>(self)
            }
        }

        impl myko_rs::prelude::ReportId for &#struct_name {
            fn report_id(&self) -> String {
                stringify!(#struct_name).to_string()
            }
        }

        impl myko_rs::prelude::ReportId for #struct_name {
            fn report_id(&self) -> String {
                stringify!(#struct_name).to_string()
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
