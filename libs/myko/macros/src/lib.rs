use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(Empty)]
pub fn empty_impl(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let generated = quote! {

        impl Empty for #name {
            fn empty(&self) -> bool {
                false
            }
        }

    };

    generated.into()
}

#[proc_macro_derive(Eventable)]
pub fn eventable_impl(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let name_str = name.to_string();

    let partial_name = format_ident!("Partial{}", name);

    let generated = quote! {
        impl myko_rs::item::Eventable<#name, #partial_name> for #name {

            fn id(&self) -> String {
                self.id.clone()
            }

            fn hash(&self) -> String {
                self.hash.clone()
            }

            fn entity_name(&self) -> String {
                #name_str.to_string()
            }

            fn entity_name_static() -> String {
                #name_str.to_string()
            }
        }

    };

    generated.into()
}

use syn::{ItemStruct, Path};

#[proc_macro_attribute]
pub fn myko_query(attr: TokenStream, input: TokenStream) -> TokenStream {
    // Parse the single argument (e.g., `File`) from the attribute
    let query_item_type: Path = parse_macro_input!(attr as Path);

    // Parse the input struct
    let input_struct = parse_macro_input!(input as ItemStruct);
    let struct_name = &input_struct.ident;

    // Generate the implementation
    let expanded = quote! {
        #input_struct

        impl myko_rs::query::MykoQuery for #struct_name {
            type Item = #query_item_type;
            fn watch(&self, client: &myko_rs::client::MykoClient) -> impl tokio_stream::Stream<Item = Vec<Self::Item>> {
                client.watch_query(self)
            }
        }

        // both as ref
        impl myko_rs::query::QueryId for &#struct_name {
            fn query_id(&self) -> String {
                stringify!(#struct_name).to_string()
            }
        }

        // and as value
        impl myko_rs::query::QueryId for #struct_name {
            fn query_id(&self) -> String {
                stringify!(#struct_name).to_string()
            }
        }

        // as ref
        impl myko_rs::query::QueryItemType for &#struct_name {
            fn query_item_type(&self) -> String {
                stringify!(#query_item_type).to_string()
            }
        }

        // and as value
        impl myko_rs::query::QueryItemType for #struct_name {
            fn query_item_type(&self) -> String {
                stringify!(#query_item_type).to_string()
            }
        }

    };

    // Return the generated code
    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn myko_query_handler(attr: TokenStream, input: TokenStream) -> TokenStream {
    // attr is a function name that will be called to handle the query
    let query_handler: Path = parse_macro_input!(attr as Path);

    // Parse the input struct

    let input_struct = parse_macro_input!(input as ItemStruct);

    let struct_name = &input_struct.ident;

    // Generate the implementation

    let expanded = quote! {
        #input_struct

        impl myko_rs::query::QueryHandler<#struct_name> for #struct_name {
            fn handle_query(
                &self,
                query: #struct_name,
                tx: String,
            ) -> impl tokio_stream::Stream<Item = myko_rs::query::QueryResult<<#struct_name as myko_rs::query::MykoQuery>::Item>> {
                #query_handler(query, tx)
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

        impl myko_rs::report::MykoReport<#report_item_type> for &#struct_name {
            fn watch(&self, client: &myko_rs::client::MykoClient) -> impl tokio_stream::Stream<Item = #report_item_type> {
                client.watch_report::<&#struct_name, #report_item_type>(self)
            }
        }

        impl myko_rs::report::MykoReport<#report_item_type> for #struct_name {
            fn watch(&self, client: &myko_rs::client::MykoClient) -> impl tokio_stream::Stream<Item = #report_item_type> {
                client.watch_report::<#struct_name, #report_item_type>(self)
            }
        }

        impl myko_rs::report::ReportId for &#struct_name {
            fn report_id(&self) -> String {
                stringify!(#struct_name).to_string()
            }
        }

        impl myko_rs::report::ReportId for #struct_name {
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
                client: &myko_rs::client::MykoClient,
            ) -> Result<R, String> {
                client.send_command::<#struct_name, R>(self).await
            }
        }
    };

    TokenStream::from(expanded)
}
