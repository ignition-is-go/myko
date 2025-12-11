use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

pub fn myko_command_impl(input_struct: ItemStruct) -> TokenStream {
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

    expanded
}
