use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemStruct, Path};

/// Generates command trait implementations and registers the command handler.
pub fn myko_command_impl(result_type: Option<Path>, input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;
    let args_struct_name = format_ident!("{}Args", struct_name);
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    // Create args struct (identical to main struct for backward compatibility)
    // TODO(ts): Remove Args pattern once all call sites are updated to use CommandRequest
    let mut args_struct = input_struct.clone();
    args_struct.ident = args_struct_name.clone();
    args_struct.attrs = vec![
        syn::parse_quote!(#[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]),
    ];
    // Add serde rename attr
    let serde_rename_attr_clone = ctx.serde_attr(quote!(rename_all = "camelCase"));
    args_struct
        .attrs
        .push(syn::parse_quote!(#serde_rename_attr_clone));

    // Default to () if no result type specified
    let result_type_tokens = match &result_type {
        Some(path) => quote! { #path },
        None => quote! { () },
    };

    // Get the result type name as a string for registration (including generic args)
    let result_type_str = match &result_type {
        Some(path) => quote!(#path).to_string(),
        None => "()".to_string(),
    };

    let derives = quote! {
         #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
         #serde_rename_attr
    };

    let pairs = input_struct
        .fields
        .iter()
        .map(|f| {
            let f_name = f.ident.as_ref().expect("must be field struct");
            quote! {#f_name: args.#f_name,}
        })
        .collect::<Vec<_>>();

    let expanded = quote! {
        #derives
        #input_struct

        #args_struct

        impl #krate::command::CommandId for &#struct_name {
            fn command_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::command::CommandId for #struct_name {
            fn command_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::command::CommandIdStatic for #struct_name {
            const COMMAND_ID: &'static str = stringify!(#struct_name);
        }

        impl #krate::command::CommandResultType for #struct_name {
            type Result = #result_type_tokens;
        }

        impl #struct_name {
            pub fn new(args: #args_struct_name) -> Self {
                Self {
                    #(#pairs)*
                }
            }

            pub fn handle(
                &self,
                client: &#krate::prelude::MykoClient,
            ) -> #krate::hypha::Cell<Option<Result<#result_type_tokens, String>>, #krate::hypha::CellImmutable> {
                client.send_command::<#struct_name, #result_type_tokens>(self)
            }
        }

        // Command registration (for type generation, server-only)
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #krate::command::CommandRegistration {
                command_id: stringify!(#struct_name),
                result_type: #result_type_str,
                result_type_crate: module_path!(),
                crate_name: module_path!(),
            }
        }

        // Register command handler for runtime dispatch (server-only)
        #[cfg(not(target_arch = "wasm32"))]
        #krate::register_command_handler!(#struct_name);

        // Register for ts-rs export
        #krate::register_ts_export!(#struct_name, #args_struct_name);
    };

    expanded
}
