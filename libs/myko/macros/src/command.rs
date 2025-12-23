use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemStruct, Path};

/// Generates command trait implementations and registers the command handler.
///
/// # Usage
///
/// ```ignore
/// #[myko_command(CreateMachineResult)]
/// pub struct CreateMachine {
///     pub name: String,
/// }
///
/// // Implement CommandHandler directly on the command struct:
/// impl myko_rs::command::CommandHandler for CreateMachine {
///     fn execute(&self, ctx: myko_rs::command::CommandContext) -> Result<CreateMachineResult, myko_rs::command::CommandError> {
///         // Handler logic - self is already the deserialized command
///     }
/// }
/// // Note: Handler registration is automatic via the macro
/// ```
pub fn myko_command_impl(result_type: Option<Path>, input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;
    let args_struct_name = format_ident!("{}Args", struct_name);

    // Create args struct (identical to main struct for backward compatibility)
    // TODO(ts): Remove Args pattern once all call sites are updated to use CommandRequest
    let mut args_struct = input_struct.clone();
    args_struct.ident = args_struct_name.clone();
    args_struct.attrs = vec![
        syn::parse_quote!(#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]),
        syn::parse_quote!(#[serde(rename_all = "camelCase")]),
    ];

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
         #[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]
         #[serde(rename_all = "camelCase")]
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

        impl myko_rs::command::CommandId for &#struct_name {
            fn command_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl myko_rs::command::CommandId for #struct_name {
            fn command_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl myko_rs::command::CommandIdStatic for #struct_name {
            const COMMAND_ID: &'static str = stringify!(#struct_name);
        }

        impl myko_rs::command::CommandResultType for #struct_name {
            type Result = #result_type_tokens;
        }

        impl #struct_name {
            /// Create a new command instance from args (backward compatibility)
            /// Prefer using CommandRequest::new(Command { ... }) for new code
            pub fn new(args: #args_struct_name) -> Self {
                Self {
                    #(#pairs)*
                }
            }

            /// Client-side handle method for sending commands
            pub async fn handle(
                &self,
                client: &myko_rs::prelude::MykoClient,
            ) -> Result<#result_type_tokens, String> {
                client.send_command::<#struct_name, #result_type_tokens>(self).await
            }
        }

        // Command registration (for type generation)
        myko_rs::submit! {
            myko_rs::command::CommandRegistration {
                command_id: stringify!(#struct_name),
                result_type: #result_type_str,
                result_type_crate: module_path!(),
                crate_name: module_path!(),
            }
        }

        // Register command handler for runtime dispatch
        myko_rs::register_command_handler!(#struct_name);

        // Register for ts-rs export
        myko_rs::register_ts_export!(#struct_name, #args_struct_name);
    };

    expanded
}
