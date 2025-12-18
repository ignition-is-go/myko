use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemStruct, Path};

/// Generates a command with its handler struct and registration.
///
/// # Usage
///
/// ```ignore
/// #[myko_command(CreateMachineResult)]
/// pub struct CreateMachine {
///     pub name: String,
/// }
///
/// // User must implement the handler:
/// impl CreateMachineHandler {
///     async fn execute(
///         cmd: CreateMachine,
///         ctx: myko_rs::prelude::CommandContext,
///     ) -> Result<CreateMachineResult, myko_rs::prelude::CommandError> {
///         // Handler logic
///     }
/// }
/// ```
pub fn myko_command_impl(result_type: Option<Path>, input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;
    let handler_name = format_ident!("{}Handler", struct_name);
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
            fn command_id_static() -> &'static str {
                stringify!(#struct_name)
            }
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

        /// Generated handler struct - user must implement the execute method
        pub struct #handler_name;

        impl myko_rs::command::CommandHandler for #handler_name {
            fn command_id(&self) -> &'static str {
                stringify!(#struct_name)
            }

            fn execute(
                &self,
                command: serde_json::Value,
                ctx: myko_rs::command::CommandContext,
            ) -> myko_rs::command::BoxFuture<'_, Result<serde_json::Value, myko_rs::command::CommandError>> {
                Box::pin(async move {
                    let cmd: #struct_name = serde_json::from_value(command).map_err(|e| {
                        myko_rs::command::CommandError {
                            tx: ctx.tx().to_string(),
                            command_id: ctx.command_id.to_string(),
                            message: format!("Failed to parse command: {}", e),
                        }
                    })?;

                    let result = #handler_name::execute(cmd, ctx).await?;

                    serde_json::to_value(&result).map_err(|e| {
                        myko_rs::command::CommandError {
                            tx: String::new(),
                            command_id: stringify!(#struct_name).to_string(),
                            message: format!("Failed to serialize result: {}", e),
                        }
                    })
                })
            }
        }

        // Handler registration (for runtime execution)
        myko_rs::submit! {
            myko_rs::command::CommandHandlerRegistration {
                command_id: stringify!(#struct_name),
                factory: || Box::new(#handler_name) as Box<dyn myko_rs::command::CommandHandler>,
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

        // Register for ts-rs export
        myko_rs::register_ts_export!(#struct_name, #args_struct_name);
    };

    expanded
}
