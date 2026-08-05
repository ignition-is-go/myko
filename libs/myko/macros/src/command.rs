use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemStruct, Path};

pub struct CommandOptions {
    pub result_type: Option<Path>,
    pub custom_serialize: bool,
}

/// Generates command trait implementations and registers the command handler.
pub fn myko_command_impl(options: CommandOptions, mut input_struct: ItemStruct) -> TokenStream {
    let CommandOptions {
        result_type,
        custom_serialize,
    } = options;
    let struct_name = &input_struct.ident;
    let args_struct_name = format_ident!("{}Args", struct_name);
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));

    // Reflection metadata for the MCP `search()` operation index — see
    // `myko::reflection` and the matching comment in `query.rs`. The Args
    // struct is field-identical to `input_struct` (cloned below), so
    // capturing from `input_struct.fields` covers both.
    let (description_tokens, args_tokens) = crate::operation_metadata_tokens(&input_struct, krate);

    // Gate user-written `#[ts(...)]` attrs behind the ts-export feature.
    crate::gate_ts_attrs(&mut input_struct.attrs);
    crate::gate_field_ts_attrs(&mut input_struct.fields);

    let ts_cfg_derive = quote!(#[derive(#krate::TS)]);

    // Create args struct (identical to main struct for backward compatibility)
    // TODO(ts): Remove Args pattern once all call sites are updated to use CommandRequest
    let mut args_struct = input_struct.clone();
    args_struct.ident = args_struct_name.clone();
    args_struct.attrs = vec![
        syn::parse_quote!(#[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize)]),
        syn::parse_quote!(#ts_cfg_derive),
    ];
    // Add serde rename attr
    let serde_rename_attr_clone = ctx.serde_attr(&quote!(rename_all = "camelCase"));
    args_struct
        .attrs
        .push(syn::parse_quote!(#serde_rename_attr_clone));

    // Default to () if no result type specified
    let result_type_tokens = result_type
        .as_ref()
        .map_or_else(|| quote! { () }, |path| quote! { #path });

    // Get the result type name as a string for registration (including generic args)
    let result_type_str = result_type
        .as_ref()
        .map_or_else(|| "()".to_string(), |path| quote!(#path).to_string());

    let derives = if custom_serialize {
        quote! {
            #[derive(Clone, Debug, #serde_path::Deserialize)]
            #ts_cfg_derive
            #serde_rename_attr
        }
    } else {
        quote! {
            #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize)]
            #ts_cfg_derive
            #serde_rename_attr
        }
    };

    let pairs = input_struct
        .fields
        .iter()
        .filter_map(|f| {
            let f_name = f.ident.as_ref()?;
            Some(quote! {#f_name: args.#f_name,})
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
            ) -> #krate::hyphae::Cell<Option<Result<#result_type_tokens, String>>, #krate::hyphae::CellImmutable> {
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
                args: #args_tokens,
                description: #description_tokens,
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
