use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemStruct, Path, Type};

pub enum CommandOwner {
    Item(Path),
    Service { service: Path, scope: Path },
}

pub struct CommandOptions {
    pub result_type: Option<Type>,
    pub custom_serialize: bool,
    pub owner: Option<CommandOwner>,
}

fn make_args_struct(
    input: &ItemStruct,
    name: &syn::Ident,
    serde_path: &TokenStream,
    krate: &syn::Path,
    serde_rename_attr: &TokenStream,
) -> ItemStruct {
    let mut args = input.clone();
    args.ident = name.clone();
    args.attrs = vec![
        syn::parse_quote!(#[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize)]),
        syn::parse_quote!(#[derive(#krate::TS)]),
        syn::parse_quote!(#[ts(crate = "myko::ts_rs")]),
        syn::parse_quote!(#serde_rename_attr),
    ];
    args
}

/// Generates command trait implementations and registers the command handler.
// Keeping the emitted command contract in one expansion makes the generated
// API easier to audit than splitting one quote across several stateful helpers.
#[allow(clippy::too_many_lines)]
pub fn myko_command_impl(options: CommandOptions, mut input_struct: ItemStruct) -> TokenStream {
    let CommandOptions {
        result_type,
        custom_serialize,
        owner,
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

    crate::gate_ts_attrs(&mut input_struct.attrs);
    crate::gate_field_ts_attrs(&mut input_struct.fields);

    let ts_cfg_derive = quote!(#[derive(#krate::TS)] #[ts(crate = "myko::ts_rs")]);

    // Args remains field-identical for wire compatibility.
    let serde_rename_attr_clone = ctx.serde_attr(&quote!(rename_all = "camelCase"));
    let args_struct = make_args_struct(
        &input_struct,
        &args_struct_name,
        serde_path,
        krate,
        &serde_rename_attr_clone,
    );

    // Default to () if no result type specified
    let result_type_tokens = result_type
        .as_ref()
        .map_or_else(|| quote! { () }, |path| quote! { #path });

    // Get the result type name as a string for registration (including generic args)
    let result_type_str = result_type
        .as_ref()
        .map_or_else(|| "()".to_string(), |path| quote!(#path).to_string());

    let (service_id, typed_contract, handler_registration) = owner.map_or_else(
        || {
            (
                quote!(None),
                quote!(),
                quote!(#krate::register_command_handler!(#struct_name, service_id = None);),
            )
        },
        |owner| {
            let (service, scope, item_type, service_id) = match owner {
                CommandOwner::Item(item) => (
                    quote!(<#item as #krate::MykoItem>::Service),
                    quote!(<#item as #krate::MykoItem>::Scope),
                    quote!(Some(<#item as #krate::MykoItem>::ITEM_TYPE)),
                    quote!(<#item as #krate::MykoItem>::SERVICE_ID),
                ),
                CommandOwner::Service { service, scope } => (
                    quote!(#service),
                    quote!(#scope),
                    quote!(None),
                    quote!(<#service as #krate::MykoService>::SERVICE_ID),
                ),
            };
            (
                quote!(Some(#service_id)),
                quote! {
                    impl #krate::MykoOperation for #struct_name {
                        const OPERATION_ID: &'static str = stringify!(#struct_name);
                    }

                    impl #krate::MykoCommandContract for #struct_name {
                        type Output = #result_type_tokens;
                        type Service = #service;
                        type Scope = #scope;
                        const ITEM_TYPE: Option<&'static str> = #item_type;
                    }

                    impl #krate::MykoCommand for #struct_name {}
                },
                quote!(
                    #krate::register_durable_command_handler!(
                        #struct_name,
                        service_id = #service_id
                    );
                ),
            )
        },
    );

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

        #typed_contract

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
                service_id: #service_id,
                result_type: #result_type_str,
                result_type_crate: module_path!(),
                crate_name: module_path!(),
                args: #args_tokens,
                description: #description_tokens,
            }
        }

        // Register command handler for runtime dispatch (server-only)
        #[cfg(not(target_arch = "wasm32"))]
        #handler_registration

        // Register for ts-rs export
        #krate::register_typegen_type!(#struct_name, #args_struct_name);
    };

    expanded
}
