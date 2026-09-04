use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Ident, ItemStruct, Path, Token,
    parse::{Parse, ParseStream},
};

pub struct ReportArgs {
    pub output_type: Path,
    pub service_item: Option<Path>,
}

impl Parse for ReportArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let output_type = input.parse()?;
        let mut service_item = None;

        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }

            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if key != "item" {
                return Err(syn::Error::new_spanned(key, "expected `item`"));
            }
            if service_item.is_some() {
                return Err(syn::Error::new_spanned(key, "duplicate `item` owner"));
            }
            service_item = Some(input.parse()?);
        }

        Ok(Self {
            output_type,
            service_item,
        })
    }
}

// Report expansion remains one linear block so registration and the typed
// runtime contract cannot be edited independently.
#[allow(clippy::too_many_lines)]
pub fn myko_report_impl(args: ReportArgs, mut input_struct: ItemStruct) -> TokenStream {
    let manual_cache_key = crate::take_manual_cache_key_attr(&mut input_struct);
    let non_hash_cache_key = crate::take_non_hash_cache_key_attr(&mut input_struct);
    let struct_name = &input_struct.ident;
    let report_output_type = &args.output_type;
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));
    let service_id = args.service_item.map_or_else(
        || quote!(None),
        |service_item| quote!(Some(<#service_item as #krate::MykoItem>::SERVICE_ID)),
    );

    // Reflection metadata for the MCP `search()` operation index — see
    // `myko::reflection` and the matching comment in `query.rs`.
    let (description_tokens, args_tokens) = crate::operation_metadata_tokens(&input_struct, krate);

    // Gate user-written `#[ts(...)]` attrs behind the codegen-ts feature.
    crate::gate_ts_attrs(&mut input_struct.attrs);
    crate::gate_field_ts_attrs(&mut input_struct.fields);

    // Check if struct has no fields (empty)
    let is_empty = matches!(&input_struct.fields, syn::Fields::Named(f) if f.named.is_empty())
        || matches!(&input_struct.fields, syn::Fields::Unit);

    let ts_cfg_derive = quote!(#[derive(#krate::TS)] #[ts(crate = "myko::ts_rs")]);

    // Apply derives (add Default for empty structs)
    let derives = if is_empty {
        if non_hash_cache_key {
            quote! {
                #[derive(Clone, Debug, Default, #serde_path::Serialize, #serde_path::Deserialize)]
                #ts_cfg_derive
                #serde_rename_attr
            }
        } else {
            quote! {
                #[derive(Clone, Debug, Default, Hash, #serde_path::Serialize, #serde_path::Deserialize)]
                #ts_cfg_derive
                #serde_rename_attr
            }
        }
    } else if non_hash_cache_key {
        quote! {
            #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize)]
            #ts_cfg_derive
            #serde_rename_attr
        }
    } else {
        quote! {
            #[derive(Clone, Debug, Hash, #serde_path::Serialize, #serde_path::Deserialize)]
            #ts_cfg_derive
            #serde_rename_attr
        }
    };

    // Generate report registration using ReportFactory trait
    let report_registration = quote! {
        #krate::prelude::ReportRegistration {
            report_id: stringify!(#struct_name),
            service_id: #service_id,
            crate_name: module_path!(),
            output_type: stringify!(#report_output_type),
            output_type_crate: module_path!(),
            parse: <#struct_name as #krate::report::ReportFactory>::parse,
            cell_factory: <#struct_name as #krate::report::ReportFactory>::cell_factory,
            #[cfg(not(target_arch = "wasm32"))]
            authority: <#struct_name as #krate::report::ReportFactory>::authority,
            args: #args_tokens,
            description: #description_tokens,
        }
    };

    let cache_key_impl = if manual_cache_key {
        quote!()
    } else if non_hash_cache_key {
        quote! {
            impl #krate::prelude::CacheKey for #struct_name {
                fn cache_key(&self, state: &mut dyn std::hash::Hasher) {
                    #krate::cache::write_serde_cache_key(self, state);
                }
            }
        }
    } else {
        quote! {
            impl #krate::prelude::CacheKey for #struct_name {
                fn cache_key(&self, state: &mut dyn std::hash::Hasher) {
                    #krate::cache::write_hash_cache_key(self, state);
                }
            }
        }
    };

    let expanded = quote! {
        #derives
        #input_struct

        // Registration is server-only (requires ReportFactory which depends on hyphae)
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #report_registration
        }

        // Register for ts-rs export (just the params type now)
        #krate::register_typegen_type!(#struct_name);

        // Impl ReportId
        impl #krate::prelude::ReportId for #struct_name {
            fn report_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::prelude::ReportIdStatic for #struct_name {
            fn report_id_static() -> &'static str {
                stringify!(#struct_name)
            }
        }

        impl #krate::prelude::ReportOutputType for #struct_name {
            type Output = #report_output_type;
        }

        #cache_key_impl
    };

    expanded
}
