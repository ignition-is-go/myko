use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemStruct, Path};

pub fn myko_query_impl(query_item_type: Path, input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    // Check if struct has no fields (empty)
    let is_empty = matches!(&input_struct.fields, syn::Fields::Named(f) if f.named.is_empty())
        || matches!(&input_struct.fields, syn::Fields::Unit);

    // Apply derives (add Default for empty structs)
    let derives = if is_empty {
        quote! {
            #[derive(Clone, Debug, Default, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
            #serde_rename_attr
        }
    } else {
        quote! {
            #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
            #serde_rename_attr
        }
    };

    // Generate query registration using QueryFactory trait
    let query_registration = quote! {
        #krate::prelude::QueryRegistration {
            query_id: stringify!(#struct_name),
            query_item_type: stringify!(#query_item_type),
            crate_name: module_path!(),
            parse: <#struct_name as #krate::query::QueryFactory>::parse,
            cell_factory: <#struct_name as #krate::query::QueryFactory>::cell_factory,
        }
    };

    let expanded = quote! {
        #derives
        #input_struct

        // Registration is server-only (requires QueryFactory which depends on hypha/store)
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #query_registration
        }

        // Register for ts-rs export (just the params type now)
        #krate::register_ts_export!(#struct_name);

        // Impl QueryId
        impl #krate::prelude::QueryId for #struct_name {
            fn query_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::prelude::QueryIdStatic for #struct_name {
            fn query_id_static() -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        // Impl QueryItemType
        impl #krate::prelude::QueryItemType for #struct_name {
            type Item = #query_item_type;

            fn query_item_type(&self) -> std::sync::Arc<str> {
                Self::query_item_type_static()
            }

            fn query_item_type_static() -> std::sync::Arc<str> {
                stringify!(#query_item_type).into()
            }
        }
    };

    expanded
}
