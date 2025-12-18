use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemStruct, Path};

pub fn myko_query_impl(query_item_type: Path, input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;

    // Check if struct has no fields (empty)
    let is_empty = matches!(&input_struct.fields, syn::Fields::Named(f) if f.named.is_empty())
        || matches!(&input_struct.fields, syn::Fields::Unit);

    // Apply derives (add Default for empty structs)
    let derives = if is_empty {
        quote! {
            #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, myko_rs::TS)]
            #[serde(rename_all = "camelCase")]
        }
    } else {
        quote! {
            #[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]
            #[serde(rename_all = "camelCase")]
        }
    };

    let query_registration = quote! {
        myko_rs::prelude::QueryRegistration {
            query_id: stringify!(#struct_name),
            query_item_type: stringify!(#query_item_type),
            crate_name: module_path!(),
            factory: || -> myko_rs::actors::query::query_manager::RegisterQueryData {
                use myko_rs::query::QueryFactory;
                #struct_name::create_registration()
            },
        }
    };

    // Generate the implementation
    // Note: We don't generate Args type or inject tx/created_at anymore.
    // Those are handled by QueryRequest<Q> wrapper.
    let expanded = quote! {
        #derives
        #input_struct

        myko_rs::submit! {
            #query_registration
        }

        // Register for ts-rs export (just the params type now)
        myko_rs::register_ts_export!(#struct_name);

        // Impl QueryId
        impl myko_rs::prelude::QueryId for #struct_name {
            fn query_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

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

        // Note: WithTransaction, AnyQuery, and Query are implemented on QueryRequest<#struct_name>
        // via blanket impls in myko_rs. The user's struct just implements the identity traits.
    };

    expanded
}
