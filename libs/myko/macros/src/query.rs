use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemStruct, Path};

fn query_derive_tokens(
    ctx: &crate::DeriveCtx,
    is_empty: bool,
    non_hash_cache_key: bool,
    export: bool,
) -> TokenStream {
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));
    let export_derive = if export {
        quote!(#[derive(#krate::TS)] #[ts(crate = "myko::ts_rs")])
    } else {
        quote!()
    };

    match (is_empty, non_hash_cache_key) {
        (true, true) => quote! {
            #[derive(Clone, Debug, Default, #serde_path::Serialize, #serde_path::Deserialize)]
            #export_derive
            #serde_rename_attr
        },
        (true, false) => quote! {
            #[derive(Clone, Debug, Default, Hash, #serde_path::Serialize, #serde_path::Deserialize)]
            #export_derive
            #serde_rename_attr
        },
        (false, true) => quote! {
            #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize)]
            #export_derive
            #serde_rename_attr
        },
        (false, false) => quote! {
            #[derive(Clone, Debug, Hash, #serde_path::Serialize, #serde_path::Deserialize)]
            #export_derive
            #serde_rename_attr
        },
    }
}

pub fn myko_query_impl(
    query_item_type: &Path,
    include_in_typegen: bool,
    mut input_struct: ItemStruct,
) -> TokenStream {
    let manual_cache_key = crate::take_manual_cache_key_attr(&mut input_struct);
    let non_hash_cache_key = crate::take_non_hash_cache_key_attr(&mut input_struct);
    let struct_name = &input_struct.ident;
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;

    // Reflection metadata for the MCP `search()` operation index — captured
    // here from the struct's own fields/doc comment before any further
    // mutation, since it's always the ground truth regardless of whether
    // ts-rs codegen has run. See `myko::reflection`.
    let (description_tokens, args_tokens) = crate::operation_metadata_tokens(&input_struct, krate);

    // Also gate any user-written `#[ts(...)]` attrs on the fields; see the
    // comment on `gate_ts_attrs` in the crate root.
    if include_in_typegen {
        crate::gate_ts_attrs(&mut input_struct.attrs);
        crate::gate_field_ts_attrs(&mut input_struct.fields);
    }

    // Check if struct has no fields (empty)
    let is_empty = matches!(&input_struct.fields, syn::Fields::Named(f) if f.named.is_empty())
        || matches!(&input_struct.fields, syn::Fields::Unit);

    let derives = query_derive_tokens(&ctx, is_empty, non_hash_cache_key, include_in_typegen);

    // Generate query registration using QueryFactory trait
    let query_registration = quote! {
        #krate::prelude::QueryRegistration {
            query_id: stringify!(#struct_name),
            query_item_type: stringify!(#query_item_type),
            crate_name: module_path!(),
            parse: <#struct_name as #krate::query::QueryFactory>::parse,
            cell_factory: <#struct_name as #krate::query::QueryFactory>::cell_factory,
            window_cell_factory: <#struct_name as #krate::query::QueryFactory>::window_cell_factory,
            args: #args_tokens,
            description: #description_tokens,
            include_in_typegen: #include_in_typegen,
        }
    };

    let typegen_registration = if include_in_typegen {
        quote!(#krate::register_typegen_type!(#struct_name);)
    } else {
        quote!()
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

    quote! {
        #derives
        #input_struct

        // Registration is server-only (requires QueryFactory which depends on hyphae/store)
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #query_registration
        }

        #typegen_registration

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

        #cache_key_impl
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_export_keeps_runtime_registration_out_of_typegen() {
        let output = myko_query_impl(
            &syn::parse_quote!(HistoryRow),
            false,
            syn::parse_quote! {
                pub struct EntityHistory {
                    pub item_id: String,
                }
            },
        )
        .to_string();

        assert!(output.contains("include_in_typegen : false"));
        assert!(!output.contains("register_typegen_type"));
        assert!(!output.contains("myko :: TS"));
    }

    #[test]
    fn queries_export_by_default() {
        let output = myko_query_impl(
            &syn::parse_quote!(HistoryRow),
            true,
            syn::parse_quote! {
                pub struct EntityHistory {
                    pub item_id: String,
                }
            },
        )
        .to_string();

        assert!(output.contains("include_in_typegen : true"));
        assert!(output.contains("register_typegen_type"));
        assert!(output.contains("myko :: TS"));
    }
}
