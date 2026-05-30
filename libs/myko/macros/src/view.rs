use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    ItemStruct, Path,
    parse::{Parse, ParseStream},
};

pub struct ViewArgs {
    pub item_type: Path,
}

impl Parse for ViewArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let item_type: Path = input.parse()?;
        Ok(Self { item_type })
    }
}

pub fn myko_view_item_impl(mut input_struct: ItemStruct) -> TokenStream {
    let name = &input_struct.ident;
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    crate::gate_ts_attrs(&mut input_struct.attrs);
    for field in input_struct.fields.iter_mut() {
        crate::gate_ts_attrs(&mut field.attrs);
    }

    quote! {
        #[derive(Debug, Clone, PartialEq, #serde_path::Serialize, #serde_path::Deserialize)]
        #[derive(#krate::TS)]
        #serde_rename_attr
        #input_struct

        #krate::register_ts_export!(#name);

        impl #krate::prelude::WithId for #name {
            fn id(&self) -> std::sync::Arc<str> {
                self.id.clone()
            }
        }

        impl #krate::common::with_id::WithTypedId for #name {
            type Id = std::sync::Arc<str>;

            fn typed_id(&self) -> Self::Id {
                self.id.clone()
            }
        }

        impl #krate::prelude::AnyItem for #name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn entity_type(&self) -> &'static str {
                stringify!(#name)
            }

            fn equals(&self, other: &dyn #krate::prelude::AnyItem) -> bool {
                other
                    .as_any()
                    .downcast_ref::<Self>()
                    .map(|typed| self == typed)
                    .unwrap_or(false)
            }
        }

        impl #krate::prelude::Eventable for #name {
            const ENTITY_NAME_STATIC: &'static str = stringify!(#name);
        }
    }
}

pub fn myko_view_impl(args: ViewArgs, mut input_struct: ItemStruct) -> TokenStream {
    let manual_cache_key = crate::take_manual_cache_key_attr(&mut input_struct);
    let non_hash_cache_key = crate::take_non_hash_cache_key_attr(&mut input_struct);
    let struct_name = &input_struct.ident;
    let item_type = args.item_type;
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    crate::gate_ts_attrs(&mut input_struct.attrs);
    for field in input_struct.fields.iter_mut() {
        crate::gate_ts_attrs(&mut field.attrs);
    }

    let is_empty = matches!(&input_struct.fields, syn::Fields::Named(f) if f.named.is_empty())
        || matches!(&input_struct.fields, syn::Fields::Unit);

    let ts_cfg_derive = quote!(#[derive(#krate::TS)]);

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

    let view_registration = quote! {
        #krate::prelude::ViewRegistration {
            view_id: stringify!(#struct_name),
            view_item_type: stringify!(#item_type),
            crate_name: module_path!(),
            parse: <#struct_name as #krate::view::ViewFactory>::parse,
            cell_factory: <#struct_name as #krate::view::ViewFactory>::cell_factory,
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

    quote! {
        #derives
        #input_struct

        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #view_registration
        }

        #krate::register_ts_export!(#struct_name);

        impl #krate::prelude::ViewId for #struct_name {
            fn view_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::prelude::ViewIdStatic for #struct_name {
            fn view_id_static() -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::prelude::ViewItemType for #struct_name {
            type Item = #item_type;

            fn view_item_type(&self) -> std::sync::Arc<str> {
                Self::view_item_type_static()
            }

            fn view_item_type_static() -> std::sync::Arc<str> {
                stringify!(#item_type).into()
            }
        }

        const _: fn() = || {
            fn assert_with_typed_id<T: #krate::common::with_id::WithTypedId>() {}
            assert_with_typed_id::<#item_type>();
        };

        #cache_key_impl

    }
}
