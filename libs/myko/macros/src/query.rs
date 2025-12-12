use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Field, FieldsNamed, ItemStruct, Path};

pub fn myko_query_impl(query_item_type: Path, mut input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;

    let args_struct_name = format_ident!("{}Args", struct_name);

    let mut args_struct = input_struct.clone();
    args_struct.ident = args_struct_name.clone();
    // Apply derives directly to args_struct
    args_struct.attrs = vec![
        syn::parse_quote!(#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]),
        syn::parse_quote!(#[serde(rename_all = "camelCase")]),
    ];

    if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
        let tx = quote! { tx };
        let arc_str = quote! { std::sync::Arc<str> };
        let pub_viz = quote! { pub };

        let created_at = quote! { created_at };

        let tx_field: Field = syn::parse_quote! {
            #pub_viz #tx: #arc_str
        };

        let created_at_field: Field = syn::parse_quote! {
            #pub_viz #created_at: #arc_str
        };

        named.push(tx_field);
        named.push(created_at_field);
    };

    let derives = quote! {
         #[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]
         #[serde(rename_all = "camelCase")]
    };

    let query_registration = quote! {
        myko_rs::prelude::QueryRegistration {
            query_id: stringify!(#struct_name),
            query_item_type: stringify!(#query_item_type),
            crate_name: module_path!(),
        }
    };

    let pairs = args_struct
        .fields
        .iter()
        .map(|f| {
            let f_name = f.ident.as_ref().expect("must be field struct");
            quote! {#f_name: args.#f_name,}
        })
        .collect::<Vec<_>>();

    // Generate the implementation
    let expanded = quote! {
        #derives
        #input_struct

        #args_struct

        impl #struct_name {
            pub fn new(args: #args_struct_name) -> Self {
                let tx: std::sync::Arc<str> = myko_rs::prelude::Uuid::new_v4().to_string().into();
                let created_at: std::sync::Arc<str> = myko_rs::prelude::Utc::now().to_rfc3339().into();
                Self {
                    tx,
                    created_at,
                    #(#pairs)*
                }
            }
        }


        myko_rs::submit! {
            #query_registration
        }

        // Register for ts-rs export
        myko_rs::register_ts_export!(#struct_name, #args_struct_name);

        // Impl MykoQuery
        impl myko_rs::prelude::Query for #struct_name {
            fn watch(&self, client: &myko_rs::prelude::MykoClient) -> impl tokio_stream::Stream<Item = Vec<<Self as myko_rs::prelude::QueryItemType>::Item>> {
                client.watch_query(self)
            }
        }

        impl myko_rs::prelude::WithTransaction for #struct_name {
            fn tx_id(&self) -> std::sync::Arc<str> {
                self.tx.clone()
            }
        }

        // Impl QueryId
        impl myko_rs::prelude::QueryId for #struct_name {
            fn query_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }

        }

        impl myko_rs::prelude::AnyQuery for #struct_name {
            fn query_item_type(&self) -> std::sync::Arc<str> {
                <Self as myko_rs::prelude::QueryItemType>::query_item_type(self)
            }

            fn to_value(&self) -> serde_json::Value {
                serde_json::to_value(self).expect("Query should serialize to JSON")
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

        impl From<myko_rs::prelude::WrappedQuery> for #struct_name {
            fn from(wrapped_query: myko_rs::prelude::WrappedQuery) -> Self {
                serde_json::from_value::<Self>(wrapped_query.query).expect("Failed to deserialize query")
            }
        }



    };

    expanded
}
