use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Field, FieldsNamed, ItemStruct, Path};

pub fn myko_report_impl(report_output_type: Path, mut input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;

    let args_struct_name = format_ident!("{}Args", struct_name);

    let mut args_struct = input_struct.clone();
    args_struct.ident = args_struct_name.clone();
    // Apply derives directly to args_struct
    args_struct.attrs = vec![
        syn::parse_quote!(#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]),
        syn::parse_quote!(#[serde(rename_all = "camelCase")]),
    ];

    // Add tx field for tracking
    if let syn::Fields::Named(FieldsNamed { named, .. }) = &mut input_struct.fields {
        let tx = quote! { tx };
        let arc_str = quote! { std::sync::Arc<str> };
        let pub_viz = quote! { pub };

        let tx_field: Field = syn::parse_quote! {
            #pub_viz #tx: #arc_str
        };

        named.push(tx_field);
    };

    let derives = quote! {
         #[derive(Clone, Debug, serde::Serialize, serde::Deserialize, myko_rs::TS)]
         #[serde(rename_all = "camelCase")]
    };

    // Convert the full output type path to a string for registration
    // This preserves generics like Option<Server> -> "Option < Server >"
    let output_type_str = quote!(#report_output_type).to_string();

    let report_registration = quote! {
        myko_rs::prelude::ReportRegistration {
            report_id: stringify!(#struct_name),
            output_type: #output_type_str,
            crate_name: module_path!(),
            // Output type crate: use module_path!() since the output type is defined
            // in the same crate as the report (either explicitly or via generated types)
            output_type_crate: module_path!(),
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
                Self {
                    tx,
                    #(#pairs)*
                }
            }
        }

        myko_rs::submit! {
            #report_registration
        }

        // Register for ts-rs export
        myko_rs::register_ts_export!(#struct_name, #args_struct_name);

        // Client-side watch (legacy compatibility)
        impl myko_rs::prelude::MykoReport<#report_output_type> for #struct_name {
            fn watch(&self, client: &myko_rs::client::MykoClient) -> impl tokio_stream::Stream<Item = #report_output_type> {
                client.watch_report::<#struct_name, #report_output_type>(self)
            }
        }

        impl myko_rs::prelude::ReportId for #struct_name {
            fn report_id(&self) -> String {
                stringify!(#struct_name).to_string()
            }
        }

        impl myko_rs::prelude::WithTransaction for #struct_name {
            fn tx_id(&self) -> std::sync::Arc<str> {
                self.tx.clone()
            }
        }

        impl myko_rs::prelude::ReportIdStatic for #struct_name {
            fn report_id_static() -> &'static str {
                stringify!(#struct_name)
            }
        }

        impl myko_rs::prelude::ReportOutputType for #struct_name {
            type Output = #report_output_type;
        }

        impl From<myko_rs::prelude::WrappedReport> for #struct_name {
            fn from(wrapped_report: myko_rs::prelude::WrappedReport) -> Self {
                serde_json::from_value::<Self>(wrapped_report.report).expect("Failed to deserialize report")
            }
        }

        // Report trait impl - requires ReportHandler to be implemented by the user
        impl myko_rs::prelude::Report for #struct_name {
            fn watch(&self, client: &myko_rs::client::MykoClient) -> impl tokio_stream::Stream<Item = #report_output_type> {
                client.watch_report::<Self, #report_output_type>(self)
            }
        }
    };

    expanded
}
