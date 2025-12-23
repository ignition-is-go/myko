use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemStruct, Path};

pub fn myko_report_impl(report_output_type: Path, input_struct: ItemStruct) -> TokenStream {
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

    // Generate report registration using ReportFactory trait
    let report_registration = quote! {
        myko_rs::prelude::ReportRegistration {
            report_id: stringify!(#struct_name),
            crate_name: module_path!(),
            output_type: stringify!(#report_output_type),
            output_type_crate: module_path!(),
            factory: || -> myko_rs::prelude::RegisterReportData {
                use myko_rs::prelude::ReportFactory;
                #struct_name::create_registration()
            },
        }
    };

    // Generate the implementation
    // Note: We don't generate Args type or inject tx anymore.
    // Those are handled by ReportRequest<R> wrapper.
    let expanded = quote! {
        #derives
        #input_struct

        myko_rs::submit! {
            #report_registration
        }

        // Register for ts-rs export (just the params type now)
        myko_rs::register_ts_export!(#struct_name);

        // Impl ReportId
        impl myko_rs::prelude::ReportId for #struct_name {
            fn report_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
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

        // Note: WithTransaction, AnyReport, and Report are implemented on ReportRequest<#struct_name>
        // via blanket impls in myko_rs. The user's struct just implements the identity traits.
        // ReportHandler must still be implemented by the user.
        // ReportFactory is implemented via blanket impl and provides create_registration().
    };

    expanded
}
