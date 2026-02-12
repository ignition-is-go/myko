use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemStruct, Path};

pub fn myko_report_impl(report_output_type: Path, input_struct: ItemStruct) -> TokenStream {
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

    // Generate report registration using ReportFactory trait
    let report_registration = quote! {
        #krate::prelude::ReportRegistration {
            report_id: stringify!(#struct_name),
            crate_name: module_path!(),
            output_type: stringify!(#report_output_type),
            output_type_crate: module_path!(),
            parse: <#struct_name as #krate::report::ReportFactory>::parse,
            cell_factory: <#struct_name as #krate::report::ReportFactory>::cell_factory,
        }
    };

    let expanded = quote! {
        #derives
        #input_struct

        // Registration is server-only (requires ReportFactory which depends on hypha)
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #report_registration
        }

        // Register for ts-rs export (just the params type now)
        #krate::register_ts_export!(#struct_name);

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
    };

    expanded
}
