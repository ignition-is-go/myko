use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput};

pub fn derive_message_events_impl(input: &DeriveInput) -> TokenStream {
    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => {
            return syn::Error::new_spanned(input, "MessageEvents can only be derived on enums")
                .to_compile_error();
        }
    };

    let registrations = variants.iter().filter_map(|variant| {
        let variant_name = &variant.ident;
        let variant_name_str = variant_name.to_string();

        // Find the #[serde(rename = "...")] attribute
        for attr in &variant.attrs {
            if attr.path().is_ident("serde")
                && let Ok(syn::Meta::NameValue(nv)) = attr.parse_args::<syn::Meta>()
                && nv.path.is_ident("rename")
                && let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit_str),
                    ..
                }) = &nv.value
            {
                let event_value = lit_str.value();
                // Use inventory directly since we can't reference myko from within myko
                return Some(quote! {
                    inventory::submit!(crate::wire::MessageEventRegistration {
                        variant_name: #variant_name_str,
                        event_value: #event_value,
                    });
                });
            }
        }
        None
    });

    let expanded = quote! {
        #(#registrations)*
    };

    expanded
}
