use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

pub fn derive_partial_matches_impl(input: DeriveInput) -> TokenStream {
    let name = &input.ident;

    // Extract the base struct name (remove "Partial" prefix)
    let base_name = if let Some(stripped) = name.to_string().strip_prefix("Partial") {
        syn::Ident::new(stripped, name.span())
    } else {
        panic!("PartialMatches can only be derived on structs with 'Partial' prefix");
    };

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("PartialMatches only works on structs with named fields"),
        },
        _ => panic!("PartialMatches only works on structs"),
    };

    // Generate match checks for each field
    let field_checks = fields.iter().map(|f| {
        let field_name = &f.ident;
        quote! {
            if let Some(ref value) = self.#field_name {
                if value != &item.#field_name {
                    return false;
                }
            }
        }
    });

    let expanded = quote! {
        impl #name {
            pub fn matches(&self, item: &#base_name) -> bool {
                #(#field_checks)*
                true
            }
        }
    };

    expanded
}
