use proc_macro2::TokenStream;
use quote::quote;
use syn::{Fields, ItemStruct, Path, Token, parse::Parse, punctuated::Punctuated};

pub struct ServiceArgs {
    items: Punctuated<Path, Token![,]>,
}

impl Parse for ServiceArgs {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let items = Punctuated::<Path, Token![,]>::parse_terminated(input)?;
        if items.is_empty() {
            return Err(input.error("a Myko service must contain at least one item"));
        }
        Ok(Self { items })
    }
}

pub fn expand(args: ServiceArgs, service: ItemStruct) -> syn::Result<TokenStream> {
    let empty = match &service.fields {
        Fields::Unit => true,
        Fields::Named(fields) => fields.named.is_empty(),
        Fields::Unnamed(fields) => fields.unnamed.is_empty(),
    };
    if !empty {
        return Err(syn::Error::new_spanned(
            service,
            "Myko services are zero-sized type markers",
        ));
    }

    let name = &service.ident;
    let items = args.items;
    let item_tuple = items.iter().map(|item| quote!(#item,));
    let item_checks = items.iter().map(|item| quote!(require_item::<#item>();));
    let krate = crate::myko_path();

    Ok(quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #service

        impl #krate::MykoService for #name {
            type Items = (#(#item_tuple)*);
            const SERVICE_ID: #krate::ServiceTypeId = #krate::ServiceTypeId::new(
                concat!(module_path!(), "::", stringify!(#name)),
            );
        }

        const _: fn() = || {
            fn require_item<I: #krate::MykoItem<Service = #name>>() {}
            #(#item_checks)*
        };
    })
}
