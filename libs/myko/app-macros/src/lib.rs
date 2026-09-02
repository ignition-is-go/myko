#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Fields, Ident, ItemStruct, Path, Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

struct OutputArguments {
    output: Type,
    owner: Option<Path>,
}

impl Parse for OutputArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let output = input.parse()?;
        let owner = if input.is_empty() {
            None
        } else {
            input.parse::<Token![,]>()?;
            let key: Ident = input.parse()?;
            if key != "item" {
                return Err(syn::Error::new(key.span(), "expected `item = ItemType`"));
            }
            input.parse::<Token![=]>()?;
            Some(input.parse()?)
        };
        if !input.is_empty() {
            return Err(input.error("unexpected operation macro arguments"));
        }
        Ok(Self { output, owner })
    }
}

fn operation_impl(handler: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl ::myko_items::MykoOperation for #handler {
            const OPERATION_ID: &'static str = stringify!(#handler);
        }
    }
}

fn operation_value(handler: &ItemStruct) -> proc_macro2::TokenStream {
    let default = matches!(&handler.fields, Fields::Named(fields) if fields.named.is_empty())
        || matches!(&handler.fields, Fields::Unit);
    let default = default.then(|| quote!(, Default));
    quote! {
        #[derive(
            Clone,
            Debug,
            Hash,
            ::myko_items::serde::Serialize,
            ::myko_items::serde::Deserialize
            #default
        )]
        #[serde(rename_all = "camelCase")]
        #handler
    }
}

/// Declares a custom item query and associates it with that item's module.
#[proc_macro_attribute]
pub fn myko_query(item_type: TokenStream, input: TokenStream) -> TokenStream {
    let item_type = parse_macro_input!(item_type as Path);
    let item = parse_macro_input!(input as ItemStruct);
    let handler = item.ident.clone();
    let operation = operation_impl(&handler);
    let item = operation_value(&item);
    quote! {
        #item
        #operation

        const _: fn() = || {
            fn require_handler<Q: ::myko_app::QueryHandler<Item = #item_type>>() {}
            require_handler::<#handler>();
        };

        ::myko_app::__private::inventory::submit! {
            ::myko_app::HandlerRegistration::query::<#item_type, #handler>()
        }
    }
    .into()
}

/// Declares a reactive report using the v6 output-first syntax.
/// `item = ItemType` optionally associates it with an item module.
#[proc_macro_attribute]
pub fn myko_report(arguments: TokenStream, input: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(arguments as OutputArguments);
    let item = parse_macro_input!(input as ItemStruct);
    let handler = item.ident.clone();
    let output = arguments.output;
    let operation = operation_impl(&handler);
    let item = operation_value(&item);
    let registration = arguments.owner.map_or_else(
        || quote!(::myko_app::HandlerRegistration::global_report::<#handler>()),
        |owner| quote!(::myko_app::HandlerRegistration::report::<#owner, #handler>()),
    );
    quote! {
        #item
        #operation

        const _: fn() = || {
            fn require_handler<R: ::myko_app::ReportHandler<Output = #output>>() {}
            require_handler::<#handler>();
        };

        ::myko_app::__private::inventory::submit! { #registration }
    }
    .into()
}

/// Declares a reactive view using the v6 row-type-first syntax.
/// `item = ItemType` optionally associates it with an item module.
#[proc_macro_attribute]
pub fn myko_view(arguments: TokenStream, input: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(arguments as OutputArguments);
    let item = parse_macro_input!(input as ItemStruct);
    let handler = item.ident.clone();
    let output = arguments.output;
    let operation = operation_impl(&handler);
    let item = operation_value(&item);
    let registration = arguments.owner.map_or_else(
        || quote!(::myko_app::HandlerRegistration::global_view::<#handler>()),
        |owner| quote!(::myko_app::HandlerRegistration::view::<#owner, #handler>()),
    );
    quote! {
        #item
        #operation

        const _: fn() = || {
            fn require_handler<V: ::myko_app::ViewHandler<Item = #output>>() {}
            require_handler::<#handler>();
        };

        ::myko_app::__private::inventory::submit! { #registration }
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_values_restore_v6_baseline_derives() {
        let value: ItemStruct = syn::parse_quote! {
            pub struct ExampleOperation {
                pub value: String,
            }
        };
        let expanded = operation_value(&value).to_string();
        for expected in [
            "Clone",
            "Debug",
            "Hash",
            "myko_items :: serde :: Serialize",
            "myko_items :: serde :: Deserialize",
            "rename_all = \"camelCase\"",
        ] {
            assert!(
                expanded.contains(expected),
                "missing {expected}: {expanded}"
            );
        }
        assert!(!expanded.contains("Default"));
    }

    #[test]
    fn empty_operation_values_are_default_constructible() {
        let value: ItemStruct = syn::parse_quote! {
            pub struct EmptyOperation;
        };
        assert!(operation_value(&value).to_string().contains("Default"));
    }
}
