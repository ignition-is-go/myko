//! `#[myko_sequential]`: which events of an item must settle one after another.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, ExprPath, Field, ItemStruct, Token, spanned::Spanned};

use crate::DeriveCtx;

const ATTR: &str = "myko_sequential";

struct With(ExprPath);

impl syn::parse::Parse for With {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let ident: syn::Ident = input.parse()?;
        if ident != "with" {
            return Err(syn::Error::new(
                ident.span(),
                "expected `with = path::to::function`",
            ));
        }
        input.parse::<Token![=]>()?;
        input.parse().map(Self)
    }
}

fn take_attrs(attrs: &mut Vec<Attribute>) -> Vec<Attribute> {
    let (taken, kept) = std::mem::take(attrs)
        .into_iter()
        .partition(|attr| attr.path().is_ident(ATTR));
    *attrs = kept;
    taken
}

fn is_option(ty: &syn::Type) -> bool {
    let syn::Type::Path(type_path) = ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "Option")
}

/// The marked field as it appears in the struct that reads the key from a
/// wire item, and the expression that takes its value out of that struct.
fn marked_field(field: &Field) -> Option<(TokenStream, TokenStream)> {
    let ident = field.ident.as_ref()?;
    let ty = &field.ty;
    let serde_attrs = field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("serde"));
    let declaration = quote! { #(#serde_attrs)* #ident: #ty };
    let value = if is_option(ty) {
        quote! { marked.#ident? }
    } else {
        quote! { marked.#ident }
    };
    Some((declaration, value))
}

/// Removes every `#[myko_sequential]` attribute from the item and returns the
/// registration of its key function, or nothing when the item has no key.
pub fn take_registration(
    input: &mut ItemStruct,
    buffered: bool,
    ctx: &DeriveCtx,
) -> syn::Result<TokenStream> {
    let struct_attrs = take_attrs(&mut input.attrs);
    let mut marked = Vec::new();
    for field in &mut input.fields {
        for attr in take_attrs(&mut field.attrs) {
            if !matches!(attr.meta, syn::Meta::Path(_)) {
                return Err(syn::Error::new(
                    attr.span(),
                    "`#[myko_sequential]` on a field takes no arguments",
                ));
            }
            let Some(field) = marked_field(field) else {
                return Err(syn::Error::new(
                    attr.span(),
                    "`#[myko_sequential]` needs a named field",
                ));
            };
            marked.push(field);
        }
    }

    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let name = &input.ident;
    let key = match (struct_attrs.as_slice(), marked.as_slice()) {
        ([], []) => return Ok(TokenStream::new()),
        ([attr], []) => {
            let With(path) = attr.parse_args()?;
            quote! {
                <#name as #serde_path::Deserialize>::deserialize(item)
                    .ok()
                    .and_then(|item| #path(&item))
            }
        }
        ([], fields) => {
            let serde_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));
            let declarations = fields.iter().map(|(declaration, _)| declaration);
            let values = fields.iter().map(|(_, value)| value);
            let value = if fields.len() == 1 {
                quote! { #(#values)* }
            } else {
                quote! { (#(#values),*) }
            };
            quote! {{
                #[derive(#serde_path::Deserialize)]
                #serde_attr
                struct Marked { #(#declarations),* }
                <Marked as #serde_path::Deserialize>::deserialize(item)
                    .ok()
                    .and_then(|marked| Some(#value))
            }}
        }
        ([_, second, ..], _) => {
            return Err(syn::Error::new(
                second.span(),
                "an item takes one `#[myko_sequential(with = ...)]`",
            ));
        }
        ([attr], _) => {
            return Err(syn::Error::new(
                attr.span(),
                "use `#[myko_sequential(with = ...)]` on the struct or `#[myko_sequential]` on fields, not both",
            ));
        }
    };
    if buffered {
        return Err(syn::Error::new(
            name.span(),
            "`ingest_buffer_ms` applies a window of events as one set, so it cannot be combined with `#[myko_sequential]`",
        ));
    }

    let name_str = name.to_string();
    Ok(quote! {
        const _: () = {
            fn key(item: &#krate::serde_json::Value) -> #krate::hyphae::SettleKey {
                #krate::item::sequential_key(#key)
            }
            #krate::submit! {
                #krate::item::SequentialRegistration { entity_type: #name_str, key }
            }
        };
    })
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;

    fn rejection(mut input: ItemStruct, buffered: bool) -> Option<String> {
        take_registration(&mut input, buffered, &DeriveCtx::new())
            .err()
            .map(|error| error.to_string())
    }

    #[test]
    fn an_item_without_the_attribute_registers_nothing() {
        let mut input: ItemStruct = parse_quote! {
            struct Reading { lane: String }
        };
        let registration = take_registration(&mut input, true, &DeriveCtx::new());
        assert!(registration.is_ok_and(|tokens| tokens.is_empty()));
    }

    #[test]
    fn the_attribute_is_removed_from_the_item() {
        let mut input: ItemStruct = parse_quote! {
            #[myko_sequential(with = lane)]
            struct Reading { lane: String }
        };
        assert!(take_registration(&mut input, false, &DeriveCtx::new()).is_ok());
        assert!(input.attrs.is_empty());

        let mut input: ItemStruct = parse_quote! {
            struct Reading { #[myko_sequential] lane: String }
        };
        assert!(take_registration(&mut input, false, &DeriveCtx::new()).is_ok());
        assert!(input.fields.iter().all(|field| field.attrs.is_empty()));
    }

    #[test]
    fn a_buffered_item_cannot_be_sequential() {
        let input = parse_quote! {
            struct Reading { #[myko_sequential] lane: String }
        };
        assert!(rejection(input, true).is_some_and(|error| error.contains("ingest_buffer_ms")));
    }

    #[test]
    fn the_struct_form_and_the_field_form_exclude_each_other() {
        let input = parse_quote! {
            #[myko_sequential(with = lane)]
            struct Reading { #[myko_sequential] lane: String }
        };
        assert!(rejection(input, false).is_some_and(|error| error.contains("not both")));
    }

    #[test]
    fn malformed_attributes_are_rejected() {
        let field_arguments = parse_quote! {
            struct Reading { #[myko_sequential(with = lane)] lane: String }
        };
        assert!(rejection(field_arguments, false).is_some());
        let no_function = parse_quote! {
            #[myko_sequential]
            struct Reading { lane: String }
        };
        assert!(rejection(no_function, false).is_some());
        let twice = parse_quote! {
            #[myko_sequential(with = lane)]
            #[myko_sequential(with = lane)]
            struct Reading { lane: String }
        };
        assert!(rejection(twice, false).is_some());
    }
}
