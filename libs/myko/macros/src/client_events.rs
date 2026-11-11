use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Ident, ItemStruct, Path, Token,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

#[derive(Clone, Copy, Default)]
pub struct Changes {
    pub set: bool,
    pub delete: bool,
}
impl Parse for Changes {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let operations = Punctuated::<Ident, Token![,]>::parse_terminated(input)?;
        if operations.is_empty() {
            return Err(input.error("select SET, DEL, or both"));
        }
        let mut changes = Self::default();
        for operation in operations {
            match operation.to_string().as_str() {
                "SET" if !changes.set => changes.set = true,
                "DEL" if !changes.delete => changes.delete = true,
                _ => {
                    return Err(syn::Error::new_spanned(
                        operation,
                        "expected distinct SET or DEL",
                    ));
                }
            }
        }
        Ok(changes)
    }
}

pub fn registration(name: &Ident, krate: &Path, changes: Changes) -> TokenStream {
    let Changes { set, delete } = changes;
    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #krate::auth::client_events::ExternalEventRegistration {
                entity_type: <#name as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                set: #set, delete: #delete,
            }
        }
    }
}

/// Consume declarations before the item macro clones deserialization helpers.
pub fn take_declaration(input: &mut ItemStruct, krate: &Path) -> syn::Result<TokenStream> {
    let mut declared = None;
    let mut attrs = Vec::new();
    for attr in std::mem::take(&mut input.attrs) {
        if attr
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "myko_external")
        {
            if declared.is_some() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "duplicate #[myko_external] declaration",
                ));
            }
            declared = Some(attr.parse_args::<Changes>()?);
        } else {
            attrs.push(attr);
        }
    }
    input.attrs = attrs;
    Ok(declared.map_or_else(TokenStream::new, |changes| {
        registration(&input.ident, krate, changes)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn external_operations_are_explicit_and_independent() {
        let set: Changes = syn::parse_quote!(SET);
        assert!(set.set && !set.delete);
        let delete: Changes = syn::parse_quote!(DEL);
        assert!(!delete.set && delete.delete);
        let both: Changes = syn::parse_quote!(DEL, SET);
        assert!(both.set && both.delete);
        for invalid in [
            quote!(),
            quote!(set),
            quote!(ALL),
            quote!(SET, SET),
            quote!(DEL, DEL),
        ] {
            assert!(syn::parse2::<Changes>(invalid).is_err());
        }
        let mut item: ItemStruct = syn::parse_quote! {
            #[myko_external(SET)]
            struct Record { value: i32 }
        };
        let registration = take_declaration(&mut item, &syn::parse_quote!(myko));
        assert!(item.attrs.is_empty());
        assert!(registration.is_ok_and(|tokens| !tokens.is_empty()));
    }
}
