use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemStruct, Path, Token, punctuated::Punctuated};

pub fn take_public(input: &mut ItemStruct) -> syn::Result<bool> {
    let mut public = false;
    let mut attrs = Vec::new();
    for attr in std::mem::take(&mut input.attrs) {
        if attr
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "myko_public")
        {
            if public || !matches!(attr.meta, syn::Meta::Path(_)) {
                return Err(syn::Error::new_spanned(
                    attr,
                    "expected one #[myko_public] without arguments",
                ));
            }
            public = true;
        } else {
            attrs.push(attr);
        }
    }
    input.attrs = attrs;
    Ok(public)
}

pub fn take_operation_access(
    input: &mut ItemStruct,
) -> syn::Result<(Vec<crate::permission::ScopeSpec>, bool)> {
    let scopes = crate::permission::take_scopes(input)?;
    let public = take_public(input)?;
    Ok((scopes, public))
}

pub fn registration(
    kind: &syn::Ident,
    name: &syn::Ident,
    scopes: &[crate::permission::ScopeSpec],
    public: bool,
    krate: &Path,
) -> TokenStream {
    let mut registration = crate::permission::registration(kind, name, scopes, krate);
    registration.extend(operation_registration(kind, name, public, krate));
    registration
}

pub fn read_registration(
    kind: &syn::Ident,
    name: &syn::Ident,
    entity: &Path,
    extra_scopes: Vec<crate::permission::ScopeSpec>,
    public: bool,
    krate: &Path,
) -> TokenStream {
    let scopes = std::iter::once(crate::permission::ScopeSpec {
        entity: entity.clone(),
        action: syn::parse_quote!(Read),
    })
    .chain(extra_scopes)
    .collect::<Vec<_>>();
    registration(kind, name, &scopes, public, krate)
}

/// Preserve additive entity requirements without giving computed output rows scopes.
pub fn view_read_registration(
    name: &syn::Ident,
    item: &Path,
    extra_scopes: &[crate::permission::ScopeSpec],
    public: bool,
    krate: &Path,
) -> TokenStream {
    let extras = extra_scopes
        .iter()
        .map(|scope| {
            let entity = &scope.entity;
            let action = &scope.action;
            quote! { #krate::auth::EntityScope {
                entity_type: <#entity as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                action: #krate::auth::EntityAction::#action,
            } }
        })
        .collect::<Vec<_>>();
    let public_registration = operation_registration(&syn::parse_quote!(View), name, public, krate);
    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #krate::auth::OperationScopeRegistration {
                kind: #krate::auth::OperationKind::View,
                operation: stringify!(#name),
                scopes: if <#item as #krate::item::Eventable>::HAS_ENTITY_PERMISSIONS {
                    &[#krate::auth::EntityScope {
                        entity_type: <#item as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                        action: #krate::auth::EntityAction::Read,
                    }, #(#extras),*]
                } else {
                    &[#(#extras),*]
                },
            }
        }
        #public_registration
    }
}

pub fn operation_registration(
    kind: &syn::Ident,
    name: &syn::Ident,
    public: bool,
    krate: &Path,
) -> TokenStream {
    if !public {
        return TokenStream::new();
    }
    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #krate::auth::public_access::PublicOperationRegistration {
                kind: #krate::auth::OperationKind::#kind,
                operation: stringify!(#name),
            }
        }
    }
}

pub fn take_public_events(input: &mut ItemStruct, krate: &Path) -> syn::Result<TokenStream> {
    let mut seen = false;
    let mut set = false;
    let mut delete = false;
    let mut attrs = Vec::new();
    for attr in std::mem::take(&mut input.attrs) {
        if attr
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "myko_public_events")
        {
            if seen {
                return Err(syn::Error::new_spanned(
                    attr,
                    "duplicate public event declaration",
                ));
            }
            seen = true;
            let changes =
                attr.parse_args_with(Punctuated::<syn::Ident, Token![,]>::parse_terminated)?;
            if changes.is_empty() {
                return Err(syn::Error::new_spanned(attr, "select set, delete, or both"));
            }
            for change in changes {
                match change.to_string().as_str() {
                    "set" if !set => set = true,
                    "delete" if !delete => delete = true,
                    _ => {
                        return Err(syn::Error::new_spanned(
                            change,
                            "expected distinct set or delete",
                        ));
                    }
                }
            }
        } else {
            attrs.push(attr);
        }
    }
    input.attrs = attrs;
    if !seen {
        return Ok(TokenStream::new());
    }
    let name = &input.ident;
    Ok(quote! {
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #krate::auth::public_access::PublicEventRegistration {
                entity_type: stringify!(#name), set: #set, delete: #delete,
            }
        }
    })
}

/// Opt in individual generated query families without exposing an entity's full API.
pub fn take_public_queries(input: &mut ItemStruct, krate: &Path) -> syn::Result<TokenStream> {
    let mut selected = std::collections::BTreeSet::new();
    let mut seen = false;
    let mut attrs = Vec::new();
    for attr in std::mem::take(&mut input.attrs) {
        if attr
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "myko_public_queries")
        {
            if seen {
                return Err(syn::Error::new_spanned(
                    attr,
                    "duplicate public query declaration",
                ));
            }
            seen = true;
            let queries =
                attr.parse_args_with(Punctuated::<syn::Ident, Token![,]>::parse_terminated)?;
            if queries.is_empty() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "select all, by_ids, or by_query",
                ));
            }
            for query in queries {
                let name = query.to_string();
                if !matches!(name.as_str(), "all" | "by_ids" | "by_query") || !selected.insert(name)
                {
                    return Err(syn::Error::new_spanned(
                        query,
                        "expected distinct all, by_ids, or by_query",
                    ));
                }
            }
        } else {
            attrs.push(attr);
        }
    }
    input.attrs = attrs;
    let entity = &input.ident;
    let mut output = TokenStream::new();
    for query in selected {
        let name = match query.as_str() {
            "all" => format_ident!("GetAll{}s", entity),
            "by_ids" => format_ident!("Get{}sByIds", entity),
            _ => format_ident!("Get{}sByQuery", entity),
        };
        output.extend(operation_registration(
            &syn::parse_quote!(Query),
            &name,
            true,
            krate,
        ));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic_in_result_fn)]
    use super::*;
    #[test]
    fn public_markers_are_explicit_and_consumed() -> syn::Result<()> {
        let mut input: ItemStruct = syn::parse_quote!(
            #[myko_public]
            struct Bootstrap {}
        );
        assert!(take_public(&mut input)?);
        assert!(input.attrs.is_empty());
        assert!(!take_public(&mut input)?);
        let mut invalid: ItemStruct = syn::parse_quote!(
            #[myko_public(anything)]
            struct Bootstrap {}
        );
        assert!(take_public(&mut invalid).is_err());
        Ok(())
    }
    #[test]
    fn entity_events_require_explicit_changes() -> syn::Result<()> {
        let path = syn::parse_quote!(myko);
        for attrs in [
            quote!(#[myko_public_events]),
            quote!(#[myko_public_events()]),
            quote!(#[myko_public_events(all)]),
            quote!(#[myko_public_events(set, set)]),
        ] {
            let mut input: ItemStruct = syn::parse_quote!(#attrs struct Remote {});
            assert!(take_public_events(&mut input, &path).is_err());
        }
        let mut input: ItemStruct = syn::parse_quote!(
            #[myko_public_events(set, delete)]
            struct Remote {}
        );
        let output = take_public_events(&mut input, &path)?.to_string();
        assert!(output.contains("PublicEventRegistration"));
        assert!(input.attrs.is_empty());
        Ok(())
    }
}
