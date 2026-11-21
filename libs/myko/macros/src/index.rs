//! `#[indexed]`, `#[indexed(each)]` and `#[index(a, b)]`: field indexes a
//! query with equality filters on those fields routes through.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Attribute, Field, ItemStruct, Token, punctuated::Punctuated};

/// One field of a declared index.
pub struct IndexField {
    pub ident: syn::Ident,
    /// `#[indexed(each)]`: the field is a list, and a row is filed under
    /// each of its elements.
    pub each: Option<syn::Type>,
}

/// One declared index: a single field, or several for a compound key.
pub struct IndexDecl {
    pub fields: Vec<IndexField>,
}

impl IndexDecl {
    /// The name the index is registered under: its field names, with `[]`
    /// marking an each-field so it never shares an index with a plain one.
    fn field_names(&self) -> Vec<String> {
        self.fields
            .iter()
            .map(|field| {
                if field.each.is_some() {
                    format!("{}[]", field.ident)
                } else {
                    field.ident.to_string()
                }
            })
            .collect()
    }
}

fn vec_element(ty: &syn::Type) -> Option<syn::Type> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Vec" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    })
}

fn is_index_attr(attr: &Attribute) -> bool {
    attr.path().is_ident("indexed") || attr.path().is_ident("index")
}

/// Reads the index declarations and removes their attributes.
pub fn take_indexes(input: &mut ItemStruct) -> syn::Result<Vec<IndexDecl>> {
    let mut decls = Vec::new();
    let syn::Fields::Named(fields) = &mut input.fields else {
        return Ok(decls);
    };
    let field_types: Vec<(syn::Ident, syn::Type)> = fields
        .named
        .iter()
        .filter_map(|field| Some((field.ident.clone()?, field.ty.clone())))
        .collect();
    for field in &mut fields.named {
        for attr in field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("indexed"))
        {
            decls.push(IndexDecl {
                fields: vec![parse_indexed(field, attr)?],
            });
        }
        field.attrs.retain(|attr| !is_index_attr(attr));
    }
    for attr in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("index"))
    {
        let names = attr.parse_args_with(Punctuated::<syn::Ident, Token![,]>::parse_terminated)?;
        if names.len() < 2 {
            return Err(syn::Error::new_spanned(
                attr,
                "#[index(..)] declares a compound key of two or more fields; use #[indexed] on a single field",
            ));
        }
        let mut index_fields = Vec::new();
        for name in names {
            if !field_types.iter().any(|(ident, _)| *ident == name) {
                return Err(syn::Error::new_spanned(&name, "no such field"));
            }
            index_fields.push(IndexField {
                ident: name,
                each: None,
            });
        }
        decls.push(IndexDecl {
            fields: index_fields,
        });
    }
    input.attrs.retain(|attr| !is_index_attr(attr));
    Ok(decls)
}

fn parse_indexed(field: &Field, attr: &Attribute) -> syn::Result<IndexField> {
    let ident = field
        .ident
        .clone()
        .ok_or_else(|| syn::Error::new_spanned(field, "#[indexed] needs a named field"))?;
    let each = match &attr.meta {
        syn::Meta::Path(_) => false,
        syn::Meta::List(_) => {
            let mode: syn::Ident = attr.parse_args()?;
            if mode != "each" {
                return Err(syn::Error::new_spanned(
                    mode,
                    "expected #[indexed] or #[indexed(each)]",
                ));
            }
            true
        }
        syn::Meta::NameValue(_) => {
            return Err(syn::Error::new_spanned(
                attr,
                "expected #[indexed] or #[indexed(each)]",
            ));
        }
    };
    let each = if each {
        Some(vec_element(&field.ty).ok_or_else(|| {
            syn::Error::new_spanned(&field.ty, "#[indexed(each)] needs a Vec<T> field")
        })?)
    } else {
        None
    };
    Ok(IndexField { ident, each })
}

/// The filter type of each each-field, which replaces the list field's
/// `Unfilterable`.
pub fn each_filter_types(decls: &[IndexDecl], krate: &syn::Path) -> Vec<(syn::Ident, TokenStream)> {
    decls
        .iter()
        .flat_map(|decl| &decl.fields)
        .filter_map(|field| {
            let element = field.each.as_ref()?;
            Some((
                field.ident.clone(),
                quote!(#krate::query::ContainsFilter<#element>),
            ))
        })
        .collect()
}

/// One route arm per declared index, with the number of fields it pins.
pub fn route_arms(
    name: &syn::Ident,
    decls: &[IndexDecl],
    krate: &syn::Path,
) -> Vec<(usize, TokenStream)> {
    decls
        .iter()
        .map(|decl| {
            let field_names = decl.field_names();
            let filters: Vec<_> = (0..decl.fields.len())
                .map(|index| format_ident!("filter{index}"))
                .collect();
            let keys: Vec<_> = (0..decl.fields.len())
                .map(|index| format_ident!("keys{index}"))
                .collect();
            let refs = decl.fields.iter().map(|field| {
                let ident = &field.ident;
                quote! { self.#ident.as_ref() }
            });
            let equality = filters
                .iter()
                .map(|filter| quote! { #krate::query::EqualityKeys::equality_keys(#filter) });
            let extract_fk = if decl.fields.iter().any(|field| field.each.is_some()) {
                quote! { |_: &dyn std::any::Any| None }
            } else {
                let parts = decl.fields.iter().map(|field| {
                    let ident = &field.ident;
                    quote! { #krate::query::IndexKey::index_key(&e.#ident)? }
                });
                quote! {
                    |item: &dyn std::any::Any| {
                        let e = item.downcast_ref::<#name>()?;
                        Some(#krate::query::CompoundKey::from_iter([#(#parts),*]))
                    }
                }
            };
            let extract_each = decl
                .fields
                .iter()
                .find(|field| field.each.is_some())
                .map_or_else(
                    || quote! { None },
                    |field| {
                        let ident = &field.ident;
                        quote! {
                            Some(|item: &dyn std::any::Any| {
                                item.downcast_ref::<#name>()
                                    .map(|e| {
                                        e.#ident
                                            .iter()
                                            .filter_map(#krate::query::IndexKey::index_key)
                                            .map(|key| #krate::query::CompoundKey::from_iter([key]))
                                            .collect()
                                    })
                                    .unwrap_or_default()
                            })
                        }
                    },
                );
            let arm = quote! {
                if let (#(Some(#filters),)*) = (#(#refs,)*) {
                    if let (#(Some(#keys),)*) = (#(#equality,)*) {
                        let keys = #krate::query::cartesian_product(vec![#(#keys),*]);
                        return Some(#krate::query::BelongsToRoute {
                            field_names: &[#(#field_names),*],
                            keys,
                            extract_fk: #extract_fk,
                            extract_each: #extract_each,
                        });
                    }
                }
            };
            (decl.fields.len(), arm)
        })
        .collect()
}
