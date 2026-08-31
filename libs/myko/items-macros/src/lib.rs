#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Fields, Ident, Item, ItemStruct, LitStr, Meta, Path, Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

struct SubtypeArguments {
    extra_derives: Vec<Path>,
}

impl Parse for SubtypeArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut extra_derives = Vec::new();
        for meta in Punctuated::<Meta, Token![,]>::parse_terminated(input)? {
            let Meta::List(list) = &meta else {
                return Err(syn::Error::new_spanned(meta, "expected `derive(...)`"));
            };
            if !list.path.is_ident("derive") {
                return Err(syn::Error::new_spanned(meta, "expected `derive(...)`"));
            }
            extra_derives
                .extend(list.parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated)?);
        }
        Ok(Self { extra_derives })
    }
}

struct CommandArguments {
    service: LitStr,
    name: LitStr,
    result: Type,
}

impl Parse for CommandArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut service: Option<LitStr> = None;
        let mut name = None;
        let mut result = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if key == "service" {
                service = Some(input.parse()?);
            } else if key == "name" {
                name = Some(input.parse()?);
            } else if key == "result" {
                result = Some(input.parse()?);
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "expected `service`, `name`, or `result`",
                ));
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self {
            service: service.ok_or_else(|| input.error("missing `service = \"...\"`"))?,
            name: name.ok_or_else(|| input.error("missing `name = \"...\"`"))?,
            result: result.unwrap_or_else(|| syn::parse_quote!(())),
        })
    }
}

enum ScopeArgument {
    Unscoped,
    Root,
    ScopedBy(Path),
}

struct ItemArguments {
    service: LitStr,
    scope: ScopeArgument,
}

impl Parse for ItemArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut service: Option<LitStr> = None;
        let mut scope = None;
        while !input.is_empty() {
            let argument: Ident = input.parse()?;
            if argument == "service" {
                if service.is_some() {
                    return Err(syn::Error::new(argument.span(), "duplicate `service`"));
                }
                input.parse::<Token![=]>()?;
                service = Some(input.parse()?);
            } else if argument == "scope_root" {
                if scope.is_some() {
                    return Err(syn::Error::new(
                        argument.span(),
                        "only one item scope declaration is supported",
                    ));
                }
                scope = Some(ScopeArgument::Root);
            } else if argument == "scoped_by" {
                if scope.is_some() {
                    return Err(syn::Error::new(
                        argument.span(),
                        "only one item scope declaration is supported",
                    ));
                }
                input.parse::<Token![=]>()?;
                scope = Some(ScopeArgument::ScopedBy(input.parse()?));
            } else {
                return Err(syn::Error::new(
                    argument.span(),
                    "expected `service = \"...\"`, `scope_root`, or `scoped_by = ItemType`",
                ));
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        let service = service.ok_or_else(|| input.error("missing `service = \"...\"`"))?;
        if service.value().is_empty() {
            return Err(syn::Error::new(
                service.span(),
                "item service cannot be empty",
            ));
        }
        Ok(Self {
            service,
            scope: scope.unwrap_or(ScopeArgument::Unscoped),
        })
    }
}

#[proc_macro_attribute]
pub fn myko_item(arguments: TokenStream, input: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(arguments as ItemArguments);
    let item = parse_macro_input!(input as ItemStruct);
    expand_item(arguments, item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declares a transport-neutral application subtype with Myko's canonical
/// serialization and baseline value semantics.
#[proc_macro_attribute]
pub fn myko_subtype(arguments: TokenStream, input: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(arguments as SubtypeArguments);
    let item = parse_macro_input!(input as Item);
    expand_subtype(arguments, item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declares a typed application command body and its stable wire contract.
#[proc_macro_attribute]
pub fn myko_command(arguments: TokenStream, input: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(arguments as CommandArguments);
    let item = parse_macro_input!(input as ItemStruct);
    expand_command(arguments, &item).into()
}

fn expand_command(arguments: CommandArguments, item: &ItemStruct) -> TokenStream2 {
    let name = item.ident.clone();
    let service = arguments.service;
    let command_name = arguments.name;
    let result = arguments.result;
    quote! {
        #[derive(Clone, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        #item

        impl ::myko_items::MykoCommand for #name {
            type Output = #result;
            const SERVICE_ID: &'static str = #service;
            const COMMAND_TYPE: &'static str = #command_name;
        }
    }
}

fn expand_subtype(arguments: SubtypeArguments, item: Item) -> syn::Result<TokenStream2> {
    let serde_attributes = match &item {
        Item::Struct(_) => quote!(#[serde(rename_all = "camelCase")]),
        Item::Enum(_) => quote!(),
        _ => {
            return Err(syn::Error::new_spanned(
                item,
                "#[myko_subtype] only supports structs and enums",
            ));
        }
    };
    let extra_derives = arguments.extra_derives;
    Ok(quote! {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            ::myko_items::serde::Serialize,
            ::myko_items::serde::Deserialize
            #(, #extra_derives)*
        )]
        #serde_attributes
        #item
    })
}

fn expand_item(arguments: ItemArguments, mut item: ItemStruct) -> syn::Result<TokenStream2> {
    let Fields::Named(fields) = &mut item.fields else {
        return Err(syn::Error::new_spanned(
            item,
            "myko items require named fields",
        ));
    };
    if fields
        .named
        .iter()
        .any(|field| field.ident.as_ref().is_some_and(|name| name == "id"))
    {
        return Err(syn::Error::new_spanned(
            item,
            "`id` is generated by `#[myko_item]`",
        ));
    }

    let name = item.ident.clone();
    let service = arguments.service;
    let id = format_ident!("{name}Id");
    fields.named.push(syn::parse_quote!(pub id: #id));
    let equal_fields = fields.named.iter().filter_map(|field| {
        let field = field.ident.as_ref()?;
        Some(quote!(self.#field == other.#field))
    });
    let equality = equal_fields
        .reduce(|left, right| quote!((#left) && (#right)))
        .unwrap_or_else(|| quote!(true));
    let scope = match arguments.scope {
        ScopeArgument::Unscoped => quote!(::myko_items::ItemScope::Unscoped),
        ScopeArgument::Root => quote!(::myko_items::ItemScope::Root),
        ScopeArgument::ScopedBy(parent) => {
            quote!(::myko_items::ItemScope::ScopedBy(<#parent as ::myko_items::MykoItem>::ITEM_TYPE))
        }
    };

    let id_definition = generate_id(&id);
    let queries = generate_queries(&name, &id, &service);
    Ok(quote! {
        #id_definition

        #[derive(Clone, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        #item

        impl ::std::cmp::PartialEq for #name {
            fn eq(&self, other: &Self) -> bool {
                #equality
            }
        }

        impl ::myko_items::MykoItem for #name {
            type Id = #id;
            const SERVICE_ID: &'static str = #service;
            const ITEM_TYPE: &'static str = stringify!(#name);
            const SCOPE: ::myko_items::ItemScope = #scope;

            fn id(&self) -> &Self::Id {
                &self.id
            }
        }

        #queries
    })
}

fn generate_id(id: &Ident) -> TokenStream2 {
    quote! {
        #[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize, Debug)]
        #[serde(transparent)]
        pub struct #id(::std::sync::Arc<str>);

        impl #id {
            #[must_use]
            pub fn new(value: impl Into<::std::sync::Arc<str>>) -> Self {
                Self(value.into())
            }
        }

        impl ::std::fmt::Display for #id {
            fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&self.0, formatter)
            }
        }

        impl ::std::convert::AsRef<str> for #id {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl ::std::convert::From<&str> for #id {
            fn from(value: &str) -> Self {
                Self(value.into())
            }
        }

        impl ::std::convert::From<::std::string::String> for #id {
            fn from(value: ::std::string::String) -> Self {
                Self(value.into())
            }
        }

        impl ::myko_items::ItemId for #id {}
    }
}

fn generate_queries(name: &Ident, id: &Ident, service: &LitStr) -> TokenStream2 {
    let get_all = format_ident!("GetAll{name}s");
    let get_one = format_ident!("Get{name}ById");
    let get_many = format_ident!("Get{name}sByIds");
    quote! {

        #[derive(Debug, Clone, Copy, Default, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize)]
        pub struct #get_all;

        impl ::myko_items::ItemQuery for #get_all {
            type Item = #name;
            type Output = ::std::vec::Vec<#name>;
            const QUERY_ID: &'static str = concat!(#service, ".", stringify!(#get_all));

            fn execute(self, projection: &::myko_items::ItemProjection<Self::Item>) -> Self::Output {
                projection.values().cloned().collect()
            }
        }

        #[derive(Debug, Clone, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize)]
        pub struct #get_one {
            pub id: #id,
        }

        impl ::myko_items::ItemQuery for #get_one {
            type Item = #name;
            type Output = ::std::option::Option<#name>;
            const QUERY_ID: &'static str = concat!(#service, ".", stringify!(#get_one));

            fn execute(self, projection: &::myko_items::ItemProjection<Self::Item>) -> Self::Output {
                projection.get(&self.id).cloned()
            }
        }

        #[derive(Debug, Clone, Default, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize)]
        pub struct #get_many {
            pub ids: ::std::vec::Vec<#id>,
        }

        impl ::myko_items::ItemQuery for #get_many {
            type Item = #name;
            type Output = ::std::vec::Vec<#name>;
            const QUERY_ID: &'static str = concat!(#service, ".", stringify!(#get_many));

            fn execute(self, projection: &::myko_items::ItemProjection<Self::Item>) -> Self::Output {
                self.ids
                    .iter()
                    .filter_map(|id| projection.get(id).cloned())
                    .collect()
            }
        }
    }
}
