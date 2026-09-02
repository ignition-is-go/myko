#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Fields, Ident, Item, ItemStruct, Meta, Path, Token, Type,
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
    result: Type,
    owner: CommandOwner,
    scope: Option<Path>,
}

enum CommandOwner {
    Item(Path),
    Service(Path),
}

impl Parse for CommandArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut result = None;
        let mut owner = None;
        let mut scope = None;
        let lookahead = input.fork();
        let starts_with_named_argument = lookahead.parse::<Ident>().is_ok_and(|key| {
            (key == "item" || key == "service" || key == "scope") && lookahead.peek(Token![=])
        });
        if !input.is_empty() && !starts_with_named_argument {
            result = Some(input.parse()?);
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if key == "item" {
                if owner.is_some() {
                    return Err(syn::Error::new(
                        key.span(),
                        "a command must name exactly one `item` or `service` owner",
                    ));
                }
                owner = Some(CommandOwner::Item(input.parse()?));
            } else if key == "service" {
                if owner.is_some() {
                    return Err(syn::Error::new(
                        key.span(),
                        "a command must name exactly one `item` or `service` owner",
                    ));
                }
                owner = Some(CommandOwner::Service(input.parse()?));
            } else if key == "scope" {
                if scope.is_some() {
                    return Err(syn::Error::new(key.span(), "duplicate `scope`"));
                }
                scope = Some(input.parse()?);
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "expected `item = ItemType`, `service = ServiceType`, or `scope = ScopeType`",
                ));
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
                if input.is_empty() {
                    return Err(input.error("unexpected trailing command argument separator"));
                }
            }
        }
        let owner = owner
            .ok_or_else(|| input.error("missing `item = ItemType` or `service = ServiceType`"))?;
        match (&owner, &scope) {
            (CommandOwner::Item(_), Some(_)) => {
                return Err(input.error(
                    "`scope` is inferred from the command item; remove the explicit `scope`",
                ));
            }
            (CommandOwner::Service(_), None) => {
                return Err(input.error("service commands must declare `scope = ScopeItem`"));
            }
            _ => {}
        }
        Ok(Self {
            result: result.unwrap_or_else(|| syn::parse_quote!(())),
            owner,
            scope,
        })
    }
}

enum ScopeArgument {
    Unscoped,
    Root,
    ScopedBy(Path),
}

struct ItemArguments {
    service: Path,
    scope: ScopeArgument,
}

impl Parse for ItemArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut service: Option<Path> = None;
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
                    "expected `service = ServiceType`, `scope_root`, or `scoped_by = ItemType`",
                ));
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        let service = service.ok_or_else(|| input.error("missing `service = ServiceType`"))?;
        Ok(Self {
            service,
            scope: scope.unwrap_or(ScopeArgument::Unscoped),
        })
    }
}

struct ServiceArguments {
    items: Punctuated<Path, Token![,]>,
}

impl Parse for ServiceArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let items = Punctuated::<Path, Token![,]>::parse_terminated(input)?;
        if items.is_empty() {
            return Err(input.error("a Myko service must contain at least one item module"));
        }
        Ok(Self { items })
    }
}

/// Declares a typed atomicity boundary and the item modules it contains.
#[proc_macro_attribute]
pub fn myko_service(arguments: TokenStream, input: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(arguments as ServiceArguments);
    let service = parse_macro_input!(input as ItemStruct);
    expand_service(arguments, service)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
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
///
/// Item-owned commands infer both service and immediate scope family from the
/// item. Service-owned commands declare `scope = ScopeItem` because one
/// service may operate in several entity scope families.
#[proc_macro_attribute]
pub fn myko_command(arguments: TokenStream, input: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(arguments as CommandArguments);
    let item = parse_macro_input!(input as ItemStruct);
    expand_command(arguments, &item).into()
}

fn expand_command(arguments: CommandArguments, item: &ItemStruct) -> TokenStream2 {
    let name = item.ident.clone();
    let CommandArguments {
        result,
        owner,
        scope,
    } = arguments;
    let (service, item_type, scope, registration) = match owner {
        CommandOwner::Item(owner) => (
            quote!(<#owner as ::myko_items::MykoItem>::Service),
            quote!(::std::option::Option::Some(<#owner as ::myko_items::MykoItem>::ITEM_TYPE)),
            quote!(<#owner as ::myko_items::MykoItem>::Scope),
            quote!(::myko_app::HandlerRegistration::command::<#owner, #name>()),
        ),
        CommandOwner::Service(owner) => (
            quote!(#owner),
            quote!(::std::option::Option::None),
            {
                let Some(scope) = scope else {
                    return quote!(::core::compile_error!(
                        "service commands must declare `scope = ScopeItem`"
                    ));
                };
                quote!(#scope)
            },
            quote!(::myko_app::HandlerRegistration::service_command::<#owner, #name>()),
        ),
    };
    quote! {
        #[derive(Clone, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        #item

        impl ::myko_items::MykoOperation for #name {
            const OPERATION_ID: &'static str = stringify!(#name);
        }

        impl ::myko_items::MykoCommandContract for #name {
            type Output = #result;
            type Service = #service;
            type Scope = #scope;
            const ITEM_TYPE: ::std::option::Option<&'static str> = #item_type;
        }

        impl ::myko_items::MykoCommand for #name {}

        const _: fn() = || {
            fn require_handler<C: ::myko_app::CommandHandler>() {}
            require_handler::<#name>();
        };

        ::myko_app::__private::inventory::submit! {
            #registration
        }
    }
}

fn expand_service(arguments: ServiceArguments, service: ItemStruct) -> syn::Result<TokenStream2> {
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

    let name = service.ident.clone();
    let items = arguments.items;
    let item_tuple = items.iter().map(|item| quote!(#item,));
    let item_checks = items.iter().map(|item| {
        quote! {
            require_item::<#item>();
        }
    });
    Ok(quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #service

        impl ::myko_items::MykoService for #name {
            type Items = (#(#item_tuple)*);
            const SERVICE_ID: ::myko_items::ServiceTypeId = ::myko_items::ServiceTypeId::new(
                concat!(module_path!(), "::", stringify!(#name)),
            );
        }

        const _: fn() = || {
            fn require_item<I: ::myko_items::MykoItem<Service = #name>>() {}
            #(#item_checks)*
        };
    })
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
    let (scope, scope_type) = match arguments.scope {
        ScopeArgument::Unscoped => (quote!(::myko_items::ItemScope::Unscoped), quote!(#name)),
        ScopeArgument::Root => (quote!(::myko_items::ItemScope::Root), quote!(#name)),
        ScopeArgument::ScopedBy(parent) => (
            quote!(::myko_items::ItemScope::ScopedBy(<#parent as ::myko_items::MykoItem>::ITEM_TYPE)),
            quote!(#parent),
        ),
    };

    let id_definition = generate_id(&id);
    let get_all = format_ident!("GetAll{name}s");
    let get_one = format_ident!("Get{name}ById");
    let get_many = format_ident!("Get{name}sByIds");
    let queries = generate_queries(&name, &id);
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
            type Service = #service;
            type Scope = #scope_type;
            type GetAllQuery = #get_all;
            type GetByIdQuery = #get_one;
            type GetByIdsQuery = #get_many;
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

fn generate_queries(name: &Ident, id: &Ident) -> TokenStream2 {
    let get_all = format_ident!("GetAll{name}s");
    let get_one = format_ident!("Get{name}ById");
    let get_many = format_ident!("Get{name}sByIds");
    quote! {

        #[derive(Debug, Clone, Copy, Default, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize)]
        pub struct #get_all;

        impl ::myko_items::MykoOperation for #get_all {
            const OPERATION_ID: &'static str = stringify!(#get_all);
        }

        impl ::myko_items::ItemQuery for #get_all {
            type Item = #name;
            type Output = ::std::vec::Vec<#name>;

            fn execute(self, projection: &::myko_items::ItemProjection<Self::Item>) -> Self::Output {
                projection.values().cloned().collect()
            }
        }

        #[derive(Debug, Clone, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize)]
        pub struct #get_one {
            pub id: #id,
        }

        impl ::myko_items::MykoOperation for #get_one {
            const OPERATION_ID: &'static str = stringify!(#get_one);
        }

        impl ::myko_items::ItemQuery for #get_one {
            type Item = #name;
            type Output = ::std::option::Option<#name>;

            fn execute(self, projection: &::myko_items::ItemProjection<Self::Item>) -> Self::Output {
                projection.get(&self.id).cloned()
            }
        }

        #[derive(Debug, Clone, Default, ::myko_items::serde::Serialize, ::myko_items::serde::Deserialize)]
        pub struct #get_many {
            pub ids: ::std::vec::Vec<#id>,
        }

        impl ::myko_items::MykoOperation for #get_many {
            const OPERATION_ID: &'static str = stringify!(#get_many);
        }

        impl ::myko_items::ItemQuery for #get_many {
            type Item = #name;
            type Output = ::std::vec::Vec<#name>;

            fn execute(self, projection: &::myko_items::ItemProjection<Self::Item>) -> Self::Output {
                self.ids
                    .iter()
                    .filter_map(|id| projection.get(id).cloned())
                    .collect()
            }
        }
    }
}
