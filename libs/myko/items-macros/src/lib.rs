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

struct ItemArguments {
    service: Path,
    scope_root: bool,
    scoped_by: Option<Path>,
}

impl Parse for ItemArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut service: Option<Path> = None;
        let mut scope_root = false;
        let mut scoped_by = None;
        while !input.is_empty() {
            let argument: Ident = input.parse()?;
            if argument == "service" {
                if service.is_some() {
                    return Err(syn::Error::new(argument.span(), "duplicate `service`"));
                }
                input.parse::<Token![=]>()?;
                service = Some(input.parse()?);
            } else if argument == "scope_root" {
                if scope_root {
                    return Err(syn::Error::new(argument.span(), "duplicate `scope_root`"));
                }
                scope_root = true;
            } else if argument == "scoped_by" {
                if scoped_by.is_some() {
                    return Err(syn::Error::new(argument.span(), "duplicate `scoped_by`"));
                }
                input.parse::<Token![=]>()?;
                scoped_by = Some(input.parse()?);
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
            scope_root,
            scoped_by,
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
    let ItemArguments {
        service,
        scope_root,
        scoped_by,
    } = arguments;
    let id = format_ident!("{name}Id");
    fields.named.push(syn::parse_quote!(pub id: #id));
    let parent = inject_parent_field(fields, scoped_by)?;
    let equal_fields = fields.named.iter().filter_map(|field| {
        let field = field.ident.as_ref()?;
        Some(quote!(self.#field == other.#field))
    });
    let equality = equal_fields
        .reduce(|left, right| quote!((#left) && (#right)))
        .unwrap_or_else(|| quote!(true));
    let (scope, scope_type, scope_id) = item_scope_tokens(&name, scope_root, parent.as_ref());
    let decode_payload = decode_payload_tokens(parent.as_ref());
    let (belongs_to, belongs_to_impl) = belongs_to_tokens(&name, parent);

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

            fn scope_id(&self) -> &<Self::Scope as ::myko_items::MykoItem>::Id {
                #scope_id
            }

            fn belongs_to(&self) -> ::std::option::Option<::myko_items::EntityRef> {
                #belongs_to
            }

            #decode_payload
        }

        #belongs_to_impl

        #queries
    })
}

fn decode_payload_tokens(parent: Option<&(Path, Ident)>) -> TokenStream2 {
    parent.map_or_else(
        || quote!(),
        |(parent, parent_id)| {
            let serialized_field = lower_camel_case(&parent_id.to_string());
            quote! {
                fn __decode_payload(
                    payload: &[u8],
                    containing_scope: ::std::option::Option<&str>,
                ) -> ::std::result::Result<Self, ::myko_items::ItemError> {
                    let mut encoded: ::myko_items::serde_json::Value =
                        ::myko_items::serde_json::from_slice(payload)?;
                    if let (::std::option::Option::Some(containing_scope),
                        ::std::option::Option::Some(object)) =
                        (containing_scope, encoded.as_object_mut())
                        && !object.contains_key(#serialized_field)
                    {
                        let parent_id = ::myko_items::scope_item_id(
                            containing_scope,
                            <#parent as ::myko_items::MykoItem>::SERVICE_ID.as_str(),
                            <#parent as ::myko_items::MykoItem>::ITEM_TYPE,
                        )
                        .ok_or_else(|| ::myko_items::ItemError::LegacyParentScopeMismatch {
                            scope_id: containing_scope.to_owned(),
                            parent_type: <#parent as ::myko_items::MykoItem>::ITEM_TYPE,
                        })?;
                        object.insert(
                            #serialized_field.to_owned(),
                            ::myko_items::serde_json::Value::String(parent_id.to_owned()),
                        );
                    }
                    ::myko_items::serde_json::from_value(encoded).map_err(::std::convert::Into::into)
                }
            }
        },
    )
}

fn inject_parent_field(
    fields: &mut syn::FieldsNamed,
    scoped_by: Option<Path>,
) -> syn::Result<Option<(Path, Ident)>> {
    scoped_by
        .map(|parent| {
            let parent_name = parent
                .segments
                .last()
                .map(|segment| segment.ident.clone())
                .ok_or_else(|| syn::Error::new_spanned(&parent, "scoped parent type is empty"))?;
            let parent_id = format_ident!("{}_id", snake_case(&parent_name.to_string()));
            if fields
                .named
                .iter()
                .any(|field| field.ident.as_ref() == Some(&parent_id))
            {
                return Err(syn::Error::new_spanned(
                    &parent_id,
                    format!("`{parent_id}` is generated by `scoped_by = {parent_name}`"),
                ));
            }
            fields
                .named
                .push(syn::parse_quote!(pub #parent_id: <#parent as ::myko_items::MykoItem>::Id));
            Ok((parent, parent_id))
        })
        .transpose()
}

fn item_scope_tokens(
    name: &Ident,
    scope_root: bool,
    parent: Option<&(Path, Ident)>,
) -> (TokenStream2, TokenStream2, TokenStream2) {
    match (scope_root, parent) {
        (false, None) => (
            quote!(::myko_items::ItemScope::Unscoped),
            quote!(#name),
            quote!(&self.id),
        ),
        (true, None) => (
            quote!(::myko_items::ItemScope::Root),
            quote!(#name),
            quote!(&self.id),
        ),
        (false, Some((parent, parent_id))) => (
            quote!(::myko_items::ItemScope::ScopedBy {
                service_id: <#parent as ::myko_items::MykoItem>::SERVICE_ID,
                item_type: <#parent as ::myko_items::MykoItem>::ITEM_TYPE,
            }),
            quote!(#parent),
            quote!(&self.#parent_id),
        ),
        (true, Some((parent, _))) => (
            quote!(::myko_items::ItemScope::RootScopedBy {
                service_id: <#parent as ::myko_items::MykoItem>::SERVICE_ID,
                item_type: <#parent as ::myko_items::MykoItem>::ITEM_TYPE,
            }),
            quote!(#name),
            quote!(&self.id),
        ),
    }
}

fn belongs_to_tokens(name: &Ident, parent: Option<(Path, Ident)>) -> (TokenStream2, TokenStream2) {
    parent.map_or_else(
        || (quote!(::std::option::Option::None), quote!()),
        |(parent, parent_id)| {
            (
                quote!(::std::option::Option::Some(
                    <Self as ::myko_items::BelongsTo>::parent_ref(self)
                )),
                quote! {
                    impl ::myko_items::BelongsTo for #name {
                        type Parent = #parent;

                        fn parent_id(&self) -> &<#parent as ::myko_items::MykoItem>::Id {
                            &self.#parent_id
                        }
                    }
                },
            )
        },
    )
}

fn snake_case(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();
    let mut previous = None;
    while let Some(character) = characters.next() {
        let previous_is_lower_or_digit = previous.is_some_and(|previous: char| {
            previous.is_ascii_lowercase() || previous.is_ascii_digit()
        });
        let next_is_lower = characters.peek().is_some_and(char::is_ascii_lowercase);
        let starts_word = previous.is_some_and(|_| {
            character.is_ascii_uppercase() && (previous_is_lower_or_digit || next_is_lower)
        });
        if starts_word {
            output.push('_');
        }
        output.extend(character.to_lowercase());
        previous = Some(character);
    }
    output
}

fn lower_camel_case(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut uppercase_next = false;
    for character in value.chars() {
        if character == '_' {
            uppercase_next = true;
        } else if uppercase_next {
            output.extend(character.to_uppercase());
            uppercase_next = false;
        } else {
            output.push(character);
        }
    }
    output
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

            fn selected_item_ids(&self) -> ::std::option::Option<::std::vec::Vec<#id>> {
                ::std::option::Option::Some(::std::vec![self.id.clone()])
            }

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

            fn selected_item_ids(&self) -> ::std::option::Option<::std::vec::Vec<#id>> {
                ::std::option::Option::Some(self.ids.clone())
            }

            fn execute(self, projection: &::myko_items::ItemProjection<Self::Item>) -> Self::Output {
                self.ids
                    .iter()
                    .filter_map(|id| projection.get(id).cloned())
                    .collect()
            }
        }
    }
}
