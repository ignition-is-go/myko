use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    GenericArgument, ImplItem, ItemImpl, ItemStruct, Path, PathArguments, Token, Type,
    punctuated::Punctuated,
};

use crate::myko_path;

fn graph_query_common(
    krate: &syn::Path,
    query_ident: &syn::Ident,
    edge_type: &Type,
) -> TokenStream {
    quote! {
        impl #krate::query::QueryId for #query_ident {
            fn query_id(&self) -> std::sync::Arc<str> {
                stringify!(#query_ident).into()
            }
        }

        impl #krate::query::QueryIdStatic for #query_ident {
            fn query_id_static() -> std::sync::Arc<str> {
                stringify!(#query_ident).into()
            }
        }

        impl #krate::query::QueryItemType for #query_ident {
            type Item = #edge_type;

            fn query_item_type(&self) -> std::sync::Arc<str> {
                Self::query_item_type_static()
            }

            fn query_item_type_static() -> std::sync::Arc<str> {
                <#edge_type as #krate::item::Eventable>::ENTITY_NAME_STATIC.into()
            }
        }

        impl #krate::cache::CacheKey for #query_ident {
            fn cache_key(&self, state: &mut dyn std::hash::Hasher) {
                #krate::cache::write_serde_cache_key(self, state);
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #krate::graph::GraphQueryRegistration {
                query_id: stringify!(#query_ident),
                edge_type: <#edge_type as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                parse: <#query_ident as #krate::query::QueryFactory>::parse,
                cell_factory: <#query_ident as #krate::query::QueryFactory>::cell_factory,
                window_cell_factory: <#query_ident as #krate::graph::GraphWindowQueryFactory>::window_cell_factory,
            }
        }
    }
}

fn edge_endpoint_types(input: &ItemImpl) -> Option<(Type, Type)> {
    let ends = input.items.iter().find_map(|item| match item {
        ImplItem::Type(associated) if associated.ident == "Ends" => Some(&associated.ty),
        _ => None,
    })?;
    let Type::Path(path) = ends else {
        return None;
    };
    let PathArguments::AngleBracketed(arguments) = &path.path.segments.last()?.arguments else {
        return None;
    };
    let mut endpoints = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(endpoint) => Some(endpoint.clone()),
        _ => None,
    });
    Some((endpoints.next()?, endpoints.next()?))
}

fn endpoint_entity_type(endpoint: &Type) -> Option<Type> {
    let Type::Path(path) = endpoint else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "ConcreteEndpoint" && segment.ident != "QualifiedEndpoint" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(entity) => Some(entity.clone()),
        _ => None,
    })
}

struct RelatedQuerySpec<'a> {
    query_ident: syn::Ident,
    address_ident: syn::Ident,
    source_endpoint: &'a Type,
    target_endpoint: &'a Type,
    target_entity: &'a Type,
    edge_position: TokenStream,
    related_position: TokenStream,
    client_trait: TokenStream,
    client_method: syn::Ident,
}

fn graph_related_query_tokens(
    ctx: &crate::DeriveCtx,
    edge_type: &Type,
    spec: RelatedQuerySpec<'_>,
) -> TokenStream {
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));
    let RelatedQuerySpec {
        query_ident,
        address_ident,
        source_endpoint,
        target_endpoint,
        target_entity,
        edge_position,
        related_position,
        client_trait,
        client_method,
    } = spec;
    let common = graph_query_common(krate, &query_ident, target_entity);

    quote! {
        /// Ordinary Myko query for typed entities reached through matching edges.
        #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize)]
        #serde_rename_attr
        pub struct #query_ident {
            pub endpoint: #address_ident,
        }

        impl #query_ident {
            #[must_use]
            pub fn new(endpoint: #address_ident) -> Self {
                Self { endpoint }
            }
        }

        #common

        impl #krate::graph::GraphWindowQueryFactory for #query_ident {
            fn window_cell_factory(
                _query: std::sync::Arc<dyn #krate::query::AnyQuery>,
                _registry: std::sync::Arc<#krate::store::StoreRegistry>,
                _request: std::sync::Arc<#krate::request::RequestContext>,
                _server: std::sync::Arc<#krate::server::MykoServerContext>,
                _window: #krate::wire::QueryWindow,
            ) -> Result<Option<#krate::query::WindowedQuerySource>, String> {
                Ok(None)
            }
        }

        impl #krate::query::QueryHandler for #query_ident {
            #[cfg(not(target_arch = "wasm32"))]
            fn build_view(
                ctx: #krate::query::QueryBuildArgs<Self>,
            ) -> Option<impl #krate::prelude::MapQuery<
                Key = std::sync::Arc<str>,
                Value = std::sync::Arc<dyn #krate::item::AnyItem>,
            >>
            where
                Self: Send + Sync + 'static,
            {
                let endpoint = <#source_endpoint as #krate::graph::EndpointSpec>::erase(
                    &ctx.query.endpoint,
                ).ok()?;
                ctx.query_context
                    .graph_related_at::<#edge_type, #target_endpoint>(
                        #edge_position,
                        &endpoint,
                        #related_position,
                    )
                    .ok()
            }
        }

        impl #client_trait for #edge_type {
            type Query = #query_ident;

            fn #client_method(endpoint: &#address_ident) -> Self::Query {
                #query_ident::new(endpoint.clone())
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn graph_aggregate_tokens(
    ctx: &crate::DeriveCtx,
    edge_name: &syn::Ident,
    edge_type: &Type,
) -> TokenStream {
    let krate = &ctx.krate;
    let a_address = format_ident!("{}AAddress", edge_name);
    let b_address = format_ident!("{}BAddress", edge_name);
    let count_from = format_ident!("{}GraphCountFrom", edge_name);
    let count_to = format_ident!("{}GraphCountTo", edge_name);
    let count_between = format_ident!("{}GraphCountBetween", edge_name);
    let exists_between = format_ident!("{}GraphExistsBetween", edge_name);

    quote! {
        /// Live number of edges whose A endpoint matches `endpoint`.
        #[#krate::myko_non_hash_cache_key]
        #[#krate::myko_report(usize)]
        pub struct #count_from {
            pub endpoint: #a_address,
        }

        impl #krate::prelude::ReportHandler for #count_from {
            type Output = usize;

            fn compute(
                &self,
                ctx: #krate::prelude::ReportContext,
            ) -> impl #krate::prelude::Materialize<
                std::sync::Arc<Self::Output>,
                #krate::prelude::Definite,
            > {
                use #krate::prelude::{GraphQuerying, MapExt};
                ctx.edges::<#edge_type>()
                    .watch_count_from(&self.endpoint)
                    .unwrap_or_else(|_| #krate::hyphae::Cell::new(0).lock())
                    .map(|count| std::sync::Arc::new(*count))
            }
        }

        /// Live number of edges whose B endpoint matches `endpoint`.
        #[#krate::myko_non_hash_cache_key]
        #[#krate::myko_report(usize)]
        pub struct #count_to {
            pub endpoint: #b_address,
        }

        impl #krate::prelude::ReportHandler for #count_to {
            type Output = usize;

            fn compute(
                &self,
                ctx: #krate::prelude::ReportContext,
            ) -> impl #krate::prelude::Materialize<
                std::sync::Arc<Self::Output>,
                #krate::prelude::Definite,
            > {
                use #krate::prelude::{GraphQuerying, MapExt};
                ctx.edges::<#edge_type>()
                    .watch_count_to(&self.endpoint)
                    .unwrap_or_else(|_| #krate::hyphae::Cell::new(0).lock())
                    .map(|count| std::sync::Arc::new(*count))
            }
        }

        /// Live number of edges matching one exact A/B pair.
        #[#krate::myko_non_hash_cache_key]
        #[#krate::myko_report(usize)]
        pub struct #count_between {
            pub a: #a_address,
            pub b: #b_address,
        }

        impl #krate::prelude::ReportHandler for #count_between {
            type Output = usize;

            fn compute(
                &self,
                ctx: #krate::prelude::ReportContext,
            ) -> impl #krate::prelude::Materialize<
                std::sync::Arc<Self::Output>,
                #krate::prelude::Definite,
            > {
                use #krate::prelude::{GraphQuerying, MapExt};
                ctx.edges::<#edge_type>()
                    .watch_count_between(&self.a, &self.b)
                    .unwrap_or_else(|_| #krate::hyphae::Cell::new(0).lock())
                    .map(|count| std::sync::Arc::new(*count))
            }
        }

        /// Live existence check for one exact A/B pair.
        #[#krate::myko_non_hash_cache_key]
        #[#krate::myko_report(bool)]
        pub struct #exists_between {
            pub a: #a_address,
            pub b: #b_address,
        }

        impl #krate::prelude::ReportHandler for #exists_between {
            type Output = bool;

            fn compute(
                &self,
                ctx: #krate::prelude::ReportContext,
            ) -> impl #krate::prelude::Materialize<
                std::sync::Arc<Self::Output>,
                #krate::prelude::Definite,
            > {
                use #krate::prelude::{GraphQuerying, MapExt};
                ctx.edges::<#edge_type>()
                    .watch_count_between(&self.a, &self.b)
                    .unwrap_or_else(|_| #krate::hyphae::Cell::new(0).lock())
                    .map(|count| std::sync::Arc::new(*count != 0))
            }
        }

        impl #krate::graph::GraphClientAggregates for #edge_type {
            type CountFromReport = #count_from;
            type CountToReport = #count_to;
            type CountBetweenReport = #count_between;
            type ExistsBetweenReport = #exists_between;

            fn count_from_report(endpoint: &#a_address) -> Self::CountFromReport {
                #count_from { endpoint: endpoint.clone() }
            }

            fn count_to_report(endpoint: &#b_address) -> Self::CountToReport {
                #count_to { endpoint: endpoint.clone() }
            }

            fn count_between_report(
                a: &#a_address,
                b: &#b_address,
            ) -> Self::CountBetweenReport {
                #count_between { a: a.clone(), b: b.clone() }
            }

            fn exists_between_report(
                a: &#a_address,
                b: &#b_address,
            ) -> Self::ExistsBetweenReport {
                #exists_between { a: a.clone(), b: b.clone() }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn graph_mutation_tokens(
    ctx: &crate::DeriveCtx,
    edge_name: &syn::Ident,
    edge_type: &Type,
) -> TokenStream {
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));
    let edge_id = format_ident!("{}Id", edge_name);
    let connect = format_ident!("Connect{}", edge_name);
    let connect_many = format_ident!("Connect{}s", edge_name);
    let ensure = format_ident!("Ensure{}", edge_name);
    let ensure_result = format_ident!("Ensure{}Result", edge_name);
    let delete = format_ident!("Delete{}", edge_name);
    let delete_result = format_ident!("Delete{}Result", edge_name);
    let delete_many = format_ident!("Delete{}s", edge_name);
    let delete_many_result = format_ident!("Delete{}sResult", edge_name);

    quote! {
        /// Authoritative graph upsert using Myko's ordinary command protocol.
        #[#krate::myko_command]
        pub struct #connect {
            pub edge: #edge_type,
        }

        impl #krate::command::CommandHandler for #connect {
            fn execute(
                self,
                ctx: #krate::prelude::CommandContext,
            ) -> Result<(), #krate::prelude::CommandError> {
                #krate::prelude::EventPublishing::emit_set(&ctx, &self.edge)
            }
        }

        /// Bulk authoritative graph upsert using one reducer batch.
        #[#krate::myko_command(usize)]
        pub struct #connect_many {
            pub edges: Vec<#edge_type>,
        }

        impl #krate::command::CommandHandler for #connect_many {
            fn execute(
                self,
                ctx: #krate::prelude::CommandContext,
            ) -> Result<usize, #krate::prelude::CommandError> {
                let affected = self.edges.len();
                #krate::prelude::EventPublishing::emit_set_batch(&ctx, &self.edges)?;
                Ok(affected)
            }
        }

        #[derive(
            Clone,
            PartialEq,
            Eq,
            Debug,
            #serde_path::Serialize,
            #serde_path::Deserialize,
            #krate::TS,
        )]
        #[ts(crate = "myko::ts_rs")]
        #serde_rename_attr
        pub struct #ensure_result {
            pub id: #edge_id,
            pub created: bool,
        }
        #krate::register_typegen_type!(#ensure_result);

        /// Idempotently establish a unique edge pair.
        ///
        /// If another command wins the unique-pair reservation concurrently,
        /// this returns that edge instead of surfacing a duplicate error.
        #[#krate::myko_command(#ensure_result)]
        pub struct #ensure {
            pub edge: #edge_type,
        }

        impl #krate::command::CommandHandler for #ensure {
            fn execute(
                self,
                ctx: #krate::prelude::CommandContext,
            ) -> Result<#ensure_result, #krate::prelude::CommandError> {
                let (a, b) = <#edge_type as #krate::graph::GraphEdge>::ends(&self.edge);
                let scope = <#edge_type as #krate::graph::GraphEdge>::scope(&self.edge);
                if let Some(existing) = ctx.graph_unique_edge::<#edge_type>(
                    &a,
                    &b,
                    scope.as_ref(),
                )? {
                    return Ok(#ensure_result {
                        id: #krate::prelude::WithTypedId::typed_id(existing.as_ref()),
                        created: false,
                    });
                }

                match #krate::prelude::EventPublishing::emit_set(&ctx, &self.edge) {
                    Ok(()) => Ok(#ensure_result {
                        id: #krate::prelude::WithTypedId::typed_id(&self.edge),
                        created: true,
                    }),
                    Err(error) => {
                        if let Ok(Some(existing)) = ctx.graph_unique_edge::<#edge_type>(
                            &a,
                            &b,
                            scope.as_ref(),
                        ) {
                            Ok(#ensure_result {
                                id: #krate::prelude::WithTypedId::typed_id(existing.as_ref()),
                                created: false,
                            })
                        } else {
                            Err(error)
                        }
                    }
                }
            }
        }

        impl #krate::graph::GraphClientMutations for #edge_type {
            type ConnectCommand = #connect;
            type ConnectManyCommand = #connect_many;
            type EnsureResult = #ensure_result;
            type EnsureCommand = #ensure;
            type DisconnectResult = #delete_result;
            type DisconnectCommand = #delete;
            type DisconnectManyResult = #delete_many_result;
            type DisconnectManyCommand = #delete_many;

            fn connect_command(edge: &Self) -> Self::ConnectCommand {
                #connect { edge: edge.clone() }
            }

            fn connect_many_command(edges: &[Self]) -> Self::ConnectManyCommand {
                #connect_many { edges: edges.to_vec() }
            }

            fn ensure_command(edge: &Self) -> Self::EnsureCommand {
                #ensure { edge: edge.clone() }
            }

            fn disconnect_command(id: &Self::Id) -> Self::DisconnectCommand {
                #delete { id: id.clone() }
            }

            fn disconnect_many_command(ids: &[Self::Id]) -> Self::DisconnectManyCommand {
                #delete_many { ids: ids.to_vec() }
            }
        }
    }
}

pub fn category(input: &ItemStruct) -> TokenStream {
    let krate = myko_path();
    let name = &input.ident;
    quote! {
        #input

        impl #krate::graph::EntityCategory for #name {
            const ID: &'static str = concat!(module_path!(), "::", stringify!(#name));
            const NAME: &'static str = stringify!(#name);
        }

        #krate::submit! {
            #krate::graph::EntityCategoryRegistration {
                id: <#name as #krate::graph::EntityCategory>::ID,
                name: <#name as #krate::graph::EntityCategory>::NAME,
                crate_path: module_path!(),
            }
        }
    }
}

pub fn category_membership(
    categories: &Punctuated<Path, Token![,]>,
    input: &ItemStruct,
) -> TokenStream {
    let krate = myko_path();
    let name = &input.ident;
    let implementations = categories.iter().map(|category| {
        quote! {
            impl #krate::graph::InCategory<#category> for #name {}
            #krate::submit! {
                #krate::graph::ItemCategoryRegistration {
                    item_type: <#name as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                    entity_category_id: <#category as #krate::graph::EntityCategory>::ID,
                    crate_path: module_path!(),
                }
            }
        }
    });
    quote! {
        #input
        #(#implementations)*
    }
}

#[allow(clippy::too_many_lines)]
pub fn edge(mut input: ItemImpl) -> TokenStream {
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));
    let edge_type = (*input.self_ty).clone();
    let edge_name = match &edge_type {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.clone()),
        _ => None,
    };
    let has_scope = input
        .items
        .iter()
        .any(|item| matches!(item, ImplItem::Type(associated) if associated.ident == "Scope"));
    let has_validator = input
        .items
        .iter()
        .any(|item| matches!(item, ImplItem::Type(associated) if associated.ident == "Validator"));

    if !has_scope {
        input
            .items
            .push(syn::parse_quote!(type Scope = #krate::graph::NoScope;));
    }
    if !has_validator {
        input
            .items
            .push(syn::parse_quote!(type Validator = #krate::graph::NoEdgeValidator;));
    }

    let validator = if has_validator {
        quote!(Some(#krate::graph::validate_edge::<#edge_type>))
    } else {
        quote!(None)
    };

    let address_aliases = edge_name.as_ref().map_or_else(TokenStream::new, |edge_name| {
        let a_address = format_ident!("{}AAddress", edge_name);
        let b_address = format_ident!("{}BAddress", edge_name);
        quote! {
            pub type #a_address =
                <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::A as #krate::graph::EndpointSpec>::Value;
            pub type #b_address =
                <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::B as #krate::graph::EndpointSpec>::Value;
        }
    });

    let graph_queries = edge_name.as_ref().map_or_else(TokenStream::new, |edge_name| {
        let a_address = format_ident!("{}AAddress", edge_name);
        let b_address = format_ident!("{}BAddress", edge_name);
        let from_query = format_ident!("{}GraphFrom", edge_name);
        let to_query = format_ident!("{}GraphTo", edge_name);
        let between_query = format_ident!("{}GraphBetween", edge_name);
        let from_common = graph_query_common(krate, &from_query, &edge_type);
        let to_common = graph_query_common(krate, &to_query, &edge_type);
        let between_common = graph_query_common(krate, &between_query, &edge_type);

        quote! {
            /// Ordinary Myko query for edges whose A endpoint matches `endpoint`.
            #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize)]
            #serde_rename_attr
            pub struct #from_query {
                pub endpoint: #a_address,
            }

            impl #from_query {
                #[must_use]
                pub fn new(endpoint: #a_address) -> Self {
                    Self { endpoint }
                }
            }

            #from_common

            impl #krate::graph::GraphWindowQueryFactory for #from_query {
                fn window_cell_factory(
                    query: std::sync::Arc<dyn #krate::query::AnyQuery>,
                    registry: std::sync::Arc<#krate::store::StoreRegistry>,
                    request: std::sync::Arc<#krate::request::RequestContext>,
                    server: std::sync::Arc<#krate::server::MykoServerContext>,
                    window: #krate::wire::QueryWindow,
                ) -> Result<Option<#krate::query::WindowedQuerySource>, String> {
                    #krate::graph::graph_window_query_at::<Self, #edge_type, _>(
                        &query,
                        registry,
                        request,
                        server,
                        window,
                        #krate::graph::EndPosition::A,
                        |query| <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::A as #krate::graph::EndpointSpec>::erase(&query.endpoint),
                    )
                }
            }

            impl #krate::query::QueryHandler for #from_query {
                fn test_entity(ctx: #krate::query::QueryTestContext<Self>) -> bool {
                    let Ok(expected) =
                        <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::A as #krate::graph::EndpointSpec>::erase(&ctx.query.endpoint)
                    else {
                        return false;
                    };
                    <<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::EdgeEnds>::erase(&ctx.item.ends())
                        .is_ok_and(|actual| actual.a == expected)
                }

                #[cfg(not(target_arch = "wasm32"))]
                fn build_view(
                    ctx: #krate::query::QueryBuildArgs<Self>,
                ) -> Option<impl #krate::prelude::MapQuery<
                    Key = std::sync::Arc<str>,
                    Value = std::sync::Arc<dyn #krate::item::AnyItem>,
                >>
                where
                    Self: Send + Sync + 'static,
                {
                    let endpoint =
                        <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::A as #krate::graph::EndpointSpec>::erase(&ctx.query.endpoint).ok()?;
                    ctx.query_context
                        .graph_watch_at::<#edge_type>(#krate::graph::EndPosition::A, &endpoint)
                        .ok()
                }
            }

            /// Ordinary Myko query for edges whose B endpoint matches `endpoint`.
            #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize)]
            #serde_rename_attr
            pub struct #to_query {
                pub endpoint: #b_address,
            }

            impl #to_query {
                #[must_use]
                pub fn new(endpoint: #b_address) -> Self {
                    Self { endpoint }
                }
            }

            #to_common

            impl #krate::graph::GraphWindowQueryFactory for #to_query {
                fn window_cell_factory(
                    query: std::sync::Arc<dyn #krate::query::AnyQuery>,
                    registry: std::sync::Arc<#krate::store::StoreRegistry>,
                    request: std::sync::Arc<#krate::request::RequestContext>,
                    server: std::sync::Arc<#krate::server::MykoServerContext>,
                    window: #krate::wire::QueryWindow,
                ) -> Result<Option<#krate::query::WindowedQuerySource>, String> {
                    #krate::graph::graph_window_query_at::<Self, #edge_type, _>(
                        &query,
                        registry,
                        request,
                        server,
                        window,
                        #krate::graph::EndPosition::B,
                        |query| <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::B as #krate::graph::EndpointSpec>::erase(&query.endpoint),
                    )
                }
            }

            impl #krate::query::QueryHandler for #to_query {
                fn test_entity(ctx: #krate::query::QueryTestContext<Self>) -> bool {
                    let Ok(expected) =
                        <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::B as #krate::graph::EndpointSpec>::erase(&ctx.query.endpoint)
                    else {
                        return false;
                    };
                    <<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::EdgeEnds>::erase(&ctx.item.ends())
                        .is_ok_and(|actual| actual.b == expected)
                }

                #[cfg(not(target_arch = "wasm32"))]
                fn build_view(
                    ctx: #krate::query::QueryBuildArgs<Self>,
                ) -> Option<impl #krate::prelude::MapQuery<
                    Key = std::sync::Arc<str>,
                    Value = std::sync::Arc<dyn #krate::item::AnyItem>,
                >>
                where
                    Self: Send + Sync + 'static,
                {
                    let endpoint =
                        <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::B as #krate::graph::EndpointSpec>::erase(&ctx.query.endpoint).ok()?;
                    ctx.query_context
                        .graph_watch_at::<#edge_type>(#krate::graph::EndPosition::B, &endpoint)
                        .ok()
                }
            }

            /// Ordinary Myko query for edges matching one exact A/B pair.
            #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize)]
            #serde_rename_attr
            pub struct #between_query {
                pub a: #a_address,
                pub b: #b_address,
            }

            impl #between_query {
                #[must_use]
                pub fn new(a: #a_address, b: #b_address) -> Self {
                    Self { a, b }
                }
            }

            #between_common

            impl #krate::graph::GraphWindowQueryFactory for #between_query {
                fn window_cell_factory(
                    query: std::sync::Arc<dyn #krate::query::AnyQuery>,
                    registry: std::sync::Arc<#krate::store::StoreRegistry>,
                    request: std::sync::Arc<#krate::request::RequestContext>,
                    server: std::sync::Arc<#krate::server::MykoServerContext>,
                    window: #krate::wire::QueryWindow,
                ) -> Result<Option<#krate::query::WindowedQuerySource>, String> {
                    #krate::graph::graph_window_query_between::<Self, #edge_type, _>(
                        &query,
                        registry,
                        request,
                        server,
                        window,
                        |query| Ok((
                            <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::A as #krate::graph::EndpointSpec>::erase(&query.a)?,
                            <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::B as #krate::graph::EndpointSpec>::erase(&query.b)?,
                        )),
                    )
                }
            }

            impl #krate::query::QueryHandler for #between_query {
                fn test_entity(ctx: #krate::query::QueryTestContext<Self>) -> bool {
                    let Ok(a) =
                        <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::A as #krate::graph::EndpointSpec>::erase(&ctx.query.a)
                    else {
                        return false;
                    };
                    let Ok(b) =
                        <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::B as #krate::graph::EndpointSpec>::erase(&ctx.query.b)
                    else {
                        return false;
                    };
                    <<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::EdgeEnds>::erase(&ctx.item.ends())
                        .is_ok_and(|actual| {
                            (actual.a == a && actual.b == b)
                                || (<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::EdgeEnds>::SHAPE
                                    == #krate::graph::EdgeShapeKind::Undirected
                                    && actual.a == b
                                    && actual.b == a)
                        })
                }

                #[cfg(not(target_arch = "wasm32"))]
                fn build_view(
                    ctx: #krate::query::QueryBuildArgs<Self>,
                ) -> Option<impl #krate::prelude::MapQuery<
                    Key = std::sync::Arc<str>,
                    Value = std::sync::Arc<dyn #krate::item::AnyItem>,
                >>
                where
                    Self: Send + Sync + 'static,
                {
                    let a =
                        <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::A as #krate::graph::EndpointSpec>::erase(&ctx.query.a).ok()?;
                    let b =
                        <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::B as #krate::graph::EndpointSpec>::erase(&ctx.query.b).ok()?;
                    ctx.query_context.graph_watch_between::<#edge_type>(&a, &b).ok()
                }
            }

            impl #krate::graph::GraphClientQueries for #edge_type {
                type FromQuery = #from_query;
                type ToQuery = #to_query;
                type BetweenQuery = #between_query;

                fn from_query(endpoint: &#a_address) -> Self::FromQuery {
                    #from_query::new(endpoint.clone())
                }

                fn to_query(endpoint: &#b_address) -> Self::ToQuery {
                    #to_query::new(endpoint.clone())
                }

                fn between_query(a: &#a_address, b: &#b_address) -> Self::BetweenQuery {
                    #between_query::new(a.clone(), b.clone())
                }
            }
        }
    });
    let related_endpoint_types = edge_endpoint_types(&input);
    let targets_from_available = edge_name.is_some()
        && related_endpoint_types
            .as_ref()
            .is_some_and(|(_, endpoint)| endpoint_entity_type(endpoint).is_some());
    let sources_to_available = edge_name.is_some()
        && related_endpoint_types
            .as_ref()
            .is_some_and(|(endpoint, _)| endpoint_entity_type(endpoint).is_some());
    let related_queries = edge_name
        .as_ref()
        .and_then(|edge_name| {
            let (a_endpoint, b_endpoint) = related_endpoint_types.as_ref()?;
            let a_entity = endpoint_entity_type(a_endpoint);
            let b_entity = endpoint_entity_type(b_endpoint);
            let a_address = format_ident!("{}AAddress", edge_name);
            let b_address = format_ident!("{}BAddress", edge_name);
            let mut generated = TokenStream::new();
            if let Some(target_entity) = b_entity.as_ref() {
                generated.extend(graph_related_query_tokens(
                    &ctx,
                    &edge_type,
                    RelatedQuerySpec {
                        query_ident: format_ident!("{}GraphTargetsFrom", edge_name),
                        address_ident: a_address,
                        source_endpoint: a_endpoint,
                        target_endpoint: b_endpoint,
                        target_entity,
                        edge_position: quote!(#krate::graph::EndPosition::A),
                        related_position: quote!(#krate::graph::EndPosition::B),
                        client_trait: quote!(#krate::graph::GraphClientTargetsFrom),
                        client_method: format_ident!("targets_from_query"),
                    },
                ));
            }
            if let Some(target_entity) = a_entity.as_ref() {
                generated.extend(graph_related_query_tokens(
                    &ctx,
                    &edge_type,
                    RelatedQuerySpec {
                        query_ident: format_ident!("{}GraphSourcesTo", edge_name),
                        address_ident: b_address,
                        source_endpoint: b_endpoint,
                        target_endpoint: a_endpoint,
                        target_entity,
                        edge_position: quote!(#krate::graph::EndPosition::B),
                        related_position: quote!(#krate::graph::EndPosition::A),
                        client_trait: quote!(#krate::graph::GraphClientSourcesTo),
                        client_method: format_ident!("sources_to_query"),
                    },
                ));
            }
            Some(generated)
        })
        .unwrap_or_default();
    let graph_mutations = edge_name
        .as_ref()
        .map_or_else(TokenStream::new, |edge_name| {
            graph_mutation_tokens(&ctx, edge_name, &edge_type)
        });
    let graph_aggregates = edge_name
        .as_ref()
        .map_or_else(TokenStream::new, |edge_name| {
            graph_aggregate_tokens(&ctx, edge_name, &edge_type)
        });

    quote! {
        #input

        #address_aliases
        #graph_queries
        #related_queries
        #graph_mutations
        #graph_aggregates

        #krate::submit! {
            #krate::graph::EdgeRegistration {
                edge_type: <#edge_type as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                crate_path: module_path!(),
                shape: <<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::EdgeEnds>::SHAPE,
                pair_policy: <#edge_type as #krate::graph::GraphEdge>::PAIR_POLICY,
                pair_projection: <#edge_type as #krate::graph::GraphEdge>::PAIR_PROJECTION,
                endpoints: &<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::EdgeEnds>::ENDPOINTS,
                scope_type: <<#edge_type as #krate::graph::GraphEdge>::Scope as #krate::graph::EdgeScope>::scope_type,
                adjacency: <#edge_type as #krate::graph::GraphEdge>::ADJACENCY,
                self_loops: <#edge_type as #krate::graph::GraphEdge>::SELF_LOOPS,
                a_delete: <#edge_type as #krate::graph::GraphEdge>::A_DELETE,
                b_delete: <#edge_type as #krate::graph::GraphEdge>::B_DELETE,
                extract: #krate::graph::extract_edge::<#edge_type>,
                extract_scope: #krate::graph::extract_edge_scope::<#edge_type>,
                validate: #validator,
            }
        }

        #krate::submit! {
            #krate::graph::EdgeAdjacencyRegistration {
                edge_type: <#edge_type as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                a: <#edge_type as #krate::graph::GraphEdge>::A_ADJACENCY,
                b: <#edge_type as #krate::graph::GraphEdge>::B_ADJACENCY,
            }
        }

        #krate::submit! {
            #krate::graph::EdgeRelatedQueryRegistration {
                edge_type: <#edge_type as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                availability: #krate::graph::EdgeRelatedQueryAvailability {
                    targets_from: #targets_from_available,
                    sources_to: #sources_to_available,
                },
            }
        }
    }
}
