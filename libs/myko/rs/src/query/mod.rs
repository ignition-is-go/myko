use crate::{
    actors::query::query_manager::{QueryManagerMsg, RegisterQueryData},
    client::MykoClient,
    common::with_transaction::WithTransaction,
    parsers::query::{AnyQuery, CapturedQueryParser, MykoQueryParser},
    prelude::AnyItem,
    server::{MykoServer, MykoServerCtx},
};
use log::error;
use serde::{Serialize, de::DeserializeOwned};
use std::{any::Any, sync::Arc};

inventory::collect!(QueryRegistration);

#[derive(Debug)]
pub struct QueryRegistration {
    pub query_id: &'static str,
    pub query_item_type: &'static str,
    pub crate_name: &'static str,
}

pub trait QueryId {
    fn query_id(&self) -> Arc<str>;
}

pub trait QueryIdStatic {
    fn query_id_static() -> Arc<str>;
}

pub trait QueryItemType {
    type Item: Send + Sync;
    fn query_item_type(&self) -> Arc<str>;
    fn query_item_type_static() -> Arc<str>;
}

/// implementing QueryHandler for a MykoQuery is required to define the logic for filtering entities based on the query.
///
/// it requires one function: test_entity which takes a `QueryHandlerContext<Self>` and returns a `bool`.
/// this answers the question of whether an entity should be included in the query results.
///
/// if `true`, updates to this query will be calculated, and the item will be added or updated as appropriate.
///
/// if `false`, updates to this query will be calculated, and the item will be removed if it exists.
///
/// any deduplication of changes to this query are handled upstream in the handler logic
pub trait QueryHandler: QueryItemType + Sized {
    fn test_entity(ctx: QueryHandlerCtx<Self>) -> bool;
}

pub struct QueryHandlerCtx<TQuery: QueryItemType> {
    pub item: Arc<TQuery::Item>,
    pub query: Arc<TQuery>,
    pub server_ctx: Arc<MykoServerCtx>,
}

#[derive(Debug)]
pub struct QueryHandlerCtxAny {
    pub item: Arc<dyn AnyItem>,
    pub query: Arc<dyn AnyQuery>,
    pub ctx: Arc<MykoServerCtx>,
}

pub trait Query:
    Serialize
    + DeserializeOwned
    + Send
    + Sync
    + QueryId
    + QueryIdStatic
    + QueryItemType
    + QueryHandler
    + WithTransaction
    + AnyQuery
    + 'static
{
    fn watch(
        &self,
        client: &MykoClient,
    ) -> impl tokio_stream::Stream<Item = Vec<<Self as QueryItemType>::Item>>;

    fn register(server: &Arc<MykoServer>) -> Result<(), anyhow::Error> {
        let closure = Arc::new(|ctx: QueryHandlerCtxAny| -> bool {
            let item_ref: Arc<dyn Any + Send + Sync> = ctx.item;
            let query_ref: Arc<dyn Any + Send + Sync> = ctx.query;

            let item = item_ref.downcast::<Self::Item>();
            let query = query_ref.downcast::<Self>();

            if query.is_err() {
                error!(
                    "Query did not correctly downcast in closure: {}",
                    Self::query_id_static()
                );
                return false;
            }

            let query = query.expect("Query downcast should be valid");

            if item.is_err() {
                error!(
                    "Item did not downcast: {} in {}",
                    Self::query_item_type_static(),
                    Self::query_id_static(),
                );
                return false;
            }

            let item = item.expect("Item downcast should be valid");

            <Self as QueryHandler>::test_entity(QueryHandlerCtx::<Self> {
                server_ctx: ctx.ctx,
                item: item.clone(),
                query: query.clone(),
            })
        });

        let parser: Arc<dyn MykoQueryParser> = Arc::new(CapturedQueryParser::<Self>::new());

        // Direct send to QueryManager (bypasses Server routing)
        if let Err(err) = server.query_manager.send_message(
            QueryManagerMsg::RegisterQuery(RegisterQueryData {
                query_id: Self::query_id_static(),
                query_item_type: Self::query_item_type_static(),
                closure,
                parser,
            }),
        ) {
            error!("Failed to register query: {}", err);
        }

        Ok(())
    }
}
