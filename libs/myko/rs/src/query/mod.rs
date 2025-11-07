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

pub trait QueryId {
    fn query_id(&self) -> Arc<str>;
}

pub trait QueryIdStatic {
    fn query_id_static() -> Arc<str>;
}

pub trait QueryItemType {
    type Item;
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
    fn test_entity(ctx: QueryHandlerContext<Self>) -> bool;
}

pub struct QueryHandlerContext<TQuery: QueryItemType> {
    pub item: Arc<TQuery::Item>,
    pub query: Arc<TQuery>,
    pub server_ctx: Arc<MykoServerCtx>,
}

pub struct QueryHandlerContextAny {
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
        let closure = Arc::new(|ctx: QueryHandlerContextAny| -> bool {
            let item_ref: Arc<dyn Any + Send + Sync + 'static> = ctx.item;
            let query_ref: Arc<dyn Any + Send + Sync + 'static> = ctx.query;

            let item = item_ref.downcast_ref::<Arc<Self::Item>>();
            let query = query_ref.downcast_ref::<Arc<Self>>();

            if let (Some(item), Some(query)) = (item, query) {
                <Self as QueryHandler>::test_entity(QueryHandlerContext::<Self> {
                    server_ctx: ctx.ctx,
                    item: item.clone(),
                    query: query.clone(),
                })
            } else {
                false
            }
        });

        let parser: Arc<dyn MykoQueryParser> = Arc::new(CapturedQueryParser::<Self>::new());

        match server
            .server
            .send_message(crate::actors::server::ServerMsg::QueryManagerMsg(
                QueryManagerMsg::RegisterQuery(RegisterQueryData {
                    query_id: Self::query_id_static(),
                    query_item_type: Self::query_item_type_static(),
                    closure,
                    parser,
                }),
            )) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to register query: {}", err);
            }
        };
        Ok(())
    }
}
