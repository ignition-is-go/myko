use std::sync::Arc;

use crate::{
    self as myko_rs,
    actors::server::MykoServerCtx,
    query::{self, QueryClosure},
};
use myko_macros::{Eventable, myko_query};
use partially::Partial;
use serde::{Deserialize, Serialize};

#[derive(Partial, PartialEq, Clone, Serialize, Deserialize, Debug, Eventable)]
#[serde(rename_all = "camelCase")]
#[partially(derive(Clone, Serialize, Deserialize, Default))]
pub struct Client {
    id: String,
    hash: String,
    server_id: Arc<str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[myko_query(Client)]
pub struct GetClientsByServerId {
    server_id: Arc<str>,
}

impl QueryClosure<Client> for GetClientsByServerId {
    fn test_entity(item: &Client, ctx: Arc<MykoServerCtx>, query: Arc<Self>) -> bool {
        item.server_id == query.server_id
    }
}
