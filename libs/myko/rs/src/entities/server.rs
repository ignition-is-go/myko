use std::any::Any;
use std::sync::Arc;

use crate::item::Eventable;
use crate::query::QueryClosure;
use crate::{self as myko_rs, actors::server::MykoServerCtx};
use myko_macros::{Eventable, myko_query};
use partially::Partial;
use serde::{Deserialize, Serialize};

#[derive(Partial, PartialEq, Clone, Serialize, Deserialize, Debug, Eventable)]
#[serde(rename_all = "camelCase")]
#[partially(derive(Clone, Serialize, Deserialize, Default))]
pub struct Server {
    pub id: String,
    pub hash: String,
    pub version: String,
    pub address: String,
    pub port: u16,
    pub started_at: String, // ISO DateTime
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[myko_query(Server)]
pub struct GetConnectedServer {}

impl QueryClosure<Server> for GetConnectedServer {
    fn test_entity(item: &Server, ctx: Arc<MykoServerCtx>, query: Arc<Self>) -> bool {
        item.id == ctx.host_id.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[myko_query(Server)]
pub struct GetPeerServers {}

impl QueryClosure<Server> for GetPeerServers {
    fn test_entity(item: &Server, ctx: Arc<MykoServerCtx>, query: Arc<Self>) -> bool {
        item.id != ctx.host_id.to_string()
    }
}
