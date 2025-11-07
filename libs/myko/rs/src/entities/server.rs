use crate::prelude::*;
use crate::{self as myko_rs};
use partially::Partial;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Partial, PartialEq, Clone, Serialize, Deserialize, Debug, Eventable)]
#[serde(rename_all = "camelCase")]
#[partially(derive(Clone, Serialize, Deserialize, Default))]
pub struct Server {
    pub id: Arc<str>,
    pub hash: Arc<str>,
    pub version: String,
    pub address: String,
    pub port: u16,
    pub started_at: String, // ISO DateTime
}

#[myko_query(Server)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetConnectedServer {}

impl QueryHandler for GetConnectedServer {
    fn test_entity(ctx: QueryHandlerContext<Self>) -> bool {
        ctx.item.id.to_string() == ctx.server_ctx.host_id.to_string()
    }
}

#[myko_query(Server)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPeerServers {}

impl QueryHandler for GetPeerServers {
    fn test_entity(ctx: QueryHandlerContext<Self>) -> bool {
        ctx.item.id.to_string() != ctx.server_ctx.host_id.to_string()
    }
}
