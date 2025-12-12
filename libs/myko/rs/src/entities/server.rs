
use crate::prelude::*;
use crate::{self as myko_rs};

#[myko_item]
pub struct Server {
    pub version: String,
    pub address: String,
    pub port: u16,
    pub started_at: String, // ISO DateTime
}

#[myko_query(Server)]
pub struct GetConnectedServer {}

impl QueryHandler for GetConnectedServer {
    fn test_entity(ctx: QueryHandlerCtx<Self>) -> bool {
        let item_id = ctx.item.id.to_string();
        let host_id = ctx.server_ctx.host_id.to_string();
        item_id == host_id
    }
}

#[myko_query(Server)]
pub struct GetPeerServers {}

impl QueryHandler for GetPeerServers {
    fn test_entity(ctx: QueryHandlerCtx<Self>) -> bool {
        let item_id = ctx.item.id.to_string();
        let host_id = ctx.server_ctx.host_id.to_string();
        item_id != host_id
    }
}
