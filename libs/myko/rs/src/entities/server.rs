use crate::prelude::*;
use crate::{self as myko_rs};
use partially::Partial;
use serde::{Deserialize, Serialize};

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
        ctx.item.id.to_string() == ctx.server_ctx.host_id.to_string()
    }
}

#[myko_query(Server)]
pub struct GetPeerServers {}

impl QueryHandler for GetPeerServers {
    fn test_entity(ctx: QueryHandlerCtx<Self>) -> bool {
        ctx.item.id.to_string() != ctx.server_ctx.host_id.to_string()
    }
}
