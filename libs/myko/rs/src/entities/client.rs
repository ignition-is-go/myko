use crate::prelude::*;
use crate::{self as myko_rs};
use partially::Partial;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Partial, PartialEq, Clone, Serialize, Deserialize, Debug, Eventable)]
#[serde(rename_all = "camelCase")]
#[partially(derive(Clone, Serialize, Deserialize, Default))]
pub struct Client {
    id: Arc<str>,
    hash: Arc<str>,
    server_id: Arc<str>,
}

#[myko_query(Client)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetClientsByServerId {
    server_id: Arc<str>,
}

impl QueryHandler for GetClientsByServerId {
    fn test_entity(ctx: QueryHandlerContext<Self>) -> bool {
        ctx.item.server_id == ctx.query.server_id
    }
}
