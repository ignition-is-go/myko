use crate::prelude::*;
use crate::{self as myko_rs};
use partially::Partial;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[myko_item]
pub struct Client {
    server_id: Arc<str>,
}

#[myko_query(Client)]
pub struct GetClientsByServerId {
    server_id: Arc<str>,
}

impl QueryHandler for GetClientsByServerId {
    fn test_entity(ctx: QueryHandlerCtx<Self>) -> bool {
        ctx.item.server_id == ctx.query.server_id
    }
}

// fn test() {
//     let a = vec![1, 2, 3];

//     let c = Client {
//         id: "1".into(),
//         hash: "h".into(),
//         server_id: "1".into(),
//     };

//     let pc = PartialClient {
//         ..Default::default()
//     };

//     let ac = Arc::new(c);

//     let m: bool = pc.matches(&ac);

//     // let b = pc.
// }
