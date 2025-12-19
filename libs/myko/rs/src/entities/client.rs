use crate::prelude::*;
use crate::report::{ReportContext, ReportHandler};
use crate::{self as myko_rs};
use futures::StreamExt;
use myko_macros::{myko_report, myko_report_output};
use std::sync::Arc;

#[myko_item]
pub struct Client {
    #[belongs_to(Server)]
    pub server_id: Arc<str>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Custom Reports
// ─────────────────────────────────────────────────────────────────────────────

#[myko_report_output]
pub struct ClientStatusOutput {
    pub online: bool,
}

/// Report that returns whether a client is currently connected
#[myko_report(ClientStatusOutput)]
pub struct ClientStatus {
    pub client_id: Arc<str>,
}

impl ReportHandler for ClientStatus {
    type Output = ClientStatusOutput;

    fn compute(&self, ctx: ReportContext) -> crate::report::ReportStream<Self::Output> {
        let client_id = self.client_id.clone();

        // Query all clients and check if one with our id exists
        let stream = ctx.query(GetAllClients {});

        Box::pin(stream.map(move |clients| {
            let online = clients.iter().any(|c| c.id == client_id);
            ClientStatusOutput { online }
        }))
    }
}
