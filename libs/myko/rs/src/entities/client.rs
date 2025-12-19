use crate::prelude::*;
use crate::report::{ReportContext, ReportHandler};
use crate::{self as myko_rs};
use futures::StreamExt;
use myko_macros::{myko_command, myko_report, myko_report_output};
use std::sync::Arc;

#[myko_item]
pub struct Client {
    #[belongs_to(Server)]
    pub server_id: Arc<str>,

    /// ISO timestamp for windback mode. When set, the client sees historical state
    /// as of this timestamp instead of live state.
    pub windback: Option<Arc<str>>,
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

// ─────────────────────────────────────────────────────────────────────────────
// Windback Support
// ─────────────────────────────────────────────────────────────────────────────

/// Report that returns the current windback time for the requesting client.
/// Returns None if the client is not in windback mode.
#[myko_report_output]
pub struct WindbackStatusOutput {
    /// ISO timestamp if in windback mode, None otherwise
    pub windback: Option<Arc<str>>,
}

#[myko_report(WindbackStatusOutput)]
pub struct WindbackStatus {}

impl ReportHandler for WindbackStatus {
    type Output = WindbackStatusOutput;

    fn compute(&self, ctx: ReportContext) -> crate::report::ReportStream<Self::Output> {
        let client_id = ctx.client_id().map(|s| Arc::from(s));

        // Query all clients and find the requesting client's windback status
        let stream = ctx.query(GetAllClients {});

        Box::pin(stream.map(move |clients| {
            let windback = client_id
                .as_ref()
                .and_then(|cid| clients.iter().find(|c| &c.id == cid))
                .and_then(|c| c.windback.clone());
            WindbackStatusOutput { windback }
        }))
    }
}

/// Command to set the windback time for the current client.
/// When set, queries return historical state as of the specified timestamp.
#[myko_command(bool)]
pub struct SetClientWindbackTime {
    /// ISO timestamp to wind back to
    pub windback: Arc<str>,
}

impl SetClientWindbackTimeHandler {
    pub async fn execute(
        cmd: SetClientWindbackTime,
        ctx: CommandContext,
    ) -> Result<bool, CommandError> {
        let client_id = ctx.client_id().ok_or_else(|| CommandError {
            tx: ctx.tx().to_string(),
            command_id: "SetClientWindbackTime".to_string(),
            message: "No client_id in context - windback requires a WebSocket connection"
                .to_string(),
        })?;

        // Find the client entity
        let client = ctx
            .query_one(&GetClientsByIds {
                ids: vec![Arc::from(client_id)],
            })
            .await?
            .ok_or_else(|| CommandError {
                tx: ctx.tx().to_string(),
                command_id: "SetClientWindbackTime".to_string(),
                message: format!("Client {} not found", client_id),
            })?;

        let windback = cmd.windback.clone();

        // Update client with new windback time
        let updated_client = Client {
            id: client.id.clone(),
            hash: Arc::from(Uuid::new_v4().to_string()),
            server_id: client.server_id,
            windback: Some(cmd.windback),
        };

        ctx.emit_set(&updated_client)?;

        // Update the windback cache in MessageHandler
        ctx.server_ctx().update_windback(client.id, Some(windback));

        Ok(true)
    }
}

/// Command to clear the windback time for the current client.
/// Returns the client to viewing live state.
#[myko_command(bool)]
pub struct ClearClientWindbackTime {}

impl ClearClientWindbackTimeHandler {
    pub async fn execute(
        _cmd: ClearClientWindbackTime,
        ctx: CommandContext,
    ) -> Result<bool, CommandError> {
        let client_id = ctx.client_id().ok_or_else(|| CommandError {
            tx: ctx.tx().to_string(),
            command_id: "ClearClientWindbackTime".to_string(),
            message: "No client_id in context - windback requires a WebSocket connection"
                .to_string(),
        })?;

        // Find the client entity
        let client = ctx
            .query_one(&GetClientsByIds {
                ids: vec![Arc::from(client_id)],
            })
            .await?
            .ok_or_else(|| CommandError {
                tx: ctx.tx().to_string(),
                command_id: "ClearClientWindbackTime".to_string(),
                message: format!("Client {} not found", client_id),
            })?;

        // Update client to clear windback
        let updated_client = Client {
            id: client.id.clone(),
            hash: Arc::from(Uuid::new_v4().to_string()),
            server_id: client.server_id,
            windback: None,
        };

        ctx.emit_set(&updated_client)?;

        // Update the windback cache in MessageHandler
        ctx.server_ctx().update_windback(client.id, None);

        Ok(true)
    }
}
