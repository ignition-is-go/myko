use std::sync::Arc;

use hyphae::MapExt;
use myko_macros::{myko_command, myko_report, myko_report_output};

use crate::{
    entities::server::{Server, ServerId},
    prelude::*,
    report::{ReportContext, ReportHandler},
};

#[myko_item]
pub struct Client {
    #[belongs_to(Server)]
    pub server_id: ServerId,

    /// Remote address of the WebSocket connection (e.g., "192.168.1.5:54320").
    #[serde(default)]
    pub address: Option<Arc<str>>,

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
    pub client_id: ClientId,
}

impl ReportHandler for ClientStatus {
    type Output = ClientStatusOutput;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let client_id = self.client_id.clone();

        // Query all clients and check if one with our id exists
        ctx.query_map(GetAllClients {})
            .entries()
            .map(move |clients| {
                let online = clients
                    .iter()
                    .any(|(_, c)| c.id.as_ref() == client_id.as_ref());
                Arc::new(ClientStatusOutput { online })
            })
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

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let client_id = ctx
            .client_id()
            .map(|id| ClientId::from(Arc::<str>::from(id)));

        // Query all clients and find the requesting client's windback status
        ctx.query_map(GetAllClients {})
            .entries()
            .map(move |clients| {
                let windback = client_id
                    .as_ref()
                    .and_then(|cid| clients.iter().find(|(_, c)| c.id.as_ref() == cid.as_ref()))
                    .and_then(|(_, c)| c.windback.clone());
                Arc::new(WindbackStatusOutput { windback })
            })
    }
}

/// Command to set the windback time for the current client.
/// When set, queries return historical state as of the specified timestamp.
#[myko_command(bool)]
pub struct SetClientWindbackTime {
    /// ISO timestamp to wind back to
    pub windback: Arc<str>,
}

impl crate::command::CommandHandler for SetClientWindbackTime {
    fn execute(
        self,
        ctx: crate::command::CommandContext,
    ) -> Result<bool, crate::command::CommandError> {
        let client_id = ctx.client_id().ok_or_else(|| {
            crate::command::CommandError::new(
                ctx.tx(),
                "SetClientWindbackTime",
                "No client_id in context - windback requires a WebSocket connection",
            )
        })?;

        // Find the client entity
        let client = ctx
            .exec_report(GetClientById {
                id: ClientId::from(Arc::<str>::from(client_id)),
            })?
            .ok_or_else(|| {
                CommandError::new(
                    ctx.tx(),
                    "SetClientWindbackTime",
                    format!("Client {} not found", client_id),
                )
            })?;

        // Update client with new windback time
        let updated_client = Client {
            id: client.id.clone(),
            server_id: client.server_id.clone(),
            address: client.address.clone(),
            windback: Some(self.windback.clone()),
        };

        ctx.emit_set(&updated_client)?;

        Ok(true)
    }
}

/// Command to clear the windback time for the current client.
/// Returns the client to viewing live state.
#[myko_command(bool)]
pub struct ClearClientWindbackTime {}

impl crate::command::CommandHandler for ClearClientWindbackTime {
    fn execute(
        self,
        ctx: crate::command::CommandContext,
    ) -> Result<bool, crate::command::CommandError> {
        let client_id = ctx.client_id().ok_or_else(|| {
            crate::command::CommandError::new(
                ctx.tx(),
                "ClearClientWindbackTime",
                "No client_id in context - windback requires a WebSocket connection",
            )
        })?;

        // Find the client entity
        let client = ctx
            .exec_report(GetClientById {
                id: ClientId::from(Arc::<str>::from(client_id)),
            })?
            .ok_or_else(|| {
                CommandError::new(
                    ctx.tx(),
                    "ClearClientWindbackTime",
                    format!("Client {} not found", client_id),
                )
            })?;
        // Update client to clear windback
        let updated_client = Client {
            id: client.id.clone(),
            server_id: client.server_id.clone(),
            address: client.address.clone(),
            windback: None,
        };

        ctx.emit_set(&updated_client)?;

        Ok(true)
    }
}
