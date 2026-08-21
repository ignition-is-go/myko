use std::sync::Arc;

use hyphae::MapExt;
use myko_macros::{myko_command, myko_report, myko_report_output};

use crate::{
    entities::server::{Server, ServerId},
    prelude::*,
    report::{ReportContext, ReportHandler},
    server::client_registry,
};

#[myko_item]
#[derive(Eq)]
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
crate::mark_framework_typegen_type!(ClientId);

// ─────────────────────────────────────────────────────────────────────────────
// Custom Reports
// ─────────────────────────────────────────────────────────────────────────────

#[myko_report_output]
#[derive(Eq)]
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

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        // ClientStatus currently targets a single-server cluster. Peer-aware
        // routing can be added here later without making replayed Client rows
        // the source of connection liveness again.
        let client_id: Arc<str> = self.client_id.clone().into();
        client_registry()
            .watch_connected(&client_id)
            .map(|online| Arc::new(ClientStatusOutput { online: *online }))
            .materialize()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Windback Support
// ─────────────────────────────────────────────────────────────────────────────

/// Report that returns the current windback time for the requesting client.
/// Returns None if the client is not in windback mode.
#[myko_report_output]
#[derive(Eq)]
pub struct WindbackStatusOutput {
    /// ISO timestamp if in windback mode, None otherwise
    pub windback: Option<Arc<str>>,
}

#[myko_report(WindbackStatusOutput)]
pub struct WindbackStatus {}

impl ReportHandler for WindbackStatus {
    type Output = WindbackStatusOutput;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let client_id = ctx.client_id().map_or_else(Arc::<str>::default, Arc::from);
        let store = ctx.registry().get_or_create(Client::ENTITY_NAME_STATIC);

        store.get(&client_id).map(|client| {
            let windback = client
                .as_ref()
                .and_then(|client| client.as_any().downcast_ref::<Client>())
                .and_then(|client| client.windback.clone());
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
                    format!("Client {client_id} not found"),
                )
            })?;

        // Update client with new windback time
        let updated_client = Client {
            id: client.id.clone(),
            server_id: client.server_id.clone(),
            address: client.address.clone(),
            windback: Some(self.windback),
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
                    format!("Client {client_id} not found"),
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

#[cfg(test)]
mod indexed_join_tests {
    use hyphae::{CellMap, LeftJoinExt, MapQuery};

    use super::*;

    #[test]
    #[allow(clippy::similar_names)]
    fn generated_relation_indexes_arc_backed_query_rows() {
        let servers = CellMap::<Arc<str>, Arc<Server>>::new();
        let clients = CellMap::<Arc<str>, Arc<Client>>::new();
        let joined = servers
            .clone()
            .left_join_fk::<ClientServerIdRelation, _>(clients.clone())
            .materialize();

        let server_a_id: Arc<str> = "server-a".into();
        let server_b_id: Arc<str> = "server-b".into();
        servers.insert(
            server_a_id.clone(),
            Arc::new(Server {
                id: ServerId::from(server_a_id.clone()),
                version: "test".to_string(),
                address: "127.0.0.1".to_string(),
                port: 1,
                started_at: "1970-01-01T00:00:00Z".to_string(),
            }),
        );
        servers.insert(
            server_b_id.clone(),
            Arc::new(Server {
                id: ServerId::from(server_b_id.clone()),
                version: "test".to_string(),
                address: "127.0.0.1".to_string(),
                port: 2,
                started_at: "1970-01-01T00:00:00Z".to_string(),
            }),
        );

        let client_id: Arc<str> = "client".into();
        let client = |server_id: Arc<str>| {
            Arc::new(Client {
                id: ClientId::from(client_id.clone()),
                server_id: ServerId::from(server_id),
                address: None,
                windback: None,
            })
        };
        clients.insert(client_id.clone(), client(server_a_id.clone()));

        assert!(matches!(
            joined.get_value(&server_a_id),
            Some((_, rows)) if rows.len() == 1
        ));
        assert!(matches!(
            joined.get_value(&server_b_id),
            Some((_, rows)) if rows.is_empty()
        ));

        clients.insert(client_id.clone(), client(server_b_id.clone()));
        assert!(matches!(
            joined.get_value(&server_a_id),
            Some((_, rows)) if rows.is_empty()
        ));
        assert!(matches!(
            joined.get_value(&server_b_id),
            Some((_, rows)) if rows.len() == 1
        ));
    }
}
