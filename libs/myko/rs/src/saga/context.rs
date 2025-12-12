//! Saga context for accessing server resources during event processing.

use std::sync::Arc;

use ractor::ActorRef;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    actors::{
        command::command_manager::CommandManagerMsg,
        event::{event_manager::EventManagerMsg, EventBus},
        query::query_manager::QueryManagerMsg,
    },
    command::{CommandError, CommandId, WrappedCommand},
    server::MykoServerCtx,
};

/// Error type for saga operations
#[derive(Debug, Clone)]
pub struct SagaError {
    pub saga_id: String,
    pub message: String,
}

impl std::fmt::Display for SagaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SagaError({}): {}", self.saga_id, self.message)
    }
}

impl std::error::Error for SagaError {}

/// Context provided to sagas for accessing server resources.
///
/// SagaContext allows sagas to:
/// - Execute commands
/// - Query current state
/// - Access server context (host_id, etc.)
/// - Subscribe to events via the event bus
#[derive(Clone)]
pub struct SagaContext {
    /// Server context with host ID
    pub server_ctx: Arc<MykoServerCtx>,

    /// Reference to the event manager actor
    pub event_manager: ActorRef<EventManagerMsg>,

    /// Reference to the command manager actor
    pub command_manager: ActorRef<CommandManagerMsg>,

    /// Reference to the query manager actor
    pub query_manager: ActorRef<QueryManagerMsg>,

    /// Shared event bus for high-throughput event distribution
    pub event_bus: EventBus,
}

impl SagaContext {
    /// Create a new SagaContext
    pub fn new(
        server_ctx: Arc<MykoServerCtx>,
        event_manager: ActorRef<EventManagerMsg>,
        command_manager: ActorRef<CommandManagerMsg>,
        query_manager: ActorRef<QueryManagerMsg>,
        event_bus: EventBus,
    ) -> Self {
        Self {
            server_ctx,
            event_manager,
            command_manager,
            query_manager,
            event_bus,
        }
    }

    /// Get the host ID for this server
    pub fn host_id(&self) -> Uuid {
        self.server_ctx.host_id
    }

    /// Execute a command and return the result.
    ///
    /// This is useful for sagas that need to perform complex operations
    /// that are already implemented as commands.
    pub async fn execute_command<C>(&self, cmd: &C) -> Result<Value, SagaError>
    where
        C: CommandId + Serialize,
    {
        let tx = Uuid::new_v4().to_string();
        let mut command_value = serde_json::to_value(cmd).map_err(|e| SagaError {
            saga_id: "context".to_string(),
            message: format!("Failed to serialize command: {}", e),
        })?;

        // Add transaction ID to command
        if let Some(obj) = command_value.as_object_mut() {
            obj.insert("tx".to_string(), Value::String(tx.clone()));
        }

        let wrapped = WrappedCommand {
            command: command_value,
            command_id: cmd.command_id(),
        };

        let client_id: Arc<str> = format!("saga-{}", self.host_id()).into();

        ractor::call!(
            self.command_manager,
            CommandManagerMsg::Execute,
            wrapped,
            client_id
        )
        .map_err(|e| SagaError {
            saga_id: "context".to_string(),
            message: format!("Failed to execute command: {}", e),
        })?
        .map_err(|e: CommandError| SagaError {
            saga_id: "context".to_string(),
            message: e.message,
        })
    }
}
