use std::{collections::HashMap, sync::Arc};

use log::{debug, error, trace};
use ractor::{Actor, ActorRef, RpcReplyPort};
use serde_json::Value;

use crate::{
    actors::{
        event::event_manager::EventManagerMsg,
        query::query_manager::QueryManagerMsg,
        report::report_manager::ReportManagerMsg,
    },
    command::{
        CommandContext, CommandError, CommandHandler, CommandHandlerRegistration, WrappedCommand,
    },
    server::MykoServerCtx,
};

pub struct CommandManager;

pub struct CommandManagerArgs {
    pub ctx: Arc<MykoServerCtx>,
    pub event_manager: ActorRef<EventManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub report_manager: ActorRef<ReportManagerMsg>,
}

pub struct CommandManagerState {
    ctx: Arc<MykoServerCtx>,
    event_manager: ActorRef<EventManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
    /// Registered command handlers by command_id
    handlers: HashMap<&'static str, Box<dyn CommandHandler>>,
    /// Self-reference for nested command execution
    myself: Option<ActorRef<CommandManagerMsg>>,
}

pub enum CommandManagerMsg {
    /// Execute a command from a client
    Execute(
        WrappedCommand,
        Arc<str>, // client_id
        RpcReplyPort<Result<Value, CommandError>>,
    ),
    /// Execute a nested command (from within a handler)
    ExecuteNested(
        WrappedCommand,
        Arc<str>,    // client_id
        Vec<String>, // lineage
        String,      // created_at
        RpcReplyPort<Result<Value, CommandError>>,
    ),
}

impl Actor for CommandManager {
    type State = CommandManagerState;
    type Msg = CommandManagerMsg;
    type Arguments = CommandManagerArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("CommandManager starting");

        // Collect all registered handlers from inventory
        let mut handlers: HashMap<&'static str, Box<dyn CommandHandler>> = HashMap::new();
        for registration in inventory::iter::<CommandHandlerRegistration> {
            debug!("Registering command handler: {}", registration.command_id);
            let handler = (registration.factory)();
            handlers.insert(registration.command_id, handler);
        }

        debug!("CommandManager registered {} handlers", handlers.len());

        Ok(CommandManagerState {
            ctx: args.ctx,
            event_manager: args.event_manager,
            query_manager: args.query_manager,
            report_manager: args.report_manager,
            handlers,
            myself: Some(myself),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            CommandManagerMsg::Execute(command, client_id, reply) => {
                let command_id = command.command_id.as_str();
                let tx: Arc<str> = command
                    .command
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .map(|s| s.into())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string().into());

                let created_at = command
                    .command
                    .get("createdAt")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

                trace!("Executing command {} with tx {}", command_id, tx);

                let result = self
                    .execute_command(
                        state,
                        &command,
                        client_id,
                        vec!["client".to_string()],
                        created_at,
                    )
                    .await;

                let _ = reply.send(result);
            }
            CommandManagerMsg::ExecuteNested(command, client_id, lineage, created_at, reply) => {
                let command_id = command.command_id.as_str();
                trace!("Executing nested command {} with lineage {:?}", command_id, lineage);

                let result = self
                    .execute_command(state, &command, client_id, lineage, created_at)
                    .await;

                let _ = reply.send(result);
            }
        }
        Ok(())
    }
}

impl CommandManager {
    async fn execute_command(
        &self,
        state: &mut CommandManagerState,
        command: &WrappedCommand,
        client_id: Arc<str>,
        lineage: Vec<String>,
        created_at: String,
    ) -> Result<Value, CommandError> {
        let command_id = command.command_id.as_str();
        let tx: Arc<str> = command
            .command
            .get("tx")
            .and_then(|v| v.as_str())
            .map(|s| s.into())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string().into());

        // Look up handler
        let handler = state.handlers.get(command_id).ok_or_else(|| {
            error!(
                "No handler registered for command {}: {:?}",
                command_id,
                state.handlers.keys().collect::<Vec<_>>()
            );
            CommandError {
                tx: tx.to_string(),
                message: format!("No handler registered for command: {}", command_id),
            }
        })?;

        // Build context
        let ctx = CommandContext::new(
            client_id,
            tx.clone(),
            lineage,
            created_at,
            state.ctx.clone(),
            state.event_manager.clone(),
            state.myself.clone().expect("myself should be set"),
            state.query_manager.clone(),
            state.report_manager.clone(),
        );

        // Execute handler
        handler.execute(command.command.clone(), ctx).await
    }
}
