use std::{collections::HashMap, sync::Arc};

use log::{debug, error, trace};
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
    context::RequestContext,
    runtime::{Actor, ActorHandle, ActorRef, RpcReplyPort},
    server::MykoServerCtx,
};

pub struct CommandManagerArgs {
    pub ctx: Arc<MykoServerCtx>,
    pub event_manager: ActorRef<EventManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub report_manager: ActorRef<ReportManagerMsg>,
}

pub struct CommandManager {
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
    /// Execute a command from a client.
    /// The RequestContext is created by the caller (MessageHandler) from the WebSocket message.
    Execute(
        WrappedCommand,
        RequestContext,
        RpcReplyPort<Result<Value, CommandError>>,
    ),
    /// Execute a nested command (from within a handler).
    /// The RequestContext has extended lineage from the parent command.
    ExecuteNested(
        WrappedCommand,
        RequestContext,
        RpcReplyPort<Result<Value, CommandError>>,
    ),
    /// Set self-reference (called immediately after spawn)
    SetMyself(ActorRef<CommandManagerMsg>),
}

impl std::fmt::Debug for CommandManagerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandManagerMsg::Execute(cmd, req, _) => {
                write!(f, "Execute({}, tx={})", cmd.command_id, req.tx())
            }
            CommandManagerMsg::ExecuteNested(cmd, req, _) => {
                write!(f, "ExecuteNested({}, tx={})", cmd.command_id, req.tx())
            }
            CommandManagerMsg::SetMyself(_) => write!(f, "SetMyself"),
        }
    }
}

impl CommandManager {
    pub fn new(args: CommandManagerArgs) -> Self {
        // Collect all registered handlers from inventory
        let mut handlers: HashMap<&'static str, Box<dyn CommandHandler>> = HashMap::new();
        for registration in inventory::iter::<CommandHandlerRegistration> {
            trace!("Registering command handler: {}", registration.command_id);
            let handler = (registration.factory)();
            handlers.insert(registration.command_id, handler);
        }

        debug!("CommandManager: {} handlers", handlers.len());

        Self {
            ctx: args.ctx,
            event_manager: args.event_manager,
            query_manager: args.query_manager,
            report_manager: args.report_manager,
            handlers,
            myself: None,
        }
    }

    pub fn spawn(args: CommandManagerArgs) -> ActorHandle<CommandManagerMsg> {
        let actor = Self::new(args);
        let handle = crate::runtime::spawn::spawn(actor);

        // Set self-reference
        let actor_ref = handle.actor_ref();
        let _ = actor_ref.send_message(CommandManagerMsg::SetMyself(actor_ref.clone()));

        handle
    }

    fn execute_command(
        &self,
        command: &WrappedCommand,
        req: RequestContext,
    ) -> Result<Value, CommandError> {
        let command_id = command.command_id.as_str();

        // Look up handler
        let handler = self.handlers.get(command_id).ok_or_else(|| {
            error!(
                "No handler registered for command {}: {:?}",
                command_id,
                self.handlers.keys().collect::<Vec<_>>()
            );
            CommandError {
                tx: req.tx().to_string(),
                message: format!("No handler registered for command: {}", command_id),
            }
        })?;

        // Build context from RequestContext
        let ctx = CommandContext::new(
            req,
            self.ctx.clone(),
            self.event_manager.clone(),
            self.myself.clone().expect("myself should be set"),
            self.query_manager.clone(),
            self.report_manager.clone(),
        );

        // Execute handler using shared tokio runtime
        self.ctx
            .tokio_handle
            .block_on(handler.execute(command.command.clone(), ctx))
    }
}

impl Actor for CommandManager {
    type Msg = CommandManagerMsg;

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            CommandManagerMsg::Execute(command, req, reply) => {
                let command_id = command.command_id.as_str();
                trace!(
                    "Executing command {} with tx {} (lineage: {})",
                    command_id,
                    req.tx(),
                    req.lineage_string()
                );

                let result = self.execute_command(&command, req);
                let _ = reply.send(result);
            }
            CommandManagerMsg::ExecuteNested(command, req, reply) => {
                let command_id = command.command_id.as_str();
                trace!(
                    "Executing nested command {} (lineage: {})",
                    command_id,
                    req.lineage_string()
                );

                let result = self.execute_command(&command, req);
                let _ = reply.send(result);
            }
            CommandManagerMsg::SetMyself(myself) => {
                self.myself = Some(myself);
            }
        }
    }
}
